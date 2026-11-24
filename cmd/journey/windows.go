package main

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// A device counts as missing from a window when it fed one of the key's
// windows in the lookback before it, but not this one.
const lookback = 30 * time.Minute

type windowRecord struct {
	Hop         string    `json:"hop"`
	Op          string    `json:"op"`
	Key         string    `json:"key"`
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	At          time.Time `json:"at"`
	InputHop    string    `json:"input_hop"`
	Inputs      uint32    `json:"inputs"`
	DeviceCount uint32    `json:"device_count"`
	Devices     []string  `json:"devices,omitempty"`
	Digest      string    `json:"device_digest,omitempty"`
	Output      string    `json:"output,omitempty"`
	Missing     []string  `json:"missing"` // devices that fed earlier windows of the key but not this one
}

// windowRecords reads the window records from `from` on, oldest first,
// optionally for one key, and works out which devices each one is missing.
func (s *server) windowRecords(ctx context.Context, key string, from time.Time) ([]windowRecord, error) {
	q := `SELECT hop, op, group_key, window_start, window_end, ts, input_hop, inputs, device_count, devices, device_digest, output
		FROM window_records WHERE tenant = ? AND window_end >= ?`
	args := []any{s.rules.Tenant, from.Add(-lookback)}
	if key != "" {
		q += ` AND group_key = ?`
		args = append(args, key)
	}
	q += ` ORDER BY group_key, window_start, ts LIMIT 1 BY group_key, window_start, hop`
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var all []windowRecord
	for rows.Next() {
		var w windowRecord
		if err := rows.Scan(&w.Hop, &w.Op, &w.Key, &w.Start, &w.End, &w.At, &w.InputHop, &w.Inputs, &w.DeviceCount, &w.Devices, &w.Digest, &w.Output); err != nil {
			return nil, err
		}
		all = append(all, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Missing devices, per key, against the previous windows of that key.
	out := []windowRecord{}
	for i := range all {
		w := &all[i]
		w.Missing = []string{}
		if w.Digest == "" {
			had := map[string]bool{}
			for _, d := range w.Devices {
				had[d] = true
			}
			before := map[string]bool{}
			for j := i - 1; j >= 0 && all[j].Key == w.Key && !all[j].Start.Before(w.Start.Add(-lookback)); j-- {
				for _, d := range all[j].Devices {
					before[d] = true
				}
			}
			for d := range before {
				if !had[d] {
					w.Missing = append(w.Missing, d)
				}
			}
			sort.Strings(w.Missing)
		}
		if !w.End.Before(from) {
			out = append(out, *w)
		}
	}
	return out, nil
}

// windows lists recent aggregated outputs, newest first, with the devices
// each one is missing.
func (s *server) windows(w http.ResponseWriter, r *http.Request) {
	window := 15 * time.Minute
	if q := r.URL.Query().Get("window"); q != "" {
		if d, err := time.ParseDuration(q); err == nil && d > 0 && d <= 72*time.Hour {
			window = d
		}
	}
	list, err := s.windowRecords(r.Context(), r.URL.Query().Get("key"), time.Now().Add(-window))
	if err != nil {
		httpError(w, err)
		return
	}
	sort.SliceStable(list, func(i, j int) bool { return list[i].Start.After(list[j].Start) })
	if len(list) > 200 {
		list = list[:200]
	}
	writeJSON(w, map[string]any{"windows": list})
}

// windowDetail resolves one output's inputs: the hop events at its input
// hop, inside its window, from the devices it lists.
func (s *server) windowDetail(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	startMs, err := strconv.ParseInt(r.PathValue("start"), 10, 64)
	if err != nil || key == "" {
		http.Error(w, "want /api/windows/{key}/{start in unix ms}", http.StatusBadRequest)
		return
	}
	start := time.UnixMilli(startMs)
	list, err := s.windowRecords(r.Context(), key, start)
	if err != nil {
		httpError(w, err)
		return
	}
	var rec *windowRecord
	for i := range list {
		if list[i].Start.Equal(start) {
			rec = &list[i]
			break
		}
	}
	if rec == nil {
		http.Error(w, "no such window", http.StatusNotFound)
		return
	}
	type input struct {
		Device   string `json:"device"`
		Readings uint64 `json:"readings"` // seen by Hopwake at the input hop in the window
	}
	inputs := []input{}
	var seen uint64
	if len(rec.Devices) > 0 {
		rows, err := s.db.Query(r.Context(), `
			SELECT device, count() FROM hop_events
			WHERE tenant = ? AND hop = ? AND device IN ? AND ts >= ? AND ts < ?
			GROUP BY device ORDER BY device`, s.rules.Tenant, rec.InputHop, rec.Devices, rec.Start, rec.End)
		if err != nil {
			httpError(w, err)
			return
		}
		for rows.Next() {
			var x input
			if err := rows.Scan(&x.Device, &x.Readings); err != nil {
				httpError(w, err)
				return
			}
			seen += x.Readings
			inputs = append(inputs, x)
		}
	}
	// Where each missing device's readings got to in the window.
	type absent struct {
		Device   string    `json:"device"`
		Furthest string    `json:"furthest,omitempty"` // the furthest route hop that saw it in the window
		LastSeen time.Time `json:"last_seen,omitempty"`
	}
	missing := []absent{}
	if len(rec.Missing) > 0 {
		rows, err := s.db.Query(r.Context(), `
			SELECT device, groupUniqArray(hop), max(ts) FROM hop_events
			WHERE tenant = ? AND device IN ? AND ts >= ? AND ts < ?
			GROUP BY device`, s.rules.Tenant, rec.Missing, rec.Start, rec.End)
		if err != nil {
			httpError(w, err)
			return
		}
		got := map[string]absent{}
		hops := s.rules.Hops()
		for rows.Next() {
			var a absent
			var seenAt []string
			if err := rows.Scan(&a.Device, &seenAt, &a.LastSeen); err != nil {
				httpError(w, err)
				return
			}
			for i := len(hops) - 1; i >= 0 && a.Furthest == ""; i-- {
				for _, h := range seenAt {
					if h == hops[i] {
						a.Furthest = h
					}
				}
			}
			got[a.Device] = a
		}
		for _, d := range rec.Missing {
			a, ok := got[d]
			if !ok {
				a = absent{Device: d}
			}
			missing = append(missing, a)
		}
	}
	writeJSON(w, map[string]any{"window": rec, "inputs": inputs, "seen": seen, "missing": missing})
}

// deviceWindows lists the outputs a device fed in the window, and those of
// its keys that it is missing from.
func (s *server) deviceWindows(ctx context.Context, device string, from time.Time) (fed, missed []windowRecord, err error) {
	list, err := s.windowRecords(ctx, "", from)
	if err != nil {
		return nil, nil, err
	}
	fed, missed = []windowRecord{}, []windowRecord{}
	for _, w := range list {
		for _, d := range w.Devices {
			if d == device {
				fed = append(fed, w)
			}
		}
		for _, d := range w.Missing {
			if d == device {
				missed = append(missed, w)
			}
		}
	}
	newest := func(l []windowRecord) {
		sort.SliceStable(l, func(i, j int) bool { return l[i].Start.After(l[j].Start) })
	}
	newest(fed)
	newest(missed)
	if len(fed) > 10 {
		fed = fed[:10]
	}
	// Strip long lists from what goes to the device page.
	for _, l := range [][]windowRecord{fed, missed} {
		for i := range l {
			l[i].Devices = nil
		}
	}
	return fed, missed, nil
}
