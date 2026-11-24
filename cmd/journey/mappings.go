package main

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/psardana/hopwake/internal/mapping"
)

// Mapping proposals need enough paired readings, nearly all agreeing.
const (
	minPairs   = 20
	agreeShare = 0.95
)

type pairPoint struct {
	Device string
	Seq    uint64
	At     time.Time
	mapping.Pair
}

// pairs returns readings captured at both hops of an edge (watched devices
// only), newest first. device may be empty for all devices.
func (s *server) pairs(ctx context.Context, inHop, outHop, device string, from time.Time, limit int) ([]pairPoint, error) {
	q := `SELECT a.device, a.seq, b.ts, a.payload, b.payload
		FROM payload_samples AS a
		INNER JOIN payload_samples AS b ON a.journey_id = b.journey_id AND a.device = b.device
		WHERE a.tenant = ? AND a.hop = ? AND b.hop = ? AND a.ts >= ? AND b.ts >= ?
			AND abs(dateDiff('second', a.ts, b.ts)) < ?`
	args := []any{s.rules.Tenant, inHop, outHop, from, from, int64(s.span.Seconds())}
	if device != "" {
		q += ` AND a.device = ?`
		args = append(args, device)
	}
	q += ` ORDER BY b.ts DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pairPoint
	for rows.Next() {
		var p pairPoint
		var in, outPayload string
		if err := rows.Scan(&p.Device, &p.Seq, &p.At, &in, &outPayload); err != nil {
			return nil, err
		}
		a, err1 := mapping.Flatten([]byte(in))
		b, err2 := mapping.Flatten([]byte(outPayload))
		if err1 != nil || err2 != nil {
			continue
		}
		p.In, p.Out, p.Label = a, b, p.Device
		out = append(out, p)
	}
	return out, rows.Err()
}

type proposedRelation struct {
	mapping.Relation
	Confirmed string `json:"confirmed,omitempty"` // the confirmed expression, if any
	Status    string `json:"status"`              // confirmed, differs or new
}

type edgeMapping struct {
	From      string             `json:"from"`
	To        string             `json:"to"`
	Via       string             `json:"via,omitempty"`
	Pairs     int                `json:"pairs"`
	Devices   int                `json:"devices"`
	Relations []proposedRelation `json:"relations"`
	Unseen    map[string]string  `json:"unseen,omitempty"` // confirmed fields no proposal supports
	Dropped   []string           `json:"dropped"`
	New       []string           `json:"new"`
	YAML      string             `json:"yaml"`
}

// mappings proposes, per route edge, how the service maps fields, from the
// readings of watched devices in the last 24 hours, next to what is confirmed.
func (s *server) mappings(w http.ResponseWriter, r *http.Request) {
	hops := s.rules.Hops()
	from := time.Now().Add(-24 * time.Hour)
	out := []edgeMapping{}
	for i := 0; i+1 < len(hops); i++ {
		ps, err := s.pairs(r.Context(), hops[i], hops[i+1], "", from, 2000)
		if err != nil {
			httpError(w, err)
			return
		}
		raw := make([]mapping.Pair, len(ps))
		devices := map[string]bool{}
		for k, p := range ps {
			raw[k] = p.Pair
			devices[p.Device] = true
		}
		prop := mapping.Propose(raw, minPairs, agreeShare)
		confirmed := s.rules.Mapping(hops[i+1])
		e := edgeMapping{From: hops[i], To: hops[i+1], Via: s.rules.Via(hops[i+1]), Pairs: prop.Pairs, Devices: len(devices),
			Dropped: prop.Dropped, New: prop.New, YAML: mapping.YAML(prop.Relations), Relations: []proposedRelation{}}
		seen := map[string]bool{}
		for _, rel := range prop.Relations {
			pr := proposedRelation{Relation: rel, Status: "new"}
			if c, ok := confirmed[rel.Out]; ok {
				pr.Confirmed = c.String()
				pr.Status = "differs"
				if c.String() == rel.Expr {
					pr.Status = "confirmed"
				}
			}
			seen[rel.Out] = true
			e.Relations = append(e.Relations, pr)
		}
		for f, c := range confirmed {
			if !seen[f] {
				if e.Unseen == nil {
					e.Unseen = map[string]string{}
				}
				e.Unseen[f] = c.String()
			}
		}
		out = append(out, e)
	}
	writeJSON(w, map[string]any{"min_pairs": minPairs, "edges": out})
}

// finding is a confirmed mapping that a device's readings broke.
type finding struct {
	Via     string           `json:"via,omitempty"`
	From    string           `json:"from"`
	To      string           `json:"to"`
	Checked int              `json:"checked"` // paired readings checked
	Broken  int              `json:"broken"`  // of those, how many broke the mapping
	First   time.Time        `json:"first"`
	Last    time.Time        `json:"last"`
	Example mapping.Mismatch `json:"example"` // from the newest broken reading
	Seq     uint64           `json:"seq"`
}

// findings checks a device's watched readings against confirmed mappings.
func (s *server) findings(ctx context.Context, device string, from time.Time) ([]finding, error) {
	hops := s.rules.Hops()
	out := []finding{}
	for i := 0; i+1 < len(hops); i++ {
		m := s.rules.Mapping(hops[i+1])
		if len(m) == 0 {
			continue
		}
		ps, err := s.pairs(ctx, hops[i], hops[i+1], device, from, 500)
		if err != nil {
			return nil, err
		}
		f := finding{Via: s.rules.Via(hops[i+1]), From: hops[i], To: hops[i+1], Checked: len(ps)}
		for _, p := range ps { // newest first
			mm := mapping.Check(m, p.Pair)
			if len(mm) == 0 {
				continue
			}
			if f.Broken == 0 {
				f.Example, f.Seq, f.Last = mm[0], p.Seq, p.At
			}
			f.Broken++
			f.First = p.At
		}
		if f.Broken > 0 {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Last.After(out[j].Last) })
	return out, nil
}
