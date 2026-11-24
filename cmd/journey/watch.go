package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// Watch mode captures the full payloads of one device at every hop, for a
// bounded time. It is off by default, and every change is logged.
const (
	defaultWatch = time.Hour
	longestWatch = 24 * time.Hour
)

type watchState struct {
	Device string    `json:"device"`
	Until  time.Time `json:"until"`
	By     string    `json:"by"`
	Note   string    `json:"note,omitempty"`
	Since  time.Time `json:"since"`
}

// activeWatches returns the devices being watched now.
func (s *server) activeWatches(ctx context.Context, device string) ([]watchState, error) {
	q := `SELECT device, argMax(until, at), argMax(by, at), argMax(note, at), max(at) FROM watches
		WHERE tenant = ?`
	args := []any{s.rules.Tenant}
	if device != "" {
		q += ` AND device = ?`
		args = append(args, device)
	}
	q += ` GROUP BY device HAVING argMax(until, at) > now64(3) ORDER BY device`
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []watchState{}
	for rows.Next() {
		var w watchState
		if err := rows.Scan(&w.Device, &w.Until, &w.By, &w.Note, &w.Since); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// watches lists the devices being watched now; taps read it.
func (s *server) watches(w http.ResponseWriter, r *http.Request) {
	list, err := s.activeWatches(r.Context(), "")
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, list)
}

// watch starts or extends watching a device: {"minutes": 60, "note": "..."}.
func (s *server) watch(w http.ResponseWriter, r *http.Request) {
	device := r.PathValue("device")
	if !validID.MatchString(device) {
		http.Error(w, "bad device id", http.StatusBadRequest)
		return
	}
	var body struct {
		Minutes int    `json:"minutes"`
		Note    string `json:"note"`
	}
	if r.ContentLength > 0 {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
	}
	d := defaultWatch
	if body.Minutes > 0 {
		d = time.Duration(body.Minutes) * time.Minute
	}
	if d > longestWatch {
		http.Error(w, "watch at most 24 hours", http.StatusBadRequest)
		return
	}
	s.recordWatch(w, r, device, time.Now().Add(d), body.Note)
}

// unwatch stops watching a device now.
func (s *server) unwatch(w http.ResponseWriter, r *http.Request) {
	device := r.PathValue("device")
	if !validID.MatchString(device) {
		http.Error(w, "bad device id", http.StatusBadRequest)
		return
	}
	s.recordWatch(w, r, device, time.Now(), "stopped")
}

func (s *server) recordWatch(w http.ResponseWriter, r *http.Request, device string, until time.Time, note string) {
	by := r.Header.Get("X-Hopwake-User")
	if by == "" {
		by = r.RemoteAddr
	}
	if len(note) > 200 {
		note = note[:200]
	}
	now := time.Now()
	if err := s.db.Exec(r.Context(), `INSERT INTO watches (tenant, device, until, by, note, at) VALUES (?, ?, ?, ?, ?, ?)`,
		s.rules.Tenant, device, until, by, note, now); err != nil {
		httpError(w, err)
		return
	}
	log.Printf("watch: %s until %s by %s (%s)", device, until.Format(time.RFC3339), by, note)
	writeJSON(w, watchState{Device: device, Until: until, By: by, Note: note, Since: now})
}
