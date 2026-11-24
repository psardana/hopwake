package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/psardana/hopwake/internal/journey"
)

func TestMarkMissing(t *testing.T) {
	t0 := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	w := func(key string, minute int, devices ...string) windowRecord {
		s := t0.Add(time.Duration(minute) * time.Minute)
		return windowRecord{Key: key, Start: s, End: s.Add(time.Minute), Devices: devices}
	}
	all := []windowRecord{
		w("site-01", 0, "a", "b"),
		w("site-01", 1, "a", "b"),
		w("site-01", 2, "a"),
		w("site-02", 0, "c"),
		w("site-02", 40, "d"), // c last fed more than the lookback ago
	}
	digest := windowRecord{Key: "site-03", Start: t0, End: t0.Add(time.Minute), Digest: "abc"}
	all = append(all, digest)

	got := markMissing(all, t0.Add(2*time.Minute))
	byKey := map[string][]string{}
	for _, r := range got {
		byKey[r.Key+r.Start.Format("15:04")] = r.Missing
	}
	if m := byKey["site-0112:02"]; len(m) != 1 || m[0] != "b" {
		t.Errorf("site-01 12:02 missing %v, want [b]", m)
	}
	if _, ok := byKey["site-0112:00"]; ok {
		t.Error("a window that ended before from was returned")
	}
	if m := byKey["site-0212:40"]; len(m) != 0 {
		t.Errorf("site-02 12:40 missing %v: c last fed outside the lookback", m)
	}
}

func testServer(t *testing.T) *server {
	t.Helper()
	rules, err := journey.LoadConfig("../../reference/journey-rules.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return &server{rules: rules, grace: 15 * time.Second, span: time.Minute}
}

// Requests that are wrong are turned away before anything reaches the store.
func TestBadRequests(t *testing.T) {
	s := testServer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/devices/{device}", s.device)
	mux.HandleFunc("POST /api/devices/{device}/watch", s.watch)
	mux.HandleFunc("GET /api/journeys/{id}", s.journey)
	mux.HandleFunc("GET /api/windows/{key}/{start}", s.windowDetail)
	for _, c := range []struct{ method, path, body string }{
		{"GET", "/api/devices/bad%20id", ""},
		{"GET", "/api/devices/dev-1?window=999h", ""},
		{"POST", "/api/devices/dev-1/watch", `{"minutes": 100000}`},
		{"POST", "/api/devices/dev-1/watch", `not json`},
		{"GET", "/api/journeys/xyz", ""},
		{"GET", "/api/windows/site-01/yesterday", ""},
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, strings.NewReader(c.body)))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s %s: status %d, want 400", c.method, c.path, rec.Code)
		}
	}
}

func TestRouteHelpers(t *testing.T) {
	s := testServer(t)
	if got := s.serviceAfter("mqtt:plant/+/+/telemetry"); got != "bridge" {
		t.Errorf("service after MQTT: %q", got)
	}
	if got := s.serviceAfter("kafka:telemetry.normalized"); got != "recorder" {
		t.Errorf("service after normalized: %q", got)
	}
	if o := s.optional(); len(o) != 1 || o[0] != "hook:api.served" {
		t.Errorf("optional %v", o)
	}
	seen := map[string]time.Time{"mqtt:plant/+/+/telemetry": time.Now(), "kafka:telemetry.dlq": time.Now()}
	if !s.rejectedAfter("kafka:telemetry.raw", seen) {
		t.Error("dead letters after raw not noticed")
	}
	if s.rejectedAfter("mqtt:plant/+/+/telemetry", seen) {
		t.Error("dead letters belong to the normaliser, not the bridge")
	}
}
