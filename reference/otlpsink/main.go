// Command otlpsink accepts OTLP/HTTP trace exports and only counts them, by
// hop and op, so a tap's output can be measured without a store behind it.
//
// Every interval it logs a JSON line; GET /stats returns the totals.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

type totals struct {
	mu      sync.Mutex
	Spans   int64            `json:"spans"`
	Bytes   int64            `json:"bytes"`
	Exports int64            `json:"exports"`
	ByHop   map[string]int64 `json:"by_hop"` // "hop op"
	// From the taps' own health reports, per hop: what they read and what
	// they had to drop.
	Seen   map[string]int64 `json:"tap_seen"`
	Missed map[string]int64 `json:"tap_missed"`
	Delay  map[string]int64 `json:"tap_max_delay_ms"`
}

func main() {
	listen := flag.String("listen", ":4318", "address to accept OTLP/HTTP on")
	every := flag.Duration("every", 10*time.Second, "how often to log the counts")
	flag.Parse()
	t := &totals{ByHop: map[string]int64{}, Seen: map[string]int64{}, Missed: map[string]int64{}, Delay: map[string]int64{}}
	http.HandleFunc("POST /v1/traces", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req coltrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		t.mu.Lock()
		t.Exports++
		t.Bytes += int64(len(body))
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, sp := range ss.Spans {
					h, op := "", ""
					var seen, missed, delay int64
					for _, a := range sp.Attributes {
						switch a.Key {
						case "hopwake.hop", "hopwake.health.hop":
							if h == "" {
								h = a.GetValue().GetStringValue()
							}
						case "hopwake.op":
							op = a.GetValue().GetStringValue()
						case "hopwake.tap.seen":
							seen = a.GetValue().GetIntValue()
						case "hopwake.tap.missed":
							missed = a.GetValue().GetIntValue()
						case "hopwake.tap.delay_ms":
							delay = a.GetValue().GetIntValue()
						case "hopwake.schema":
							if op == "" && a.GetValue().GetStringValue() != "hop/v1" {
								op = a.GetValue().GetStringValue()
							}
						}
					}
					t.Spans++
					t.ByHop[h+" "+op]++
					if op == "tap/v1" {
						t.Seen[h] += seen
						t.Missed[h] += missed
						t.Delay[h] = max(t.Delay[h], delay)
					}
				}
			}
		}
		t.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
	})
	http.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		t.mu.Lock()
		defer t.mu.Unlock()
		// ?reset_delay=1 starts a new maximum, for measuring one stretch.
		defer func() {
			if r.URL.Query().Get("reset_delay") == "1" {
				t.Delay = map[string]int64{}
			}
		}()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(t)
	})
	go func() {
		for range time.Tick(*every) {
			t.mu.Lock()
			b, _ := json.Marshal(t)
			t.mu.Unlock()
			log.Printf("%s", b)
		}
	}()
	log.Printf("counting OTLP spans on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, nil))
}
