// Command journey serves device journeys from the hop events in ClickHouse:
// what each recent reading of a device reached, and a verdict on where its
// data stops.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/psardana/hopwake/internal/fleet"
	"github.com/psardana/hopwake/internal/journey"
	"github.com/psardana/hopwake/internal/shape"
	"github.com/psardana/hopwake/internal/verdict"
)

// The device-ordered store: one row per hop event, filled from the raw spans
// the collector writes, ordered so that one device's history is one range.
var schema = []string{
	`CREATE TABLE IF NOT EXISTS hop_events (
		tenant LowCardinality(String),
		device String,
		ts DateTime64(9),
		journey_id String,
		seq UInt64,
		hop LowCardinality(String),
		op LowCardinality(String),
		correlation LowCardinality(String),
		bytes UInt32,
		schema_fp LowCardinality(String)
	) ENGINE = MergeTree
	ORDER BY (tenant, device, ts)
	TTL toDateTime(ts) + INTERVAL 3 DAY`,
	// Stores created by hop/v0 lack the shape column.
	`ALTER TABLE hop_events ADD COLUMN IF NOT EXISTS schema_fp LowCardinality(String)`,
	// The view is replaced on every start so that it always matches this code.
	`DROP VIEW IF EXISTS hop_events_mv`,
	`CREATE MATERIALIZED VIEW hop_events_mv TO hop_events AS
	SELECT
		SpanAttributes['hopwake.tenant'] AS tenant,
		SpanAttributes['hopwake.device.id'] AS device,
		Timestamp AS ts,
		TraceId AS journey_id,
		toUInt64OrZero(SpanAttributes['hopwake.seq']) AS seq,
		SpanAttributes['hopwake.hop'] AS hop,
		SpanAttributes['hopwake.op'] AS op,
		SpanAttributes['hopwake.correlation'] AS correlation,
		toUInt32OrZero(SpanAttributes['hopwake.payload.bytes']) AS bytes,
		SpanAttributes['hopwake.schema.fp'] AS schema_fp
	FROM otel_traces
	WHERE SpanAttributes['hopwake.schema'] IN ('hop/v0', 'hop/v1')`,
	// One row per shape per hop and tap start: the fields arrive with the first
	// event of a shape. Read with min(first_seen) per shape.
	`CREATE TABLE IF NOT EXISTS shapes (
		tenant LowCardinality(String),
		hop LowCardinality(String),
		fingerprint String,
		fields String,
		first_seen DateTime64(9)
	) ENGINE = MergeTree
	ORDER BY (tenant, hop, fingerprint)`,
	`DROP VIEW IF EXISTS shapes_mv`,
	`CREATE MATERIALIZED VIEW shapes_mv TO shapes AS
	SELECT
		SpanAttributes['hopwake.tenant'] AS tenant,
		SpanAttributes['hopwake.hop'] AS hop,
		SpanAttributes['hopwake.schema.fp'] AS fingerprint,
		SpanAttributes['hopwake.shape.fields'] AS fields,
		Timestamp AS first_seen
	FROM otel_traces
	WHERE SpanAttributes['hopwake.shape.fields'] != ''`,
}

//go:embed web
var webFiles embed.FS

var validID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type server struct {
	db       driver.Conn
	rules    *journey.Config
	grace    time.Duration
	expected []string      // devices that should report; empty when no fleet file is given
	span     time.Duration // the longest a reading may take across all hops
}

func main() {
	rulesPath := flag.String("rules", env("JOURNEY_RULES", "reference/journey-rules.yaml"), "journey ID rules with the expected route")
	addr := flag.String("clickhouse", env("JOURNEY_CLICKHOUSE", "localhost:9000"), "ClickHouse native address")
	listen := flag.String("listen", env("JOURNEY_LISTEN", ":8090"), "HTTP address")
	grace := flag.Duration("grace", 15*time.Second, "how long a reading may take before it is judged")
	span := flag.Duration("journey-span", 60*time.Second, "events with the same journey ID further apart than this are different readings (a device reboot restarts its sequence)")
	fleetPath := flag.String("fleet", env("JOURNEY_FLEET", ""), "file listing the devices that should report, one per line (optional)")
	flag.Parse()

	rules, err := journey.LoadConfig(*rulesPath)
	if err != nil {
		log.Fatal(err)
	}
	if len(rules.Route) < 2 {
		log.Fatal("the rules file needs a route of at least two hops")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*addr},
		Auth: clickhouse.Auth{
			Database: env("JOURNEY_DB", "hopwake"),
			Username: env("JOURNEY_USER", "hopwake"),
			Password: env("JOURNEY_PASSWORD", "hopwake"),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := ensureSchema(ctx, db); err != nil {
		log.Fatal(err)
	}

	var expected []string
	if *fleetPath != "" {
		if expected, err = fleet.Load(*fleetPath); err != nil {
			log.Fatal(err)
		}
		log.Printf("expecting %d devices from %s", len(expected), *fleetPath)
	}
	s := &server{db: db, rules: rules, grace: *grace, expected: expected, span: *span}
	mux := http.NewServeMux()
	web, _ := fs.Sub(webFiles, "web")
	static := http.FileServerFS(web)
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		// Embedded files carry no modification time: make browsers ask again.
		w.Header().Set("Cache-Control", "no-cache")
		static.ServeHTTP(w, r)
	})
	mux.HandleFunc("GET /api/fleet", s.fleet)
	mux.HandleFunc("GET /api/devices/{device}", s.device)
	mux.HandleFunc("GET /api/journeys/{id}", s.journey)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, map[string]bool{"ok": true})
	})
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	log.Printf("journey service on %s, route %s", *listen, strings.Join(rules.Hops(), " → "))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// ensureSchema waits for the collector's span table, then adds the
// device-ordered table and the view that fills it.
func ensureSchema(ctx context.Context, db driver.Conn) error {
	for i := 0; ; i++ {
		var n uint64
		err := db.QueryRow(ctx, `SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'otel_traces'`).Scan(&n)
		if err == nil && n == 1 {
			break
		}
		if i%10 == 0 {
			log.Printf("waiting for otel_traces (the collector creates it): %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	for _, q := range schema {
		if err := db.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

type hopStat struct {
	Hop      string    `json:"hop"`
	Count    uint64    `json:"count"`
	LastSeen time.Time `json:"last_seen"`
}

func (s *server) device(w http.ResponseWriter, r *http.Request) {
	device := r.PathValue("device")
	if !validID.MatchString(device) {
		http.Error(w, "bad device id", http.StatusBadRequest)
		return
	}
	window := 15 * time.Minute
	if q := r.URL.Query().Get("window"); q != "" {
		d, err := time.ParseDuration(q)
		if err != nil || d <= 0 || d > 72*time.Hour {
			http.Error(w, "bad window", http.StatusBadRequest)
			return
		}
		window = d
	}
	ctx := r.Context()
	tenant := s.rules.Tenant
	now := time.Now()
	from := now.Add(-window)

	rows, err := s.db.Query(ctx, `
		SELECT journey_id, seq, hop, ts
		FROM hop_events
		WHERE tenant = ? AND device = ? AND ts >= ?
		ORDER BY ts DESC
		LIMIT 20000`, tenant, device, from)
	if err != nil {
		httpError(w, err)
		return
	}
	var events []verdict.Event
	for rows.Next() {
		var e verdict.Event
		if err := rows.Scan(&e.JourneyID, &e.Seq, &e.Hop, &e.At); err != nil {
			httpError(w, err)
			return
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		httpError(w, err)
		return
	}
	js := verdict.Assemble(events, s.span)
	if len(js) > 500 {
		js = js[:500]
	}

	var lastSeen *verdict.HopTime
	if len(js) == 0 {
		var hopName string
		var at time.Time
		err := s.db.QueryRow(ctx, `SELECT argMax(hop, ts), max(ts) FROM hop_events WHERE tenant = ? AND device = ?`, tenant, device).Scan(&hopName, &at)
		if err == nil && hopName != "" {
			lastSeen = &verdict.HopTime{Hop: hopName, At: at}
		}
	}

	stats := []hopStat{}
	srows, err := s.db.Query(ctx, `
		SELECT hop, count(), max(ts) FROM hop_events
		WHERE tenant = ? AND device = ? AND ts >= ?
		GROUP BY hop`, tenant, device, from)
	if err != nil {
		httpError(w, err)
		return
	}
	for srows.Next() {
		var h hopStat
		if err := srows.Scan(&h.Hop, &h.Count, &h.LastSeen); err != nil {
			httpError(w, err)
			return
		}
		stats = append(stats, h)
	}

	shapes, err := s.shapeHistory(ctx, tenant, device, from)
	if err != nil {
		httpError(w, err)
		return
	}

	v := verdict.Decide(device, s.rules.Hops(), js, now, s.grace, lastSeen)
	v.At(device, s.rules.Via(v.NotReached))
	if len(js) > 50 {
		js = js[:50]
	}
	writeJSON(w, map[string]any{
		"tenant":   tenant,
		"device":   device,
		"window":   window.String(),
		"route":    s.rules.Hops(),
		"via":      s.via(),
		"verdict":  v,
		"hops":     stats,
		"shapes":   shapes,
		"journeys": js,
	})
}

type shapeSeen struct {
	Fingerprint string    `json:"fingerprint"`
	Fields      []string  `json:"fields,omitempty"`
	Count       uint64    `json:"count"`
	First       time.Time `json:"first"`
	Last        time.Time `json:"last"`
}

type hopShapes struct {
	Hop      string        `json:"hop"`
	Current  shapeSeen     `json:"current"`
	Previous *shapeSeen    `json:"previous,omitempty"`
	Change   *shape.Change `json:"change,omitempty"`
}

// shapeHistory reports, per hop, the device's current payload shape and, when
// it changed in the window, the one before and what moved between them.
func (s *server) shapeHistory(ctx context.Context, tenant, device string, from time.Time) ([]hopShapes, error) {
	rows, err := s.db.Query(ctx, `
		SELECT hop, schema_fp, count(), min(ts), max(ts) FROM hop_events
		WHERE tenant = ? AND device = ? AND ts >= ? AND schema_fp != ''
		GROUP BY hop, schema_fp
		ORDER BY hop, max(ts)`, tenant, device, from)
	if err != nil {
		return nil, err
	}
	byHop := map[string][]shapeSeen{}
	var fps []string
	for rows.Next() {
		var h string
		var x shapeSeen
		if err := rows.Scan(&h, &x.Fingerprint, &x.Count, &x.First, &x.Last); err != nil {
			return nil, err
		}
		byHop[h] = append(byHop[h], x)
		fps = append(fps, x.Fingerprint)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	fields := map[string]string{}
	if len(fps) > 0 {
		frows, err := s.db.Query(ctx, `SELECT fingerprint, any(fields) FROM shapes WHERE tenant = ? AND fingerprint IN ? GROUP BY fingerprint`, tenant, fps)
		if err != nil {
			return nil, err
		}
		for frows.Next() {
			var fp, f string
			if err := frows.Scan(&fp, &f); err != nil {
				return nil, err
			}
			fields[fp] = f
		}
	}
	withFields := func(x shapeSeen) shapeSeen {
		if f, ok := fields[x.Fingerprint]; ok {
			x.Fields = strings.Fields(f)
		}
		return x
	}
	out := []hopShapes{}
	for _, h := range s.rules.Hops() {
		seen := byHop[h]
		if len(seen) == 0 {
			continue
		}
		// Rows are ordered by when each shape was last seen: the last one is current.
		hs := hopShapes{Hop: h, Current: withFields(seen[len(seen)-1])}
		if len(seen) > 1 {
			prev := withFields(seen[len(seen)-2])
			hs.Previous = &prev
			if fields[prev.Fingerprint] != "" && fields[hs.Current.Fingerprint] != "" {
				c := shape.Diff(shape.Parse(fields[prev.Fingerprint]), shape.Parse(fields[hs.Current.Fingerprint]))
				hs.Change = &c
			}
		}
		out = append(out, hs)
	}
	return out, nil
}

type fleetDevice struct {
	Device       string               `json:"device"`
	State        verdict.State        `json:"state"`
	StoppedAfter string               `json:"stopped_after,omitempty"`
	StoppedAt    string               `json:"stopped_at,omitempty"`
	LastSeen     map[string]time.Time `json:"last_seen"`
	LastEver     *time.Time           `json:"last_ever,omitempty"` // for expected devices not seen in the window
	Expected     bool                 `json:"expected"`
	ShapeChanged bool                 `json:"shape_changed"` // more than one payload shape at a hop in the window
}

// fleet judges every device seen in the window from the last time each hop
// saw it. Problems come first.
func (s *server) fleet(w http.ResponseWriter, r *http.Request) {
	window := 15 * time.Minute
	if q := r.URL.Query().Get("window"); q != "" {
		d, err := time.ParseDuration(q)
		if err != nil || d <= 0 || d > 72*time.Hour {
			http.Error(w, "bad window", http.StatusBadRequest)
			return
		}
		window = d
	}
	now := time.Now()
	rows, err := s.db.Query(r.Context(), `
		SELECT device, hop, max(ts) FROM hop_events
		WHERE tenant = ? AND ts >= ?
		GROUP BY device, hop`, s.rules.Tenant, now.Add(-window))
	if err != nil {
		httpError(w, err)
		return
	}
	last := map[string]map[string]time.Time{}
	for rows.Next() {
		var device, hopName string
		var at time.Time
		if err := rows.Scan(&device, &hopName, &at); err != nil {
			httpError(w, err)
			return
		}
		if last[device] == nil {
			last[device] = map[string]time.Time{}
		}
		last[device][hopName] = at
	}
	changed := map[string]bool{}
	crows, err := s.db.Query(r.Context(), `
		SELECT DISTINCT device FROM (
			SELECT device, hop FROM hop_events
			WHERE tenant = ? AND ts >= ? AND schema_fp != ''
			GROUP BY device, hop
			HAVING uniqExact(schema_fp) > 1)`, s.rules.Tenant, now.Add(-window))
	if err != nil {
		httpError(w, err)
		return
	}
	for crows.Next() {
		var d string
		if err := crows.Scan(&d); err != nil {
			httpError(w, err)
			return
		}
		changed[d] = true
	}

	order := map[verdict.State]int{verdict.Stopped: 0, verdict.Silent: 1, verdict.Unknown: 2, verdict.OK: 3}
	expected := map[string]bool{}
	for _, d := range s.expected {
		expected[d] = true
	}
	out := make([]fleetDevice, 0, len(last)+len(s.expected))
	for device, seen := range last {
		state, after := verdict.Quick(s.rules.Hops(), seen, now, s.grace, s.grace+45*time.Second)
		out = append(out, fleetDevice{Device: device, State: state, StoppedAfter: after, StoppedAt: s.serviceAfter(after), LastSeen: seen, Expected: expected[device], ShapeChanged: changed[device]})
	}

	// Expected devices with nothing in the window: silent if ever seen, else never seen.
	var missing []string
	for _, d := range s.expected {
		if _, ok := last[d]; !ok {
			missing = append(missing, d)
		}
	}
	if len(missing) > 0 {
		ever := map[string]time.Time{}
		mrows, err := s.db.Query(r.Context(), `
			SELECT device, max(ts) FROM hop_events
			WHERE tenant = ? AND device IN ? GROUP BY device`, s.rules.Tenant, missing)
		if err != nil {
			httpError(w, err)
			return
		}
		for mrows.Next() {
			var d string
			var at time.Time
			if err := mrows.Scan(&d, &at); err != nil {
				httpError(w, err)
				return
			}
			ever[d] = at
		}
		for _, d := range missing {
			fd := fleetDevice{Device: d, State: verdict.Unknown, LastSeen: map[string]time.Time{}, Expected: true}
			if at, ok := ever[d]; ok {
				fd.State, fd.LastEver = verdict.Silent, &at
			}
			out = append(out, fd)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if order[out[i].State] != order[out[j].State] {
			return order[out[i].State] < order[out[j].State]
		}
		return out[i].Device < out[j].Device
	})
	writeJSON(w, map[string]any{"tenant": s.rules.Tenant, "route": s.rules.Hops(), "via": s.via(), "window": window.String(), "expected": len(s.expected), "devices": out})
}

func (s *server) journey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" {
		http.Error(w, "bad journey id", http.StatusBadRequest)
		return
	}
	rows, err := s.db.Query(r.Context(), `
		SELECT device, seq, hop, op, ts, bytes FROM hop_events
		WHERE journey_id = ? ORDER BY ts`, id)
	if err != nil {
		httpError(w, err)
		return
	}
	type row struct {
		Device string    `json:"device"`
		Seq    uint64    `json:"seq"`
		Hop    string    `json:"hop"`
		Op     string    `json:"op"`
		At     time.Time `json:"at"`
		Bytes  uint32    `json:"bytes"`
	}
	out := []row{}
	for rows.Next() {
		var x row
		if err := rows.Scan(&x.Device, &x.Seq, &x.Hop, &x.Op, &x.At, &x.Bytes); err != nil {
			httpError(w, err)
			return
		}
		out = append(out, x)
	}
	writeJSON(w, map[string]any{"journey_id": id, "route": s.rules.Hops(), "via": s.via(), "hops": out})
}

// via maps each hop to the service that carries readings to it, where the
// route names one.
func (s *server) via() map[string]string {
	out := map[string]string{}
	for _, st := range s.rules.Route {
		if st.Via != "" {
			out[st.Hop] = st.Via
		}
	}
	return out
}

// serviceAfter is the service between hop and the next hop of the route.
func (s *server) serviceAfter(hop string) string {
	hops := s.rules.Hops()
	for i := 0; i+1 < len(hops); i++ {
		if hops[i] == hop {
			return s.rules.Via(hops[i+1])
		}
	}
	return ""
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, err error) {
	log.Printf("query: %v", err)
	http.Error(w, "query failed", http.StatusInternalServerError)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
