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

	"github.com/psardana/hopwake/internal/alias"
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
		schema_fp LowCardinality(String),
		reason String,
		message_type LowCardinality(String),
		alias String
	) ENGINE = MergeTree
	PARTITION BY toDate(ts)
	ORDER BY (tenant, device, ts)
	TTL toDateTime(ts) + INTERVAL 30 DAY
	SETTINGS ttl_only_drop_parts = 1`,
	// Stores created by hop/v0 lack the shape column.
	`ALTER TABLE hop_events ADD COLUMN IF NOT EXISTS schema_fp LowCardinality(String)`,
	`ALTER TABLE hop_events ADD COLUMN IF NOT EXISTS reason String`,
	`ALTER TABLE hop_events ADD COLUMN IF NOT EXISTS message_type LowCardinality(String)`,
	`ALTER TABLE hop_events ADD COLUMN IF NOT EXISTS alias String`,
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
		SpanAttributes['hopwake.schema.fp'] AS schema_fp,
		SpanAttributes['hopwake.reason'] AS reason,
		SpanAttributes['hopwake.message.type'] AS message_type,
		SpanAttributes['hopwake.device.alias'] AS alias
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
	// The tap's own health per hop, every few seconds.
	`CREATE TABLE IF NOT EXISTS tap_health (
		tenant LowCardinality(String),
		hop LowCardinality(String),
		ts DateTime64(9),
		tap String,
		connected UInt8,
		seen UInt32,
		missed UInt32,
		delay_ms UInt32,
		every_ms UInt32
	) ENGINE = MergeTree
	PARTITION BY toDate(ts)
	ORDER BY (tenant, hop, ts)
	TTL toDateTime(ts) + INTERVAL 30 DAY
	SETTINGS ttl_only_drop_parts = 1`,
	`DROP VIEW IF EXISTS tap_health_mv`,
	`CREATE MATERIALIZED VIEW tap_health_mv TO tap_health AS
	SELECT
		SpanAttributes['hopwake.tenant'] AS tenant,
		SpanAttributes['hopwake.hop'] AS hop,
		Timestamp AS ts,
		SpanAttributes['hopwake.tap.id'] AS tap,
		SpanAttributes['hopwake.tap.connected'] = 'true' AS connected,
		toUInt32OrZero(SpanAttributes['hopwake.tap.seen']) AS seen,
		toUInt32OrZero(SpanAttributes['hopwake.tap.missed']) AS missed,
		toUInt32OrZero(SpanAttributes['hopwake.tap.delay_ms']) AS delay_ms,
		toUInt32OrZero(SpanAttributes['hopwake.tap.every_ms']) AS every_ms
	FROM otel_traces
	WHERE SpanAttributes['hopwake.schema'] = 'tap/v1'`,
	// Watch mode: who watched which device until when. Every change is a new
	// row, so the table is also the log; the newest row per device counts.
	`CREATE TABLE IF NOT EXISTS watches (
		tenant LowCardinality(String),
		device String,
		until DateTime64(3),
		by String,
		note String,
		at DateTime64(3)
	) ENGINE = MergeTree
	ORDER BY (tenant, device, at)`,
	// Payloads of watched devices, as the tap sent them (already redacted).
	`CREATE TABLE IF NOT EXISTS payload_samples (
		tenant LowCardinality(String),
		device String,
		ts DateTime64(9),
		journey_id String,
		seq UInt64,
		hop LowCardinality(String),
		payload String
	) ENGINE = MergeTree
	ORDER BY (tenant, device, ts)
	TTL toDateTime(ts) + INTERVAL 24 HOUR`,
	`DROP VIEW IF EXISTS payload_samples_mv`,
	`CREATE MATERIALIZED VIEW payload_samples_mv TO payload_samples AS
	SELECT
		SpanAttributes['hopwake.tenant'] AS tenant,
		SpanAttributes['hopwake.device.id'] AS device,
		Timestamp AS ts,
		TraceId AS journey_id,
		toUInt64OrZero(SpanAttributes['hopwake.seq']) AS seq,
		SpanAttributes['hopwake.hop'] AS hop,
		SpanAttributes['hopwake.payload'] AS payload
	FROM otel_traces
	WHERE SpanAttributes['hopwake.payload'] != ''`,
	// Aggregated outputs and how to find the readings behind them.
	`CREATE TABLE IF NOT EXISTS window_records (
		tenant LowCardinality(String),
		hop LowCardinality(String),
		ts DateTime64(9),
		op LowCardinality(String),
		group_key String,
		window_start DateTime64(3),
		window_end DateTime64(3),
		input_hop LowCardinality(String),
		inputs UInt32,
		device_count UInt32,
		devices Array(String),
		device_digest String,
		output String
	) ENGINE = MergeTree
	ORDER BY (tenant, group_key, window_start)
	TTL toDateTime(ts) + INTERVAL 30 DAY`,
	`DROP VIEW IF EXISTS window_records_mv`,
	`CREATE MATERIALIZED VIEW window_records_mv TO window_records AS
	SELECT
		SpanAttributes['hopwake.tenant'] AS tenant,
		SpanAttributes['hopwake.hop'] AS hop,
		Timestamp AS ts,
		SpanAttributes['hopwake.window.op'] AS op,
		SpanAttributes['hopwake.window.key'] AS group_key,
		parseDateTime64BestEffort(SpanAttributes['hopwake.window.start'], 3) AS window_start,
		parseDateTime64BestEffort(SpanAttributes['hopwake.window.end'], 3) AS window_end,
		SpanAttributes['hopwake.window.input_hop'] AS input_hop,
		toUInt32OrZero(SpanAttributes['hopwake.window.inputs']) AS inputs,
		toUInt32OrZero(SpanAttributes['hopwake.window.device_count']) AS device_count,
		arrayFilter(x -> x != '', splitByChar(',', SpanAttributes['hopwake.window.devices'])) AS devices,
		SpanAttributes['hopwake.window.device_digest'] AS device_digest,
		SpanAttributes['hopwake.window.output'] AS output
	FROM otel_traces
	WHERE SpanAttributes['hopwake.schema'] = 'window/v1'`,
	// The last time each hop saw each device, per day: what the fleet view
	// needs over long windows, without reading every hop event of them.
	`CREATE TABLE IF NOT EXISTS hop_last_seen (
		tenant LowCardinality(String),
		day Date,
		device String,
		hop LowCardinality(String),
		message_type LowCardinality(String),
		last SimpleAggregateFunction(max, DateTime64(9)),
		n SimpleAggregateFunction(sum, UInt64)
	) ENGINE = AggregatingMergeTree
	PARTITION BY day
	ORDER BY (tenant, day, device, hop, message_type)
	TTL day + INTERVAL 30 DAY
	SETTINGS ttl_only_drop_parts = 1`,
	`DROP VIEW IF EXISTS hop_last_seen_mv`,
	`CREATE MATERIALIZED VIEW hop_last_seen_mv TO hop_last_seen AS
	SELECT
		SpanAttributes['hopwake.tenant'] AS tenant,
		toDate(Timestamp) AS day,
		SpanAttributes['hopwake.device.id'] AS device,
		SpanAttributes['hopwake.hop'] AS hop,
		SpanAttributes['hopwake.message.type'] AS message_type,
		Timestamp AS last,
		toUInt64(1) AS n
	FROM otel_traces
	WHERE SpanAttributes['hopwake.schema'] IN ('hop/v0', 'hop/v1')`,
	// Every message per device, hop and five minutes, sampled or not.
	`CREATE TABLE IF NOT EXISTS device_counters (
		tenant LowCardinality(String),
		device String,
		hop LowCardinality(String),
		bucket DateTime,
		n SimpleAggregateFunction(sum, UInt64),
		errors SimpleAggregateFunction(sum, UInt64),
		last_seen SimpleAggregateFunction(max, DateTime64(3))
	) ENGINE = AggregatingMergeTree
	ORDER BY (tenant, device, hop, bucket)
	TTL bucket + INTERVAL 30 DAY`,
	`DROP VIEW IF EXISTS device_counters_mv`,
	`CREATE MATERIALIZED VIEW device_counters_mv TO device_counters AS
	SELECT
		SpanAttributes['hopwake.tenant'] AS tenant,
		c.1 AS device,
		SpanAttributes['hopwake.hop'] AS hop,
		toStartOfFiveMinutes(Timestamp) AS bucket,
		toUInt64(c.2) AS n,
		toUInt64(c.3) AS errors,
		fromUnixTimestamp64Milli(toInt64(c.4)) AS last_seen
	FROM otel_traces
	ARRAY JOIN arrayZip(
		splitByChar(',', SpanAttributes['hopwake.counts.devices']),
		arrayMap(x -> toUInt64OrZero(x), splitByChar(',', SpanAttributes['hopwake.counts.n'])),
		arrayMap(x -> toUInt64OrZero(x), splitByChar(',', SpanAttributes['hopwake.counts.errors'])),
		arrayMap(x -> toInt64OrZero(x), splitByChar(',', SpanAttributes['hopwake.counts.last_ms']))) AS c
	WHERE SpanAttributes['hopwake.schema'] = 'counts/v1'`,
}

//go:embed web
var webFiles embed.FS

var validID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type server struct {
	maxWindow time.Duration // the longest window offered: the retention
	db        driver.Conn
	rules     *journey.Config
	grace     time.Duration
	expected  []string      // devices that should report; empty when no fleet file is given
	span      time.Duration // the longest a reading may take across all hops
}

func main() {
	rulesPath := flag.String("rules", env("JOURNEY_RULES", "reference/journey-rules.yaml"), "journey ID rules with the expected route")
	addr := flag.String("clickhouse", env("JOURNEY_CLICKHOUSE", "localhost:9000"), "ClickHouse native address")
	listen := flag.String("listen", env("JOURNEY_LISTEN", ":8090"), "HTTP address")
	grace := flag.Duration("grace", 15*time.Second, "how long a reading may take before it is judged")
	span := flag.Duration("journey-span", 60*time.Second, "events with the same journey ID further apart than this are different readings (a device reboot restarts its sequence)")
	fleetPath := flag.String("fleet", env("JOURNEY_FLEET", ""), "file listing the devices that should report, one per line (optional)")
	keepFlag := flag.String("retention", env("HOPWAKE_RETENTION", "30d"), "how long hop events, counters and tap health are kept, and the longest window the view offers: 30d, 72h, …")
	rawFlag := flag.String("raw-retention", env("HOPWAKE_RAW_RETENTION", "3d"), "how long the raw spans are kept; the view never reads them, so this can be short")
	payloadFlag := flag.String("payload-retention", env("HOPWAKE_PAYLOAD_RETENTION", "24h"), "how long payloads captured in watch mode are kept")
	flag.Parse()
	keep, err := parseRetention(*keepFlag)
	if err != nil {
		log.Fatalf("retention: %v", err)
	}
	raw, err := parseRetention(*rawFlag)
	if err != nil {
		log.Fatalf("raw-retention: %v", err)
	}
	payloads, err := parseRetention(*payloadFlag)
	if err != nil {
		log.Fatalf("payload-retention: %v", err)
	}

	rules, err := journey.LoadConfig(*rulesPath)
	if err != nil {
		log.Fatal(err)
	}
	ret := retention{keep: keep, raw: raw, payloads: payloads}
	for _, rt := range rules.AllRoutes() {
		if len(rt.Steps) < 2 {
			log.Fatalf("route %s needs at least two hops", rt.Name)
		}
	}
	if len(rules.AllRoutes()) == 0 {
		log.Fatal("the rules file needs a route: route, or routes by message type")
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
	if err := ensureSchema(ctx, db, ret); err != nil {
		log.Fatal(err)
	}
	log.Printf("keeping hop events %s, raw spans %s, watched payloads %s", ret.keep, ret.raw, ret.payloads)

	var expected []string
	if *fleetPath != "" {
		if expected, err = fleet.Load(*fleetPath); err != nil {
			log.Fatal(err)
		}
		log.Printf("expecting %d devices from %s", len(expected), *fleetPath)
	}
	s := &server{db: db, rules: rules, grace: *grace, expected: expected, span: *span, maxWindow: ret.keep}
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
	mux.HandleFunc("GET /api/watches", s.watches)
	mux.HandleFunc("GET /api/aliases", s.aliases)
	mux.HandleFunc("GET /api/mappings", s.mappings)
	mux.HandleFunc("GET /api/windows", s.windows)
	mux.HandleFunc("GET /api/windows/{key}/{start}", s.windowDetail)
	mux.HandleFunc("POST /api/devices/{device}/watch", s.watch)
	mux.HandleFunc("DELETE /api/devices/{device}/watch", s.unwatch)
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
func ensureSchema(ctx context.Context, db driver.Conn, ret retention) error {
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
	for _, q := range append(append([]string{}, schema...), ret.statements()...) {
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
	// Readings that broke the hop's declared schema, and the latest reason.
	Invalid       uint64 `json:"invalid"`
	InvalidReason string `json:"invalid_reason,omitempty"`
	// From counters, which cover every message whether sampled or not, in
	// five-minute buckets from the start of the window.
	Counted       uint64 `json:"counted"`
	CountedErrors uint64 `json:"counted_errors"`
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
		if err != nil || d <= 0 || d > s.maxWindow {
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
		SELECT journey_id, seq, hop, ts, op, reason, message_type
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
		if err := rows.Scan(&e.JourneyID, &e.Seq, &e.Hop, &e.At, &e.Op, &e.Reason, &e.Type); err != nil {
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
		SELECT hop, count(), max(ts), countIf(op = 'error'), argMaxIf(reason, ts, op = 'error') FROM hop_events
		WHERE tenant = ? AND device = ? AND ts >= ?
		GROUP BY hop`, tenant, device, from)
	if err != nil {
		httpError(w, err)
		return
	}
	for srows.Next() {
		var h hopStat
		if err := srows.Scan(&h.Hop, &h.Count, &h.LastSeen, &h.Invalid, &h.InvalidReason); err != nil {
			httpError(w, err)
			return
		}
		stats = append(stats, h)
	}

	// Counters cover every message, so they show how much the hop events
	// above were sampled.
	crows, err := s.db.Query(ctx, `
		SELECT hop, sum(n), sum(errors) FROM device_counters
		WHERE tenant = ? AND device = ? AND bucket >= toStartOfFiveMinutes(?)
		GROUP BY hop`, tenant, device, from)
	if err != nil {
		httpError(w, err)
		return
	}
	counted := map[string][2]uint64{}
	for crows.Next() {
		var h string
		var n, e uint64
		if err := crows.Scan(&h, &n, &e); err != nil {
			httpError(w, err)
			return
		}
		counted[h] = [2]uint64{n, e}
	}
	for i := range stats {
		stats[i].Counted, stats[i].CountedErrors = counted[stats[i].Hop][0], counted[stats[i].Hop][1]
	}

	shapes, err := s.shapeHistory(ctx, tenant, device, from)
	if err != nil {
		httpError(w, err)
		return
	}

	fed, missed, err := s.deviceWindows(ctx, device, from)
	if err != nil {
		httpError(w, err)
		return
	}
	found, err := s.findings(ctx, device, from)
	if err != nil {
		httpError(w, err)
		return
	}
	var watching *watchState
	if ws, err := s.activeWatches(ctx, device); err != nil {
		httpError(w, err)
		return
	} else if len(ws) > 0 {
		watching = &ws[0]
	}

	blind, err := s.blindness(ctx, from, now)
	if err != nil {
		httpError(w, err)
		return
	}
	// One verdict per route: a reading is judged on the routes its message
	// type follows. The page shows one route at a time, by default the first
	// with a problem.
	type routeView struct {
		Name     string          `json:"name"`
		Types    []string        `json:"types,omitempty"`
		Readings int             `json:"readings"`
		Verdict  verdict.Verdict `json:"verdict"`
	}
	var views []routeView
	subsets := map[string][]verdict.Journey{}
	routes := s.rules.AllRoutes()
	for _, rt := range routes {
		var sub []verdict.Journey
		for _, j := range js {
			for _, r := range s.rules.RoutesFor(j.Type) {
				if r.Name == rt.Name {
					sub = append(sub, j)
					break
				}
			}
		}
		subsets[rt.Name] = sub
		views = append(views, routeView{Name: rt.Name, Types: rt.Types, Readings: len(sub), Verdict: s.judge(device, rt, sub, now, lastSeen, blind)})
	}
	sel := -1
	if q := r.URL.Query().Get("route"); q != "" {
		for i, v := range views {
			if v.Name == q {
				sel = i
			}
		}
	}
	if sel < 0 {
		for i, v := range views {
			if v.Readings > 0 && v.Verdict.State != verdict.OK && sel < 0 {
				sel = i
			}
		}
	}
	if sel < 0 {
		for i, v := range views {
			if sel < 0 || v.Readings > views[sel].Readings {
				sel = i
			}
		}
	}
	shown := []routeView{}
	for _, v := range views {
		if v.Readings > 0 || len(views) == 1 {
			shown = append(shown, v)
		}
	}
	rt := routes[sel]
	v := views[sel].Verdict
	js = subsets[rt.Name]
	if len(js) > 50 {
		js = js[:50]
	}
	writeJSON(w, map[string]any{
		"tenant":     tenant,
		"device":     device,
		"window":     window.String(),
		"route":      rt.Hops(),
		"route_name": rt.Name,
		"routes":     shown,
		"optional":   rt.Optional(),
		"via":        viaOf(rt),
		"tap":        blindList(blind),
		"watch":      watching,
		"findings":   found,
		"aggregates": map[string]any{"fed": fed, "missing": missed},
		"verdict":    v,
		"hops":       stats,
		"shapes":     shapes,
		"journeys":   js,
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
		SELECT hop, message_type, schema_fp, count(), min(ts), max(ts) FROM hop_events
		WHERE tenant = ? AND device = ? AND ts >= ? AND schema_fp != ''
		GROUP BY hop, message_type, schema_fp
		ORDER BY hop, message_type, max(ts)`, tenant, device, from)
	if err != nil {
		return nil, err
	}
	// Each message type has its own shape: only a change within one type is
	// a change. Per hop, show the type that changed, else the newest.
	byType := map[string]map[string][]shapeSeen{}
	var fps []string
	for rows.Next() {
		var h, t string
		var x shapeSeen
		if err := rows.Scan(&h, &t, &x.Fingerprint, &x.Count, &x.First, &x.Last); err != nil {
			return nil, err
		}
		if byType[h] == nil {
			byType[h] = map[string][]shapeSeen{}
		}
		byType[h][t] = append(byType[h][t], x)
		fps = append(fps, x.Fingerprint)
	}
	byHop := map[string][]shapeSeen{}
	for h, types := range byType {
		for _, seen := range types {
			cur, best := seen[len(seen)-1], byHop[h]
			switch {
			case best == nil,
				len(seen) > 1 && len(best) == 1,
				(len(seen) > 1) == (len(best) > 1) && cur.Last.After(best[len(best)-1].Last):
				byHop[h] = seen
			}
		}
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
	for _, h := range s.allHops() {
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
	Route        string               `json:"route,omitempty"`    // the route the state is about
	Rejected     bool                 `json:"rejected,omitempty"` // the service after StoppedAfter is rejecting its readings
	LastSeen     map[string]time.Time `json:"last_seen"`
	LastEver     *time.Time           `json:"last_ever,omitempty"` // for expected devices not seen in the window
	Expected     bool                 `json:"expected"`
	ShapeChanged bool                 `json:"shape_changed"`          // more than one payload shape at a hop in the window
	Invalid      uint64               `json:"invalid,omitempty"`      // hop events that broke a declared schema in the window
	WrongValues  int                  `json:"wrong_values,omitempty"` // watched readings that broke a confirmed mapping
	Watched      bool                 `json:"watched,omitempty"`
}

// shapeWindow bounds the fleet's search for shape changes: over a long
// window it would read every hop event, and a change older than a day is
// no longer news on the fleet page (a device's own page still shows it).
const shapeWindow = 24 * time.Hour

// fleet judges every device seen in the window from the last time each hop
// saw it. Problems come first.
func (s *server) fleet(w http.ResponseWriter, r *http.Request) {
	window := 15 * time.Minute
	if q := r.URL.Query().Get("window"); q != "" {
		d, err := time.ParseDuration(q)
		if err != nil || d <= 0 || d > s.maxWindow {
			http.Error(w, "bad window", http.StatusBadRequest)
			return
		}
		window = d
	}
	now := time.Now()
	// Last sightings from hop events, which may be sampled, and from
	// counters, which cover all traffic but arrive every few seconds.
	// Counters carry no message type, so they count only for the untyped route.
	rows, err := s.db.Query(r.Context(), `
		SELECT device, hop, message_type, max(t) FROM (
			SELECT device, hop, message_type, max(last) AS t FROM hop_last_seen
			WHERE tenant = ? AND day >= toDate(?)
			GROUP BY device, hop, message_type
			HAVING t >= ?
			UNION ALL
			SELECT device, hop, ? AS message_type, toDateTime64(max(last_seen), 9) AS t FROM device_counters
			WHERE tenant = ? AND bucket >= toStartOfFiveMinutes(?) AND last_seen >= ?
			GROUP BY device, hop)
		GROUP BY device, hop, message_type`, s.rules.Tenant, now.Add(-window), now.Add(-window), counterType, s.rules.Tenant, now.Add(-window), now.Add(-window))
	if err != nil {
		httpError(w, err)
		return
	}
	last := map[string]map[string]time.Time{}             // device → hop → last seen, any type
	typed := map[string]map[string]map[string]time.Time{} // device → type → hop → last seen
	for rows.Next() {
		var device, hopName, msgType string
		var at time.Time
		if err := rows.Scan(&device, &hopName, &msgType, &at); err != nil {
			httpError(w, err)
			return
		}
		if last[device] == nil {
			last[device] = map[string]time.Time{}
			typed[device] = map[string]map[string]time.Time{}
		}
		if at.After(last[device][hopName]) {
			last[device][hopName] = at
		}
		if msgType == counterType {
			// Counters mix every type: they can stand for the untyped route
			// only, never for one typed route.
			if len(s.rules.Route) == 0 {
				continue
			}
			msgType = ""
		}
		if typed[device][msgType] == nil {
			typed[device][msgType] = map[string]time.Time{}
		}
		if at.After(typed[device][msgType][hopName]) {
			typed[device][msgType][hopName] = at
		}
	}
	changed := map[string]bool{}
	crows, err := s.db.Query(r.Context(), `
		SELECT DISTINCT device FROM (
			SELECT device, hop, message_type FROM hop_events
			WHERE tenant = ? AND ts >= ? AND schema_fp != ''
			GROUP BY device, hop, message_type
			HAVING uniqExact(schema_fp) > 1)`, s.rules.Tenant, now.Add(-min(window, shapeWindow)))
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
	invalid := map[string]uint64{}
	irows, err := s.db.Query(r.Context(), `
		SELECT device, count() FROM hop_events
		WHERE tenant = ? AND ts >= ? AND op = 'error'
		GROUP BY device`, s.rules.Tenant, now.Add(-window))
	if err != nil {
		httpError(w, err)
		return
	}
	for irows.Next() {
		var d string
		var n uint64
		if err := irows.Scan(&d, &n); err != nil {
			httpError(w, err)
			return
		}
		invalid[d] = n
	}

	order := map[verdict.State]int{verdict.Stopped: 0, verdict.Silent: 1, verdict.Unsure: 2, verdict.Unknown: 3, verdict.OK: 4}
	expected := map[string]bool{}
	for _, d := range s.expected {
		expected[d] = true
	}
	out := make([]fleetDevice, 0, len(last)+len(s.expected))
	blind, err := s.blindness(r.Context(), now.Add(-window), now)
	if err != nil {
		httpError(w, err)
		return
	}
	// Only watched devices have payloads, so only they can break a mapping.
	wrong, watchedNow := map[string]int{}, map[string]bool{}
	ws, err := s.activeWatches(r.Context(), "")
	if err != nil {
		httpError(w, err)
		return
	}
	for _, wd := range ws {
		watchedNow[wd.Device] = true
		fs, err := s.findings(r.Context(), wd.Device, now.Add(-window))
		if err != nil {
			httpError(w, err)
			return
		}
		for _, f := range fs {
			wrong[wd.Device] += f.Broken
		}
	}
	routes := s.rules.AllRoutes()
	for device, all := range last {
		// Judge each route on the readings whose types follow it, and show
		// the worst.
		best := fleetDevice{Device: device, State: verdict.Unknown}
		for _, rt := range routes {
			seen := map[string]time.Time{}
			for msgType, hops := range typed[device] {
				follows := false
				for _, r := range s.rules.RoutesFor(msgType) {
					if r.Name == rt.Name {
						follows = true
					}
				}
				if !follows {
					continue
				}
				for h, at := range hops {
					if at.After(seen[h]) {
						seen[h] = at
					}
				}
			}
			if len(seen) == 0 {
				continue
			}
			fd := s.quick(device, rt, seen, now, blind)
			if best.State == verdict.Unknown || order[fd.State] < order[best.State] {
				best = fd
			}
		}
		best.LastSeen, best.Expected, best.ShapeChanged, best.Invalid = all, expected[device], changed[device], invalid[device]
		best.WrongValues, best.Watched = wrong[device], watchedNow[device]
		out = append(out, best)
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
	type routeHops struct {
		Name  string   `json:"name"`
		Types []string `json:"types,omitempty"`
		Hops  []string `json:"hops"`
	}
	var rs []routeHops
	for _, rt := range s.rules.AllRoutes() {
		rs = append(rs, routeHops{Name: rt.Name, Types: rt.Types, Hops: rt.Hops()})
	}
	writeJSON(w, map[string]any{"tenant": s.rules.Tenant, "route": s.rules.Hops(), "routes": rs, "optional": s.optional(), "via": s.via(), "tap": blindList(blind), "window": window.String(), "retention_hours": int(s.maxWindow.Hours()), "expected": len(s.expected), "devices": out})
}

func (s *server) journey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" {
		http.Error(w, "bad journey id", http.StatusBadRequest)
		return
	}
	rows, err := s.db.Query(r.Context(), `
		SELECT e.device, e.seq, e.hop, e.op, e.ts, e.bytes, e.reason, p.payload
		FROM hop_events AS e
		LEFT JOIN (SELECT hop, ts, payload FROM payload_samples WHERE journey_id = ?) AS p ON p.hop = e.hop AND p.ts = e.ts
		WHERE e.journey_id = ? ORDER BY e.ts`, id, id)
	if err != nil {
		httpError(w, err)
		return
	}
	type row struct {
		Device  string    `json:"device"`
		Seq     uint64    `json:"seq"`
		Hop     string    `json:"hop"`
		Op      string    `json:"op"`
		At      time.Time `json:"at"`
		Bytes   uint32    `json:"bytes"`
		Reason  string    `json:"reason,omitempty"`
		Payload string    `json:"payload,omitempty"` // watched devices only
	}
	out := []row{}
	for rows.Next() {
		var x row
		if err := rows.Scan(&x.Device, &x.Seq, &x.Hop, &x.Op, &x.At, &x.Bytes, &x.Reason, &x.Payload); err != nil {
			httpError(w, err)
			return
		}
		out = append(out, x)
	}
	writeJSON(w, map[string]any{"journey_id": id, "route": s.rules.Hops(), "via": s.via(), "hops": out})
}

// aliases lists the other names devices go by, as learned at hops that
// know both: taps and hooks further down resolve them back to the device.
func (s *server) aliases(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(r.Context(), `
		SELECT alias, argMax(device, ts) FROM hop_events
		WHERE tenant = ? AND alias != ''
		GROUP BY alias`, s.rules.Tenant)
	if err != nil {
		httpError(w, err)
		return
	}
	defer rows.Close()
	out := []alias.Pair{}
	for rows.Next() {
		var a, device string
		if err := rows.Scan(&a, &device); err != nil {
			httpError(w, err)
			return
		}
		if kind, value, ok := alias.Parse(a); ok {
			out = append(out, alias.Pair{Kind: kind, Value: value, Device: device})
		}
	}
	writeJSON(w, out)
}

// judge works out one route's verdict for a device.
func (s *server) judge(device string, rt journey.TypedRoute, js []verdict.Journey, now time.Time, lastSeen *verdict.HopTime, blind map[string]*verdict.Blind) verdict.Verdict {
	// A device that marks its own sends is judged from the device.
	reported := map[string]bool{}
	for _, j := range js {
		for _, h := range j.Hops {
			reported[h.Hop] = true
		}
	}
	judged := rt.JudgedHops(func(h string) bool { return reported[h] })
	v := verdict.Decide(device, judged, js, now, s.grace, lastSeen)
	v.At(device, viaOf(rt)[v.NotReached])
	switch v.State {
	case verdict.Stopped:
		if v.NotReached != "" {
			v.Hold(device, blind[v.NotReached], now, s.grace)
		}
	case verdict.Silent:
		v.Hold(device, blind[judged[0]], now, s.grace)
	case verdict.OK:
		// Healthy on readings from before the tap went blind: nothing seen
		// since, so nothing to say until it is back.
		if b := blind[judged[0]]; b.Recent(now, s.grace) && len(js) > 0 && js[0].First.Before(b.From) {
			v.Wait(device, b)
		}
	}
	return v
}

// viaOf maps each hop of a route to the service that carries readings to it.
func viaOf(rt journey.TypedRoute) map[string]string {
	out := map[string]string{}
	for _, st := range rt.Steps {
		if st.Via != "" {
			out[st.Hop] = st.Via
		}
	}
	return out
}

// optional lists the route's optional hops.
func (s *server) optional() []string {
	out := []string{}
	for _, st := range s.rules.Route {
		if st.Optional {
			out = append(out, st.Hop)
		}
	}
	return out
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

// blindness finds, per hop, when the tap was not fully watching it between
// from and now. Hops whose tap sends no health reports are left out.
func (s *server) blindness(ctx context.Context, from, now time.Time) (map[string]*verdict.Blind, error) {
	rows, err := s.db.Query(ctx, `
		SELECT hop, ts, connected, missed, delay_ms, every_ms FROM tap_health
		WHERE tenant = ? AND ts >= ?
		ORDER BY hop, ts`, s.rules.Tenant, from.Add(-time.Minute))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	beats := map[string][]verdict.Beat{}
	for rows.Next() {
		var hopName string
		var at time.Time
		var connected uint8
		var missed, delay, every uint32
		if err := rows.Scan(&hopName, &at, &connected, &missed, &delay, &every); err != nil {
			return nil, err
		}
		beats[hopName] = append(beats[hopName], verdict.Beat{At: at, Connected: connected == 1, Missed: int(missed),
			Delay: time.Duration(delay) * time.Millisecond, Every: time.Duration(every) * time.Millisecond})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := map[string]*verdict.Blind{}
	for h, b := range beats {
		if x := verdict.Blindness(h, b, from, now, s.grace/2); x != nil {
			out[h] = x
		}
	}
	return out, nil
}

func blindList(m map[string]*verdict.Blind) []*verdict.Blind {
	out := []*verdict.Blind{}
	for _, b := range m {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hop < out[j].Hop })
	return out
}

// quick judges one route of one device from its last sightings.
func (s *server) quick(device string, rt journey.TypedRoute, seen map[string]time.Time, now time.Time, blind map[string]*verdict.Blind) fleetDevice {
	hops := rt.JudgedHops(func(h string) bool { _, ok := seen[h]; return ok })
	state, after := verdict.Quick(hops, seen, now, s.grace, s.grace+45*time.Second)
	// A stop just before a hop the tap was not watching, or silence while it
	// was not watching the first hop, may be the tap's own doing.
	depends, next := hops[0], ""
	if state == verdict.Stopped && after != "" {
		for i := 0; i+1 < len(hops); i++ {
			if hops[i] == after {
				depends, next = hops[i+1], hops[i+1]
			}
		}
	}
	// The hop's last sighting of the device; a stop or silence that began
	// before the tap went blind stands.
	lastAt := seen[depends]
	if state == verdict.Silent {
		lastAt = seen[hops[0]]
	}
	if b := blind[depends]; (state == verdict.Stopped || state == verdict.Silent) && b.Recent(now, s.grace) && !b.Predates(lastAt, s.grace) {
		state = verdict.Unsure
	}
	fd := fleetDevice{Device: device, State: state, StoppedAfter: after, Route: rt.Name}
	if next != "" {
		fd.StoppedAt = viaOf(rt)[next]
		if state == verdict.Stopped {
			for _, dl := range s.rules.DeadLetters() {
				if t, ok := seen[dl.HopName()]; ok && dl.Rejects == next && !t.Before(seen[hops[0]].Add(-s.grace)) {
					fd.Rejected = true
				}
			}
		}
	}
	return fd
}

// allHops lists every route's hops once, in route order.
func (s *server) allHops() []string {
	var out []string
	seen := map[string]bool{}
	for _, rt := range s.rules.AllRoutes() {
		for _, h := range rt.Hops() {
			if !seen[h] {
				seen[h] = true
				out = append(out, h)
			}
		}
	}
	return out
}

// counterType marks fleet sightings that come from device counters, which
// carry no message type.
const counterType = "\x00counters"

// rejectedAfter reports whether a dead-letter source for the hop after
// stoppedAfter saw the device as recently as its latest reading.
func (s *server) rejectedAfter(stoppedAfter string, seen map[string]time.Time) bool {
	hops := s.rules.Hops()
	first, ok := seen[hops[0]]
	if !ok {
		return false
	}
	for i := 0; i+1 < len(hops); i++ {
		if hops[i] != stoppedAfter {
			continue
		}
		for _, dl := range s.rules.DeadLetters() {
			if t, ok := seen[dl.HopName()]; ok && dl.Rejects == hops[i+1] && !t.Before(first.Add(-s.grace)) {
				return true
			}
		}
	}
	return false
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
