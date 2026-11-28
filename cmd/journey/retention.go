package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// retention says how long the store keeps each kind of data. Hop events,
// counters, tap health and aggregate records are what the view reads and
// are kept longest; the raw spans they are copied from are read by nothing
// but the copying views, so they can go after a few days.
type retention struct {
	keep     time.Duration
	raw      time.Duration
	payloads time.Duration
}

// parseRetention reads "30d", "72h", "90m" or any Go duration.
func parseRetention(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if n, ok := strings.CutSuffix(s, "d"); ok {
		days, err := strconv.Atoi(n)
		if err != nil || days < 1 {
			return 0, fmt.Errorf("%q is not a number of days", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < time.Hour {
		return 0, fmt.Errorf("%q: give days (30d) or hours (72h), at least 1h", s)
	}
	return d, nil
}

// statements set each table's time to live from the retention. They run
// on every start, so a changed setting takes effect at the next restart;
// ClickHouse drops whole days once they are out of range.
func (r retention) statements() []string {
	ttl := func(table, expr string, d time.Duration) string {
		return fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s + toIntervalSecond(%d)", table, expr, int64(d.Seconds()))
	}
	return []string{
		ttl("hop_events", "toDateTime(ts)", r.keep),
		ttl("hop_last_seen", "toDateTime(day)", r.keep+24*time.Hour),
		ttl("tap_health", "toDateTime(ts)", r.keep),
		ttl("window_records", "toDateTime(ts)", r.keep),
		ttl("device_counters", "bucket", r.keep),
		ttl("payload_samples", "toDateTime(ts)", r.payloads),
		ttl("otel_traces", "toDateTime(Timestamp)", r.raw),
		"ALTER TABLE otel_traces_trace_id_ts MODIFY TTL toDateTime(Start) + toIntervalSecond(" + strconv.FormatInt(int64(r.raw.Seconds()), 10) + ")",
	}
}
