package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseRetention(t *testing.T) {
	for in, want := range map[string]time.Duration{"30d": 720 * time.Hour, "72h": 72 * time.Hour, "1d": 24 * time.Hour, " 7d ": 168 * time.Hour} {
		got, err := parseRetention(in)
		if err != nil || got != want {
			t.Errorf("%q: got %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "0d", "-1d", "thirty", "30m", "1.5d"} {
		if _, err := parseRetention(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

func TestRetentionStatements(t *testing.T) {
	r := retention{keep: 30 * 24 * time.Hour, raw: 3 * 24 * time.Hour, payloads: 24 * time.Hour}
	all := strings.Join(r.statements(), "\n")
	for _, want := range []string{
		"ALTER TABLE hop_events MODIFY TTL toDateTime(ts) + toIntervalSecond(2592000)",
		"ALTER TABLE otel_traces MODIFY TTL toDateTime(Timestamp) + toIntervalSecond(259200)",
		"ALTER TABLE payload_samples MODIFY TTL toDateTime(ts) + toIntervalSecond(86400)",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("missing %q in\n%s", want, all)
		}
	}
}
