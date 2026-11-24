package hop

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// CountsSchema marks counter records: how many messages each device sent
// through a hop in an interval, whether or not their hop events were sampled.
const CountsSchema = "counts/v1"

// Attribute keys on counter spans. The per-device values are parallel,
// comma-separated lists, so one span carries a whole hop's interval.
const (
	AttrCountsFrom    = "hopwake.counts.from"
	AttrCountsDevices = "hopwake.counts.devices"
	AttrCountsN       = "hopwake.counts.n"
	AttrCountsErrors  = "hopwake.counts.errors"
	AttrCountsLast    = "hopwake.counts.last_ms"
)

// Count is one device's traffic through a hop in an interval.
type Count struct {
	N, Errors int
	Last      time.Time
}

// RecordCounts sends one hop's counts for the interval [from, to).
func (r *Recorder) RecordCounts(ctx context.Context, tenant, hopName string, from, to time.Time, counts map[string]Count) error {
	if hopName == "" || len(counts) == 0 {
		return errors.New("hop: counts need a hop and at least one device")
	}
	devices := make([]string, 0, len(counts))
	for d := range counts {
		devices = append(devices, d)
	}
	sort.Strings(devices)
	n := make([]string, len(devices))
	e := make([]string, len(devices))
	last := make([]string, len(devices))
	for i, d := range devices {
		c := counts[d]
		n[i] = strconv.Itoa(c.N)
		e[i] = strconv.Itoa(c.Errors)
		last[i] = strconv.FormatInt(c.Last.UnixMilli(), 10)
	}
	_, span := r.tracer.Start(ctx, "counts "+hopName,
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(to),
		trace.WithAttributes(
			attribute.String(AttrSchema, CountsSchema),
			attribute.String(AttrTenant, tenant),
			attribute.String(AttrHop, hopName),
			attribute.Int64(AttrCountsFrom, from.UnixMilli()),
			attribute.String(AttrCountsDevices, strings.Join(devices, ",")),
			attribute.String(AttrCountsN, strings.Join(n, ",")),
			attribute.String(AttrCountsErrors, strings.Join(e, ",")),
			attribute.String(AttrCountsLast, strings.Join(last, ",")),
		))
	span.End(trace.WithTimestamp(to))
	return nil
}
