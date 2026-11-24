package hop

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// HealthSchema marks tap health reports, which share the hop event pipeline
// but are not hop events.
const HealthSchema = "tap/v1"

// Attribute keys on tap health spans.
const (
	AttrTap          = "hopwake.tap.id"
	AttrTapConnected = "hopwake.tap.connected"
	AttrTapSeen      = "hopwake.tap.seen"
	AttrTapMissed    = "hopwake.tap.missed"
	AttrTapDelay     = "hopwake.tap.delay_ms"
	AttrTapEvery     = "hopwake.tap.every_ms"
)

// Health is what a tap says about one hop it watches, once per interval: so
// the journey service can tell a reading that never arrived from one the tap
// failed to see.
type Health struct {
	Tenant    string
	Hop       string
	Tap       string        // which tap process
	Connected bool          // the tap was connected to the source for the whole interval
	Seen      int           // messages the tap read in the interval
	Missed    int           // messages the tap knows it dropped in the interval
	MaxDelay  time.Duration // the longest a message waited before the tap read it
	Every     time.Duration // the interval; a missing report means the tap was not watching
	At        time.Time     // end of the interval
}

// RecordHealth sends one health report.
func (r *Recorder) RecordHealth(ctx context.Context, h Health) error {
	if h.Hop == "" || h.Tap == "" || h.Every <= 0 {
		return errors.New("hop: health needs a hop, a tap and an interval")
	}
	at := h.At
	if at.IsZero() {
		at = time.Now()
	}
	_, span := r.tracer.Start(ctx, "tap health "+h.Hop,
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(at),
		trace.WithAttributes(
			attribute.String(AttrSchema, HealthSchema),
			attribute.String(AttrTenant, h.Tenant),
			attribute.String(AttrHop, h.Hop),
			attribute.String(AttrTap, h.Tap),
			attribute.Bool(AttrTapConnected, h.Connected),
			attribute.Int(AttrTapSeen, h.Seen),
			attribute.Int(AttrTapMissed, h.Missed),
			attribute.Int64(AttrTapDelay, h.MaxDelay.Milliseconds()),
			attribute.Int64(AttrTapEvery, h.Every.Milliseconds()),
		))
	span.End(trace.WithTimestamp(at))
	return nil
}
