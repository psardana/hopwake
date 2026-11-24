package hop

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// WindowSchema marks window records: one aggregated output and how to find
// the readings behind it.
const WindowSchema = "window/v1"

// Attribute keys on window record spans.
const (
	AttrWindowOp       = "hopwake.window.op"
	AttrWindowKey      = "hopwake.window.key"
	AttrWindowStart    = "hopwake.window.start"
	AttrWindowEnd      = "hopwake.window.end"
	AttrWindowInputHop = "hopwake.window.input_hop"
	AttrWindowInputs   = "hopwake.window.inputs"
	AttrWindowDevices  = "hopwake.window.devices"       // comma-separated, when listed
	AttrWindowDigest   = "hopwake.window.device_digest" // when there are too many to list
	AttrWindowCount    = "hopwake.window.device_count"
	AttrWindowOutput   = "hopwake.window.output" // the output itself, short
)

// MaxOutput is the most of an aggregate's output a window record carries.
const MaxOutput = 2048

// Window is one aggregated output, as its job described it.
type Window struct {
	Tenant      string
	Hop         string // where the output was seen, e.g. "kafka:telemetry.site_minute"
	Op          string // median, avg, count, ...
	Key         string // the group key, e.g. a site
	Start, End  time.Time
	InputHop    string   // where the inputs were read
	Inputs      int      // how many readings went in
	Devices     []string // which devices sent them, when listed
	Digest      string   // otherwise a digest of the device list
	DeviceCount int
	Output      string
	At          time.Time
}

// RecordWindow sends one window record.
func (r *Recorder) RecordWindow(ctx context.Context, w Window) error {
	if w.Hop == "" || w.Key == "" || w.End.IsZero() {
		return errors.New("hop: a window needs a hop, a key and an end")
	}
	at := w.At
	if at.IsZero() {
		at = time.Now()
	}
	out := w.Output
	if len(out) > MaxOutput {
		out = out[:MaxOutput]
	}
	_, span := r.tracer.Start(ctx, "window "+w.Hop,
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithTimestamp(at),
		trace.WithAttributes(
			attribute.String(AttrSchema, WindowSchema),
			attribute.String(AttrTenant, w.Tenant),
			attribute.String(AttrHop, w.Hop),
			attribute.String(AttrOp, string(OpAggregate)),
			attribute.String(AttrWindowOp, w.Op),
			attribute.String(AttrWindowKey, w.Key),
			attribute.String(AttrWindowStart, w.Start.UTC().Format(time.RFC3339Nano)),
			attribute.String(AttrWindowEnd, w.End.UTC().Format(time.RFC3339Nano)),
			attribute.String(AttrWindowInputHop, w.InputHop),
			attribute.Int(AttrWindowInputs, w.Inputs),
			attribute.Int(AttrWindowCount, w.DeviceCount),
			attribute.String(AttrWindowDevices, strings.Join(w.Devices, ",")),
			attribute.String(AttrWindowDigest, w.Digest),
			attribute.String(AttrWindowOutput, out),
		))
	span.End(trace.WithTimestamp(at))
	return nil
}
