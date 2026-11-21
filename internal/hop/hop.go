// Package hop records hop events: one component saw one reading from one
// device. Hop events travel as OpenTelemetry spans whose trace ID is the
// reading's journey ID, so every hop of a reading lands in the same trace in
// any OTLP backend, and Hopwake's store can index them by device.
package hop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Attribute keys on every hop span. The schema is versioned through
// AttrSchema so the store can tell old events from new ones.
const (
	AttrSchema      = "hopwake.schema"
	AttrTenant      = "hopwake.tenant"
	AttrDevice      = "hopwake.device.id"
	AttrSeq         = "hopwake.seq"
	AttrJourney     = "hopwake.journey.id"
	AttrHop         = "hopwake.hop"
	AttrOp          = "hopwake.op"
	AttrCorrelation = "hopwake.correlation"
	AttrBytes       = "hopwake.payload.bytes"
	AttrSystem      = "messaging.system"
	AttrDestination = "messaging.destination.name"

	SchemaVersion = "hop/v0"
)

// Op is what a hop did with the reading.
type Op string

// The operations a hop can report.
const (
	OpReceive   Op = "receive"
	OpRoute     Op = "route"
	OpTransform Op = "transform"
	OpAggregate Op = "aggregate"
	OpStore     Op = "store"
	OpServe     Op = "serve"
	OpDrop      Op = "drop"
	OpError     Op = "error"
)

// Correlation says how the journey ID was found.
type Correlation string

// The ways a journey ID can be found.
const (
	Propagated Correlation = "propagated" // from a traceparent
	Derived    Correlation = "derived"    // from tenant, device and sequence number
)

// Event is one hop of one reading.
type Event struct {
	JourneyID   string // 32 hex characters
	Tenant      string
	Device      string
	Seq         uint64
	Hop         string // where it happened, e.g. "mqtt:plant/+/+/telemetry" or "kafka:telemetry.raw"
	System      string // "mqtt" or "kafka"
	Destination string // topic the reading was seen on
	Op          Op
	Correlation Correlation
	Bytes       int
	At          time.Time
}

// Validate reports what is wrong with an event, if anything.
func (e Event) Validate() error {
	if _, err := trace.TraceIDFromHex(e.JourneyID); err != nil {
		return fmt.Errorf("hop: journey ID %q is not a trace ID", e.JourneyID)
	}
	if e.Device == "" || e.Hop == "" || e.Op == "" {
		return fmt.Errorf("hop: device, hop and op are required")
	}
	return nil
}

// Recorder turns hop events into spans.
type Recorder struct{ tracer trace.Tracer }

// NewRecorder records through the given tracer provider. The provider must
// be built with IDGenerator() so that each span takes its journey ID.
func NewRecorder(tp trace.TracerProvider) *Recorder {
	return &Recorder{tracer: tp.Tracer("hopwake/hop")}
}

// Record emits one hop event as a span starting and ending at e.At.
func (r *Recorder) Record(ctx context.Context, e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	at := e.At
	if at.IsZero() {
		at = time.Now()
	}
	ctx = context.WithValue(ctx, journeyKey{}, e.JourneyID)
	_, span := r.tracer.Start(ctx, "hop "+e.Hop,
		trace.WithNewRoot(),
		trace.WithTimestamp(at),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String(AttrSchema, SchemaVersion),
			attribute.String(AttrTenant, e.Tenant),
			attribute.String(AttrDevice, e.Device),
			attribute.Int64(AttrSeq, int64(e.Seq)),
			attribute.String(AttrJourney, e.JourneyID),
			attribute.String(AttrHop, e.Hop),
			attribute.String(AttrOp, string(e.Op)),
			attribute.String(AttrCorrelation, string(e.Correlation)),
			attribute.Int(AttrBytes, e.Bytes),
			attribute.String(AttrSystem, e.System),
			attribute.String(AttrDestination, e.Destination),
		))
	span.End(trace.WithTimestamp(at))
	return nil
}

type journeyKey struct{}

// IDGenerator gives each hop span the trace ID of its journey, and a random
// span ID. Spans started without a journey get random IDs.
func IDGenerator() sdktrace.IDGenerator { return idGen{} }

type idGen struct{}

func (idGen) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	tid := randomTraceID()
	if s, ok := ctx.Value(journeyKey{}).(string); ok {
		if parsed, err := trace.TraceIDFromHex(s); err == nil {
			tid = parsed
		}
	}
	return tid, randomSpanID()
}

func (idGen) NewSpanID(context.Context, trace.TraceID) trace.SpanID { return randomSpanID() }

func randomTraceID() trace.TraceID {
	var t trace.TraceID
	_, _ = rand.Read(t[:])
	return t
}

func randomSpanID() trace.SpanID {
	var s trace.SpanID
	_, _ = rand.Read(s[:])
	return s
}

// HexID is a convenience for tests and logs.
func HexID(t trace.TraceID) string { return hex.EncodeToString(t[:]) }
