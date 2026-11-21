package hop

import (
	"context"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/psardana/hopwake/internal/journey"
)

func recorder(t *testing.T) (*Recorder, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithIDGenerator(IDGenerator()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return NewRecorder(tp), exp
}

func TestHopsOfOneReadingShareATrace(t *testing.T) {
	r, exp := recorder(t)
	id := journey.ID("reference", "dev-00042", 7)
	at := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	for i, h := range []string{"mqtt:plant/+/+/telemetry", "kafka:telemetry.raw", "kafka:telemetry.normalized"} {
		err := r.Record(context.Background(), Event{
			JourneyID: id, Tenant: "reference", Device: "dev-00042", Seq: 7,
			Hop: h, Op: OpReceive, Correlation: Derived, At: at.Add(time.Duration(i) * 10 * time.Millisecond),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	spans := exp.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("got %d spans", len(spans))
	}
	seen := map[string]bool{}
	for _, s := range spans {
		if got := HexID(s.SpanContext.TraceID()); got != id {
			t.Fatalf("trace ID %s, want journey ID %s", got, id)
		}
		if s.Parent.IsValid() {
			t.Fatal("hop spans must be roots: no other hop is their parent")
		}
		seen[s.SpanContext.SpanID().String()] = true
		if !s.StartTime.Equal(s.EndTime) {
			t.Fatal("a hop is an instant")
		}
	}
	if len(seen) != 3 {
		t.Fatal("span IDs must differ")
	}
	attrs := map[string]string{}
	for _, a := range spans[1].Attributes {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	for k, want := range map[string]string{AttrDevice: "dev-00042", AttrHop: "kafka:telemetry.raw", AttrSeq: "7", AttrSchema: SchemaVersion, AttrCorrelation: "derived"} {
		if attrs[k] != want {
			t.Errorf("%s = %q, want %q", k, attrs[k], want)
		}
	}
}

func TestInvalidEventsAreRefused(t *testing.T) {
	r, exp := recorder(t)
	for name, e := range map[string]Event{
		"bad journey": {JourneyID: "xyz", Device: "d", Hop: "h", Op: OpReceive},
		"no device":   {JourneyID: journey.ID("t", "d", 1), Hop: "h", Op: OpReceive},
		"no hop":      {JourneyID: journey.ID("t", "d", 1), Device: "d", Op: OpReceive},
	} {
		if err := r.Record(context.Background(), e); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if n := len(exp.GetSpans()); n != 0 {
		t.Fatalf("%d spans written for invalid events", n)
	}
}
