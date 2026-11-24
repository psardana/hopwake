// Package hook lets a service in the pipeline say that it stored or served
// a reading, for hops no tap can watch from outside, such as a database
// write or an HTTP response.
//
// A hook sends the same hop events a tap does, keyed by the same derived
// journey ID, to Hopwake's collector:
//
//	h, err := hook.New(ctx, hook.Config{Tenant: "reference", Hop: "hook:recorder.stored"})
//	...
//	h.Mark(ctx, hook.Store, device, seq)
//
// When Hopwake is not running, marks are dropped quietly; the service is
// never slowed or stopped by its hook.
package hook

import (
	"context"
	"errors"
	"os"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/psardana/hopwake/internal/hop"
	"github.com/psardana/hopwake/internal/journey"
)

// What a hook can say happened to a reading.
const (
	Store = hop.OpStore
	Serve = hop.OpServe
)

// Config says where marks go and what they are called.
type Config struct {
	Tenant   string // as in the rules file
	Hop      string // the hop name, matching a hook source: "hook:" + match
	Endpoint string // Hopwake's OTLP/HTTP endpoint; default $HOPWAKE_OTLP_ENDPOINT, else http://localhost:4318
	Service  string // the service name on the spans; default the hop name
}

// Hook sends marks for one hop.
type Hook struct {
	cfg Config
	tp  *sdktrace.TracerProvider
	rec *hop.Recorder
}

// New starts a hook. It does not contact Hopwake until the first marks are sent.
func New(ctx context.Context, c Config) (*Hook, error) {
	if c.Tenant == "" || c.Hop == "" {
		return nil, errors.New("hook: tenant and hop are required")
	}
	if c.Endpoint == "" {
		c.Endpoint = os.Getenv("HOPWAKE_OTLP_ENDPOINT")
	}
	if c.Endpoint == "" {
		c.Endpoint = "http://localhost:4318"
	}
	if c.Service == "" {
		c.Service = c.Hop
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(c.Endpoint+"/v1/traces"))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		// A full queue drops marks rather than block the service.
		sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(65536)),
		sdktrace.WithIDGenerator(hop.IDGenerator()),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName(c.Service))),
	)
	return &Hook{cfg: c, tp: tp, rec: hop.NewRecorder(tp)}, nil
}

// Mark says what happened to one reading, now.
func (h *Hook) Mark(ctx context.Context, op hop.Op, device string, seq uint64) error {
	return h.MarkBoot(ctx, op, device, "", seq)
}

// MarkBoot is Mark for sources that name a boot field.
func (h *Hook) MarkBoot(ctx context.Context, op hop.Op, device, boot string, seq uint64) error {
	k := journey.Key{Tenant: h.cfg.Tenant, Device: device, Boot: boot, Seq: seq}
	return h.rec.Record(ctx, hop.Event{
		JourneyID:   k.ID(),
		Tenant:      h.cfg.Tenant,
		Device:      device,
		Seq:         seq,
		Hop:         h.cfg.Hop,
		System:      "hook",
		Op:          op,
		Correlation: hop.Derived,
		At:          time.Now(),
	})
}

// Close sends what is queued and stops the hook.
func (h *Hook) Close(ctx context.Context) error { return h.tp.Shutdown(ctx) }
