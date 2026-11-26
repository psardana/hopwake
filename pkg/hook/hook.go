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
// never slowed or stopped by its hook. A reading is marked served only the
// first time, however often it is asked for.
//
// Hooks in other languages follow the same contract; see docs/hooks.md.
package hook

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/psardana/hopwake/internal/alias"
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
	Tenant   string // as in the rules file; default $HOPWAKE_TENANT
	Hop      string // the hop name, matching a hook source: "hook:" + match
	Endpoint string // Hopwake's OTLP/HTTP endpoint; default $HOPWAKE_OTLP_ENDPOINT, else http://localhost:4318
	Service  string // the service name on the spans; default the hop name
	// TimePrecision must match the rules file's time_precision, so marks
	// made by time agree with the taps' journey IDs.
	TimePrecision time.Duration
	// Aliases is the journey service's base URL, for marks made under an
	// alias such as a system ID (see MarkTimeAlias); default $HOPWAKE_API.
	Aliases string
}

// Hook sends marks for one hop.
type Hook struct {
	cfg     Config
	tp      *sdktrace.TracerProvider
	rec     *hop.Recorder
	aliases *alias.Resolver

	mu     sync.Mutex
	served map[string]struct{} // journey IDs already marked served
}

// servedLimit bounds the memory of served readings; past it the hook
// forgets and starts again, at worst marking a reading served twice.
const servedLimit = 100_000

// firstServe reports whether a serve mark for the journey should be sent.
func (h *Hook) firstServe(op hop.Op, id string) bool {
	if op != Serve {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.served[id]; ok {
		return false
	}
	if len(h.served) >= servedLimit {
		h.served = map[string]struct{}{}
	}
	h.served[id] = struct{}{}
	return true
}

// New starts a hook. It does not contact Hopwake until the first marks are sent.
func New(ctx context.Context, c Config) (*Hook, error) {
	if c.Tenant == "" {
		c.Tenant = os.Getenv("HOPWAKE_TENANT")
	}
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
	if c.TimePrecision == 0 {
		c.TimePrecision, _ = time.ParseDuration(os.Getenv("HOPWAKE_TIME_PRECISION"))
	}
	h := &Hook{cfg: c, tp: tp, rec: hop.NewRecorder(tp), served: map[string]struct{}{}}
	if c.Aliases == "" {
		c.Aliases = os.Getenv("HOPWAKE_API")
	}
	if c.Aliases != "" {
		h.aliases = alias.NewResolver(context.Background(), c.Aliases, 5*time.Second)
	}
	return h, nil
}

// Mark says what happened to one reading, now.
func (h *Hook) Mark(ctx context.Context, op hop.Op, device string, seq uint64) error {
	return h.MarkBoot(ctx, op, device, "", seq)
}

// MarkBoot is Mark for sources that name a boot field.
func (h *Hook) MarkBoot(ctx context.Context, op hop.Op, device, boot string, seq uint64) error {
	k := journey.Key{Tenant: h.cfg.Tenant, Device: device, Boot: boot, Seq: seq}
	if !h.firstServe(op, k.ID()) {
		return nil
	}
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

// MarkTime is Mark for readings that carry no sequence number: they are
// known by their device, message type and the time the device gave them,
// exactly as a tap derives their journey ID.
func (h *Hook) MarkTime(ctx context.Context, op hop.Op, device, messageType string, at time.Time) error {
	return h.markTime(ctx, op, device, "", messageType, at)
}

// MarkTimeTeach is MarkTime at a hop that knows both the device and another
// name it goes by further down, such as the system ID a store keys it by.
// Hopwake learns the pair, so later hops can mark readings under the alias.
func (h *Hook) MarkTimeTeach(ctx context.Context, op hop.Op, device, aliasKind, aliasValue, messageType string, at time.Time) error {
	return h.markTime(ctx, op, device, alias.Format(aliasKind, aliasValue), messageType, at)
}

// ErrUnknownAlias means Hopwake has not learned which device an alias names.
var ErrUnknownAlias = errors.New("hook: unknown alias")

// MarkTimeAlias is MarkTime for a hop that knows the device only by an alias,
// such as a system ID. The hook resolves it to the device through the pairs
// Hopwake learned upstream.
func (h *Hook) MarkTimeAlias(ctx context.Context, op hop.Op, aliasKind, aliasValue, messageType string, at time.Time) error {
	if h.aliases == nil {
		return ErrUnknownAlias
	}
	device, ok := h.aliases.Device(aliasKind, aliasValue)
	if !ok {
		return ErrUnknownAlias
	}
	return h.markTime(ctx, op, device, alias.Format(aliasKind, aliasValue), messageType, at)
}

func (h *Hook) markTime(ctx context.Context, op hop.Op, device, aliasOf, messageType string, at time.Time) error {
	k := journey.Key{Tenant: h.cfg.Tenant, Device: device, Type: messageType, Time: journey.FormatTime(at, h.cfg.TimePrecision)}
	if !h.firstServe(op, k.ID()) {
		return nil
	}
	return h.rec.Record(ctx, hop.Event{
		JourneyID:   k.ID(),
		Tenant:      h.cfg.Tenant,
		Device:      device,
		Hop:         h.cfg.Hop,
		System:      "hook",
		Op:          op,
		Correlation: hop.Derived,
		At:          time.Now(),
		MessageType: messageType,
		Alias:       aliasOf,
	})
}

// Close sends what is queued and stops the hook.
func (h *Hook) Close(ctx context.Context) error { return h.tp.Shutdown(ctx) }
