// Command ingest accepts ordinary OpenTelemetry spans from services that
// already trace, and turns those that carry a device and a sequence number
// into hop events, so teams with tracing get hops without a tap.
//
// Services send OTLP/HTTP (protobuf or JSON) to ingest instead of, or as well
// as, their usual backend. Which spans count, and where their device and
// sequence number are, is set by the spans section of the rules file. Spans
// that match no rule are ignored.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/psardana/hopwake/internal/hop"
	"github.com/psardana/hopwake/internal/journey"
)

func main() {
	rulesPath := flag.String("rules", env("INGEST_RULES", "reference/journey-rules.yaml"), "rules file with a spans section")
	listen := flag.String("listen", env("INGEST_LISTEN", ":4319"), "OTLP/HTTP address to accept spans on")
	flag.Parse()

	rules, err := journey.LoadConfig(*rulesPath)
	if err != nil {
		log.Fatal(err)
	}
	if len(rules.Spans) == 0 {
		log.Printf("the rules file has no spans section: every span will be ignored")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Hop events go to OTEL_EXPORTER_OTLP_ENDPOINT, Hopwake's collector.
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		log.Fatalf("otlp: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(65536)),
		sdktrace.WithIDGenerator(hop.IDGenerator()),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName("hopwake-ingest"))),
	)
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(c)
	}()
	in := &ingester{rules: rules, rec: hop.NewRecorder(tp)}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", in.serve)
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.Printf("spans: %d received, %d turned into hop events", in.received.Load(), in.mapped.Load())
			}
		}
	}()
	log.Printf("ingest on %s, %d span rules", *listen, len(rules.Spans))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

type ingester struct {
	rules            *journey.Config
	rec              *hop.Recorder
	received, mapped atomic.Uint64
}

func (in *ingester) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	var req coltrace.ExportTraceServiceRequest
	json := r.Header.Get("Content-Type") == "application/json"
	if json {
		err = protojson.Unmarshal(body, &req)
	} else {
		err = proto.Unmarshal(body, &req)
	}
	if err != nil {
		http.Error(w, "not an OTLP trace export", http.StatusBadRequest)
		return
	}
	for _, e := range in.events(&req) {
		_ = in.rec.Record(r.Context(), e)
	}
	resp := &coltrace.ExportTraceServiceResponse{}
	var out []byte
	if json {
		out, _ = protojson.Marshal(resp)
		w.Header().Set("Content-Type", "application/json")
	} else {
		out, _ = proto.Marshal(resp)
		w.Header().Set("Content-Type", "application/x-protobuf")
	}
	_, _ = w.Write(out)
}

// events turns the matching spans of an export into hop events.
func (in *ingester) events(req *coltrace.ExportTraceServiceRequest) []hop.Event {
	var out []hop.Event
	for _, rs := range req.GetResourceSpans() {
		service := attr(rs.GetResource().GetAttributes(), "service.name")
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				in.received.Add(1)
				e, ok := in.event(service, sp)
				if ok {
					in.mapped.Add(1)
					out = append(out, e)
				}
			}
		}
	}
	return out
}

func (in *ingester) event(service string, sp *tracepb.Span) (hop.Event, bool) {
	rule, ok := in.rules.SpanRule(service, sp.GetName())
	if !ok {
		return hop.Event{}, false
	}
	device := attr(sp.GetAttributes(), rule.DeviceAttr)
	seq, err := strconv.ParseUint(attr(sp.GetAttributes(), rule.SeqAttr), 10, 64)
	if device == "" || err != nil {
		return hop.Event{}, false
	}
	boot := ""
	if rule.BootAttr != "" {
		if boot = attr(sp.GetAttributes(), rule.BootAttr); boot == "" {
			return hop.Event{}, false
		}
	}
	k := journey.Key{Tenant: in.rules.Tenant, Device: device, Boot: boot, Seq: seq}
	e := hop.Event{
		JourneyID:   k.ID(),
		Tenant:      in.rules.Tenant,
		Device:      device,
		Seq:         seq,
		Hop:         rule.Hop,
		System:      "span",
		Destination: service,
		Op:          hop.OpTransform,
		Correlation: hop.Derived,
		At:          time.Unix(0, int64(sp.GetEndTimeUnixNano())),
	}
	if sp.GetStatus().GetCode() == tracepb.Status_STATUS_CODE_ERROR {
		e.Op, e.Reason = hop.OpError, sp.GetStatus().GetMessage()
		if e.Reason == "" {
			e.Reason = exceptionMessage(sp)
		}
		if e.Reason == "" {
			e.Reason = "span ended with an error"
		}
	}
	return e, true
}

// exceptionMessage returns the message of the span's first exception event.
func exceptionMessage(sp *tracepb.Span) string {
	for _, ev := range sp.GetEvents() {
		if ev.GetName() == "exception" {
			return attr(ev.GetAttributes(), "exception.message")
		}
	}
	return ""
}

// attr returns an attribute's value as a string, or "".
func attr(kvs []*commonpb.KeyValue, key string) string {
	for _, kv := range kvs {
		if kv.GetKey() != key {
			continue
		}
		switch v := kv.GetValue().GetValue().(type) {
		case *commonpb.AnyValue_StringValue:
			return v.StringValue
		case *commonpb.AnyValue_IntValue:
			return strconv.FormatInt(v.IntValue, 10)
		case *commonpb.AnyValue_DoubleValue:
			return strconv.FormatFloat(v.DoubleValue, 'f', -1, 64)
		case *commonpb.AnyValue_BoolValue:
			return strconv.FormatBool(v.BoolValue)
		}
	}
	return ""
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
