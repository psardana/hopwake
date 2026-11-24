package main

import (
	"testing"

	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/psardana/hopwake/internal/hop"
	"github.com/psardana/hopwake/internal/journey"
)

func str(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func num(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}}
}

func TestEvents(t *testing.T) {
	rules, err := journey.ParseConfig([]byte(`
tenant: t
sources:
  - {name: a, kind: mqtt, match: a/+, device_from_topic: "a/{device}", seq_field: seq}
route: [mqtt:a/+]
spans:
  - {name: n, service: normaliser, span: process raw, device_attr: device.id, seq_attr: reading.seq, hop: "span:normaliser.process"}
`))
	if err != nil {
		t.Fatal(err)
	}
	in := &ingester{rules: rules}
	req := &coltrace.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{str("service.name", "normaliser")}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
			{Name: "process raw", EndTimeUnixNano: 1, Attributes: []*commonpb.KeyValue{str("device.id", "dev-1"), num("reading.seq", 42)}},
			{Name: "process raw", Attributes: []*commonpb.KeyValue{str("device.id", "dev-1"), num("reading.seq", 43)},
				Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR},
				Events: []*tracepb.Span_Event{{Name: "exception", Attributes: []*commonpb.KeyValue{str("exception.message", "no field temp_c")}}}},
			{Name: "process raw", Attributes: []*commonpb.KeyValue{str("device.id", "dev-1")}}, // no seq
			{Name: "poll", Attributes: []*commonpb.KeyValue{str("device.id", "dev-1"), num("reading.seq", 44)}},
		}}},
	}}}
	evs := in.events(req)
	if len(evs) != 2 {
		t.Fatalf("got %d events: %+v", len(evs), evs)
	}
	if evs[0].JourneyID != journey.ID("t", "dev-1", 42) || evs[0].Hop != "span:normaliser.process" || evs[0].Op != hop.OpTransform {
		t.Fatalf("%+v", evs[0])
	}
	if evs[1].Op != hop.OpError || evs[1].Reason != "no field temp_c" {
		t.Fatalf("%+v", evs[1])
	}
	if in.received.Load() != 4 || in.mapped.Load() != 2 {
		t.Fatalf("counts %d %d", in.received.Load(), in.mapped.Load())
	}
}
