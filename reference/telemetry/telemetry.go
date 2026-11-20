// Package telemetry sets up plain OpenTelemetry tracing for the reference
// pipeline's Go services, the way a typical team would: OTLP over HTTP, W3C
// trace context, and settings from the standard OTEL_* environment variables.
package telemetry

import (
	"context"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Setup installs a tracer provider when OTEL_EXPORTER_OTLP_ENDPOINT is set and
// OTEL_SDK_DISABLED is not "true", and a no-op one otherwise. The returned
// function flushes and stops it.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" || os.Getenv("OTEL_SDK_DISABLED") == "true" {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	if os.Getenv("OTEL_SERVICE_NAME") == "" {
		os.Setenv("OTEL_SERVICE_NAME", service)
	}
	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithTelemetrySDK())
	if err != nil {
		return nil, err
	}
	// The sampler comes from OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG.
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the named tracer of the installed provider.
func Tracer(name string) trace.Tracer { return otel.Tracer(name) }

// Headers carries trace context in Kafka record headers.
type Headers struct{ R *kgo.Record }

// Get returns the first header value with the key.
func (h Headers) Get(key string) string {
	for _, x := range h.R.Headers {
		if x.Key == key {
			return string(x.Value)
		}
	}
	return ""
}

// Set replaces the header with the key.
func (h Headers) Set(key, value string) {
	for i, x := range h.R.Headers {
		if x.Key == key {
			h.R.Headers[i].Value = []byte(value)
			return
		}
	}
	h.R.Headers = append(h.R.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

// Keys lists the header keys.
func (h Headers) Keys() []string {
	keys := make([]string, 0, len(h.R.Headers))
	for _, x := range h.R.Headers {
		keys = append(keys, x.Key)
	}
	return keys
}

// Extract returns the context carried by a record.
func Extract(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, Headers{R: r})
}

// Inject writes the context into a record's headers.
func Inject(ctx context.Context, r *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, Headers{R: r})
}
