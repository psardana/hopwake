"""Plain OpenTelemetry tracing for the normaliser, configured by OTEL_* variables.

Tracing is on only when OTEL_EXPORTER_OTLP_ENDPOINT is set and OTEL_SDK_DISABLED
is not "true"; otherwise every call here is a cheap no-op, so the normaliser
runs without a collector.
"""

import os

from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind, Status, StatusCode  # noqa: F401 (used by the normaliser)

TRACE_HEADERS = ("traceparent", "tracestate")


def setup(service: str):
    if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") and os.environ.get("OTEL_SDK_DISABLED") != "true":
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        # The sampler comes from OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG.
        provider = TracerProvider(resource=Resource.create({"service.name": os.environ.get("OTEL_SERVICE_NAME", service)}))
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)
    return trace.get_tracer("hopwake.reference.normaliser")


def extract(headers):
    """Context carried by Kafka headers, a list of (str, bytes) pairs."""
    carrier = {k: v.decode("latin-1") for k, v in headers if k in TRACE_HEADERS and v is not None}
    return propagate.extract(carrier)


def inject(headers):
    """Headers with the current trace context in place of any earlier one."""
    carrier = {}
    propagate.inject(carrier)
    kept = [(k, v) for k, v in headers if k not in TRACE_HEADERS]
    return kept + [(k, v.encode("latin-1")) for k, v in carrier.items()]


def shutdown():
    provider = trace.get_tracer_provider()
    if hasattr(provider, "shutdown"):
        provider.shutdown()


CONSUMER = SpanKind.CONSUMER
