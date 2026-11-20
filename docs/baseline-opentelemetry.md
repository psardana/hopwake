# Baseline: what stock OpenTelemetry shows

Measured on 18 November 2026 against the reference pipeline: 50 devices, one reading per second each, with `dev-00042` dropped at the bridge on purpose. Every service was instrumented the way a careful team would do it, and generously: each span carries the device ID.

**Result: the traces describe each reading in isolation, and miss the three things the problem needed.** A silently dropped device looks healthy. An aggregate is filed under one arbitrary reading. The portal's requests never join any reading's journey.

## Setup

| Service | Instrumentation |
| --- | --- |
| Bridge (Go) | OpenTelemetry SDK: a consumer span per MQTT message, a producer span per Kafka record, `traceparent` injected into Kafka headers |
| Normaliser (Python) | OpenTelemetry SDK: a consumer span per record, continuing the incoming trace and passing it on |
| Aggregator (Java, Kafka Streams) | OpenTelemetry Java agent 2.11.0, no code change |
| Recorder (Go) | OpenTelemetry SDK: a consumer span per stored record |
| API (TypeScript) | OpenTelemetry Node auto-instrumentation (HTTP, Postgres) |
| Backend | Jaeger 2.5.0, all spans kept (sampling ratio 1) |

Run it with:

```bash
DEVICES=50 BRIDGE_FAULT_DEVICES=dev-00042 BRIDGE_FAULT_DROP=1 \
docker compose -f reference/compose.yaml --env-file reference/tracing.env up -d --build
```

Jaeger is at http://localhost:16686.

## Findings

| # | Question | What the traces show |
| --- | --- | --- |
| 1 | Where does a healthy reading go? | One trace per reading: bridge receive → bridge send → normaliser → aggregator → recorder. The device and Mosquitto hops are missing: the device sends no `traceparent`, and MQTT 3.1.1 has nowhere to carry one. |
| 2 | Where did `dev-00042`'s data stop? | 100 traces in 8 minutes, each a single bridge receive span with status OK. Nothing is marked as an error and nothing says data was lost. In Jaeger this looks the same as a healthy device whose later spans were sampled away. |
| 3 | Which readings never reached Postgres? | Not answerable. A trace store holds what happened; it cannot be asked about what did not happen. |
| 4 | Which readings went into a site's minute median? | The stored minute for site-03 sits inside the trace of one reading from `dev-00035`, the record that closed the window. The other readings it summarises, about 780 from 13 devices, have no link to it. The Java agent records no producer span for the aggregate. |
| 5 | Did a device's data reach the portal? | No API trace contains a reading's spans: 20 of 20 API traces held only API spans. The last hop, data served to a person, cannot be joined to a journey. |
| 6 | Show me one device over time | Jaeger's tag search finds `device.id=dev-00044` and returns a list of separate traces, at most the search limit. There is no per-device timeline, counter or gap view. |
| 7 | What does sampling do? | With head sampling at 10%, nine readings in ten from any device leave no trace. A gap in one device's traces then means nothing, so finding 2 gets worse. |

## What this means for Hopwake

| Finding | Hopwake's answer |
| --- | --- |
| 1, no device or MQTT hop | Passive MQTT tap and a derived journey ID, so the first hop is seen without a header |
| 2 and 3, silent loss | Per-device counters at every hop and expected routes, so a missing hop is a result, not an absence |
| 4, aggregates misfiled | Window records: an aggregate keeps its operator, key and window, and its inputs are resolved by query |
| 5, portal not joined | An API hook that records which device's data was served |
| 6, no device view | Storage ordered by device and time |
| 7, sampling | Sample devices, not messages; counters cover all traffic |

The journey-ID probe in `cmd/journeyprobe` already answers question 2 on the same pipeline without any headers: it reports `dev-00042` as lost between MQTT and Kafka, with its sequence numbers.
