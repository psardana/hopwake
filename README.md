# Hopwake

Follow one device's data through every hop of an IoT pipeline.

A reading leaves a sensor, crosses an MQTT broker, a bridge, Kafka, a few services that reshape and aggregate it, and finally reaches a dashboard or an API. When it does not arrive, finding where it stopped means reading broker logs, consumer offsets and service logs one by one. Hopwake records a small event each time a component sees a device's data and joins those events into a journey per device, including through transforms and aggregations.

The question it answers: **where is device X's data, and what happened to it on the way?**

## Status

Early work. Nothing here is usable yet.

| Phase | Goal | State |
| --- | --- | --- |
| 0. Validate | Reference pipeline with a simulated fleet, baseline with plain OpenTelemetry | Done |
| 1. Core journey | MQTT and Kafka taps, journey IDs, storage, a device journey view | In progress: taps, store, verdicts and the journey view work |
| 2. Transforms and aggregates | Schema diffs per hop, watch mode, aggregation drill-down | Not started |

## How it works

- **Beside the pipeline, never in it.** Passive taps read a copy of broker traffic, so nothing in the data path changes and nothing slows down if Hopwake is down.
- **Journey IDs are derived, not only propagated.** MQTT 3.1.1 has no headers, so a journey ID is computed from the device ID and its sequence number or timestamp. Any hop that still sees those fields computes the same ID on its own. A `traceparent` is used when a message carries one.
- **The device is the index.** Storage is ordered by device and time, so a single device's history is one lookup.
- **Aggregates keep their inputs findable.** An aggregated value records its operator, key and window; its contributing devices are resolved when you ask.

See [docs/architecture.md](docs/architecture.md) and [docs/hop-event.md](docs/hop-event.md).

## Layout

| Path | What |
| --- | --- |
| `docs/` | Architecture and data model |
| `internal/journey/` | Journey IDs and the per-source rules file: where each source keeps the device and sequence number, and the expected route |
| `internal/hop/` | Hop events as OTLP spans whose trace ID is the journey ID |
| `internal/verdict/` | Decides, from the hops a device's readings reached, whether its data gets through and where it stops |
| `cmd/tap/` | Passive MQTT and Kafka tap: one hop event per reading seen |
| `cmd/journey/` | Journey service: device journeys and verdicts from the store |
| `cmd/journeyprobe/` | The Phase 0 prototype: compares two hops in memory |
| `deploy/` | Hopwake's own stack: tap, OpenTelemetry Collector, ClickHouse, journey service |
| `reference/` | A self-contained IoT pipeline with a simulated fleet, used to develop and test Hopwake |

## Try it

Start the reference pipeline, then Hopwake next to it:

```bash
DEVICES=50 docker compose -f reference/compose.yaml up -d --build
docker compose -f deploy/compose.yaml up -d --build
```

Open the journey view at http://localhost:8090: the fleet, problems first, and for each device a verdict, the route and its recent readings.

Break one device at the bridge, then ask where its data stops:

```bash
DEVICES=50 BRIDGE_FAULT_DEVICES=dev-00042 BRIDGE_FAULT_DROP=1 \
  docker compose -f reference/compose.yaml up -d --no-deps bridge
curl -s localhost:8090/api/devices/dev-00042
```

```
dev-00042: readings stop after mqtt:plant/+/+/telemetry. 17 readings since 10:03:48 UTC did not reach kafka:telemetry.raw.
```

No header was needed and nothing in the pipeline changed: the tap derives each reading's journey ID at every hop from the device and its sequence number.

| Endpoint | Returns |
| --- | --- |
| `GET :8090/api/fleet?window=15m` | Every device seen, judged from the last time each hop saw it; problems first |
| `GET :8090/api/devices/{device}?window=15m` | Verdict, hop counts and the latest journeys of one device |
| `GET :8090/api/journeys/{journey_id}` | Every hop of one reading |

## Licence

Apache License 2.0. See [LICENSE](LICENSE).
