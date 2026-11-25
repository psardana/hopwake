# Hopwake

Follow one device's data through every hop of an IoT pipeline.

A reading leaves a sensor, crosses an MQTT broker, a bridge, Kafka, a few services that reshape and aggregate it, and finally reaches a dashboard or an API. When it does not arrive, finding where it stopped means reading broker logs, consumer offsets and service logs one by one. Hopwake records a small event each time a component sees a device's data and joins those events into a journey per device, including through transforms and aggregations.

The question it answers: **where is device X's data, and what happened to it on the way?**

## Status

Early work: it runs against its own reference pipeline, not yet against anyone else's.

| Phase | Goal | State |
| --- | --- | --- |
| 0. Validate | Reference pipeline with a simulated fleet, baseline with plain OpenTelemetry | Done |
| 1. Core journey | MQTT and Kafka taps, journey IDs, storage, a device journey view | Done |
| 2. Transforms and aggregates | Why a reading stopped, schemas and mappings, aggregates, readings followed to the portal | Done; the gate needs outside teams |

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
| `internal/fleet/` | The list of devices that should report, so one that never does is a problem rather than absent |
| `internal/verdict/` | Decides, from the hops a device's readings reached, whether its data gets through, where it stops and why |
| `internal/shape/`, `internal/schema/` | Payload shapes and their drift; checks against declared JSON Schemas |
| `internal/mapping/` | Proposes how a service maps fields, from the same readings seen at two hops, and checks values |
| `internal/capture/` | Payloads of watched devices, redacted before they leave the tap |
| `pkg/hook/` | Go and Node hooks for hops a tap cannot see, such as a database write or an API response |
| `cmd/tap/` | Passive MQTT and Kafka tap: hop events, shapes, schema checks, dead letters, aggregates, counters and its own health |
| `cmd/journey/` | Journey service and view: verdicts, shapes, watch mode, mappings and aggregates |
| `cmd/ingest/` | Turns spans from services that already trace into hop events |
| `cmd/journeyprobe/` | The Phase 0 prototype: compares two hops in memory |
| `deploy/` | Hopwake's own stack: tap, OpenTelemetry Collector, ClickHouse, journey service |
| `reference/` | A self-contained IoT pipeline with a simulated fleet, used to develop and test Hopwake |

## Try it

The quickest way is the demo, which starts everything, breaks four devices in four different ways and shows how Hopwake explains each:

```bash
scripts/demo-phase-2.sh
```

Or start the reference pipeline, then Hopwake next to it:

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
dev-00042: readings stop at bridge. 17 readings since 10:03:48 UTC reached mqtt:plant/+/+/telemetry and never arrived at kafka:telemetry.raw.
```

No header was needed and nothing in the pipeline changed: the tap derives each reading's journey ID at every hop from the device and its sequence number.

| Endpoint | Returns |
| --- | --- |
| `GET :8090/api/fleet?window=15m` | Every device seen, plus every expected device from the fleet list, judged from the last time each hop saw it; problems first |
| `GET :8090/api/devices/{device}?window=15m` | Verdict, hop counts and the latest journeys of one device |
| `GET :8090/api/journeys/{journey_id}` | Every hop of one reading, with payloads for watched devices |
| `POST`, `DELETE :8090/api/devices/{device}/watch` | Start or stop watching a device |
| `GET :8090/api/mappings` | Proposed and confirmed field mappings per route edge |
| `GET :8090/api/windows?window=15m` | Aggregated outputs, with the devices missing from each |

How to describe a pipeline to Hopwake: [docs/rules.md](docs/rules.md).

## Licence

Apache License 2.0. See [LICENSE](LICENSE).
