# Hopwake

Follow one device's data through every hop of an IoT pipeline.

A reading leaves a sensor, crosses an MQTT broker, a bridge, Kafka, a few services that reshape and aggregate it, and finally reaches a dashboard or an API. When it does not arrive, finding where it stopped means reading broker logs, consumer offsets and service logs one by one. Hopwake records a small event each time a component sees a device's data and joins those events into a journey per device, including through transforms and aggregations.

The question it answers: **where is device X's data, and what happened to it on the way?**

## Status

Early work. Nothing here is usable yet.

| Phase | Goal | State |
| --- | --- | --- |
| 0. Validate | Reference pipeline with a simulated fleet, baseline with plain OpenTelemetry | In progress |
| 1. Core journey | MQTT and Kafka taps, journey IDs, storage, a device journey view | Not started |
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
| `reference/` | A self-contained IoT pipeline with a simulated fleet, used to develop and test Hopwake |

## Licence

Apache License 2.0. See [LICENSE](LICENSE).
