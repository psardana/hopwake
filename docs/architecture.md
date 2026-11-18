# Architecture

Hopwake sits beside an IoT pipeline. Capture points emit small hop events, a collector normalises them, and a journey service stitches them into one journey per device.

```
Devices ──► MQTT broker ──► bridge ──► Kafka ──► services / stream jobs ──► DB / API ──► apps
   ┆             ┆                       ┆                 ┆                      ┆
 device SDK   MQTT tap               Kafka tap      service SDK / job plugin   API hook
 (optional)
   └─────────────┴───────────── hop events (OTLP) ─────────┴──────────────────────┘
                                         ▼
                                     Collector
                                         ▼
                ClickHouse  ◄──  Journey service  ◄──  Web UI
                                         ▲
                                   Control plane
```

## Principles

1. **Out of band, fail open.** Losing Hopwake never blocks or slows the pipeline it watches.
2. **Derived journey IDs.** A hop that has the device ID and the device's sequence number or timestamp computes the journey ID on its own, so passive taps can join a journey without any header. W3C `traceparent` is used when present.
3. **Device-first index.** Stored events are ordered by tenant, device and time.
4. **Sample devices, not messages.** A device is in or out of the sample by a hash of its ID, so every sampled device has complete journeys. Counters cover all traffic.
5. **Aggregates resolve at query time.** A window record stores the operator, group key and window bounds; the inputs are found when someone asks.
6. **Standard plumbing.** OTLP on the wire, the OpenTelemetry Collector for ingest, ClickHouse for storage.

## Components

| Component | Responsibility |
| --- | --- |
| MQTT tap | Shared subscription on chosen topics; emits a hop event or a counter per message |
| Kafka tap | Separate consumer group; header aware; emits hop events |
| Job plugins | Emit a window record for each aggregate a stream job produces |
| Service SDK | Adds device ID and journey ID to ordinary OpenTelemetry spans |
| API hook | Records that a device's data was served to an app |
| Collector | Extracts device IDs, derives journey IDs, samples, redacts payloads, batches |
| Store | Hop events, counters, window records, short-lived payload samples |
| Journey service | Stitches hops into a graph, resolves aggregate inputs, compares with expected routes |
| Control plane | Watch lists, sampling rules, expected routes |
| Web UI | Device journey view, fleet map, aggregation drill-down, watch mode |

## Targets for the first version

| Area | Target |
| --- | --- |
| Scale | 100,000 devices, 5,000 messages a second, 8 hops per route |
| Freshness | A hop visible within 10 seconds (p95) |
| Query | A device's journeys load in under 2 seconds (p95) |
| Overhead | Passive taps add none to the data path |
| Capture | Counters for all messages; full journeys for sampled and watched devices and for errors |

## Hard cases

| Case | Approach |
| --- | --- |
| No headers (MQTT 3.1.1) | Derived journey ID from device ID plus sequence or timestamp |
| Managed hubs that emit logs, not traces | Tap before and after the hub; ingest its logs as hop events marked as such |
| A hop that drops the device ID | Needs the SDK or a plugin; until then shown as a blind hop between two taps |
| Duplicates from QoS 1 or at-least-once delivery | Events are idempotent per journey, component and operation; repeats are counted and shown |
| Devices without a reliable clock | Broker receive time is authoritative; device time is kept and skew is flagged |
| Personal or commercial payloads | Payload capture off by default, redaction at the tap, short retention |
