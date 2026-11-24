# Hop event model (draft v0)

A hop event says that one component saw one message from one device. A journey is the graph of hop events that share a journey ID.

## Hop event

| Field | Type | Meaning |
| --- | --- | --- |
| `journey_id` | string | Joins hops of one message's path |
| `hop_id` | string | Unique per event |
| `parent_hop_ids` | string[] | Upstream hops, when known |
| `tenant` | string | Owner of the device |
| `device_id` | string | The device the data came from |
| `component` | string | Where the hop happened, e.g. `mqtt:plant/+/telemetry`, `kafka:telemetry.normalized` |
| `op` | enum | `receive`, `route`, `transform`, `aggregate`, `store`, `serve`, `drop`, `error` |
| `ts_in`, `ts_out` | timestamp | When the component received and released the message |
| `status` | enum | `ok`, `dropped`, `error` |
| `schema_fp_in`, `schema_fp_out` | string | Fingerprint of the payload shape on each side |
| `bytes` | int | Payload size |
| `payload_hash` | string | Hash of the payload, for matching without storing it |
| `correlation` | enum | `propagated` (from `traceparent`), `derived` (computed), `inferred` (matched by time window) |

Example:

```json
{
  "journey_id": "7f3a1c9e0b2d4c21",
  "hop_id": "b91e04",
  "parent_hop_ids": ["a3f7d2"],
  "tenant": "acme",
  "device_id": "pump-0042",
  "component": "kafka:telemetry.normalized",
  "op": "transform",
  "ts_in": "2026-11-17T09:14:03.120Z",
  "ts_out": "2026-11-17T09:14:03.134Z",
  "status": "ok",
  "schema_fp_in": "cbor:9c1e",
  "schema_fp_out": "json:44ab",
  "bytes": 212,
  "correlation": "derived"
}
```

## On the wire: OTLP spans

Hop events are sent as OpenTelemetry spans, so any OTLP pipeline can carry them. Implemented in `internal/hop`.

| Span field | Value |
| --- | --- |
| Trace ID | The journey ID, so every hop of one reading lands in one trace |
| Span ID | Random |
| Parent | None: hops are siblings, not a call tree |
| Name | `hop <hop>`, e.g. `hop kafka:telemetry.raw` |
| Start and end | The moment the hop saw the reading |
| Kind | Consumer |

| Attribute | Field |
| --- | --- |
| `hopwake.schema` | `hop/v1` (adds the shape and reason attributes to `hop/v0`) |
| `hopwake.tenant` | `tenant` |
| `hopwake.device.id` | `device_id` |
| `hopwake.seq` | the device's sequence number |
| `hopwake.journey.id` | `journey_id`, repeated for stores that do not index trace IDs |
| `hopwake.hop` | `component` |
| `hopwake.op` | `op` |
| `hopwake.correlation` | `correlation` |
| `hopwake.payload.bytes` | `bytes` |
| `hopwake.schema.fp` | fingerprint of the payload's shape (field paths and types) at this hop |
| `hopwake.shape.fields` | the shape's fields, on the first event of a new shape only |
| `hopwake.reason` | why a reading was rejected or failed, on `drop` and `error` events |
| `messaging.system`, `messaging.destination.name` | where the reading was seen, per the OpenTelemetry messaging conventions |

## Journey ID

Resolved in this order:

1. `traceparent` on the message (MQTT 5 user property, Kafka header, AMQP property, HTTP header): use its trace ID.
2. Otherwise `hash(tenant, device_id, seq)`, or `hash(tenant, device_id, device_ts)` when there is no sequence number. The fields are extracted by a configured rule per topic (topic pattern or JSON path).
3. Otherwise the first tap mints an ID and later hops match on device ID within a time window. Such links are marked `inferred`.

### Device reboots

A device that reboots usually starts its sequence number again, so its new readings derive the same journey IDs as old ones. Two safeguards:

- **Boot field.** When the payload carries a boot counter or boot ID, `boot_field` on the source adds it to the journey ID, so readings from different boots never share an ID. Every source on a route must name the same boot information.
- **Time split.** Without a boot field, the journey service treats events with the same journey ID that lie more than `-journey-span` apart (default 60 s) as different readings. This fails only if a device reboots and reaches the same sequence number within that span.

## Rejections

A source marked `role: dead-letter` holds readings a service rejected. The tap records each one as a hop event with `op: drop` at the dead-letter hop, with the reason taken from the header named by `reason_header` (or `rejected` when there is none). `rejects` names the route hop those readings never reached:

```yaml
- name: readings the normaliser rejected
  kind: kafka
  match: telemetry.dlq
  device_field: device_id
  seq_field: seq
  role: dead-letter
  rejects: kafka:telemetry.normalized
  reason_header: dlq_reason
```

A stop whose readings turn up there is reported as a rejection with its reason, not as lost data.

## Tap health

Every few seconds (`-health-every`, default 5 s) the tap sends one report per hop it watches, as a span with `hopwake.schema` = `tap/v1`:

| Attribute | Meaning |
| --- | --- |
| `hopwake.hop` | The hop |
| `hopwake.tap.id` | Which tap process |
| `hopwake.tap.connected` | Connected to the source for the whole interval |
| `hopwake.tap.seen` | Messages read in the interval |
| `hopwake.tap.missed` | Messages the tap dropped because it could not keep up |
| `hopwake.tap.delay_ms` | The longest a message waited before the tap read it |
| `hopwake.tap.every_ms` | The interval; a missing report means the tap was not watching |

The journey service treats a hop as not fully watched while reports say the tap was disconnected, dropped messages or was more than half the grace period behind, and while reports are missing. A stop or silence that such a stretch could explain is reported as waiting for the tap, not as a fault in the pipeline.

## Watched payloads

For devices on the watch list (set in the journey view, for an hour by default and at most a day, every change logged), the tap adds `hopwake.payload`: the payload as compact JSON, at most 16 KB, with the rules file's `redact` fields blanked. Payloads are kept 24 hours. Without a watch, nothing but the shape leaves the tap.

## Counters

Every few seconds (`-counts-every`, default 5 s) the tap sends, per hop, how many messages each device sent and how many were rejected or broke a schema, as one span with `hopwake.schema` = `counts/v1` and parallel comma-separated lists in `hopwake.counts.devices`, `.n`, `.errors` and `.last_ms`. Counters cover every message, also when hop events are sampled (`-sample N` keeps one journey in N, chosen from the journey ID so every hop agrees; watched devices, rejections and schema breaks are always kept). The store sums them per device, hop and five minutes.

## Window record

One aggregated output and how to find its inputs. A job adds it to each output as a Kafka header (`hopwake-window` in the reference aggregator); the tap reads it from a source with `role: aggregate` and sends it with `hopwake.schema` = `window/v1`. The journey service finds the inputs on request: hop events at the input hop, inside the window, from the listed devices. A device that fed the key's windows in the last 30 minutes but not this one is reported missing.

| Field | Meaning |
| --- | --- |
| `output_hop_id` | The hop event of the aggregated output |
| `operator` | `median`, `avg`, `count`, `max`, … |
| `group_key` | The key the aggregate was grouped by |
| `window_start`, `window_end` | Window bounds |
| `input_count` | Number of inputs |
| `device_ids` or `device_digest` | Contributing devices, or a digest when there are many |
| `input_hop_ids` | Exact inputs, only below a cap (proposed 256) or for watched devices |

## Device counter

Per device, component and five-minute bucket: message count, error count, last seen, and a latency sketch. Counters cover all traffic, sampled or not.

## Open questions

- Hash function and ID length for derived journey IDs.
- Whether `component` should split into kind and name.
- How to version the schema once events are stored.
