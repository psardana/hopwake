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

## Journey ID

Resolved in this order:

1. `traceparent` on the message (MQTT 5 user property, Kafka header, AMQP property, HTTP header): use its trace ID.
2. Otherwise `hash(tenant, device_id, seq)`, or `hash(tenant, device_id, device_ts)` when there is no sequence number. The fields are extracted by a configured rule per topic (topic pattern or JSON path).
3. Otherwise the first tap mints an ID and later hops match on device ID within a time window. Such links are marked `inferred`.

## Window record

One aggregated output and how to find its inputs.

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
