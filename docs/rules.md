# The rules file

One YAML file tells Hopwake how to follow readings through a pipeline: where each hop finds the device and sequence number, the route readings should take, and what is known about their payloads. Taps, the journey service and ingest all read the same file. The reference pipeline's is [`reference/journey-rules.yaml`](../reference/journey-rules.yaml).

## Tenant

```yaml
tenant: reference
```

Part of every journey ID, so two tenants' devices never share one.

## Sources

Each source is something a tap reads, or a hop a service marks itself. A source's hop name is its kind, a colon and its match, e.g. `kafka:telemetry.raw`.

| Field | Meaning |
| --- | --- |
| `name` | For people |
| `kind` | `mqtt`, `kafka` or `hook` |
| `match` | MQTT topic filter (`+`, `#`), Kafka topic, or a hook's name |
| `device_from_topic` | MQTT only: the topic with `{device}` in place of the device level |
| `device_field` | Field holding the device (a dotted path such as `meta.serial` works) |
| `device_header` | Kafka header or MQTT user property holding the device |
| `device_from_key` | Kafka only: the record key is the device |
| `device_alias` | The device found above is an alias of this kind (for example `systemId`), not the device itself; see below |
| `seq_field` | Field holding the device's sequence number |
| `seq_from` | Header or user property holding the sequence number |
| `boot_field`, `boot_from` | Optional boot counter or boot ID, from a field or a header, so a reboot does not reuse journey IDs |
| `time_field` | No sequence number: the field holding the reading's own time; a comma-separated list is tried in order |
| `format` | `json` (default) or `protobuf` |
| `proto` | Protobuf only: `path` (a directory of `.proto` files, relative to the rules file) and either `type_from` (the header or user property naming each message's type) or `type` (one fixed type) |
| `as_type` | The message type to count readings as, where this hop no longer carries the original one |
| `schema` | Optional JSON Schema file the payloads are declared to follow, relative to the rules file |
| `validate_every` | Check one reading in N against the schema (default 1) |
| `role` | Empty, `dead-letter` or `aggregate` |
| `rejects` | Dead letters: the route hop their readings never reached |
| `reason_header` | Dead letters: the Kafka header that says why |
| `window_header` | Aggregates: the Kafka header carrying the window record |

Set exactly one of `device_from_topic`, `device_field`, `device_header` and `device_from_key`, and exactly one of `seq_field`, `seq_from` and `time_field`. Hook sources take only `name`, `kind` and `match`; the service passes the device and sequence number itself. Aggregate sources take no device, sequence or schema fields.

A payload that breaks its declared schema becomes a hop event with `op: error` and a short reason, such as `pressure_kpa: got string, want number`. The reading still counts as having reached the hop.

## Protobuf sources

```yaml
- name: EMS devices
  kind: mqtt
  match: fleet/+/from/ems/#
  device_from_topic: fleet/{device}/from/ems/#
  format: protobuf
  proto: {path: protos, type_from: MessageType}
  time_field: measurement_time
```

The tap compiles the directory once and decodes each payload as the type its header or user property names. The decoded message is handed on as JSON, with the proto field names, so shapes, schema checks, watch mode, redaction and mappings work as for JSON. Shapes take their types from the descriptor. A payload that does not decode, a missing required field or an unknown type becomes an `op: error` hop event with a short reason, and the same bad message is joined across hops by a hash of its bytes.

## Readings without a sequence number

With `time_field`, a reading is known by its tenant, device, message type and its own time. Two settings matter:

- **Message type.** One device can send several types with the same time, so the type is part of the journey ID.
- **Time precision.** `time_precision: 1s` at the top of the file rounds every reading's time down before it becomes part of the journey ID. Use it when a later hop keeps the time more coarsely than the device sends it, for example a time-series store with whole seconds. Hooks need the same precision (`HOPWAKE_TIME_PRECISION`).

A device that sends the same reading twice with the same time gives two hop events on one journey.

## Readings that change identity

Pipelines often swap the device's ID for another key on the way, such as a system or site ID from master data. Hopwake follows the reading across the swap by learning the pair:

1. The service that does the lookup marks its hop with both IDs (`MarkTimeTeach` in the Go hook). The hop event carries `hopwake.device.alias` = `systemId:100046`.
2. The journey service publishes the pairs it has seen at `GET /api/aliases`.
3. Later hops name the alias instead of the device: a tap source with `device_alias: systemId`, or a hook calling `MarkTimeAlias`. Both look the alias up and place the reading on the device's journey.

A reading whose alias is not known yet is counted as unparsed by the tap and retried by hooks. A hop that strips the message type takes `as_type`.

## Route

The hops every reading should reach, in order:

```yaml
route:
  - hop: mqtt:plant/+/+/telemetry
  - hop: kafka:telemetry.raw
    via: bridge
  - hop: kafka:telemetry.normalized
    via: normaliser
    mapping:
      temperature_k: temp_c + 273.15
      pressure_pa: pressure_kpa * 1000
  - hop: hook:recorder.stored
    via: recorder
  - hop: hook:api.served
    via: api
    optional: true
```

| Field | Meaning |
| --- | --- |
| `hop` | A source's hop name, or a span rule's hop |
| `via` | The service that carries readings here from the hop before; stops are reported at it |
| `mapping` | How that service maps fields, as a person confirmed it: output field, then an input field optionally followed by `+ - * /` and a number |
| `optional` | Happens only on demand (serving a reading to someone who asks). Shown when it happens, never a reason for a stop. Optional hops come last. |

A step may also be written as just the hop name.

### Routes per message type

Where services route by message type, a rules file holds several named routes, each for one or more type prefixes. A reading follows every route whose prefix matches its type.

```yaml
routes:
  - name: measurements
    types: [ems.measurements]
    steps:
      - hop: mqtt:fleet/+/from/ems/#
      - hop: kafka:ems.measurements
        via: bridge
  - name: status
    types: [ems.status]
    steps: [...]
```

`route:` stays valid and means every type. The device page shows one chip per route, and the fleet shows each device's worst route. The Mappings page in the journey view proposes mappings from watched devices' readings and shows them as YAML to paste here.

## Spans

Spans from services that already trace, sent to Hopwake's ingest, become hop events when they carry the device and sequence number:

```yaml
spans:
  - name: normaliser processing
    service: normaliser          # resource service.name
    span: process telemetry.raw  # span name; empty for any
    device_attr: device.id
    seq_attr: reading.seq
    hop: span:normaliser.process
```

A span with error status becomes an `error` hop event, with its status message or exception as the reason.

## Redact

```yaml
redact: [owner.email, readings.gps]
```

Dotted field paths a tap blanks out before it sends a watched device's payload anywhere.
