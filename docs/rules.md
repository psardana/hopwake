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
| `device_field` | Top-level JSON field holding the device |
| `seq_field` | Top-level JSON field holding the device's sequence number |
| `boot_field` | Optional boot counter or boot ID, so a reboot does not reuse journey IDs |
| `schema` | Optional JSON Schema file the payloads are declared to follow, relative to the rules file |
| `validate_every` | Check one reading in N against the schema (default 1) |
| `role` | Empty, `dead-letter` or `aggregate` |
| `rejects` | Dead letters: the route hop their readings never reached |
| `reason_header` | Dead letters: the Kafka header that says why |
| `window_header` | Aggregates: the Kafka header carrying the window record |

Set exactly one of `device_from_topic` and `device_field`. Hook sources take only `name`, `kind` and `match`; the service passes the device and sequence number itself. Aggregate sources take no device, sequence or schema fields.

A payload that breaks its declared schema becomes a hop event with `op: error` and a short reason, such as `pressure_kpa: got string, want number`. The reading still counts as having reached the hop.

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

A step may also be written as just the hop name. The Mappings page in the journey view proposes mappings from watched devices' readings and shows them as YAML to paste here.

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
