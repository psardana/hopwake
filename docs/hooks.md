# Hooks

A tap watches MQTT and Kafka from the outside. Some hops cannot be watched
that way: a service writing a reading to a database, an API serving it, an
app proxy passing it on. At those hops the service marks the reading itself
with a **hook**.

A hook sends the same hop events a tap does, keyed by the same journey ID, so
the journey view puts them on the same reading. Hooks exist for:

| Language | Where | Notes |
| --- | --- | --- |
| Go | `pkg/hook` | |
| Java (and Kotlin) | `pkg/hook/java` | plain library, plus a Spring Boot starter |
| Python | `pkg/hook/python` | standard library only |
| .NET | `pkg/hook/dotnet` | no dependencies beyond the base library |
| Rust | `pkg/hook/rust` | |
| Node | `pkg/hook/node` | |
| C and C++ (devices) | `pkg/hook/c` | marks go over the device's own MQTT link, through the tap; see [Device marks](#device-marks) |

Every hook follows the contract below. The shared test vectors
(`pkg/hook/testdata/vectors.json`) and the wire check (`cmd/hookcheck`)
make sure they do.

## Using a hook

A hook is made for one hop, then marks readings as they pass:

```
hook = Hook(tenant: "fleet", hop: "hook:lvc.stored")
hook.mark(store, device, seq)                     # readings with a sequence number
hook.markTime(store, device, messageType, time)   # readings known by type and time
hook.markTimeTeach(store, device, "systemId", "100042", messageType, time)
hook.markTimeAlias(serve, "systemId", "100042", messageType, time)
hook.close()
```

- `hop` must match a hook source in the rules file: `hook:` and its `match`.
- `op` is `store` (the service kept the reading) or `serve` (it handed the
  reading to someone). Hooks that mark Kafka records themselves, such as the
  Spring Boot starter, also use `receive` (a service handled a record it
  consumed) and `route` (a record it sent was acknowledged).
- `markBoot` is `mark` for sources with a boot counter.
- `markTime` is for readings without a sequence number. They are known by
  device, message type and the time the device gave them, exactly as a tap
  finds them (`time_field` in the rules file).
- `markTimeTeach` is for the hop that knows both the device and another name
  it goes by further down, such as the system ID a store keys it by. Hopwake
  learns the pair from this mark.
- `markTimeAlias` is for hops that know only that other name. The hook looks
  the device up in the pairs Hopwake learned. When the pair is not known yet,
  the call says so (an error, `false` or an exception, as the language does
  it) and sends nothing. The service carries on; the mark is simply missing.
- `close` sends what is queued.

### Configuration

| Setting | Environment | Default |
| --- | --- | --- |
| tenant | `HOPWAKE_TENANT` | required |
| hop | | required |
| endpoint, Hopwake's OTLP/HTTP address | `HOPWAKE_OTLP_ENDPOINT` | `http://localhost:4318` |
| service, the `service.name` on the spans | | the hop name |
| time precision, as `time_precision` in the rules file | `HOPWAKE_TIME_PRECISION` (`1s`, `500ms`, `1m`) | none: the time as given |
| aliases, the journey service's base URL | `HOPWAKE_API` | none: `markTimeAlias` always says unknown |

A setting given in code wins over the environment.

## The contract

### Journey IDs

A journey ID is the first 16 bytes of a SHA-256, written as 32 lowercase hex
characters. The hash is fed **fields**: each one is its UTF-8 bytes, preceded
by their length as 4 bytes, big-endian.

For a reading with a sequence number:

1. the field `hopwake/journey/v1`
2. the field *tenant*
3. the field *device*
4. only when there is a boot: the field `boot:` + *boot*
5. the sequence number as 8 bytes, big-endian, unsigned, without a length

For a reading known by its time:

1. the field `hopwake/journey/time/v1`
2. the field *tenant*
3. the field *device*
4. the field `type:` + *message type* (the type may be empty)
5. the field `time:` + *the formatted time*

If all 16 bytes are zero, the last one becomes 1.

### Times

The reading's time is first rounded **down** to the time precision, counted
from the Unix epoch. A precision must divide one second evenly, or be a whole
number of seconds that divides a day evenly. (The Go code counts from the
year 1; for these precisions the two give the same result, for others they
would not.) An unreadable `HOPWAKE_TIME_PRECISION` is ignored.

It is then written in UTC as `YYYY-MM-DDThh:mm:ss`, followed by a fraction
of up to nine digits without trailing zeros (none when it is zero), then `Z`:

| Time | Written |
| --- | --- |
| 10:00:00.000 | `2026-11-18T10:00:00Z` |
| 10:00:00.500 | `2026-11-18T10:00:00.5Z` |
| 10:00:00.120 | `2026-11-18T10:00:00.12Z` |
| 10:00:00.123456789 | `2026-11-18T10:00:00.123456789Z` |

Languages whose time type is less precise than nanoseconds simply cannot
produce the finer vectors; they skip them.

### Aliases

An alias travels as `kind:value`, for example `systemId:100042`.

The alias list is `GET {aliases}/api/aliases`, a JSON array of
`{"kind": "systemId", "value": "100042", "device": "EMS0000042"}`. A hook
fetches it when it starts and every 5 seconds after, replacing its copy
each time. A failed fetch keeps the copy it has.

### On the wire

Marks are OTLP/HTTP trace exports, `POST {endpoint}/v1/traces`, as protobuf
or as JSON. In JSON, trace and span IDs are hex and 64-bit integers are
strings.

Each mark is one span:

| Field | Value |
| --- | --- |
| trace ID | the journey ID |
| span ID | 8 random bytes, not all zero |
| parent | none |
| name | `hop ` + hop |
| kind | consumer (5) |
| start, end | both the time of the mark (now, not the reading's time) |
| resource `service.name` | the service |

Attributes:

| Attribute | Value |
| --- | --- |
| `hopwake.schema` | `hop/v1` |
| `hopwake.tenant` | tenant |
| `hopwake.device.id` | device (for an alias mark, the device it resolved to) |
| `hopwake.seq` | sequence number as an integer; 0 for time marks. OTLP integers are signed 64-bit, so a sequence number of 2^63 or more arrives negative; the journey ID always uses the full unsigned value |
| `hopwake.journey.id` | the journey ID again |
| `hopwake.hop` | hop |
| `hopwake.op` | `store` or `serve`; `receive` or `route` for Kafka marks |
| `hopwake.correlation` | `derived` |
| `messaging.system` | `hook` |
| `hopwake.message.type` | time marks only: the message type, when not empty |
| `hopwake.device.alias` | teach and alias marks only: `kind:value` |

`hopwake.payload.bytes` (0) and `messaging.destination.name` (empty) may be
sent; they are ignored for hooks.

### Behaviour

- **Never in the way.** A mark returns at once. Marks wait in a bounded
  queue of at least 10,000 and are sent in batches at least once a second.
  When the queue is full, new marks are dropped. A send that fails or takes
  longer than 3 seconds is given up on. Nothing a hook does throws into the
  service, apart from a missing tenant or hop when the hook is made.
- **Served once.** A `serve` mark is sent only the first time for a journey
  ID, however often the reading is asked for. The hook remembers up to
  100,000 served journey IDs, then forgets them all and starts again.
  `store` marks are always sent.
- **Close** sends what is queued, waiting at most a few seconds.

## Checking a hook

Two checks, both run by `scripts/check-hooks.sh`:

1. **Test vectors.** Each hook's unit tests read
   `pkg/hook/testdata/vectors.json` and compute every journey ID, formatted
   time and alias in it. The file is written by the Go tests
   (`go test ./pkg/hook -run TestVectors -update`), so it changes only with
   the Go code that defines journey IDs.
2. **The wire check.** `cmd/hookcheck` stands in for Hopwake. It runs a
   hook's conformance program with `HOPWAKE_OTLP_ENDPOINT`, `HOPWAKE_API`,
   `HOPWAKE_TIME_PRECISION=1s`, `HOPWAKE_TENANT=conformance` and
   `HOPWAKE_HOP=hook:conformance.check` set, then compares the spans that
   arrived with the script below. Anything missing, extra or different fails.

### The conformance script

Each hook has a small program that does exactly this, with service name
`conformance-<language>`:

1. `mark(store, "dev-00042", 1683)`
2. `markBoot(store, "dev-00042", "7", 12)`
3. `mark(serve, "dev-00042", 1683)`
4. `mark(serve, "dev-00042", 1683)` again: must not be sent
5. `markTime(store, "EMS0000042", "ems.measurements.Measurement", 2026-11-18T10:00:00.750Z)`
6. `markTimeTeach(store, "EMS0000042", "systemId", "100042", same type, same time)`
7. `markTimeAlias(serve, "systemId", "100042", same type, same time)`, retried
   until the alias list has arrived (up to 15 seconds)
8. `markTimeAlias(serve, "systemId", "999999", same type, same time)` must
   say unknown and send nothing; the program fails if it does not
9. `close()`

## Device marks

Devices rarely have spare memory for an OTLP exporter, and often no HTTP
client at all. They do have an MQTT connection. So a device marks its own
sends with a small message on its own MQTT topic, and the tap turns it into
a hop event, deriving the journey ID itself. The C hook (`pkg/hook/c`)
builds these messages and keeps them while the device is offline. The
message and the rules for it are described in
[rules.md](rules.md#device-marks).
