# Hopwake hook for Python

Lets a Python service say that it stored or served a reading, at hops no tap
can watch from outside. It follows the contract in
[docs/hooks.md](../../../docs/hooks.md). Python 3.9 or later, standard
library only.

## Use

```python
from hopwake_hook import Hook, STORE, SERVE

hook = Hook(tenant="fleet", hop="hook:lvc.stored")

hook.mark(STORE, device, seq)                       # readings with a sequence number
hook.mark_boot(STORE, device, boot, seq)            # sources with a boot counter
hook.mark_time(STORE, device, message_type, at)     # readings known by type and time
hook.mark_time_teach(STORE, device, "systemId", "100042", message_type, at)
if not hook.mark_time_alias(SERVE, "systemId", "100042", message_type, at):
    pass  # the alias is not known yet; nothing was sent

hook.close()
```

`Hook` also works as a context manager and closes on exit.

`at` is a `datetime` (a naive one is taken as UTC) or an `int` of Unix
nanoseconds.

Settings, each falling back to the environment when not given:

| Argument | Environment | Default |
| --- | --- | --- |
| `tenant` | `HOPWAKE_TENANT` | required |
| `hop` | | required |
| `endpoint` | `HOPWAKE_OTLP_ENDPOINT` | `http://localhost:4318` |
| `service` | | the hop name |
| `time_precision` (`"1s"`, a `timedelta` or nanoseconds) | `HOPWAKE_TIME_PRECISION` | none |
| `aliases`, the journey service's base URL | `HOPWAKE_API` | none |

Only a missing tenant or hop raises. Marks return at once and are sent in
the background, in batches, at least once a second, as OTLP/JSON. When
Hopwake cannot be reached they are dropped. `flush()` and `close()` wait at
most 5 seconds by default and return `False` when that was not enough.
`accepted`, `dropped` and `queued` count marks.

The journey ID functions are there for services that need them directly:
`journey_id`, `time_journey_id`, `format_time`, `format_alias` and
`parse_duration`.

### Kafka headers

`hopwake_hook.kafka.header(msg.headers(), "seq")` reads a header from the
`(key, value)` list confluent-kafka and kafka-python give, the way the tap
does: the first header with that name, as text, `""` when there is none.

## Test

```sh
cd pkg/hook/python
python3 -m unittest discover -s tests -v
```

The tests check every vector in `pkg/hook/testdata/vectors.json`. The wire
check, from the repository root:

```sh
go build -o bin/hookcheck ./cmd/hookcheck
bin/hookcheck -service conformance-python -- python3 pkg/hook/python/conformance.py
```
