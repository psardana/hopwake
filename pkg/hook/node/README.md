# Node hook

Marks readings at hops no tap can watch from outside: a service storing a
reading, an API serving it. The hook follows the contract in
[docs/hooks.md](../../../docs/hooks.md).

`hopwake-hook.ts` is one file with no dependencies. Copy it into a service,
or link to it, as `reference/api` does. It runs as TypeScript directly on
Node 22.6 and later (type stripping; on by default from Node 23.6). On older
Node, compile it with `tsc` first.

## Using it

```ts
import { Hook } from './hopwake-hook.ts';

const hook = new Hook({ tenant: 'fleet', hop: 'hook:lvc.stored' });

hook.mark('store', device, seq);                        // readings with a sequence number
hook.markBoot('store', device, boot, seq);              // ... and a boot counter
hook.markTime('store', device, messageType, time);      // readings known by type and time
hook.markTimeTeach('store', device, 'systemId', '100042', messageType, time);
if (!hook.markTimeAlias('serve', 'systemId', '100042', messageType, time)) {
  // Hopwake has not learned this system ID yet; nothing was marked.
}

await hook.close(); // sends what is queued, waiting at most 5 seconds
```

`time` is a `Date`, or a `bigint` of nanoseconds since the Unix epoch when
the reading's time is finer than a millisecond.

Marks return at once and never throw. They wait in a queue and are sent at
least once a second. When Hopwake cannot be reached, they are dropped. The
only error the hook raises is a missing tenant or hop in the constructor.

## Settings

| Setting | Environment | Default |
| --- | --- | --- |
| `tenant` | `HOPWAKE_TENANT` | required |
| `hop` | | required |
| `endpoint` | `HOPWAKE_OTLP_ENDPOINT` | `http://localhost:4318` |
| `service` | | the hop |
| `timePrecision`, milliseconds or a duration (`'1s'`, `'500ms'`) | `HOPWAKE_TIME_PRECISION` | none: the time as given |
| `aliases`, the journey service's base URL | `HOPWAKE_API` | none: `markTimeAlias` always returns `false` |
| `maxQueue` | | 50,000 |

A setting given in code wins over the environment. `timePrecision` must
match the rules file's `time_precision`, or time marks get journey IDs the
taps do not.

The module also exports `journeyId`, `timeJourneyId`, `formatTime`,
`formatAlias` and `parseDuration`.

## Checking it

From the repository root:

```sh
node --experimental-strip-types --test pkg/hook/node/hopwake-hook.test.ts

go build -o bin/hookcheck ./cmd/hookcheck
bin/hookcheck -service conformance-node -- node --experimental-strip-types pkg/hook/node/conformance.ts
```

The tests compute every vector in `pkg/hook/testdata/vectors.json`.
`hookcheck` runs `conformance.ts` and checks the spans it sends.
