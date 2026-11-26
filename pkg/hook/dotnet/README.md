# Hopwake hook for .NET

Lets a .NET service say that it stored or served a reading, for hops no tap
can watch from outside: a database write, an API response, a proxy. It
follows the contract in [docs/hooks.md](../../../docs/hooks.md) and needs
nothing beyond the base library.

The library targets net8.0 and net10.0.

## Use

Reference `src/Hopwake.Hook/Hopwake.Hook.csproj`, then make one hook per hop:

```csharp
using Hopwake;

await using var stored = new Hook(new HookOptions { Tenant = "fleet", Hop = "hook:lvc.stored" });

stored.Mark(HopOp.Store, device, seq);                  // readings with a sequence number
stored.MarkBoot(HopOp.Store, device, boot, seq);        // ... and a boot counter
stored.MarkTime(HopOp.Store, device, messageType, at);  // readings known by type and time
stored.MarkTimeTeach(HopOp.Store, device, "systemId", "100042", messageType, at);

// At a hop that knows only the other name:
if (!served.MarkTimeAlias(HopOp.Serve, "systemId", "100042", messageType, at))
{
    // Hopwake has not learned this alias yet; nothing was sent.
}
```

- `Hop` must match a hook source in the rules file (`hook:` and its `match`).
- Marks return at once. They are queued (up to 65,536, then dropped) and sent
  in the background at least once a second, as OTLP/JSON to
  `{Endpoint}/v1/traces`. A send that fails or takes over 3 seconds is given up.
- A `Serve` mark is sent only the first time for a reading.
- `Dispose` or `DisposeAsync` sends what is queued, waiting at most 5 seconds.
  `FlushAsync` sends what is queued now.
- Only a missing tenant or hop throws, from the constructor.

Settings left null come from the environment:

| Option | Environment | Default |
| --- | --- | --- |
| `Tenant` | `HOPWAKE_TENANT` | required |
| `Hop` | | required |
| `Endpoint` | `HOPWAKE_OTLP_ENDPOINT` | `http://localhost:4318` |
| `Service` | | the hop name |
| `TimePrecision` | `HOPWAKE_TIME_PRECISION` (`1s`, `500ms`, `1m`) | none |
| `Aliases` | `HOPWAKE_API` | none: `MarkTimeAlias` always returns false |

`TimePrecision` must match `time_precision` in the rules file.
`TimeSpan.Zero` means none, whatever the environment says.

The building blocks are public too: `JourneyId.Seq`, `JourneyId.Time`,
`HopTime.Format` (with an overload taking Unix nanoseconds, for times finer
than `DateTimeOffset`'s 100 ns), `Alias.Format` and `Durations.Parse`.

### Kafka headers

`KafkaHeaders.Get` reads a header without depending on a Kafka client. With
Confluent.Kafka:

```csharp
var pairs = result.Message.Headers.Select(h => KeyValuePair.Create(h.Key, h.GetValueBytes()));
var seq = ulong.Parse(KafkaHeaders.Get(pairs, "seq") ?? "0");
```

It returns the first header with the name, as UTF-8 text, as the tap does,
or null when there is none.

## Test

```sh
cd pkg/hook/dotnet
dotnet test
```

The tests compute every vector in `pkg/hook/testdata/vectors.json` and check
the hook's behaviour against a local stand-in for Hopwake.

The wire check runs the conformance program under `cmd/hookcheck`, from the
repository root:

```sh
go build -o bin/hookcheck ./cmd/hookcheck
dotnet build -c Release pkg/hook/dotnet/conformance/Hopwake.Hook.Conformance
bin/hookcheck -service conformance-dotnet -- \
  dotnet pkg/hook/dotnet/conformance/Hopwake.Hook.Conformance/bin/Release/net10.0/Hopwake.Hook.Conformance.dll
```

The tests and the conformance program target net10.0.
