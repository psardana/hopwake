# hopwake-hook for Rust

Marks readings at hops no tap can watch from outside: a service writing a
reading to a database, an API serving it, a proxy passing it on. Each mark
is sent to Hopwake as a hop event, keyed by the same journey ID a tap
derives. The contract every hook follows is in [docs/hooks.md](../../../docs/hooks.md).

## Use

The crate is not on crates.io. Depend on it by path or git:

```toml
[dependencies]
hopwake-hook = { path = "../hopwake/pkg/hook/rust" }
```

Make one hook per hop and mark readings as they pass:

```rust
use std::time::SystemTime;
use hopwake_hook::{Config, Hook, Op};

let hook = Hook::new(Config {
    tenant: Some("fleet".into()),
    hop: "hook:lvc.stored".into(),
    ..Config::default()
})?;

hook.mark(Op::Store, "dev-00042", 1683);                 // readings with a sequence number
hook.mark_boot(Op::Store, "dev-00042", "7", 12);         // ... and a boot counter
hook.mark_time(Op::Store, "EMS0000042", "ems.measurements.Measurement", at);
hook.mark_time_teach(Op::Store, "EMS0000042", "systemId", "100042", "ems.measurements.Measurement", at);
if hook.mark_time_alias(Op::Serve, "systemId", "100042", "ems.measurements.Measurement", at).is_err() {
    // Hopwake has not learned this alias yet; nothing was sent.
}
hook.flush(); // dropping the hook flushes too
```

`Hook` is `Send + Sync`; share it between threads with an `Arc`.

Marks never block and never panic. They wait in a queue of 10,000 and a
background thread sends them as OTLP/JSON at least once a second. When the
queue is full, new marks are dropped; a send that fails or takes longer than
3 seconds is given up on. A `serve` mark is sent only the first time for a
reading. Dropping the hook sends what is queued, waiting at most 4 seconds.

### Configuration

A setting left `None` (or empty) is read from the environment, then from
the default. `Config::from_env(hop)` reads them all from the environment.

| Field | Environment | Default |
| --- | --- | --- |
| `tenant` | `HOPWAKE_TENANT` | required |
| `hop` | | required |
| `endpoint` | `HOPWAKE_OTLP_ENDPOINT` | `http://localhost:4318` |
| `service` | | the hop name |
| `time_precision` | `HOPWAKE_TIME_PRECISION` (`1s`, `500ms`, `1m`) | none: the time as given |
| `aliases` | `HOPWAKE_API` | none: `mark_time_alias` always says unknown |

`time_precision` must match the rules file's `time_precision`, or time marks
get journey IDs the taps never see.

The free functions `journey_id`, `time_journey_id`, `format_time`,
`format_alias` and `parse_duration` compute what the hook sends, for
services that want the journey ID themselves.

## Test

```sh
cargo test
```

runs the unit tests and the shared vectors in `../testdata/vectors.json`.

The wire check runs the conformance program against a stand-in for
Hopwake. From the repository root:

```sh
go build -o bin/hookcheck ./cmd/hookcheck
cargo build --release --manifest-path pkg/hook/rust/Cargo.toml
bin/hookcheck -service conformance-rust -- pkg/hook/rust/target/release/conformance
```
