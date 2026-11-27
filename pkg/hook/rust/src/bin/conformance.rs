//! Runs the hook conformance script (docs/hooks.md) with the Rust hook.
//! Run it under hookcheck:
//!
//!     hookcheck -service conformance-rust -- target/release/conformance

use std::env;
use std::process::exit;
use std::thread::sleep;
use std::time::{Duration, Instant, UNIX_EPOCH};

use hopwake_hook::{Config, Hook, Op};

fn fail(msg: &str) -> ! {
    eprintln!("conformance: {msg}");
    exit(1);
}

fn main() {
    let mut cfg = Config::from_env(env::var("HOPWAKE_HOP").unwrap_or_default());
    cfg.service = Some("conformance-rust".into());
    let hook = Hook::new(cfg).unwrap_or_else(|e| fail(&e.to_string()));
    let at = UNIX_EPOCH + Duration::new(1_794_996_000, 750_000_000); // 2026-11-18T10:00:00.750Z
    let typ = "ems.measurements.Measurement";

    hook.mark(Op::Store, "dev-00042", 1683);
    hook.mark_boot(Op::Store, "dev-00042", "7", 12);
    hook.mark(Op::Serve, "dev-00042", 1683);
    hook.mark(Op::Serve, "dev-00042", 1683); // the same reading served again: not sent
    hook.mark_time(Op::Store, "EMS0000042", typ, at);
    hook.mark_time_teach(Op::Store, "EMS0000042", "systemId", "100042", typ, at);
    // The alias list arrives shortly after the hook starts.
    let deadline = Instant::now() + Duration::from_secs(15);
    while let Err(e) = hook.mark_time_alias(Op::Serve, "systemId", "100042", typ, at) {
        if Instant::now() > deadline {
            fail(&e.to_string());
        }
        sleep(Duration::from_millis(100));
    }
    if hook.mark_time_alias(Op::Serve, "systemId", "999999", typ, at).is_ok() {
        fail("an unknown alias was marked, want UnknownAlias");
    }
    if !hook.flush() {
        fail("the queued marks were not sent in time");
    }
    drop(hook);
}
