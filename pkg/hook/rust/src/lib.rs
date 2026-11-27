//! Hopwake hook for Rust services: lets a service say that it stored or
//! served a reading, for hops no tap can watch from outside, such as a
//! database write or an HTTP response.
//!
//! ```no_run
//! use hopwake_hook::{Config, Hook, Op};
//!
//! let hook = Hook::new(Config::from_env("hook:lvc.stored")).expect("HOPWAKE_TENANT is set");
//! hook.mark(Op::Store, "dev-00042", 1683);
//! ```
//!
//! Marks are hop events sent as OTLP/JSON spans to Hopwake's collector,
//! keyed by the same journey ID a tap derives. They are queued and sent from
//! a background thread at least once a second, and dropped quietly when
//! Hopwake cannot be reached: the service is never slowed or stopped by its
//! hook. A reading is marked served only the first time, however often it
//! is asked for. The contract every hook follows is in `docs/hooks.md`.

mod aliases;
mod journey;
mod sender;

use std::collections::HashSet;
use std::env;
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

pub use journey::{
    format_alias, format_time, format_unix_nanos, journey_id, parse_duration, time_journey_id,
    ParseDurationError,
};

use aliases::Aliases;
use sender::Sender;

const DEFAULT_ENDPOINT: &str = "http://localhost:4318";
/// A send that takes longer is given up on.
const SEND_TIMEOUT: Duration = Duration::from_secs(3);
/// How long `flush` waits for the queue to go out.
const FLUSH_BOUND: Duration = Duration::from_secs(5);
/// How long dropping a hook waits for the queue to go out.
const CLOSE_BOUND: Duration = Duration::from_secs(4);
/// Past this many served journey IDs the hook forgets them and starts again,
/// at worst marking a reading served twice.
const SERVED_LIMIT: usize = 100_000;

/// What a hook says happened to a reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    /// The service kept the reading.
    Store,
    /// The service handed the reading to someone.
    Serve,
}

impl Op {
    pub fn as_str(self) -> &'static str {
        match self {
            Op::Store => "store",
            Op::Serve => "serve",
        }
    }
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where marks go and what they are called. A setting left `None` (or an
/// empty string) is taken from the environment, then from the default.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// As in the rules file; default `$HOPWAKE_TENANT`. Required.
    pub tenant: Option<String>,
    /// The hop name, matching a hook source in the rules file: `hook:` + match. Required.
    pub hop: String,
    /// Hopwake's OTLP/HTTP address; default `$HOPWAKE_OTLP_ENDPOINT`, else `http://localhost:4318`.
    pub endpoint: Option<String>,
    /// The `service.name` on the spans; default the hop name.
    pub service: Option<String>,
    /// Must match the rules file's `time_precision`; default
    /// `$HOPWAKE_TIME_PRECISION`, else none (the time as given).
    pub time_precision: Option<Duration>,
    /// The journey service's base URL, for [`Hook::mark_time_alias`];
    /// default `$HOPWAKE_API`, else none.
    pub aliases: Option<String>,
}

impl Config {
    /// A config for `hop` with every other setting read from the environment.
    pub fn from_env(hop: impl Into<String>) -> Config {
        Config {
            tenant: env_var("HOPWAKE_TENANT"),
            hop: hop.into(),
            endpoint: env_var("HOPWAKE_OTLP_ENDPOINT"),
            service: None,
            time_precision: env_precision(),
            aliases: env_var("HOPWAKE_API"),
        }
    }
}

fn env_var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.is_empty())
}

// An unreadable precision is ignored, as the Go hook does.
fn env_precision() -> Option<Duration> {
    env_var("HOPWAKE_TIME_PRECISION").and_then(|v| parse_duration(&v).ok())
}

fn given(v: Option<String>) -> Option<String> {
    v.filter(|v| !v.is_empty())
}

/// Why a hook could not be made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// No tenant in the config or in `HOPWAKE_TENANT`.
    MissingTenant,
    /// No hop name.
    MissingHop,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::MissingTenant => f.write_str("hopwake: tenant is required (set HOPWAKE_TENANT)"),
            Error::MissingHop => f.write_str("hopwake: hop is required"),
        }
    }
}

impl std::error::Error for Error {}

/// Hopwake has not learned which device an alias names; nothing was sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownAlias;

impl fmt::Display for UnknownAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("hopwake: unknown alias")
    }
}

impl std::error::Error for UnknownAlias {}

/// Sends marks for one hop. Share it between threads with an `Arc`.
pub struct Hook {
    tenant: String,
    hop: String,
    precision: Duration,
    sender: Sender,
    aliases: Option<Aliases>,
    served: Mutex<HashSet<String>>, // journey IDs already marked served
}

impl Hook {
    /// Starts a hook. It does not contact Hopwake until the first marks are
    /// sent, apart from fetching the alias list when one is configured.
    pub fn new(c: Config) -> Result<Hook, Error> {
        let tenant = given(c.tenant).or_else(|| env_var("HOPWAKE_TENANT")).ok_or(Error::MissingTenant)?;
        if c.hop.is_empty() {
            return Err(Error::MissingHop);
        }
        let endpoint = given(c.endpoint)
            .or_else(|| env_var("HOPWAKE_OTLP_ENDPOINT"))
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
        let service = given(c.service).unwrap_or_else(|| c.hop.clone());
        let precision = c.time_precision.or_else(env_precision).unwrap_or(Duration::ZERO);
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(SEND_TIMEOUT))
            .http_status_as_error(false)
            .build()
            .into();
        let url = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
        let aliases = given(c.aliases)
            .or_else(|| env_var("HOPWAKE_API"))
            .map(|base| Aliases::follow(agent.clone(), &base));
        Ok(Hook {
            tenant,
            hop: c.hop,
            precision,
            sender: Sender::start(agent, url, service),
            aliases,
            served: Mutex::new(HashSet::new()),
        })
    }

    /// Marks one reading with a sequence number, now.
    pub fn mark(&self, op: Op, device: &str, seq: u64) {
        self.mark_boot(op, device, "", seq);
    }

    /// [`mark`](Hook::mark) for sources with a boot counter or boot ID.
    pub fn mark_boot(&self, op: Op, device: &str, boot: &str, seq: u64) {
        let id = journey_id(&self.tenant, device, seq, boot);
        self.record(op, id, device, seq, "", "");
    }

    /// Marks a reading that has no sequence number: it is known by its
    /// device, message type and the time the device gave it, exactly as a
    /// tap finds it.
    pub fn mark_time(&self, op: Op, device: &str, message_type: &str, at: SystemTime) {
        self.record_time(op, device, "", message_type, at);
    }

    /// [`mark_time`](Hook::mark_time) at a hop that knows both the device
    /// and another name it goes by further down, such as the system ID a
    /// store keys it by. Hopwake learns the pair from this mark.
    pub fn mark_time_teach(
        &self,
        op: Op,
        device: &str,
        alias_kind: &str,
        alias_value: &str,
        message_type: &str,
        at: SystemTime,
    ) {
        self.record_time(op, device, &format_alias(alias_kind, alias_value), message_type, at);
    }

    /// [`mark_time`](Hook::mark_time) for a hop that knows the device only
    /// by an alias. When Hopwake has not learned the alias yet, nothing is
    /// sent and [`UnknownAlias`] is returned.
    pub fn mark_time_alias(
        &self,
        op: Op,
        alias_kind: &str,
        alias_value: &str,
        message_type: &str,
        at: SystemTime,
    ) -> Result<(), UnknownAlias> {
        let device = self.aliases.as_ref().and_then(|a| a.device(alias_kind, alias_value)).ok_or(UnknownAlias)?;
        self.record_time(op, &device, &format_alias(alias_kind, alias_value), message_type, at);
        Ok(())
    }

    /// Sends what is queued, waiting at most a few seconds. Reports whether
    /// everything queued went out in time (a failed send counts as gone).
    pub fn flush(&self) -> bool {
        self.sender.flush(FLUSH_BOUND)
    }

    fn record_time(&self, op: Op, device: &str, alias: &str, message_type: &str, at: SystemTime) {
        let formatted = format_time(at, self.precision);
        let id = time_journey_id(&self.tenant, device, message_type, &formatted);
        self.record(op, id, device, 0, message_type, alias);
    }

    fn record(&self, op: Op, id: String, device: &str, seq: u64, message_type: &str, alias: &str) {
        if !self.first_serve(op, &id) || !self.sender.has_room() {
            return;
        }
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0).to_string();
        let s = |k: &str, v: &str| json!({"key": k, "value": {"stringValue": v}});
        let i = |k: &str, v: i64| json!({"key": k, "value": {"intValue": v.to_string()}});
        let mut attrs = vec![
            s("hopwake.schema", "hop/v1"),
            s("hopwake.tenant", &self.tenant),
            s("hopwake.device.id", device),
            // A signed 64-bit integer on the wire, as the Go hook sends it.
            i("hopwake.seq", seq as i64),
            s("hopwake.journey.id", &id),
            s("hopwake.hop", &self.hop),
            s("hopwake.op", op.as_str()),
            s("hopwake.correlation", "derived"),
            i("hopwake.payload.bytes", 0),
            s("messaging.system", "hook"),
            s("messaging.destination.name", ""),
        ];
        if !message_type.is_empty() {
            attrs.push(s("hopwake.message.type", message_type));
        }
        if !alias.is_empty() {
            attrs.push(s("hopwake.device.alias", alias));
        }
        let span: Value = json!({
            "traceId": id,
            "spanId": span_id(),
            "name": format!("hop {}", self.hop),
            "kind": 5, // consumer
            "startTimeUnixNano": now,
            "endTimeUnixNano": now,
            "attributes": attrs,
        });
        self.sender.push(span);
    }

    // Whether a mark should be sent: serve marks only the first time per journey.
    fn first_serve(&self, op: Op, id: &str) -> bool {
        if op != Op::Serve {
            return true;
        }
        let mut served = self.served.lock().unwrap_or_else(|e| e.into_inner());
        if served.contains(id) {
            return false;
        }
        if served.len() >= SERVED_LIMIT {
            served.clear();
        }
        served.insert(id.to_string());
        true
    }
}

impl Drop for Hook {
    /// Sends what is queued, waiting at most a few seconds.
    fn drop(&mut self) {
        if let Some(a) = &self.aliases {
            a.stop();
        }
        self.sender.close(CLOSE_BOUND);
    }
}

impl fmt::Debug for Hook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hook").field("tenant", &self.tenant).field("hop", &self.hop).finish_non_exhaustive()
    }
}

// 8 random bytes, not all zero, in hex.
fn span_id() -> String {
    let mut b = [0u8; 8];
    for _ in 0..4 {
        if getrandom::fill(&mut b).is_ok() && b != [0; 8] {
            return journey::hex(&b);
        }
    }
    // No randomness to be had: fall back on the clock.
    let n = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(1);
    journey::hex(&(n | 1).to_be_bytes())
}

#[cfg(test)]
mod tests;
