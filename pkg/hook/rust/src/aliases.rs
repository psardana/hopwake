use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use serde_json::Value;

use crate::journey::format_alias;

/// How often the alias list is fetched again.
const EVERY: Duration = Duration::from_secs(5);

struct Shared {
    pairs: RwLock<HashMap<String, String>>, // kind:value → device
    stop: Mutex<bool>,
    wake: Condvar,
}

/// A copy of the alias pairs Hopwake learned, refreshed in the background.
pub(crate) struct Aliases {
    shared: Arc<Shared>,
}

impl Aliases {
    /// Follows `{base}/api/aliases` until stopped.
    pub(crate) fn follow(agent: ureq::Agent, base: &str) -> Aliases {
        let shared = Arc::new(Shared { pairs: RwLock::default(), stop: Mutex::new(false), wake: Condvar::new() });
        let url = format!("{}/api/aliases", base.trim_end_matches('/'));
        let worker = shared.clone();
        let _ = thread::Builder::new()
            .name("hopwake-aliases".into())
            .spawn(move || run(&worker, &agent, &url));
        Aliases { shared }
    }

    pub(crate) fn device(&self, kind: &str, value: &str) -> Option<String> {
        let pairs = self.shared.pairs.read().unwrap_or_else(|e| e.into_inner());
        pairs.get(&format_alias(kind, value)).cloned()
    }

    pub(crate) fn stop(&self) {
        *self.shared.stop.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.shared.wake.notify_all();
    }
}

fn run(shared: &Shared, agent: &ureq::Agent, url: &str) {
    loop {
        // A failed fetch keeps the copy we have.
        if let Some(list) = fetch(agent, url) {
            *shared.pairs.write().unwrap_or_else(|e| e.into_inner()) = list;
        }
        let stop = shared.stop.lock().unwrap_or_else(|e| e.into_inner());
        if *stop {
            return;
        }
        let (stop, _) = match shared.wake.wait_timeout(stop, EVERY) {
            Ok(r) => r,
            Err(e) => e.into_inner(),
        };
        if *stop {
            return;
        }
    }
}

fn fetch(agent: &ureq::Agent, url: &str) -> Option<HashMap<String, String>> {
    let mut resp = agent.get(url).call().ok()?;
    if resp.status() != 200 {
        return None;
    }
    let text = resp.body_mut().read_to_string().ok()?;
    let list: Value = serde_json::from_str(&text).ok()?;
    let mut pairs = HashMap::new();
    for p in list.as_array()? {
        let get = |k: &str| p.get(k).and_then(Value::as_str).unwrap_or("");
        pairs.insert(format_alias(get("kind"), get("value")), get("device").to_string());
    }
    Some(pairs)
}
