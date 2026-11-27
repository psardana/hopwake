use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

/// How many marks may wait to be sent; past it new marks are dropped.
pub(crate) const QUEUE_LIMIT: usize = 10_000;
/// The most spans in one request.
pub(crate) const MAX_BATCH: usize = 512;
/// Queued marks are sent at least this often.
const INTERVAL: Duration = Duration::from_secs(1);

#[derive(Default)]
struct State {
    spans: VecDeque<Value>,
    queued: u64, // marks ever queued
    done: u64,   // marks taken and sent, or given up on
    dropped: u64,
    flush_to: u64,
    closed: bool,
}

struct Shared {
    state: Mutex<State>,
    wake: Condvar,
    sent: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Queues spans and sends them from a background thread.
pub(crate) struct Sender {
    shared: Arc<Shared>,
    running: bool,
}

impl Sender {
    pub(crate) fn start(agent: ureq::Agent, url: String, service: String) -> Sender {
        let shared = Arc::new(Shared { state: Mutex::default(), wake: Condvar::new(), sent: Condvar::new() });
        let worker = shared.clone();
        let running = thread::Builder::new()
            .name("hopwake-hook".into())
            .spawn(move || run(&worker, &agent, &url, &service))
            .is_ok();
        Sender { shared, running }
    }

    /// Whether a mark would be queued now; saves building a span only to drop it.
    pub(crate) fn has_room(&self) -> bool {
        let mut st = self.shared.lock();
        let room = self.running && !st.closed && st.spans.len() < QUEUE_LIMIT;
        if !room {
            st.dropped += 1;
        }
        room
    }

    /// Queues one span, or drops it when the queue is full. Never blocks
    /// beyond a short lock.
    pub(crate) fn push(&self, span: Value) {
        let mut st = self.shared.lock();
        if !self.running || st.closed || st.spans.len() >= QUEUE_LIMIT {
            st.dropped += 1;
            return;
        }
        st.spans.push_back(span);
        st.queued += 1;
        if st.spans.len() == MAX_BATCH {
            self.shared.wake.notify_one();
        }
    }

    /// Sends what is queued now, waiting at most `bound`. Reports whether
    /// all of it went out in time.
    pub(crate) fn flush(&self, bound: Duration) -> bool {
        self.flush_or_close(bound, false)
    }

    /// Flushes, then stops the sender. What is still queued after `bound`
    /// is given up on.
    pub(crate) fn close(&self, bound: Duration) -> bool {
        self.flush_or_close(bound, true)
    }

    fn flush_or_close(&self, bound: Duration, close: bool) -> bool {
        let deadline = Instant::now() + bound;
        let mut st = self.shared.lock();
        if close {
            st.closed = true;
        }
        let target = st.queued;
        st.flush_to = st.flush_to.max(target);
        self.shared.wake.notify_one();
        while self.running && st.done < target {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            st = match self.shared.sent.wait_timeout(st, left) {
                Ok((g, _)) => g,
                Err(e) => e.into_inner().0,
            };
        }
        let ok = st.done >= target;
        if close && !ok {
            st.spans.clear();
        }
        ok
    }

    #[cfg(test)]
    pub(crate) fn counts(&self) -> (u64, u64, usize) {
        let st = self.shared.lock();
        (st.queued, st.dropped, st.spans.len())
    }
}

fn run(shared: &Shared, agent: &ureq::Agent, url: &str, service: &str) {
    loop {
        let batch: Vec<Value> = {
            let mut st = shared.lock();
            if !(st.closed || st.flush_to > st.done || st.spans.len() >= MAX_BATCH) {
                st = match shared.wake.wait_timeout(st, INTERVAL) {
                    Ok((g, _)) => g,
                    Err(e) => e.into_inner().0,
                };
            }
            if st.spans.is_empty() {
                if st.closed {
                    return;
                }
                continue;
            }
            let n = st.spans.len().min(MAX_BATCH);
            st.spans.drain(..n).collect()
        };
        let n = batch.len() as u64;
        // A failed send loses these marks; the service carries on.
        let _ = catch_unwind(AssertUnwindSafe(|| send(agent, url, service, batch)));
        let mut st = shared.lock();
        st.done += n;
        shared.sent.notify_all();
    }
}

fn send(agent: &ureq::Agent, url: &str, service: &str, spans: Vec<Value>) {
    let body = json!({
        "resourceSpans": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": service}}]},
            "scopeSpans": [{"scope": {"name": "hopwake/hook"}, "spans": spans}],
        }],
    });
    let resp = agent
        .post(url)
        .header("Content-Type", "application/json")
        .send(body.to_string().as_bytes());
    if let Ok(mut r) = resp {
        // Read the answer so the connection can be used again.
        let _ = r.body_mut().with_config().limit(64 * 1024).read_to_vec();
    }
}
