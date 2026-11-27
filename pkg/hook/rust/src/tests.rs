use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use serde_json::Value;

use super::*;
use crate::sender::{MAX_BATCH, QUEUE_LIMIT};

/// A stand-in for Hopwake: takes trace exports and serves one alias pair.
struct Server {
    base: String,
    bodies: Arc<Mutex<Vec<String>>>,
}

impl Server {
    fn start() -> Server {
        let ln = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", ln.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let b = bodies.clone();
        thread::spawn(move || {
            for conn in ln.incoming().flatten() {
                let b = b.clone();
                thread::spawn(move || serve(conn, &b));
            }
        });
        Server { base, bodies }
    }

    fn spans(&self) -> Vec<Value> {
        let mut out = Vec::new();
        for body in self.bodies.lock().unwrap().iter() {
            let v: Value = serde_json::from_str(body).unwrap();
            for rs in v["resourceSpans"].as_array().unwrap() {
                assert_eq!(rs["resource"]["attributes"][0]["key"], "service.name");
                for ss in rs["scopeSpans"].as_array().unwrap() {
                    out.extend(ss["spans"].as_array().unwrap().iter().cloned());
                }
            }
        }
        out
    }
}

fn serve(conn: TcpStream, bodies: &Mutex<Vec<String>>) {
    let mut w = conn.try_clone().unwrap();
    let mut r = BufReader::new(conn);
    loop {
        let mut line = String::new();
        if r.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        let mut len = 0;
        loop {
            let mut h = String::new();
            if r.read_line(&mut h).unwrap_or(0) == 0 {
                return;
            }
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some((k, v)) = h.split_once(':') {
                if k.eq_ignore_ascii_case("content-length") {
                    len = v.trim().parse().unwrap();
                }
            }
        }
        let mut body = vec![0; len];
        r.read_exact(&mut body).unwrap();
        let answer = if line.starts_with("GET /api/aliases") {
            r#"[{"kind":"systemId","value":"100042","device":"EMS0000042"}]"#
        } else {
            bodies.lock().unwrap().push(String::from_utf8(body).unwrap());
            "{}"
        };
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{answer}",
            answer.len()
        );
        if w.write_all(resp.as_bytes()).is_err() {
            return;
        }
    }
}

fn config(endpoint: &str) -> Config {
    Config {
        tenant: Some("test".into()),
        hop: "hook:test.stored".into(),
        endpoint: Some(endpoint.into()),
        service: Some("hook-test".into()),
        time_precision: Some(Duration::from_secs(1)),
        aliases: None,
    }
}

fn attr<'a>(span: &'a Value, key: &str) -> &'a Value {
    let attrs = span["attributes"].as_array().unwrap();
    attrs.iter().find(|a| a["key"] == key).map(|a| &a["value"]).unwrap_or(&Value::Null)
}

#[test]
fn served_once_on_the_wire() {
    let srv = Server::start();
    let hook = Hook::new(config(&srv.base)).unwrap();
    hook.mark(Op::Store, "dev-00042", 1683);
    hook.mark(Op::Store, "dev-00042", 1683);
    for _ in 0..3 {
        hook.mark(Op::Serve, "dev-00042", 1683);
    }
    hook.mark(Op::Serve, "dev-00042", 1684);
    assert!(hook.flush());
    let spans = srv.spans();
    let ops: Vec<_> = spans.iter().map(|s| attr(s, "hopwake.op")["stringValue"].as_str().unwrap()).collect();
    assert_eq!(ops, ["store", "store", "serve", "serve"]);

    let s = &spans[0];
    let id = journey_id("test", "dev-00042", 1683, "");
    assert_eq!(s["traceId"], id.as_str());
    assert_eq!(s["spanId"].as_str().unwrap().len(), 16);
    assert_eq!(s["name"], "hop hook:test.stored");
    assert_eq!(s["kind"], 5);
    assert_eq!(s["startTimeUnixNano"], s["endTimeUnixNano"]);
    assert_eq!(attr(s, "hopwake.seq")["intValue"], "1683");
    assert_eq!(attr(s, "hopwake.journey.id")["stringValue"], id.as_str());
    assert!(attr(s, "hopwake.message.type").is_null());
}

#[test]
fn served_ids_are_bounded() {
    let hook = Hook::new(config("http://127.0.0.1:1")).unwrap();
    assert!(hook.first_serve(Op::Serve, "first"));
    for i in 1..SERVED_LIMIT {
        assert!(hook.first_serve(Op::Serve, &i.to_string()));
    }
    assert!(!hook.first_serve(Op::Serve, "first"));
    assert!(hook.first_serve(Op::Store, "first"));
    // Past the bound the hook forgets and starts again.
    assert!(hook.first_serve(Op::Serve, "one more"));
    assert!(hook.first_serve(Op::Serve, "first"));
}

#[test]
fn unknown_alias_sends_nothing() {
    let srv = Server::start();
    let mut c = config(&srv.base);
    c.aliases = Some(srv.base.clone());
    let hook = Hook::new(c).unwrap();
    let at = SystemTime::UNIX_EPOCH + Duration::new(1_794_996_000, 750_000_000);
    let typ = "ems.measurements.Measurement";
    let deadline = Instant::now() + Duration::from_secs(10);
    while hook.mark_time_alias(Op::Serve, "systemId", "100042", typ, at).is_err() {
        assert!(Instant::now() < deadline, "the alias list never arrived");
        thread::sleep(Duration::from_millis(20));
    }
    let before = hook.sender.counts().0;
    assert_eq!(hook.mark_time_alias(Op::Serve, "systemId", "999999", typ, at), Err(UnknownAlias));
    assert_eq!(hook.sender.counts().0, before);
    assert!(hook.flush());

    let spans = srv.spans();
    assert_eq!(spans.len(), 1);
    let id = time_journey_id("test", "EMS0000042", typ, "2026-11-18T10:00:00Z");
    assert_eq!(spans[0]["traceId"], id.as_str());
    assert_eq!(attr(&spans[0], "hopwake.device.id")["stringValue"], "EMS0000042");
    assert_eq!(attr(&spans[0], "hopwake.device.alias")["stringValue"], "systemId:100042");
    assert_eq!(attr(&spans[0], "hopwake.message.type")["stringValue"], typ);
    assert_eq!(attr(&spans[0], "hopwake.seq")["intValue"], "0");
}

#[test]
fn no_alias_list_means_unknown() {
    let mut c = config("http://127.0.0.1:1");
    c.aliases = Some("http://127.0.0.1:1".into());
    let hook = Hook::new(c).unwrap();
    let r = hook.mark_time_alias(Op::Serve, "systemId", "100042", "", SystemTime::now());
    assert_eq!(r, Err(UnknownAlias));
    assert_eq!(hook.sender.counts().0, 0);
}

#[test]
fn full_queue_drops_instead_of_blocking() {
    // Accepts connections but never answers: every send hangs until its timeout.
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let hook = Hook::new(config(&format!("http://{}", ln.local_addr().unwrap()))).unwrap();
    let n = 3 * QUEUE_LIMIT as u64;
    // A mark that waited on the hung send would take its 3 s timeout.
    let mut slowest = Duration::ZERO;
    for seq in 0..n {
        let t = Instant::now();
        hook.mark(Op::Store, "dev-00042", seq);
        slowest = slowest.max(t.elapsed());
    }
    assert!(slowest < Duration::from_secs(1), "the slowest mark took {slowest:?}");
    let (queued, dropped, waiting) = hook.sender.counts();
    assert!(waiting <= QUEUE_LIMIT);
    assert_eq!(queued + dropped, n);
    // At most a few batches left the queue while the sender hung.
    assert!(queued <= (QUEUE_LIMIT + 4 * MAX_BATCH) as u64, "{queued} queued");

    let start = Instant::now();
    drop(hook);
    assert!(start.elapsed() < CLOSE_BOUND + Duration::from_secs(1), "closing took {:?}", start.elapsed());
    drop(ln);
}

#[test]
fn unreachable_endpoint_returns_at_once() {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let hook = Hook::new(config(&format!("http://127.0.0.1:{port}"))).unwrap();
    let mut slowest = Duration::ZERO;
    for seq in 0..1000 {
        let t = Instant::now();
        hook.mark(Op::Store, "dev-00042", seq);
        hook.mark_time(Op::Serve, "dev-00042", "t", SystemTime::now());
        slowest = slowest.max(t.elapsed());
    }
    assert!(slowest < Duration::from_secs(1), "the slowest mark took {slowest:?}");
    // Refused sends are given up on at once, so flushing does not wait out its bound.
    let start = Instant::now();
    assert!(hook.flush());
    drop(hook);
    assert!(start.elapsed() < Duration::from_secs(3), "flush and close took {:?}", start.elapsed());
}

#[test]
fn tenant_and_hop_are_required() {
    let mut c = config("http://127.0.0.1:1");
    c.hop = String::new();
    assert_eq!(Hook::new(c).unwrap_err(), Error::MissingHop);
}

#[test]
fn hook_can_be_shared_between_threads() {
    fn shareable<T: Send + Sync>() {}
    shareable::<Hook>();
}
