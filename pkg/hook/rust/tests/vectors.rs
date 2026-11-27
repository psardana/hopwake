//! The shared test vectors every hook must reproduce (pkg/hook/testdata).

use std::time::{Duration, UNIX_EPOCH};

use hopwake_hook::{format_alias, format_time, format_unix_nanos, journey_id, time_journey_id};
use serde_json::Value;

fn vectors() -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../testdata/vectors.json");
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    serde_json::from_str(&text).unwrap()
}

fn list<'a>(v: &'a Value, key: &str) -> &'a Vec<Value> {
    let l = v[key].as_array().unwrap();
    assert!(!l.is_empty(), "no {key} vectors");
    l
}

fn text<'a>(v: &'a Value, key: &str) -> &'a str {
    v[key].as_str().unwrap_or_else(|| panic!("{key} missing in {v}"))
}

#[test]
fn seq_vectors() {
    for v in list(&vectors(), "seq") {
        let seq: u64 = text(v, "seq").parse().unwrap();
        let got = journey_id(text(v, "tenant"), text(v, "device"), seq, text(v, "boot"));
        assert_eq!(got, text(v, "id"), "{v}");
    }
}

#[test]
fn time_vectors() {
    for v in list(&vectors(), "time") {
        let nanos: i128 = text(v, "unix_nanos").parse().unwrap();
        let precision = Duration::from_nanos(text(v, "precision_ns").parse().unwrap());
        let formatted = format_unix_nanos(nanos, precision);
        assert_eq!(formatted, text(v, "formatted"), "{v}");
        let at = UNIX_EPOCH + Duration::from_nanos(nanos as u64);
        assert_eq!(format_time(at, precision), formatted, "{v}");
        let id = time_journey_id(text(v, "tenant"), text(v, "device"), text(v, "type"), &formatted);
        assert_eq!(id, text(v, "id"), "{v}");
    }
}

#[test]
fn alias_vectors() {
    for v in list(&vectors(), "alias") {
        assert_eq!(format_alias(text(v, "kind"), text(v, "value")), text(v, "formatted"), "{v}");
    }
}

#[test]
fn time_table_in_the_contract() {
    let at = |ms: u64, ns: u32| UNIX_EPOCH + Duration::new(1_794_996_000 + ms / 1000, (ms % 1000) as u32 * 1_000_000 + ns);
    let none = Duration::ZERO;
    assert_eq!(format_time(at(0, 0), none), "2026-11-18T10:00:00Z");
    assert_eq!(format_time(at(500, 0), none), "2026-11-18T10:00:00.5Z");
    assert_eq!(format_time(at(120, 0), none), "2026-11-18T10:00:00.12Z");
    assert_eq!(format_time(at(123, 456_789), none), "2026-11-18T10:00:00.123456789Z");
}
