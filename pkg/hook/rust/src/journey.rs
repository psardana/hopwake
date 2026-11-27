use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

/// The journey ID of a reading with a sequence number, as Hopwake derives it
/// (32 lowercase hex characters). An empty `boot` means the source has none.
pub fn journey_id(tenant: &str, device: &str, seq: u64, boot: &str) -> String {
    let mut h = Sha256::new();
    field(&mut h, "hopwake/journey/v1");
    field(&mut h, tenant);
    field(&mut h, device);
    if !boot.is_empty() {
        field(&mut h, &format!("boot:{boot}"));
    }
    h.update(seq.to_be_bytes());
    finish(h)
}

/// The journey ID of a reading known by its device, message type and its
/// own time, already written by [`format_time`].
pub fn time_journey_id(tenant: &str, device: &str, message_type: &str, formatted: &str) -> String {
    let mut h = Sha256::new();
    field(&mut h, "hopwake/journey/time/v1");
    field(&mut h, tenant);
    field(&mut h, device);
    field(&mut h, &format!("type:{message_type}"));
    field(&mut h, &format!("time:{formatted}"));
    finish(h)
}

/// An alias as it travels on hop events: `kind:value`.
pub fn format_alias(kind: &str, value: &str) -> String {
    format!("{kind}:{value}")
}

/// Writes a reading's time as journey IDs use it: rounded down to the
/// precision (zero keeps it as given), in UTC, with up to nine fraction
/// digits and no trailing zeros.
pub fn format_time(at: SystemTime, precision: Duration) -> String {
    format_unix_nanos(unix_nanos(at), precision)
}

/// [`format_time`] for a time given in nanoseconds since the Unix epoch.
pub fn format_unix_nanos(nanos: i128, precision: Duration) -> String {
    let p = precision.as_nanos() as i128;
    let t = if p > 0 { nanos - nanos.rem_euclid(p) } else { nanos };
    let secs = t.div_euclid(1_000_000_000);
    let frac = t.rem_euclid(1_000_000_000) as u32;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400) as u32;
    let (y, m, d) = civil_from_days(days as i64);
    let mut s = format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
        sod / 3600,
        sod / 60 % 60,
        sod % 60
    );
    if frac > 0 {
        let f = format!("{frac:09}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
    s.push('Z');
    s
}

pub(crate) fn unix_nanos(at: SystemTime) -> i128 {
    match at.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i128,
        Err(e) => -(e.duration().as_nanos() as i128),
    }
}

fn field(h: &mut Sha256, s: &str) {
    h.update((s.len() as u32).to_be_bytes());
    h.update(s.as_bytes());
}

fn finish(h: Sha256) -> String {
    let sum = h.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&sum[..16]);
    if id.iter().all(|&b| b == 0) {
        id[15] = 1;
    }
    hex(&id)
}

pub(crate) fn hex(b: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push(DIGITS[(x >> 4) as usize] as char);
        s.push(DIGITS[(x & 15) as usize] as char);
    }
    s
}

// Days since 1970-01-01 to a proleptic Gregorian date.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// A duration that could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseDurationError(String);

impl fmt::Display for ParseDurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "hopwake: invalid duration {:?}", self.0)
    }
}

impl std::error::Error for ParseDurationError {}

/// Reads a duration as the rules file and `HOPWAKE_TIME_PRECISION` write
/// it: `1s`, `500ms`, `1m`, `1h`, `250us`, `100ns`. Fractions (`1.5s`) and
/// sums (`1m30s`) are read too.
pub fn parse_duration(s: &str) -> Result<Duration, ParseDurationError> {
    let err = || ParseDurationError(s.to_string());
    let text = s.trim();
    if text == "0" {
        return Ok(Duration::ZERO);
    }
    if text.is_empty() {
        return Err(err());
    }
    let mut total: u128 = 0;
    let mut rest = text;
    while !rest.is_empty() {
        let n = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
        let whole = &rest[..n];
        rest = &rest[n..];
        let mut frac = "";
        if let Some(r) = rest.strip_prefix('.') {
            let n = r.find(|c: char| !c.is_ascii_digit()).unwrap_or(r.len());
            frac = &r[..n];
            rest = &r[n..];
        }
        if whole.is_empty() && frac.is_empty() {
            return Err(err());
        }
        let n = rest.find(|c: char| c.is_ascii_digit() || c == '.').unwrap_or(rest.len());
        let unit: u128 = match &rest[..n] {
            "ns" => 1,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(err()),
        };
        rest = &rest[n..];
        let w: u128 = if whole.is_empty() { 0 } else { whole.parse().map_err(|_| err())? };
        total = w.checked_mul(unit).and_then(|v| total.checked_add(v)).ok_or_else(err)?;
        if !frac.is_empty() {
            let digits = &frac[..frac.len().min(18)];
            let f: u128 = digits.parse().map_err(|_| err())?;
            total += f * unit / 10u128.pow(digits.len() as u32);
        }
    }
    let secs = u64::try_from(total / 1_000_000_000).map_err(|_| err())?;
    Ok(Duration::new(secs, (total % 1_000_000_000) as u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        let ms = Duration::from_millis;
        assert_eq!(parse_duration("1s"), Ok(ms(1000)));
        assert_eq!(parse_duration("500ms"), Ok(ms(500)));
        assert_eq!(parse_duration("1m"), Ok(ms(60_000)));
        assert_eq!(parse_duration("1h"), Ok(ms(3_600_000)));
        assert_eq!(parse_duration("250us"), Ok(Duration::from_micros(250)));
        assert_eq!(parse_duration("100ns"), Ok(Duration::from_nanos(100)));
        assert_eq!(parse_duration("1.5s"), Ok(ms(1500)));
        assert_eq!(parse_duration("1m30s"), Ok(ms(90_000)));
        assert_eq!(parse_duration("0"), Ok(Duration::ZERO));
        for bad in ["", "s", "10", "1d", "-1s", "1.s.", "ms5"] {
            assert!(parse_duration(bad).is_err(), "{bad:?} was accepted");
        }
    }

    #[test]
    fn times_before_the_epoch() {
        assert_eq!(format_unix_nanos(-1, Duration::ZERO), "1969-12-31T23:59:59.999999999Z");
        assert_eq!(format_unix_nanos(-1, Duration::from_secs(1)), "1969-12-31T23:59:59Z");
        assert_eq!(format_unix_nanos(951_782_400_000_000_000, Duration::ZERO), "2000-02-29T00:00:00Z");
    }
}
