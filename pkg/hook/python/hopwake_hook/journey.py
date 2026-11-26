"""Journey IDs, times and aliases, exactly as docs/hooks.md defines them."""

import datetime as _dt
import hashlib
import re
import struct
from typing import Union

_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
_SECOND = 1_000_000_000
_MAX_SEQ = (1 << 64) - 1

Time = Union[_dt.datetime, int]


def _field(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack(">I", len(b)) + b


def _finish(h) -> str:
    id_ = bytearray(h.digest()[:16])
    if not any(id_):
        id_[15] = 1
    return id_.hex()


def journey_id(tenant: str, device: str, seq: int, boot: str = "") -> str:
    """The journey ID of a reading with a sequence number (and a boot, if any)."""
    seq = int(seq)
    if seq < 0 or seq > _MAX_SEQ:
        raise ValueError("seq must fit in 64 unsigned bits")
    h = hashlib.sha256()
    h.update(_field("hopwake/journey/v1"))
    h.update(_field(tenant))
    h.update(_field(device))
    if boot:
        h.update(_field("boot:" + boot))
    h.update(struct.pack(">Q", seq))
    return _finish(h)


def time_journey_id(tenant: str, device: str, message_type: str, formatted_time: str) -> str:
    """The journey ID of a reading known by its type and time (see format_time)."""
    h = hashlib.sha256()
    h.update(_field("hopwake/journey/time/v1"))
    h.update(_field(tenant))
    h.update(_field(device))
    h.update(_field("type:" + message_type))
    h.update(_field("time:" + formatted_time))
    return _finish(h)


def unix_nanos(at: Time) -> int:
    """Nanoseconds since the Unix epoch. A naive datetime is taken as UTC."""
    if isinstance(at, bool):
        raise TypeError("a time is a datetime or Unix nanoseconds")
    if isinstance(at, int):
        return at
    if isinstance(at, _dt.datetime):
        if at.tzinfo is None or at.tzinfo.utcoffset(at) is None:
            at = at.replace(tzinfo=_dt.timezone.utc)
        d = at - _EPOCH
        return (d.days * 86_400 + d.seconds) * _SECOND + d.microseconds * 1_000
    raise TypeError("a time is a datetime or Unix nanoseconds")


def format_time(at: Time, precision_ns: int = 0) -> str:
    """The time as it enters a journey ID: rounded down to the precision,
    counted from the Unix epoch, then written in UTC with a fraction of up
    to nine digits and no trailing zeros."""
    ns = unix_nanos(at)
    precision_ns = int(precision_ns or 0)
    if precision_ns > 0:
        ns -= ns % precision_ns
    secs, frac = divmod(ns, _SECOND)
    t = _EPOCH + _dt.timedelta(seconds=secs)
    out = "%04d-%02d-%02dT%02d:%02d:%02d" % (t.year, t.month, t.day, t.hour, t.minute, t.second)
    if frac:
        out += "." + ("%09d" % frac).rstrip("0")
    return out + "Z"


def format_alias(kind: str, value: str) -> str:
    """An alias as it travels on hop events: kind:value."""
    return kind + ":" + value


_UNITS = {
    "ns": 1,
    "us": 1_000,
    "µs": 1_000,  # micro sign
    "μs": 1_000,  # Greek mu
    "ms": 1_000_000,
    "s": _SECOND,
    "m": 60 * _SECOND,
    "h": 3600 * _SECOND,
}
_PART = re.compile(r"(\d*)(?:\.(\d*))?([^\d.]*)")


def parse_duration(s: str) -> int:
    """Reads a duration such as "1s", "500ms", "1m" or "1h30m", as Go writes
    them, and returns it in nanoseconds."""
    orig = s
    if not isinstance(s, str) or not s:
        raise ValueError("invalid duration %r" % (orig,))
    sign = 1
    if s[0] in "+-":
        sign = -1 if s[0] == "-" else 1
        s = s[1:]
    if s == "0":
        return 0
    if not s:
        raise ValueError("invalid duration %r" % (orig,))
    total = 0
    pos = 0
    while pos < len(s):
        m = _PART.match(s, pos)
        whole, frac, unit = m.group(1), m.group(2), m.group(3)
        if not (whole or frac) or unit not in _UNITS:
            raise ValueError("invalid duration %r" % (orig,))
        n = _UNITS[unit]
        total += int(whole or "0") * n
        if frac:
            total += int(frac) * n // 10 ** len(frac)
        pos = m.end()
    return sign * total
