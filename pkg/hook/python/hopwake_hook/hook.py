"""The hook: marks readings at one hop and sends them to Hopwake."""

import datetime as _dt
import json
import logging
import os
import queue
import threading
import time
import urllib.request
from typing import Dict, List, Optional, Union

from .journey import Time, format_alias, format_time, journey_id, parse_duration, time_journey_id, unix_nanos

STORE = "store"
SERVE = "serve"

_log = logging.getLogger("hopwake_hook")

QUEUE_SIZE = 65_536
SERVED_LIMIT = 100_000
SEND_TIMEOUT = 3.0
SEND_EVERY = 1.0
ALIASES_EVERY = 5.0
_BATCH = 1_000
_MAX_SEQ = (1 << 64) - 1


def _opener():
    # Proxies from the environment only, as Go does.
    return urllib.request.build_opener(urllib.request.ProxyHandler(urllib.request.getproxies_environment()))


def _precision_ns(p) -> int:
    if p is None or p == "":
        return 0
    if isinstance(p, _dt.timedelta):
        return (p.days * 86_400 + p.seconds) * 1_000_000_000 + p.microseconds * 1_000
    if isinstance(p, str):
        return parse_duration(p)
    if isinstance(p, int) and not isinstance(p, bool):
        return p
    raise TypeError("time_precision is a duration string, a timedelta or nanoseconds")


class _Aliases:
    """A copy of the alias list, refreshed from {base}/api/aliases."""

    def __init__(self, base: str, opener, stop: threading.Event):
        self.url = base.rstrip("/") + "/api/aliases"
        self._opener = opener
        self._stop = stop
        self._pairs: Dict[str, str] = {}
        t = threading.Thread(target=self._follow, name="hopwake-aliases", daemon=True)
        t.start()

    def device(self, kind: str, value: str) -> Optional[str]:
        return self._pairs.get(format_alias(kind, value))

    def _follow(self):
        while not self._stop.is_set():
            self.fetch()
            self._stop.wait(ALIASES_EVERY)

    def fetch(self) -> bool:
        try:
            with self._opener.open(self.url, timeout=SEND_TIMEOUT) as resp:
                if resp.status != 200:
                    return False
                data = json.load(resp)
            pairs = {}
            for p in data:
                kind, value, device = p["kind"], p["value"], p["device"]
                if isinstance(kind, str) and isinstance(value, str) and isinstance(device, str):
                    pairs[format_alias(kind, value)] = device
        except Exception:
            return False  # a failed fetch keeps the copy we have
        self._pairs = pairs
        return True


class Hook:
    """Marks readings at one hop.

    Marks return at once; a background thread sends them in batches. When
    Hopwake cannot be reached they are dropped quietly.
    """

    def __init__(
        self,
        tenant: Optional[str] = None,
        hop: Optional[str] = None,
        endpoint: Optional[str] = None,
        service: Optional[str] = None,
        time_precision: Union[str, int, _dt.timedelta, None] = None,
        aliases: Optional[str] = None,
        *,
        queue_size: int = QUEUE_SIZE,
    ):
        tenant = tenant or os.environ.get("HOPWAKE_TENANT", "")
        if not tenant or not hop:
            raise ValueError("hopwake hook: tenant and hop are required")
        self.tenant = tenant
        self.hop = hop
        self.endpoint = (endpoint or os.environ.get("HOPWAKE_OTLP_ENDPOINT") or "http://localhost:4318").rstrip("/")
        self.service = service or hop
        if time_precision is None:
            try:
                self.time_precision_ns = parse_duration(os.environ.get("HOPWAKE_TIME_PRECISION", ""))
            except ValueError:
                self.time_precision_ns = 0
        else:
            self.time_precision_ns = _precision_ns(time_precision)
        self.aliases_url = aliases or os.environ.get("HOPWAKE_API") or None

        self._opener = _opener()
        self._queue: "queue.Queue[dict]" = queue.Queue(maxsize=queue_size)
        self._accepted = 0
        self._dropped = 0
        self._count_lock = threading.Lock()
        self._served = set()
        self._served_lock = threading.Lock()
        self._closed = False
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._waiters: List[threading.Event] = []
        self._waiters_lock = threading.Lock()
        self._aliases = _Aliases(self.aliases_url, self._opener, self._stop) if self.aliases_url else None
        self._sender = threading.Thread(target=self._run, name="hopwake-hook", daemon=True)
        self._sender.start()

    # -- marks

    def mark(self, op: str, device: str, seq: int) -> None:
        """Marks a reading known by its sequence number."""
        self.mark_boot(op, device, "", seq)

    def mark_boot(self, op: str, device: str, boot: str, seq: int) -> None:
        """mark for sources with a boot counter."""
        try:
            seq = int(seq)
            if seq < 0 or seq > _MAX_SEQ:
                raise ValueError("seq out of range")
            id_ = journey_id(self.tenant, device, seq, boot or "")
        except Exception as e:
            _log.warning("hopwake hook: mark dropped: %s", e)
            return
        self._record(op, id_, device, seq, "", "")

    def mark_time(self, op: str, device: str, message_type: str, at: Time) -> None:
        """Marks a reading known by device, message type and its own time."""
        self._mark_time(op, device, "", message_type, at)

    def mark_time_teach(self, op: str, device: str, alias_kind: str, alias_value: str, message_type: str, at: Time) -> None:
        """mark_time at a hop that knows both the device and an alias of it."""
        self._mark_time(op, device, format_alias(alias_kind, alias_value), message_type, at)

    def mark_time_alias(self, op: str, alias_kind: str, alias_value: str, message_type: str, at: Time) -> bool:
        """mark_time for a hop that knows the device only by an alias. Returns
        False, and sends nothing, when the alias is not known (yet)."""
        if self._aliases is None:
            return False
        device = self._aliases.device(alias_kind, alias_value)
        if device is None:
            return False
        return self._mark_time(op, device, format_alias(alias_kind, alias_value), message_type, at)

    def _mark_time(self, op, device, alias, message_type, at) -> bool:
        try:
            message_type = message_type or ""
            t = format_time(unix_nanos(at), self.time_precision_ns)
            id_ = time_journey_id(self.tenant, device, message_type, t)
        except Exception as e:
            _log.warning("hopwake hook: mark dropped: %s", e)
            return False
        self._record(op, id_, device, 0, message_type, alias)
        return True

    def _first_serve(self, op: str, id_: str) -> bool:
        if op != SERVE:
            return True
        with self._served_lock:
            if id_ in self._served:
                return False
            if len(self._served) >= SERVED_LIMIT:
                self._served = set()
            self._served.add(id_)
            return True

    def _record(self, op, id_, device, seq, message_type, alias) -> None:
        if op not in (STORE, SERVE):
            _log.warning("hopwake hook: mark dropped: op %r is neither store nor serve", op)
            return
        if self._closed or not self._first_serve(op, id_):
            return
        now = str(time.time_ns())
        attrs = [
            _s("hopwake.schema", "hop/v1"),
            _s("hopwake.tenant", self.tenant),
            _s("hopwake.device.id", device),
            # OTLP integers are signed 64-bit; the largest sequence numbers wrap, as in Go.
            {"key": "hopwake.seq", "value": {"intValue": str(seq if seq < 1 << 63 else seq - (1 << 64))}},
            _s("hopwake.journey.id", id_),
            _s("hopwake.hop", self.hop),
            _s("hopwake.op", op),
            _s("hopwake.correlation", "derived"),
            _s("messaging.system", "hook"),
        ]
        if message_type:
            attrs.append(_s("hopwake.message.type", message_type))
        if alias:
            attrs.append(_s("hopwake.device.alias", alias))
        span = {
            "traceId": id_,
            "spanId": _span_id(),
            "name": "hop " + self.hop,
            "kind": 5,  # consumer
            "startTimeUnixNano": now,
            "endTimeUnixNano": now,
            "attributes": attrs,
        }
        try:
            self._queue.put_nowait(span)
        except queue.Full:
            with self._count_lock:
                self._dropped += 1
            return
        with self._count_lock:
            self._accepted += 1

    # -- state

    @property
    def accepted(self) -> int:
        """Marks taken into the queue so far."""
        return self._accepted

    @property
    def dropped(self) -> int:
        """Marks dropped because the queue was full."""
        return self._dropped

    @property
    def queued(self) -> int:
        """Marks waiting to be sent."""
        return self._queue.qsize()

    # -- sending

    def flush(self, timeout: float = 5.0) -> bool:
        """Sends what is queued, waiting at most timeout seconds. Returns
        False when that was not enough."""
        if not self._sender.is_alive():
            return self._queue.empty()
        done = threading.Event()
        with self._waiters_lock:
            self._waiters.append(done)
        self._wake.set()
        return done.wait(timeout)

    def close(self, timeout: float = 5.0) -> bool:
        """Sends what is queued and stops the hook; later marks are dropped."""
        if self._closed:
            return self._queue.empty()
        self._closed = True
        ok = self.flush(timeout)
        self._stop.set()
        self._wake.set()
        return ok

    def __enter__(self) -> "Hook":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _run(self) -> None:
        while True:
            self._wake.wait(SEND_EVERY)
            self._wake.clear()
            with self._waiters_lock:
                waiters, self._waiters = self._waiters, []
            self._send_queued()
            for w in waiters:
                w.set()
            if self._stop.is_set():
                return

    def _send_queued(self) -> None:
        while True:
            batch = []
            while len(batch) < _BATCH:
                try:
                    batch.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            if not batch:
                return
            self._send(batch)

    def _send(self, spans: List[dict]) -> None:
        body = {
            "resourceSpans": [
                {
                    "resource": {"attributes": [_s("service.name", self.service)]},
                    "scopeSpans": [{"scope": {"name": "hopwake/hook"}, "spans": spans}],
                }
            ]
        }
        req = urllib.request.Request(
            self.endpoint + "/v1/traces",
            data=json.dumps(body, separators=(",", ":")).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self._opener.open(req, timeout=SEND_TIMEOUT) as resp:
                resp.read()
        except Exception:
            pass  # Hopwake is not reachable: these marks are lost, the service carries on


def _s(key: str, value: str) -> dict:
    return {"key": key, "value": {"stringValue": value}}


def _span_id() -> str:
    while True:
        b = os.urandom(8)
        if any(b):
            return b.hex()
