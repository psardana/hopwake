import datetime as dt
import json
import os
import socket
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import hopwake_hook  # noqa: E402
from hopwake_hook import SERVE, STORE, Hook, journey_id, time_journey_id  # noqa: E402
from hopwake_hook import hook as hookmod  # noqa: E402
from hopwake_hook.kafka import header  # noqa: E402

AT = dt.datetime(2026, 11, 18, 10, 0, 0, 750000, tzinfo=dt.timezone.utc)
TYPE = "ems.measurements.Measurement"
ENV = ("HOPWAKE_TENANT", "HOPWAKE_OTLP_ENDPOINT", "HOPWAKE_TIME_PRECISION", "HOPWAKE_API")


class Collector:
    """Stands in for Hopwake: takes OTLP/JSON spans and serves aliases."""

    def __init__(self, aliases=None):
        self.requests = []
        self.aliases = aliases if aliases is not None else []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                outer.requests.append((self.path, self.headers.get("Content-Type"), json.loads(body)))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"{}")

            def do_GET(self):
                data = json.dumps(outer.aliases).encode()
                self.send_response(200 if self.path == "/api/aliases" else 404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:%d" % self.server.server_address[1]

    def spans(self):
        out = []
        for _, _, body in self.requests:
            for rs in body["resourceSpans"]:
                svc = {a["key"]: a["value"]["stringValue"] for a in rs["resource"]["attributes"]}["service.name"]
                for ss in rs["scopeSpans"]:
                    for s in ss["spans"]:
                        s = dict(s)
                        s["service"] = svc
                        s["attrs"] = {a["key"]: next(iter(a["value"].values())) for a in s["attributes"]}
                        out.append(s)
        return out

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class Silent:
    """Accepts connections and never answers."""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.url = "http://127.0.0.1:%d" % self.sock.getsockname()[1]

    def close(self):
        self.sock.close()


def unused_port_url():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return "http://127.0.0.1:%d" % port


class HookTest(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, {k: "" for k in ENV})
        self.env.start()
        for k in ENV:
            del os.environ[k]
        self.addCleanup(self.env.stop)

    def collector(self, aliases=None):
        c = Collector(aliases)
        self.addCleanup(c.close)
        return c

    def test_requires_tenant_and_hop(self):
        with self.assertRaises(ValueError):
            Hook(hop="hook:x")
        with self.assertRaises(ValueError):
            Hook(tenant="t")

    def test_defaults(self):
        h = Hook(tenant="t", hop="hook:x")
        self.addCleanup(h.close)
        self.assertEqual(h.endpoint, "http://localhost:4318")
        self.assertEqual(h.service, "hook:x")
        self.assertEqual(h.time_precision_ns, 0)
        self.assertIsNone(h.aliases_url)

    def test_environment_and_code_wins(self):
        os.environ.update({
            "HOPWAKE_TENANT": "env-tenant",
            "HOPWAKE_OTLP_ENDPOINT": unused_port_url(),
            "HOPWAKE_TIME_PRECISION": "500ms",
            "HOPWAKE_API": unused_port_url(),
        })
        h = Hook(hop="hook:x")
        self.addCleanup(h.close)
        self.assertEqual(h.tenant, "env-tenant")
        self.assertEqual(h.endpoint, os.environ["HOPWAKE_OTLP_ENDPOINT"])
        self.assertEqual(h.time_precision_ns, 500_000_000)
        self.assertEqual(h.aliases_url, os.environ["HOPWAKE_API"])
        g = Hook(tenant="code", hop="hook:x", endpoint="http://e:1", time_precision="1m", aliases="http://a:1")
        self.addCleanup(g.close)
        self.assertEqual((g.tenant, g.endpoint, g.time_precision_ns, g.aliases_url), ("code", "http://e:1", 60 * 10**9, "http://a:1"))
        self.assertEqual(Hook(tenant="t", hop="h", time_precision=dt.timedelta(seconds=10), endpoint="http://e:1").time_precision_ns, 10**10)

    def test_bad_precision_in_environment_is_ignored(self):
        os.environ["HOPWAKE_TIME_PRECISION"] = "soon"
        h = Hook(tenant="t", hop="h", endpoint=unused_port_url())
        self.addCleanup(h.close)
        self.assertEqual(h.time_precision_ns, 0)

    def test_on_the_wire(self):
        c = self.collector()
        before = time.time_ns()
        with Hook(tenant="t", hop="hook:x.stored", endpoint=c.url, service="svc", time_precision="1s") as h:
            h.mark(STORE, "dev-1", 7)
            h.mark_boot(STORE, "dev-1", "3", 18446744073709551615)
            h.mark_time_teach(STORE, "SEN1", "systemId", "100042", TYPE, AT)
        after = time.time_ns()
        path, ctype, _ = c.requests[0]
        self.assertEqual(path, "/v1/traces")
        self.assertEqual(ctype, "application/json")
        spans = c.spans()
        self.assertEqual(len(spans), 3)
        for s in spans:
            self.assertEqual(s["service"], "svc")
            self.assertEqual(s["name"], "hop hook:x.stored")
            self.assertEqual(s["kind"], 5)
            self.assertRegex(s["traceId"], r"^[0-9a-f]{32}$")
            self.assertRegex(s["spanId"], r"^[0-9a-f]{16}$")
            self.assertNotEqual(s["spanId"], "0" * 16)
            self.assertNotIn("parentSpanId", s)
            self.assertIsInstance(s["startTimeUnixNano"], str)
            self.assertEqual(s["startTimeUnixNano"], s["endTimeUnixNano"])
            self.assertTrue(before <= int(s["startTimeUnixNano"]) <= after)
            a = s["attrs"]
            self.assertEqual(a["hopwake.journey.id"], s["traceId"])
            for k, v in [("hopwake.schema", "hop/v1"), ("hopwake.tenant", "t"), ("hopwake.hop", "hook:x.stored"),
                         ("hopwake.op", "store"), ("hopwake.correlation", "derived"), ("messaging.system", "hook")]:
                self.assertEqual(a[k], v)
        seq, boot, teach = spans
        self.assertEqual(seq["traceId"], journey_id("t", "dev-1", 7))
        self.assertEqual(seq["attrs"]["hopwake.seq"], "7")
        self.assertNotIn("hopwake.message.type", seq["attrs"])
        self.assertNotIn("hopwake.device.alias", seq["attrs"])
        self.assertEqual(boot["traceId"], journey_id("t", "dev-1", 18446744073709551615, "3"))
        self.assertEqual(boot["attrs"]["hopwake.seq"], "-1")  # int64 on the wire, as Go sends it
        self.assertEqual(teach["traceId"], time_journey_id("t", "SEN1", TYPE, "2026-11-18T10:00:00Z"))
        self.assertEqual(teach["attrs"]["hopwake.seq"], "0")
        self.assertEqual(teach["attrs"]["hopwake.device.id"], "SEN1")
        self.assertEqual(teach["attrs"]["hopwake.message.type"], TYPE)
        self.assertEqual(teach["attrs"]["hopwake.device.alias"], "systemId:100042")

    def test_empty_message_type_is_not_sent(self):
        c = self.collector()
        with Hook(tenant="t", hop="h", endpoint=c.url) as h:
            h.mark_time(STORE, "m", "", 1794996000 * 10**9)
        (s,) = c.spans()
        self.assertNotIn("hopwake.message.type", s["attrs"])
        self.assertEqual(s["traceId"], time_journey_id("t", "m", "", "2026-11-18T10:00:00Z"))

    def test_served_once(self):
        c = self.collector()
        with Hook(tenant="t", hop="h", endpoint=c.url) as h:
            for _ in range(3):
                h.mark(SERVE, "d", 1)
                h.mark(STORE, "d", 1)
                h.mark_time(SERVE, "d", TYPE, AT)
        ops = sorted(s["attrs"]["hopwake.op"] for s in c.spans())
        self.assertEqual(ops, ["serve", "serve", "store", "store", "store"])

    def test_served_memory_is_bounded(self):
        h = Hook(tenant="t", hop="h", endpoint=unused_port_url())
        self.addCleanup(h.close)
        self.assertTrue(h._first_serve(SERVE, "first"))
        for i in range(hookmod.SERVED_LIMIT - 1):
            h._first_serve(SERVE, str(i))
        self.assertFalse(h._first_serve(SERVE, "first"))
        self.assertTrue(h._first_serve(SERVE, "one more"))  # forgets all, then remembers this one
        self.assertEqual(len(h._served), 1)
        self.assertTrue(h._first_serve(SERVE, "first"))
        self.assertTrue(h._first_serve(STORE, "one more"))

    def test_unknown_alias_sends_nothing(self):
        h = Hook(tenant="t", hop="h", endpoint=unused_port_url())
        self.addCleanup(h.close)
        self.assertFalse(h.mark_time_alias(SERVE, "systemId", "100042", TYPE, AT))
        self.assertEqual(h.accepted, 0)
        self.assertEqual(h.queued, 0)

    def test_alias_from_the_list(self):
        c = self.collector([{"kind": "systemId", "value": "100042", "device": "EMS0000042"}])
        with Hook(tenant="t", hop="h", endpoint=c.url, aliases=c.url + "/", time_precision="1s") as h:
            deadline = time.monotonic() + 5
            while not h.mark_time_alias(SERVE, "systemId", "100042", TYPE, AT):
                self.assertLess(time.monotonic(), deadline, "the alias list never arrived")
                time.sleep(0.02)
            self.assertFalse(h.mark_time_alias(SERVE, "systemId", "999999", TYPE, AT))
            self.assertEqual(h.accepted, 1)
        (s,) = c.spans()
        self.assertEqual(s["attrs"]["hopwake.device.id"], "EMS0000042")
        self.assertEqual(s["attrs"]["hopwake.device.alias"], "systemId:100042")
        self.assertEqual(s["attrs"]["hopwake.op"], "serve")
        self.assertEqual(s["traceId"], time_journey_id("t", "EMS0000042", TYPE, "2026-11-18T10:00:00Z"))

    def test_failed_alias_fetch_keeps_the_copy(self):
        c = self.collector([{"kind": "k", "value": "v", "device": "d"}])
        h = Hook(tenant="t", hop="h", endpoint=c.url, aliases=c.url)
        self.addCleanup(h.close)
        self.assertTrue(h._aliases.fetch())
        c.close()
        self.assertFalse(h._aliases.fetch())
        self.assertEqual(h._aliases.device("k", "v"), "d")

    def test_full_queue_drops(self):
        silent = Silent()
        self.addCleanup(silent.close)
        h = Hook(tenant="t", hop="h", endpoint=silent.url, queue_size=100)
        start = time.monotonic()
        for i in range(1000):
            h.mark(STORE, "d", i)
        self.assertLess(time.monotonic() - start, 0.5)
        self.assertLessEqual(h.queued, 100)
        self.assertGreaterEqual(h.dropped, 800)
        self.assertEqual(h.accepted + h.dropped, 1000)
        start = time.monotonic()
        self.assertFalse(h.close(timeout=0.5))
        self.assertLess(time.monotonic() - start, 1.5)

    def test_unreachable_endpoint(self):
        h = Hook(tenant="t", hop="h", endpoint=unused_port_url())
        start = time.monotonic()
        h.mark(STORE, "d", 1)
        h.mark_time(SERVE, "d", TYPE, AT)
        self.assertLess(time.monotonic() - start, 0.05)
        self.assertTrue(h.close(timeout=5))
        self.assertLess(time.monotonic() - start, 5)

    def test_slow_endpoint_is_given_up_on(self):
        silent = Silent()
        self.addCleanup(silent.close)
        h = Hook(tenant="t", hop="h", endpoint=silent.url)
        start = time.monotonic()
        h.mark(STORE, "d", 1)
        self.assertLess(time.monotonic() - start, 0.05)
        self.assertTrue(h.close(timeout=6))
        self.assertLess(time.monotonic() - start, 5)

    def test_bad_input_does_not_raise(self):
        h = Hook(tenant="t", hop="h", endpoint=unused_port_url())
        self.addCleanup(h.close)
        with self.assertLogs("hopwake_hook", "WARNING"):
            h.mark("stored", "d", 1)
            h.mark(STORE, "d", -1)
            h.mark_time(STORE, "d", TYPE, "yesterday")
        self.assertEqual(h.accepted, 0)

    def test_marks_after_close_are_dropped(self):
        c = self.collector()
        h = Hook(tenant="t", hop="h", endpoint=c.url)
        h.close()
        h.mark(STORE, "d", 1)
        self.assertEqual(h.accepted, 0)
        self.assertTrue(h.close())

    def test_exports(self):
        self.assertEqual((hopwake_hook.STORE, hopwake_hook.SERVE), ("store", "serve"))
        self.assertEqual(hopwake_hook.__version__, "0.1.0")


class KafkaHeaders(unittest.TestCase):
    def test_header(self):
        hs = [("seq", b"1683"), ("device", b"dev-00042"), ("seq", b"9"), ("empty", None)]
        self.assertEqual(header(hs, "seq"), "1683")
        self.assertEqual(header(hs, "device"), "dev-00042")
        self.assertEqual(header(hs, "empty"), "")
        self.assertEqual(header(hs, "missing"), "")
        self.assertIsNone(header(hs, "missing", None))
        self.assertEqual(header(None, "seq"), "")
        self.assertEqual(header({"seq": b"5"}, "seq"), "5")
        self.assertEqual(header([(b"seq", "6")], "seq"), "6")


if __name__ == "__main__":
    unittest.main()
