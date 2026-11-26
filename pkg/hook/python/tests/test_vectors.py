import datetime as dt
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hopwake_hook import format_alias, format_time, journey_id, parse_duration, time_journey_id  # noqa: E402

VECTORS = os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "vectors.json")

with open(VECTORS, encoding="utf-8") as f:
    V = json.load(f)


class SeqVectors(unittest.TestCase):
    def test_seq(self):
        self.assertTrue(V["seq"])
        for v in V["seq"]:
            with self.subTest(v=v):
                self.assertEqual(journey_id(v["tenant"], v["device"], int(v["seq"]), v["boot"]), v["id"])

    def test_seq_out_of_range(self):
        with self.assertRaises(ValueError):
            journey_id("t", "d", -1)
        with self.assertRaises(ValueError):
            journey_id("t", "d", 1 << 64)


class TimeVectors(unittest.TestCase):
    def test_time_from_nanos(self):
        # Every resolution, nanoseconds included.
        self.assertTrue(V["time"])
        for v in V["time"]:
            with self.subTest(v=v):
                got = format_time(int(v["unix_nanos"]), int(v["precision_ns"]))
                self.assertEqual(got, v["formatted"])
                self.assertEqual(time_journey_id(v["tenant"], v["device"], v["type"], got), v["id"])

    def test_time_from_datetime(self):
        # A datetime holds microseconds, so the nanosecond vectors are skipped.
        epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        n = 0
        for v in V["time"]:
            if v["resolution"] == "ns":
                continue
            ns = int(v["unix_nanos"])
            self.assertEqual(ns % 1000, 0)
            at = epoch + dt.timedelta(microseconds=ns // 1000)
            for t in (at, at.replace(tzinfo=None), at.astimezone(dt.timezone(dt.timedelta(hours=5, minutes=30)))):
                with self.subTest(v=v, at=t):
                    self.assertEqual(format_time(t, int(v["precision_ns"])), v["formatted"])
            n += 1
        self.assertGreater(n, 0)

    def test_table_in_contract(self):
        base = dt.datetime(2026, 11, 18, 10, 0, 0)
        ns = 1794996000 * 10**9
        self.assertEqual(format_time(base), "2026-11-18T10:00:00Z")
        self.assertEqual(format_time(base.replace(microsecond=500000)), "2026-11-18T10:00:00.5Z")
        self.assertEqual(format_time(base.replace(microsecond=120000)), "2026-11-18T10:00:00.12Z")
        self.assertEqual(format_time(ns + 123456789), "2026-11-18T10:00:00.123456789Z")

    def test_rounds_down_before_the_epoch(self):
        self.assertEqual(format_time(-1, 10**9), "1969-12-31T23:59:59Z")
        self.assertEqual(format_time(-500_000_000), "1969-12-31T23:59:59.5Z")


class AliasVectors(unittest.TestCase):
    def test_alias(self):
        self.assertTrue(V["alias"])
        for v in V["alias"]:
            with self.subTest(v=v):
                self.assertEqual(format_alias(v["kind"], v["value"]), v["formatted"])


class Durations(unittest.TestCase):
    def test_parse(self):
        for s, ns in [
            ("1s", 10**9),
            ("500ms", 5 * 10**8),
            ("1m", 60 * 10**9),
            ("1h", 3600 * 10**9),
            ("250us", 250_000),
            ("250µs", 250_000),
            ("100ns", 100),
            ("1.5s", 1_500_000_000),
            ("1m30s", 90 * 10**9),
            ("0", 0),
        ]:
            with self.subTest(s=s):
                self.assertEqual(parse_duration(s), ns)

    def test_invalid(self):
        for s in ["", "1", "s", "1x", "1.2.3s", "ms"]:
            with self.subTest(s=s):
                with self.assertRaises(ValueError):
                    parse_duration(s)


if __name__ == "__main__":
    unittest.main()
