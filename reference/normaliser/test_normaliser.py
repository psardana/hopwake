import unittest

import random

from normaliser import Faults, normalise

RAW = {"device_id": "dev-00042", "site": "site-02", "seq": 3, "ts": "2026-11-18T06:38:18Z",
       "temp_c": 20.0, "pressure_kpa": 101.0, "vibration_mm_s": 1.0}


class FaultsTest(unittest.TestCase):
    def test_off_by_default(self):
        self.assertIs(Faults(env={}).apply(RAW), RAW)

    def test_drop_only_listed_devices(self):
        f = Faults(env={"FAULT_DROP": "1", "FAULT_DEVICES": "dev-00042"}, rng=random.Random(1))
        self.assertIsNone(f.apply(RAW))
        other = dict(RAW, device_id="dev-00001")
        self.assertIs(f.apply(other), other)

    def test_rename_breaks_normalising(self):
        f = Faults(env={"FAULT_RENAME": "1"}, rng=random.Random(1))
        changed = f.apply(RAW)
        self.assertNotIn("temp_c", changed)
        self.assertIn("temp_c", RAW, "the original reading is left as it was")
        with self.assertRaises(KeyError):
            normalise(changed)


class NormaliseTest(unittest.TestCase):
    def test_converts_units_and_renames_fields(self):
        out = normalise({
            "device_id": "dev-00038", "site": "site-02", "seq": 7, "ts": "2026-11-18T06:38:18Z",
            "temp_c": 24.16, "pressure_kpa": 101.47, "vibration_mm_s": 0.999,
        })
        self.assertEqual(out["device"], "dev-00038")
        self.assertEqual(out["seq"], 7)
        self.assertEqual(out["temperature_k"], 297.31)
        self.assertEqual(out["pressure_pa"], 101470)
        self.assertEqual(out["observed_at"], "2026-11-18T06:38:18Z")

    def test_missing_field_raises(self):
        with self.assertRaises(KeyError):
            normalise({"site": "site-02"})


if __name__ == "__main__":
    unittest.main()
