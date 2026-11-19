import unittest

from normaliser import normalise


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
