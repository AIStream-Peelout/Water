from datetime import datetime
import json
import os
import tempfile
import unittest

import pandas as pd

from catchment_dataset import build_catchment_bundle, usgs_to_hourly_utc


class TestCatchmentBundle(unittest.TestCase):
    """Live end-to-end test building a bundle for the Cache la Poudre gauge near a SCAN station."""

    @classmethod
    def setUpClass(cls):
        cls.output_dir = tempfile.mkdtemp()
        cls.bundle = build_catchment_bundle("06752260", datetime(2024, 6, 1), datetime(2024, 6, 3),
                                            output_dir=cls.output_dir)

    def test_static_attributes_present(self):
        static = self.bundle["static"]
        self.assertEqual(static["site_no"], "06752260")
        self.assertIsNotNone(static["drain_area_va"])
        self.assertIn("scan_triplet", static)
        self.assertIn("basin_bbox", static)

    def test_hourly_frame_joined(self):
        hourly = self.bundle["hourly"]
        self.assertIn("cfs", hourly.columns)
        self.assertGreaterEqual(len(hourly), 24)
        self.assertEqual(str(pd.to_datetime(hourly["datetime"]).dt.tz), "UTC")
        soil_cols = [c for c in hourly.columns if c.startswith("SMS_")]
        self.assertGreater(len(soil_cols), 0)
        self.assertGreater(hourly[soil_cols[0]].notna().sum(), 0)

    def test_basin_geometry_written(self):
        self.assertIn(self.bundle["basin_geometry"]["type"], ("Polygon", "MultiPolygon"))
        geojson_path = os.path.join(self.output_dir, "06752260_basin.geojson")
        with open(geojson_path) as f:
            feature = json.load(f)
        self.assertEqual(feature["properties"]["site_no"], "06752260")

    def test_artifacts_written(self):
        for suffix in ("_static.json", "_hourly.csv", "_basin.geojson"):
            self.assertTrue(os.path.exists(os.path.join(self.output_dir, "06752260" + suffix)))

    @unittest.skipUnless(os.environ.get("EARTHDATA_TOKEN"), "EARTHDATA_TOKEN not set")
    def test_bundle_with_nldas_pet(self):
        """A bundle with NLDAS enabled should carry radiation and a plausible daily PET total in mm."""
        with tempfile.TemporaryDirectory() as nldas_dir:
            bundle = build_catchment_bundle("06752260", datetime(2024, 6, 1), datetime(2024, 6, 2),
                                            output_dir=nldas_dir, include_nldas=True)
        hourly = bundle["hourly"]
        self.assertIn("shortwave_radiation", hourly.columns)
        self.assertGreater(hourly["shortwave_radiation"].max(), 300.0)
        daily_pet = hourly["pet_mm_hr"].iloc[:24].sum()
        # NLDAS PotEvap is kg/m^2 per hour (= mm); a June day in Colorado should total a few mm.
        self.assertGreater(daily_pet, 1.0)
        self.assertLess(daily_pet, 20.0)


class TestUsgsHourlyConversion(unittest.TestCase):
    """Offline test of the USGS raw-to-hourly-UTC conversion."""

    def test_hourly_utc_conversion(self):
        raw = pd.DataFrame({
            "tz_cd": ["6s", "MST", "MST", "MST"],
            "datetime": ["junk", "2023-06-01 05:00", "2023-06-01 05:15", "2023-06-01 06:00"],
            "cfs": ["5s", "100", "101", "102"],
        })
        hourly = usgs_to_hourly_utc(raw)
        self.assertEqual(len(hourly), 2)
        self.assertEqual(hourly["cfs"].iloc[0], 100.0)
        # The repo's timezone_map maps MST to America/Denver, which observes DST: on June 1 the zone
        # is UTC-6, so 05:00 local becomes 11:00 UTC.
        self.assertEqual(hourly["datetime"].iloc[0].hour, 11)


if __name__ == "__main__":
    unittest.main()
