from datetime import datetime
import unittest

import pandas as pd

from awdb_functions import (get_awdb_stations, find_nearest_awdb_station, get_scan_soil_moisture,
                            get_snotel_awdb_data)


class TestAwdbScraping(unittest.TestCase):
    """Live tests against the USDA AWDB REST API (SCAN soil moisture and SNOTEL snow data)."""

    def test_get_scan_stations(self):
        stations = get_awdb_stations(network="SCAN", state_code="CO")
        self.assertIsInstance(stations, pd.DataFrame)
        self.assertGreater(len(stations), 0)
        self.assertTrue(stations["stationTriplet"].str.endswith(":CO:SCAN").all())
        self.assertIn("latitude", stations.columns)
        self.assertIn("longitude", stations.columns)

    def test_find_nearest_station(self):
        # Near Nunn, CO which hosts a long-running SCAN site.
        nearest = find_nearest_awdb_station(40.87, -104.73, network="SCAN", state_code="CO")
        self.assertIn("stationTriplet", nearest)
        self.assertLess(nearest["distance_km"], 100.0)

    def test_hourly_soil_moisture(self):
        df = get_scan_soil_moisture("2017:CO:SCAN", datetime(2023, 6, 1), datetime(2023, 6, 1, 6))
        self.assertIn("datetime", df.columns)
        self.assertIn("SMS_-2in", df.columns)
        self.assertEqual(len(df), 7)
        self.assertTrue(df["SMS_-2in"].between(0, 100).all())

    def test_soil_moisture_multiple_depths(self):
        df = get_scan_soil_moisture("2017:CO:SCAN", datetime(2023, 6, 1), datetime(2023, 6, 1, 3))
        depth_cols = [col for col in df.columns if col.startswith("SMS_")]
        self.assertGreaterEqual(len(depth_cols), 3)

    def test_snotel_via_awdb(self):
        df = get_snotel_awdb_data("713:CO:SNTL", datetime(2022, 1, 1), datetime(2022, 1, 10))
        self.assertIn("WTEQ", df.columns)
        self.assertEqual(len(df), 10)
        self.assertTrue((df["WTEQ"].dropna() >= 0).all())

    def test_utc_offset_shifts_timestamps(self):
        df = get_scan_soil_moisture("2017:CO:SCAN", datetime(2023, 6, 1), datetime(2023, 6, 1, 3),
                                    utc_offset_hours=-7.0)
        self.assertEqual(str(df["datetime"].dt.tz), "UTC")
        self.assertEqual(df["datetime"].iloc[0].hour, 7)


if __name__ == "__main__":
    unittest.main()
