from datetime import datetime
import unittest

import pandas as pd

from weather_scraping_functions import (get_asos_stations, find_nearest_asos_station, get_hourly_asos,
                                        FIPS_TO_STATE)


class TestAsosStationDiscovery(unittest.TestCase):
    """Live tests for ASOS station discovery and the hourly UTC fetch wrapper."""

    def test_fips_map_covers_conterminous(self):
        self.assertEqual(FIPS_TO_STATE["08"], "CO")
        self.assertEqual(FIPS_TO_STATE["23"], "ME")
        self.assertGreaterEqual(len(FIPS_TO_STATE), 50)

    def test_get_asos_stations(self):
        stations = get_asos_stations("CO")
        self.assertIsInstance(stations, pd.DataFrame)
        self.assertGreater(len(stations), 20)
        self.assertIn("station_id", stations.columns)
        self.assertTrue(stations["latitude"].between(35, 42).all())

    def test_nearest_station_to_poudre_gauge(self):
        nearest = find_nearest_asos_station(40.588, -105.069, "CO")
        # Fort Collins/Loveland (FNL) is ~15 km from the gauge; anything nearer is fine too.
        self.assertLess(nearest["distance_km"], 30.0)

    def test_hourly_asos_utc(self):
        df = get_hourly_asos("FNL", datetime(2024, 6, 1), datetime(2024, 6, 2))
        self.assertIn("tmpf", df.columns)
        self.assertIn("p01m", df.columns)
        self.assertGreaterEqual(len(df), 20)
        self.assertEqual(str(df["datetime"].dt.tz), "UTC")
        # June temperatures at Fort Collins should be sane Fahrenheit values.
        self.assertTrue(df["tmpf"].dropna().between(20, 110).all())


if __name__ == "__main__":
    unittest.main()
