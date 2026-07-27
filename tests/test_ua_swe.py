from datetime import datetime
import unittest

import numpy as np

from ua_swe_functions import (water_year, ua_daily_file_url, get_ua_swe_grid,
                              get_ua_basin_swe_series, get_snowview_watershed_series,
                              download_ua_water_year)
from snodas_functions import basin_mean_swe


class TestUaSweHelpers(unittest.TestCase):
    """Offline tests for water-year math and URL construction."""

    def test_water_year_boundaries(self):
        self.assertEqual(water_year(datetime(2023, 10, 1)), 2024)
        self.assertEqual(water_year(datetime(2024, 9, 30)), 2024)
        self.assertEqual(water_year(datetime(2024, 10, 1)), 2025)
        self.assertEqual(water_year(datetime(2024, 6, 1)), 2024)

    def test_daily_file_url(self):
        self.assertEqual(ua_daily_file_url(datetime(2024, 6, 1)),
                         "https://climate.arizona.edu/data/UA_SWE/DailyData_4km/WY2024/"
                         "UA_SWE_Depth_4km_v1_20240601_stable.nc")
        self.assertIn("_provisional.nc", ua_daily_file_url(datetime(2024, 6, 1),
                                                           variant="provisional"))

    def test_water_year_download_rejects_unknown_source(self):
        with self.assertRaises(ValueError):
            download_ua_water_year(2024, source="ftp")


class TestUaSweLive(unittest.TestCase):
    """Live UA 4-km SWE fetches for the Cache la Poudre basin (anonymous, ~100 KB per day)."""

    @classmethod
    def setUpClass(cls):
        from usgs_scraping_functions import get_basin_boundary
        cls.basin = get_basin_boundary("06752260")
        cls.grid = get_ua_swe_grid(datetime(2024, 6, 1))

    def test_grid_geometry(self):
        self.assertEqual(self.grid["swe_mm"].shape, (621, 1405))
        self.assertEqual(self.grid["date"], datetime(2024, 6, 1))
        self.assertAlmostEqual(float(self.grid["lats"][0]), 24.0833, places=3)
        self.assertAlmostEqual(float(self.grid["lons"][0]), -125.0, places=3)
        self.assertTrue(np.isnan(self.grid["swe_mm"]).any())

    def test_poudre_basin_mean_june_2024(self):
        mean = basin_mean_swe(self.grid, geometry=self.basin)
        self.assertGreater(mean, 10.0)
        self.assertLess(mean, 100.0)

    def test_poudre_melt_out_series(self):
        series = get_ua_basin_swe_series(self.basin, datetime(2024, 6, 21), datetime(2024, 6, 22))
        self.assertEqual(len(series), 2)
        self.assertLess(series["ua_swe_mm"].max(), 5.0)

    def test_snowview_watershed_series(self):
        series = get_snowview_watershed_series("10190007")
        june = series[(series["datetime"] >= "2024-06-01") & (series["datetime"] <= "2024-06-30")]
        self.assertEqual(len(june), 30)
        # The HUC8 mean melts out over June 2024 like the basin does.
        self.assertGreater(june["swe_mm"].iloc[0], june["swe_mm"].iloc[-1])
        self.assertLess(june["swe_mm"].iloc[-1], 5.0)


if __name__ == "__main__":
    unittest.main()
