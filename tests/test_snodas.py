from datetime import datetime
import unittest

import numpy as np

from snodas_functions import (snodas_tar_url, parse_snodas_header, basin_mean_swe,
                              get_snodas_swe_grid, get_snodas_basin_series)

SAMPLE_HEADER = """Format version: NOHRSC GIS/RS raster file v1.1
Description: Modeled snow water equivalent, total of snow layers
Data units: Meters / 1000.000000
Data type: integer
Data bytes per pixel: 2
Data intercept: 0.00000000000000
Data slope: 1.00000000000000
No data value: -9999.00000000000
Number of columns: 6935
Number of rows: 3351
Benchmark x-axis coordinate: -124.729166666662
Benchmark y-axis coordinate: 52.8708333333312
X-axis resolution: 0.00833333333333300
Y-axis resolution: 0.00833333333333300
Start year: 2024
Start month: 6
Start day: 1
"""


class TestSnodasParsing(unittest.TestCase):
    """Offline tests for URL construction, header parsing and basin averaging."""

    def test_tar_url_masked_and_unmasked(self):
        date = datetime(2024, 6, 1)
        self.assertEqual(snodas_tar_url(date),
                         "https://noaadata.apps.nsidc.org/NOAA/G02158/masked/2024/06_Jun/"
                         "SNODAS_20240601.tar")
        self.assertEqual(snodas_tar_url(date, masked=False),
                         "https://noaadata.apps.nsidc.org/NOAA/G02158/unmasked/2024/06_Jun/"
                         "SNODAS_unmasked_20240601.tar")

    def test_parse_header_types(self):
        header = parse_snodas_header(SAMPLE_HEADER)
        self.assertEqual(header["Number of columns"], 6935)
        self.assertEqual(header["Number of rows"], 3351)
        self.assertAlmostEqual(header["X-axis resolution"], 1.0 / 120.0, places=6)
        self.assertAlmostEqual(header["Benchmark y-axis coordinate"], 52.8708333, places=5)
        self.assertEqual(header["Data type"], "integer")
        self.assertEqual(header["Start year"], 2024)

    def test_basin_mean_with_bbox_ignores_nan(self):
        swe = np.full((4, 4), np.nan)
        swe[1, 1], swe[1, 2] = 100.0, 200.0
        grid = {"swe_mm": swe, "lats": np.array([41.0, 40.9, 40.8, 40.7]),
                "lons": np.array([-106.0, -105.9, -105.8, -105.7])}
        mean = basin_mean_swe(grid, bbox=[-105.95, 40.85, -105.75, 40.95])
        self.assertAlmostEqual(mean, 150.0)

    def test_basin_mean_with_polygon(self):
        swe = np.zeros((4, 4))
        swe[1, 1] = 80.0
        grid = {"swe_mm": swe, "lats": np.array([41.0, 40.9, 40.8, 40.7]),
                "lons": np.array([-106.0, -105.9, -105.8, -105.7])}
        square = {"type": "Polygon", "coordinates": [[[-105.95, 40.85], [-105.85, 40.85],
                                                      [-105.85, 40.95], [-105.95, 40.95],
                                                      [-105.95, 40.85]]]}
        self.assertAlmostEqual(basin_mean_swe(grid, geometry=square), 80.0)

    def test_basin_mean_requires_region(self):
        grid = {"swe_mm": np.zeros((2, 2)), "lats": np.array([41.0, 40.9]),
                "lons": np.array([-106.0, -105.9])}
        with self.assertRaises(ValueError):
            basin_mean_swe(grid)


class TestSnodasLive(unittest.TestCase):
    """Live SNODAS download for the Cache la Poudre basin (anonymous access, ~6 MB per day)."""

    @classmethod
    def setUpClass(cls):
        from usgs_scraping_functions import get_basin_boundary
        cls.basin = get_basin_boundary("06752260")
        cls.grid = get_snodas_swe_grid(datetime(2024, 6, 1))

    def test_grid_geometry(self):
        self.assertEqual(self.grid["swe_mm"].shape, (3351, 6935))
        self.assertEqual(self.grid["date"], datetime(2024, 6, 1))
        self.assertAlmostEqual(float(self.grid["lats"][0]), 52.8708, places=3)
        self.assertAlmostEqual(float(self.grid["lons"][0]), -124.7292, places=3)
        self.assertTrue(np.isnan(self.grid["swe_mm"]).any())

    def test_poudre_basin_mean_june_2024(self):
        # June 1 2024: high-country snow remains (in-basin SNOTEL up to ~485 mm) while most of the
        # basin is bare, so the basin mean sits in the tens of millimeters.
        mean = basin_mean_swe(self.grid, geometry=self.basin)
        self.assertGreater(mean, 20.0)
        self.assertLess(mean, 120.0)

    def test_poudre_melt_out_series(self):
        series = get_snodas_basin_series(self.basin, datetime(2024, 6, 21), datetime(2024, 6, 22))
        self.assertEqual(len(series), 2)
        self.assertLess(series["snodas_swe_mm"].max(), 5.0)


if __name__ == "__main__":
    unittest.main()
