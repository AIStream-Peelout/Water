from datetime import datetime
import unittest

import numpy as np

from dem_functions import normalize_geometry, polygon_mask, sample_dem_at, band_mean_swe

UNIT_SQUARE = {"type": "Polygon", "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0],
                                                   [0.0, 1.0], [0.0, 0.0]]]}


class TestPolygonMask(unittest.TestCase):
    """Offline tests for GeoJSON handling and rasterization."""

    def test_normalize_unwraps_feature(self):
        feature = {"type": "Feature", "properties": {}, "geometry": UNIT_SQUARE}
        self.assertEqual(normalize_geometry(feature)["type"], "Polygon")
        with self.assertRaises(ValueError):
            normalize_geometry({"type": "Point", "coordinates": [0.0, 0.0]})

    def test_mask_of_unit_square(self):
        lats = np.array([1.5, 0.5, -0.5])
        lons = np.array([-0.5, 0.5, 1.5])
        mask = polygon_mask(lats, lons, UNIT_SQUARE)
        self.assertEqual(mask.shape, (3, 3))
        self.assertEqual(int(mask.sum()), 1)
        self.assertTrue(mask[1, 1])

    def test_mask_subtracts_holes(self):
        hole = [[0.4, 0.4], [0.6, 0.4], [0.6, 0.6], [0.4, 0.6], [0.4, 0.4]]
        ring = UNIT_SQUARE["coordinates"][0]
        with_hole = {"type": "Polygon", "coordinates": [ring, hole]}
        mask = polygon_mask(np.array([0.5]), np.array([0.5]), with_hole)
        self.assertFalse(mask[0, 0])

    def test_multipolygon_is_union(self):
        second = [[[2.0, 0.0], [3.0, 0.0], [3.0, 1.0], [2.0, 1.0], [2.0, 0.0]]]
        multi = {"type": "MultiPolygon",
                 "coordinates": [UNIT_SQUARE["coordinates"], second]}
        mask = polygon_mask(np.array([0.5]), np.array([0.5, 1.5, 2.5]), multi)
        self.assertEqual(mask.tolist(), [[True, False, True]])


class TestBandMeans(unittest.TestCase):
    """Offline tests for elevation-band assignment with an injected synthetic DEM."""

    def make_dem(self):
        # Elevation increases northward from 1000 m to 3000 m across five rows.
        return {"elevation_m": np.repeat(np.array([[3000.0], [2500.0], [2000.0], [1500.0],
                                                   [1000.0]]), 5, axis=1),
                "lats": np.array([0.9, 0.7, 0.5, 0.3, 0.1]),
                "lons": np.array([0.1, 0.3, 0.5, 0.7, 0.9])}

    def test_sample_dem_at_nearest_neighbor(self):
        dem = self.make_dem()
        sampled = sample_dem_at(dem, np.array([0.88, 0.12]), np.array([0.5]))
        self.assertEqual(sampled[0, 0], 3000.0)
        self.assertEqual(sampled[1, 0], 1000.0)

    def test_band_means_follow_elevation(self):
        dem = self.make_dem()
        lats, lons = dem["lats"], dem["lons"]
        # SWE mirrors elevation: 0 below 2000 m and 100 mm above 2400 m.
        swe = np.where(dem["elevation_m"] >= 2400.0, 100.0,
                       np.where(dem["elevation_m"] >= 2000.0, 20.0, 0.0))
        result = band_mean_swe(swe, lats, lons, UNIT_SQUARE, [1000.0, 2000.0, 3000.0], dem=dem)
        # Band edges fall at 1500/2500 m: the low band is the 1000 m row, the middle band the
        # 1500 m (0 mm) and 2000 m (20 mm) rows, and the top band the 2500/3000 m rows.
        self.assertEqual(result["band_mean_swe_mm"][0], 0.0)
        self.assertEqual(result["band_mean_swe_mm"][1], 10.0)
        self.assertGreater(result["band_mean_swe_mm"][2], 90.0)
        self.assertEqual(sum(result["band_cell_counts"]), 25)

    def test_band_elevations_must_ascend(self):
        dem = self.make_dem()
        with self.assertRaises(ValueError):
            band_mean_swe(np.zeros((5, 5)), dem["lats"], dem["lons"], UNIT_SQUARE,
                          [2000.0, 1000.0], dem=dem)


class TestDemLive(unittest.TestCase):
    """Live terrain-tile fetch and full band pipeline over the Cache la Poudre basin."""

    @classmethod
    def setUpClass(cls):
        from usgs_scraping_functions import get_basin_boundary
        cls.basin = get_basin_boundary("06752260")

    def test_sample_dem_grid_poudre_headwaters(self):
        from dem_functions import sample_dem_grid
        dem = sample_dem_grid([-105.9, 40.4, -105.1, 41.0], zoom=8)
        self.assertGreater(dem["elevation_m"].max(), 3500.0)
        self.assertGreater(dem["elevation_m"].min(), 1000.0)
        self.assertTrue((np.diff(dem["lats"]) < 0).all())

    def test_snodas_band_means_june_2024(self):
        from snodas_functions import get_snodas_swe_grid
        from swe_assimilation import equal_area_bands
        bands = equal_area_bands(2511.2, 482.4)  # GAGES-II stats for site 06752260
        grid = get_snodas_swe_grid(datetime(2024, 6, 1))
        result = band_mean_swe(grid["swe_mm"], grid["lats"], grid["lons"], self.basin,
                               bands["elevations_m"], zoom=8)
        self.assertTrue(all(count > 0 for count in result["band_cell_counts"]))
        # June 1 2024: bare below ~2800 m, substantial snow in the highest band.
        self.assertLess(result["band_mean_swe_mm"][0], 5.0)
        self.assertGreater(result["band_mean_swe_mm"][-1], 100.0)
        self.assertGreater(result["basin_mean_swe_mm"], 20.0)


if __name__ == "__main__":
    unittest.main()
