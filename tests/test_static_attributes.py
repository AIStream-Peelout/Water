import unittest

from usgs_scraping_functions import get_site_metadata, get_basin_boundary, basin_bounding_box


class TestStaticAttributes(unittest.TestCase):
    """Live tests for NWIS expanded site metadata and NLDI basin boundaries."""

    def test_get_site_metadata(self):
        metadata = get_site_metadata("01010500")
        self.assertEqual(metadata["site_no"], "01010500")
        self.assertAlmostEqual(metadata["drain_area_va"], 2695.0)
        self.assertAlmostEqual(metadata["dec_lat_va"], 47.113, delta=0.01)
        self.assertIn("huc_cd", metadata)

    def test_get_basin_boundary(self):
        geometry = get_basin_boundary("01010500")
        self.assertIn(geometry["type"], ("Polygon", "MultiPolygon"))
        min_lon, min_lat, max_lon, max_lat = basin_bounding_box(geometry, buffer_degrees=0.05)
        self.assertLess(min_lon, max_lon)
        self.assertLess(min_lat, max_lat)
        # The gauge itself must fall inside the (buffered) basin bounding box.
        self.assertTrue(min_lon <= -69.088 <= max_lon)
        self.assertTrue(min_lat <= 47.113 <= max_lat)

    def test_bounding_box_multipolygon(self):
        geometry = {"type": "MultiPolygon",
                    "coordinates": [[[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]],
                                    [[[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 2.0]]]]}
        self.assertEqual(basin_bounding_box(geometry), (0.0, 0.0, 3.0, 3.0))


if __name__ == "__main__":
    unittest.main()
