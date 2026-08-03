from datetime import datetime
import os
import tempfile
import unittest

import numpy as np

from sentinel_functions import (parse_product_id, latlon_to_mgrs_tile, list_sentinel_safes,
                                find_granule_prefix, get_cloud_cover, extract_patch,
                                build_patch_time_series)

POUDRE_LAT, POUDRE_LON = 40.588, -105.069


class TestProductParsing(unittest.TestCase):
    """Offline tests for product-id parsing and tile derivation."""

    def test_parse_product_id(self):
        parsed = parse_product_id(
            "tiles/13/T/DE/S2B_MSIL1C_20240609T174909_N0510_R141_T13TDE_20240609T205810.SAFE/")
        self.assertEqual(parsed["satellite"], "S2B")
        self.assertEqual(parsed["tile"], "13TDE")
        self.assertEqual(parsed["orbit"], "141")
        self.assertEqual(parsed["sensing_time"].isoformat(), "2024-06-09T17:49:09+00:00")

    def test_parse_rejects_non_l1c(self):
        self.assertIsNone(parse_product_id("tiles/13/T/DE/index.csv"))

    def test_latlon_to_mgrs(self):
        self.assertEqual(latlon_to_mgrs_tile(POUDRE_LAT, POUDRE_LON), "13TDE")


class TestSentinelListing(unittest.TestCase):
    """Live tests against the public GCS Sentinel-2 bucket (anonymous access)."""

    def test_list_scenes_in_range(self):
        scenes = list_sentinel_safes("13TDE", datetime(2024, 6, 1), datetime(2024, 6, 30))
        self.assertGreaterEqual(len(scenes), 5)
        self.assertTrue(scenes["sensing_time"].is_monotonic_increasing)
        self.assertTrue((scenes["tile"] == "13TDE").all())

    def test_find_granule_and_cloud_cover(self):
        scenes = list_sentinel_safes("13TDE", datetime(2024, 6, 9), datetime(2024, 6, 10))
        self.assertEqual(len(scenes), 1)
        granule = find_granule_prefix(scenes["safe_prefix"].iloc[0])
        self.assertIn("GRANULE/L1C_T13TDE", granule)
        cloud = get_cloud_cover(scenes["safe_prefix"].iloc[0])
        self.assertGreaterEqual(cloud, 0.0)
        self.assertLessEqual(cloud, 100.0)


class TestPatchExtraction(unittest.TestCase):
    """Live streamed patch extraction; small patch to keep runtime and transfer modest."""

    def test_extract_and_manifest(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest = build_patch_time_series(
                POUDRE_LAT, POUDRE_LON, datetime(2024, 6, 9), datetime(2024, 6, 10),
                output_dir=out_dir, bands=("B04", "B08"), patch_size=32, max_scenes=1)
            self.assertEqual(len(manifest), 1)
            self.assertTrue(os.path.exists(manifest["patch_path"].iloc[0]))
            patch = np.load(manifest["patch_path"].iloc[0])
            self.assertEqual(patch.shape, (2, 32, 32))
            self.assertGreater(manifest["valid_fraction"].iloc[0], 0.5)
            self.assertGreater(patch.max(), 0.0)
            self.assertTrue(os.path.exists(os.path.join(out_dir, "sentinel_manifest.csv")))


if __name__ == "__main__":
    unittest.main()
