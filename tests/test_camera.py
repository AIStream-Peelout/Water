from datetime import datetime, timedelta, timezone
import os
import tempfile
import unittest

from camera_functions import (list_cameras, find_camera_prefix, list_camera_images,
                              download_camera_images)

NATHROP_PREFIX = "overlay/CO_Arkansas_River_near_Nathrop/"


class TestCameraDiscovery(unittest.TestCase):
    """Live tests against the public usgs-nims-images S3 bucket."""

    def test_list_cameras_colorado(self):
        cameras = list_cameras("CO")
        self.assertGreater(len(cameras), 10)
        self.assertIn("CO_Arkansas_River_near_Nathrop", cameras)

    def test_find_camera_from_nwis_name(self):
        prefix = find_camera_prefix("CO", "ARKANSAS RIVER NEAR NATHROP, CO.")
        self.assertEqual(prefix, NATHROP_PREFIX)

    def test_gauge_without_camera_returns_none(self):
        prefix = find_camera_prefix("CO", "CACHE LA POUDRE RIVER AT FORT COLLINS, CO")
        self.assertIsNone(prefix)


class TestCameraImages(unittest.TestCase):
    """Live listing and a tiny download; uses a recent window since the bucket retention rolls."""

    def setUp(self):
        self.end = datetime.now(timezone.utc).replace(tzinfo=None)
        self.start = self.end - timedelta(days=2)

    def test_list_recent_images(self):
        listing = list_camera_images(NATHROP_PREFIX, self.start, self.end)
        self.assertGreater(len(listing), 20)  # ~15-minute cadence
        self.assertEqual(str(listing["datetime"].dt.tz), "UTC")
        self.assertTrue(listing["datetime"].is_monotonic_increasing)

    def test_download_subsampled(self):
        with tempfile.TemporaryDirectory() as out_dir:
            manifest = download_camera_images(NATHROP_PREFIX, out_dir, self.start, self.end,
                                              min_interval_minutes=30.0, max_images=2)
            self.assertEqual(len(manifest), 2)
            for path in manifest["image_path"]:
                self.assertTrue(os.path.exists(path))
                self.assertGreater(os.path.getsize(path), 10000)
            spacing = manifest["datetime"].iloc[1] - manifest["datetime"].iloc[0]
            self.assertGreaterEqual(spacing.total_seconds(), 30 * 60)
            self.assertTrue(os.path.exists(os.path.join(out_dir, "camera_manifest.csv")))


if __name__ == "__main__":
    unittest.main()
