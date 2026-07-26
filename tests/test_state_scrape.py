import os
import tempfile
import unittest

from state_scrape import list_state_gauges, load_registry, save_registry, registry_report


class TestGaugeEnumeration(unittest.TestCase):
    """Live test of the NWIS state gauge listing."""

    def test_list_colorado_gauges(self):
        gauges = list_state_gauges("CO")
        self.assertGreater(len(gauges), 200)
        self.assertIn("06752260", gauges["site_no"].values)
        self.assertTrue(gauges["site_no"].str.len().ge(8).all())


class TestRegistry(unittest.TestCase):
    """Offline tests of the progress registry."""

    def test_registry_roundtrip_and_report(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "registry.json")
            self.assertEqual(load_registry(path), {})
            registry = {"111": {"status": "completed"}, "222": {"status": "failed", "error": "x"},
                        "333": {"status": "completed"}}
            save_registry(registry, path, gcs_prefix=None)
            self.assertEqual(load_registry(path), registry)
            self.assertEqual(registry_report(registry), {"completed": 2, "failed": 1})


if __name__ == "__main__":
    unittest.main()
