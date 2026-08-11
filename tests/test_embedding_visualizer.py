import os
import tempfile
import unittest

import numpy as np

from embedding_visualizer import CatchmentVisualization


def write_records(data_dir: str, n_sites: int = 10) -> list:
    """Writes minimal synthetic embedding records and returns the site ids."""
    rng = np.random.default_rng(3)
    site_ids = []
    for i in range(n_sites):
        site = "%08d" % i
        site_ids.append(site)
        np.savez_compressed(
            os.path.join(data_dir, site + ".npz"),
            image=rng.uniform(0, 3000, (4, 16, 16)).astype(np.float32),
            history=rng.uniform(1, 500, 400).astype(np.float32), history_start="2020-01-01",
            static=rng.standard_normal(4).astype(np.float32),
            static_names=np.array(["ELEV_MEAN_M_BASIN", "SNOW_PCT_PRECIP", "DRAIN_SQKM",
                                   "SLOPE_PCT"], dtype=str))
    return site_ids


class TestCatchmentVisualization(unittest.TestCase):
    """Offline tests for the interactive embedding explorer builder."""

    def test_build_dataframe_and_html(self):
        with tempfile.TemporaryDirectory() as data_dir:
            site_ids = write_records(data_dir)
            embeddings = np.random.default_rng(0).standard_normal((10, 32))
            visualizer = CatchmentVisualization(output_dir=data_dir)
            df = visualizer.create_visualization_dataframe(data_dir, site_ids, embeddings,
                                                           n_neighbors=3, n_clusters=3)
            self.assertEqual(len(df), 10)
            self.assertTrue(df["image_base64"].str.startswith("data:image/jpeg").all())
            self.assertTrue(df["hydrograph_base64"].str.startswith("data:image/png").all())
            self.assertEqual(len(df["neighbors"].iloc[0]), 3)
            self.assertNotIn(df["site_no"].iloc[0],
                             [n["site"] for n in df["neighbors"].iloc[0]])

            path = visualizer.create_main_visualization(df, "test_explorer.html")
            self.assertTrue(os.path.exists(path))
            with open(path) as f:
                html = f.read()
            self.assertIn("plotly_click", html)
            self.assertIn("catchment-popup", html)
            self.assertIn("Nearest in embedding space", html)


if __name__ == "__main__":
    unittest.main()
