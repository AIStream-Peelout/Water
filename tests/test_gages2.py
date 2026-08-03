import io
import os
import tempfile
import unittest
import zipfile

import pandas as pd

from gages2_functions import (download_gages2, load_gages2_table, get_gages2_attributes,
                              INNER_ZIP_NAME, DEFAULT_TABLES)

# Reuse an already-downloaded archive when available (e.g. a local dev cache) to avoid re-downloading
# the ~55 MB file; CI falls back to a real download once per run.
CACHE_ENV_VAR = "GAGES2_ZIP_PATH"


def make_synthetic_archive(path: str) -> None:
    """
    Builds a tiny GAGES-II-shaped archive (outer zip containing the inner CSV zip) for offline tests.

    :param path: Where to write the synthetic outer zip.
    :type path: str
    :return: None
    :rtype: None
    """
    topo = pd.DataFrame({"STAID": ["01010500", "06752260"], "SLOPE_PCT": [7.5, 21.3]})
    soils = pd.DataFrame({"STAID": ["01010500", "06752260"], "AWCAVE": [0.13, 0.11]})
    inner_buffer = io.BytesIO()
    with zipfile.ZipFile(inner_buffer, "w") as inner:
        inner.writestr("conterm_topo.txt", topo.to_csv(index=False))
        inner.writestr("conterm_soils.txt", soils.to_csv(index=False))
    with zipfile.ZipFile(path, "w") as outer:
        outer.writestr(INNER_ZIP_NAME, inner_buffer.getvalue())


class TestGages2Offline(unittest.TestCase):
    """Offline tests of the table loading and join logic against a synthetic archive."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.zip_path = os.path.join(self.temp_dir, "synthetic_gages2.zip")
        make_synthetic_archive(self.zip_path)

    def test_load_table_staid_is_string(self):
        table = load_gages2_table("conterm_topo.txt", self.zip_path)
        self.assertEqual(table["STAID"].iloc[0], "01010500")

    def test_attributes_merged_across_tables(self):
        attributes = get_gages2_attributes("06752260", self.zip_path,
                                           tables=["conterm_topo.txt", "conterm_soils.txt"])
        self.assertAlmostEqual(attributes["SLOPE_PCT"], 21.3)
        self.assertAlmostEqual(attributes["AWCAVE"], 0.11)
        self.assertNotIn("STAID", attributes)

    def test_unknown_gauge_raises(self):
        with self.assertRaises(KeyError):
            get_gages2_attributes("99999999", self.zip_path, tables=["conterm_topo.txt"])


class TestGages2Live(unittest.TestCase):
    """Live test against the real USGS archive (downloaded once, or reused via GAGES2_ZIP_PATH)."""

    @classmethod
    def setUpClass(cls):
        cls.zip_path = os.environ.get(CACHE_ENV_VAR)
        if not cls.zip_path or not os.path.exists(cls.zip_path):
            cls.zip_path = os.path.join(tempfile.mkdtemp(), "gages2.zip")
            download_gages2(cls.zip_path)

    def test_poudre_attributes(self):
        attributes = get_gages2_attributes("06752260", self.zip_path)
        self.assertIn("SLOPE_PCT", attributes)
        self.assertIn("AWCAVE", attributes)
        self.assertIn("DRAIN_SQKM", attributes)
        self.assertGreater(attributes["SLOPE_PCT"], 0.0)
        self.assertGreater(attributes["DRAIN_SQKM"], 100.0)

    def test_default_tables_all_present(self):
        for table_name in DEFAULT_TABLES:
            table = load_gages2_table(table_name, self.zip_path)
            self.assertGreater(len(table), 9000)
            self.assertIn("STAID", table.columns)


if __name__ == "__main__":
    unittest.main()
