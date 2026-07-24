from datetime import datetime
import json
import os
import tempfile
import unittest

import pandas as pd

from long_term_scrape import chunk_bounds, run_long_term_scrape
from usgs_scraping_functions import get_period_of_record


class TestChunkBounds(unittest.TestCase):
    """Offline tests for calendar chunking."""

    def test_yearly_chunks_align_to_calendar(self):
        bounds = chunk_bounds(datetime(1987, 12, 31), datetime(1990, 1, 1), chunk_months=12)
        self.assertEqual(bounds[0], (datetime(1987, 12, 31), datetime(1988, 12, 1)))
        self.assertEqual(bounds[1], (datetime(1988, 12, 1), datetime(1989, 12, 1)))
        self.assertEqual(bounds[-1][1], datetime(1990, 1, 1))

    def test_chunks_abut_exactly(self):
        bounds = chunk_bounds(datetime(2020, 3, 15), datetime(2022, 7, 1), chunk_months=6)
        for previous, current in zip(bounds, bounds[1:]):
            self.assertEqual(previous[1], current[0])
        self.assertEqual(bounds[0][0], datetime(2020, 3, 15))
        self.assertEqual(bounds[-1][1], datetime(2022, 7, 1))

    def test_monthly_chunks(self):
        bounds = chunk_bounds(datetime(2024, 1, 1), datetime(2024, 4, 1), chunk_months=1)
        self.assertEqual(len(bounds), 3)


class TestPeriodOfRecord(unittest.TestCase):
    """Live test of the NWIS series catalog lookup."""

    def test_poudre_period_of_record(self):
        catalog = get_period_of_record("06752260")
        self.assertIn("uv_00060", catalog)
        self.assertIn("dv_00060", catalog)
        self.assertEqual(catalog["uv_00060"]["begin_date"], "1987-12-31")
        # The daily record begins earlier than the instantaneous record.
        self.assertLess(catalog["dv_00060"]["begin_date"], catalog["uv_00060"]["begin_date"])


class TestLongTermScrape(unittest.TestCase):
    """Live two-chunk smoke test of the resumable scrape (no NLDAS, no backup)."""

    def test_two_monthly_chunks_and_resume(self):
        with tempfile.TemporaryDirectory() as out_dir:
            summary = run_long_term_scrape("06752260", start_time=datetime(2024, 5, 1),
                                           end_time=datetime(2024, 7, 1), chunk_months=1,
                                           output_dir=out_dir, include_nldas=False, backup=False)
            self.assertEqual(summary["chunks_fetched"], 2)
            self.assertEqual(summary["chunk_failures"], [])
            self.assertGreater(summary["combined_rows"], 1400)
            combined = pd.read_csv(os.path.join(out_dir, "06752260_hourly_full.csv"))
            self.assertFalse(combined["datetime"].duplicated().any())
            self.assertIn("tmpf", combined.columns)
            self.assertTrue(os.path.exists(os.path.join(out_dir, "06752260_static.json")))
            # Second run should skip both existing chunks (resume behavior).
            resumed = run_long_term_scrape("06752260", start_time=datetime(2024, 5, 1),
                                           end_time=datetime(2024, 7, 1), chunk_months=1,
                                           output_dir=out_dir, include_nldas=False, backup=False)
            self.assertEqual(resumed["chunks_fetched"], 0)
            self.assertEqual(resumed["chunks_skipped_existing"], 2)
            with open(os.path.join(out_dir, "06752260_scrape_summary.json")) as f:
                self.assertEqual(json.load(f)["chunks_skipped_existing"], 2)


if __name__ == "__main__":
    unittest.main()
