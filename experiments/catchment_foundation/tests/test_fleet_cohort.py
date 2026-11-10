"""Tests for the split-restricted manifest build and multi-fold embedding holdouts."""
import json
import os
import tempfile
import unittest
from unittest import mock

import numpy as np
import pandas as pd
import torch

import train_catchment_embeddings as embedding_trainer
from experiments.catchment_foundation import build_manifest

SPLIT_SITES = {
    "a": {"fold": 0, "state": "CO"},
    "b": {"fold": 1, "state": "UT"},
    "c": {"fold": 2, "state": "CO"},
    "d": {"fold": 2, "state": "FL"},
}


class TestSiteSelection(unittest.TestCase):
    """Checks which gauges a split-restricted build reads and how the holdout is drawn."""

    def test_split_restricts_sites_to_the_state_it_assigns_them(self):
        completed = ["a", "b", "c", "x"]  # x is completed but not in the split
        self.assertEqual(build_manifest.select_sites(completed, SPLIT_SITES, "CO"), ["a", "c"])
        self.assertEqual(build_manifest.select_sites(completed, SPLIT_SITES, "UT"), ["b"])
        self.assertEqual(build_manifest.select_sites(completed, SPLIT_SITES, "FL"), [])

    def test_no_split_keeps_every_completed_gauge(self):
        self.assertEqual(build_manifest.select_sites(["a", "x"], None, "CO"), ["a", "x"])

    def _basins(self):
        return [{"site_id": "s%d" % i, "has_embedding": i != 3,
                 "rows_2023_plus": 0 if i == 4 else 24 * 200, "split": "train"}
                for i in range(10)]

    def test_zero_holdout_leaves_every_basin_in_training(self):
        basins = self._basins()
        self.assertEqual(build_manifest.assign_random_holdout(basins, {}, 0, seed=42), [])
        self.assertEqual({basin["split"] for basin in basins}, {"train"})

    def test_random_holdout_is_seeded_and_skips_ineligible_basins(self):
        basins = self._basins()
        carried = {"s0": "basin_valid"}
        held = build_manifest.assign_random_holdout(basins, carried, 3, seed=42)
        eligible = ["s%d" % i for i in range(10) if i not in (0, 3, 4)]
        expected = sorted(np.random.default_rng(42).choice(eligible, size=3,
                                                           replace=False).tolist())
        self.assertEqual(held, expected)
        self.assertEqual(sorted(b["site_id"] for b in basins if b["split"] == "holdout"),
                         expected)
        self.assertTrue({"s0", "s3", "s4"}.isdisjoint(held))

    def test_gauge_elevation_prefers_nwis_feet_then_gages2_metres(self):
        self.assertAlmostEqual(build_manifest.gauge_elevation_m(
            {"alt_va": 1000.0, "gages2_ELEV_SITE_M": 5}), 304.8)
        self.assertEqual(build_manifest.gauge_elevation_m(
            {"alt_va": None, "gages2_ELEV_SITE_M": 10}), 10.0)
        self.assertEqual(build_manifest.gauge_elevation_m(
            {"alt_va": "", "gages2_ELEV_SITE_M": "12.5"}), 12.5)
        self.assertIsNone(build_manifest.gauge_elevation_m({"alt_va": None}))

    def test_bank_with_numpy_statistics_still_yields_site_ids(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "embeddings_concat.pt")
            torch.save({"site_ids": ["a", "b"], "embeddings": torch.zeros(2, 4),
                        "static_mean": np.zeros(3, dtype=np.float32),
                        "static_std": np.ones(3, dtype=np.float32)}, path)
            self.assertEqual(build_manifest.load_embedding_sites(path), {"a", "b"})


class TestBasinEntry(unittest.TestCase):
    """Builds entries from a small synthetic scrape tree shaped like a Florida gauge."""

    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.scrapes = os.path.join(self.folder.name, "scrapes")
        patches = [mock.patch.object(build_manifest, "SCRAPE_ROOT", self.scrapes),
                   mock.patch.object(build_manifest, "SNODAS_ROOT",
                                     os.path.join(self.folder.name, "snodas_series"))]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def _write_gauge(self, site, static, train_hours=3 * 365 * 24, recent_hours=150 * 24):
        directory = os.path.join(self.scrapes, "FL", site)
        os.makedirs(directory)
        with open(os.path.join(directory, site + "_static.json"), "w") as file:
            json.dump(static, file)
        end = pd.Timestamp("2022-01-01", tz="UTC")
        times = pd.date_range(end - pd.Timedelta(hours=train_hours), periods=train_hours,
                              freq="h").append(pd.date_range("2023-01-01", periods=recent_hours,
                                                             freq="h", tz="UTC"))
        rng = np.random.default_rng(0)
        frame = pd.DataFrame({"datetime": times.astype(str)})
        frame["cfs"] = 10.0 + rng.normal(size=len(frame))
        frame.loc[frame.index % 10 == 0, "cfs"] = -5.0  # tidal reversals: 10% of hours
        for column in build_manifest.MET_COLS:
            if column != "p01m":  # the station column is optional
                frame[column] = rng.normal(size=len(frame))
        frame.to_csv(os.path.join(directory, site + "_hourly_full.csv"), index=False)

    def test_entry_for_a_gauge_without_altitude_or_snow_series(self):
        self._write_gauge("02000001", {"gages2_DRAIN_SQKM": 100.0, "alt_va": None,
                                       "gages2_ELEV_SITE_M": 10, "gages2_ELEV_MEAN_M_BASIN": 110.0})
        skipped = {}
        entry = build_manifest.build_basin_entry("02000001", {"02000001"}, 2 * 365 * 24,
                                                 state="FL", skipped=skipped)
        self.assertEqual(skipped, {})
        self.assertEqual(entry["split"], "train")
        self.assertTrue(entry["has_embedding"])
        self.assertNotIn("swe_csv_path", entry)
        self.assertAlmostEqual(entry["temp_offset_c"], -0.65)  # 100 m above the gauge
        self.assertEqual(entry["train_valid_hours"], 3 * 365 * 24)
        self.assertEqual(entry["rows_2023_plus"], 150 * 24)
        self.assertEqual(entry["flow_hours_2023_plus"], 150 * 24)
        self.assertAlmostEqual(entry["negative_flow_fraction"], 0.1, places=3)
        self.assertEqual(sorted(entry["met_stats"]), sorted(build_manifest.MET_COLS))

    def test_unusable_gauges_report_why(self):
        self._write_gauge("02000002", {"gages2_DRAIN_SQKM": None, "drain_area_va": ""})
        self._write_gauge("02000003", {"gages2_DRAIN_SQKM": 100.0}, train_hours=24 * 30)
        skipped = {}
        for site in ("02000002", "02000003", "02000004"):
            self.assertIsNone(build_manifest.build_basin_entry(site, set(), 2 * 365 * 24,
                                                               state="FL", skipped=skipped))
        self.assertEqual(skipped, {"02000002": "no drainage area",
                                   "02000003": "too few valid pre-cutoff flow hours",
                                   "02000004": "missing files"})


class TestHoldoutFolds(unittest.TestCase):
    """Checks which sites the embedding trainer holds out for one or several folds."""

    def test_single_fold_matches_the_earlier_behaviour(self):
        self.assertEqual(embedding_trainer.select_holdout_sites(SPLIT_SITES, [0], None), ["a"])
        self.assertEqual(embedding_trainer.select_holdout_sites(SPLIT_SITES, [2], None),
                         ["c", "d"])

    def test_several_folds_are_held_out_together(self):
        self.assertEqual(embedding_trainer.select_holdout_sites(SPLIT_SITES, [0, 1], None),
                         ["a", "b"])

    def test_state_holdout_alone_and_combined_with_folds(self):
        self.assertEqual(embedding_trainer.select_holdout_sites(SPLIT_SITES, None, "CO"),
                         ["a", "c"])
        self.assertEqual(embedding_trainer.select_holdout_sites(SPLIT_SITES, [1], "FL"),
                         ["b", "d"])
        self.assertEqual(embedding_trainer.select_holdout_sites(SPLIT_SITES, None, None), [])

    def test_unknown_fold_is_rejected(self):
        with self.assertRaises(ValueError):
            embedding_trainer.select_holdout_sites(SPLIT_SITES, [0, 7], None)

    def test_summary_keeps_a_single_fold_as_a_number(self):
        self.assertIsNone(embedding_trainer.summary_holdout_fold(None))
        self.assertEqual(embedding_trainer.summary_holdout_fold([0]), 0)
        self.assertEqual(embedding_trainer.summary_holdout_fold([0, 1]), [0, 1])
        self.assertEqual(embedding_trainer.describe_holdout([0], None), "fold 0")
        self.assertEqual(embedding_trainer.describe_holdout([0, 1], None), "folds 0, 1")
        self.assertEqual(embedding_trainer.describe_holdout([2], "CO"), "fold 2 and state CO")
        self.assertEqual(embedding_trainer.describe_holdout(None, "CO"), "state CO")

    def test_training_links_exclude_held_out_records_even_when_the_directory_is_reused(self):
        with tempfile.TemporaryDirectory() as folder:
            combined = os.path.join(folder, "combined_records")
            training = os.path.join(folder, "training_records")
            os.makedirs(combined)
            for site in ("a", "b", "c"):
                with open(os.path.join(combined, site + ".npz"), "w") as file:
                    file.write(site)

            self.assertEqual(embedding_trainer.link_training_records(combined, training, {"b"}), 2)
            self.assertEqual(sorted(os.listdir(training)), ["a.npz", "c.npz"])
            # A second run that also holds out "a" must not keep the first run's link to it.
            self.assertEqual(
                embedding_trainer.link_training_records(combined, training, {"a", "b"}), 1)
            self.assertEqual(os.listdir(training), ["c.npz"])
            self.assertTrue(os.path.exists(os.path.join(combined, "a.npz")))

            with open(os.path.join(training, "b.npz"), "w") as file:
                file.write("a real file, not a link")
            with self.assertRaises(SystemExit):
                embedding_trainer.link_training_records(combined, training, {"a", "b"})


if __name__ == "__main__":
    unittest.main()
