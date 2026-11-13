"""Tests for the tabular forecast baseline's feature/target alignment and scoring."""
import os
import tempfile
import unittest

import numpy as np
import pandas as pd

from experiments.catchment_foundation import tabular_forecast_baseline as tabular


class TestBasinTable(unittest.TestCase):
    """Targets must lie strictly after the issue day and features at or before it."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        hours = pd.date_range("2021-06-01", "2023-06-30 23:00", freq="h")
        day_number = (hours.normalize() - hours[0].normalize()).days.to_numpy()
        frame = pd.DataFrame({
            "datetime": hours,
            "cfs": 100.0 + day_number,                    # daily mean flow = 100 + day number
            "precipitation": 1.0,                          # 24 mm per day
            "temperature": 273.15 + day_number % 10,
            "shortwave_radiation": 200.0,
            "pet_mm_hr": 0.1,
        })
        path = os.path.join(cls.tmp.name, "basin.csv")
        frame.to_csv(path, index=False)
        cls.scale = 2.0
        cls.basin = {"site_id": "x", "split": "train", "state": "CO", "csv_path": path,
                     "area_sq_km": tabular.CFS_TO_MM_HR, "flow_scale_mm_hr": cls.scale}
        cls.table = tabular.basin_table(cls.basin, fit_stride=1, eval_stride=1)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_targets_are_band_means_after_the_issue_day_minus_issue_flow(self):
        row = self.table.loc[pd.Timestamp("2021-09-01")]
        # Flow rises one cfs per day, so the mean over days first..last is ahead by their mean.
        for band, (first, last) in tabular.BANDS.items():
            self.assertAlmostEqual(row[f"y_{band}"], (first + last) / 2.0 / self.scale)
        self.assertAlmostEqual(row["q0"] - row["q_1"], 1.0 / self.scale)
        self.assertAlmostEqual(row["q0"] - row["q_7"], 7.0 / self.scale)

    def test_weather_windows_cover_exactly_their_days(self):
        row = self.table.loc[pd.Timestamp("2021-09-01")]
        self.assertAlmostEqual(row["p_prev7"], 7 * 24.0)
        self.assertAlmostEqual(row["p_day1-3"], 3 * 24.0)
        self.assertAlmostEqual(row["p_day8-14"], 7 * 24.0)
        issue_day = (pd.Timestamp("2021-09-01") - pd.Timestamp("2021-06-01")).days
        expected = np.mean([(issue_day + lead) % 10 for lead in (4, 5, 6, 7)])
        self.assertAlmostEqual(row["t_day4-7"], expected)

    def test_periods_follow_the_cutoffs(self):
        periods = self.table.groupby("period").apply(lambda rows: (rows.index.min(), rows.index.max()))
        self.assertLess(periods["fit"][1], pd.Timestamp(tabular.CUTOFF))
        self.assertGreaterEqual(periods["2022"][0], pd.Timestamp(tabular.CUTOFF))
        self.assertLess(periods["2022"][1], pd.Timestamp(tabular.EVAL_START))
        self.assertGreaterEqual(periods["2023+"][0], pd.Timestamp(tabular.EVAL_START))


class TestScore(unittest.TestCase):
    """Scores are per basin against persistence and paired with the reference arm."""

    def test_perfect_and_persistence_forecasts(self):
        rows = 40
        data = pd.DataFrame({"site_id": ["a"] * 20 + ["b"] * 20, "split": "holdout",
                             "period": "2023+"})
        truth = np.linspace(-1.0, 1.0, rows)
        for band in tabular.BANDS:
            data[f"y_{band}"] = truth
        predictions = {}
        for band in tabular.BANDS:
            predictions[("flow+weather+snow", band)] = np.zeros(rows)       # persistence
            predictions[("flow", band)] = truth.copy()                       # perfect
            predictions[("flow+weather+snow+some_bank", band)] = 0.5 * truth  # free-form arm
        report = tabular.score(data, predictions, "flow+weather+snow", n_boot=50)
        band = report["holdout"]["2023+"]["bands"]["day1-3"]
        self.assertAlmostEqual(band["flow+weather+snow"]["median_basin_skill_pct"], 0.0)
        self.assertAlmostEqual(band["flow"]["median_basin_skill_pct"], 100.0)
        self.assertEqual(band["flow"]["better_than_reference_pct"], 100.0)
        self.assertEqual(report["holdout"]["2023+"]["basins"], 2)
        self.assertAlmostEqual(band["flow+weather+snow+some_bank"]["median_basin_skill_pct"], 75.0)
        self.assertEqual(list(band), ["flow+weather+snow", "flow", "flow+weather+snow+some_bank"])

    def test_arms_can_also_be_paired_with_other_arms(self):
        rows = 60
        data = pd.DataFrame({"site_id": np.repeat(["a", "b", "c"], 20), "split": "holdout",
                             "period": "2023+"})
        truth = np.tile(np.linspace(-1.0, 1.0, 20), 3)
        predictions = {}
        for band in tabular.BANDS:
            data[f"y_{band}"] = truth
            predictions[("flow+weather+snow", band)] = np.zeros(rows)
            predictions[("half", band)] = 0.5 * truth                 # error a quarter
            predictions[("most", band)] = 0.75 * truth                # error a sixteenth
        plain = tabular.score(data, predictions, "flow+weather+snow", n_boot=50)
        report = tabular.score(data, predictions, "flow+weather+snow", n_boot=50,
                               also_pair=["half"])
        band = report["holdout"]["2023+"]["bands"]["day4-7"]
        paired = band["most"]["paired"]["half"]
        self.assertAlmostEqual(paired["error_cut_pct"], 75.0)         # 1/16 against 1/4
        self.assertEqual(paired["better_pct"], 100.0)
        self.assertAlmostEqual(band["flow+weather+snow"]["paired"]["half"]["error_cut_pct"],
                               -300.0)
        self.assertNotIn("paired", band["half"])
        # The comparison with the reference is untouched by the extra pairs.
        self.assertEqual(band["most"]["error_cut_ci95"],
                         plain["holdout"]["2023+"]["bands"]["day4-7"]["most"]["error_cut_ci95"])

    def test_an_arm_is_a_named_arm_or_any_joined_list_of_groups(self):
        self.assertEqual(tabular.arm_groups("flow+weather"), ("flow", "weather"))
        self.assertEqual(tabular.arm_groups("flow+snow+some_bank"), ("flow", "snow", "some_bank"))


if __name__ == "__main__":
    unittest.main()
