"""Tests of the neural forecaster's windows and scoring helpers (decoder_forecaster.py)."""
import os
import sys
import unittest

import numpy as np
import pandas as pd
import torch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
if os.environ.get("FF_REPO"):
    sys.path.insert(0, os.environ["FF_REPO"])

from experiments.catchment_foundation import decoder_forecaster as forecaster  # noqa: E402
from experiments.catchment_foundation import pretrain_behaviour_code as behaviour  # noqa: E402


def basin(days: int = 400, seed: int = 0) -> pd.DataFrame:
    index = pd.date_range("2019-01-01", periods=days, freq="1D")
    rng = np.random.default_rng(seed)
    return pd.DataFrame({"q_mm": 10.0 + np.arange(days, dtype=float), "p": rng.gamma(0.5, 4.0, days),
                         "t": rng.normal(8.0, 6.0, days), "pet": rng.gamma(2.0, 1.0, days)},
                        index=index).astype(np.float32)


class TestForecastWindows(unittest.TestCase):
    def setUp(self):
        self.daily = {"a": basin()}
        self.scale = {"a": 0.5}                                  # 12 mm per day is one unit
        self.description = pd.DataFrame({"x": [1.0], "y": [2.0]}, index=["a"])
        self.issue = self.daily["a"].index[250]
        rows = pd.DataFrame({"site_id": ["a"], "date": [self.issue]})
        self.windows = forecaster.ForecastWindows(self.daily, self.scale, rows, self.description,
                                                  history_days=30, clip_target=None)

    def test_history_is_known_and_the_horizon_is_the_target(self):
        item = self.windows[0]
        drivers, target = item["drivers"].numpy(), item["target"].numpy()
        self.assertEqual(drivers.shape, (30 + forecaster.HORIZON, forecaster.N_DRIVERS))
        np.testing.assert_array_equal(drivers[:30, 6], 1.0)
        np.testing.assert_array_equal(drivers[30:, 6], 0.0)
        np.testing.assert_array_equal(drivers[30:, 5], 0.0)
        self.assertAlmostEqual(float(drivers[29, 5]), 0.0, places=6)          # the issue day itself
        self.assertAlmostEqual(float(drivers[28, 5]), -1.0 / 12.0, places=5)  # one day earlier
        self.assertTrue(np.isnan(target[:30]).all())
        np.testing.assert_allclose(target[30:], np.arange(1, 15) / 12.0, atol=1e-5)
        self.assertAlmostEqual(float(drivers[0, 7]), np.log(260.0 + behaviour.FLOW_FLOOR_MM), places=4)
        np.testing.assert_array_equal(item["statics"].numpy(), [1.0, 2.0])

    def test_weather_covers_history_and_horizon(self):
        drivers = self.windows[0]["drivers"].numpy()
        expected = behaviour.weather_channels(self.daily["a"])[250 - 29:250 + forecaster.HORIZON + 1]
        np.testing.assert_allclose(drivers[:, :5], expected, atol=1e-6)

    def test_inputs_do_not_depend_on_flow_after_the_issue_day(self):
        changed = {"a": self.daily["a"].copy()}
        changed["a"].loc[changed["a"].index > self.issue, "q_mm"] *= 9.0
        rows = pd.DataFrame({"site_id": ["a"], "date": [self.issue]})
        again = forecaster.ForecastWindows(changed, self.scale, rows, self.description,
                                           history_days=30, clip_target=None)
        np.testing.assert_array_equal(self.windows[0]["drivers"].numpy(), again[0]["drivers"].numpy())
        moved = np.abs(self.windows[0]["target"].numpy() - again[0]["target"].numpy())
        self.assertGreater(float(np.nanmax(moved)), 1.0)

    def test_a_window_before_the_record_starts_is_padded(self):
        rows = pd.DataFrame({"site_id": ["a"], "date": [self.daily["a"].index[10]]})
        item = forecaster.ForecastWindows(self.daily, self.scale, rows, self.description,
                                          history_days=30)[0]
        drivers = item["drivers"].numpy()
        np.testing.assert_array_equal(drivers[:19, :7], 0.0)
        np.testing.assert_array_equal(drivers[19:30, 6], 1.0)

    def test_missing_flow_is_flagged_unknown(self):
        gappy = {"a": self.daily["a"].copy()}
        gappy["a"].iloc[240, gappy["a"].columns.get_loc("q_mm")] = np.nan
        rows = pd.DataFrame({"site_id": ["a"], "date": [self.issue]})
        drivers = forecaster.ForecastWindows(gappy, self.scale, rows, self.description,
                                             history_days=30)[0]["drivers"].numpy()
        self.assertEqual(float(drivers[19, 6]), 0.0)
        self.assertEqual(float(drivers[19, 5]), 0.0)

    def test_targets_are_clipped_for_training(self):
        rows = pd.DataFrame({"site_id": ["a"], "date": [self.issue]})
        clipped = forecaster.ForecastWindows(self.daily, self.scale, rows, self.description,
                                             history_days=30, clip_target=0.5)[0]["target"].numpy()
        self.assertAlmostEqual(float(np.nanmax(clipped)), 0.5, places=6)

    def test_random_draws_come_from_the_rows(self):
        rows = pd.DataFrame({"site_id": ["a", "a"], "date": [self.issue, self.daily["a"].index[300]]})
        drawn = forecaster.ForecastWindows(self.daily, self.scale, rows, self.description,
                                           history_days=30, samples=7, seed=1)
        self.assertEqual(len(drawn), 7)
        levels = {round(float(drawn[i]["drivers"][0, 7]), 3) for i in range(7)}
        self.assertTrue(levels <= {round(float(np.log(260.01)), 3), round(float(np.log(310.01)), 3)})


class TestScoring(unittest.TestCase):
    def test_band_means_average_the_right_days(self):
        daily = np.tile(np.arange(1.0, 15.0), (2, 1))
        means = forecaster.band_means(daily)
        np.testing.assert_allclose(means["day1-3"], [2.0, 2.0])
        np.testing.assert_allclose(means["day4-7"], [5.5, 5.5])
        np.testing.assert_allclose(means["day8-14"], [11.0, 11.0])

    def test_predict_returns_fourteen_days_per_window(self):
        torch.manual_seed(0)
        daily, scale = {"a": basin()}, {"a": 0.5}
        description = pd.DataFrame({"x": [1.0], "y": [2.0]}, index=["a"])
        rows = pd.DataFrame({"site_id": ["a"] * 3, "date": daily["a"].index[[100, 200, 300]]})
        windows = forecaster.ForecastWindows(daily, scale, rows, description, history_days=30)
        reader, decoder = forecaster.build_models(2, code_dim=4, dim=8, hidden=8)
        self.assertEqual(forecaster.predict(reader, decoder, windows).shape, (3, forecaster.HORIZON))


if __name__ == "__main__":
    unittest.main()
