"""Tests for the representation benchmark's behaviour numbers, donor search and scoring."""
import unittest

import numpy as np
import pandas as pd

from experiments.catchment_foundation import representation_benchmark as bench
from experiments.catchment_foundation import tabular_forecast_baseline as tabular


def synthetic_record(start: str, end: str, lag_hours: int = 5, response: float = 0.25):
    """A basin whose flow is a constant plus a share of the rain that fell ``lag_hours`` ago."""
    hours = pd.date_range(start, end, freq="h")
    day = (hours.normalize() - hours[0].normalize()).days.to_numpy()
    rain = np.where((day % 10 == 0) & (hours.hour < 6), 4.0, 0.0)       # 24 mm every tenth day
    cold = np.isin(hours.month, [12, 1, 2])
    flow_mm_hr = 0.01 + response * np.roll(rain, lag_hours)
    area = 100.0
    hourly = pd.DataFrame({"cfs": flow_mm_hr * area / tabular.CFS_TO_MM_HR,
                           "precipitation": rain,
                           "temperature": np.where(cold, 268.15, 283.15),
                           "shortwave_radiation": 200.0, "pet_mm_hr": 0.1}, index=hours)
    basin = {"area_sq_km": area, "flow_scale_mm_hr": 0.05}
    return hourly, tabular.daily_frame(hourly, basin), area


class TestBehaviour(unittest.TestCase):
    """The behaviour numbers must recover what a synthetic basin was built with."""

    @classmethod
    def setUpClass(cls):
        cls.hourly, cls.daily, cls.area = synthetic_record("2015-10-01", "2018-09-30 23:00")
        cls.numbers = bench.behaviour(cls.daily, cls.hourly, cls.area)

    def test_runoff_ratio_snow_share_and_aridity_match_the_construction(self):
        rain = self.hourly.precipitation.sum()
        flow = (self.hourly.cfs * tabular.CFS_TO_MM_HR / self.area).sum()
        self.assertAlmostEqual(self.numbers["log_runoff_ratio"], np.log(flow / rain + 1e-3),
                               places=6)
        cold = self.hourly.precipitation[self.hourly.temperature < 273.15].sum()
        self.assertAlmostEqual(self.numbers["snow_fraction"], cold / rain, places=6)
        self.assertAlmostEqual(self.numbers["log_aridity"],
                               np.log(0.1 * len(self.hourly) / rain + 1e-3), places=6)
        self.assertEqual(self.numbers["zero_flow_fraction"], 0.0)

    def test_lag_is_found_from_the_hourly_record(self):
        self.assertEqual(self.numbers["lag_hours"], 5.0)
        self.assertGreater(self.numbers["rain_flow_coupling"], 0.3)

    def test_flood_size_is_the_mean_annual_hourly_peak(self):
        self.assertAlmostEqual(self.numbers["flood_peak"], np.log(0.01 + 0.25 * 4.0 + 1e-4),
                               places=6)
        mean = (self.hourly.cfs * tabular.CFS_TO_MM_HR / self.area).mean()
        self.assertAlmostEqual(self.numbers["flood_ratio"], np.log(1.01 / mean), places=6)

    def test_storm_response_is_the_rise_per_mm_of_warm_rain(self):
        # 24 mm arrive in six hours; the storm day's mean flow rises by 0.25 * 24 mm per day.
        self.assertAlmostEqual(self.numbers["storm_response"], np.log(0.25 + 1e-3), places=2)

    def test_a_steeper_response_changes_only_what_it_should(self):
        hourly, daily, area = synthetic_record("2015-10-01", "2018-09-30 23:00", lag_hours=12,
                                               response=0.5)
        other = bench.behaviour(daily, hourly, area)
        self.assertEqual(other["lag_hours"], 12.0)
        self.assertGreater(other["log_runoff_ratio"], self.numbers["log_runoff_ratio"])
        self.assertAlmostEqual(other["snow_fraction"], self.numbers["snow_fraction"], places=6)

    def test_the_elevation_offset_turns_rain_into_snow(self):
        colder = bench.behaviour(self.daily, self.hourly, self.area, temp_offset_c=-12.0)
        self.assertEqual(colder["snow_fraction"], 1.0)
        self.assertTrue(np.isnan(colder["storm_response"]))   # no warm storms are left
        self.assertTrue(np.isnan(colder["lag_hours"]))

    def test_short_periods_are_refused(self):
        hourly, daily, area = synthetic_record("2015-10-01", "2016-09-30 23:00")
        self.assertIsNone(bench.behaviour(daily, hourly, area))

    def test_every_listed_number_is_produced(self):
        for name in set(bench.RESPONSE + bench.EXTREMES + bench.BEHAVIOUR):
            self.assertIn(name, self.numbers)


class TestRegimes(unittest.TestCase):
    """Regime rules apply in order, with thresholds from training basins only."""

    def test_precedence_and_training_thresholds(self):
        late = pd.DataFrame({
            "zero_flow_fraction": [0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "snow_fraction": [0.5, 0.5, 0.0, 0.0, 0.0, 0.0, 0.0],
            "melt_share": [0.9, 0.9, 0.2, 0.2, 0.2, 0.2, 0.2],
            "rb_flashiness": [0.1, 0.1, 0.9, 0.1, 0.2, 0.3, 5.0],
            "bfi": [0.5, 0.5, 0.2, 0.9, 0.3, 0.4, 0.1]},
            index=["dry", "snow", "flashy", "steady", "mixed", "mixed2", "held"])
        regime, thresholds = bench.regime_classes(
            late, ["dry", "snow", "flashy", "steady", "mixed", "mixed2"])
        self.assertEqual(list(regime[["dry", "snow", "flashy", "steady", "mixed"]]),
                         ["intermittent", "snowmelt", "flashy", "steady", "mixed"])
        self.assertEqual(regime["held"], "flashy")
        # The held-out basin's extreme flashiness must not move the threshold.
        self.assertLess(thresholds["flashy_min_rb"], 1.0)


class TestScoring(unittest.TestCase):
    """Donor search, efficiency, statistics and the end-to-end ranking."""

    def test_nearest_returns_the_closest_training_rows_in_order(self):
        train = np.array([[0.0, 0.0], [1.0, 0.0], [5.0, 5.0]])
        order = bench.nearest(train, np.array([[0.9, 0.1], [4.0, 4.0]]), 2)
        np.testing.assert_array_equal(order, [[1, 0], [2, 1]])

    def test_kge_is_one_for_a_perfect_match_and_falls_with_bias(self):
        observed = np.sin(np.arange(200) / 7.0) + 2.0
        self.assertAlmostEqual(bench.kge(observed, observed), 1.0)
        self.assertAlmostEqual(bench.kge(2.0 * observed, observed), 1.0 - np.sqrt(2.0))
        self.assertTrue(np.isnan(bench.kge(np.ones(10), observed[:10])))

    def test_statistics_and_paired_interval(self):
        truth = np.array(["a", "a", "a", "b"])
        result = {"kind": "balanced_accuracy", "sites": list("wxyz"), "truth": truth,
                  "predicted": np.array(["a", "a", "b", "b"])}
        self.assertAlmostEqual(bench.statistic(result), (2 / 3 + 1.0) / 2)
        rng = np.random.default_rng(0)
        total = rng.uniform(0.5, 2.0, size=80)
        good = {"kind": "r2", "sites": list(range(80)), "error": 0.1 * total, "total": total}
        poor = {"kind": "r2", "sites": list(range(80)), "error": 0.9 * total, "total": total}
        self.assertAlmostEqual(bench.statistic(good), 0.9)
        low, high = bench.interval(good, poor, n_boot=200)
        self.assertAlmostEqual(low, 0.8)
        self.assertAlmostEqual(high, 0.8)
        self.assertAlmostEqual(bench.statistic({"kind": "median", "sites": [0, 1, 2],
                                                "values": np.array([0.1, np.nan, 0.5])}), 0.3)

    def test_an_informative_representation_beats_its_permutation_on_every_task(self):
        rng = np.random.default_rng(3)
        sites = ["s%03d" % index for index in range(180)]
        latent = rng.normal(size=(180, 2))
        columns = sorted(set(bench.BEHAVIOUR + bench.EXTREME_TARGETS))
        weights = rng.normal(size=(2, len(columns)))
        late = pd.DataFrame(latent @ weights + 0.2 * rng.normal(size=(180, len(columns))),
                            index=sites, columns=columns)
        regime = pd.Series(np.where(latent[:, 0] > 0, "flashy", "steady"), index=sites)
        days = np.arange(500)
        scale, phase = np.exp(latent[:, 1]), latent[:, 0]
        flow = pd.DataFrame({site: scale[row] * (1.5 + np.sin(days / 20.0 + phase[row]))
                             for row, site in enumerate(sites)})
        benchmark = {"late": late, "regime": regime, "later_flow": flow,
                     "split": pd.Series(["train"] * 120 + ["holdout"] * 60, index=sites)}
        informative = pd.DataFrame(latent + 0.05 * rng.normal(size=latent.shape), index=sites)
        permuted = pd.DataFrame(informative.to_numpy()[rng.permutation(180)], index=sites)
        good = bench.evaluate(informative, benchmark)
        bad = bench.evaluate(permuted, benchmark)
        for task in bench.TASKS:
            self.assertGreater(bench.statistic(good[task]), bench.statistic(bad[task]) + 0.2,
                               task)
            self.assertEqual(len(good[task]["sites"]), 60)
        self.assertGreater(bench.statistic(good["flood_peak"]), 0.7)
        self.assertLess(abs(bench.statistic(bad["flood_peak"])), 0.2)


if __name__ == "__main__":
    unittest.main()
