"""Tests for the per-basin statistics behind the forecast diagnostics."""
import os
import unittest

import numpy as np

from experiments.catchment_foundation import forecast_diagnostics as diagnostics


class TestForecastDiagnostics(unittest.TestCase):
    """Checks error aggregation, skill summaries, paired comparison and path resolution."""

    def setUp(self):
        # Two basins: basin 0 has two quiet windows, basin 1 one window with a large event.
        self.observed = np.array([[1.0, 1.0], [1.0, 1.0], [10.0, 30.0]])
        self.persistence = np.array([[1.0, 2.0], [1.0, 2.0], [10.0, 10.0]])
        self.basin = np.array([0, 0, 1])

    def _errors(self, series):
        return diagnostics.basin_error_sums(series, self.observed, self.basin, 2, 0, 2)

    def test_error_sums_group_window_mse_by_basin(self):
        np.testing.assert_allclose(self._errors(self.persistence), [1.0, 200.0])
        band = diagnostics.basin_error_sums(self.persistence, self.observed, self.basin, 2, 0, 1)
        np.testing.assert_allclose(band, [0.0, 0.0])

    def test_pooled_skill_follows_the_dominant_basin_but_the_median_does_not(self):
        forecast = np.array([[1.0, 3.0], [1.0, 3.0], [10.0, 30.0]])  # worse on 0, perfect on 1
        summary = diagnostics.skill_summary(self._errors(forecast),
                                            self._errors(self.persistence))
        self.assertAlmostEqual(summary["pooled_skill_pct"], 100.0 * (1.0 - 4.0 / 201.0))
        self.assertAlmostEqual(summary["median_basin_skill_pct"], -100.0)
        self.assertAlmostEqual(summary["basins_beating_persistence_pct"], 50.0)
        self.assertAlmostEqual(summary["top_basin_share_of_pooled_error_pct"],
                               100.0 * 200.0 / 201.0)

    def test_paired_summary_reports_direction_and_interval(self):
        rng = np.random.default_rng(1)
        persistence = rng.uniform(1.0, 2.0, size=40)
        worse = rng.uniform(0.5, 1.5, size=40)
        better = 0.5 * worse
        paired = diagnostics.paired_summary(better, worse, persistence, n_boot=500)
        self.assertEqual(paired["a_better_basins_pct"], 100.0)
        self.assertAlmostEqual(paired["median_mse_reduction_pct"], 50.0)
        self.assertGreater(paired["pooled_skill_difference_ci95"][0], 0.0)
        reverse = diagnostics.paired_summary(worse, better, persistence, n_boot=500)
        self.assertEqual(reverse["a_better_basins_pct"], 0.0)
        self.assertLess(reverse["pooled_skill_difference_ci95"][1], 0.0)

    def test_resolve_path_reroots_paths_from_the_old_checkout(self):
        stale = "/somewhere/else/flow-forecast/experiments/catchment_foundation/README.md"
        self.assertEqual(diagnostics.resolve_path(stale),
                         os.path.join(diagnostics.HERE, "README.md"))
        self.assertEqual(diagnostics.resolve_path(__file__), __file__)

    def test_identity_corrections_recover_a_stable_basin_offset_and_nothing_else(self):
        rng = np.random.default_rng(0)

        def windows(n, offsets, noise):
            basin = np.repeat(["a", "b"], n)
            obs = rng.normal(size=(2 * n, 4))
            sim = obs - np.repeat(offsets, n)[:, None] + noise * rng.normal(size=(2 * n, 4))
            return {"sim": sim, "obs": obs, "per": np.zeros(2 * n), "basin": basin,
                    "month": rng.integers(1, 13, size=2 * n)}

        fit = windows(200, [1.0, -2.0], 0.1)       # each basin's forecasts are biased by a constant
        later = windows(50, [1.0, -2.0], 0.1)
        corrected = diagnostics.identity_corrections(fit, later, fit)

        def mse(series):
            return float(((series - later["obs"]) ** 2).mean())

        self.assertGreater(mse(later["sim"]), 2.0)
        self.assertLess(mse(corrected["offset"]), 0.05)            # the per-basin offset removes it
        self.assertLess(mse(corrected["seasonal_offset"]), 0.1)
        self.assertGreater(mse(corrected["pooled_seasonal_offset"]), 1.5)   # no identity: no help
        self.assertEqual(corrected["offset"].shape, later["sim"].shape)


if __name__ == "__main__":
    unittest.main()
