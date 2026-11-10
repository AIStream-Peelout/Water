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


if __name__ == "__main__":
    unittest.main()
