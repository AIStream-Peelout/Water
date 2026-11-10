"""Tests for the context-ablation preparation, queue commands and run comparison."""
import json
import os
import tempfile
import unittest

import numpy as np

from experiments.catchment_foundation import forecast_diagnostics as diagnostics
from experiments.catchment_foundation.prepare_context_ablation import (
    CLIP,
    assign_splits,
    shuffle_within_groups,
    standardize_context,
)
from experiments.catchment_foundation.run_context_ablation import build_command, run_name


class TestSplitAssignment(unittest.TestCase):
    """Fold labels must come from the frozen split and nothing else."""

    def test_folds_become_labels_and_unknown_sites_are_dropped(self):
        basins = [{"site_id": site, "split": "holdout"} for site in ("a", "b", "c", "d")]
        labelled = assign_splits(basins, {"a": 0, "b": 1, "c": 2}, holdout_folds=[0],
                                 dev_folds=[1])
        self.assertEqual({b["site_id"]: b["split"] for b in labelled},
                         {"a": "holdout", "b": "basin_valid", "c": "train"})
        self.assertEqual([b["fold"] for b in labelled], [0, 1, 2])
        self.assertEqual(basins[2]["split"], "holdout")  # the input is not modified

    def test_a_fold_cannot_be_both_held_out_and_development(self):
        with self.assertRaises(ValueError):
            assign_splits([], {}, holdout_folds=[0], dev_folds=[0])


class TestContextStandardization(unittest.TestCase):
    """Every arm is scaled with training-basin statistics only."""

    def test_training_rows_alone_set_the_statistics(self):
        matrix = np.array([[0.0, 5.0], [2.0, 5.0], [100.0, np.nan]])
        train = np.array([True, True, False])
        out = standardize_context(matrix, train, width=4)
        self.assertEqual(out.shape, (3, 4))
        np.testing.assert_allclose(out[:2, 0], [-1.0, 1.0])      # mean 1, std 1 from rows 0-1
        self.assertEqual(out[2, 0], CLIP)                        # (100 - 1) / 1 is clipped
        np.testing.assert_allclose(out[:, 1], 0.0)               # constant column, NaN imputed
        np.testing.assert_allclose(out[:, 2:], 0.0)              # zero padding

    def test_a_context_wider_than_the_model_is_rejected(self):
        with self.assertRaises(ValueError):
            standardize_context(np.zeros((2, 5)), np.array([True, False]), width=4)


class TestShuffle(unittest.TestCase):
    """The shuffled control never moves a vector across split labels."""

    def test_rows_move_only_within_their_group_and_never_stay(self):
        groups = ["train"] * 6 + ["holdout"] * 4 + ["basin_valid"]
        source = shuffle_within_groups(groups, seed=3)
        self.assertEqual(sorted(source.tolist()), list(range(len(groups))))
        for row, origin in enumerate(source):
            self.assertEqual(groups[row], groups[origin])
        self.assertTrue((source[:10] != np.arange(10)).all())
        self.assertEqual(source[10], 10)  # a single-member group cannot be permuted


class TestQueueCommands(unittest.TestCase):
    """Arms differ in manifest only; the two special arms change one flag each."""

    def setUp(self):
        self.manifests = {"none": "none.json", "v10": "v10.json", "learned_id": "ids.json"}
        self.budget = {"epochs": "2", "samples": "64", "batch": "8", "lr": "1e-3",
                       "patience": "2"}

    def _command(self, model, arm):
        return build_command("python", model, arm, 42, run_name("t", model, arm, 42),
                             self.manifests, self.budget)

    def test_hybrid_arms_share_every_flag_but_the_manifest(self):
        first, second = self._command("hybrid", "none"), self._command("hybrid", "v10")
        differing = [(a, b) for a, b in zip(first, second) if a != b]
        self.assertEqual(differing, [("t_hy_none_s42", "t_hy_v10_s42"), ("none.json", "v10.json")])
        self.assertIn("--require-embeddings", first)
        self.assertNotIn("--require-embeddings", self._command("hybrid", "learned_id"))

    def test_crossformer_context_path_and_learned_ids(self):
        tokens = self._command("crossformer", "v10")
        self.assertEqual(tokens[tokens.index("--context-mode") + 1], "tokens")
        legacy = self._command("crossformer", "v10@channels")
        self.assertEqual(legacy[legacy.index("--context-mode") + 1], "channels")
        self.assertEqual(legacy[legacy.index("--manifest") + 1], "v10.json")
        self.assertIn("--allow-missing-embeddings", self._command("crossformer", "learned_id"))

    def test_unknown_arms_and_variants_are_rejected(self):
        with self.assertRaises(ValueError):
            self._command("crossformer", "missing")
        with self.assertRaises(ValueError):
            self._command("hybrid", "v10@channels")


class TestRunComparison(unittest.TestCase):
    """Runs are paired basin by basin on the errors their evaluations saved."""

    def _write(self, directory, name, model_errors):
        out = os.path.join(directory, name, "eval_ungauged_2023")
        os.makedirs(out)
        errors = {}
        for site, value in model_errors.items():
            errors[site] = {"n_windows": 2}
            for band in diagnostics.BANDS:
                errors[site][band] = {"model_mse": value, "persistence_mse": 1.0}
        with open(os.path.join(out, "basin_band_errors.json"), "w") as file:
            json.dump(errors, file)
        return os.path.join(directory, name)

    def test_each_run_is_paired_with_the_reference(self):
        with tempfile.TemporaryDirectory() as directory:
            runs = {
                "none": self._write(directory, "none", {"a": 1.0, "b": 2.0, "c": 0.5}),
                "context": self._write(directory, "context", {"a": 0.5, "b": 1.0, "c": 0.25}),
            }
            report = diagnostics.compare_runs(runs, "none", "ungauged_2023")
        self.assertEqual(report["basins"], 3)
        band = report["bands"]["all"]
        self.assertNotIn("versus_reference", band["none"])
        self.assertAlmostEqual(band["context"]["median_basin_skill_pct"], 50.0)
        versus = band["context"]["versus_reference"]
        self.assertEqual(versus["a_better_basins_pct"], 100.0)
        self.assertAlmostEqual(versus["median_mse_reduction_pct"], 50.0)


if __name__ == "__main__":
    unittest.main()
