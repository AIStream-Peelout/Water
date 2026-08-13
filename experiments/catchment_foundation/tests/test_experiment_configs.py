"""Tests for catchment-specific cohort and run configuration orchestration."""
import json
import os
import tempfile
import unittest

from experiments.catchment_foundation.run_crossformer import build_crossformer_params
from experiments.catchment_foundation.run_training import (
    build_params,
    make_basin_validation_manifest,
)


class TestBasinValidationSplit(unittest.TestCase):
    """The development cohort must reserve whole basins without changing final holdouts."""

    def test_reserves_embedded_training_basin(self):
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, "manifest.json")
            derived = os.path.join(directory, "development_manifest.json")
            with open(source, "w") as file:
                json.dump({"basins": [
                    {"site_id": "basinA", "split": "train", "has_embedding": True},
                    {"site_id": "basinB", "split": "train", "has_embedding": True},
                    {"site_id": "basinC", "split": "holdout", "has_embedding": True},
                ]}, file)

            selected = make_basin_validation_manifest(
                source, derived, count=1, seed=42, require_pretrained_embedding=True)
            with open(derived) as file:
                manifest = json.load(file)
            splits = {basin["site_id"]: basin["split"] for basin in manifest["basins"]}

            self.assertEqual(len(selected), 1)
            self.assertEqual(splits[selected[0]], "basin_valid")
            self.assertEqual(splits["basinC"], "holdout")
            params = build_params(
                derived, "test", 1, 2, 4, None, 1e-3, False,
                valid_basin_split="basin_valid")
            self.assertEqual(params["dataset_params"]["valid_basin_split"], "basin_valid")


class TestCrossformerExperimentConfig(unittest.TestCase):
    """The Water control config should preserve the intended scientific comparison."""

    def test_config_is_direct_and_split_compatible(self):
        params = build_crossformer_params(
            "manifest.json", "unit", epochs=2, batch_size=4, samples_per_epoch=32,
            max_basins=3, lr=1e-3, use_wandb=False)
        self.assertEqual(params["model_name"], "CrossformerMultiBasin")
        self.assertEqual(params["dataset_params"]["class"], "MultiBasinCatchmentWindow")
        self.assertEqual(params["dataset_params"]["train_end_date"], "2022-01-01")
        self.assertEqual(params["dataset_params"]["valid_start_date"], "2022-01-01")
        self.assertEqual(params["dataset_params"]["test_start_date"], "2023-01-01")
        self.assertEqual(params["dataset_params"]["event_sample_power"], 0.0)
        self.assertTrue(params["dataset_params"]["require_pretrained_embedding"])
        self.assertNotIn("temp_lapse_k", params["dataset_params"]["relevant_cols"])
        self.assertNotIn("sw_raw", params["dataset_params"]["relevant_cols"])
        self.assertFalse(params["wandb"])

    def test_huber_and_shape_controls_are_serialized(self):
        params = build_crossformer_params(
            "manifest.json", "unit", epochs=2, batch_size=4, samples_per_epoch=32,
            max_basins=3, lr=3e-4, use_wandb=False, loss="huber", huber_beta=0.5,
            residual_smoothing_hours=12, nonnegative=True, event_sample_power=0.0)
        self.assertEqual(params["training_params"]["criterion"], "SmoothL1Loss")
        self.assertEqual(params["training_params"]["criterion_params"], {"beta": 0.5})
        self.assertEqual(params["model_params"]["residual_smoothing_hours"], 12)
        self.assertTrue(params["model_params"]["nonnegative"])
        self.assertEqual(params["dataset_params"]["event_sample_power"], 0.0)


if __name__ == "__main__":
    unittest.main()
