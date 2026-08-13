"""Tests for reproducible nested basin-count scaling cohorts."""
import unittest

from experiments.catchment_foundation.prepare_scaling_ablation import (
    apply_reference_splits,
    build_manifest,
    nested_state_balanced_order,
)


class TestScalingAblation(unittest.TestCase):
    """Checks nesting, split isolation, and approximate state balance."""

    def setUp(self):
        self.basins = [
            {"site_id": "c%d" % index, "state": "CO", "split": "train"}
            for index in range(4)
        ] + [
            {"site_id": "u%d" % index, "state": "UT", "split": "train"}
            for index in range(6)
        ] + [
            {"site_id": "dev", "state": "CO", "split": "basin_valid"},
            {"site_id": "test", "state": "UT", "split": "holdout"},
        ]

    def test_prefixes_are_nested_and_preserve_evaluation_splits(self):
        order = nested_state_balanced_order(self.basins, seed=42)
        small = set(order[:4])
        large = set(order[:8])
        self.assertTrue(small < large)

        manifest = build_manifest({"basins": self.basins}, small, size=4, seed=42)
        splits = {basin["site_id"]: basin["split"] for basin in manifest["basins"]}
        self.assertEqual(splits["dev"], "basin_valid")
        self.assertEqual(splits["test"], "holdout")
        self.assertEqual(sum(split == "train" for split in splits.values()), 4)
        self.assertEqual(sum(split == "scale_excluded" for split in splits.values()), 6)
        self.assertEqual(
            manifest["scaling_ablation"]["selected_state_counts"], {"CO": 2, "UT": 2})

    def test_reference_splits_extend_training_without_changing_evaluation(self):
        source = {"basins": self.basins}
        reference = {"basins": [
            {"site_id": "c0", "split": "train"},
            {"site_id": "dev", "split": "basin_valid"},
            {"site_id": "test", "split": "holdout"},
        ]}
        extended = apply_reference_splits(source, reference)
        splits = {basin["site_id"]: basin["split"] for basin in extended["basins"]}
        self.assertEqual(splits["dev"], "basin_valid")
        self.assertEqual(splits["test"], "holdout")
        self.assertTrue(all(
            split == "train" for site, split in splits.items()
            if site not in {"dev", "test"}))

    def test_reference_splits_reject_changed_final_holdout(self):
        source = {"basins": self.basins}
        reference = {"basins": [{"site_id": "different", "split": "holdout"}]}
        with self.assertRaisesRegex(ValueError, "final holdout sets differ"):
            apply_reference_splits(source, reference)


if __name__ == "__main__":
    unittest.main()
