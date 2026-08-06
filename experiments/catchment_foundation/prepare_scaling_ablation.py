"""Build deterministic nested training cohorts for basin-count scaling experiments.

Every output manifest preserves the same basin-validation and final-holdout cohorts. Training
basins are placed in a state-balanced random order, then each requested cohort is a prefix of that
order. Basins outside a prefix are labelled ``scale_excluded`` so they cannot enter either forecast
training or parameter-head pretraining.
"""
import argparse
import copy
import json
import os
from collections import Counter, defaultdict
from typing import Dict, Iterable, List

import numpy as np


def apply_reference_splits(source: Dict, reference: Dict) -> Dict:
    """Carries a reference development/final-holdout split onto a larger basin manifest.

    The final-holdout site set must match exactly. All other source sites are reset to training
    unless they are development sites in the reference. This allows a larger training universe to
    extend an existing scaling curve without changing either evaluation cohort.
    """
    manifest = copy.deepcopy(source)
    reference_development = {
        str(basin["site_id"]) for basin in reference["basins"]
        if basin.get("split") == "basin_valid"
    }
    reference_holdout = {
        str(basin["site_id"]) for basin in reference["basins"]
        if basin.get("split") == "holdout"
    }
    source_sites = {str(basin["site_id"]) for basin in manifest["basins"]}
    source_holdout = {
        str(basin["site_id"]) for basin in manifest["basins"]
        if basin.get("split") == "holdout"
    }
    if not reference_development <= source_sites:
        missing = sorted(reference_development - source_sites)
        raise ValueError("Reference development sites missing from source: %s" % missing)
    if reference_holdout != source_holdout:
        raise ValueError(
            "Reference/source final holdout sets differ; refusing to change the test cohort")

    for basin in manifest["basins"]:
        site = str(basin["site_id"])
        if site in reference_holdout:
            basin["split"] = "holdout"
        elif site in reference_development:
            basin["split"] = "basin_valid"
        else:
            basin["split"] = "train"
    manifest["basin_validation"] = {
        "method": "carried_from_scaling_reference",
        "source": reference.get("basin_validation", {}),
        "site_ids": sorted(reference_development),
    }
    return manifest


def nested_state_balanced_order(basins: Iterable[Dict], seed: int) -> List[str]:
    """Returns a seeded nested order whose prefixes track the full state proportions."""
    groups = defaultdict(list)
    for basin in basins:
        if basin.get("split") == "train":
            groups[basin.get("state", "unknown")].append(str(basin["site_id"]))
    if not groups:
        raise ValueError("The source manifest contains no training basins")

    rng = np.random.default_rng(seed)
    queues = {}
    for state, sites in groups.items():
        queues[state] = list(rng.permutation(sorted(sites)))

    totals = {state: len(sites) for state, sites in queues.items()}
    selected = Counter()
    order = []
    total = sum(totals.values())
    for position in range(1, total + 1):
        available = [state for state in sorted(queues) if selected[state] < totals[state]]
        state = max(
            available,
            key=lambda name: (position * totals[name] / total - selected[name], name),
        )
        order.append(str(queues[state][selected[state]]))
        selected[state] += 1
    return order


def build_manifest(source: Dict, selected_sites: set, size: int, seed: int) -> Dict:
    """Copies a manifest and excludes training sites outside ``selected_sites``."""
    manifest = copy.deepcopy(source)
    state_counts = Counter()
    for basin in manifest["basins"]:
        if basin.get("split") == "train":
            if str(basin["site_id"]) in selected_sites:
                state_counts[basin.get("state", "unknown")] += 1
            else:
                basin["split"] = "scale_excluded"
    manifest["scaling_ablation"] = {
        "method": "nested_state_balanced_prefix",
        "seed": seed,
        "train_size": size,
        "selected_site_ids": sorted(selected_sites),
        "selected_state_counts": dict(state_counts),
    }
    return manifest


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-manifest", required=True)
    parser.add_argument(
        "--split-reference", default=None,
        help="Optional prior scaling manifest whose development and holdout cohorts are carried "
             "onto a larger base manifest",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--sizes", type=int, nargs="+", default=[24, 48, 96])
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    with open(args.base_manifest) as file:
        source = json.load(file)
    if args.split_reference:
        with open(args.split_reference) as file:
            reference = json.load(file)
        source = apply_reference_splits(source, reference)
    order = nested_state_balanced_order(source["basins"], args.seed)
    sizes = sorted(set(args.sizes))
    if not sizes or sizes[0] < 1 or sizes[-1] > len(order):
        parser.error("sizes must be between 1 and the %d available training basins" % len(order))

    os.makedirs(args.output_dir, exist_ok=True)
    manifests = {}
    cohorts = {}
    for size in sizes:
        selected = set(order[:size])
        manifest = build_manifest(source, selected, size, args.seed)
        output_path = os.path.abspath(os.path.join(
            args.output_dir, "manifest_hourly_v4b_n%d.json" % size))
        with open(output_path, "w") as file:
            json.dump(manifest, file, indent=1)
        manifests[str(size)] = output_path
        cohorts[str(size)] = manifest["scaling_ablation"]

    design = {
        "base_manifest": os.path.abspath(args.base_manifest),
        "split_reference": (os.path.abspath(args.split_reference)
                            if args.split_reference else None),
        "seed": args.seed,
        "available_training_basins": len(order),
        "sizes": sizes,
        "nested_order": order,
        "cohorts": cohorts,
        "manifests": manifests,
    }
    design_path = os.path.join(args.output_dir, "scaling_design.json")
    with open(design_path, "w") as file:
        json.dump(design, file, indent=2)
    print(json.dumps(design, indent=2))


if __name__ == "__main__":
    main()
