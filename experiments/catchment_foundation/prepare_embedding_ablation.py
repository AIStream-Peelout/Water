"""Prepare matched manifests and a shuffled bank for the embedding forecast ablation.

The output cohorts contain only sites present in every requested embedding bank.  Existing
development labels are reset to training, the final holdout is preserved, and a fresh deterministic
development split is sampled within each state.  Consequently every condition sees the same rivers,
windows, snow data and split labels; only the context bank changes.
"""
import argparse
import copy
import json
import os
from collections import Counter, defaultdict
from typing import Dict, Iterable, Tuple

import numpy as np
import torch


def _parse_bank(specification: str) -> Tuple[str, str]:
    """Parses ``label=/path/to/bank.pt`` CLI values."""
    label, separator, path = specification.partition("=")
    if not separator or not label or not path:
        raise argparse.ArgumentTypeError("embedding banks must use label=/path/to/bank.pt")
    return label, os.path.abspath(path)


def _bank_sites(path: str) -> set:
    """Returns the string site identifiers in an embedding bank."""
    bank = torch.load(path, map_location="cpu", weights_only=True)
    return {str(site) for site in bank["site_ids"]}


def _select_development_sites(basins: Iterable[Dict], per_state: int,
                              seed: int) -> list:
    """Selects a deterministic state-stratified development cohort."""
    groups = defaultdict(list)
    for basin in basins:
        if basin.get("split") != "holdout":
            groups[basin.get("state", "unknown")].append(basin["site_id"])
    rng = np.random.default_rng(seed)
    selected = []
    for state in sorted(groups):
        candidates = np.array(sorted(groups[state]))
        if len(candidates) <= per_state:
            raise ValueError("State %s has only %d eligible sites; need more than %d" %
                             (state, len(candidates), per_state))
        chosen = rng.choice(candidates, size=per_state, replace=False)
        selected.extend(str(site) for site in chosen)
    return sorted(selected)


def _write_manifest(base: Dict, bank_path: str, output_path: str, metadata: Dict) -> None:
    """Writes one bank-specific copy of the matched manifest."""
    manifest = copy.deepcopy(base)
    manifest["embedding_path"] = os.path.abspath(bank_path)
    manifest["embedding_ablation"] = metadata
    for basin in manifest["basins"]:
        basin["has_embedding"] = True
    with open(output_path, "w") as file:
        json.dump(manifest, file, indent=1)


def _write_shuffled_bank(source_path: str, common_sites: set, output_path: str,
                         seed: int) -> Dict[str, str]:
    """Writes a deterministic site-label permutation of one bank."""
    bank = torch.load(source_path, map_location="cpu", weights_only=True)
    lookup = {str(site): index for index, site in enumerate(bank["site_ids"])}
    sites = sorted(common_sites)
    contexts = torch.stack([bank["embeddings"][lookup[site]] for site in sites])
    generator = torch.Generator().manual_seed(seed)
    permutation = torch.randperm(len(sites), generator=generator)
    while bool((permutation == torch.arange(len(sites))).any()):
        permutation = torch.randperm(len(sites), generator=generator)
    torch.save({"site_ids": sites, "embeddings": contexts[permutation]}, output_path)
    return {site: sites[int(source)] for site, source in zip(sites, permutation)}


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-manifest", required=True)
    parser.add_argument("--bank", action="append", type=_parse_bank, required=True)
    parser.add_argument("--shuffle-source", required=True,
                        help="Label of the bank used to make the shuffled condition")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--valid-per-state", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    banks = dict(args.bank)
    if args.shuffle_source not in banks:
        parser.error("--shuffle-source must name one of the --bank labels")
    os.makedirs(args.output_dir, exist_ok=True)
    with open(args.base_manifest) as file:
        source_manifest = json.load(file)

    common_sites = set.intersection(*[_bank_sites(path) for path in banks.values()])
    matched_basins = [copy.deepcopy(basin) for basin in source_manifest["basins"]
                      if basin["site_id"] in common_sites]
    for basin in matched_basins:
        if basin.get("split") == "basin_valid":
            basin["split"] = "train"
    development_sites = set(_select_development_sites(
        matched_basins, args.valid_per_state, args.seed))
    for basin in matched_basins:
        if basin["site_id"] in development_sites:
            basin["split"] = "basin_valid"

    matched = copy.deepcopy(source_manifest)
    matched["basins"] = matched_basins
    matched["basin_validation"] = {
        "method": "state_stratified_common_embedding_sites",
        "seed": args.seed,
        "per_state": args.valid_per_state,
        "site_ids": sorted(development_sites),
    }
    base_path = os.path.join(args.output_dir, "manifest_common_base.json")
    with open(base_path, "w") as file:
        json.dump(matched, file, indent=1)

    manifests = {}
    for label, path in banks.items():
        output_path = os.path.join(args.output_dir, "manifest_%s.json" % label)
        _write_manifest(matched, path, output_path,
                        {"condition": label, "kind": "ordered", "seed": args.seed})
        manifests[label] = os.path.abspath(output_path)

    shuffled_label = args.shuffle_source + "_shuffled"
    shuffled_path = os.path.abspath(os.path.join(
        args.output_dir, "embeddings_%s.pt" % shuffled_label))
    mapping = _write_shuffled_bank(
        banks[args.shuffle_source], {b["site_id"] for b in matched_basins},
        shuffled_path, args.seed)
    shuffled_manifest = os.path.join(args.output_dir, "manifest_%s.json" % shuffled_label)
    _write_manifest(matched, shuffled_path, shuffled_manifest,
                    {"condition": shuffled_label, "kind": "shuffled",
                     "source": args.shuffle_source, "seed": args.seed})
    manifests[shuffled_label] = os.path.abspath(shuffled_manifest)

    states = Counter(basin.get("state", "unknown") for basin in matched_basins)
    splits = Counter(basin.get("split") for basin in matched_basins)
    design = {
        "base_manifest": os.path.abspath(args.base_manifest),
        "seed": args.seed,
        "common_site_count": len(matched_basins),
        "state_counts": dict(states),
        "split_counts": dict(splits),
        "development_site_ids": sorted(development_sites),
        "banks": banks,
        "manifests": manifests,
        "shuffled_assignment": mapping,
    }
    design_path = os.path.join(args.output_dir, "experiment_design.json")
    with open(design_path, "w") as file:
        json.dump(design, file, indent=2)
    print(json.dumps(design, indent=2))


if __name__ == "__main__":
    main()
