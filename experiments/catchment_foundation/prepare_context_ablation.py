"""
Prepare matched manifests for a forecast ablation over catchment contexts.

Every arm trains and scores the forecaster on the same basins, windows and split labels; only
the per-basin context vector changes. Arms written by this script:

* ``none`` — an all-zero context, so the forecaster has nothing basin-specific to condition on;
* one arm per ``--bank label=path`` (an embedding bank ``{"site_ids", "embeddings"}``);
* ``<label>_shuffled`` for the ``--shuffle`` bank — the same vectors permuted across basins
  *within* each split label, so no evaluation basin ever receives a training basin's vector;
* ``statics`` — physical GAGES-II attributes (identifiers and streamflow-derived fields excluded);
* ``statics_signatures`` — those attributes plus flow signatures computed from each basin's own
  pre-cutoff record (the hand-built context a learned embedding has to beat);
* ``learned_id`` — no bank at all: the forecaster fits a private context row per training basin.
  This is the ceiling for basins it trained on and says nothing about unseen basins, which get
  no usable context in this arm.

Split labels come from the frozen basin split: ``--holdout-folds`` become ``holdout`` (scored as
``ungauged_2023``), ``--dev-folds`` become ``basin_valid`` and the rest ``train``. Each context is
imputed and standardized per dimension with statistics of the *training* basins only, clipped,
and zero-padded to one width, so the arms differ in content rather than in scale or shape.
For banks that record their own held-out sites the report states how many evaluation basins the
encoder was trained on.
"""
import argparse
import copy
import json
import os
import sys
from collections import Counter
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import torch

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..")))

CLIP = 10.0


def assign_splits(basins: Iterable[Dict], folds: Dict[str, int], holdout_folds: Iterable[int],
                  dev_folds: Iterable[int]) -> List[Dict]:
    """
    Labels basins from frozen folds, dropping basins the split does not contain.

    :param basins: Manifest basin entries.
    :type basins: Iterable[Dict]
    :param folds: Site id -> fold number from the frozen split.
    :type folds: Dict[str, int]
    :param holdout_folds: Folds labelled ``holdout`` (never trained on by the forecaster).
    :type holdout_folds: Iterable[int]
    :param dev_folds: Folds labelled ``basin_valid`` (development basins).
    :type dev_folds: Iterable[int]
    :return: Copies of the kept entries with ``fold`` and ``split`` set.
    :rtype: List[Dict]
    """
    holdout, dev = set(holdout_folds), set(dev_folds)
    if holdout & dev:
        raise ValueError("a fold cannot be both held out and a development fold")
    labelled = []
    for basin in basins:
        fold = folds.get(basin["site_id"])
        if fold is None:
            continue
        entry = copy.deepcopy(basin)
        entry["fold"] = int(fold)
        entry["split"] = ("holdout" if fold in holdout else
                          "basin_valid" if fold in dev else "train")
        labelled.append(entry)
    return labelled


def standardize_context(matrix: np.ndarray, train_rows: np.ndarray, width: int) -> np.ndarray:
    """
    Imputes, standardizes and zero-pads a context matrix using training rows only.

    :param matrix: Raw contexts of shape (n_sites, n_features); NaN marks a missing value.
    :type matrix: np.ndarray
    :param train_rows: Boolean mask of the training basins' rows.
    :type train_rows: np.ndarray
    :param width: Output width; features are zero-padded up to it.
    :type width: int
    :return: Standardized contexts of shape (n_sites, width), clipped to +/- ``CLIP``.
    :rtype: np.ndarray
    """
    matrix = np.asarray(matrix, dtype=np.float64)
    if matrix.shape[1] > width:
        raise ValueError("context has %d features but the model width is %d"
                         % (matrix.shape[1], width))
    train = matrix[train_rows]
    with np.errstate(all="ignore"):
        medians = np.nanmedian(train, axis=0)
    medians = np.where(np.isfinite(medians), medians, 0.0)
    filled = np.where(np.isfinite(matrix), matrix, medians)
    mean, std = filled[train_rows].mean(axis=0), filled[train_rows].std(axis=0)
    std = np.where(std > 0, std, 1.0)
    standardized = np.clip((filled - mean) / std, -CLIP, CLIP)
    padded = np.zeros((matrix.shape[0], width))
    padded[:, :matrix.shape[1]] = standardized
    return padded


def shuffle_within_groups(groups: List[str], seed: int) -> np.ndarray:
    """
    Draws a permutation that only moves rows within their own group.

    :param groups: Group label of every row (here the split label).
    :type groups: List[str]
    :param seed: Permutation seed.
    :type seed: int
    :return: ``source`` such that row i receives the vector of row ``source[i]``; no row keeps
        its own vector unless its group has a single member.
    :rtype: np.ndarray
    """
    rng = np.random.default_rng(seed)
    source = np.arange(len(groups))
    for label in sorted(set(groups)):
        members = np.array([i for i, group in enumerate(groups) if group == label])
        if len(members) < 2:
            continue
        permuted = rng.permutation(members)
        while (permuted == members).any():
            permuted = rng.permutation(members)
        source[members] = permuted
    return source


def physical_statics(records_dir: str, sites: List[str]) -> Tuple[np.ndarray, List[str]]:
    """
    Loads the physical GAGES-II attributes of the given sites.

    :param records_dir: Directory of ``<site>.npz`` panel records.
    :type records_dir: str
    :param sites: Site ids in row order.
    :type sites: List[str]
    :return: (matrix of shape (n_sites, n_kept), kept attribute names).
    :rtype: Tuple[np.ndarray, List[str]]
    """
    from experiments.catchment_foundation.embedding_probes import (
        STATICS_BASELINE_EXCLUDE, load_statics)
    matrix, names = load_statics(records_dir, sites)
    keep = [i for i, name in enumerate(names)
            if not any(name.startswith(prefix) for prefix in STATICS_BASELINE_EXCLUDE)]
    return matrix[:, keep], [names[i] for i in keep]


def _parse_bank(specification: str) -> Tuple[str, str]:
    """Parses ``label=/path/to/bank.pt`` CLI values."""
    label, separator, path = specification.partition("=")
    if not separator or not label or not path:
        raise argparse.ArgumentTypeError("embedding banks must use label=/path/to/bank.pt")
    return label, os.path.abspath(path)


def _write_arm(base: Dict, sites: List[str], contexts: np.ndarray, label: str, output_dir: str,
               metadata: Dict) -> str:
    """Writes one arm's context bank and the manifest that points at it."""
    bank_path = os.path.abspath(os.path.join(output_dir, "context_%s.pt" % label))
    torch.save({"site_ids": sites, "embeddings": torch.tensor(contexts, dtype=torch.float32)},
               bank_path)
    manifest = copy.deepcopy(base)
    manifest["embedding_path"] = bank_path
    manifest["context_ablation"] = dict(metadata, arm=label)
    for basin in manifest["basins"]:
        basin["has_embedding"] = True
    manifest_path = os.path.abspath(os.path.join(output_dir, "manifest_%s.json" % label))
    with open(manifest_path, "w") as file:
        json.dump(manifest, file, indent=1)
    return manifest_path


def prepare(base_manifest: str, split_path: str, holdout_folds: List[int], dev_folds: List[int],
            banks: Dict[str, str], shuffle: Optional[str], records_dir: Optional[str],
            signatures_csv: Optional[str], output_dir: str, width: int, seed: int) -> Dict:
    """
    Writes every arm's bank and manifest and returns the design summary.

    :param base_manifest: Forecast manifest supplying the basins' data paths and statistics.
    :type base_manifest: str
    :param split_path: Frozen basin split JSON (``sites`` -> ``fold``).
    :type split_path: str
    :param holdout_folds: Folds held out of forecaster training.
    :type holdout_folds: List[int]
    :param dev_folds: Development folds.
    :type dev_folds: List[int]
    :param banks: Embedding banks by arm label.
    :type banks: Dict[str, str]
    :param shuffle: Label of the bank to permute within splits, or None.
    :type shuffle: str, optional
    :param records_dir: Panel records for the statics arms, or None to skip them.
    :type records_dir: str, optional
    :param signatures_csv: Signature table (``site_id`` + columns) for the signatures arm.
    :type signatures_csv: str, optional
    :param output_dir: Where banks, manifests and ``design.json`` are written.
    :type output_dir: str
    :param width: Context width expected by the forecasters.
    :type width: int
    :param seed: Shuffle seed.
    :type seed: int
    :return: The design summary (also written to ``design.json``).
    :rtype: Dict
    """
    os.makedirs(output_dir, exist_ok=True)
    with open(base_manifest) as file:
        source = json.load(file)
    with open(split_path) as file:
        folds = {site: info["fold"] for site, info in json.load(file)["sites"].items()}
    basins = assign_splits(source["basins"], folds, holdout_folds, dev_folds)

    loaded = {label: torch.load(path, map_location="cpu", weights_only=False)
              for label, path in banks.items()}
    common = {basin["site_id"] for basin in basins}
    for bank in loaded.values():
        common &= {str(site) for site in bank["site_ids"]}
    signatures = None
    if signatures_csv:
        import pandas as pd
        signatures = pd.read_csv(signatures_csv, dtype={"site_id": str}).set_index("site_id")
        common &= set(signatures.index)
    if records_dir:
        common &= {name[:-4] for name in os.listdir(records_dir) if name.endswith(".npz")}
    basins = [basin for basin in basins if basin["site_id"] in common]
    sites = [basin["site_id"] for basin in basins]
    labels = [basin["split"] for basin in basins]
    train_rows = np.array([label == "train" for label in labels])
    if not train_rows.any():
        raise ValueError("no training basins left in the matched cohort")

    base = copy.deepcopy(source)
    base["basins"] = basins
    metadata = {"split": os.path.abspath(split_path), "holdout_folds": sorted(holdout_folds),
                "dev_folds": sorted(dev_folds), "seed": seed}
    arms, raw_width, seen = {}, {}, {}

    def add(label: str, matrix: np.ndarray) -> np.ndarray:
        contexts = standardize_context(matrix, train_rows, width)
        arms[label] = _write_arm(base, sites, contexts, label, output_dir, metadata)
        raw_width[label] = int(matrix.shape[1])
        return contexts

    add("none", np.zeros((len(sites), 1)))
    for label, bank in loaded.items():
        row = {str(site): i for i, site in enumerate(bank["site_ids"])}
        matrix = bank["embeddings"][[row[site] for site in sites]].double().numpy()
        contexts = add(label, matrix)
        held = bank.get("holdout_sites")
        evaluated = [site for site, split in zip(sites, labels) if split != "train"]
        seen[label] = (None if held is None else
                       len([site for site in evaluated if site not in set(held)]))
        if label == shuffle:
            source_rows = shuffle_within_groups(labels, seed)
            arms[label + "_shuffled"] = _write_arm(base, sites, contexts[source_rows],
                                                   label + "_shuffled", output_dir, metadata)
            raw_width[label + "_shuffled"] = int(matrix.shape[1])
    if records_dir:
        statics, _ = physical_statics(records_dir, sites)
        add("statics", statics)
        if signatures is not None:
            add("statics_signatures",
                np.concatenate([statics, signatures.loc[sites].to_numpy(dtype=float)], axis=1))

    learned = copy.deepcopy(base)
    learned["embedding_path"] = None
    learned["context_ablation"] = dict(metadata, arm="learned_id")
    for basin in learned["basins"]:
        basin["has_embedding"] = False
    arms["learned_id"] = os.path.abspath(os.path.join(output_dir, "manifest_learned_id.json"))
    with open(arms["learned_id"], "w") as file:
        json.dump(learned, file, indent=1)

    design = {
        "base_manifest": os.path.abspath(base_manifest), **metadata,
        "basins": len(sites),
        "split_counts": dict(Counter(labels)),
        "state_by_split": {split: dict(Counter(b.get("state", "unknown") for b in basins
                                               if b["split"] == split))
                           for split in sorted(set(labels))},
        "context_width": width, "clip": CLIP, "raw_width": raw_width,
        # None: the bank does not record what its encoder was trained on (assume all basins).
        "evaluation_basins_seen_by_encoder": seen,
        "banks": banks, "manifests": arms,
    }
    with open(os.path.join(output_dir, "design.json"), "w") as file:
        json.dump(design, file, indent=1)
    return design


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base-manifest", required=True)
    parser.add_argument("--split", default=os.path.join(HERE, "splits", "basin_split_v1.json"))
    parser.add_argument("--holdout-folds", type=int, nargs="+", required=True)
    parser.add_argument("--dev-folds", type=int, nargs="*", default=[])
    parser.add_argument("--bank", action="append", type=_parse_bank, default=[])
    parser.add_argument("--shuffle", default=None,
                        help="Label of the bank to permute within split labels")
    parser.add_argument("--records-dir", default=None,
                        help="Panel records (<site>.npz) for the statics arms")
    parser.add_argument("--signatures", default=None,
                        help="CSV of pre-cutoff flow signatures indexed by site_id")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--context-width", type=int, default=256)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    banks = dict(args.bank)
    if args.shuffle and args.shuffle not in banks:
        parser.error("--shuffle must name one of the --bank labels")
    design = prepare(args.base_manifest, args.split, args.holdout_folds, args.dev_folds, banks,
                     args.shuffle, args.records_dir, args.signatures, args.output_dir,
                     args.context_width, args.seed)
    print(json.dumps({key: value for key, value in design.items() if key != "manifests"},
                     indent=1))
    for label, path in design["manifests"].items():
        print("%-22s %s" % (label, path))


if __name__ == "__main__":
    main()
