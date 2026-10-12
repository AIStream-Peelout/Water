"""
Re-probes every embedding milestone under one protocol and writes a consolidated table.

Protocol: L2-normalized bank -> ridge with the penalty selected per target inside each outer
training fold (``embedding_probes.ridge_probe_r2`` default), 5 outer folds, seed 42; the same
outer folds for the physical-statics-only baseline. Two probe sets: fleet-wide (all sites of a
fleet-trained bank) and the fixed CO/UT subset (sites present in every bank), so banks trained
on different cohorts compare like for like. Seeds of one configuration are averaged.

Example::

    python reprobe_milestones.py --output calibration/reprobe_milestones.json
"""
import argparse
import json
import os
import sys
from typing import Dict, List

import numpy as np
import torch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from embedding_probes import build_signature_table, ridge_probe_r2, statics_baseline_r2  # noqa: E402

WATER_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SIGNATURES = ["log_mean_flow", "rb_flashiness", "melt_fraction", "bfi", "cv", "diurnal_strength"]
PRE2022 = os.path.join(WATER_ROOT, "pilot_data", "embedding_dataset_hourly_pre2022")
HOURLY = os.path.join(WATER_ROOT, "pilot_data", "embedding_dataset_hourly")

# label -> (bank directories, fleet-wide?)
MILESTONES = {
    "v4b  COUT 143, untrained fusion, b64": ([os.path.join(HOURLY, "COUT_v4b")], False),
    "v4c  COUT 206, untrained fusion, b64": (
        [os.path.join(PRE2022, d) for d in ("COUT_v4c_pre2022", "COUT_v4c_pre2022_s43",
                                             "COUT_v4c_pre2022_s44")], False),
    "v5   fleet, untrained fusion, b64": (
        [os.path.join(PRE2022, "FLEET_v5_s%d" % s) for s in (42, 43, 44)], True),
    "v5   fleet, untrained fusion, b128": ([os.path.join(PRE2022, "FLEET_v5_b128_s42")], True),
    "v6   fleet, fused views (shortcut), b64": (
        [os.path.join(PRE2022, "FLEET_v6_s%d" % s) for s in (42, 43, 44)], True),
    "v7   fleet, modality dropout, MLP head, b64": (
        [os.path.join(PRE2022, "FLEET_v7_s%d" % s) for s in (42, 43, 44)], True),
    "v7   fleet, modality dropout, MLP head, b128": (
        [os.path.join(PRE2022, "FLEET_v7_b128_s42")], True),
    "v8   fleet, linear head, b128, + regional tower": (
        [os.path.join(PRE2022, "FLEET_v8_s%d" % s) for s in (42, 43, 44)], True),
    "v8ctl fleet, linear head, b128": (
        [os.path.join(PRE2022, "FLEET_v8ctl_s%d" % s) for s in (42, 43, 44)], True),
    "v9w256 COUT, linear, b128, dim 256": (
        [os.path.join(PRE2022, "COUT_v9w256_s%d" % s) for s in (42, 43, 44)], False),
    "v9w384 COUT, linear, b128, dim 384": (
        [os.path.join(PRE2022, "COUT_v9w384_s%d" % s) for s in (42, 43, 44)], False),
    "v9w512 COUT, linear, b128, dim 512": (
        [os.path.join(PRE2022, "COUT_v9w512_s%d" % s) for s in (42, 43, 44)], False),
    "v9c  COUT, linear, b64, dim 512 (control)": (
        [os.path.join(PRE2022, "COUT_v9c_s%d" % s) for s in (42, 43, 44)], False),
    "v9r  COUT, regional image as vision input": (
        [os.path.join(PRE2022, "COUT_v9r_s%d" % s) for s in (42, 43, 44)], False),
    "v9t  COUT, reach + regional fourth tower": (
        [os.path.join(PRE2022, "COUT_v9t_s%d" % s) for s in (42, 43, 44)], False),
}


def load_bank(directory: str):
    """
    Loads a bank's site ids and L2-normalized embeddings.

    :param directory: Version directory holding embeddings_concat.pt.
    :type directory: str
    :return: (site ids, embedding matrix) or None if the bank is missing.
    :rtype: Optional[tuple]
    """
    path = os.path.join(directory, "embeddings_concat.pt")
    if not os.path.exists(path):
        return None
    bank = torch.load(path, weights_only=False)
    return list(bank["site_ids"]), torch.nn.functional.normalize(bank["embeddings"].float(),
                                                                  dim=-1).numpy()


def main() -> None:
    """
    CLI entry point.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Re-probe all milestones under one protocol")
    parser.add_argument("--output", required=True)
    parser.add_argument("--scrape-root", default=os.path.join(WATER_ROOT, "pilot_data",
                                                              "scrapes"))
    parser.add_argument("--subset-reference", default=os.path.join(PRE2022, "COUT_v9c_s42"),
                        help="Bank whose site set defines the fixed CO/UT comparison subset")
    args = parser.parse_args()
    roots = {s: os.path.join(args.scrape_root, s) for s in ("CO", "UT", "WY", "FL", "PA")}

    subset_sites = load_bank(args.subset_reference)[0]
    tables: Dict[tuple, tuple] = {}

    def signature_table(site_ids: List[str]):
        key = tuple(site_ids)
        if key not in tables:
            tables[key] = build_signature_table(site_ids, roots)
        return tables[key]

    results: Dict[str, Dict] = {}
    for label, (directories, fleet_wide) in MILESTONES.items():
        banks = [b for b in (load_bank(d) for d in directories) if b is not None]
        if not banks:
            print("skip %s (no banks)" % label, flush=True)
            continue
        entry: Dict[str, Dict] = {"n_seeds": len(banks)}
        for probe_set in (["fleet"] if fleet_wide else []) + ["cout206"]:
            per_seed = []
            for site_ids, matrix in banks:
                if probe_set == "cout206":
                    index = {s: i for i, s in enumerate(site_ids)}
                    kept_sites = [s for s in subset_sites if s in index]
                    matrix_set = matrix[[index[s] for s in kept_sites]]
                else:
                    kept_sites, matrix_set = site_ids, matrix
                table, kept = signature_table(kept_sites)
                r2 = ridge_probe_r2(matrix_set[kept], table)
                per_seed.append([r2[s] for s in SIGNATURES])
            per_seed_array = np.array(per_seed)
            entry[probe_set] = {"n_sites": len(kept_sites),
                                "mean": dict(zip(SIGNATURES, per_seed_array.mean(0).round(3).tolist())),
                                "seed_sd": dict(zip(SIGNATURES, per_seed_array.std(0).round(3).tolist()))}
        results[label] = entry
        print("done %s" % label, flush=True)

    baselines = {}
    for name, (directory, sites) in {
            "fleet": (os.path.join(PRE2022, "FLEET_v8ctl_s42", "combined_records"),
                      load_bank(os.path.join(PRE2022, "FLEET_v8ctl_s42"))[0]),
            "cout206": (os.path.join(args.subset_reference, "combined_records"), subset_sites)}.items():
        table, _ = signature_table(sites)
        baselines[name] = {"n_sites": len(table),
                           "statics_only": statics_baseline_r2(directory, sites, table)}
    report = {"protocol": "nested ridge alpha (RidgeCV LOO inside each of 5 outer folds, seed 42)",
              "signatures": SIGNATURES, "milestones": results, "baselines": baselines}
    with open(args.output, "w") as handle:
        json.dump(report, handle, indent=2)

    for probe_set in ("fleet", "cout206"):
        print("\n== %s probe set (nested-alpha ridge R2, seed means)" % probe_set)
        print("%-46s %4s" % ("configuration", "n") + "".join("%9s" % s[:8] for s in SIGNATURES))
        base = baselines[probe_set]["statics_only"]
        print("%-46s %4d" % ("statics-only baseline (physical GAGES-II)", baselines[probe_set]["n_sites"])
              + "".join("%9.3f" % base[s] for s in SIGNATURES))
        for label, entry in results.items():
            if probe_set in entry:
                print("%-46s %4d" % (label, entry[probe_set]["n_sites"])
                      + "".join("%9.3f" % entry[probe_set]["mean"][s] for s in SIGNATURES))


if __name__ == "__main__":
    main()
