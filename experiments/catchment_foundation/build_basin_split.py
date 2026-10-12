"""
Builds the frozen held-out-basin split for catchment-embedding evaluation.

Why: every probe number so far was transductive — the encoder had seen every evaluated gauge.
A spatial split lets us ask what the representation knows about basins it never trained on.

Grouping: gauges are grouped by HUC6 basin (the first six digits of the NWIS HUC code) so
nested upstream/downstream gauges on one river system stay in one group; groups are assigned
to ``n_folds`` folds separately within each state (largest group first, to the fold with the
fewest sites of that state), so every fold holds out ~20% of every state — a within-region
spatial holdout. Each site also carries its state for a whole-state holdout stress test.

The output is versioned and must not be regenerated once experiments reference it; create a
new version instead.

Example::

    python build_basin_split.py --output splits/basin_split_v1.json
"""
import argparse
import json
import os
from collections import defaultdict
from datetime import date
from typing import Dict, List

import pandas as pd

WATER_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
STATES = ("CO", "UT", "WY", "FL", "PA")


def assign_folds(groups: Dict[str, List[str]], n_folds: int) -> Dict[str, int]:
    """
    Assigns groups to folds, largest group first, each to the currently lightest fold.

    :param groups: Group key -> site ids (one state's groups).
    :type groups: Dict[str, List[str]]
    :param n_folds: Number of folds.
    :type n_folds: int
    :return: Group key -> fold index.
    :rtype: Dict[str, int]
    """
    load = [0] * n_folds
    assignment = {}
    for key in sorted(groups, key=lambda k: (-len(groups[k]), k)):
        fold = min(range(n_folds), key=lambda f: (load[f], f))
        assignment[key] = fold
        load[fold] += len(groups[key])
    return assignment


def main() -> None:
    """
    CLI entry point.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Frozen held-out-basin split")
    parser.add_argument("--records-root", default=os.path.join(
        WATER_ROOT, "pilot_data", "embedding_dataset_hourly_pre2022"))
    parser.add_argument("--scrape-root", default=os.path.join(WATER_ROOT, "pilot_data", "scrapes"))
    parser.add_argument("--n-folds", type=int, default=5)
    parser.add_argument("--group-digits", type=int, default=6, help="HUC digits per group")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise SystemExit("%s exists; splits are frozen — write a new version" % args.output)

    sites: Dict[str, Dict] = {}
    for state in STATES:
        gauges = pd.read_csv(os.path.join(args.scrape_root, state, "gauges.csv"),
                             dtype=str).set_index("site_no")
        state_dir = os.path.join(args.records_root, state)
        for name in sorted(os.listdir(state_dir)):
            if not name.endswith(".npz") or name.endswith("_regional.npz"):
                continue
            site = name[:-4]
            huc = str(gauges.loc[site, "huc_cd"]) if site in gauges.index else None
            if not huc:
                raise SystemExit("no HUC code for %s %s" % (state, site))
            sites[site] = {"state": state, "huc": huc, "group": huc[:args.group_digits]}

    for state in STATES:
        groups: Dict[str, List[str]] = defaultdict(list)
        for site, info in sites.items():
            if info["state"] == state:
                groups[info["group"]].append(site)
        for group, fold in assign_folds(groups, args.n_folds).items():
            for site in groups[group]:
                sites[site]["fold"] = fold

    summary = {}
    for fold in range(args.n_folds):
        summary[fold] = {state: sum(1 for s in sites.values()
                                    if s["fold"] == fold and s["state"] == state)
                         for state in STATES}
        summary[fold]["total"] = sum(summary[fold][s] for s in STATES)
    split = {"version": os.path.splitext(os.path.basename(args.output))[0],
             "created": str(date.today()), "grouping": "huc%d" % args.group_digits,
             "n_folds": args.n_folds, "states": list(STATES), "n_sites": len(sites),
             "fold_summary": summary, "sites": sites}
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as handle:
        json.dump(split, handle, indent=1, sort_keys=True)
    print("%d sites in %d %s groups -> %d folds" % (
        len(sites), len({s["group"] for s in sites.values()}), split["grouping"], args.n_folds))
    for fold, counts in summary.items():
        print("  fold %d: %s" % (fold, counts))


if __name__ == "__main__":
    main()
