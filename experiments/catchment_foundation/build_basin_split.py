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

A later version can extend an earlier one with gauges added since (``--extend``): no site of
the earlier split moves, a new gauge joins the fold its HUC6 group already has, and only groups
the earlier split never saw are assigned afresh. Scores on the earlier held-out folds therefore
stay comparable, and a new gauge never trains next to a held-out neighbour.

Examples::

    python build_basin_split.py --output splits/basin_split_v1.json
    python build_basin_split.py --extend splits/basin_split_v1.json \
        --manifest manifests/fleet6_all_manifest_v1.json --output splits/basin_split_v2.json
"""
import argparse
import json
import os
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Sequence

import pandas as pd

WATER_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
STATES = ("CO", "UT", "WY", "FL", "PA")


def assign_folds(groups: Dict[str, List[str]], n_folds: int,
                 load: Optional[Sequence[int]] = None, ties_high: bool = False) -> Dict[str, int]:
    """
    Assigns groups to folds, largest group first, each to the currently lightest fold.

    :param groups: Group key -> site ids (one state's groups).
    :type groups: Dict[str, List[str]]
    :param n_folds: Number of folds.
    :type n_folds: int
    :param load: Sites each fold already holds, defaults to None (empty folds).
    :type load: Sequence[int], optional
    :param ties_high: Give a group to the highest-numbered of equally light folds, defaults to
        False (the lowest, as in ``basin_split_v1``).
    :type ties_high: bool, optional
    :return: Group key -> fold index.
    :rtype: Dict[str, int]
    """
    load = [0] * n_folds if load is None else list(load)
    assignment = {}
    for key in sorted(groups, key=lambda k: (-len(groups[k]), k)):
        fold = min(range(n_folds), key=lambda f: (load[f], -f if ties_high else f))
        assignment[key] = fold
        load[fold] += len(groups[key])
    return assignment


def extend_split(base_sites: Dict[str, Dict], new_sites: Dict[str, Dict], n_folds: int,
                 version: str) -> Dict[str, Dict]:
    """
    Adds gauges to a frozen split without moving any site already in it.

    A new gauge takes the fold its HUC6 group has in its own state. Where its state has no such
    group but another state does, it takes the lowest-numbered fold the group has there, so a
    group that touches the development or the reserved fold stays held out. Groups the base
    split never saw are assigned per state as in :func:`assign_folds`, starting from the sites
    each fold of that state already holds. Equally light folds are filled from the highest
    number down: folds 0 and 1 are the held-out ones, and in a state new to the split the
    largest river system (84 of Oregon's 191 gauges sit in the Willamette) would otherwise
    fill one of them on its own.

    :param base_sites: The frozen split's ``sites`` (site id -> state, huc, group, fold).
    :type base_sites: Dict[str, Dict]
    :param new_sites: Candidate gauges (site id -> state, huc, group); those already in the
        base split are ignored.
    :type new_sites: Dict[str, Dict]
    :param n_folds: Number of folds of the base split.
    :type n_folds: int
    :param version: Name of the new version, stored as ``since`` on every added site.
    :type version: str
    :return: The sites of the extended split.
    :rtype: Dict[str, Dict]
    """
    sites = {site: dict(info) for site, info in base_sites.items()}
    own_state = {(info["state"], info["group"]): info["fold"] for info in base_sites.values()}
    lowest: Dict[str, int] = {}
    for info in base_sites.values():
        lowest[info["group"]] = min(lowest.get(info["group"], n_folds), info["fold"])
    unseen: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    for site in sorted(set(new_sites) - set(base_sites)):
        info = new_sites[site]
        entry = {"state": info["state"], "huc": info["huc"], "group": info["group"],
                 "since": version}
        if (info["state"], info["group"]) in own_state:
            entry["fold"] = own_state[(info["state"], info["group"])]
        elif info["group"] in lowest:
            entry["fold"] = lowest[info["group"]]
        else:
            unseen[info["state"]][info["group"]].append(site)
        sites[site] = entry
    for state, groups in unseen.items():
        load = [sum(1 for info in sites.values()
                    if info["state"] == state and info.get("fold") == fold)
                for fold in range(n_folds)]
        for group, fold in assign_folds(groups, n_folds, load, ties_high=True).items():
            for site in groups[group]:
                sites[site]["fold"] = fold
    return sites


def manifest_sites(manifest_path: str, scrape_root: str, group_digits: int) -> Dict[str, Dict]:
    """
    Lists the gauges of a manifest with their state and HUC code.

    :param manifest_path: Manifest written by ``build_manifest.py`` (basins carry ``state``).
    :type manifest_path: str
    :param scrape_root: Scrape root holding ``<state>/gauges.csv`` with the HUC codes.
    :type scrape_root: str
    :param group_digits: HUC digits per group.
    :type group_digits: int
    :return: Site id -> state, huc, group. A gauge listed under two states keeps the first.
    :rtype: Dict[str, Dict]
    """
    with open(manifest_path) as handle:
        basins = json.load(handle)["basins"]
    hucs: Dict[str, pd.Series] = {}
    sites: Dict[str, Dict] = {}
    for basin in basins:
        site, state = str(basin["site_id"]), basin["state"]
        if site in sites:
            print("%s is listed under %s and %s; kept under %s"
                  % (site, sites[site]["state"], state, sites[site]["state"]))
            continue
        if state not in hucs:
            hucs[state] = pd.read_csv(os.path.join(scrape_root, state, "gauges.csv"),
                                      dtype=str).drop_duplicates("site_no").set_index(
                                          "site_no")["huc_cd"]
        huc = hucs[state].get(site)
        if not isinstance(huc, str) or not huc:
            raise SystemExit("no HUC code for %s %s" % (state, site))
        sites[site] = {"state": state, "huc": huc, "group": huc[:group_digits]}
    return sites


def record_sites(records_root: str, scrape_root: str, group_digits: int) -> Dict[str, Dict]:
    """
    Lists the gauges that have a panel record, with their state and HUC code.

    :param records_root: Root of the per-state ``<site>.npz`` panel records.
    :type records_root: str
    :param scrape_root: Scrape root holding ``<state>/gauges.csv`` with the HUC codes.
    :type scrape_root: str
    :param group_digits: HUC digits per group.
    :type group_digits: int
    :return: Site id -> state, huc, group.
    :rtype: Dict[str, Dict]
    """
    sites: Dict[str, Dict] = {}
    for state in STATES:
        gauges = pd.read_csv(os.path.join(scrape_root, state, "gauges.csv"),
                             dtype=str).set_index("site_no")
        state_dir = os.path.join(records_root, state)
        for name in sorted(os.listdir(state_dir)):
            if not name.endswith(".npz") or name.endswith("_regional.npz"):
                continue
            site = name[:-4]
            huc = str(gauges.loc[site, "huc_cd"]) if site in gauges.index else None
            if not huc:
                raise SystemExit("no HUC code for %s %s" % (state, site))
            sites[site] = {"state": state, "huc": huc, "group": huc[:group_digits]}
    return sites


def fold_summary(sites: Dict[str, Dict], states: Sequence[str], n_folds: int) -> Dict:
    """Counts the sites of every fold, per state and in total."""
    summary = {}
    for fold in range(n_folds):
        summary[fold] = {state: sum(1 for s in sites.values()
                                    if s["fold"] == fold and s["state"] == state)
                         for state in states}
        summary[fold]["total"] = sum(summary[fold][s] for s in states)
    return summary


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
    parser.add_argument("--extend", default=None,
                        help="Frozen split to extend; its sites keep their folds")
    parser.add_argument("--manifest", default=None,
                        help="With --extend: manifest whose gauges are added to the split")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if os.path.exists(args.output):
        raise SystemExit("%s exists; splits are frozen — write a new version" % args.output)
    version = os.path.splitext(os.path.basename(args.output))[0]

    extra = {}
    if args.extend:
        if not args.manifest:
            raise SystemExit("--extend needs --manifest (the gauges to add)")
        with open(args.extend) as handle:
            base = json.load(handle)
        n_folds, digits = base["n_folds"], int(base["grouping"][3:])
        candidates = manifest_sites(args.manifest, args.scrape_root, digits)
        sites = extend_split(base["sites"], candidates, n_folds, version)
        states = list(base["states"]) + sorted(
            {info["state"] for info in sites.values()} - set(base["states"]))
        extra = {"extends": base["version"], "n_sites_added": len(sites) - len(base["sites"]),
                 "source_manifest": os.path.basename(args.manifest)}
    else:
        n_folds, digits, states = args.n_folds, args.group_digits, list(STATES)
        sites = record_sites(args.records_root, args.scrape_root, digits)
        for state in states:
            groups: Dict[str, List[str]] = defaultdict(list)
            for site, info in sites.items():
                if info["state"] == state:
                    groups[info["group"]].append(site)
            for group, fold in assign_folds(groups, n_folds).items():
                for site in groups[group]:
                    sites[site]["fold"] = fold

    summary = fold_summary(sites, states, n_folds)
    split = {"version": version, "created": str(date.today()), "grouping": "huc%d" % digits,
             "n_folds": n_folds, "states": states, "n_sites": len(sites),
             "fold_summary": summary, "sites": sites, **extra}
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as handle:
        json.dump(split, handle, indent=1, sort_keys=True)
    print("%d sites in %d %s groups -> %d folds" % (
        len(sites), len({s["group"] for s in sites.values()}), split["grouping"], n_folds))
    for fold, counts in summary.items():
        print("  fold %d: %s" % (fold, counts))


if __name__ == "__main__":
    main()
