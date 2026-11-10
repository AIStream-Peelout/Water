"""
Builds the multi-basin manifest for catchment foundation-model training.

Reads the Water repo's scrape registries (one per state), keeps gauges marked "completed" with
usable static attributes, computes per-basin train-period normalization stats (met means/stds and
the flow standard deviation in mm/hr), assigns the ungauged-generalization holdout, and writes the
manifest JSON consumed by ``MultiBasinWindowLoader`` and ``HybridGR4MultiBasin``.

Usage::

    python experiments/catchment_foundation/build_manifest.py \
        --out experiments/catchment_foundation/manifests/co_manifest.json

A cohort tied to the frozen basin split (every basin labelled ``train``; the evaluation folds are
stamped afterwards from the same split)::

    python experiments/catchment_foundation/build_manifest.py \
        --states CO UT WY FL PA --n-holdout 0 \
        --sites-from-split experiments/catchment_foundation/splits/basin_split_v1.json \
        --embedding-bank pilot_data/embedding_dataset_hourly_pre2022/<BANK>/embeddings_concat.pt \
        --out experiments/catchment_foundation/manifests/fleet5_manifest_v1.json
"""
import argparse
import json
import os
from collections import Counter
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import torch

WATER_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PILOT_DATA_ROOT = os.environ.get(
    "WATER_PILOT_DATA_ROOT", os.path.join(WATER_ROOT, "pilot_data"))
SCRAPE_ROOT = os.path.join(PILOT_DATA_ROOT, "scrapes")
SNODAS_ROOT = os.path.join(PILOT_DATA_ROOT, "snodas_series")
DEFAULT_EMBEDDING_PATH = os.path.join(
    PILOT_DATA_ROOT, "embedding_dataset", "CO", "embeddings_concat.pt")
CFS_TO_MM_HR = 0.0283168 * 3.6  # multiply by 1/area_km2 for mm/hr
TRAIN_END = "2022-01-01"  # scale stats and training data end here (valid=2022, test=2023+)
MET_COLS = ["precipitation", "temperature", "shortwave_radiation", "longwave_radiation",
            "specific_humidity", "wind_east", "wind_north", "p01m", "pet_mm_hr"]


def _as_float(value) -> Optional[float]:
    """
    Converts a static attribute to a finite float.

    :param value: The raw attribute (number, numeric string, blank string or None).
    :return: The value as a float, or None when it is missing, blank or not finite.
    :rtype: Optional[float]
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def gauge_elevation_m(static: Dict) -> Optional[float]:
    """
    Returns the gauge elevation in metres.

    NWIS reports the gauge altitude in feet (``alt_va``); many gauges outside the mountain states
    (39 of the 176 Florida panel gauges) have none, so the GAGES-II site elevation (already in
    metres) is the fallback.

    :param static: The gauge's static attribute dict.
    :type static: Dict
    :return: Gauge elevation in metres, or None when neither source has one.
    :rtype: Optional[float]
    """
    alt_ft = _as_float(static.get("alt_va"))
    if alt_ft is not None:
        return alt_ft * 0.3048
    return _as_float(static.get("gages2_ELEV_SITE_M"))


def load_embedding_sites(bank_path: str) -> set:
    """
    Returns the site ids present in an embedding bank.

    Banks written with ``train_catchment_embeddings.py --split`` up to ``FLEET_v10_f0_s42`` carry
    the fitted static statistics as NumPy arrays, which the weights-only unpickler refuses. Only
    NumPy's array reconstruction is allow-listed here; nothing else is admitted.

    :param bank_path: Path to an ``embeddings_<fusion>.pt`` bank.
    :type bank_path: str
    :return: The bank's site ids.
    :rtype: set
    """
    numpy_core = getattr(np, "_core", None) or np.core
    allowed = [numpy_core.multiarray._reconstruct, np.ndarray, np.dtype,
               type(np.dtype(np.float32)), type(np.dtype(np.float64))]
    safe_globals = getattr(torch.serialization, "safe_globals", None)
    if safe_globals is None:
        bank = torch.load(bank_path, map_location="cpu", weights_only=True)
    else:
        with safe_globals(allowed):
            bank = torch.load(bank_path, map_location="cpu", weights_only=True)
    return set(bank["site_ids"])


def load_split_sites(split_path: str) -> Dict[str, Dict]:
    """
    Reads the per-site entries of a frozen basin split.

    :param split_path: Path to a split JSON such as ``splits/basin_split_v1.json``.
    :type split_path: str
    :return: Site id -> {"fold", "state", ...}.
    :rtype: Dict[str, Dict]
    """
    with open(split_path) as f:
        return json.load(f)["sites"]


def select_sites(completed: List[str], split_sites: Optional[Dict[str, Dict]],
                 state: str) -> List[str]:
    """
    Chooses which of a state's completed gauges to build.

    :param completed: The state's completed gauge ids.
    :type completed: List[str]
    :param split_sites: Per-site split entries, or None to build every completed gauge.
    :type split_sites: Optional[Dict[str, Dict]]
    :param state: The two-letter state being built.
    :type state: str
    :return: The gauges to build, in the order given. With a split, only its sites are kept, and
        only under the state the split assigns them to.
    :rtype: List[str]
    """
    if split_sites is None:
        return list(completed)
    return [site for site in completed
            if site in split_sites and split_sites[site].get("state", state) == state]


def assign_random_holdout(basins: List[Dict], carried: Dict[str, str], n_holdout: int,
                          seed: int) -> List[str]:
    """
    Marks a random ungauged-generalization holdout in place.

    Candidates are basins without a carried split label that have a pretrained embedding and more
    than 120 days of rows from 2023 on, so that the holdout is evaluable.

    :param basins: Manifest entries; chosen ones get ``split = "holdout"``.
    :type basins: List[Dict]
    :param carried: Site id -> split label carried over from an earlier manifest.
    :type carried: Dict[str, str]
    :param n_holdout: Number of basins to hold out; zero or less holds out none.
    :type n_holdout: int
    :param seed: Selection seed.
    :type seed: int
    :return: The sorted site ids held out.
    :rtype: List[str]
    """
    if n_holdout <= 0:
        return []
    eligible = [b["site_id"] for b in basins
                if b["site_id"] not in carried and b["has_embedding"] and b["rows_2023_plus"] > 24 * 120]
    rng = np.random.default_rng(seed)
    holdout = set(rng.choice(eligible, size=min(n_holdout, len(eligible)),
                             replace=False).tolist())
    for basin in basins:
        if basin["site_id"] in holdout:
            basin["split"] = "holdout"
    return sorted(holdout)


def build_basin_entry(site_id: str, embedding_sites: set, min_train_hours: int,
                      state: str = "CO", skipped: Optional[Dict[str, str]] = None
                      ) -> Optional[Dict]:
    """
    Builds one basin's manifest entry, or None when the gauge is unusable.

    :param site_id: The USGS site id.
    :type site_id: str
    :param embedding_sites: Site ids present in the pretrained embedding bank.
    :type embedding_sites: set
    :param min_train_hours: Minimum valid pre-cutoff flow hours required to keep the gauge.
    :type min_train_hours: int
    :param state: Two-letter state whose scrape directory holds the gauge, defaults to "CO".
    :type state: str, optional
    :param skipped: Optional dict that receives ``site_id -> reason`` for an unusable gauge,
        defaults to None.
    :type skipped: Dict[str, str], optional
    :return: The manifest entry dict, or None (with a printed reason).
    :rtype: Optional[Dict]
    """
    def skip(reason: str, detail: str = "") -> None:
        if skipped is not None:
            skipped[site_id] = reason
        print("skip %s: %s%s" % (site_id, reason, detail))

    scrape_dir = os.path.join(SCRAPE_ROOT, state)
    static_path = os.path.join(scrape_dir, site_id, site_id + "_static.json")
    csv_path = os.path.join(scrape_dir, site_id, site_id + "_hourly_full.csv")
    if not (os.path.exists(static_path) and os.path.exists(csv_path)):
        return skip("missing files")
    with open(static_path) as f:
        static = json.load(f)
    area = _as_float(static.get("gages2_DRAIN_SQKM"))
    if not area or area <= 0:
        drain_sq_mi = _as_float(static.get("drain_area_va"))
        area = drain_sq_mi * 2.58999 if drain_sq_mi else None
    if not area or area <= 0:
        return skip("no drainage area")
    elev_basin = _as_float(static.get("gages2_ELEV_MEAN_M_BASIN"))
    gauge_m = gauge_elevation_m(static)
    temp_offset_c = 0.0
    if elev_basin is not None and gauge_m is not None:
        temp_offset_c = -6.5 * (elev_basin - gauge_m) / 1000.0
    header = pd.read_csv(csv_path, nrows=0).columns
    present = [col for col in MET_COLS if col in header]
    required = [col for col in MET_COLS if col != "p01m"]  # p01m is fillable from NLDAS precip
    missing = [col for col in required if col not in header]
    if "cfs" not in header:
        return skip("missing flow column")
    if missing:
        return skip("missing met columns", " %s" % missing)
    frame = pd.read_csv(csv_path, usecols=["datetime", "cfs"] + present)
    if "p01m" not in frame.columns:
        frame["p01m"] = np.nan
    frame["datetime"] = pd.to_datetime(frame["datetime"], utc=True).dt.tz_localize(None)
    train = frame[frame["datetime"] < pd.Timestamp(TRAIN_END)]
    flow_mm = train["cfs"].to_numpy() * CFS_TO_MM_HR / area
    valid_hours = int(np.isfinite(flow_mm).sum())
    if valid_hours < min_train_hours:
        return skip("too few valid pre-cutoff flow hours",
                    " (%d before %s)" % (valid_hours, TRAIN_END))
    flow_scale = float(np.nanstd(flow_mm))
    if not np.isfinite(flow_scale) or flow_scale <= 0:
        return skip("degenerate flow variance")
    # Physical sanity: unit-runoff variability beyond a few mm/hr (or a sub-half-km2 "basin")
    # signals a spring, canal or the 0.01-sq-mi NWIS placeholder drainage area. Feeding such
    # flows to a water-balance model produces impossible states and NaN losses.
    if flow_scale > 5.0 or area < 0.5:
        return skip("implausible unit runoff",
                    " (scale %.1f mm/hr, area %.3f km2)" % (flow_scale, area))
    filled_p01m = train["p01m"].fillna(train["precipitation"])
    met_stats = {}
    for col in MET_COLS:
        series = filled_p01m if col == "p01m" else train[col]
        mean, std = float(series.mean()), float(series.std())
        if not (np.isfinite(mean) and np.isfinite(std)) or std <= 0:
            return skip("degenerate met column", " %s" % col)
        met_stats[col] = [mean, std]
    recent = frame[frame["datetime"] >= pd.Timestamp("2023-01-01")]
    entry = {"site_id": site_id, "csv_path": csv_path, "area_sq_km": float(area),
             "temp_offset_c": round(temp_offset_c, 4), "flow_scale_mm_hr": flow_scale,
             "met_stats": met_stats, "has_embedding": site_id in embedding_sites,
             "train_valid_hours": valid_hours, "rows_2023_plus": int(len(recent)),
             # Informational: observed flow hours from 2023 on, and the share of pre-cutoff
             # flow that is negative (tidal or backwater gauges, common in Florida).
             "flow_hours_2023_plus": int(np.isfinite(recent["cfs"].to_numpy()).sum()),
             "negative_flow_fraction":
                 round(float((flow_mm[np.isfinite(flow_mm)] < 0).mean()), 6),
             "split": "train"}
    swe_csv = os.path.join(SNODAS_ROOT, state, site_id + "_snodas_swe.csv")
    if os.path.exists(swe_csv):
        entry["swe_csv_path"] = swe_csv
    return entry


def main() -> None:
    """
    Builds and writes the manifest.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True, help="Output manifest JSON path")
    parser.add_argument("--states", nargs="+", default=["CO"],
                        help="States whose completed scrapes populate the manifest")
    parser.add_argument("--embedding-bank", default=DEFAULT_EMBEDDING_PATH,
                        help="Embedding bank path (embeddings_<fusion>.pt)")
    parser.add_argument("--carry-splits", default=None,
                        help="Existing manifest whose split labels (holdout/basin_valid) are "
                             "preserved for basins it contains — keeps the final test set and "
                             "development-validation basins IDENTICAL across manifest rebuilds")
    parser.add_argument("--n-holdout", type=int, default=15,
                        help="Holdout size among basins NOT covered by --carry-splits; "
                             "0 leaves every such basin labelled train")
    parser.add_argument("--sites-from-split", default=None,
                        help="Frozen basin split JSON (splits/basin_split_v1.json). Only its "
                             "sites are built — other gauges are skipped before their hourly CSV "
                             "is read — and each entry records its fold. Use with --n-holdout 0 "
                             "when the evaluation folds are stamped from the same split later")
    parser.add_argument("--min-train-years", type=float, default=2.0,
                        help="Minimum valid pre-cutoff years of flow to keep a gauge")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    embedding_sites = load_embedding_sites(args.embedding_bank)
    carried = {}
    if args.carry_splits:
        with open(args.carry_splits) as f:
            carried = {b["site_id"]: b.get("split", "train")
                       for b in json.load(f)["basins"]}
    split_sites = load_split_sites(args.sites_from_split) if args.sites_from_split else None

    basins = []
    summary = {}
    for state in args.states:
        with open(os.path.join(SCRAPE_ROOT, state, "registry.json")) as f:
            registry = json.load(f)
        completed = sorted(k for k, v in registry.items() if v.get("status") == "completed")
        selected = select_sites(completed, split_sites, state)
        print("%s: %d completed gauges%s" % (
            state, len(completed),
            "" if split_sites is None else ", %d in the split" % len(selected)))
        skipped: Dict[str, str] = {}
        state_basins = []
        for site_id in selected:
            entry = build_basin_entry(site_id, embedding_sites,
                                      int(args.min_train_years * 365 * 24), state=state,
                                      skipped=skipped)
            if entry is not None:
                entry["state"] = state
                if split_sites is not None:
                    entry["fold"] = split_sites[site_id]["fold"]
                state_basins.append(entry)
        basins.extend(state_basins)
        summary[state] = {"completed": len(completed), "selected": len(selected),
                          "kept": len(state_basins),
                          "with_swe_series": sum("swe_csv_path" in b for b in state_basins),
                          "dropped": dict(Counter(skipped.values())),
                          "dropped_sites": skipped}
        if split_sites is not None:
            expected = [site for site, info in split_sites.items()
                        if info.get("state") == state]
            summary[state]["split_sites_not_completed"] = sorted(set(expected) - set(selected))

    for basin in basins:
        if basin["site_id"] in carried:
            basin["split"] = carried[basin["site_id"]]
    # Fresh holdout among carried-split-free basins: embedded with 2023+ data so evaluable.
    assign_random_holdout(basins, carried, args.n_holdout, args.seed)

    manifest = {
        "embedding_path": os.path.abspath(args.embedding_bank),
        "train_end": TRAIN_END,
        "sites_from_split": (os.path.abspath(args.sites_from_split)
                             if args.sites_from_split else None),
        "build_summary": summary,
        "preprocessing": {"fill_from": {"p01m": "precipitation"},
                          # Unscaled copies for the physics path. copy_cols runs AFTER fill_from,
                          # so asos_raw carries the gridded value where the station was missing,
                          # which makes the station innovation exactly zero there; asos_observed
                          # (taken BEFORE the fill) records where the station was genuinely
                          # reporting.
                          "copy_cols": {"sw_raw": "shortwave_radiation",
                                        "precip_raw": "precipitation",
                                        "pet_raw": "pet_mm_hr",
                                        "asos_raw": "p01m"},
                          "observed_mask_cols": {"asos_observed": "p01m"},
                          # Precipitation is never linearly interpolated: it would invent rain,
                          # shift storm timing and alter totals. Windows with precipitation gaps
                          # are rejected instead.
                          "no_interp_cols": ["precipitation", "p01m", "precip_raw", "asos_raw"],
                          "lapse": {"source": "temperature", "target": "temp_lapse_k"},
                          "swe_col": "snodas_swe_mm"},
        "basins": basins,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(manifest, f, indent=1)
    n_train = sum(1 for b in basins if b["split"] == "train")
    n_embedded = sum(1 for b in basins if b["has_embedding"])
    for state, counts in summary.items():
        print("%s: selected %d, kept %d (%d with a SWE series), dropped %s"
              % (state, counts["selected"], counts["kept"], counts["with_swe_series"],
                 counts["dropped"] or "none"))
    print("Wrote %s: %d basins (%d train / %d holdout), %d with pretrained embeddings"
          % (args.out, len(basins), n_train, len(basins) - n_train, n_embedded))


if __name__ == "__main__":
    main()
