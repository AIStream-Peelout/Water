"""
Builds hourly-panel embedding records: seasonal + extreme 3-month slices per gauge.

The v0 embedding records carry 15 years of daily flow; the panel records replace that history
with six deterministic 92-day (2208-hour) HOURLY slices chosen to cover the river's character:
the best-observed window of each calendar season (winter/spring/summer/fall), the window around
the record's highest hourly flow (flash/flood behavior), and the lowest 92-day mean-flow window
(drought behavior). Deterministic selection means nothing regime-defining is left to a sampler,
and hourly resolution preserves the sub-daily dynamics the forecast model's short spin-up
cannot represent.

Slices are stored as raw cfs with start timestamps and type labels; normalization is the
training loader's job. Imagery and statics are carried over from the existing records.
"""
import argparse
import json
import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SLICE_HOURS = 92 * 24
SEASON_STARTS = {"winter": (12, 1), "spring": (3, 1), "summer": (6, 1), "fall": (9, 1)}


def load_hourly_flow(csv_path: str, end_date: Optional[str] = None) -> pd.Series:
    """
    Loads a gauge's hourly cfs series from a fleet-scrape CSV.

    :param csv_path: Path to <site>_hourly_full.csv.
    :type csv_path: str
    :param end_date: Optional exclusive upper bound (e.g. a training cutoff), defaults to None.
    :type end_date: str, optional
    :return: An hourly cfs series with NaNs for missing hours.
    :rtype: pd.Series
    """
    if "cfs" not in pd.read_csv(csv_path, nrows=0).columns:
        # Stage-only gauges (height without discharge) cannot supply a flow panel.
        return pd.Series(dtype=np.float32)
    frame = pd.read_csv(csv_path, usecols=["datetime", "cfs"], parse_dates=["datetime"])
    frame = frame.set_index("datetime").sort_index()
    if frame.index.tz is not None:
        frame.index = frame.index.tz_convert("UTC").tz_localize(None)
    flow = pd.to_numeric(frame["cfs"], errors="coerce")
    if end_date is not None:
        flow = flow[flow.index < pd.Timestamp(end_date)]
    return flow.resample("1h").mean()


def slice_window(flow: pd.Series, start: pd.Timestamp) -> np.ndarray:
    """
    Extracts one fixed-length hourly slice, padded with NaNs at the record edges.

    :param flow: The hourly series.
    :type flow: pd.Series
    :param start: The slice start hour.
    :type start: pd.Timestamp
    :return: Values of shape (SLICE_HOURS,).
    :rtype: np.ndarray
    """
    index = pd.date_range(start, periods=SLICE_HOURS, freq="1h")
    return flow.reindex(index).to_numpy(dtype=np.float32)


def select_panel(flow: pd.Series, min_coverage: float = 0.5,
                 years_per_season: int = 3) -> Optional[List[Tuple[str, pd.Timestamp]]]:
    """
    Chooses the deterministic slice start times for one gauge.

    Each season contributes its ``years_per_season`` best-observed years (coverage-descending,
    so index 0 of each season is the canonical extraction slice); different-year seasonal
    slices are the raw material for cross-year positive views during contrastive training.

    :param flow: The hourly cfs series.
    :type flow: pd.Series
    :param min_coverage: Minimum observed fraction for seasonal/drought windows, defaults
        to 0.5.
    :type min_coverage: float, optional
    :param years_per_season: Number of distinct years stored per season (fewer if the record
        cannot supply them), defaults to 3.
    :type years_per_season: int, optional
    :return: Ordered (slice_type, start) pairs, or None if the record cannot support a panel.
    :rtype: List[Tuple[str, pd.Timestamp]], optional
    """
    valid = flow.dropna()
    if len(valid) < 3 * 8760 or not isinstance(flow.index, pd.DatetimeIndex):
        return None
    observed = flow.notna().astype(np.float32)
    coverage = observed.rolling(SLICE_HOURS).mean()
    panel: List[Tuple[str, pd.Timestamp]] = []
    years = range(flow.index[0].year, flow.index[-1].year + 1)
    for season, (month, day) in SEASON_STARTS.items():
        candidates: List[Tuple[float, int, pd.Timestamp]] = []
        for year in years:
            start = pd.Timestamp(year=year, month=month, day=day)
            end = start + pd.Timedelta(hours=SLICE_HOURS - 1)
            if start < flow.index[0] or end > flow.index[-1]:
                continue
            window_coverage = float(coverage.get(end, 0.0))
            if window_coverage >= min_coverage:
                # Recency breaks coverage ties in favor of newer years.
                candidates.append((window_coverage, year, start))
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        for _, _, start in candidates[:years_per_season]:
            panel.append((season, start))
    peak_time = valid.idxmax()
    peak_start = max(flow.index[0], peak_time - pd.Timedelta(hours=SLICE_HOURS // 2))
    peak_start = min(peak_start, flow.index[-1] - pd.Timedelta(hours=SLICE_HOURS - 1))
    panel.append(("flood", peak_start.floor("1h")))
    log_flow = np.log1p(flow.clip(lower=0.0))
    rolling_mean = log_flow.rolling(SLICE_HOURS, min_periods=int(SLICE_HOURS * min_coverage)
                                    ).mean()
    if rolling_mean.notna().any():
        drought_end = rolling_mean.idxmin()
        drought_start = drought_end - pd.Timedelta(hours=SLICE_HOURS - 1)
    else:
        drought_start = panel[0][1]
    panel.append(("drought", drought_start))
    return panel


def regional_arrays(source_dir: str, site: str) -> Dict[str, np.ndarray]:
    """
    Loads a site's regional-context sidecar (``<site>_regional.npz``) if it exists.

    :param source_dir: The state's embedding-record directory.
    :type source_dir: str
    :param site: The USGS site id.
    :type site: str
    :return: The sidecar arrays keyed as stored (image_regional, image_regional_alt, ...), or {}.
    :rtype: Dict[str, np.ndarray]
    """
    path = os.path.join(source_dir, site + "_regional.npz")
    if not os.path.exists(path):
        return {}
    with np.load(path, allow_pickle=True) as sidecar:
        return {key: sidecar[key] for key in sidecar.files}


def compact_regional(regional: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    """
    Stores regional images as uint16 (lossless for Sentinel-2 L1C digital numbers).

    A float32 512x512x6 patch is 6.3 MB; two seasons per record, zlib-compressed, cost
    ~3 min of decompression per fleet epoch. uint16 halves the bytes and, written
    uncompressed, loads as a plain copy.

    :param regional: The sidecar arrays.
    :type regional: Dict[str, np.ndarray]
    :return: The arrays with image_regional* cast to uint16.
    :rtype: Dict[str, np.ndarray]
    """
    return {key: (np.clip(np.rint(value), 0, 65535).astype(np.uint16)
                  if key.startswith("image_regional") else value)
            for key, value in regional.items()}


def merge_regional(state: str, embedding_root: str, output_root: str,
                   force: bool = False) -> Dict[str, int]:
    """
    Adds regional-context arrays to already-built panel records that lack them.

    Records are rewritten uncompressed with uint16 regional images (see
    :func:`compact_regional`) so training epochs are not decompression-bound.

    :param state: Two-letter state abbreviation.
    :type state: str
    :param embedding_root: Root of the embedding records (holding the sidecars).
    :type embedding_root: str
    :param output_root: Root of the panel records.
    :type output_root: str
    :param force: Rewrite records that already carry regional arrays (e.g. to convert
        earlier float32/compressed merges), defaults to False.
    :type force: bool, optional
    :return: Counts of merged/already_merged/no_sidecar records.
    :rtype: Dict[str, int]
    """
    source_dir, output_dir = os.path.join(embedding_root, state), os.path.join(output_root, state)
    counts = {"merged": 0, "already_merged": 0, "no_sidecar": 0}
    for name in sorted(os.listdir(output_dir)):
        if not name.endswith(".npz") or name.endswith("_regional.npz"):
            continue
        output_path = os.path.join(output_dir, name)
        with np.load(output_path, allow_pickle=True) as record:
            if "image_regional" in record.files and not force:
                counts["already_merged"] += 1
                continue
            arrays = {key: record[key] for key in record.files
                      if not key.startswith("image_regional") and key not in ("bands",
                                                                              "pixel_meters")}
        regional = regional_arrays(source_dir, name[:-4])
        if not regional:
            counts["no_sidecar"] += 1
            continue
        np.savez(output_path + ".tmp.npz", **arrays, **compact_regional(regional))
        os.replace(output_path + ".tmp.npz", output_path)
        counts["merged"] += 1
    return counts


def build_state(state: str, embedding_root: str, scrape_root: str, output_root: str,
                end_date: Optional[str] = None) -> Dict[str, int]:
    """
    Builds panel records for every embedded site of a state with an available hourly scrape.

    :param state: Two-letter state abbreviation.
    :type state: str
    :param embedding_root: Root of the existing v0 embedding records.
    :type embedding_root: str
    :param scrape_root: Root of the fleet scrape directories.
    :type scrape_root: str
    :param output_root: Root for the panel records.
    :type output_root: str
    :param end_date: Optional exclusive record cutoff, defaults to None.
    :type end_date: str, optional
    :return: Counts of built/skipped/missing records.
    :rtype: Dict[str, int]
    """
    source_dir = os.path.join(embedding_root, state)
    output_dir = os.path.join(output_root, state)
    os.makedirs(output_dir, exist_ok=True)
    counts = {"built": 0, "already_done": 0, "no_hourly_csv": 0, "record_too_short": 0}
    for name in sorted(os.listdir(source_dir)):
        if not name.endswith(".npz") or name.endswith("_regional.npz"):
            continue
        site = name[:-4]
        output_path = os.path.join(output_dir, name)
        if os.path.exists(output_path):
            counts["already_done"] += 1
            continue
        csv_path = os.path.join(scrape_root, state, site, "%s_hourly_full.csv" % site)
        if not os.path.exists(csv_path):
            counts["no_hourly_csv"] += 1
            continue
        flow = load_hourly_flow(csv_path, end_date=end_date)
        panel = select_panel(flow)
        if panel is None:
            counts["record_too_short"] += 1
            continue
        slices = np.stack([slice_window(flow, start) for _, start in panel])
        # Uncompressed: training reads every record every epoch, and regional images make
        # zlib decompression the epoch bottleneck (the panel itself is only ~400 KB).
        with np.load(os.path.join(source_dir, name), allow_pickle=True) as record:
            np.savez(
                output_path, image=record["image"], static=record["static"],
                static_names=record["static_names"], panel=slices,
                panel_types=np.array([t for t, _ in panel]),
                panel_starts=np.array([str(s) for _, s in panel]),
                **compact_regional(regional_arrays(source_dir, site)))
        counts["built"] += 1
        print("%s: built (%s)" % (site, ", ".join(t for t, _ in panel)), flush=True)
    return counts


def main() -> None:
    """
    CLI entry point.

    :return: None
    :rtype: None
    """
    parser = argparse.ArgumentParser(description="Build hourly-panel embedding records.")
    parser.add_argument("--states", nargs="+", default=["CO", "UT"])
    parser.add_argument("--embedding-root", default=os.path.join("pilot_data",
                                                                 "embedding_dataset"))
    parser.add_argument("--scrape-root", default=os.path.join("pilot_data", "scrapes"))
    parser.add_argument("--output-root", default=os.path.join("pilot_data",
                                                              "embedding_dataset_hourly"))
    parser.add_argument("--end-date", default=None,
                        help="Optional exclusive cutoff (e.g. 2022-01-01) for leakage-clean "
                             "banks")
    parser.add_argument("--merge-regional", action="store_true",
                        help="Only add regional-context sidecar arrays to existing panel "
                             "records that lack them (no panel rebuild)")
    parser.add_argument("--force-regional", action="store_true",
                        help="With --merge-regional: rewrite records that already carry "
                             "regional arrays (converts them to uint16, uncompressed)")
    args = parser.parse_args()
    summary = {}
    for state in args.states:
        if args.merge_regional:
            summary[state] = merge_regional(state, args.embedding_root, args.output_root,
                                            force=args.force_regional)
        else:
            summary[state] = build_state(state, args.embedding_root, args.scrape_root,
                                         args.output_root, end_date=args.end_date)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
