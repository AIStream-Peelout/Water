"""
Bulk builder of daily basin-mean SNODAS SWE series for the CO pilot fleet.

Downloads the daily SNODAS masked tars (see ``snodas_functions``), extracts the basin-mean SWE for
every completed registry basin with a cached boundary GeoJSON in one pass per day, writes one JSON
per day under ``pilot_data/snodas_series/CO/daily/`` (resume-safe: existing days are skipped, and
upstream-missing days leave a ``{"missing": true}`` marker), then deletes the tar to bound disk
use. Basin masks over the fixed SNODAS grid are precomputed once with ``build_masks`` (the 1-km
masked grid geometry never changes) and reused as flat cell indices. ``compile_series`` folds the
daily JSONs into per-basin ``<site>_snodas_swe.csv`` files.

Only days between October 1 and July 15 are fetched: outside that window CO basin-mean SWE is
zero, and the downstream forecast loader treats missing days as "no observation", which yields the
same (empty) snow-store initialization.

Usage:
    python snodas_series_scrape.py masks
    python snodas_series_scrape.py scrape --range 2022-12-01:2026-07-26 --range 2019-10-01:2022-11-30
    python snodas_series_scrape.py compile
"""
import argparse
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from dem_functions import polygon_mask
from snodas_functions import (SWE_PRODUCT_CODE, download_snodas_tar, read_snodas_swe,
                              snodas_tar_url, swe_grid_from_members)

SERIES_DIR = os.path.join("pilot_data", "snodas_series", "CO")
MASKS_PATH = os.path.join(SERIES_DIR, "basin_masks.npz")
REGISTRY_PATH = os.path.join("pilot_data", "scrapes", "CO", "registry.json")
SCRAPES_DIR = os.path.join("pilot_data", "scrapes", "CO")
SEASON_START = (10, 1)  # Oct 1
SEASON_END = (7, 15)  # Jul 15


def completed_sites(registry_path: str = REGISTRY_PATH) -> List[str]:
    """
    Lists registry sites marked completed that also have a cached basin boundary GeoJSON.

    :param registry_path: The path of the CO scrape registry JSON.
    :type registry_path: str, optional
    :return: The usable site ids, sorted.
    :rtype: List[str]
    """
    with open(registry_path) as f:
        registry = json.load(f)
    sites = []
    for site, entry in registry.items():
        status = entry.get("status") if isinstance(entry, dict) else entry
        geojson = os.path.join(SCRAPES_DIR, site, site + "_basin.geojson")
        if status == "completed" and os.path.exists(geojson):
            sites.append(site)
    return sorted(sites)


def build_masks(reference_date: datetime = datetime(2024, 6, 1),
                masks_path: str = MASKS_PATH) -> Dict[str, np.ndarray]:
    """
    Precomputes flat grid-cell indices inside each basin polygon on the fixed SNODAS masked grid.

    :param reference_date: Any date whose tar supplies the grid geometry, defaults to 2024-06-01.
    :type reference_date: datetime, optional
    :param masks_path: Where the mask archive is written, defaults to
        "pilot_data/snodas_series/CO/basin_masks.npz".
    :type masks_path: str, optional
    :return: site id -> flat indices into the raveled (rows * cols) SWE grid.
    :rtype: Dict[str, np.ndarray]
    """
    grid = read_snodas_swe(download_snodas_tar(reference_date))
    lats, lons = grid["lats"], grid["lons"]
    masks: Dict[str, np.ndarray] = {}
    for site in completed_sites():
        with open(os.path.join(SCRAPES_DIR, site, site + "_basin.geojson")) as f:
            geometry = json.load(f)
        inside = polygon_mask(lats, lons, geometry)
        indices = np.flatnonzero(inside.ravel())
        if len(indices) == 0:
            print("WARN no grid cells inside basin " + site + "; skipping")
            continue
        masks[site] = indices
        print("mask %s: %d cells" % (site, len(indices)))
    os.makedirs(os.path.dirname(masks_path), exist_ok=True)
    np.savez_compressed(masks_path, shape=np.array(grid["swe_mm"].shape), **masks)
    return masks


def season_dates(start: datetime, end: datetime) -> List[datetime]:
    """
    Enumerates the dates in [start, end] that fall inside the Oct 1 - Jul 15 snow season.

    :param start: The first candidate date.
    :type start: datetime
    :param end: The last candidate date (inclusive).
    :type end: datetime
    :return: The in-season dates, ascending.
    :rtype: List[datetime]
    """
    dates = []
    date = start
    while date <= end:
        after_start = (date.month, date.day) >= SEASON_START
        before_end = (date.month, date.day) <= SEASON_END
        if after_start or before_end:
            dates.append(date)
        date += timedelta(days=1)
    return dates


_THREAD_LOCAL = threading.local()


def _session() -> requests.Session:
    """
    Returns this thread's keep-alive HTTP session (created on first use).

    :return: The thread-local requests session.
    :rtype: requests.Session
    """
    if not hasattr(_THREAD_LOCAL, "session"):
        _THREAD_LOCAL.session = requests.Session()
    return _THREAD_LOCAL.session


def fetch_swe_members(date: datetime) -> Optional[Tuple[bytes, bytes]]:
    """
    Fetches only the SWE (product 1034) header/data members of a day's tar via HTTP Range reads.

    Walks the tar's 512-byte member headers with small ranged requests and downloads just the two
    SWE members (~6 MB) instead of the full ~28 MB archive — NSIDC throttles bandwidth per IP, so
    shrinking bytes is the only real speedup. Falls back to the caller on servers that ignore
    Range (a 200 response raises).

    :param date: The day to fetch.
    :type date: datetime
    :return: The (gzipped header bytes, gzipped data bytes) pair, or None when the day is missing
        upstream (HTTP 404).
    :rtype: Tuple[bytes, bytes], optional
    """
    url = snodas_tar_url(date)
    session = _session()
    pos = 0
    header_gz = data_gz = None
    for _ in range(64):
        resp = session.get(url, headers={"Range": "bytes=%d-%d" % (pos, pos + 511)}, timeout=120)
        if resp.status_code == 404:
            return None
        if resp.status_code != 206:
            raise ValueError("Server ignored Range request (%d) for %s" % (resp.status_code, url))
        block = resp.content
        if len(block) < 512 or not block[:100].strip(b"\x00"):
            break
        name = block[:100].split(b"\x00", 1)[0].decode("ascii", errors="replace")
        size = int(block[124:136].split(b"\x00", 1)[0].strip() or b"0", 8)
        if SWE_PRODUCT_CODE in name and name.endswith((".dat.gz", ".txt.gz")):
            member = session.get(url, headers={"Range": "bytes=%d-%d"
                                               % (pos + 512, pos + 511 + size)}, timeout=300)
            member.raise_for_status()
            if name.endswith(".dat.gz"):
                data_gz = member.content
            else:
                header_gz = member.content
            if header_gz is not None and data_gz is not None:
                return header_gz, data_gz
        pos += 512 + ((size + 511) // 512) * 512
    raise ValueError("SWE members not found in " + url)


def basin_means_from_grid(grid: Dict, masks: Dict[str, np.ndarray],
                          expected_shape: Tuple[int, int]) -> Dict[str, float]:
    """
    Extracts every basin's mean SWE from a parsed grid using precomputed flat cell indices.

    :param grid: A grid dict from :func:`snodas_functions.swe_grid_from_members`.
    :type grid: Dict
    :param masks: site id -> flat grid indices from :func:`build_masks`.
    :type masks: Dict[str, np.ndarray]
    :param expected_shape: The (rows, cols) the masks were computed on; mismatching grids raise.
    :type expected_shape: Tuple[int, int]
    :return: site id -> basin-mean SWE in mm (NaN when the basin has no valid cells).
    :rtype: Dict[str, float]
    """
    if tuple(grid["swe_mm"].shape) != tuple(expected_shape):
        raise ValueError("Grid shape changed: %s" % (grid["swe_mm"].shape,))
    flat = grid["swe_mm"].ravel()
    means = {}
    for site, indices in masks.items():
        values = flat[indices]
        values = values[~np.isnan(values)]
        means[site] = float(values.mean()) if len(values) else float("nan")
    return means


def day_json_path(date: datetime) -> str:
    """
    Builds the output path of one day's basin-mean JSON.

    :param date: The day.
    :type date: datetime
    :return: The JSON path under the daily output directory.
    :rtype: str
    """
    return os.path.join(SERIES_DIR, "daily", date.strftime("%Y%m%d") + ".json")


def extract_day(date: datetime, masks: Dict[str, np.ndarray], expected_shape: Tuple[int, int],
                keep_tar: bool = False) -> Optional[Dict[str, float]]:
    """
    Downloads (if needed) one day's tar, extracts every basin's mean SWE and removes the tar.

    :param date: The day to process.
    :type date: datetime
    :param masks: site id -> flat grid indices from :func:`build_masks`.
    :type masks: Dict[str, np.ndarray]
    :param expected_shape: The (rows, cols) the masks were computed on; mismatching grids raise.
    :type expected_shape: Tuple[int, int]
    :param keep_tar: Whether to keep the downloaded tar on disk, defaults to False.
    :type keep_tar: bool, optional
    :return: site id -> basin-mean SWE in mm, or None when the day is missing upstream.
    :rtype: Dict[str, float], optional
    """
    try:
        tar = download_snodas_tar(date)
    except FileNotFoundError:
        return None
    grid = read_snodas_swe(tar)
    means = basin_means_from_grid(grid, masks, expected_shape)
    if not keep_tar:
        os.remove(tar)
    return means


def scrape_ranges(ranges: List[Tuple[datetime, datetime]], workers: int = 6) -> None:
    """
    Processes date ranges in the order given (put the highest-priority range first).

    Downloads run in a thread pool while extraction happens as archives arrive; each finished day
    is written immediately so an interrupted run resumes where it stopped.

    :param ranges: (start, end) date pairs, processed sequentially.
    :type ranges: List[Tuple[datetime, datetime]]
    :param workers: Parallel download threads, defaults to 6.
    :type workers: int, optional
    """
    archive = np.load(MASKS_PATH)
    expected_shape = tuple(archive["shape"])
    masks = {site: archive[site] for site in archive.files if site != "shape"}
    print("Loaded %d basin masks" % len(masks))
    for start, end in ranges:
        dates = [d for d in season_dates(start, end) if not os.path.exists(day_json_path(d))]
        print("Range %s -> %s: %d days to fetch" % (start.date(), end.date(), len(dates)))
        os.makedirs(os.path.join(SERIES_DIR, "daily"), exist_ok=True)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetch_swe_members, date): date for date in dates}
            for future, date in futures.items():
                try:
                    members = future.result()
                    if members is None:
                        means = None
                    else:
                        means = basin_means_from_grid(swe_grid_from_members(*members), masks,
                                                      expected_shape)
                except Exception as ranged_error:  # noqa: BLE001 - fall back to the full tar
                    try:
                        means = extract_day(date, masks, expected_shape)
                    except FileNotFoundError:
                        means = None
                    except Exception as error:  # noqa: BLE001 - transient, retried on resume
                        print("RETRY-LATER %s: ranged=%s full=%s"
                              % (date.date(), ranged_error, error))
                        continue
                payload = {"missing": True} if means is None else means
                with open(day_json_path(date), "w") as f:
                    json.dump(payload, f)
                print("done %s%s" % (date.date(), " (missing upstream)" if means is None else ""))


def compile_series() -> None:
    """
    Folds the daily JSONs into one ``<site>_snodas_swe.csv`` per basin (datetime ascending).
    """
    daily_dir = os.path.join(SERIES_DIR, "daily")
    rows: Dict[str, List[Tuple[str, float]]] = {}
    for name in sorted(os.listdir(daily_dir)):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(daily_dir, name)) as f:
            day = json.load(f)
        if day.get("missing"):
            continue
        stamp = datetime.strptime(name[:8], "%Y%m%d").strftime("%Y-%m-%d")
        for site, value in day.items():
            rows.setdefault(site, []).append((stamp, value))
    for site, records in rows.items():
        frame = pd.DataFrame(records, columns=["datetime", "snodas_swe_mm"])
        frame.to_csv(os.path.join(SERIES_DIR, site + "_snodas_swe.csv"), index=False)
    print("Wrote %d basin series" % len(rows))


def main() -> None:
    """
    Command-line entry point: ``masks``, ``scrape`` or ``compile``.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["masks", "scrape", "compile"])
    parser.add_argument("--range", action="append", default=[],
                        help="start:end date pair, e.g. 2022-12-01:2026-07-26; repeatable, "
                             "processed in order")
    parser.add_argument("--workers", type=int, default=6)
    args = parser.parse_args()
    if args.command == "masks":
        build_masks()
    elif args.command == "scrape":
        ranges = []
        for pair in args.range:
            start, end = pair.split(":")
            ranges.append((datetime.strptime(start, "%Y-%m-%d"),
                           datetime.strptime(end, "%Y-%m-%d")))
        scrape_ranges(ranges, workers=args.workers)
    else:
        compile_series()


if __name__ == "__main__":
    main()
