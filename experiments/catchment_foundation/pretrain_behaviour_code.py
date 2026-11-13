"""
Pretrains a general catchment code and writes it as a context bank.

Step 2 of ``notes/REPRESENTATION_PLAN_2026-11-12.md``. The reader of the gauged record sees a few
water years of a basin's flow and weather, in physical units, and writes a short code. A decoder
shared by all basins must reproduce the flow of another water year from that year's weather and
the code (Flow Forecast ``behaviour_pretrain``). Nothing after the cutoff is read, held-out
basins are never trained on, and their codes come from their own pre-cutoff record.

With ``--static-banks`` and ``--image-records`` the same code is also written by a reader of the
static description and a reader of the reach image. Readers are switched off at random in
training, so the code can be written from any of them: besides ``context_<name>.pt`` (all
readers) the run writes ``context_<name>_ungauged.pt`` (no gauged record), ``_gauge``,
``_statics`` and ``_image``.

``align`` is the second stage of §3 of the plan. The gauge reader and the decoder of a finished
``train`` run stay as they are; a second encoder reads the static description (and optionally
the reach image) and is fitted on the training basins so that the frozen decoder reproduces
their flow from its code and its code matches the gauge code of the same basin. The gauged code
is untouched, and an ungauged basin lands in the same space.

``pool`` adds basins to pretraining without touching the cohort a code is scored on. It stores
the pre-cutoff record of further basins of a larger manifest, leaving out the cohort's own and
every basin whose HUC6 group holds a held-out site; ``train --with-pool`` and ``align
--with-pool`` then fit on them as well. Validation, the basins that get a code and the scale of
the banks stay the cohort's.

    cache    read every basin's hourly file once and keep the pre-cutoff daily record
    pool     store the pre-cutoff record of extra basins that may only be pretrained on
    train    pretrain on the training basins and write the banks for every basin
    align    fit the statics (and image) readers to the code of a finished gauge-only run
    shuffle  write ``context_<name>_shuffled.pt``: a bank's codes permuted within each split

    python -m experiments.catchment_foundation.pretrain_behaviour_code cache --arms-dir <arms dir>
    python -m experiments.catchment_foundation.pretrain_behaviour_code train \
        --arms-dir <arms dir> --name behaviour_s0 --seed 0
    python -m experiments.catchment_foundation.pretrain_behaviour_code train \
        --arms-dir <arms dir> --name multimodal_s0 --seed 0 --aux-weight 1.0 \
        --static-banks statics location --image-records pilot_data/embedding_dataset
    python -m experiments.catchment_foundation.pretrain_behaviour_code align \
        --arms-dir <arms dir> --name student_s0 --teacher behaviour_aux_s0

Score the banks with ``representation_benchmark.py run`` and ``tabular_forecast_baseline.py``.
Flow Forecast is imported from ``FF_REPO`` or ``PYTHONPATH`` (branch ``behaviour-pretraining``).
"""
import argparse
import json
import os
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

from experiments.catchment_foundation import representation_benchmark as benchmark
from experiments.catchment_foundation import tabular_forecast_baseline as tabular

if os.environ.get("FF_REPO"):
    sys.path.insert(0, os.environ["FF_REPO"])

CUTOFF = tabular.CUTOFF
YEAR_DAYS, SPIN_UP_DAYS = 365, 90
MIN_FLOW_DAYS = 150                   # valid flow days for a water year to be used (seasonal
#                                       gauges report about half of each year)
FLOW_FLOOR_MM = 0.01                  # mm per day added before the logarithm
AUX = ["log_runoff_mm", "log_runoff_ratio", "snow_fraction", "zero_flow_fraction", "log_aridity",
       "recession_rate", "storm_response", "rain_flow_coupling", "rb_flashiness", "bfi",
       "melt_share", "flood_peak", "flood_ratio", "low_flow"]
CACHE = "daily_pre_cutoff.pkl"
POOL = "daily_pool_pre_cutoff.pkl"    # extra basins a code may be pretrained on (never scored)
IMAGE_SCALE = 3000.0                  # reflectance divisor, as in Flow Forecast's catchment loader
HISTORY_CHANNELS = 7                  # log flow, valid flag and the five weather channels


def daily_record(basin: Dict) -> Optional[Tuple[str, pd.DataFrame]]:
    """
    Reads one basin's daily flow and weather before the cutoff.

    :param basin: Manifest entry.
    :type basin: Dict
    :return: Site id and a daily frame with ``q_mm``, ``p``, ``t`` (deg C at basin elevation)
        and ``pet``, or None when the hourly file cannot be read.
    :rtype: Tuple[str, pd.DataFrame], optional
    """
    hourly = tabular.load_hourly(basin)
    if hourly is None:
        return None
    daily = tabular.daily_frame(hourly, basin)
    daily = daily.loc[daily.index < pd.Timestamp(CUTOFF), ["q_mm", "p", "t", "pet"]]
    daily["t"] = daily["t"] + float(basin.get("temp_offset_c") or 0.0)
    return str(basin["site_id"]), daily.asfreq("1D").astype(np.float32)


def build_cache(manifest_path: str, arms_dir: str, workers: int) -> Dict:
    """
    Stores the pre-cutoff daily record and the split of every basin of a manifest.

    :param manifest_path: Manifest whose basins carry ``split`` (train or holdout).
    :type manifest_path: str
    :param arms_dir: Directory of the context banks; the cache is written here.
    :type arms_dir: str
    :param workers: Processes reading hourly files.
    :type workers: int
    :return: The cache (``daily``: site id -> frame, ``split``: site id -> split).
    :rtype: Dict
    """
    with open(manifest_path) as file:
        basins = json.load(file)["basins"]
    started = time.time()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        records = [item for item in pool.map(daily_record, basins, chunksize=4) if item]
    cache = {"daily": dict(records),
             "split": {str(basin["site_id"]): basin["split"] for basin in basins},
             "manifest": os.path.abspath(manifest_path)}
    pd.to_pickle(cache, os.path.join(arms_dir, CACHE))
    print("cached %d of %d basins in %.0f s" % (len(records), len(basins),
                                                time.time() - started), flush=True)
    return cache


def pool_record(basin: Dict) -> Optional[Tuple[str, pd.DataFrame, Optional[Dict]]]:
    """
    Reads one extra basin: its daily record and behaviour numbers, both before the cutoff.

    :param basin: Manifest entry.
    :type basin: Dict
    :return: Site id, the daily frame of :func:`daily_record` and the behaviour numbers (None
        when the record is too short for them), or None when the hourly file cannot be read.
    :rtype: Tuple[str, pd.DataFrame, Optional[Dict]], optional
    """
    hourly = tabular.load_hourly(basin)
    if hourly is None:
        return None
    daily = tabular.daily_frame(hourly, basin)
    cutoff = pd.Timestamp(CUTOFF)
    offset = float(basin.get("temp_offset_c") or 0.0)
    early = benchmark.behaviour(daily[daily.index < cutoff], hourly[hourly.index < cutoff],
                                basin["area_sq_km"], offset)
    record = daily.loc[daily.index < cutoff, ["q_mm", "p", "t", "pet"]].copy()
    record["t"] = record["t"] + offset
    return str(basin["site_id"]), record.asfreq("1D").astype(np.float32), early


def select_pool(basins: List[Dict], split_sites: Dict[str, Dict], cohort: Iterable[str],
                exclude_folds: Iterable[int], max_negative_flow_fraction: Optional[float] = None
                ) -> Tuple[List[Dict], Dict[str, int]]:
    """
    Chooses the extra basins a code may be pretrained on.

    A basin is left out when the cohort already holds it, when the split does not know it, or
    when its HUC6 group holds a site of an excluded fold in any state, so no extra basin trains
    upstream or downstream of a held-out one. Tidal and backwater gauges can be left out by
    their share of negative flow, as ``prepare_context_ablation.py`` does for a cohort.

    :param basins: Manifest entries of the candidates.
    :type basins: List[Dict]
    :param split_sites: The basin split's ``sites`` (site id -> group and fold).
    :type split_sites: Dict[str, Dict]
    :param cohort: Site ids of the cohort the code is scored on.
    :type cohort: Iterable[str]
    :param exclude_folds: Folds whose groups stay untouched (the held-out folds).
    :type exclude_folds: Iterable[int]
    :param max_negative_flow_fraction: Largest ``negative_flow_fraction`` of a manifest entry
        that is kept, defaults to None (no limit).
    :type max_negative_flow_fraction: float, optional
    :return: The chosen entries and the number of basins left out for each reason.
    :rtype: Tuple[List[Dict], Dict[str, int]]
    """
    excluded = set(exclude_folds)
    limit = np.inf if max_negative_flow_fraction is None else max_negative_flow_fraction
    held_groups = {info["group"] for info in split_sites.values() if info["fold"] in excluded}
    taken, chosen, dropped = set(str(site) for site in cohort), [], Counter()
    for basin in basins:
        site = str(basin["site_id"])
        if site in taken:
            dropped["already in the cohort or listed twice"] += 1
        elif site not in split_sites:
            dropped["not in the split"] += 1
        elif split_sites[site]["group"] in held_groups:
            dropped["in a held-out group"] += 1
        elif basin.get("negative_flow_fraction", 0.0) > limit:
            dropped["tidal or backwater"] += 1
        else:
            chosen.append(basin)
            taken.add(site)
    return chosen, dict(dropped)


def pool_static_banks(arms_dir: str, split: Dict[str, str], pool_sites: List[str],
                      gages2_zip: str, gauges_root: str) -> Dict[str, pd.DataFrame]:
    """
    Describes extra basins on the scale of the cohort's ``statics`` and ``location`` banks.

    The cohort's rows are rebuilt alongside and must reproduce the banks on disk, so an extra
    basin's numbers mean what a cohort basin's numbers mean.

    :param arms_dir: Directory of the cohort's context banks.
    :type arms_dir: str
    :param split: Cohort site id -> split (train or holdout), in bank order.
    :type split: Dict[str, str]
    :param pool_sites: The extra basins.
    :type pool_sites: List[str]
    :param gages2_zip: Path of the GAGES-II archive.
    :type gages2_zip: str
    :param gauges_root: Scrape root holding ``<state>/gauges.csv``.
    :type gauges_root: str
    :return: Bank name -> frame indexed by site id with the columns ``tabular.load_bank``
        returns. ``statics`` only holds the extra basins that are in GAGES-II.
    :rtype: Dict[str, pd.DataFrame]
    """
    from embedding_dataset import build_static_matrix
    from experiments.catchment_foundation.embedding_probes import STATICS_BASELINE_EXCLUDE
    from experiments.catchment_foundation.prepare_context_ablation import standardize_context
    cohort = list(split)
    train_sites = [site for site in cohort if split[site] == "train"]
    matrix, names = build_static_matrix(gages2_zip)
    physical = [name for name in names
                if not any(name.startswith(prefix) for prefix in STATICS_BASELINE_EXCLUDE)]
    described = [site for site in pool_sites if site in matrix.index]
    raw = matrix.reindex(cohort + described)[physical].to_numpy(dtype=np.float32)
    train_rows = np.array([split[site] == "train" for site in cohort] + [False] * len(described))
    scaled = standardize_context(raw, train_rows, benchmark.CONTEXT_WIDTH)
    place = benchmark.load_locations(gauges_root)
    rebuilt = {"statics": (scaled, described),
               "location": (benchmark.location_km(place, cohort + pool_sites, cohort, train_sites),
                            pool_sites)}
    banks = {}
    for name, (values, extra) in rebuilt.items():
        stored = torch.load(os.path.join(arms_dir, "context_%s.pt" % name),
                            weights_only=True)["embeddings"].numpy()
        filled = np.abs(stored).sum(axis=0) > 0
        values = values[:, :stored.shape[1]][:, filled]
        gap = float(np.abs(values[:len(cohort)] - stored[:, filled]).max())
        if gap > 1e-3:
            raise ValueError("the rebuilt %s bank differs from the one on disk by up to %.3g"
                             % (name, gap))
        banks[name] = pd.DataFrame(values[len(cohort):], index=extra, dtype=np.float32)
    return banks


def build_pool(manifest_path: str, split_path: str, arms_dir: str,
               exclude_folds: Sequence[int] = (0,), workers: int = 6,
               gages2_zip: Optional[str] = None, gauges_root: Optional[str] = None,
               max_negative_flow_fraction: Optional[float] = None) -> Dict:
    """
    Stores the extra basins a code may be pretrained on, next to a cohort's daily cache.

    The cohort and its split are untouched: codes are still written and scored for the
    cohort's basins only. The pool adds basins to the fitting set and nothing else.

    :param manifest_path: Manifest of the candidate basins (any set that includes the extras).
    :type manifest_path: str
    :param split_path: Basin split that knows the candidates (site id -> group and fold).
    :type split_path: str
    :param arms_dir: Directory holding the cohort's daily cache; the pool is written here.
    :type arms_dir: str
    :param exclude_folds: Folds whose HUC6 groups are kept out of the pool, defaults to (0,).
    :type exclude_folds: Sequence[int], optional
    :param workers: Processes reading hourly files.
    :type workers: int
    :param gages2_zip: GAGES-II archive; with ``gauges_root``, the pool also carries the
        ``statics`` and ``location`` descriptions of its basins. Defaults to None.
    :type gages2_zip: str, optional
    :param gauges_root: Scrape root holding ``<state>/gauges.csv``, defaults to None.
    :type gauges_root: str, optional
    :param max_negative_flow_fraction: Leave out basins with a larger share of negative flow,
        defaults to None. Use the limit the cohort was built with.
    :type max_negative_flow_fraction: float, optional
    :return: The pool (``daily``, ``early``, ``banks``, ``state``, ``fold`` and its sources).
    :rtype: Dict
    """
    split = pd.read_pickle(os.path.join(arms_dir, CACHE))["split"]
    with open(manifest_path) as file:
        basins = json.load(file)["basins"]
    with open(split_path) as file:
        split_sites = json.load(file)["sites"]
    chosen, dropped = select_pool(basins, split_sites, split, exclude_folds,
                                  max_negative_flow_fraction)
    started = time.time()
    with ProcessPoolExecutor(max_workers=workers) as readers:
        records = [item for item in readers.map(pool_record, chosen, chunksize=4) if item]
    daily = {site: frame for site, frame, _ in records}
    early = pd.DataFrame({site: numbers for site, _, numbers in records if numbers}).T
    banks = None
    if gages2_zip and gauges_root:
        banks = pool_static_banks(arms_dir, split, sorted(daily), gages2_zip, gauges_root)
    pool = {"daily": daily, "early": early, "banks": banks,
            "state": {str(basin["site_id"]): basin.get("state", "") for basin in chosen},
            "fold": {site: split_sites[site]["fold"] for site in daily},
            "manifest": os.path.abspath(manifest_path), "split": os.path.abspath(split_path),
            "exclude_folds": sorted(exclude_folds), "dropped": dropped,
            "max_negative_flow_fraction": max_negative_flow_fraction}
    pd.to_pickle(pool, os.path.join(arms_dir, POOL))
    usable = sum(len(water_years(frame)) >= 2 for frame in daily.values())
    print("pool: %d extra basins read in %.0f s (%d with two usable water years, %d with "
          "behaviour numbers, %s with statics); left out: %s"
          % (len(daily), time.time() - started, usable, len(early),
             "none" if banks is None else len(banks["statics"]), dropped), flush=True)
    return pool


def pool_members(pool: Optional[Dict], split: Dict[str, str],
                 statics: Optional[pd.DataFrame]) -> List[str]:
    """
    Lists the pool's basins a run can fit on.

    :param pool: The pool of :func:`build_pool`, or None.
    :type pool: Dict, optional
    :param split: The cohort's split; its basins are never taken from the pool.
    :type split: Dict[str, str]
    :param statics: The static description the run reads, or None; a pool basin without one
        is left out, so a statics reader never fits on a blank description.
    :type statics: pd.DataFrame, optional
    :return: Site ids, sorted.
    :rtype: List[str]
    """
    if pool is None:
        return []
    return sorted(site for site in pool["daily"]
                  if site not in split and (statics is None or site in statics.index))


def water_years(daily: pd.DataFrame) -> List[pd.Timestamp]:
    """
    Lists the 1 October starts of the water years a basin can be trained or read on.

    A year qualifies with at least ``MIN_FLOW_DAYS`` days of flow, 365 days inside the record
    and ``SPIN_UP_DAYS`` of record before its start.

    :param daily: Daily frame from :func:`daily_record`.
    :type daily: pd.DataFrame
    :return: The qualifying start dates, in order.
    :rtype: List[pd.Timestamp]
    """
    if daily.empty:
        return []
    starts = []
    for year in range(daily.index[0].year, daily.index[-1].year + 1):
        start = pd.Timestamp(year=year, month=10, day=1)
        first, last = start - pd.Timedelta(days=SPIN_UP_DAYS), start + pd.Timedelta(days=YEAR_DAYS - 1)
        if first < daily.index[0] or last > daily.index[-1]:
            continue
        if daily.q_mm[start:last].notna().sum() >= MIN_FLOW_DAYS:
            starts.append(start)
    return starts


def weather_channels(block: pd.DataFrame) -> np.ndarray:
    """
    Scales a block's weather with fixed constants (the same for every basin) and adds the season.

    :param block: Daily rows with ``p``, ``pet`` (mm per day) and ``t`` (deg C).
    :type block: pd.DataFrame
    :return: Array of shape (n_days, 5): log rain, temperature, evaporation demand, and the
        sine and cosine of the day of year. Missing weather becomes zero.
    :rtype: np.ndarray
    """
    angle = 2.0 * np.pi * block.index.dayofyear.to_numpy() / 365.25
    channels = np.stack([np.log1p(block.p.clip(lower=0).to_numpy()), block.t.to_numpy() / 10.0,
                         block.pet.to_numpy() / 5.0, np.sin(angle), np.cos(angle)], axis=1)
    return np.nan_to_num(channels).astype(np.float32)


def flow_channel(block: pd.DataFrame) -> np.ndarray:
    """Log flow in mm per day (NaN where the gauge has no value)."""
    return np.log(block.q_mm.to_numpy() + FLOW_FLOOR_MM).astype(np.float32)


def load_static_banks(arms_dir: str, groups: Sequence[str], train_sites: List[str],
                      extra: Optional[Dict[str, pd.DataFrame]] = None) -> pd.DataFrame:
    """
    Loads the static description the statics reader sees, from the banks the baselines use.

    :param arms_dir: Directory of the context banks.
    :type arms_dir: str
    :param groups: Bank names (for example ``statics`` and ``location``); the banks are already
        standardized on the training basins.
    :type groups: Sequence[str]
    :param train_sites: The training basins.
    :type train_sites: List[str]
    :param extra: Bank name -> rows of further basins on the same scale (a pool's ``banks``),
        defaults to None. A basin missing from any of the groups is left out.
    :type extra: Dict[str, pd.DataFrame], optional
    :return: One row per basin; columns that do not vary over the training basins are dropped.
    :rtype: pd.DataFrame
    """
    blocks = []
    for group in groups:
        block = tabular.basin_block(group, arms_dir, train_sites)
        if extra is not None:
            if group not in extra:
                raise ValueError("the pool has no %s description" % group)
            block = pd.concat([block, extra[group].loc[~extra[group].index.isin(block.index)]])
        blocks.append(block)
    frame = pd.concat(blocks, axis=1, join="inner")
    frame.columns = range(frame.shape[1])
    varying = frame.loc[frame.index.intersection(train_sites)].std() > 0
    return frame.loc[:, varying.to_numpy()].astype(np.float32)


def load_images(records_root: str, states: Dict[str, str], fit_sites: List[str]
                ) -> Dict[str, np.ndarray]:
    """
    Loads every basin's reach image and standardizes each band on the fitting basins.

    :param records_root: Root of the embedding records: ``<root>/<state>/<site>.npz`` holding
        ``image`` of shape (bands, height, width).
    :type records_root: str
    :param states: Site id -> state folder.
    :type states: Dict[str, str]
    :param fit_sites: Basins the band means and standard deviations are fitted on.
    :type fit_sites: List[str]
    :return: Site id -> float32 image; a basin without a record is left out.
    :rtype: Dict[str, np.ndarray]
    """
    images = {}
    for site, state in states.items():
        path = os.path.join(records_root, state, site + ".npz")
        if os.path.exists(path):
            with np.load(path) as record:
                images[site] = np.clip(record["image"] / IMAGE_SCALE, 0.0, 2.0).astype(np.float32)
    fitted = np.stack([images[site] for site in fit_sites if site in images])
    mean = fitted.mean(axis=(0, 2, 3)).reshape(-1, 1, 1)
    std = fitted.std(axis=(0, 2, 3)).reshape(-1, 1, 1)
    return {site: ((image - mean) / np.where(std > 0, std, 1.0)).astype(np.float32)
            for site, image in images.items()}


def reader_subsets(readers: Sequence[str]) -> Dict[str, Tuple[str, ...]]:
    """
    Lists the banks a run writes: bank suffix -> the readers its codes are written from.

    :param readers: The readers of the run, ``history`` first.
    :type readers: Sequence[str]
    :return: ``""`` for all readers; with several readers also ``gauge`` (the gauged record
        alone), ``ungauged`` (every other reader) and one entry per other reader on its own.
    :rtype: Dict[str, Tuple[str, ...]]
    """
    subsets = {"": tuple(readers)}
    others = tuple(reader for reader in readers if reader != "history")
    if others:
        subsets.update({"gauge": ("history",), "ungauged": others})
        if len(others) > 1:
            subsets.update({reader: (reader,) for reader in others})
    return subsets


class WaterYearDataset(Dataset):
    """
    Serves, per basin, a few water years for the reader and another one for the decoder.

    ``history`` has shape (n_read, 365, 7): log flow (zero where missing), a flag for a valid
    flow value and the five weather channels. ``drivers`` covers the spin-up and the decoded
    year, shape (455, 5); ``target`` is the decoded year's log flow with NaN over the spin-up.
    ``statics`` and ``image`` are added when the basin's static description and reach image are
    given.
    """

    def __init__(self, daily: Dict[str, pd.DataFrame], sites: List[str], n_read: int = 3,
                 draws: int = 1, seed: int = 0, fixed: bool = False,
                 aux: Optional[pd.DataFrame] = None, statics: Optional[pd.DataFrame] = None,
                 images: Optional[Dict[str, np.ndarray]] = None, augment: bool = False):
        """
        Initializes the dataset.

        :param daily: Site id -> daily frame (see :func:`daily_record`).
        :type daily: Dict[str, pd.DataFrame]
        :param sites: The basins to serve; those with fewer than two usable years are dropped.
        :type sites: List[str]
        :param n_read: Water years the reader sees, defaults to 3 (repeated when a basin has
            fewer besides the decoded one).
        :type n_read: int, optional
        :param draws: Items per basin and pass, defaults to 1.
        :type draws: int, optional
        :param seed: Seed of the year sampling, defaults to 0.
        :type seed: int, optional
        :param fixed: Serve the same years on every pass (for validation), defaults to False.
        :type fixed: bool, optional
        :param aux: Standardized auxiliary targets indexed by site id, defaults to None.
        :type aux: pd.DataFrame, optional
        :param statics: Static description indexed by site id, defaults to None.
        :type statics: pd.DataFrame, optional
        :param images: Site id -> image of shape (bands, height, width), defaults to None.
        :type images: Dict[str, np.ndarray], optional
        :param augment: Turn and mirror the image at random (ignored when ``fixed``), defaults
            to False.
        :type augment: bool, optional
        """
        self.years = {site: water_years(daily[site]) for site in sites if site in daily}
        self.site_ids = [site for site in sites if len(self.years.get(site, [])) >= 2]
        self.daily, self.n_read, self.draws, self.fixed = daily, n_read, draws, fixed
        self.seed, self.aux = seed, aux
        self.statics, self.images, self.augment = statics, images, augment and not fixed
        self.rng = np.random.default_rng(seed)

    def __len__(self) -> int:
        return len(self.site_ids) * self.draws

    def choose(self, index: int) -> Tuple[str, pd.Timestamp, List[pd.Timestamp]]:
        """Picks the basin, its decoded year and the years the reader sees."""
        site = self.site_ids[index % len(self.site_ids)]
        years = self.years[site]
        rng = np.random.default_rng([self.seed, index]) if self.fixed else self.rng
        decoded = years[int(rng.integers(len(years)))]
        others = [year for year in years if year != decoded]
        picked = rng.choice(len(others), size=self.n_read, replace=len(others) < self.n_read)
        return site, decoded, [others[int(position)] for position in sorted(picked)]

    def member(self, site: str, start: pd.Timestamp) -> np.ndarray:
        """One water year as the reader sees it."""
        block = self.daily[site][start:start + pd.Timedelta(days=YEAR_DAYS - 1)]
        flow = flow_channel(block)
        valid = np.isfinite(flow)
        return np.column_stack([np.where(valid, flow, 0.0), valid.astype(np.float32),
                                weather_channels(block)]).astype(np.float32)

    def lasting(self, site: str, augment: bool = False) -> Dict[str, torch.Tensor]:
        """The basin's inputs that do not change with time: static description and image."""
        item = {}
        if self.statics is not None:
            item["statics"] = torch.tensor(self.statics.loc[site].to_numpy(dtype=np.float32))
        if self.images is not None:
            image = self.images[site]
            if augment:
                # North is not special for a reach: turn and mirror the image at random.
                image = np.rot90(image, int(self.rng.integers(4)), axes=(1, 2))
                if self.rng.integers(2):
                    image = image[:, :, ::-1]
            item["image"] = torch.from_numpy(np.ascontiguousarray(image))
        return item

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        site, decoded, read = self.choose(index)
        block = self.daily[site][decoded - pd.Timedelta(days=SPIN_UP_DAYS):
                                 decoded + pd.Timedelta(days=YEAR_DAYS - 1)]
        target = flow_channel(block)
        target[:SPIN_UP_DAYS] = np.nan
        item = {"history": torch.from_numpy(np.stack([self.member(site, year) for year in read])),
                "drivers": torch.from_numpy(weather_channels(block)),
                "target": torch.from_numpy(target)}
        if self.aux is not None:
            item["aux"] = torch.tensor(self.aux.loc[site].to_numpy(dtype=np.float32))
        item.update(self.lasting(site, self.augment))
        return item


class ReadYearsDataset(Dataset):
    """
    Serves ``n_read`` water years of each basin, for writing its code.

    ``group`` selects which years: group g starts ``g * n_read`` years into the basin's list and
    wraps around, so successive groups cover the whole record. With ``read_history`` off (codes
    written without the gauged record) every basin is served and its record is left blank.
    """

    def __init__(self, daily: Dict[str, pd.DataFrame], sites: List[str], n_read: int = 3,
                 statics: Optional[pd.DataFrame] = None,
                 images: Optional[Dict[str, np.ndarray]] = None, read_history: bool = True):
        self.source = WaterYearDataset(daily, [], n_read=n_read, statics=statics, images=images)
        self.years = {site: water_years(daily[site]) for site in sites if site in daily}
        self.site_ids = [site for site in sites if self.years.get(site) or not read_history]
        self.n_read, self.group, self.read_history = n_read, 0, read_history

    def __len__(self) -> int:
        return len(self.site_ids)

    def read_years(self, site: str) -> List[pd.Timestamp]:
        """The water years of the current group."""
        years = self.years[site]
        return [years[(self.group * self.n_read + step) % len(years)]
                for step in range(self.n_read)]

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        site = self.site_ids[index]
        if self.read_history:
            history = np.stack([self.source.member(site, year) for year in self.read_years(site)])
        else:
            history = np.zeros((self.n_read, YEAR_DAYS, HISTORY_CHANNELS), dtype=np.float32)
        return {"history": torch.from_numpy(history), **self.source.lasting(site)}


def write_codes(encoder, daily: Dict[str, pd.DataFrame], sites: List[str], n_read: int,
                groups: int, batch_size: int = 64, device: str = "cpu",
                statics: Optional[pd.DataFrame] = None,
                images: Optional[Dict[str, np.ndarray]] = None,
                readers: Optional[Sequence[str]] = None) -> pd.DataFrame:
    """
    Writes every basin's code as the mean over groups of its water years.

    :param encoder: The pretrained encoder.
    :type encoder: flood_forecast.meta_models.multimodal_encoder.MultiModalEncoder
    :param daily: Site id -> daily frame.
    :type daily: Dict[str, pd.DataFrame]
    :param sites: Basins to encode. When the gauged record is read, those without a usable
        water year are left out.
    :type sites: List[str]
    :param n_read: Water years per group.
    :type n_read: int
    :param groups: Number of groups averaged.
    :type groups: int
    :param statics: Static description indexed by site id, defaults to None.
    :type statics: pd.DataFrame, optional
    :param images: Site id -> image, defaults to None.
    :type images: Dict[str, np.ndarray], optional
    :param readers: Write the code from these readers only, defaults to None (all of them).
    :type readers: Sequence[str], optional
    :return: Codes indexed by site id.
    :rtype: pd.DataFrame
    """
    from flood_forecast.meta_models.contrastive_train import extract_embeddings
    read_history = "history" in encoder.encoders and (readers is None or "history" in readers)
    reader = ReadYearsDataset(daily, sites, n_read=n_read, statics=statics, images=images,
                              read_history=read_history)
    options = {} if readers is None else {"modalities": list(readers)}
    passes, total = groups if read_history else 1, None
    for group in range(passes):
        reader.group = group
        _, codes = extract_embeddings(encoder, reader, batch_size=batch_size, device=device,
                                      **options)
        total = codes if total is None else total + codes
    return pd.DataFrame((total / passes).numpy(), index=reader.site_ids)


def validation_losses(encoder, decoder, dataset: Dataset, subsets: Dict[str, Tuple[str, ...]],
                      batch_size: int = 64, device: str = "cpu") -> Dict[str, float]:
    """
    Scores the decoder on held-out basins with the code written from each subset of readers.

    :param encoder: The pretrained encoder.
    :type encoder: flood_forecast.meta_models.multimodal_encoder.MultiModalEncoder
    :param decoder: The pretrained decoder.
    :type decoder: torch.nn.Module
    :param dataset: A fixed :class:`WaterYearDataset` of basins not trained on.
    :type dataset: Dataset
    :param subsets: Label -> readers (see :func:`reader_subsets`).
    :type subsets: Dict[str, Tuple[str, ...]]
    :return: Label -> mean squared error of the decoded log flow (``all`` for the empty
        label), plus ``mean code``: every basin given the batch mean of its code from all
        readers, the level of a code that says nothing about the basin.
    :rtype: Dict[str, float]
    """
    from flood_forecast.meta_models.behaviour_pretrain import behaviour_step, masked_mse
    encoder.to(device).eval(), decoder.to(device).eval()
    loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=False)
    losses = {}
    with torch.no_grad():
        for label, members in list(subsets.items()) + [("mean code", None)]:
            batches = []
            for batch in loader:
                drivers, target = batch["drivers"].to(device), batch["target"].to(device)
                step = behaviour_step(encoder, decoder,
                                      {name: batch[name].to(device) for name in encoder.encoders},
                                      drivers, target, modalities=members)
                if label == "mean code":
                    code = step["code"].mean(dim=0, keepdim=True).expand_as(step["code"])
                    batches.append(masked_mse(decoder(drivers, code), target).item())
                else:
                    batches.append(step["series"].item())
            losses[label or "all"] = float(np.mean(batches))
    return losses


def standardized_aux(early: pd.DataFrame, train_sites: List[str]) -> pd.DataFrame:
    """
    Standardizes the pre-cutoff behaviour numbers with the training basins' statistics.

    :param early: ``benchmark.pkl``'s ``early`` frame (site id by behaviour number).
    :type early: pd.DataFrame
    :param train_sites: Basins the statistics are fitted on.
    :type train_sites: List[str]
    :return: The ``AUX`` columns, standardized; missing values stay NaN.
    :rtype: pd.DataFrame
    """
    frame = early[AUX].astype(float)
    fitted = frame.loc[frame.index.intersection(train_sites)]
    return ((frame - fitted.mean()) / fitted.std().replace(0.0, 1.0)).clip(-5.0, 5.0)


def build_models(code_dim: int, dim: int, depth: int, hidden: int, dropout: float,
                 n_statics: int = 0, image_shape: Optional[Tuple[int, int, int]] = None,
                 patch_size: int = 16, gauged: bool = True):
    """
    Builds the readers and the decoder from Flow Forecast parts.

    :param n_statics: Width of the static description (0 leaves the statics reader out).
    :type n_statics: int
    :param image_shape: (bands, height, width) of the reach image (None leaves the reader out).
    :type image_shape: Tuple[int, int, int], optional
    :param patch_size: Side of the image patches in pixels.
    :type patch_size: int
    :param gauged: Include the reader of the gauged record, defaults to True.
    :type gauged: bool, optional
    :return: The encoder (readers ``history``, ``statics``, ``image``) and the decoder.
    :rtype: tuple
    """
    from flood_forecast.meta_models.behaviour_pretrain import DriverConditionedDecoder
    from flood_forecast.meta_models.multimodal_encoder import (ImagePatchEncoder,
                                                               MultiModalEncoder,
                                                               PanelSequenceEncoder,
                                                               TabularEncoder)
    transformer = {"dim": dim, "depth": depth, "heads": 4, "dim_head": dim // 4,
                   "mlp_dim": 2 * dim, "dropout": dropout}
    readers = {}
    if gauged:
        readers["history"] = PanelSequenceEncoder(HISTORY_CHANNELS, YEAR_DAYS, token_kernel=14,
                                                  token_stride=7, **transformer)
    if n_statics:
        readers["statics"] = TabularEncoder(n_statics, dim=dim, hidden_dim=dim, dropout=dropout)
    if image_shape:
        readers["image"] = ImagePatchEncoder(tuple(image_shape[1:]), patch_size, image_shape[0],
                                             **transformer)
    encoder = MultiModalEncoder(readers, dim=dim, embedding_dim=code_dim, fusion="concat",
                                fusion_head="linear",
                                sequence_modalities=[name for name in ("history", "image")
                                                     if name in readers])
    return encoder, DriverConditionedDecoder(5, code_dim, hidden_dim=hidden)


def save_bank(arms_dir: str, name: str, codes: pd.DataFrame, sites: List[str],
              train_sites: List[str], scale_of: Optional[pd.DataFrame] = None) -> None:
    """
    Standardizes codes on the training basins and writes them as ``context_<name>.pt``.

    A basin without a code gets the training mean, so every bank covers the same basins and
    comparisons stay on matched cohorts. ``scale_of`` gives the codes whose training-basin
    mean and spread are used instead of the codes' own (to keep two banks on one scale).
    """
    fitted = codes if scale_of is None else scale_of
    fitted = fitted.loc[fitted.index.intersection(train_sites)]
    scaled = ((codes - fitted.mean()) / fitted.std().replace(0.0, 1.0)).clip(-8.0, 8.0)
    scaled = scaled.reindex(sites).fillna(0.0)
    torch.save({"site_ids": list(scaled.index),
                "embeddings": torch.tensor(scaled.to_numpy(), dtype=torch.float32)},
               os.path.join(arms_dir, "context_%s.pt" % name))


def train(arms_dir: str, name: str, seed: int = 0, epochs: int = 60, code_dim: int = 32,
          dim: int = 64, depth: int = 2, hidden: int = 64, dropout: float = 0.1,
          n_read: int = 3, draws: int = 8, batch_size: int = 64, lr: float = 1e-3,
          weight_decay: float = 1e-4, aux_weight: float = 0.0, validation_fraction: float = 0.15,
          read_passes: int = 6, device: str = "cpu", static_banks: Sequence[str] = (),
          image_records: Optional[str] = None, modality_dropout: float = 0.3,
          patch_size: int = 16, with_pool: bool = False, cache: Optional[Dict] = None,
          early: Optional[pd.DataFrame] = None, statics: Optional[pd.DataFrame] = None,
          images: Optional[Dict[str, np.ndarray]] = None, pool: Optional[Dict] = None,
          verbose: bool = True) -> Dict:
    """
    Pretrains the code on the training basins and writes its banks for all basins.

    With a pool (:func:`build_pool`), its basins are added to the fitting basins and to nothing
    else: the validation basins, the basins that get a code and the scale of the banks are the
    cohort's, so a run with a pool is scored exactly like a run without one.

    :param arms_dir: Directory holding the daily cache and ``benchmark.pkl``; outputs go here.
    :type arms_dir: str
    :param name: Bank name.
    :type name: str
    :param seed: Seed of the weights, the year sampling and the validation basins.
    :type seed: int
    :param epochs: Passes over the training basins (``draws`` items per basin each).
    :type epochs: int
    :param code_dim: Width of the code.
    :type code_dim: int
    :param aux_weight: Weight of the behaviour-number side targets (0 switches them off).
    :type aux_weight: float
    :param validation_fraction: Share of training basins held out to pick the restored epoch.
    :type validation_fraction: float
    :param read_passes: Groups of water years averaged into a basin's code.
    :type read_passes: int
    :param static_banks: Banks in ``arms_dir`` read by the statics reader (none leaves it out).
    :type static_banks: Sequence[str]
    :param image_records: Root of the embedding records read by the image reader (None leaves
        it out).
    :type image_records: str, optional
    :param modality_dropout: Chance that a reader is switched off for a training item (at least
        one reader always stays on); only used with several readers.
    :type modality_dropout: float
    :param with_pool: Read the pool stored in ``arms_dir``, defaults to False.
    :type with_pool: bool, optional
    :param cache: The daily cache, defaults to None (read from ``arms_dir``).
    :type cache: Dict, optional
    :param early: Pre-cutoff behaviour numbers, defaults to None (read from ``benchmark.pkl``).
    :type early: pd.DataFrame, optional
    :param statics: Static description indexed by site id, defaults to None (``static_banks``).
    :type statics: pd.DataFrame, optional
    :param images: Site id -> standardized image, defaults to None (``image_records``).
    :type images: Dict[str, np.ndarray], optional
    :param pool: Extra fitting basins, defaults to None (``with_pool``).
    :type pool: Dict, optional
    :return: The run summary, also written to ``reports/pretrain_<name>.json``.
    :rtype: Dict
    """
    from flood_forecast.meta_models.behaviour_pretrain import pretrain_behaviour
    cache = cache or pd.read_pickle(os.path.join(arms_dir, CACHE))
    daily, split = cache["daily"], cache["split"]
    sites = list(split)
    train_sites, fit_sites, validation_sites = split_basins(split, seed, validation_fraction)
    torch.manual_seed(seed)
    if pool is None and with_pool:
        pool = pd.read_pickle(os.path.join(arms_dir, POOL))

    if statics is None and static_banks:
        statics = load_static_banks(arms_dir, static_banks, train_sites,
                                    extra=pool["banks"] if pool else None)
    extras = pool_members(pool, split, statics)
    if pool is not None:
        daily = {**pool["daily"], **daily}
    aux, aux_head = None, None
    if aux_weight > 0:
        if early is None:
            early = pd.read_pickle(os.path.join(arms_dir, "benchmark.pkl"))["early"]
        if pool is not None:
            early = pd.concat([early, pool["early"].loc[~pool["early"].index.isin(early.index)]])
        aux = standardized_aux(early, fit_sites + extras).reindex(sites + extras)
        aux_head = torch.nn.Linear(code_dim, len(AUX))
    if statics is not None:
        statics = statics.reindex(sites + extras).fillna(0.0).astype(np.float32)
    if images is None and image_records:
        with open(cache["manifest"]) as file:
            states = {str(basin["site_id"]): str(basin.get("state", ""))
                      for basin in json.load(file)["basins"]}
        images = load_images(image_records, states, fit_sites)
    if images is not None:
        blank = np.zeros_like(next(iter(images.values())))       # the band means
        images = {site: images.get(site, blank) for site in sites + extras}
    lasting = {"statics": statics, "images": images}
    readers = ["history"] + [reader for reader, given in (("statics", statics), ("image", images))
                             if given is not None]
    subsets = reader_subsets(readers)

    fit = WaterYearDataset(daily, fit_sites + extras, n_read=n_read, draws=draws, seed=seed,
                           aux=aux, augment=True, **lasting)
    validation = None
    if validation_sites:
        validation = WaterYearDataset(daily, validation_sites, n_read=n_read, draws=4, seed=seed,
                                      fixed=True, aux=aux, **lasting)
    encoder, decoder = build_models(
        code_dim, dim, depth, hidden, dropout, n_statics=0 if statics is None else statics.shape[1],
        image_shape=None if images is None else blank.shape, patch_size=patch_size)
    several = {}
    if len(readers) > 1:
        # The restored epoch is chosen on gauged and ungauged basins alike.
        several = {"modality_dropout": modality_dropout,
                   "validation_subsets": [list(subsets[""]), list(subsets["ungauged"])]}
    started = time.time()
    history = pretrain_behaviour(encoder, decoder, fit, epochs=epochs, batch_size=batch_size,
                                 lr=lr, weight_decay=weight_decay, device=device,
                                 validation_dataset=validation, aux_head=aux_head,
                                 aux_weight=aux_weight, verbose=verbose, **several)
    minutes = (time.time() - started) / 60.0

    banks, basins_with_code = [], 0
    for suffix, members in subsets.items():
        codes = write_codes(encoder, daily, sites, n_read, read_passes, batch_size, device,
                            readers=members if len(readers) > 1 else None, **lasting)
        basins_with_code = basins_with_code if suffix else len(codes)
        banks.append(name + ("_" + suffix if suffix else ""))
        save_bank(arms_dir, banks[-1], codes, sites, train_sites)
    torch.save({"encoder": encoder.state_dict(), "decoder": decoder.state_dict()},
               os.path.join(arms_dir, "behaviour_%s.pt" % name))
    by_subset = None
    if validation is not None and len(readers) > 1:
        by_subset = validation_losses(encoder, decoder, validation, subsets, batch_size, device)
    summary = {"name": name, "seed": seed, "epochs": epochs, "code_dim": code_dim, "dim": dim,
               "depth": depth, "hidden": hidden, "dropout": dropout, "n_read": n_read,
               "draws": draws, "batch_size": batch_size, "lr": lr, "weight_decay": weight_decay,
               "aux_weight": aux_weight, "readers": readers, "banks": banks,
               "static_banks": list(static_banks), "image_records": image_records,
               "static_width": 0 if statics is None else int(statics.shape[1]),
               "modality_dropout": modality_dropout if len(readers) > 1 else 0.0,
               "fit_basins": len(fit.site_ids),
               "pool_basins": len(set(fit.site_ids) & set(extras)),
               "validation_basins": len(validation.site_ids) if validation else 0,
               "validation_fraction": validation_fraction,
               "basins_with_code": basins_with_code, "minutes": round(minutes, 2),
               "best_epoch": history.get("best_epoch", [epochs])[0],
               "validation_by_readers": by_subset,
               "series_loss": history["series"], "validation_loss": history.get("validation"),
               "validation_curves": {key.partition(":")[2]: values
                                     for key, values in history.items()
                                     if key.startswith("validation:")}}
    os.makedirs(os.path.join(arms_dir, "reports"), exist_ok=True)
    with open(os.path.join(arms_dir, "reports", "pretrain_%s.json" % name), "w") as file:
        json.dump(summary, file, indent=2)
    return summary


def split_basins(split: Dict[str, str], seed: int, validation_fraction: float
                 ) -> Tuple[List[str], List[str], List[str]]:
    """Returns the training basins and their fitting and validation parts for a seed."""
    train_sites = sorted(site for site in split if split[site] == "train")
    order = np.random.default_rng(seed).permutation(len(train_sites))
    n_validation = int(round(validation_fraction * len(train_sites)))
    validation_sites = sorted(train_sites[index] for index in order[:n_validation])
    fit_sites = sorted(train_sites[index] for index in order[n_validation:])
    return train_sites, fit_sites, validation_sites


def align(arms_dir: str, name: str, teacher: str, epochs: int = 60, dim: int = 64,
          depth: int = 2, dropout: float = 0.1, draws: int = 8, batch_size: int = 64,
          lr: float = 1e-3, weight_decay: float = 1e-4, code_weight: float = 1.0,
          validation_fraction: float = 0.15, read_passes: int = 6, device: str = "cpu",
          static_banks: Sequence[str] = ("statics", "location"),
          image_records: Optional[str] = None, modality_dropout: float = 0.3,
          patch_size: int = 16, with_pool: bool = False, cache: Optional[Dict] = None,
          statics: Optional[pd.DataFrame] = None,
          images: Optional[Dict[str, np.ndarray]] = None, pool: Optional[Dict] = None,
          verbose: bool = True) -> Dict:
    """
    Fits the readers of an ungauged basin to the code of a finished gauge-only run.

    The teacher's gauge reader and decoder are loaded and left unchanged. A second encoder
    (statics reader, and image reader when images are given) is trained on the teacher's
    fitting basins with two terms: the frozen decoder must reproduce a water year from the
    new code, and the new code must match the basin's gauge code. Two banks are written, both
    on the scale of the teacher's bank: ``context_<name>.pt`` holds every basin's code from
    the new readers; ``context_<name>_transfer.pt`` holds the gauge code for training basins
    and the new code for held-out basins, which is what a model fitted on gauged basins sees
    when it is applied to ungauged ones.

    With a pool (:func:`build_pool`), its basins that have a static description are added to
    the fitting basins, each with the gauge code the teacher reads from its record; validation,
    the banks and their scale stay the cohort's.

    :param arms_dir: Directory holding the daily cache, the banks and the teacher's files.
    :type arms_dir: str
    :param name: Name of the new banks.
    :type name: str
    :param teacher: Name of a finished gauge-only ``train`` run; its seed and split are reused.
    :type teacher: str
    :param code_weight: Weight of the code-matching term (in units of the teacher bank's
        spread per number).
    :type code_weight: float
    :param validation_fraction: Must equal the teacher's.
    :type validation_fraction: float
    :param static_banks: Banks read by the statics reader.
    :type static_banks: Sequence[str]
    :param image_records: Root of the embedding records for an image reader, defaults to None.
    :type image_records: str, optional
    :param with_pool: Read the pool stored in ``arms_dir``, defaults to False.
    :type with_pool: bool, optional
    :param pool: Extra fitting basins, defaults to None (``with_pool``).
    :type pool: Dict, optional
    :return: The run summary, also written to ``reports/pretrain_<name>.json``.
    :rtype: Dict
    """
    from flood_forecast.meta_models.behaviour_pretrain import pretrain_behaviour
    with open(os.path.join(arms_dir, "reports", "pretrain_%s.json" % teacher)) as file:
        taught = json.load(file)
    if taught.get("readers", ["history"]) != ["history"]:
        raise ValueError("the teacher must be a gauge-only run but reads %s" % taught["readers"])
    cache = cache or pd.read_pickle(os.path.join(arms_dir, CACHE))
    daily, split = cache["daily"], cache["split"]
    sites, seed, code_dim = list(split), taught["seed"], taught["code_dim"]
    train_sites, fit_sites, validation_sites = split_basins(split, seed, validation_fraction)
    torch.manual_seed(seed)
    if pool is None and with_pool:
        pool = pd.read_pickle(os.path.join(arms_dir, POOL))
    if statics is None and static_banks:
        statics = load_static_banks(arms_dir, static_banks, train_sites,
                                    extra=pool["banks"] if pool else None)
    # Only a pool basin with a static description can teach the statics reader.
    extras = pool_members(pool, split, statics) if statics is not None else []
    if pool is not None:
        daily = {**pool["daily"], **daily}

    gauge_reader, decoder = build_models(code_dim, taught["dim"], taught["depth"],
                                         taught["hidden"], taught["dropout"])
    state = torch.load(os.path.join(arms_dir, "behaviour_%s.pt" % teacher), weights_only=True)
    gauge_reader.load_state_dict(state["encoder"]), decoder.load_state_dict(state["decoder"])
    for parameter in decoder.parameters():
        parameter.requires_grad_(False)
    gauge_codes = write_codes(gauge_reader, daily, sites + extras, taught["n_read"], read_passes,
                              batch_size, device)
    fitted = gauge_codes.loc[gauge_codes.index.intersection(train_sites)]
    mean, spread = fitted.mean(), fitted.std().replace(0.0, 1.0)
    # The new code is compared with the gauge code on the scale of the teacher's bank.
    to_bank_scale = torch.nn.Linear(code_dim, code_dim)
    with torch.no_grad():
        to_bank_scale.weight.copy_(torch.diag(torch.tensor((1.0 / spread).to_numpy(),
                                                           dtype=torch.float32)))
        to_bank_scale.bias.copy_(torch.tensor((-mean / spread).to_numpy(), dtype=torch.float32))
    for parameter in to_bank_scale.parameters():
        parameter.requires_grad_(False)
    targets = ((gauge_codes - mean) / spread).reindex(sites + extras)

    if statics is not None:
        statics = statics.reindex(sites + extras).fillna(0.0).astype(np.float32)
    if images is None and image_records:
        with open(cache["manifest"]) as file:
            states = {str(basin["site_id"]): str(basin.get("state", ""))
                      for basin in json.load(file)["basins"]}
        images = load_images(image_records, states, fit_sites)
    if images is not None:
        blank = np.zeros_like(next(iter(images.values())))
        images = {site: images.get(site, blank) for site in sites + extras}
    lasting = {"statics": statics, "images": images}
    student, _ = build_models(code_dim, dim, depth, taught["hidden"], dropout,
                              n_statics=0 if statics is None else statics.shape[1],
                              image_shape=None if images is None else blank.shape,
                              patch_size=patch_size, gauged=False)
    if not student.encoders:
        raise ValueError("give static banks or image records for the new readers")
    fit = WaterYearDataset(daily, fit_sites + extras, n_read=1, draws=draws, seed=seed,
                           aux=targets, augment=True, **lasting)
    validation = WaterYearDataset(daily, validation_sites, n_read=1, draws=4, seed=seed,
                                  fixed=True, aux=targets, **lasting)
    started = time.time()
    history = pretrain_behaviour(
        student, decoder, fit, epochs=epochs, batch_size=batch_size, lr=lr,
        weight_decay=weight_decay, device=device, validation_dataset=validation,
        aux_head=to_bank_scale, aux_weight=code_weight,
        modality_dropout=modality_dropout if len(student.encoders) > 1 else 0.0, verbose=verbose)
    minutes = (time.time() - started) / 60.0
    unchanged = all(torch.equal(value, state["decoder"][key])
                    for key, value in decoder.state_dict().items())

    codes = write_codes(student, daily, sites, 1, 1, batch_size, device, **lasting)
    save_bank(arms_dir, name, codes, sites, train_sites, scale_of=gauge_codes)
    transfer = gauge_codes.reindex(sites)
    held_out = [site for site in sites if split[site] != "train"]
    transfer.loc[held_out] = codes.loc[held_out]
    save_bank(arms_dir, name + "_transfer", transfer, sites, train_sites, scale_of=gauge_codes)
    torch.save({"encoder": student.state_dict(), "decoder": decoder.state_dict()},
               os.path.join(arms_dir, "behaviour_%s.pt" % name))
    gauged_check = WaterYearDataset(daily, validation_sites, n_read=taught["n_read"], draws=4,
                                    seed=seed, fixed=True)
    gauged = validation_losses(gauge_reader, decoder, gauged_check, {"": ("history",)},
                               batch_size, device)
    ungauged = validation_losses(student, decoder, validation, {"": tuple(student.encoders)},
                                 batch_size, device)
    summary = {"name": name, "teacher": teacher, "seed": seed, "epochs": epochs,
               "code_dim": code_dim, "dim": dim, "depth": depth, "dropout": dropout,
               "draws": draws, "batch_size": batch_size, "lr": lr, "weight_decay": weight_decay,
               "code_weight": code_weight, "readers": list(student.encoders),
               "banks": [name, name + "_transfer"], "static_banks": list(static_banks),
               "image_records": image_records, "fit_basins": len(fit.site_ids),
               "pool_basins": len(set(fit.site_ids) & set(extras)),
               "validation_basins": len(validation.site_ids), "minutes": round(minutes, 2),
               "best_epoch": history["best_epoch"][0], "decoder_unchanged": unchanged,
               "validation": {"gauge code": gauged["all"], "new code": ungauged["all"],
                              "mean code": gauged["mean code"]},
               "series_loss": history["series"], "code_loss": history["aux"],
               "validation_loss": history["validation"]}
    with open(os.path.join(arms_dir, "reports", "pretrain_%s.json" % name), "w") as file:
        json.dump(summary, file, indent=2)
    return summary


def shuffle_bank(arms_dir: str, name: str, seed: int = 0,
                 split: Optional[Dict[str, str]] = None) -> str:
    """
    Writes ``context_<name>_shuffled.pt``: the bank's codes handed to other basins of the same
    split (the control that tells a real description from extra columns).

    :param arms_dir: Directory of the context banks.
    :type arms_dir: str
    :param name: The bank to shuffle.
    :type name: str
    :param seed: Permutation seed.
    :type seed: int
    :param split: Site id -> split, defaults to None (read from ``manifest_none.json``).
    :type split: Dict[str, str], optional
    :return: The name of the shuffled bank.
    :rtype: str
    """
    from experiments.catchment_foundation.prepare_context_ablation import shuffle_within_groups
    if split is None:
        with open(os.path.join(arms_dir, "manifest_none.json")) as file:
            split = {str(basin["site_id"]): basin["split"] for basin in json.load(file)["basins"]}
    bank = torch.load(os.path.join(arms_dir, "context_%s.pt" % name), weights_only=True)
    source = shuffle_within_groups([split[str(site)] for site in bank["site_ids"]], seed)
    torch.save({"site_ids": bank["site_ids"], "embeddings": bank["embeddings"][source]},
               os.path.join(arms_dir, "context_%s_shuffled.pt" % name))
    return name + "_shuffled"


def main() -> None:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    cacher = commands.add_parser("cache", help="store every basin's pre-cutoff daily record")
    cacher.add_argument("--arms-dir", required=True)
    cacher.add_argument("--manifest", default=None, help="defaults to <arms dir>/manifest_none.json")
    cacher.add_argument("--workers", type=int, default=6)
    pooler = commands.add_parser("pool", help="store extra basins a code may be pretrained on")
    pooler.add_argument("--arms-dir", required=True)
    pooler.add_argument("--manifest", required=True, help="manifest holding the extra basins")
    pooler.add_argument("--split", required=True, help="basin split that knows those basins")
    pooler.add_argument("--exclude-folds", type=int, nargs="+", default=[0],
                        help="folds whose HUC6 groups stay out of the pool")
    pooler.add_argument("--gages2-zip", default=None,
                        help="GAGES-II archive, to describe the extra basins for a statics reader")
    pooler.add_argument("--gauges-root", default=None, help="scrape root with <state>/gauges.csv")
    pooler.add_argument("--max-negative-flow-fraction", type=float, default=None,
                        help="leave out tidal and backwater gauges, as the cohort does")
    pooler.add_argument("--workers", type=int, default=6)
    trainer = commands.add_parser("train", help="pretrain the code and write its bank")
    trainer.add_argument("--arms-dir", required=True)
    trainer.add_argument("--name", required=True)
    trainer.add_argument("--seed", type=int, default=0)
    trainer.add_argument("--epochs", type=int, default=60)
    trainer.add_argument("--code-dim", type=int, default=32)
    trainer.add_argument("--dim", type=int, default=64)
    trainer.add_argument("--depth", type=int, default=2)
    trainer.add_argument("--hidden", type=int, default=64)
    trainer.add_argument("--dropout", type=float, default=0.1)
    trainer.add_argument("--n-read", type=int, default=3)
    trainer.add_argument("--draws", type=int, default=8)
    trainer.add_argument("--batch-size", type=int, default=64)
    trainer.add_argument("--lr", type=float, default=1e-3)
    trainer.add_argument("--weight-decay", type=float, default=1e-4)
    trainer.add_argument("--aux-weight", type=float, default=0.0)
    trainer.add_argument("--validation-fraction", type=float, default=0.15)
    trainer.add_argument("--read-passes", type=int, default=6)
    trainer.add_argument("--device", default="cpu")
    trainer.add_argument("--static-banks", nargs="*", default=[],
                         help="banks read by the statics reader, e.g. statics location")
    trainer.add_argument("--image-records", default=None,
                         help="embedding records root (<root>/<state>/<site>.npz) for the image "
                              "reader")
    trainer.add_argument("--modality-dropout", type=float, default=0.3)
    trainer.add_argument("--patch-size", type=int, default=16)
    trainer.add_argument("--with-pool", action="store_true",
                         help="also fit on the extra basins stored by the pool command")
    aligner = commands.add_parser("align", help="fit statics (and image) readers to the code "
                                                "of a gauge-only run")
    aligner.add_argument("--arms-dir", required=True)
    aligner.add_argument("--name", required=True)
    aligner.add_argument("--teacher", required=True, help="name of a finished gauge-only run")
    aligner.add_argument("--epochs", type=int, default=60)
    aligner.add_argument("--dim", type=int, default=64)
    aligner.add_argument("--depth", type=int, default=2)
    aligner.add_argument("--dropout", type=float, default=0.1)
    aligner.add_argument("--draws", type=int, default=8)
    aligner.add_argument("--batch-size", type=int, default=64)
    aligner.add_argument("--lr", type=float, default=1e-3)
    aligner.add_argument("--weight-decay", type=float, default=1e-4)
    aligner.add_argument("--code-weight", type=float, default=1.0)
    aligner.add_argument("--validation-fraction", type=float, default=0.15)
    aligner.add_argument("--device", default="cpu")
    aligner.add_argument("--static-banks", nargs="*", default=["statics", "location"])
    aligner.add_argument("--image-records", default=None)
    aligner.add_argument("--modality-dropout", type=float, default=0.3)
    aligner.add_argument("--patch-size", type=int, default=16)
    aligner.add_argument("--with-pool", action="store_true",
                         help="also fit on the pool's basins that have a static description")
    shuffler = commands.add_parser("shuffle", help="write a bank permuted within each split")
    shuffler.add_argument("--arms-dir", required=True)
    shuffler.add_argument("--name", required=True)
    shuffler.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()
    if args.command == "cache":
        manifest = args.manifest or os.path.join(args.arms_dir, "manifest_none.json")
        build_cache(manifest, args.arms_dir, args.workers)
        return
    if args.command == "pool":
        build_pool(args.manifest, args.split, args.arms_dir, args.exclude_folds, args.workers,
                   args.gages2_zip, args.gauges_root, args.max_negative_flow_fraction)
        return
    if args.command == "shuffle":
        print("wrote context_%s.pt" % shuffle_bank(args.arms_dir, args.name, args.seed))
        return
    options = {key: value for key, value in vars(args).items() if key != "command"}
    summary = align(**options) if args.command == "align" else train(**options)
    print(json.dumps({key: value for key, value in summary.items()
                      if key not in ("series_loss", "validation_loss", "validation_curves",
                                     "code_loss")}, indent=2))


if __name__ == "__main__":
    main()
