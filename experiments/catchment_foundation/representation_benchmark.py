"""
Held-out-basin benchmark for catchment representations, beyond forecasting.

A representation is judged by what a simple model reads from it about basins it never saw, in
years no representation read. Three task families (forecasting is scored separately by
``tabular_forecast_baseline.py``):

* extremes: flood size per unit area, flood size relative to mean flow, the flow response to the
  largest warm storms, and low flow;
* regime class: intermittent, snowmelt, flashy, steady or mixed, from the later record;
* similar basins: the nearest training basins in the representation's space stand in for a
  held-out basin, as donors of its behaviour numbers and of its daily flow.

``build`` makes one pass over the hourly files of a manifest. From the record before the cutoff
it writes a hand-built description next to the other context banks (``context_response.pt``: how
flow relates to weather; ``context_extremes.pt``: past flood and low-flow sizes;
``context_location.pt``); from the record after the cutoff it writes the targets
(``benchmark.pkl``). ``run`` scores any ``+``-joined list of banks. ``recover`` asks how much of
one description another can reproduce on held-out basins (the job of an ungauged student).
``imagery`` writes hand-built numbers from each basin's satellite patches
(``context_imagery_reach.pt``, ``context_imagery_regional.pt``): band means and spreads and the
usual vegetation, water, moisture and snow indices. They show whether the patches hold anything
a simple model can use before an image reader is trained on them.

Example::

    python experiments/catchment_foundation/representation_benchmark.py build \
        --manifest .../context_ablation_fleet5_f0/manifest_none.json \
        --arms-dir .../context_ablation_fleet5_f0 --gauges-root pilot_data/scrapes
    python experiments/catchment_foundation/representation_benchmark.py run \
        --arms-dir .../context_ablation_fleet5_f0 \
        --representations statics statics_signatures statics_signatures+response v10
"""
import argparse
import glob
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..")))

from experiments.catchment_foundation import tabular_forecast_baseline as tabular  # noqa: E402

CUTOFF = tabular.CUTOFF
MIN_DAYS = 730                       # a period needs two years of flow and weather
RESPONSE = ["log_runoff_mm", "log_runoff_ratio", "log_aridity", "snow_fraction",
            "recession_rate", "storm_response", "lag_hours", "rain_flow_coupling",
            "half_flow_day", "zero_flow_fraction"]
EXTREMES = ["flood_peak", "flood_ratio", "low_flow"]
EXTREME_TARGETS = ["flood_peak", "flood_ratio", "storm_response", "low_flow"]
BEHAVIOUR = ["log_runoff_mm", "log_runoff_ratio", "recession_rate", "storm_response",
             "melt_share", "zero_flow_fraction", "bfi", "rb_flashiness", "flood_ratio",
             "low_flow"]
SNOW_BELOW_C, WARM_ABOVE_C = 1.0, 2.0   # daily basin temperature: snow at or below, rain above
REGIMES = ["intermittent", "snowmelt", "flashy", "steady", "mixed"]
CLIMATE = ["log_rain", "log_evaporation", "log_aridity", "temperature", "temperature_swing",
           "snow_share", "rain_season_cos", "rain_season_sin", "heavy_rain_days", "dry_days",
           "rain_with_warmth"]
CONTEXT_WIDTH = 256                  # width of the context banks the forecasters read
# Sentinel-2 bands of the patches in the embedding records: the 10 m reach patch at the gauge
# and the 50 m regional patch around it.
IMAGE_SOURCES = {"reach": ("{site}.npz", "image", ("B02", "B03", "B04", "B08")),
                 "regional": ("{site}_regional.npz", "image_regional",
                              ("B02", "B03", "B04", "B08", "B11", "B12"))}
# Normalized differences (first minus second, over their sum).
IMAGE_INDICES = {"ndvi": ("B08", "B04"), "ndwi": ("B03", "B08"), "ndmi": ("B08", "B11"),
                 "ndsi": ("B03", "B11")}


def rain_flow_lag(flow: np.ndarray, rain: np.ndarray, max_lag: int = 168) -> Tuple[float, float]:
    """
    Finds the delay at which hourly flow responds most consistently to rain.

    Both series are differenced first (the usual prewhitening), so the slow seasonal movement
    of flow does not enter and a storm of several hours still gives its true delay.

    :param flow: Hourly flow, NaN where missing.
    :type flow: np.ndarray
    :param rain: Hourly liquid precipitation, NaN where missing.
    :type rain: np.ndarray
    :param max_lag: Longest delay tried, in hours.
    :type max_lag: int
    :return: (delay in hours, correlation between the change in rain and the change in flow at
        that delay). The delay is NaN when that correlation is within what the best of
        ``max_lag`` unrelated delays would give (no detectable response to rain, as in
        snowmelt basins); both are NaN when either series does not vary.
    :rtype: Tuple[float, float]
    """
    rise = np.diff(flow, prepend=np.nan)
    rain = np.diff(rain, prepend=np.nan)
    valid = np.isfinite(rise) & np.isfinite(rain)
    if valid.sum() < 1000:
        return float("nan"), float("nan")
    rise = np.where(valid, rise - rise[valid].mean(), 0.0)
    rain = np.where(valid, rain - rain[valid].mean(), 0.0)
    norm = np.sqrt((rise ** 2).sum() * (rain ** 2).sum())
    if not norm > 0:
        return float("nan"), float("nan")
    count = len(rain)
    correlation = np.array([(rain[:count - lag] * rise[lag:]).sum()
                            for lag in range(max_lag + 1)]) / norm
    best = int(correlation.argmax())
    detectable = correlation[best] >= 4.0 / np.sqrt(valid.sum())
    return (float(best) if detectable else float("nan")), float(correlation[best])


def _yearly(series: pd.Series, how: str, min_count: int) -> pd.Series:
    """Applies ``how`` per calendar year, keeping years with at least ``min_count`` values."""
    grouped = series.groupby(series.index.year)
    return grouped.agg(how)[grouped.count() >= min_count]


def behaviour(daily: pd.DataFrame, hourly: pd.DataFrame, area_sq_km: float,
              temp_offset_c: float = 0.0) -> Optional[Dict[str, float]]:
    """
    Describes how a basin behaved over one period of its record.

    :param daily: Output of ``tabular.daily_frame`` for the period (continuous daily index).
    :type daily: pd.DataFrame
    :param hourly: Output of ``tabular.load_hourly`` for the same period.
    :type hourly: pd.DataFrame
    :param area_sq_km: Drainage area.
    :type area_sq_km: float
    :param temp_offset_c: Added to the gridded temperature to give the basin's own (the
        manifest's elevation correction), defaults to 0.0.
    :type temp_offset_c: float
    :return: Name -> value for every number in ``RESPONSE``, ``EXTREMES`` and ``BEHAVIOUR``
        (NaN where the period cannot support one), or None with fewer than ``MIN_DAYS`` days
        of flow and weather.
    :rtype: Dict[str, float], optional
    """
    both = daily.dropna(subset=["q_mm", "p", "t"])
    if len(both) < MIN_DAYS or not both.q_mm.sum() > 0 or not both.p.sum() > 0:
        return None
    nan = float("nan")
    flow, rain, temp = daily.q_mm, daily.p, daily.t + temp_offset_c
    snowfall = both.p[both.t + temp_offset_c <= SNOW_BELOW_C]
    out = {"log_runoff_mm": float(np.log(both.q_mm.mean() + 1e-3)),
           "log_runoff_ratio": float(np.log(both.q_mm.sum() / both.p.sum() + 1e-3)),
           "snow_fraction": float(snowfall.sum() / both.p.sum()),
           "zero_flow_fraction": float((both.q_mm <= 1e-4).mean())}
    demand = both.dropna(subset=["pet"])
    out["log_aridity"] = (float(np.log(demand.pet.sum() / demand.p.sum() + 1e-3))
                          if len(demand) >= MIN_DAYS and demand.p.sum() > 0 else nan)

    # Recession: the third and later day of falling flow with under 1 mm of rain in three days.
    fall = -np.log(flow.where(flow > 1e-4)).diff()
    falling = fall > 0
    dry = rain.rolling(3, min_periods=3).sum() < 1.0
    steady = falling & falling.shift(1, fill_value=False) & falling.shift(2, fill_value=False) & dry
    out["recession_rate"] = float(fall[steady].median()) if steady.sum() >= 30 else nan

    # Storm response: flow rise within three days per mm of rain, on the wettest warm days.
    warm_rain = rain.where(temp > WARM_ABOVE_C)
    peak_ahead = flow[::-1].rolling(4, min_periods=4).max()[::-1]
    rise = ((peak_ahead - flow.shift(1)).clip(lower=0) / warm_rain)
    rise = rise[warm_rain >= max(float(warm_rain.quantile(0.98)), 2.0)].dropna()
    out["storm_response"] = float(np.log(rise.median() + 1e-3)) if len(rise) >= 5 else nan

    flow_mm_hr = hourly["cfs"] * tabular.CFS_TO_MM_HR / area_sq_km
    hourly_temp = hourly["temperature"] - 273.15 + temp_offset_c
    liquid = hourly["precipitation"].where(hourly_temp > WARM_ABOVE_C, 0.0)
    lag, coupling = rain_flow_lag(flow_mm_hr.to_numpy(), liquid.to_numpy())
    out["lag_hours"], out["rain_flow_coupling"] = lag, coupling

    # Timing: the day of the water year (from 1 October) by which half its flow has passed.
    water_year = pd.Series(daily.index.year + (daily.index.month >= 10), index=daily.index)
    half_days = []
    for year, group in flow.groupby(water_year):
        if group.notna().sum() >= 330 and group.sum() > 0:
            passed = group.fillna(0.0).cumsum()
            reached = passed.index[(passed >= 0.5 * passed.iloc[-1]).to_numpy().argmax()]
            half_days.append((reached - pd.Timestamp(year - 1, 10, 1)).days + 1)
    out["half_flow_day"] = float(np.mean(half_days)) if half_days else nan

    # The same definitions as embedding_probes.flow_signatures, on this period only.
    out["rb_flashiness"] = float(flow.diff().abs().sum() / flow.sum())
    baseflow = flow.rolling(30, center=True, min_periods=10).min()
    out["bfi"] = float((baseflow / flow.where(flow > 0)).clip(0, 1).mean())
    monthly = flow.groupby(flow.index.month).mean()
    out["melt_share"] = float(monthly.reindex([4, 5, 6, 7]).sum() / monthly.sum())

    # 150 days of hourly flow make a flood year, so gauges run only in the open season count.
    floods = _yearly(flow_mm_hr.dropna(), "max", 3600)
    mean_hourly = float(flow_mm_hr.mean())
    out["flood_peak"] = float(np.log(floods.mean() + 1e-4)) if len(floods) else nan
    out["flood_ratio"] = (float(np.log(floods.mean() / mean_hourly))
                          if len(floods) and mean_hourly > 0 and floods.mean() > 0 else nan)
    droughts = _yearly(flow.rolling(7, min_periods=7).mean().dropna(), "min", 300)
    out["low_flow"] = float(np.log(droughts.mean() + 1e-3)) if len(droughts) else nan
    return out


def basin_summary(basin: Dict) -> Optional[Dict]:
    """
    Computes one basin's behaviour before and after the cutoff and its later daily flow.

    :param basin: Manifest entry.
    :type basin: Dict
    :return: ``site_id``, ``early`` and ``late`` behaviour (None when a period is too short) and
        ``later_flow`` (mm per day), or None when the hourly file cannot be read.
    :rtype: Dict, optional
    """
    hourly = tabular.load_hourly(basin)
    if hourly is None:
        return None
    daily = tabular.daily_frame(hourly, basin)
    cutoff = pd.Timestamp(CUTOFF)
    area, offset = basin["area_sq_km"], float(basin.get("temp_offset_c") or 0.0)
    before, after = daily.index < cutoff, daily.index >= cutoff
    return {"site_id": str(basin["site_id"]),
            "early": behaviour(daily[before], hourly[hourly.index < cutoff], area, offset),
            "late": behaviour(daily[after], hourly[hourly.index >= cutoff], area, offset),
            "later_flow": daily.q_mm[daily.index >= cutoff].astype(np.float32)}


def regime_classes(late: pd.DataFrame, train_sites: List[str]) -> Tuple[pd.Series, Dict]:
    """
    Assigns each basin one regime from its later record.

    Rules, first match wins: intermittent (no flow on over 5% of days), snowmelt (at least 30%
    of precipitation falls as snow and at least half the flow passes in April to July),
    flashy, steady, else mixed. The flashiness and baseflow thresholds are the upper
    terciles of the training basins that are neither intermittent nor snowmelt.

    :param late: Later-period behaviour indexed by site id.
    :type late: pd.DataFrame
    :param train_sites: Training basins (thresholds come from these only).
    :type train_sites: List[str]
    :return: (regime per site, the two thresholds).
    :rtype: Tuple[pd.Series, Dict]
    """
    intermittent = late.zero_flow_fraction > 0.05
    snowmelt = (late.snow_fraction >= 0.3) & (late.melt_share >= 0.5)
    rest = late[~intermittent & ~snowmelt]
    rest = rest.loc[rest.index.intersection(train_sites)]
    thresholds = {"flashy_min_rb": float(rest.rb_flashiness.quantile(2 / 3)),
                  "steady_min_bfi": float(rest.bfi.quantile(2 / 3))}
    labels = np.select([intermittent, snowmelt, late.rb_flashiness >= thresholds["flashy_min_rb"],
                        late.bfi >= thresholds["steady_min_bfi"]], REGIMES[:4], default="mixed")
    return pd.Series(labels, index=late.index), thresholds


def load_locations(gauges_root: str) -> pd.DataFrame:
    """Reads gauge coordinates from every ``<state>/gauges.csv`` under the scrape root."""
    frames = [pd.read_csv(path, dtype={"site_no": str})
              for path in sorted(glob.glob(os.path.join(gauges_root, "*", "gauges.csv")))]
    table = pd.concat(frames).drop_duplicates("site_no").set_index("site_no")
    return table[["dec_lat_va", "dec_long_va"]].astype(float)


def location_km(place: pd.DataFrame, sites: List[str], cohort: List[str],
                train_sites: List[str]) -> np.ndarray:
    """
    Places gauges east and north of the centre of the training basins, in units of 500 km.

    Both axes share one scale, so distances stay distances. The scale and the centre come from
    the cohort alone, so gauges outside it can be placed on the cohort's own map.

    :param place: Coordinates from :func:`load_locations`.
    :type place: pd.DataFrame
    :param sites: The gauges to place, in row order.
    :type sites: List[str]
    :param cohort: The basins that fix the east-west scale (their mean latitude).
    :type cohort: List[str]
    :param train_sites: The basins whose centre is the origin.
    :type train_sites: List[str]
    :return: Array of shape (n_sites, 2); a gauge without coordinates sits at the origin.
    :rtype: np.ndarray
    """
    shrink = np.cos(np.nanmean(np.radians(place.dec_lat_va.reindex(cohort).to_numpy())))

    def position(names: List[str]) -> Tuple[np.ndarray, np.ndarray]:
        rows = place.reindex(names)
        return (6371.0 * np.radians(rows.dec_long_va.to_numpy()) * shrink,
                6371.0 * np.radians(rows.dec_lat_va.to_numpy()))

    centre_east, centre_north = (np.nanmean(axis) for axis in position(train_sites))
    east, north = position(sites)
    return np.nan_to_num(np.stack([east - centre_east, north - centre_north], axis=1) / 500.0)


def image_statistics(image: np.ndarray, bands: Sequence[str]) -> Dict[str, float]:
    """
    Summarizes a satellite patch as hand-built numbers.

    :param image: Patch of shape (bands, height, width) in digital numbers; a pixel that is zero
        in every band lies outside the scene and is left out.
    :type image: np.ndarray
    :param bands: Sentinel-2 names of the bands, in order.
    :type bands: Sequence[str]
    :return: Mean and spread of every band and of every index of ``IMAGE_INDICES`` whose bands
        are present, the share of open water (NDWI above 0) and, with band B11, of snow (NDSI
        above 0.4). Empty when fewer than 16 pixels hold data.
    :rtype: Dict[str, float]
    """
    valid = (image > 0).any(axis=0)
    if valid.sum() < 16:
        return {}
    pixels = {band: image[position][valid].astype(np.float64)
              for position, band in enumerate(bands)}
    numbers = {}
    for band, values in pixels.items():
        numbers[band + "_mean"], numbers[band + "_std"] = values.mean(), values.std()
    for name, (first, second) in IMAGE_INDICES.items():
        if first in pixels and second in pixels:
            index = (pixels[first] - pixels[second]) / np.maximum(pixels[first] + pixels[second],
                                                                  1.0)
            numbers[name + "_mean"], numbers[name + "_std"] = index.mean(), index.std()
            if name == "ndwi":
                numbers["water_share"] = (index > 0.0).mean()
            if name == "ndsi":
                numbers["snow_share"] = (index > 0.4).mean()
    return {name: float(value) for name, value in numbers.items()}


def build_imagery(manifest_path: str, arms_dir: str, records_root: str) -> Dict[str, int]:
    """
    Writes ``context_imagery_<source>.pt`` for the reach and the regional patch of every basin.

    :param manifest_path: Manifest whose basins carry ``split`` and ``state``.
    :type manifest_path: str
    :param arms_dir: Directory of the context banks; outputs are written here.
    :type arms_dir: str
    :param records_root: Root of the embedding records (``<root>/<state>/<site>.npz`` and
        ``<site>_regional.npz``).
    :type records_root: str
    :return: Source -> number of basins with a patch (the others get the training median).
    :rtype: Dict[str, int]
    """
    from experiments.catchment_foundation.prepare_context_ablation import standardize_context
    with open(manifest_path) as file:
        basins = json.load(file)["basins"]
    sites = [str(basin["site_id"]) for basin in basins]
    train_rows = np.array([basin["split"] == "train" for basin in basins])
    found = {}
    for source, (pattern, key, bands) in IMAGE_SOURCES.items():
        rows = {}
        for basin in basins:
            path = os.path.join(records_root, str(basin.get("state", "")),
                                pattern.format(site=basin["site_id"]))
            if os.path.exists(path):
                with np.load(path) as record:
                    rows[str(basin["site_id"])] = image_statistics(record[key], bands)
        table = pd.DataFrame(rows).T.reindex(sites)
        found[source] = int(table.notna().any(axis=1).sum())
        _write_bank(arms_dir, "imagery_" + source, sites,
                    standardize_context(table.to_numpy(dtype=float), train_rows, CONTEXT_WIDTH))
        print("imagery_%s: %d numbers, %d of %d basins with a patch"
              % (source, table.shape[1], found[source], len(sites)), flush=True)
    return found


def climate_indices(daily: pd.DataFrame, min_days: int = 365) -> Dict[str, float]:
    """
    Describes a basin's climate from its weather alone: no flow is read.

    These are the numbers a basin without a gauge still has. They are computed over the days
    with weather, so for a gauge whose file lacks winter weather they describe its open season.

    :param daily: Daily frame with ``p`` and ``pet`` (mm per day) and ``t`` (deg C).
    :type daily: pd.DataFrame
    :param min_days: Days of weather needed, defaults to 365.
    :type min_days: int, optional
    :return: The ``CLIMATE`` numbers, or NaN for each when there is too little weather.
    :rtype: Dict[str, float]
    """
    frame = daily[["p", "t", "pet"]].dropna()
    if len(frame) < min_days or frame.p.sum() <= 0:
        return {name: float("nan") for name in CLIMATE}
    rain, warmth, demand = frame.p.clip(lower=0), frame.t, frame.pet.clip(lower=0)
    monthly_rain = rain.groupby(frame.index.month).mean().reindex(range(1, 13))
    monthly_warmth = warmth.groupby(frame.index.month).mean().reindex(range(1, 13))
    angle = 2.0 * np.pi * (np.arange(12) + 0.5) / 12.0
    relative = (monthly_rain / monthly_rain.mean()).to_numpy()
    both = monthly_rain.notna() & monthly_warmth.notna()
    together = np.corrcoef(monthly_rain[both], monthly_warmth[both])[0, 1] if both.sum() > 2 else 0.0
    return {
        "log_rain": float(np.log(rain.mean() + 0.01)),
        "log_evaporation": float(np.log(demand.mean() + 0.01)),
        "log_aridity": float(np.log((demand.mean() + 0.01) / (rain.mean() + 0.01))),
        "temperature": float(warmth.mean() / 10.0),
        "temperature_swing": float(np.nanstd(monthly_warmth.to_numpy()) / 10.0),
        "snow_share": float(rain[warmth < 0].sum() / rain.sum()),
        "rain_season_cos": float(np.nanmean(relative * np.cos(angle)) * 2.0),
        "rain_season_sin": float(np.nanmean(relative * np.sin(angle)) * 2.0),
        "heavy_rain_days": float((rain >= 5.0 * rain.mean()).mean()),
        "dry_days": float((rain < 1.0).mean()),
        "rain_with_warmth": float(np.nan_to_num(together)),
    }


def build_climate(arms_dir: str, daily_path: str) -> int:
    """
    Writes ``context_climate.pt``: every basin's climate indices, standardized on the training
    basins (a basin with too little weather gets their mean).

    :param arms_dir: Directory of the context banks.
    :type arms_dir: str
    :param daily_path: Pickle with ``daily`` (site id -> daily frame before the cutoff) and
        ``split`` (site id -> train or holdout), as ``pretrain_behaviour_code.py cache`` writes.
    :type daily_path: str
    :return: The number of basins with indices of their own.
    :rtype: int
    """
    cache = pd.read_pickle(daily_path)
    sites = list(cache["split"])
    table = pd.DataFrame({site: climate_indices(cache["daily"][site]) for site in sites
                          if site in cache["daily"]}).T.reindex(sites)[CLIMATE]
    fitted = table.loc[[site for site in sites if cache["split"][site] == "train"]]
    scaled = ((table - fitted.mean()) / fitted.std().replace(0.0, 1.0)).clip(-8.0, 8.0)
    _write_bank(arms_dir, "climate", sites, scaled.fillna(0.0).to_numpy())
    return int(table.notna().all(axis=1).sum())


def _write_bank(arms_dir: str, name: str, sites: List[str], matrix: np.ndarray) -> None:
    """Writes a context bank in the format of ``prepare_context_ablation.py``."""
    import torch
    torch.save({"site_ids": sites, "embeddings": torch.tensor(matrix, dtype=torch.float32)},
               os.path.join(arms_dir, "context_%s.pt" % name))


def build(manifest_path: str, arms_dir: str, gauges_root: Optional[str], workers: int) -> Dict:
    """
    Writes the hand-built description banks and the benchmark targets of a manifest.

    :param manifest_path: Manifest whose basins carry ``split`` (train or holdout).
    :type manifest_path: str
    :param arms_dir: Directory of the context banks; outputs are written here.
    :type arms_dir: str
    :param gauges_root: Scrape root holding ``<state>/gauges.csv``, or None to skip locations.
    :type gauges_root: str, optional
    :param workers: Processes reading hourly files.
    :type workers: int
    :return: The saved benchmark (``early``, ``late``, ``regime``, ``later_flow``, ``split``).
    :rtype: Dict
    """
    from experiments.catchment_foundation.prepare_context_ablation import standardize_context
    with open(manifest_path) as file:
        basins = json.load(file)["basins"]
    started = time.time()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        summaries = [item for item in pool.map(basin_summary, basins, chunksize=4) if item]
    print("read %d of %d basins in %.0f s" % (len(summaries), len(basins),
                                              time.time() - started), flush=True)
    sites = [str(basin["site_id"]) for basin in basins]
    split = pd.Series({str(basin["site_id"]): basin["split"] for basin in basins})
    state = pd.Series({str(basin["site_id"]): basin.get("state", "unknown") for basin in basins})
    train_sites = sorted(split.index[split == "train"])
    early = pd.DataFrame({item["site_id"]: item["early"] for item in summaries
                          if item["early"]}).T
    late = pd.DataFrame({item["site_id"]: item["late"] for item in summaries if item["late"]}).T
    regime, thresholds = regime_classes(late, train_sites)
    later_flow = pd.DataFrame({item["site_id"]: item["later_flow"] for item in summaries})

    train_rows = (split.reindex(sites) == "train").to_numpy()
    for name, columns in (("response", RESPONSE), ("extremes", EXTREMES)):
        raw = early.reindex(sites)[columns].to_numpy(dtype=float)
        _write_bank(arms_dir, name, sites, standardize_context(raw, train_rows, CONTEXT_WIDTH))
    if gauges_root:
        place = load_locations(gauges_root)
        _write_bank(arms_dir, "location", sites, location_km(place, sites, sites, train_sites))
    benchmark = {"early": early, "late": late, "regime": regime, "later_flow": later_flow,
                 "split": split, "state": state, "regime_thresholds": thresholds,
                 "manifest": os.path.abspath(manifest_path)}
    pd.to_pickle(benchmark, os.path.join(arms_dir, "benchmark.pkl"))
    print("basins with an early description: %d; with later targets: %d (%d held out)"
          % (len(early), len(late), int((split.reindex(late.index) == "holdout").sum())))
    print("regimes (training / held-out):")
    counts = pd.crosstab(regime, split.reindex(regime.index))
    print(counts.to_string())
    return benchmark


def load_representation(spec: str, arms_dir: str, train_sites: List[str]) -> pd.DataFrame:
    """Loads a ``+``-joined list of per-basin feature groups as one frame indexed by site id."""
    blocks = [tabular.basin_block(group, arms_dir, train_sites) for group in spec.split("+")]
    frame = pd.concat(blocks, axis=1, join="inner")
    frame.columns = range(frame.shape[1])
    return frame


def choose_readout(features: np.ndarray, target: np.ndarray, classify: bool, seed: int):
    """
    Fits a linear model and boosted trees and keeps the one that cross-validates better.

    The choice uses the training basins only (five folds), so it is part of fitting, not of
    scoring.

    :param features: Training-basin features.
    :type features: np.ndarray
    :param target: Training-basin target (labels when ``classify``).
    :type target: np.ndarray
    :param classify: Whether the target is a class.
    :type classify: bool
    :param seed: Seed of the folds and of the trees.
    :type seed: int
    :return: (fitted model, name of the chosen readout).
    :rtype: tuple
    """
    from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
    from sklearn.linear_model import LogisticRegression, RidgeCV
    from sklearn.model_selection import KFold, StratifiedKFold, cross_val_score
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    trees = dict(max_iter=150, learning_rate=0.05, max_leaf_nodes=6, min_samples_leaf=8,
                 l2_regularization=1.0, random_state=seed)
    if classify:
        candidates = {
            "linear": make_pipeline(StandardScaler(), LogisticRegression(
                C=0.1, class_weight="balanced", max_iter=3000)),
            "trees": HistGradientBoostingClassifier(class_weight="balanced", **trees)}
        folds = StratifiedKFold(5, shuffle=True, random_state=seed)
        scoring = "balanced_accuracy"
    else:
        candidates = {
            "linear": make_pipeline(StandardScaler(), RidgeCV(alphas=np.logspace(-1, 4, 11))),
            "trees": HistGradientBoostingRegressor(**trees)}
        folds = KFold(5, shuffle=True, random_state=seed)
        scoring = "r2"
    scores = {name: cross_val_score(model, features, target, cv=folds, scoring=scoring).mean()
              for name, model in candidates.items()}
    best = max(scores, key=scores.get)
    return candidates[best].fit(features, target), best


def nearest(train: np.ndarray, test: np.ndarray, k: int) -> np.ndarray:
    """Returns, for each test row, the indices of its ``k`` nearest training rows (Euclidean)."""
    cross = 2.0 * test @ train.T
    squared = (test ** 2).sum(axis=1)[:, None] + (train ** 2).sum(axis=1)[None, :] - cross
    return np.argsort(squared, axis=1, kind="stable")[:, :k]


def kge(simulated: np.ndarray, observed: np.ndarray) -> float:
    """Kling-Gupta efficiency of two aligned series (NaN when either does not vary)."""
    if simulated.std() == 0 or observed.std() == 0 or observed.mean() == 0:
        return float("nan")
    correlation = np.corrcoef(simulated, observed)[0, 1]
    spread = simulated.std() / observed.std()
    bias = simulated.mean() / observed.mean()
    return float(1.0 - np.sqrt((correlation - 1) ** 2 + (spread - 1) ** 2 + (bias - 1) ** 2))


def statistic(result: Dict, rows: Optional[np.ndarray] = None) -> float:
    """
    Evaluates a task result on a subset of its held-out basins.

    :param result: ``kind`` plus per-basin arrays: ``r2`` (``error``, ``total``; a 2-D pair gives
        the mean over columns), ``balanced_accuracy`` (``truth``, ``predicted``) or ``median``
        (``values``).
    :type result: Dict
    :param rows: Basin rows to use (with repeats, for a bootstrap), defaults to all.
    :type rows: np.ndarray, optional
    :return: The statistic.
    :rtype: float
    """
    def pick(name):
        return result[name] if rows is None else result[name][rows]

    if result["kind"] == "r2":
        error, total = pick("error").sum(axis=0), pick("total").sum(axis=0)
        return float(np.mean(1.0 - error / np.where(total > 0, total, np.nan)))
    if result["kind"] == "balanced_accuracy":
        truth, predicted = pick("truth"), pick("predicted")
        return float(np.mean([(predicted[truth == label] == label).mean()
                              for label in np.unique(truth)]))
    return float(np.nanmedian(pick("values")))


def interval(result: Dict, reference: Optional[Dict] = None, n_boot: int = 2000,
             seed: int = 0) -> List[float]:
    """
    Basin-bootstrap 95% interval of a statistic, or of its difference from a reference.

    :param result: Task result of one representation.
    :type result: Dict
    :param reference: Result of another representation on the same basins, to pair against.
    :type reference: Dict, optional
    :param n_boot: Resamples.
    :type n_boot: int
    :param seed: Bootstrap seed.
    :type seed: int
    :return: [2.5th, 97.5th percentile].
    :rtype: List[float]
    """
    rng = np.random.default_rng(seed)
    count = len(result["sites"])
    draws = []
    for _ in range(n_boot):
        rows = rng.integers(0, count, size=count)
        value = statistic(result, rows)
        draws.append(value - statistic(reference, rows) if reference else value)
    return [float(np.nanpercentile(draws, 2.5)), float(np.nanpercentile(draws, 97.5))]


def evaluate(features: pd.DataFrame, benchmark: Dict, seed: int = 0, neighbours: int = 5,
             donors: int = 3) -> Dict[str, Dict]:
    """
    Scores one representation on every task.

    :param features: Representation indexed by site id (training and held-out basins).
    :type features: pd.DataFrame
    :param benchmark: Output of :func:`build`.
    :type benchmark: Dict
    :param seed: Seed of the readouts.
    :type seed: int
    :param neighbours: Training basins averaged for the behaviour donor test.
    :type neighbours: int
    :param donors: Training basins averaged for the flow donor test.
    :type donors: int
    :return: Task name -> result for :func:`statistic`, each with its held-out ``sites``.
    :rtype: Dict[str, Dict]
    """
    late, split = benchmark["late"], benchmark["split"]
    known = late.index.intersection(features.index)
    train = [site for site in known if split[site] == "train"]
    held = [site for site in known if split[site] == "holdout"]
    results = {}
    for name in EXTREME_TARGETS:
        fit = [site for site in train if np.isfinite(late.at[site, name])]
        test = [site for site in held if np.isfinite(late.at[site, name])]
        target = late.loc[fit, name].to_numpy(dtype=float)
        model, readout = choose_readout(features.loc[fit].to_numpy(), target, False, seed)
        truth = late.loc[test, name].to_numpy(dtype=float)
        results[name] = {"kind": "r2", "sites": test, "readout": readout,
                         "error": (model.predict(features.loc[test].to_numpy()) - truth) ** 2,
                         "total": (truth - target.mean()) ** 2}

    regime = benchmark["regime"]
    model, readout = choose_readout(features.loc[train].to_numpy(), regime[train].to_numpy(),
                                    True, seed)
    results["regime"] = {"kind": "balanced_accuracy", "sites": held, "readout": readout,
                         "truth": regime[held].to_numpy(),
                         "predicted": model.predict(features.loc[held].to_numpy())}

    # Donors: nearest training basins in the representation's own space, used as it is.
    order = nearest(features.loc[train].to_numpy(dtype=float),
                    features.loc[held].to_numpy(dtype=float), max(neighbours, donors))
    numbers = late.loc[train, BEHAVIOUR].astype(float)
    centre, spread = numbers.mean(), numbers.std().replace(0.0, 1.0)
    donor_numbers = ((numbers - centre) / spread).to_numpy()
    truth = ((late.loc[held, BEHAVIOUR].astype(float) - centre) / spread).to_numpy()
    with np.errstate(all="ignore"):
        guess = np.nanmean(donor_numbers[order[:, :neighbours]], axis=1)
    usable = np.isfinite(truth) & np.isfinite(guess)
    results["similar_behaviour"] = {"kind": "r2", "sites": held,
                                    "error": np.where(usable, (guess - truth) ** 2, 0.0),
                                    "total": np.where(usable, truth ** 2, 0.0)}

    flow = benchmark["later_flow"]
    donor_flow = flow[train].to_numpy(dtype=float)
    scores = []
    for row, site in enumerate(held):
        with np.errstate(all="ignore"):
            simulated = np.nanmean(donor_flow[:, order[row, :donors]], axis=1)
        observed = flow[site].to_numpy(dtype=float)
        days = np.isfinite(simulated) & np.isfinite(observed)
        scores.append(kge(simulated[days], observed[days]) if days.sum() >= 365
                      else float("nan"))
    results["similar_flow"] = {"kind": "median", "sites": held, "values": np.array(scores)}
    return results


TASKS = EXTREME_TARGETS + ["regime", "similar_behaviour", "similar_flow"]


def run(arms_dir: str, representations: List[str], reference: str, seed: int, n_boot: int,
        output: Optional[str], also_pair: Sequence[str] = ()) -> Dict:
    """
    Scores the representations, prints the tables and returns the report.

    Every representation is compared with ``reference`` and, under ``paired``, with each
    representation in ``also_pair`` (difference of scores and its basin-bootstrap interval).
    """
    benchmark = pd.read_pickle(os.path.join(arms_dir, "benchmark.pkl"))
    split = benchmark["split"]
    train_sites = sorted(split.index[split == "train"])
    if reference not in representations:
        representations = [reference] + representations
    results, started = {}, time.time()
    for spec in representations:
        features = load_representation(spec, arms_dir, train_sites)
        results[spec] = evaluate(features, benchmark, seed=seed)
        print("scored %-44s %3d features  %4.0f s" % (spec, features.shape[1],
                                                      time.time() - started), flush=True)
    report = {"arms_dir": os.path.abspath(arms_dir), "reference": reference, "seed": seed,
              "held_out_basins": len(results[reference]["regime"]["sites"]), "scores": {}}
    for spec in representations:
        entry = {}
        for task in TASKS:
            result = results[spec][task]
            stats = {"score": statistic(result), "ci95": interval(result, n_boot=n_boot),
                     "basins": len(result["sites"])}
            if "readout" in result:
                stats["readout"] = result["readout"]
            if spec != reference:
                paired = results[reference][task]
                stats["vs_reference"] = stats["score"] - statistic(paired)
                stats["vs_reference_ci95"] = interval(result, paired, n_boot=n_boot)
            for other in also_pair:
                if other != spec and other in results:
                    stats.setdefault("paired", {})[other] = {
                        "difference": stats["score"] - statistic(results[other][task]),
                        "ci95": interval(result, results[other][task], n_boot=n_boot)}
            entry[task] = stats
        entry["extremes_mean"] = float(np.mean([entry[name]["score"]
                                                for name in EXTREME_TARGETS]))
        report["scores"][spec] = entry

    print("\nHeld-out basins (%d), targets from %s on. R2 for the four extremes and for "
          "similar_behaviour, balanced accuracy for regime, median KGE for similar_flow."
          % (report["held_out_basins"], CUTOFF))
    print("%-44s" % "representation" + "".join("%18s" % task for task in TASKS))
    for spec in representations:
        print("%-44s" % spec + "".join("%18.3f" % report["scores"][spec][task]["score"]
                                       for task in TASKS))
    print("\nDifference from %s (95%% interval; * excludes zero)" % reference)
    for spec in representations:
        if spec == reference:
            continue
        line = "%-44s" % spec
        for task in TASKS:
            stats = report["scores"][spec][task]
            low, high = stats["vs_reference_ci95"]
            line += "%18s" % ("%+.2f%s(%+.2f,%+.2f)" % (
                stats["vs_reference"], "*" if low > 0 or high < 0 else " ", low, high))
        print(line)
    for other in also_pair:
        print("\nDifference from %s (95%% interval; * excludes zero)" % other)
        for spec in representations:
            cells = [report["scores"][spec][task].get("paired", {}).get(other) for task in TASKS]
            if all(cells):
                print("%-44s" % spec + "".join("%18s" % ("%+.2f%s(%+.2f,%+.2f)" % (
                    cell["difference"], "*" if cell["ci95"][0] > 0 or cell["ci95"][1] < 0 else " ",
                    *cell["ci95"])) for cell in cells))
    if output:
        os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
        with open(output, "w") as file:
            json.dump(report, file, indent=1)
    return report


def recover(arms_dir: str, sources: List[str], targets: str, seed: int,
            output: Optional[str]) -> pd.DataFrame:
    """
    Measures how much of a target description each source reproduces on held-out basins.

    :param arms_dir: Directory of the context banks and ``benchmark.pkl``.
    :type arms_dir: str
    :param sources: Representations to predict from.
    :type sources: List[str]
    :param targets: Representation whose columns are predicted one by one.
    :type targets: str
    :param seed: Seed of the readouts.
    :type seed: int
    :param output: Optional JSON path for the table.
    :type output: str, optional
    :return: Held-out R2 per target column (rows) and source (columns).
    :rtype: pd.DataFrame
    """
    split = pd.read_pickle(os.path.join(arms_dir, "benchmark.pkl"))["split"]
    train = sorted(split.index[split == "train"])
    held = sorted(split.index[split == "holdout"])
    wanted = load_representation(targets, arms_dir, train)
    names = []
    for group in targets.split("+"):
        width = tabular.basin_block(group, arms_dir, train).shape[1]
        known = {"response": RESPONSE, "extremes": EXTREMES}.get(group, [])
        names += ["%s:%s" % (group, known[i] if len(known) == width else i) for i in range(width)]
    table = pd.DataFrame(index=names, columns=sources, dtype=float)
    for source in sources:
        features = load_representation(source, arms_dir, train)
        for column, name in enumerate(names):
            fit, truth = wanted.loc[train, column].to_numpy(), wanted.loc[held, column].to_numpy()
            model, _ = choose_readout(features.loc[train].to_numpy(), fit, False, seed)
            error = ((model.predict(features.loc[held].to_numpy()) - truth) ** 2).sum()
            table.loc[name, source] = 1.0 - error / ((truth - fit.mean()) ** 2).sum()
    print(table.round(2).to_string())
    print("mean  " + "  ".join("%s %.2f" % (source, table[source].mean()) for source in sources))
    if output:
        os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
        table.to_json(output, indent=1)
    return table


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    builder = commands.add_parser("build", help="write the hand-built banks and the targets")
    builder.add_argument("--manifest", required=True)
    builder.add_argument("--arms-dir", required=True,
                         help="Directory of context_<name>.pt banks; outputs go here")
    builder.add_argument("--gauges-root", default=None,
                         help="Scrape root with <state>/gauges.csv (for the location bank)")
    builder.add_argument("--workers", type=int, default=4)
    runner = commands.add_parser("run", help="score representations on the benchmark")
    runner.add_argument("--arms-dir", required=True)
    runner.add_argument("--representations", nargs="+", required=True,
                        help="Bank names or +-joined lists, e.g. statics_signatures+response")
    runner.add_argument("--reference", default="statics")
    runner.add_argument("--seed", type=int, default=0)
    runner.add_argument("--n-boot", type=int, default=2000)
    runner.add_argument("--also-pair", nargs="*", default=[],
                        help="Further representations every one is compared with")
    runner.add_argument("--output", default=None)
    imager = commands.add_parser("imagery", help="write hand-built numbers of the satellite "
                                                 "patches as banks")
    imager.add_argument("--manifest", required=True)
    imager.add_argument("--arms-dir", required=True)
    imager.add_argument("--records-root", required=True,
                        help="Embedding records root: <root>/<state>/<site>.npz")
    climate = commands.add_parser("climate", help="write the climate-index bank (weather only)")
    climate.add_argument("--arms-dir", required=True)
    climate.add_argument("--daily", required=True,
                         help="pre-cutoff daily cache written by pretrain_behaviour_code.py cache")
    recoverer = commands.add_parser("recover", help="predict one description from others")
    recoverer.add_argument("--arms-dir", required=True)
    recoverer.add_argument("--from", dest="sources", nargs="+", required=True)
    recoverer.add_argument("--targets", default="signatures+response+extremes")
    recoverer.add_argument("--seed", type=int, default=0)
    recoverer.add_argument("--output", default=None)
    args = parser.parse_args()
    if args.command == "build":
        build(args.manifest, args.arms_dir, args.gauges_root, args.workers)
    elif args.command == "imagery":
        build_imagery(args.manifest, args.arms_dir, args.records_root)
    elif args.command == "recover":
        recover(args.arms_dir, args.sources, args.targets, args.seed, args.output)
    elif args.command == "climate":
        print("%d basins with climate indices" % build_climate(args.arms_dir, args.daily))
    else:
        run(args.arms_dir, args.representations, args.reference, args.seed, args.n_boot,
            args.output, also_pair=args.also_pair)


if __name__ == "__main__":
    main()
