"""
Tabular forecast baseline: does a catchment context help a forecaster that uses the weather?

Gradient-boosted trees on daily features, one model per lead band. The target is the band-mean
flow over days 1-3, 4-7 and 8-14 after the issue day minus the issue-day flow, in the same
per-basin standardized units the neural forecasters use; skill is against persistence
(predicting no change). Features come in groups, so each arm adds one thing:

* ``flow``    - flow at issue time and over the previous 30 days, plus day of year;
* ``weather`` - precipitation, temperature, shortwave and potential evaporation over the
  previous 7 and 30 days and over each lead band (realized weather: a hindcast, like the neural
  forecasters), in physical units; ``weather_z`` is the same standardized per basin;
* ``snow``    - basin snow water equivalent on the issue day (SNODAS, where a series exists);
* a context bank written by ``prepare_context_ablation.py`` (statics, statics + signatures, an
  embedding, its shuffled twin), ``signatures`` alone, ``<bank>_pca<k>`` (the bank reduced to k
  principal components fitted on the training basins, so a wide embedding is not penalized for
  its width), or ``basin_id`` (a categorical identifier: meaningful only on basins the model was
  fitted on).

It trains in minutes on CPU, so it is the quick way to ask whether a context carries forecast
value before spending neural-network runs on the question. Everything is fitted on the training
basins before the cutoff; scores are reported for trained-on and held-out basins, for the first
year after the cutoff and for the years after that.

    python experiments/catchment_foundation/tabular_forecast_baseline.py build \
        --manifest <arms dir>/manifest_none.json --out <arms dir>/tabular.pkl
    python experiments/catchment_foundation/tabular_forecast_baseline.py run \
        --data <arms dir>/tabular.pkl --arms-dir <arms dir> --output <arms dir>/reports/tabular.json
"""
import argparse
import json
import os
import sys
import time
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

CUTOFF, EVAL_START = "2022-01-01", "2023-01-01"
BANDS = {"day1-3": (1, 3), "day4-7": (4, 7), "day8-14": (8, 14)}
FLOW = ["q0", "q_1", "q_3", "q_7", "q_mean7", "q_mean30", "q_max30", "q_min30", "sin_doy", "cos_doy"]
WEATHER = [f"{v}_prev{n}" for v in ("p", "t", "sw", "pet") for n in (7, 30)]
WEATHER += [f"{v}_{band}" for band in BANDS for v in ("p", "t", "sw")]
SNOW = ["swe0"]
CFS_TO_MM_HR = 0.0283168 * 3.6
MAX_CATEGORIES = 255

ARMS = {
    "flow": ("flow",),
    "flow+weather": ("flow", "weather"),
    "flow+weather_z": ("flow", "weather_z"),
    "flow+weather+snow": ("flow", "weather", "snow"),
    "flow+weather+snow+statics": ("flow", "weather", "snow", "statics"),
    "flow+weather+snow+statics_signatures": ("flow", "weather", "snow", "statics_signatures"),
    "flow+weather+snow+v10": ("flow", "weather", "snow", "v10"),
    "flow+weather+snow+v10_shuffled": ("flow", "weather", "snow", "v10_shuffled"),
    "flow+weather+snow+basin_id": ("flow", "weather", "snow", "basin_id"),
    "flow+statics_signatures": ("flow", "statics_signatures"),
    "flow+weather+snow+signatures": ("flow", "weather", "snow", "signatures"),
    "flow+weather+snow+v10_pca16": ("flow", "weather", "snow", "v10_pca16"),
    "flow+weather+snow+statics_signatures+v10_pca16": ("flow", "weather", "snow",
                                                       "statics_signatures", "v10_pca16"),
}
N_SIGNATURES = 8      # prepare_context_ablation.py appends the flow signatures after the statics
REFERENCE = "flow+weather+snow"


def load_hourly(basin: Dict) -> Optional[pd.DataFrame]:
    """
    Reads one basin's hourly flow and gridded forcing.

    :param basin: Manifest entry with ``csv_path``.
    :type basin: Dict
    :return: Frame indexed by naive UTC hour with ``cfs`` (negative values clipped to zero) and
        the forcing columns, or None when the file cannot be read.
    :rtype: pd.DataFrame, optional
    """
    columns = ["precipitation", "temperature", "shortwave_radiation", "pet_mm_hr"]
    try:
        hourly = pd.read_csv(basin["csv_path"], usecols=["datetime", "cfs"] + columns,
                             parse_dates=["datetime"]).set_index("datetime").sort_index()
    except (OSError, ValueError):
        return None
    if hourly.index.tz is not None:
        hourly.index = hourly.index.tz_convert("UTC").tz_localize(None)
    hourly = hourly[~hourly.index.duplicated()].apply(pd.to_numeric, errors="coerce")
    hourly["cfs"] = hourly["cfs"].clip(lower=0)
    return hourly


def daily_frame(hourly: pd.DataFrame, basin: Dict) -> pd.DataFrame:
    """
    Aggregates a basin's hourly record to days.

    :param hourly: Output of :func:`load_hourly`.
    :type hourly: pd.DataFrame
    :param basin: Manifest entry (``area_sq_km``, ``flow_scale_mm_hr``, optional
        ``swe_csv_path``).
    :type basin: Dict
    :return: Daily frame with ``q`` (flow in the forecasters' per-basin standardized units),
        ``q_mm`` (flow in mm per day over the basin), ``p`` and ``pet`` (mm per day), ``t``
        (deg C), ``sw`` and ``swe`` (snowpack, NaN without a snowpack file).
    :rtype: pd.DataFrame
    """
    flow_mm_hr = hourly["cfs"] * CFS_TO_MM_HR / basin["area_sq_km"]
    counted = flow_mm_hr.resample("1D").count() >= 18
    daily = pd.DataFrame({
        "q": (flow_mm_hr / basin["flow_scale_mm_hr"]).resample("1D").mean().where(counted),
        "q_mm": (24.0 * flow_mm_hr.resample("1D").mean()).where(counted),
        "p": hourly["precipitation"].resample("1D").sum(min_count=18),
        "t": hourly["temperature"].resample("1D").mean() - 273.15,
        "sw": hourly["shortwave_radiation"].resample("1D").mean(),
        "pet": hourly["pet_mm_hr"].resample("1D").sum(min_count=18)})
    daily["swe"] = np.nan
    swe_path = basin.get("swe_csv_path")
    if swe_path and os.path.exists(swe_path):
        series = pd.read_csv(swe_path, parse_dates=["datetime"]).set_index("datetime")
        series = pd.to_numeric(series.iloc[:, 0], errors="coerce")
        daily["swe"] = series[~series.index.duplicated()].reindex(daily.index).ffill(limit=10)
    return daily


def basin_table(basin: Dict, fit_stride: int, eval_stride: int) -> Optional[pd.DataFrame]:
    """
    Builds one basin's daily feature and target rows.

    :param basin: Manifest entry (``csv_path``, ``area_sq_km``, ``flow_scale_mm_hr``, split).
    :type basin: Dict
    :param fit_stride: Days between issue dates before the cutoff.
    :type fit_stride: int
    :param eval_stride: Days between issue dates after the cutoff.
    :type eval_stride: int
    :return: Rows indexed by issue date, or None when the hourly file cannot be read.
    :rtype: pd.DataFrame, optional
    """
    hourly = load_hourly(basin)
    if hourly is None:
        return None
    daily = daily_frame(hourly, basin)

    rows = pd.DataFrame(index=daily.index)
    rows["q0"] = daily.q
    for lag in (1, 3, 7):
        rows[f"q_{lag}"] = daily.q.shift(lag)
    rows["q_mean7"] = daily.q.rolling(7, min_periods=5).mean()
    month = daily.q.rolling(30, min_periods=20)
    rows["q_mean30"], rows["q_max30"], rows["q_min30"] = month.mean(), month.max(), month.min()
    day_of_year = daily.index.dayofyear
    rows["sin_doy"] = np.sin(2 * np.pi * day_of_year / 365.25)
    rows["cos_doy"] = np.cos(2 * np.pi * day_of_year / 365.25)
    for variable in ("p", "t", "sw", "pet"):
        how = "sum" if variable in ("p", "pet") else "mean"
        rows[f"{variable}_prev7"] = getattr(daily[variable].rolling(7, min_periods=6), how)()
        rows[f"{variable}_prev30"] = getattr(daily[variable].rolling(30, min_periods=25), how)()
    for band, (first, last) in BANDS.items():
        length = last - first + 1

        def ahead(series: pd.Series, how: str) -> pd.Series:
            return getattr(series.shift(-last).rolling(length, min_periods=length), how)()

        rows[f"y_{band}"] = ahead(daily.q, "mean") - daily.q
        rows[f"p_{band}"] = ahead(daily.p, "sum")
        rows[f"t_{band}"] = ahead(daily.t, "mean")
        rows[f"sw_{band}"] = ahead(daily.sw, "mean")
    rows["swe0"] = daily.swe
    required = ["q0", "q_mean30", "p_prev30", "t_prev30"]
    required += [f"{name}_{band}" for band in BANDS for name in ("y", "p", "t")]
    rows = rows.dropna(subset=required)
    rows["site_id"], rows["split"] = basin["site_id"], basin["split"]
    rows["state"] = basin.get("state", "unknown")
    before = rows[rows.index < CUTOFF].iloc[::fit_stride].assign(period="fit")
    first_year = rows[(rows.index >= CUTOFF) & (rows.index < EVAL_START)].iloc[::eval_stride]
    later = rows[rows.index >= EVAL_START].iloc[::eval_stride]
    return pd.concat([before, first_year.assign(period="2022"), later.assign(period="2023+")])


def build(manifest_path: str, out_path: str, fit_stride: int, eval_stride: int) -> None:
    """Builds and saves the table for every basin of a manifest."""
    with open(manifest_path) as file:
        basins = json.load(file)["basins"]
    tables = []
    for count, basin in enumerate(basins, 1):
        table = basin_table(basin, fit_stride, eval_stride)
        if table is not None and len(table):
            tables.append(table)
        if count % 100 == 0:
            print("%d of %d basins" % (count, len(basins)), flush=True)
    data = pd.concat(tables)
    data.index.name = "date"
    data.reset_index().to_pickle(out_path)
    print(data.groupby(["split", "period"]).size().to_string())
    print("%d basins, %d rows -> %s" % (data.site_id.nunique(), len(data), out_path))


def load_bank(arms_dir: str, name: str) -> pd.DataFrame:
    """Loads a prepared context bank as a frame indexed by site id (zero padding dropped)."""
    import torch
    bank = torch.load(os.path.join(arms_dir, "context_%s.pt" % name), weights_only=True)
    values = bank["embeddings"].numpy()
    keep = np.abs(values).sum(axis=0) > 0
    return pd.DataFrame(values[:, keep], index=[str(site) for site in bank["site_ids"]])


def basin_block(group: str, arms_dir: str, fitted_sites: List[str]) -> pd.DataFrame:
    """
    Loads one per-basin feature group.

    :param group: ``signatures`` (the flow signatures inside the ``statics_signatures`` bank),
        ``<bank>_pca<k>`` (the first k principal components, axes fitted on ``fitted_sites``)
        or the name of any ``context_<name>.pt`` bank in ``arms_dir``.
    :type group: str
    :param arms_dir: Directory holding the prepared context banks.
    :type arms_dir: str
    :param fitted_sites: Basins a downstream model is fitted on.
    :type fitted_sites: List[str]
    :return: Features indexed by site id.
    :rtype: pd.DataFrame
    """
    if group == "signatures":
        return load_bank(arms_dir, "statics_signatures").iloc[:, -N_SIGNATURES:]
    if "_pca" in group:
        name, _, components = group.rpartition("_pca")
        bank = load_bank(arms_dir, name)
        fitted = bank.loc[bank.index.intersection(fitted_sites)].to_numpy()
        centre = fitted.mean(axis=0)
        _, _, axes = np.linalg.svd(fitted - centre, full_matrices=False)
        return pd.DataFrame((bank.to_numpy() - centre) @ axes[:int(components)].T,
                            index=bank.index)
    return load_bank(arms_dir, group)


def arm_groups(arm: str) -> tuple:
    """Returns the feature groups of an arm: a named arm, or any ``+``-joined list of groups."""
    return ARMS.get(arm) or tuple(arm.split("+"))


def features(data: pd.DataFrame, groups: tuple, arms_dir: str, fit_rows: np.ndarray):
    """
    Assembles one arm's feature matrix.

    :param data: The full table.
    :type data: pd.DataFrame
    :param groups: Feature groups of the arm.
    :type groups: tuple
    :param arms_dir: Directory holding the prepared context banks.
    :type arms_dir: str
    :param fit_rows: Boolean mask of the rows the model is fitted on (for per-basin scaling).
    :type fit_rows: np.ndarray
    :return: (float32 matrix, list flagging categorical columns).
    :rtype: tuple
    """
    blocks, categorical = [], []
    for group in groups:
        if group == "flow":
            block = data[FLOW].to_numpy(np.float32)
        elif group == "snow":
            block = data[SNOW].to_numpy(np.float32)
        elif group == "weather":
            block = data[WEATHER].to_numpy(np.float32)
        elif group == "weather_z":
            # What the neural forecasters see: each basin's weather standardized on its own
            # pre-cutoff statistics, which hides absolute levels such as the freezing point.
            # A held-out basin has such statistics too (weather needs no fitted model), so
            # they are taken from every basin's own rows whose horizon ends before the cutoff.
            stats = data[settled_rows(data)].groupby("site_id")[WEATHER].agg(["mean", "std"])
            mean = stats.xs("mean", axis=1, level=1).reindex(data.site_id).to_numpy()
            std = stats.xs("std", axis=1, level=1).reindex(data.site_id).to_numpy()
            block = ((data[WEATHER].to_numpy() - mean) / np.where(std > 0, std, 1.0))
            block = block.astype(np.float32)
        elif group.startswith("sim:") or group == "depth":
            # Simulated flow from a pretrained decoder, or the issue-day flow as a depth
            # (decoder_simulation.py); imported here because that module builds on this one.
            root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            if root not in sys.path:
                sys.path.insert(0, root)
            from experiments.catchment_foundation import decoder_simulation
            if group == "depth":
                block = decoder_simulation.depth_block(data, arms_dir)
            else:
                block = decoder_simulation.feature_block(data, arms_dir, group[4:])
        elif group == "basin_id":
            fitted = sorted(set(data.loc[fit_rows, "site_id"]))
            codes = data.site_id.map({site: i for i, site in enumerate(fitted)})
            codes = codes.to_numpy(np.float32)      # NaN for basins the model is not fitted on
            if len(fitted) <= MAX_CATEGORIES:
                block = codes[:, None]
            else:
                # The tree library caps a categorical feature at 255 levels: split the
                # identifier into two digits, which two tree splits recombine.
                base = int(np.ceil(np.sqrt(len(fitted))))
                block = np.stack([codes // base, codes % base], axis=1)
        else:
            bank = basin_block(group, arms_dir, sorted(set(data.loc[fit_rows, "site_id"])))
            block = bank.reindex(data.site_id).to_numpy(np.float32)
        blocks.append(block)
        categorical += [group == "basin_id"] * block.shape[1]
    return np.hstack(blocks), categorical


def paired_cut(reference_errors: np.ndarray, arm_errors: np.ndarray, rng: np.random.Generator,
               n_boot: int) -> Dict:
    """
    Compares two arms basin by basin.

    :param reference_errors: Summed squared error of the reference arm per basin.
    :type reference_errors: np.ndarray
    :param arm_errors: Summed squared error of the arm per basin, same order.
    :type arm_errors: np.ndarray
    :param rng: Generator of the basin bootstrap.
    :type rng: np.random.Generator
    :param n_boot: Bootstrap resamples over basins.
    :type n_boot: int
    :return: ``error_cut_pct`` (median basin's error reduction), its ``ci95`` and
        ``better_pct`` (share of basins where the arm has the lower error).
    :rtype: Dict
    """
    ratio = np.log(reference_errors / np.maximum(arm_errors, 1e-30))
    draws = [np.median(ratio[rng.integers(0, len(ratio), len(ratio))]) for _ in range(n_boot)]

    def cut(value: float) -> float:
        return float(100.0 * (1.0 - np.exp(-value)))

    return {"error_cut_pct": cut(np.median(ratio)),
            "ci95": [cut(np.percentile(draws, 2.5)), cut(np.percentile(draws, 97.5))],
            "better_pct": float(100.0 * (ratio > 0).mean())}


def score(data: pd.DataFrame, predictions: Dict, reference: str, n_boot: int = 2000,
          also_pair: Sequence[str] = ()) -> Dict:
    """
    Scores every arm per split, period and band, paired basin by basin with the reference arm.

    :param data: The full table.
    :type data: pd.DataFrame
    :param predictions: (arm, band) -> predicted band-mean change for every row.
    :type predictions: Dict
    :param reference: Arm every other arm is paired against.
    :type reference: str
    :param n_boot: Bootstrap resamples over basins, defaults to 2000.
    :type n_boot: int, optional
    :param also_pair: Further arms every arm is paired against; the comparisons are stored
        under ``paired`` -> that arm. Defaults to none.
    :type also_pair: Sequence[str], optional
    :return: Nested report: split -> period -> band -> arm -> statistics.
    :rtype: Dict
    """
    rng, extra_rng = np.random.default_rng(0), np.random.default_rng(1)
    arms = list(dict.fromkeys(arm for arm, _ in predictions))     # in the order fitted
    report = {}
    for split in ("holdout", "train"):
        for period in ("2023+", "2022"):
            mask = ((data.split == split) & (data.period == period)).to_numpy()
            if not mask.any():
                continue
            sites = sorted(data.site_id[mask].unique())
            index = data.site_id[mask].map({site: i for i, site in enumerate(sites)}).to_numpy()
            entry = {"basins": len(sites), "forecasts": int(mask.sum()), "bands": {}}
            for band in BANDS:
                truth = data[f"y_{band}"].to_numpy()[mask]
                persistence = np.bincount(index, weights=truth ** 2, minlength=len(sites))
                errors = {arm: np.bincount(index, weights=(predictions[(arm, band)][mask] - truth) ** 2,
                                           minlength=len(sites)) for arm in arms}
                entry["bands"][band] = {}
                for arm in arms:
                    skill = 100.0 * (1.0 - errors[arm] / np.maximum(persistence, 1e-30))
                    stats = {"median_basin_skill_pct": float(np.median(skill)),
                             "basins_beating_persistence_pct": float(100.0 * (skill > 0).mean())}
                    if arm != reference and reference in errors:
                        paired = paired_cut(errors[reference], errors[arm], rng, n_boot)
                        stats.update(error_cut_vs_reference_pct=paired["error_cut_pct"],
                                     error_cut_ci95=paired["ci95"],
                                     better_than_reference_pct=paired["better_pct"])
                    for other in also_pair:
                        if other != arm and other in errors:
                            stats.setdefault("paired", {})[other] = paired_cut(
                                errors[other], errors[arm], extra_rng, n_boot)
                    entry["bands"][band][arm] = stats
            report.setdefault(split, {})[period] = entry
    return report


def settled_rows(data: pd.DataFrame) -> np.ndarray:
    """
    Marks the rows whose whole horizon lies before the cutoff.

    A row issued in the last days before the cutoff has a target that reaches past it; such
    rows are left out of fitting so that nothing after the cutoff is trained on.

    :param data: Forecast table with ``date`` (the issue day).
    :type data: pd.DataFrame
    :return: Boolean array, True where the last target day is before the cutoff.
    :rtype: np.ndarray
    """
    horizon = max(last for _, last in BANDS.values())
    last_issue = pd.Timestamp(CUTOFF) - pd.Timedelta(days=horizon)
    return (pd.to_datetime(data["date"]) < last_issue).to_numpy()


def run(data_path: str, arms_dir: str, arm_names: List[str], fit_subsample: int, seed: int,
        output: Optional[str], reference: Optional[str] = None,
        also_pair: Sequence[str] = ()) -> Dict:
    """Fits every requested arm and prints and returns the scores."""
    from sklearn.ensemble import HistGradientBoostingRegressor
    data = pd.read_pickle(data_path)
    fit_rows = ((data.period == "fit") & (data.split == "train")).to_numpy()
    fit_index = np.flatnonzero(fit_rows & settled_rows(data))[::fit_subsample]
    print("%d basins; fitting on %d rows of %d training basins"
          % (data.site_id.nunique(), len(fit_index), data.site_id[fit_rows].nunique()), flush=True)
    predictions, started = {}, time.time()
    for arm in arm_names:
        matrix, categorical = features(data, arm_groups(arm), arms_dir, fit_rows)
        for band in BANDS:
            model = HistGradientBoostingRegressor(
                max_iter=300, learning_rate=0.06, max_leaf_nodes=31, min_samples_leaf=40,
                l2_regularization=1.0, random_state=seed,
                categorical_features=categorical if any(categorical) else None)
            # Clipped targets: a handful of floods would otherwise own the squared error.
            model.fit(matrix[fit_index], data[f"y_{band}"].to_numpy()[fit_index].clip(-3, 3))
            predictions[(arm, band)] = model.predict(matrix)
        print("fitted %-40s %3d features  %4.0f s" % (arm, matrix.shape[1], time.time() - started),
              flush=True)
        del matrix
    reference = reference or (REFERENCE if REFERENCE in arm_names else arm_names[0])
    report = {"data": os.path.abspath(data_path), "reference": reference, "seed": seed,
              "scores": score(data, predictions, reference, also_pair=also_pair)}
    for split, periods in report["scores"].items():
        for period, entry in periods.items():
            print("\n%s basins, %s: %d basins, %d forecasts (median skill %% / basins beating "
                  "persistence %% / error cut vs %s)" % (
                      "held-out" if split == "holdout" else "trained-on", period,
                      entry["basins"], entry["forecasts"], reference))
            print("  %-40s" % "arm" + "".join("%32s" % band for band in BANDS))
            for arm in arm_names:
                line = "  %-40s" % arm
                for band in BANDS:
                    stats = entry["bands"][band][arm]
                    paired = " " * 16
                    if "error_cut_vs_reference_pct" in stats:
                        paired = "  %+5.1f (%+.0f,%+.0f)" % (
                            stats["error_cut_vs_reference_pct"], *stats["error_cut_ci95"])
                    line += "  %5.1f /%3.0f%s" % (stats["median_basin_skill_pct"],
                                                  stats["basins_beating_persistence_pct"], paired)
                print(line)
    held_out = report["scores"].get("holdout", {}).get("2023+")
    for other in also_pair if held_out else ():
        print("\nheld-out basins, 2023+: error cut vs %s (95%% interval) / basins better %%" % other)
        for arm in arm_names:
            cells = [held_out["bands"][band][arm].get("paired", {}).get(other) for band in BANDS]
            if all(cells):
                print("  %-40s" % arm + "".join("  %+5.1f (%+.0f,%+.0f) /%3.0f" % (
                    cell["error_cut_pct"], *cell["ci95"], cell["better_pct"]) for cell in cells))
    if output:
        os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
        with open(output, "w") as file:
            json.dump(report, file, indent=1)
    return report


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    builder = commands.add_parser("build", help="build the daily feature table of a manifest")
    builder.add_argument("--manifest", required=True)
    builder.add_argument("--out", required=True)
    builder.add_argument("--fit-stride", type=int, default=3)
    builder.add_argument("--eval-stride", type=int, default=7)
    runner = commands.add_parser("run", help="fit the arms and score them")
    runner.add_argument("--data", required=True)
    runner.add_argument("--arms-dir", required=True,
                        help="Directory of context_<name>.pt banks from prepare_context_ablation.py")
    runner.add_argument("--arms", nargs="+", default=list(ARMS),
                        help="Named arms, or any +-joined list of feature groups and bank names")
    runner.add_argument("--fit-subsample", type=int, default=1,
                        help="Keep every n-th fitting row (large cohorts)")
    runner.add_argument("--seed", type=int, default=0)
    runner.add_argument("--reference", default=None,
                        help="Arm every other arm is paired against (default: %s)" % REFERENCE)
    runner.add_argument("--also-pair", nargs="*", default=[],
                        help="Further arms every arm is compared with, basin by basin")
    runner.add_argument("--output", default=None)
    args = parser.parse_args()
    if args.command == "build":
        build(args.manifest, args.out, args.fit_stride, args.eval_stride)
    else:
        run(args.data, args.arms_dir, args.arms, args.fit_subsample, args.seed, args.output,
            reference=args.reference, also_pair=args.also_pair)


if __name__ == "__main__":
    main()
