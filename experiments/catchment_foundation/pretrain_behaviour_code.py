"""
Pretrains a general catchment code on the daily record and writes it as a context bank.

Step 2 of ``notes/REPRESENTATION_PLAN_2026-11-12.md``. The reader of the gauged record sees a few
water years of a basin's flow and weather, in physical units, and writes a short code. A decoder
shared by all basins must reproduce the flow of another water year from that year's weather and
the code (Flow Forecast ``behaviour_pretrain``). Nothing after the cutoff is read, held-out
basins are never trained on, and their codes come from their own pre-cutoff record.

    cache   read every basin's hourly file once and keep the pre-cutoff daily record
    train   pretrain on the training basins and write ``context_<name>.pt`` for every basin

    python -m experiments.catchment_foundation.pretrain_behaviour_code cache --arms-dir <arms dir>
    python -m experiments.catchment_foundation.pretrain_behaviour_code train \
        --arms-dir <arms dir> --name behaviour_s0 --seed 0

Score the bank with ``representation_benchmark.py run`` and ``tabular_forecast_baseline.py``.
Flow Forecast is imported from ``FF_REPO`` or ``PYTHONPATH`` (branch ``behaviour-pretraining``).
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

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


class WaterYearDataset(Dataset):
    """
    Serves, per basin, a few water years for the reader and another one for the decoder.

    ``history`` has shape (n_read, 365, 7): log flow (zero where missing), a flag for a valid
    flow value and the five weather channels. ``drivers`` covers the spin-up and the decoded
    year, shape (455, 5); ``target`` is the decoded year's log flow with NaN over the spin-up.
    """

    def __init__(self, daily: Dict[str, pd.DataFrame], sites: List[str], n_read: int = 3,
                 draws: int = 1, seed: int = 0, fixed: bool = False,
                 aux: Optional[pd.DataFrame] = None):
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
        """
        self.years = {site: water_years(daily[site]) for site in sites if site in daily}
        self.site_ids = [site for site in sites if len(self.years.get(site, [])) >= 2]
        self.daily, self.n_read, self.draws, self.fixed = daily, n_read, draws, fixed
        self.seed, self.aux = seed, aux
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
        return item


class ReadYearsDataset(Dataset):
    """
    Serves ``n_read`` water years of each basin, for writing its code.

    ``group`` selects which years: group g starts ``g * n_read`` years into the basin's list and
    wraps around, so successive groups cover the whole record.
    """

    def __init__(self, daily: Dict[str, pd.DataFrame], sites: List[str], n_read: int = 3):
        self.source = WaterYearDataset(daily, [], n_read=n_read)
        self.years = {site: water_years(daily[site]) for site in sites if site in daily}
        self.site_ids = [site for site in sites if self.years.get(site)]
        self.n_read, self.group = n_read, 0

    def __len__(self) -> int:
        return len(self.site_ids)

    def read_years(self, site: str) -> List[pd.Timestamp]:
        """The water years of the current group."""
        years = self.years[site]
        return [years[(self.group * self.n_read + step) % len(years)]
                for step in range(self.n_read)]

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        site = self.site_ids[index]
        return {"history": torch.from_numpy(np.stack([self.source.member(site, year)
                                                      for year in self.read_years(site)]))}


def write_codes(encoder, daily: Dict[str, pd.DataFrame], sites: List[str], n_read: int,
                groups: int, batch_size: int = 64, device: str = "cpu") -> pd.DataFrame:
    """
    Writes every basin's code as the mean over groups of its water years.

    :param encoder: The pretrained reader.
    :type encoder: flood_forecast.meta_models.multimodal_encoder.MultiModalEncoder
    :param daily: Site id -> daily frame.
    :type daily: Dict[str, pd.DataFrame]
    :param sites: Basins to encode (those without a usable water year are left out).
    :type sites: List[str]
    :param n_read: Water years per group.
    :type n_read: int
    :param groups: Number of groups averaged.
    :type groups: int
    :return: Codes indexed by site id.
    :rtype: pd.DataFrame
    """
    from flood_forecast.meta_models.contrastive_train import extract_embeddings
    reader = ReadYearsDataset(daily, sites, n_read=n_read)
    total = None
    for group in range(groups):
        reader.group = group
        _, codes = extract_embeddings(encoder, reader, batch_size=batch_size, device=device)
        total = codes if total is None else total + codes
    return pd.DataFrame((total / groups).numpy(), index=reader.site_ids)


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


def build_models(code_dim: int, dim: int, depth: int, hidden: int, dropout: float):
    """Builds the gauged-record reader and the decoder from Flow Forecast parts."""
    from flood_forecast.meta_models.behaviour_pretrain import DriverConditionedDecoder
    from flood_forecast.meta_models.multimodal_encoder import (MultiModalEncoder,
                                                               PanelSequenceEncoder)
    history = PanelSequenceEncoder(7, YEAR_DAYS, dim=dim, depth=depth, heads=4, dim_head=dim // 4,
                                   mlp_dim=2 * dim, dropout=dropout, token_kernel=14,
                                   token_stride=7)
    encoder = MultiModalEncoder({"history": history}, dim=dim, embedding_dim=code_dim,
                                fusion="concat", fusion_head="linear",
                                sequence_modalities=["history"])
    return encoder, DriverConditionedDecoder(5, code_dim, hidden_dim=hidden)


def train(arms_dir: str, name: str, seed: int = 0, epochs: int = 60, code_dim: int = 32,
          dim: int = 64, depth: int = 2, hidden: int = 64, dropout: float = 0.1,
          n_read: int = 3, draws: int = 8, batch_size: int = 64, lr: float = 1e-3,
          weight_decay: float = 1e-4, aux_weight: float = 0.0, validation_fraction: float = 0.15,
          read_passes: int = 6, device: str = "cpu", cache: Optional[Dict] = None,
          early: Optional[pd.DataFrame] = None, verbose: bool = True) -> Dict:
    """
    Pretrains the code on the training basins and writes ``context_<name>.pt`` for all basins.

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
    :param cache: The daily cache, defaults to None (read from ``arms_dir``).
    :type cache: Dict, optional
    :param early: Pre-cutoff behaviour numbers, defaults to None (read from ``benchmark.pkl``).
    :type early: pd.DataFrame, optional
    :return: The run summary, also written to ``reports/pretrain_<name>.json``.
    :rtype: Dict
    """
    from flood_forecast.meta_models.behaviour_pretrain import pretrain_behaviour
    cache = cache or pd.read_pickle(os.path.join(arms_dir, CACHE))
    daily, split = cache["daily"], cache["split"]
    sites = list(split)
    train_sites = sorted(site for site in sites if split[site] == "train")
    torch.manual_seed(seed)
    order = np.random.default_rng(seed).permutation(len(train_sites))
    n_validation = int(round(validation_fraction * len(train_sites)))
    validation_sites = sorted(train_sites[index] for index in order[:n_validation])
    fit_sites = sorted(train_sites[index] for index in order[n_validation:])

    aux, aux_head = None, None
    if aux_weight > 0:
        if early is None:
            early = pd.read_pickle(os.path.join(arms_dir, "benchmark.pkl"))["early"]
        aux = standardized_aux(early, fit_sites).reindex(sites)
        aux_head = torch.nn.Linear(code_dim, len(AUX))
    fit = WaterYearDataset(daily, fit_sites, n_read=n_read, draws=draws, seed=seed, aux=aux)
    validation = None
    if validation_sites:
        validation = WaterYearDataset(daily, validation_sites, n_read=n_read, draws=4, seed=seed,
                                      fixed=True, aux=aux)
    encoder, decoder = build_models(code_dim, dim, depth, hidden, dropout)
    started = time.time()
    history = pretrain_behaviour(encoder, decoder, fit, epochs=epochs, batch_size=batch_size,
                                 lr=lr, weight_decay=weight_decay, device=device,
                                 validation_dataset=validation, aux_head=aux_head,
                                 aux_weight=aux_weight, verbose=verbose)
    minutes = (time.time() - started) / 60.0

    codes = write_codes(encoder, daily, sites, n_read, read_passes, batch_size, device)
    read_sites = list(codes.index)
    fitted = codes.loc[codes.index.intersection(train_sites)]
    scaled = ((codes - fitted.mean()) / fitted.std().replace(0.0, 1.0)).clip(-8.0, 8.0)
    # A basin without a usable water year gets the training mean, so every bank covers the same
    # basins and comparisons stay on matched cohorts.
    scaled = scaled.reindex(sites).fillna(0.0)
    torch.save({"site_ids": list(scaled.index),
                "embeddings": torch.tensor(scaled.to_numpy(), dtype=torch.float32)},
               os.path.join(arms_dir, "context_%s.pt" % name))
    torch.save({"encoder": encoder.state_dict(), "decoder": decoder.state_dict()},
               os.path.join(arms_dir, "behaviour_%s.pt" % name))
    summary = {"name": name, "seed": seed, "epochs": epochs, "code_dim": code_dim, "dim": dim,
               "depth": depth, "hidden": hidden, "dropout": dropout, "n_read": n_read,
               "draws": draws, "batch_size": batch_size, "lr": lr, "weight_decay": weight_decay,
               "aux_weight": aux_weight, "fit_basins": len(fit.site_ids),
               "validation_basins": len(validation.site_ids) if validation else 0,
               "basins_with_code": len(read_sites), "minutes": round(minutes, 2),
               "best_epoch": history.get("best_epoch", [epochs])[0],
               "series_loss": history["series"], "validation_loss": history.get("validation")}
    os.makedirs(os.path.join(arms_dir, "reports"), exist_ok=True)
    with open(os.path.join(arms_dir, "reports", "pretrain_%s.json" % name), "w") as file:
        json.dump(summary, file, indent=2)
    return summary


def main() -> None:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    cacher = commands.add_parser("cache", help="store every basin's pre-cutoff daily record")
    cacher.add_argument("--arms-dir", required=True)
    cacher.add_argument("--manifest", default=None, help="defaults to <arms dir>/manifest_none.json")
    cacher.add_argument("--workers", type=int, default=6)
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
    args = parser.parse_args()
    if args.command == "cache":
        manifest = args.manifest or os.path.join(args.arms_dir, "manifest_none.json")
        build_cache(manifest, args.arms_dir, args.workers)
        return
    options = {key: value for key, value in vars(args).items() if key != "command"}
    summary = train(**options)
    print(json.dumps({key: value for key, value in summary.items()
                      if key not in ("series_loss", "validation_loss")}, indent=2))


if __name__ == "__main__":
    main()
