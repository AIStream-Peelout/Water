"""
How well can a basin be described from a short gauge record?

A new gauge has months of flow, not years. Two ways of describing a basin from such a record
are trained and scored here in exactly the same way:

``record``
    the learned reader of behaviour pretraining, shown the weather and the flow of the
    record's days and nothing else;
``numbers``
    hand-built statistics computed from the same days (the daily counterparts of the numbers in
    ``representation_benchmark.behaviour`` and ``embedding_probes.flow_signatures``), read by a
    small tabular reader together with flags for the ones the record cannot support, the
    record's length, the share of its days with flow and the season it ends in;
``record_weather``
    a control: the learned reader with the weather of all three years before the end day. In
    this data weather exists only where the gauge reported, so those years also show when the
    gauge runs; with a record of zero months this reader has the weather alone.

Each conditions a Flow Forecast ``DriverConditionedDecoder`` that has to reproduce the flow of a
water year outside the record from that year's weather. A record is the ``months`` before an
end day; in training both are drawn at random, so a record can start in any season.

Commands
    train      fit one description (reader and decoder) on records of random length
    simulate   describe every basin from the months before an end day and simulate its record
    skill      score those simulations against observed flow on held-out basins

Run from the Water root with Flow Forecast on the path, for example::

    python -m experiments.catchment_foundation.short_record train \
        --arms-dir experiments/catchment_foundation/calibration/<cohort> \
        --name short_record_s0 --kind record --seed 0
"""
import argparse
import json
import os
import sys
import time
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

from experiments.catchment_foundation import decoder_simulation as simulation
from experiments.catchment_foundation import pretrain_behaviour_code as behaviour
from experiments.catchment_foundation import representation_benchmark as benchmark

if os.environ.get("FF_REPO"):
    sys.path.insert(0, os.environ["FF_REPO"])

MONTHS = (1, 3, 6, 12, 24, 36)
MONTH_DAYS = 30.44
BLOCKS = 3                            # 365-day blocks the record reader sees
MIN_VISIBLE_SHARE = 0.5               # of a training record's days must hold a flow value
EXTRAS = 4                            # record length, share of days with flow, season of the end day
KINDS = ("record", "numbers", "record_weather")
NUMBERS = ["log_runoff_mm", "log_runoff_ratio", "log_aridity", "snow_fraction",
           "zero_flow_fraction", "recession_rate", "storm_response", "half_flow_day", "cv",
           "rb_flashiness", "bfi", "fdc_slope", "high_low_ratio", "melt_share", "low_flow",
           "flood_peak"]


def visible_days(months: float) -> int:
    """The number of days in a record of ``months`` months."""
    return int(round(MONTH_DAYS * months))


def read_blocks(daily: pd.DataFrame, last: int, months: float, all_weather: bool = False
                ) -> np.ndarray:
    """
    Builds what the record reader sees of a basin whose record ends on a given day.

    Three 365-day blocks end on that day. Flow is given only on the last ``months`` months,
    the record, and marked unknown before it. So is the weather, unless ``all_weather`` is on.

    :param daily: The basin's daily frame without gaps (``q_mm``, ``p``, ``t``, ``pet``).
    :type daily: pd.DataFrame
    :param last: Position of the record's last day in ``daily``.
    :type last: int
    :param months: Length of the record in months; zero shows no flow at all.
    :type months: float
    :param all_weather: Give the weather of all three blocks instead of the record's days
        only, defaults to False.
    :type all_weather: bool, optional
    :return: Array of shape (3, 365, 7): log flow (zero where unknown), a flag for a known
        flow, and the five weather channels (zero outside the record unless ``all_weather``).
        Days before the frame starts are zero.
    :rtype: np.ndarray
    """
    length = BLOCKS * behaviour.YEAR_DAYS
    first = last - length + 1
    block = daily.iloc[max(first, 0):last + 1]
    flow = behaviour.flow_channel(block)
    positions = np.arange(max(first, 0), last + 1)
    inside = positions > last - visible_days(months)
    known = np.isfinite(flow) & inside
    weather = behaviour.weather_channels(block)
    if not all_weather:
        weather = np.where(inside[:, None], weather, 0.0)
    rows = np.column_stack([np.where(known, flow, 0.0), known.astype(np.float32),
                            weather]).astype(np.float32)
    padded = np.zeros((length, behaviour.HISTORY_CHANNELS), dtype=np.float32)
    padded[length - len(rows):] = rows
    return padded.reshape(BLOCKS, behaviour.YEAR_DAYS, behaviour.HISTORY_CHANNELS)


def short_numbers(record: pd.DataFrame) -> Dict[str, float]:
    """
    Computes the hand-built numbers a record of any length can support.

    The definitions follow ``representation_benchmark.behaviour`` and
    ``embedding_probes.flow_signatures`` on daily values. A number the record cannot support
    (too few days, no full year, no recessions) is NaN.

    :param record: Daily frame of the record's days (``q_mm``, ``p``, ``pet`` in mm per day,
        ``t`` in deg C at basin elevation).
    :type record: pd.DataFrame
    :return: Name -> value for every name in ``NUMBERS``.
    :rtype: Dict[str, float]
    """
    out = {name: float("nan") for name in NUMBERS}
    both = record.dropna(subset=["q_mm", "p", "t"])
    if len(both) < 10 or not both.q_mm.sum() > 0:
        return out
    flow, rain, temp = record.q_mm, record.p, record.t
    mean = float(both.q_mm.mean())
    out["log_runoff_mm"] = float(np.log(mean + 1e-3))
    out["zero_flow_fraction"] = float((both.q_mm <= 1e-4).mean())
    if both.p.sum() > 0:
        out["log_runoff_ratio"] = float(np.log(both.q_mm.sum() / both.p.sum() + 1e-3))
        out["snow_fraction"] = float(both.p[both.t <= benchmark.SNOW_BELOW_C].sum() / both.p.sum())
        demand = both.dropna(subset=["pet"])
        if len(demand) >= 10 and demand.p.sum() > 0:
            out["log_aridity"] = float(np.log(demand.pet.sum() / demand.p.sum() + 1e-3))
    fall = -np.log(flow.where(flow > 1e-4)).diff()
    falling = fall > 0
    dry = rain.rolling(3, min_periods=3).sum() < 1.0
    steady = falling & falling.shift(1, fill_value=False) & falling.shift(2, fill_value=False) & dry
    if steady.sum() >= 8:
        out["recession_rate"] = float(fall[steady].median())
    warm_rain = rain.where(temp > benchmark.WARM_ABOVE_C)
    wet = warm_rain[warm_rain >= 2.0]
    if len(wet) >= 3:
        # The wettest warm days: the top 2% of a long record, never fewer than three, so a
        # record of a few months can still show how flow answers rain.
        chosen = wet.nlargest(max(3, int(np.ceil(0.02 * warm_rain.notna().sum()))))
        peak_ahead = flow[::-1].rolling(4, min_periods=4).max()[::-1]
        rise = ((peak_ahead - flow.shift(1)).clip(lower=0) / warm_rain)[chosen.index].dropna()
        if len(rise) >= 3:
            out["storm_response"] = float(np.log(rise.median() + 1e-3))
    # Timing: the day of the water year (from 1 October) by which half the flow has passed,
    # over the record's last 365 days whichever month they start in.
    year = flow.iloc[-behaviour.YEAR_DAYS:]
    if year.notna().sum() >= 330 and year.sum() > 0:
        start = pd.to_datetime({"year": year.index.year - (year.index.month < 10), "month": 10, "day": 1})
        day_of_water_year = (year.index - pd.DatetimeIndex(start)).days + 1
        ordered = year.fillna(0.0).to_numpy()[np.argsort(day_of_water_year, kind="stable")]
        passed = np.cumsum(ordered)
        out["half_flow_day"] = float(np.sort(day_of_water_year)[(passed >= 0.5 * passed[-1]).argmax()])
    valid = flow.dropna().clip(lower=0.0)
    out["cv"] = float(valid.std() / mean) if len(valid) > 1 and mean > 0 else float("nan")
    change = flow.diff()
    if flow[change.notna()].sum() > 0:
        out["rb_flashiness"] = float(change.abs().sum() / flow[change.notna()].sum())
    if len(valid) >= 30:
        baseflow = flow.rolling(30, center=True, min_periods=10).min()
        out["bfi"] = float((baseflow / flow.where(flow > 0)).clip(0, 1).mean())
        quantiles = valid.quantile([0.05, 0.33, 0.66, 0.95])
        epsilon = 1e-2 * mean
        out["fdc_slope"] = float(np.log(quantiles[0.33] + epsilon) - np.log(quantiles[0.66] + epsilon))
        out["high_low_ratio"] = float(np.log((quantiles[0.95] + epsilon) / (quantiles[0.05] + epsilon)))
        out["flood_peak"] = float(np.log(valid.max() + 1e-4))
    monthly = flow.groupby(flow.index.month).mean()
    if monthly.notna().sum() == 12 and monthly.sum() > 0:
        out["melt_share"] = float(monthly.reindex([4, 5, 6, 7]).sum() / monthly.sum())
    weekly = flow.rolling(7, min_periods=7).mean().dropna()
    if len(weekly) >= 60:
        out["low_flow"] = float(np.log(weekly.min() + 1e-3))
    return out


def number_scale(daily: Dict[str, pd.DataFrame], sites: Sequence[str]) -> pd.DataFrame:
    """
    The mean and spread of each hand-built number over basins' whole records.

    :param daily: Daily records by site id.
    :type daily: Dict[str, pd.DataFrame]
    :param sites: The basins the statistics come from (the training basins).
    :type sites: Sequence[str]
    :return: Frame indexed by ``NUMBERS`` with columns ``mean`` and ``std``.
    :rtype: pd.DataFrame
    """
    table = pd.DataFrame({site: short_numbers(daily[site]) for site in sites if site in daily}).T
    return pd.DataFrame({"mean": table[NUMBERS].mean(), "std": table[NUMBERS].std().replace(0.0, 1.0)})


def number_vector(record: pd.DataFrame, scale: pd.DataFrame, months: float) -> np.ndarray:
    """
    The tabular reader's input for a record: standardized numbers, a flag for each one, and
    what the record reader can also see: the record's length, the share of its days with a
    flow value and the season it ends in.

    :param record: The record's days.
    :type record: pd.DataFrame
    :param scale: Output of :func:`number_scale`.
    :type scale: pd.DataFrame
    :param months: Length of the record in months.
    :type months: float
    :return: Array of shape (2 * len(NUMBERS) + EXTRAS,); an unsupported number is zero with
        flag zero.
    :rtype: np.ndarray
    """
    values = pd.Series(short_numbers(record))[NUMBERS]
    scaled = ((values - scale["mean"]) / scale["std"]).clip(-8.0, 8.0)
    angle = 2.0 * np.pi * record.index[-1].dayofyear / 365.25 if len(record) else 0.0
    extras = [np.log1p(months) / np.log1p(max(MONTHS)),
              float(record.q_mm.notna().mean()) if len(record) else 0.0,
              np.sin(angle), np.cos(angle)]
    return np.concatenate([scaled.fillna(0.0).to_numpy(np.float32),
                           values.notna().to_numpy(np.float32),
                           np.asarray(extras, dtype=np.float32)])


class ShortRecordDataset(Dataset):
    """
    Serves a basin through a record of some length and a water year to decode outside it.

    ``history`` is :func:`read_blocks` of the record, ``statics`` is :func:`number_vector` of
    the same days, ``drivers`` and ``target`` are the decoded year as in
    ``pretrain_behaviour_code.WaterYearDataset``. The decoded year never overlaps the record:
    a record too long to leave the basin a year outside it is shortened until one is left.
    """

    def __init__(self, daily: Dict[str, pd.DataFrame], sites: List[str], scale: pd.DataFrame,
                 months: Sequence[float] = MONTHS, draws: int = 8, seed: int = 0,
                 fixed: bool = False, aux: Optional[pd.DataFrame] = None,
                 inputs: Sequence[str] = ("history", "statics"), all_weather: bool = False):
        """
        Initializes the dataset.

        :param daily: Daily records by site id (no gaps in the index).
        :type daily: Dict[str, pd.DataFrame]
        :param sites: Basins to serve; one without two usable water years is left out.
        :type sites: List[str]
        :param scale: Output of :func:`number_scale`.
        :type scale: pd.DataFrame
        :param months: Record lengths drawn from, defaults to ``MONTHS``.
        :type months: Sequence[float], optional
        :param draws: Items per basin and pass, defaults to 8.
        :type draws: int, optional
        :param seed: Seed of the draws, defaults to 0.
        :type seed: int, optional
        :param fixed: Serve the same items on every pass (for validation), defaults to False.
        :type fixed: bool, optional
        :param aux: Side targets indexed by site id, defaults to None.
        :type aux: pd.DataFrame, optional
        :param inputs: Which descriptions to build per item (``history``, ``statics``); a run
            needs only its own reader's. Defaults to both.
        :type inputs: Sequence[str], optional
        :param all_weather: See :func:`read_blocks`, defaults to False.
        :type all_weather: bool, optional
        """
        self.inputs, self.all_weather = tuple(inputs), all_weather
        self.years = {site: behaviour.water_years(daily[site]) for site in sites if site in daily}
        self.site_ids = [site for site in sites if len(self.years.get(site, [])) >= 2]
        self.daily, self.scale = daily, scale
        self.months = tuple(sorted(months, reverse=True))
        self.draws, self.seed, self.fixed, self.aux = draws, seed, fixed, aux
        self.rng = np.random.default_rng(seed)
        self.observed = {site: daily[site].q_mm.notna().to_numpy() for site in self.site_ids}
        self.flow_days = {site: np.flatnonzero(self.observed[site]) for site in self.site_ids}

    def __len__(self) -> int:
        return len(self.site_ids) * self.draws

    def record_end(self, site: str, days: int, rng: np.random.Generator) -> int:
        """Draws a last day whose record of ``days`` days is mostly observed (best of 20)."""
        known, observed = self.flow_days[site], self.observed[site]
        candidates = known[known >= days - 1]
        if not len(candidates):
            candidates = known
        best, best_share = int(candidates[-1]), -1.0
        for _ in range(20):
            last = int(candidates[int(rng.integers(len(candidates)))])
            share = observed[max(last - days + 1, 0):last + 1].sum() / max(days, 1)
            if share > best_share:
                best, best_share = last, share
            if share >= MIN_VISIBLE_SHARE:
                break
        return best

    def choose(self, index: int) -> Tuple[str, float, int, pd.Timestamp]:
        """Picks the basin, the record's length and last day, and the decoded year."""
        site = self.site_ids[index % len(self.site_ids)]
        rng = np.random.default_rng([self.seed, index]) if self.fixed else self.rng
        frame, years = self.daily[site], self.years[site]
        wanted = self.months[int(rng.integers(len(self.months)))]
        for months in [wanted] + [shorter for shorter in self.months if shorter < wanted]:
            days = visible_days(months)
            last = self.record_end(site, days, rng)
            if days == 0:
                return site, months, last, years[int(rng.integers(len(years)))]
            start, end = frame.index[max(last - days + 1, 0)], frame.index[last]
            outside = [year for year in years
                       if year > end or year + pd.Timedelta(days=behaviour.YEAR_DAYS - 1) < start]
            if outside:
                return site, months, last, outside[int(rng.integers(len(outside)))]
        raise RuntimeError("basin %s has no water year outside a record of %g months"
                           % (site, self.months[-1]))

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        site, months, last, decoded = self.choose(index)
        frame = self.daily[site]
        record = frame.iloc[max(last - visible_days(months) + 1, 0):last + 1]
        block = frame[decoded - pd.Timedelta(days=behaviour.SPIN_UP_DAYS):
                      decoded + pd.Timedelta(days=behaviour.YEAR_DAYS - 1)]
        target = behaviour.flow_channel(block)
        target[:behaviour.SPIN_UP_DAYS] = np.nan
        item = {"drivers": torch.from_numpy(behaviour.weather_channels(block)),
                "target": torch.from_numpy(target)}
        if "history" in self.inputs:
            item["history"] = torch.from_numpy(read_blocks(frame, last, months, self.all_weather))
        if "statics" in self.inputs:
            item["statics"] = torch.from_numpy(number_vector(record, self.scale, months))
        if self.aux is not None:
            item["aux"] = torch.tensor(self.aux.loc[site].to_numpy(dtype=np.float32))
        return item


def build_models(kind: str, code_dim: int = 32, dim: int = 64, depth: int = 2, hidden: int = 64,
                 dropout: float = 0.1):
    """
    Builds the reader of one description and its decoder from Flow Forecast parts.

    :param kind: ``record`` or ``record_weather`` (the learned reader of the daily record) or
        ``numbers`` (a tabular reader of the hand-built numbers).
    :type kind: str
    :param code_dim: Width of the code, defaults to 32.
    :type code_dim: int, optional
    :param dim: Width of the reader, defaults to 64.
    :type dim: int, optional
    :param depth: Transformer blocks of the record reader, defaults to 2.
    :type depth: int, optional
    :param hidden: Recurrent state width of the decoder, defaults to 64.
    :type hidden: int, optional
    :param dropout: Reader dropout, defaults to 0.1.
    :type dropout: float, optional
    :return: The reader and the decoder.
    :rtype: Tuple[torch.nn.Module, torch.nn.Module]
    """
    if kind not in KINDS:
        raise ValueError("kind must be one of %s but got %s" % (KINDS, kind))
    if kind == "numbers":
        return behaviour.build_models(code_dim, dim, depth, hidden, dropout,
                                      n_statics=2 * len(NUMBERS) + EXTRAS, gauged=False)
    return behaviour.build_models(code_dim, dim, depth, hidden, dropout)


def losses_by_length(reader, decoder, daily: Dict[str, pd.DataFrame], sites: List[str],
                     scale: pd.DataFrame, months: Sequence[float], seed: int,
                     all_weather: bool = False, batch_size: int = 64) -> Dict[str, float]:
    """
    Scores the decoder on basins held out of fitting, one record length at a time.

    :param reader: The trained reader.
    :type reader: torch.nn.Module
    :param decoder: The trained decoder.
    :type decoder: torch.nn.Module
    :param daily: Daily records by site id.
    :type daily: Dict[str, pd.DataFrame]
    :param sites: The validation basins.
    :type sites: List[str]
    :param scale: Output of :func:`number_scale`.
    :type scale: pd.DataFrame
    :param months: Record lengths to score.
    :type months: Sequence[float]
    :param seed: Seed of the fixed draws.
    :type seed: int
    :param all_weather: See :func:`read_blocks`, defaults to False.
    :type all_weather: bool, optional
    :param batch_size: Items per forward pass, defaults to 64.
    :type batch_size: int, optional
    :return: Record length in months -> series loss, averaged over items.
    :rtype: Dict[str, float]
    """
    from flood_forecast.meta_models.behaviour_pretrain import behaviour_step
    reader.eval(), decoder.eval()
    result = {}
    with torch.no_grad():
        for length in months:
            dataset = ShortRecordDataset(daily, sites, scale, months=(length,), draws=4, seed=seed,
                                         fixed=True, inputs=tuple(reader.encoders),
                                         all_weather=all_weather)
            loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=False)
            total, count = 0.0, 0
            for batch in loader:
                step = behaviour_step(reader, decoder, {name: batch[name] for name in reader.encoders},
                                      batch["drivers"], batch["target"])
                total += step["series"].item() * len(batch["target"])
                count += len(batch["target"])
            result["%g" % length] = total / max(count, 1)
    return result


def train(arms_dir: str, name: str, kind: str = "record", seed: int = 0, epochs: int = 60,
          code_dim: int = 32, dim: int = 64, depth: int = 2, hidden: int = 64,
          dropout: float = 0.1, draws: int = 8, batch_size: int = 64, lr: float = 1e-3,
          weight_decay: float = 1e-4, aux_weight: float = 1.0, validation_fraction: float = 0.15,
          months: Optional[Sequence[float]] = None, device: str = "cpu",
          verbose: bool = True) -> Dict:
    """
    Fits one description on records of random length and end day.

    :param arms_dir: Cohort directory holding the pre-cutoff daily cache.
    :type arms_dir: str
    :param name: Run name: ``short_<name>.pt`` and ``reports/short_<name>.json``.
    :type name: str
    :param kind: ``record``, ``numbers`` or ``record_weather`` (see the module docstring).
    :type kind: str
    :param seed: Seed of the validation basins, the draws and the weights, defaults to 0.
    :type seed: int, optional
    :param epochs: Passes, defaults to 60.
    :type epochs: int, optional
    :param code_dim: Width of the code, defaults to 32.
    :type code_dim: int, optional
    :param dim: Width of the reader, defaults to 64.
    :type dim: int, optional
    :param depth: Transformer blocks of the record reader, defaults to 2.
    :type depth: int, optional
    :param hidden: Recurrent state width, defaults to 64.
    :type hidden: int, optional
    :param dropout: Reader dropout, defaults to 0.1.
    :type dropout: float, optional
    :param draws: Items per basin and pass, defaults to 8.
    :type draws: int, optional
    :param batch_size: Items per step, defaults to 64.
    :type batch_size: int, optional
    :param lr: AdamW learning rate, defaults to 1e-3.
    :type lr: float, optional
    :param weight_decay: AdamW weight decay, defaults to 1e-4.
    :type weight_decay: float, optional
    :param aux_weight: Weight of the side targets (the whole-record behaviour numbers of
        ``pretrain_behaviour_code.AUX``); zero turns them off. Defaults to 1.0.
    :type aux_weight: float, optional
    :param validation_fraction: Share of the training basins held out to pick the epoch.
    :type validation_fraction: float, optional
    :param months: Record lengths drawn from; None takes ``MONTHS``, with a record of zero
        months added for ``record_weather``. Defaults to None.
    :type months: Sequence[float], optional
    :param device: Torch device, defaults to "cpu".
    :type device: str, optional
    :param verbose: Print one line per epoch, defaults to True.
    :type verbose: bool, optional
    :return: The run's report.
    :rtype: Dict
    """
    from flood_forecast.meta_models.behaviour_pretrain import pretrain_behaviour
    cache = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))
    daily, split = cache["daily"], cache["split"]
    _, fit_sites, validation_sites = behaviour.split_basins(split, seed, validation_fraction)
    all_weather = kind == "record_weather"
    if months is None:
        months = ((0,) if all_weather else ()) + MONTHS
    torch.manual_seed(seed)
    scale = number_scale(daily, fit_sites)
    aux, aux_head = None, None
    if aux_weight > 0:
        early = pd.read_pickle(os.path.join(arms_dir, "benchmark.pkl"))["early"]
        aux = behaviour.standardized_aux(early, fit_sites).reindex(list(split))
        aux_head = torch.nn.Linear(code_dim, len(behaviour.AUX))
    reader, decoder = build_models(kind, code_dim, dim, depth, hidden, dropout)
    options = {"inputs": tuple(reader.encoders), "all_weather": all_weather, "aux": aux}
    fit = ShortRecordDataset(daily, fit_sites, scale, months=months, draws=draws, seed=seed, **options)
    validation = ShortRecordDataset(daily, validation_sites, scale, months=months, draws=4,
                                    seed=seed, fixed=True, **options)
    started = time.time()
    history = pretrain_behaviour(reader, decoder, fit, epochs=epochs, batch_size=batch_size, lr=lr,
                                 weight_decay=weight_decay, device=device,
                                 validation_dataset=validation, aux_head=aux_head,
                                 aux_weight=aux_weight, verbose=verbose)
    minutes = (time.time() - started) / 60.0
    reader.to("cpu"), decoder.to("cpu")
    torch.save({"encoder": reader.state_dict(), "decoder": decoder.state_dict()},
               os.path.join(arms_dir, "short_%s.pt" % name))
    report = {"name": name, "kind": kind, "seed": seed, "epochs": epochs, "code_dim": code_dim,
              "dim": dim, "depth": depth, "hidden": hidden, "dropout": dropout, "draws": draws,
              "batch_size": batch_size, "lr": lr, "weight_decay": weight_decay,
              "aux_weight": aux_weight, "months": list(months),
              "validation_fraction": validation_fraction,
              "number_scale": {"mean": scale["mean"].to_dict(), "std": scale["std"].to_dict()},
              "fit_basins": len(fit.site_ids), "validation_basins": len(validation.site_ids),
              "minutes": round(minutes, 2), "best_epoch": history.get("best_epoch", [epochs])[0],
              "validation_by_months": losses_by_length(reader, decoder, daily, validation_sites,
                                                       scale, months, seed, all_weather),
              "series_loss": history["series"], "validation_loss": history.get("validation")}
    os.makedirs(os.path.join(arms_dir, "reports"), exist_ok=True)
    with open(os.path.join(arms_dir, "reports", "short_%s.json" % name), "w") as file:
        json.dump(report, file, indent=2)
    return report


def load_run(arms_dir: str, name: str):
    """
    Rebuilds a :func:`train` run.

    :param arms_dir: Cohort directory.
    :type arms_dir: str
    :param name: Run name.
    :type name: str
    :return: The reader, the decoder, the run's report and the scale of its hand-built numbers.
    :rtype: Tuple[torch.nn.Module, torch.nn.Module, Dict, pd.DataFrame]
    """
    with open(os.path.join(arms_dir, "reports", "short_%s.json" % name)) as file:
        report = json.load(file)
    reader, decoder = build_models(report["kind"], report["code_dim"], report["dim"],
                                   report["depth"], report["hidden"], report["dropout"])
    state = torch.load(os.path.join(arms_dir, "short_%s.pt" % name), weights_only=True)
    reader.load_state_dict(state["encoder"]), decoder.load_state_dict(state["decoder"])
    scale = pd.DataFrame(report["number_scale"]).reindex(NUMBERS)
    return reader.eval(), decoder.eval(), report, scale


def record_anchor(frame: pd.DataFrame, end: str, months: float) -> Optional[int]:
    """
    Finds the last day on or before ``end`` that closes a mostly observed record.

    A new gauge's record holds flow by definition. A fixed calendar day would hand a gauge that
    is shut in winter an empty record, so the record is moved back to the latest day whose
    ``months`` months hold a flow value on at least ``MIN_VISIBLE_SHARE`` of the days (failing
    that, the day with the most).

    :param frame: The basin's daily frame without gaps.
    :type frame: pd.DataFrame
    :param end: The latest day the record may end on.
    :type end: str
    :param months: Length of the record in months.
    :type months: float
    :return: Position of the record's last day, or None when the frame has no flow by ``end``.
    :rtype: int, optional
    """
    upto = frame.loc[:pd.Timestamp(end)]
    if upto.empty or not upto.q_mm.notna().any():
        return None
    days = visible_days(months)
    if days == 0:
        return len(upto) - 1
    observed = upto.q_mm.notna()
    share = (observed.rolling(days, min_periods=1).sum() / days).where(observed).to_numpy()
    enough = np.flatnonzero(share >= MIN_VISIBLE_SHARE)
    return int(enough[-1]) if len(enough) else int(np.nanargmax(share))


def write_codes(reader, kind: str, daily: Dict[str, pd.DataFrame], sites: Sequence[str],
                end: str, months: float, scale: pd.DataFrame, batch_size: int = 128
                ) -> pd.DataFrame:
    """
    Describes every basin from a record of ``months`` months closing on or before an end day
    (see :func:`record_anchor`).

    :param reader: A trained reader.
    :type reader: torch.nn.Module
    :param kind: ``record``, ``numbers`` or ``record_weather``.
    :type kind: str
    :param daily: Daily records by site id; nothing after ``end`` is read.
    :type daily: Dict[str, pd.DataFrame]
    :param sites: The basins to describe.
    :type sites: Sequence[str]
    :param end: The latest day a record may end on.
    :type end: str
    :param months: Length of the record in months.
    :type months: float
    :param scale: The scale of the hand-built numbers the run was trained with.
    :type scale: pd.DataFrame
    :param batch_size: Basins per forward pass, defaults to 128.
    :type batch_size: int, optional
    :return: Codes indexed by site id; a basin with no flow by ``end`` is left out.
    :rtype: pd.DataFrame
    """
    from flood_forecast.meta_models.behaviour_pretrain import encode_code
    key = "statics" if kind == "numbers" else "history"
    kept, inputs = [], []
    for site in sites:
        frame = daily.get(site)
        last = None if frame is None else record_anchor(frame, end, months)
        if last is None:
            continue
        if kind == "numbers":
            record = frame.iloc[max(last - visible_days(months) + 1, 0):last + 1]
            inputs.append(number_vector(record, scale, months))
        else:
            inputs.append(read_blocks(frame, last, months, kind == "record_weather"))
        kept.append(site)
    if not kept:
        return pd.DataFrame()
    reader.eval()
    with torch.no_grad():
        codes = [encode_code(reader, {key: torch.from_numpy(np.stack(inputs[start:start + batch_size]))})
                 for start in range(0, len(inputs), batch_size)]
    return pd.DataFrame(torch.cat(codes).numpy(), index=kept)


def simulate(arms_dir: str, name: str, months: float, end: str = "2021-12-31",
             training_months: Optional[float] = None) -> str:
    """
    Describes every basin from a short record and simulates its whole daily record.

    Writes ``sim_<label>.pkl`` in the format of ``decoder_simulation.simulate``, with the label
    ``short_<name>@m<months>`` (and ``@<end>`` appended unless it is the last day before the
    cutoff), so the tree forecaster can read it as ``sim:<label>``.

    :param arms_dir: Cohort directory.
    :type arms_dir: str
    :param name: A :func:`train` run.
    :type name: str
    :param months: Length of the held-out basins' records in months.
    :type months: float
    :param end: The latest day a record may end on, defaults to the last day before the cutoff.
    :type end: str, optional
    :param training_months: Length of the training basins' records. A forecaster stacked on
        these simulations is fitted on the training basins; describing them from the longest
        record keeps its fitting rows the same whatever ``months`` is. Defaults to None (the
        longest record length).
    :type training_months: float, optional
    :return: The label.
    :rtype: str
    """
    if pd.Timestamp(end) >= pd.Timestamp(behaviour.CUTOFF):
        raise ValueError("a record must end before the cutoff %s" % behaviour.CUTOFF)
    reader, decoder, report, scale = load_run(arms_dir, name)
    cache = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))
    split = cache["split"]
    longest = max(MONTHS) if training_months is None else training_months
    groups = {months: [site for site in split if split[site] != "train"],
              longest: [site for site in split if split[site] == "train"]}
    if months == longest:
        groups = {months: list(split)}
    codes = pd.concat([write_codes(reader, report["kind"], cache["daily"], sites, end, length, scale)
                       for length, sites in groups.items()])
    whole = pd.read_pickle(os.path.join(arms_dir, simulation.DAILY))["daily"]
    simulated, overrun = {}, {}
    for site in codes.index:
        if site in whole:
            code = torch.tensor(codes.loc[site].to_numpy(dtype=np.float32))
            series, beyond = simulation.simulate_basin(decoder, whole[site], code)
            simulated[site] = pd.Series(series, index=whole[site].index)
            overrun[site] = pd.Series(beyond, index=whole[site].index)
    label = "short_%s@m%g" % (name, months)
    if end != "2021-12-31":
        label += "@" + end
    pd.to_pickle({"sim": simulated, "overrun": overrun, "run": name, "months": months, "end": end,
                  "training_months": longest},
                 os.path.join(arms_dir, "sim_%s.pkl" % label))
    print("simulated %d basins with %s" % (len(simulated), label), flush=True)
    return label


def skill(arms_dir: str, labels: Sequence[str], since: str = "2023-01-01",
          holdout: str = "holdout") -> pd.DataFrame:
    """
    Scores simulations against observed flow on the held-out basins, with no flow given.

    :param arms_dir: Cohort directory.
    :type arms_dir: str
    :param labels: Labels of ``sim_<label>.pkl`` files.
    :type labels: Sequence[str]
    :param since: First scored day, defaults to 2023-01-01.
    :type since: str, optional
    :param holdout: Split label of the scored basins, defaults to "holdout".
    :type holdout: str, optional
    :return: Per label the number of basins, the median Nash-Sutcliffe efficiency of log flow
        and of flow, and the share of basins whose efficiency of log flow exceeds 0.5.
    :rtype: pd.DataFrame
    """
    whole = pd.read_pickle(os.path.join(arms_dir, simulation.DAILY))["daily"]
    split = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))["split"]
    rows = {}
    for label in labels:
        simulated = pd.read_pickle(os.path.join(arms_dir, "sim_%s.pkl" % label))["sim"]
        of_log, of_flow = [], []
        for site, series in simulated.items():
            if split.get(site) != holdout:
                continue
            late = series.index >= since
            observed = np.log(whole[site].q_mm[late] + behaviour.FLOW_FLOOR_MM)
            pair = pd.concat([series[late], observed], axis=1).dropna()
            if len(pair) < 200:
                continue
            sim, obs = pair.iloc[:, 0].to_numpy(np.float64), pair.iloc[:, 1].to_numpy(np.float64)
            of_log.append(1.0 - ((sim - obs) ** 2).mean() / obs.var())
            of_flow.append(1.0 - ((np.exp(sim) - np.exp(obs)) ** 2).mean() / np.exp(obs).var())
        rows[label] = {"basins": len(of_log), "median_nse_log": float(np.median(of_log)),
                       "median_nse": float(np.median(of_flow)),
                       "share_nse_log_above_half": float((np.asarray(of_log) > 0.5).mean())}
    return pd.DataFrame(rows).T


def main() -> None:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    trainer = commands.add_parser("train", help="fit one description on records of random length")
    trainer.add_argument("--arms-dir", required=True)
    trainer.add_argument("--name", required=True)
    trainer.add_argument("--kind", choices=KINDS, default="record")
    trainer.add_argument("--seed", type=int, default=0)
    trainer.add_argument("--epochs", type=int, default=60)
    trainer.add_argument("--draws", type=int, default=8)
    trainer.add_argument("--batch-size", type=int, default=64)
    trainer.add_argument("--lr", type=float, default=1e-3)
    trainer.add_argument("--aux-weight", type=float, default=1.0)
    trainer.add_argument("--months", type=float, nargs="+", default=None)
    trainer.add_argument("--device", default="cpu")
    simulator = commands.add_parser("simulate", help="describe basins from a short record and "
                                                     "simulate them")
    simulator.add_argument("--arms-dir", required=True)
    simulator.add_argument("--name", nargs="+", required=True)
    simulator.add_argument("--months", type=float, nargs="+", default=list(MONTHS))
    simulator.add_argument("--end", nargs="+", default=["2021-12-31"])
    scorer = commands.add_parser("skill", help="score simulations with no observed flow")
    scorer.add_argument("--arms-dir", required=True)
    scorer.add_argument("--labels", nargs="+", required=True)
    args = parser.parse_args()
    if args.command == "train":
        options = {key: value for key, value in vars(args).items() if key != "command"}
        report = train(**options)
        print(json.dumps({key: value for key, value in report.items()
                          if key not in ("series_loss", "validation_loss", "number_scale")}, indent=2))
    elif args.command == "simulate":
        for name in args.name:
            for end in args.end:
                for months in args.months:
                    simulate(args.arms_dir, name, months, end)
    else:
        print(skill(args.arms_dir, args.labels).round(3).to_string())


if __name__ == "__main__":
    main()
