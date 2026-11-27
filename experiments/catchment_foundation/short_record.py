"""
How well can a basin be described from a short gauge record?

A new gauge has months of flow, not years. Two ways of describing a basin from such a record
are trained and scored here in exactly the same way:

``record``
    the learned reader of behaviour pretraining, shown three years of weather and the flow of
    the record's days only;
``numbers``
    hand-built statistics computed from the same days (the daily counterparts of the numbers in
    ``representation_benchmark.behaviour`` and ``embedding_probes.flow_signatures``), read by a
    small tabular reader together with flags for the ones the record cannot support.

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
NUMBERS = ["log_runoff_mm", "log_runoff_ratio", "log_aridity", "snow_fraction",
           "zero_flow_fraction", "recession_rate", "storm_response", "half_flow_day", "cv",
           "rb_flashiness", "bfi", "fdc_slope", "high_low_ratio", "melt_share", "low_flow",
           "flood_peak"]


def visible_days(months: float) -> int:
    """The number of days in a record of ``months`` months."""
    return int(round(MONTH_DAYS * months))


def read_blocks(daily: pd.DataFrame, last: int, months: float) -> np.ndarray:
    """
    Builds what the record reader sees of a basin whose record ends on a given day.

    Three 365-day blocks end on that day. Their weather is given in full; the flow is given
    only on the last ``months`` months, the record, and marked unknown before it.

    :param daily: The basin's daily frame without gaps (``q_mm``, ``p``, ``t``, ``pet``).
    :type daily: pd.DataFrame
    :param last: Position of the record's last day in ``daily``.
    :type last: int
    :param months: Length of the record in months.
    :type months: float
    :return: Array of shape (3, 365, 7): log flow (zero where unknown), a flag for a known
        flow, and the five weather channels. Days before the frame starts are zero.
    :rtype: np.ndarray
    """
    length = BLOCKS * behaviour.YEAR_DAYS
    first = last - length + 1
    block = daily.iloc[max(first, 0):last + 1]
    flow = behaviour.flow_channel(block)
    positions = np.arange(max(first, 0), last + 1)
    known = np.isfinite(flow) & (positions > last - visible_days(months))
    rows = np.column_stack([np.where(known, flow, 0.0), known.astype(np.float32),
                            behaviour.weather_channels(block)]).astype(np.float32)
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
    if warm_rain.notna().sum() >= 20:
        peak_ahead = flow[::-1].rolling(4, min_periods=4).max()[::-1]
        rise = (peak_ahead - flow.shift(1)).clip(lower=0) / warm_rain
        rise = rise[warm_rain >= max(float(warm_rain.quantile(0.98)), 2.0)].dropna()
        if len(rise) >= 3:
            out["storm_response"] = float(np.log(rise.median() + 1e-3))
    water_year = pd.Series(record.index.year + (record.index.month >= 10), index=record.index)
    half_days = []
    for year, group in flow.groupby(water_year):
        if group.notna().sum() >= 330 and group.sum() > 0:
            passed = group.fillna(0.0).cumsum()
            reached = passed.index[(passed >= 0.5 * passed.iloc[-1]).to_numpy().argmax()]
            half_days.append((reached - pd.Timestamp(year - 1, 10, 1)).days + 1)
    if half_days:
        out["half_flow_day"] = float(np.mean(half_days))
    valid = flow.dropna().clip(lower=0.0)
    out["cv"] = float(valid.std() / mean) if len(valid) > 1 and mean > 0 else float("nan")
    if valid.sum() > 0:
        out["rb_flashiness"] = float(flow.diff().abs().sum() / flow.sum())
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


def number_vector(record: pd.DataFrame, scale: pd.DataFrame) -> np.ndarray:
    """
    The tabular reader's input for a record: standardized numbers, then a flag for each one.

    :param record: The record's days.
    :type record: pd.DataFrame
    :param scale: Output of :func:`number_scale`.
    :type scale: pd.DataFrame
    :return: Array of shape (2 * len(NUMBERS),); an unsupported number is zero with flag zero.
    :rtype: np.ndarray
    """
    values = pd.Series(short_numbers(record))[NUMBERS]
    scaled = ((values - scale["mean"]) / scale["std"]).clip(-8.0, 8.0)
    return np.concatenate([scaled.fillna(0.0).to_numpy(np.float32),
                           values.notna().to_numpy(np.float32)])


class ShortRecordDataset(Dataset):
    """
    Serves a basin through a record of some length and a water year to decode outside it.

    ``history`` is :func:`read_blocks` of the record, ``statics`` is :func:`number_vector` of
    the same days, ``drivers`` and ``target`` are the decoded year as in
    ``pretrain_behaviour_code.WaterYearDataset``. The decoded year never overlaps the record.
    """

    def __init__(self, daily: Dict[str, pd.DataFrame], sites: List[str], scale: pd.DataFrame,
                 months: Sequence[float] = MONTHS, draws: int = 8, seed: int = 0,
                 fixed: bool = False, aux: Optional[pd.DataFrame] = None):
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
        """
        self.years = {site: behaviour.water_years(daily[site]) for site in sites if site in daily}
        self.site_ids = [site for site in sites if len(self.years.get(site, [])) >= 2]
        self.daily, self.scale, self.months = daily, scale, tuple(months)
        self.draws, self.seed, self.fixed, self.aux = draws, seed, fixed, aux
        self.rng = np.random.default_rng(seed)
        self.flow_days = {site: np.flatnonzero(daily[site].q_mm.notna().to_numpy())
                          for site in self.site_ids}

    def __len__(self) -> int:
        return len(self.site_ids) * self.draws

    def choose(self, index: int) -> Tuple[str, float, int, pd.Timestamp]:
        """Picks the basin, the record's length and last day, and the decoded year."""
        site = self.site_ids[index % len(self.site_ids)]
        rng = np.random.default_rng([self.seed, index]) if self.fixed else self.rng
        months = self.months[int(rng.integers(len(self.months)))]
        frame, days, known = self.daily[site], visible_days(months), self.flow_days[site]
        observed = frame.q_mm.notna().to_numpy()
        last = int(known[int(rng.integers(len(known)))])
        for _ in range(20):
            if observed[max(last - days + 1, 0):last + 1].sum() >= MIN_VISIBLE_SHARE * days:
                break
            last = int(known[int(rng.integers(len(known)))])
        start, end = frame.index[max(last - days + 1, 0)], frame.index[last]
        years = self.years[site]
        outside = [year for year in years
                   if year > end or year + pd.Timedelta(days=behaviour.YEAR_DAYS - 1) < start]
        pool = outside or years
        return site, months, last, pool[int(rng.integers(len(pool)))]

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        site, months, last, decoded = self.choose(index)
        frame = self.daily[site]
        record = frame.iloc[max(last - visible_days(months) + 1, 0):last + 1]
        block = frame[decoded - pd.Timedelta(days=behaviour.SPIN_UP_DAYS):
                      decoded + pd.Timedelta(days=behaviour.YEAR_DAYS - 1)]
        target = behaviour.flow_channel(block)
        target[:behaviour.SPIN_UP_DAYS] = np.nan
        item = {"history": torch.from_numpy(read_blocks(frame, last, months)),
                "statics": torch.from_numpy(number_vector(record, self.scale)),
                "drivers": torch.from_numpy(behaviour.weather_channels(block)),
                "target": torch.from_numpy(target)}
        if self.aux is not None:
            item["aux"] = torch.tensor(self.aux.loc[site].to_numpy(dtype=np.float32))
        return item


def build_models(kind: str, code_dim: int = 32, dim: int = 64, depth: int = 2, hidden: int = 64,
                 dropout: float = 0.1):
    """
    Builds the reader of one description and its decoder from Flow Forecast parts.

    :param kind: ``record`` (the learned reader of the daily record) or ``numbers`` (a tabular
        reader of the hand-built numbers).
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
    if kind not in ("record", "numbers"):
        raise ValueError("kind must be record or numbers but got %s" % kind)
    if kind == "record":
        return behaviour.build_models(code_dim, dim, depth, hidden, dropout)
    return behaviour.build_models(code_dim, dim, depth, hidden, dropout,
                                  n_statics=2 * len(NUMBERS), gauged=False)


def losses_by_length(reader, decoder, daily: Dict[str, pd.DataFrame], sites: List[str],
                     scale: pd.DataFrame, months: Sequence[float], seed: int,
                     batch_size: int = 64) -> Dict[str, float]:
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
    :param batch_size: Items per forward pass, defaults to 64.
    :type batch_size: int, optional
    :return: Record length in months -> series loss.
    :rtype: Dict[str, float]
    """
    from flood_forecast.meta_models.behaviour_pretrain import behaviour_step
    reader.eval(), decoder.eval()
    result = {}
    with torch.no_grad():
        for length in months:
            dataset = ShortRecordDataset(daily, sites, scale, months=(length,), draws=4,
                                         seed=seed, fixed=True)
            loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=False)
            batches = [behaviour_step(reader, decoder, {name: batch[name] for name in reader.encoders},
                                      batch["drivers"], batch["target"])["series"].item()
                       for batch in loader]
            result["%g" % length] = float(np.mean(batches))
    return result


def train(arms_dir: str, name: str, kind: str = "record", seed: int = 0, epochs: int = 60,
          code_dim: int = 32, dim: int = 64, depth: int = 2, hidden: int = 64,
          dropout: float = 0.1, draws: int = 8, batch_size: int = 64, lr: float = 1e-3,
          weight_decay: float = 1e-4, aux_weight: float = 1.0, validation_fraction: float = 0.15,
          months: Sequence[float] = MONTHS, device: str = "cpu", verbose: bool = True) -> Dict:
    """
    Fits one description on records of random length and end day.

    :param arms_dir: Cohort directory holding the pre-cutoff daily cache.
    :type arms_dir: str
    :param name: Run name: ``short_<name>.pt`` and ``reports/short_<name>.json``.
    :type name: str
    :param kind: ``record`` or ``numbers`` (see :func:`build_models`).
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
    :param months: Record lengths drawn from, defaults to ``MONTHS``.
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
    train_sites, fit_sites, validation_sites = behaviour.split_basins(split, seed, validation_fraction)
    torch.manual_seed(seed)
    scale = number_scale(daily, train_sites)
    aux, aux_head = None, None
    if aux_weight > 0:
        early = pd.read_pickle(os.path.join(arms_dir, "benchmark.pkl"))["early"]
        aux = behaviour.standardized_aux(early, fit_sites).reindex(list(split))
        aux_head = torch.nn.Linear(code_dim, len(behaviour.AUX))
    fit = ShortRecordDataset(daily, fit_sites, scale, months=months, draws=draws, seed=seed, aux=aux)
    validation = ShortRecordDataset(daily, validation_sites, scale, months=months, draws=4,
                                    seed=seed, fixed=True, aux=aux)
    reader, decoder = build_models(kind, code_dim, dim, depth, hidden, dropout)
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
              "fit_basins": len(fit.site_ids), "validation_basins": len(validation.site_ids),
              "minutes": round(minutes, 2), "best_epoch": history.get("best_epoch", [epochs])[0],
              "validation_by_months": losses_by_length(reader, decoder, daily, validation_sites,
                                                       scale, months, seed),
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
    :return: The reader, the decoder and the run's report.
    :rtype: Tuple[torch.nn.Module, torch.nn.Module, Dict]
    """
    with open(os.path.join(arms_dir, "reports", "short_%s.json" % name)) as file:
        report = json.load(file)
    reader, decoder = build_models(report["kind"], report["code_dim"], report["dim"],
                                   report["depth"], report["hidden"], report["dropout"])
    state = torch.load(os.path.join(arms_dir, "short_%s.pt" % name), weights_only=True)
    reader.load_state_dict(state["encoder"]), decoder.load_state_dict(state["decoder"])
    return reader.eval(), decoder.eval(), report


def write_codes(reader, kind: str, daily: Dict[str, pd.DataFrame], sites: Sequence[str],
                end: str, months: float, scale: pd.DataFrame, batch_size: int = 128
                ) -> pd.DataFrame:
    """
    Describes every basin from the ``months`` months of record before an end day.

    :param reader: A trained reader.
    :type reader: torch.nn.Module
    :param kind: ``record`` or ``numbers``.
    :type kind: str
    :param daily: Daily records by site id; nothing after ``end`` is read.
    :type daily: Dict[str, pd.DataFrame]
    :param sites: The basins to describe.
    :type sites: Sequence[str]
    :param end: The record's last day.
    :type end: str
    :param months: Length of the record in months.
    :type months: float
    :param scale: Output of :func:`number_scale`.
    :type scale: pd.DataFrame
    :param batch_size: Basins per forward pass, defaults to 128.
    :type batch_size: int, optional
    :return: Codes indexed by site id; a basin whose frame does not reach ``end`` is left out.
    :rtype: pd.DataFrame
    """
    from flood_forecast.meta_models.behaviour_pretrain import encode_code
    key = "history" if kind == "record" else "statics"
    kept, inputs = [], []
    for site in sites:
        frame = daily.get(site)
        if frame is None or frame.index[0] > pd.Timestamp(end) or frame.index[-1] < pd.Timestamp(end):
            continue
        last = int(frame.index.get_loc(pd.Timestamp(end)))
        if kind == "record":
            inputs.append(read_blocks(frame, last, months))
        else:
            inputs.append(number_vector(frame.iloc[max(last - visible_days(months) + 1, 0):last + 1],
                                        scale))
        kept.append(site)
    reader.eval()
    with torch.no_grad():
        codes = [encode_code(reader, {key: torch.from_numpy(np.stack(inputs[start:start + batch_size]))})
                 for start in range(0, len(inputs), batch_size)]
    return pd.DataFrame(torch.cat(codes).numpy(), index=kept)


def simulate(arms_dir: str, name: str, months: float, end: str = "2021-12-31") -> str:
    """
    Describes every basin from a short record and simulates its whole daily record.

    Writes ``sim_<label>.pkl`` in the format of ``decoder_simulation.simulate``, with the label
    ``short_<name>@m<months>`` (and ``@<end>`` appended unless it is the last day before the
    cutoff), so the tree forecaster can read it as ``sim:<label>``.

    :param arms_dir: Cohort directory.
    :type arms_dir: str
    :param name: A :func:`train` run.
    :type name: str
    :param months: Length of the record in months.
    :type months: float
    :param end: The record's last day, defaults to the last day before the cutoff.
    :type end: str, optional
    :return: The label.
    :rtype: str
    """
    if pd.Timestamp(end) >= pd.Timestamp(behaviour.CUTOFF):
        raise ValueError("a record must end before the cutoff %s" % behaviour.CUTOFF)
    reader, decoder, report = load_run(arms_dir, name)
    cache = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))
    split = cache["split"]
    train_sites = sorted(site for site in split if split[site] == "train")
    scale = number_scale(cache["daily"], train_sites)
    codes = write_codes(reader, report["kind"], cache["daily"], list(split), end, months, scale)
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
    pd.to_pickle({"sim": simulated, "overrun": overrun, "run": name, "months": months, "end": end},
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
    trainer.add_argument("--kind", choices=("record", "numbers"), default="record")
    trainer.add_argument("--seed", type=int, default=0)
    trainer.add_argument("--epochs", type=int, default=60)
    trainer.add_argument("--draws", type=int, default=8)
    trainer.add_argument("--batch-size", type=int, default=64)
    trainer.add_argument("--lr", type=float, default=1e-3)
    trainer.add_argument("--aux-weight", type=float, default=1.0)
    trainer.add_argument("--months", type=float, nargs="+", default=list(MONTHS))
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
                          if key not in ("series_loss", "validation_loss")}, indent=2))
    elif args.command == "simulate":
        for name in args.name:
            for end in args.end:
                for months in args.months:
                    simulate(args.arms_dir, name, months, end)
    else:
        print(skill(args.arms_dir, args.labels).round(3).to_string())


if __name__ == "__main__":
    main()
