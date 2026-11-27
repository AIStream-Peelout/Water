"""
Simulated daily flow from a pretrained behaviour decoder, as inputs to the tree forecaster.

Behaviour pretraining (``pretrain_behaviour_code.py``) leaves two things per run: a code for
every basin and a decoder that turns daily weather into log flow for a code. A context arm
hands the forecaster only the code, as constants. Here the decoder is run over each basin's
whole record with a chosen code, so the forecaster can be given what the code and the decoder
say together about the days ahead. ``tabular_forecast_baseline`` reads the result as the
feature group ``sim:<label>``.

The decoder only ever reads weather up to the day it writes, and the forecaster already sees
the realized weather of the horizon, so the simulation adds no information about the future
beyond what the forecaster is given. What it adds is a learned, basin-specific response to
that weather.

Commands
    daily      cache every basin's whole daily record (the weather after the cutoff is needed)
    simulate   write ``sim_<label>.pkl`` for one pretraining run and one choice of code

Run from the Water root with Flow Forecast on the path, for example::

    FF_REPO=/path/to/flow-forecast python experiments/catchment_foundation/decoder_simulation.py \
        simulate --arms-dir experiments/catchment_foundation/calibration/<cohort> \
        --run behaviour_aux_s0
"""
import argparse
import functools
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from experiments.catchment_foundation import pretrain_behaviour_code as behaviour  # noqa: E402
from experiments.catchment_foundation import tabular_forecast_baseline as tabular  # noqa: E402

DAILY = "daily_full.pkl"
CODES = ("own", "mean", "shuffled", "none", "other_state", "donor")
READ_PASSES = 6                       # as ``pretrain_behaviour_code.train`` writes its banks
MAX_LOG_RATIO = 3.0                   # simulated change is capped at a factor of about 20
HORIZON_DAYS = max(last for _, last in tabular.BANDS.values())
FEATURES = [f"sim_{kind}_{band}" for kind in ("dlog", "add", "dq") for band in tabular.BANDS]
FEATURES += ["sim_err0", "sim_err7"]
MEMBER_YEARS = 10                     # other years whose weather stands in for an unknown horizon


def whole_record(basin: Dict) -> Optional[Tuple[str, pd.DataFrame]]:
    """One basin's whole daily record (see :func:`pretrain_behaviour_code.daily_record`)."""
    return behaviour.daily_record(basin, until=None)


def build_daily(arms_dir: str, workers: int = 6) -> Dict:
    """
    Caches the whole daily record and the flow scale of every basin of the cohort.

    :param arms_dir: Cohort directory holding the pre-cutoff cache (which names the manifest).
    :type arms_dir: str
    :param workers: Processes reading hourly files, defaults to 6.
    :type workers: int, optional
    :return: ``daily`` (site id -> frame) and ``scale`` (site id -> flow scale in mm per hour).
    :rtype: Dict
    """
    manifest_path = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))["manifest"]
    with open(manifest_path) as file:
        basins = json.load(file)["basins"]
    started = time.time()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        records = [item for item in pool.map(whole_record, basins, chunksize=4) if item]
    cache = {"daily": dict(records),
             "scale": {str(basin["site_id"]): float(basin["flow_scale_mm_hr"])
                       for basin in basins}}
    pd.to_pickle(cache, os.path.join(arms_dir, DAILY))
    print("cached %d of %d basins in %.0f s" % (len(records), len(basins),
                                                time.time() - started), flush=True)
    return cache


def year_blocks(index: pd.DatetimeIndex) -> List[Tuple[int, int, int]]:
    """
    Cuts a daily record into the blocks the decoder is run on.

    As in pretraining, a block is a water year (from 1 October) preceded by
    ``SPIN_UP_DAYS`` of weather whose simulated flow is discarded. The first block starts with
    the record and the last one ends with it.

    :param index: The record's daily index, without gaps.
    :type index: pd.DatetimeIndex
    :return: Per block the positions (first day read, first day written, one past the last).
    :rtype: List[Tuple[int, int, int]]
    """
    if len(index) == 0:
        return []
    first, last = index[0], index[-1]
    spin_up = pd.Timedelta(days=behaviour.SPIN_UP_DAYS)
    blocks = []
    for year in range(first.year - 1, last.year + 1):
        start = pd.Timestamp(year=year, month=10, day=1)
        end = min(pd.Timestamp(year=year + 1, month=9, day=30), last)
        begin = max(start - spin_up, first)
        write_from = max(start, begin + spin_up)
        if write_from <= end:
            blocks.append(((begin - first).days, (write_from - first).days,
                           (end - first).days + 1))
    return blocks


def water_year_start(day: pd.Timestamp) -> pd.Timestamp:
    """The 1 October that opens the water year a day belongs to."""
    return pd.Timestamp(year=day.year if day.month >= 10 else day.year - 1, month=10, day=1)


def simulate_basin(decoder: torch.nn.Module, daily: pd.DataFrame, code: torch.Tensor,
                   batch_size: int = 64,
                   year_codes: Optional[Dict[pd.Timestamp, torch.Tensor]] = None
                   ) -> Tuple[np.ndarray, np.ndarray]:
    """
    Runs the decoder over one basin's whole record.

    Each block is also run ``HORIZON_DAYS`` past its end. Those days belong to the next block,
    which restarts the decoder; the overrun is returned separately so a forecast issued in the
    last days of a block can take its issue day and its horizon from one and the same run.

    :param decoder: The pretrained decoder, mapping (drivers, code) to log flow.
    :type decoder: torch.nn.Module
    :param daily: The basin's daily frame without gaps (``p``, ``t``, ``pet``).
    :type daily: pd.DataFrame
    :param code: The basin code of shape (code_dim,).
    :type code: torch.Tensor
    :param batch_size: Blocks per forward pass, defaults to 64.
    :type batch_size: int, optional
    :param year_codes: Codes for single water years, keyed by their 1 October; a block whose
        year is listed is run with that code instead of ``code``. Defaults to None.
    :type year_codes: Dict[pd.Timestamp, torch.Tensor], optional
    :return: Simulated log flow in mm per day for every day (NaN before the first spin-up
        ends), and the overrun: on the first ``HORIZON_DAYS`` of a block, what the previous
        block's run says (NaN elsewhere).
    :rtype: Tuple[np.ndarray, np.ndarray]
    """
    simulated = np.full(len(daily), np.nan, dtype=np.float32)
    overrun = np.full(len(daily), np.nan, dtype=np.float32)
    drivers = behaviour.weather_channels(daily)
    by_length: Dict[int, List[Tuple[int, int, int, int]]] = {}
    for begin, write_from, stop in year_blocks(daily.index):
        run_to = min(stop + HORIZON_DAYS, len(daily))
        by_length.setdefault(run_to - begin, []).append((begin, write_from, stop, run_to))
    with torch.no_grad():
        for blocks in by_length.values():
            for offset in range(0, len(blocks), batch_size):
                chunk = blocks[offset:offset + batch_size]
                batch = torch.from_numpy(np.stack([drivers[begin:run_to]
                                                   for begin, _, _, run_to in chunk]))
                codes = torch.stack([(year_codes or {}).get(
                    water_year_start(daily.index[write_from]), code)
                    for _, write_from, _, _ in chunk])
                output = decoder(batch, codes).numpy()
                for row, (begin, write_from, stop, run_to) in enumerate(chunk):
                    simulated[write_from:stop] = output[row, write_from - begin:stop - begin]
                    overrun[stop:run_to] = output[row, stop - begin:]
    return simulated, overrun


def unknown_weather_changes(decoder: torch.nn.Module, daily: pd.DataFrame, code: torch.Tensor,
                            issue_days: np.ndarray, usable_before: int,
                            members: int = MEMBER_YEARS, batch_size: int = 4096) -> np.ndarray:
    """
    Simulates the 14 days after each issue day without knowing their weather.

    The decoder is run on the realized weather up to the issue day. From its state on that day
    it is then continued once for each of several other years, with that year's weather of the
    same season standing in for the unknown horizon, and the simulated flows are averaged. This
    is the classic ensemble-streamflow-prediction recipe, and what a forecast can rest on when
    no weather forecast is at hand.

    :param decoder: The pretrained decoder (plain or wrapped in ``CodeBlind``).
    :type decoder: torch.nn.Module
    :param daily: The basin's daily frame without gaps (``p``, ``t``, ``pet``).
    :type daily: pd.DataFrame
    :param code: The basin code of shape (code_dim,).
    :type code: torch.Tensor
    :param issue_days: Positions of the issue days in ``daily``.
    :type issue_days: np.ndarray
    :param usable_before: Stand-in weather is only taken from positions before this one (the
        cutoff), so nothing after it is used for any issue day.
    :type usable_before: int
    :param members: Other years tried on each side of the issue day, defaults to
        ``MEMBER_YEARS``; a year without weather for those days is skipped.
    :type members: int, optional
    :param batch_size: Continuations per forward pass, defaults to 4096.
    :type batch_size: int, optional
    :return: Array of shape (issue days, 1 + 14): the simulated log flow on the issue day, then
        the log of the member-mean simulated flow on each of the 14 days after it. NaN where
        the issue day has no simulation or no other year has weather.
    :rtype: np.ndarray
    """
    inner = decoder.decoder if isinstance(decoder, behaviour.CodeBlind) else decoder
    if isinstance(decoder, behaviour.CodeBlind):
        code = torch.zeros_like(code)
    drivers = behaviour.weather_channels(daily)
    present = daily[["p", "t", "pet"]].notna().all(axis=1).to_numpy()
    hidden = np.full((len(daily), inner.hidden_dim), np.nan, dtype=np.float32)
    start_state = torch.tanh(inner.initial_state(code.unsqueeze(0))).reshape(1, 1, inner.hidden_dim)
    with torch.no_grad():
        for begin, write_from, stop in year_blocks(daily.index):
            inputs = torch.cat([torch.from_numpy(drivers[begin:stop]),
                                code.unsqueeze(0).expand(stop - begin, -1)], dim=-1).unsqueeze(0)
            states, _ = inner.rnn(inputs, start_state.contiguous())
            hidden[write_from:stop] = states[0, write_from - begin:].numpy()
        result = np.full((len(issue_days), 1 + HORIZON_DAYS), np.nan, dtype=np.float64)
        known = np.isfinite(hidden[issue_days, 0])
        result[known, 0] = inner.head(torch.from_numpy(hidden[issue_days[known]])).squeeze(-1).numpy()
        rows, starts = [], []
        for row in np.flatnonzero(known):
            for years in list(range(1, members + 1)) + list(range(-members, 0)):
                first = issue_days[row] + 1 - 365 * years
                if first < 0 or first + HORIZON_DAYS > usable_before:
                    continue
                if present[first:first + HORIZON_DAYS].sum() >= HORIZON_DAYS - 2:
                    rows.append(row)
                    starts.append(first)
        rows, starts = np.asarray(rows, dtype=np.int64), np.asarray(starts, dtype=np.int64)
        total = np.zeros((len(issue_days), HORIZON_DAYS))
        count = np.zeros(len(issue_days))
        offsets = np.arange(HORIZON_DAYS)
        for chunk in range(0, len(rows), batch_size):
            pick, first = rows[chunk:chunk + batch_size], starts[chunk:chunk + batch_size]
            weather = torch.from_numpy(drivers[first[:, None] + offsets[None, :]])
            inputs = torch.cat([weather, code.expand(len(pick), HORIZON_DAYS, -1)], dim=-1)
            state = torch.from_numpy(hidden[issue_days[pick]]).unsqueeze(0).contiguous()
            output, _ = inner.rnn(inputs, state)
            np.add.at(total, pick, np.exp(inner.head(output).squeeze(-1).numpy().astype(np.float64)))
            np.add.at(count, pick, 1.0)
    filled = count > 0
    result[filled, 1:] = np.log(total[filled] / count[filled, None])
    return result


def unknown_weather_features(changes: np.ndarray, scale: float) -> np.ndarray:
    """
    Turns :func:`unknown_weather_changes` into the ``sim_dlog`` and ``sim_add`` features.

    :param changes: Array of shape (issue days, 1 + 14) of simulated log flow.
    :type changes: np.ndarray
    :param scale: The basin's flow scale in mm per hour.
    :type scale: float
    :return: Array of shape (issue days, 2 * bands): ``sim_dlog`` per band, then ``sim_add``.
    :rtype: np.ndarray
    """
    level, ahead = np.exp(changes[:, 0]), np.exp(changes[:, 1:])
    relative, additive = [], []
    for first, last in tabular.BANDS.values():
        mean = ahead[:, first - 1:last].mean(axis=1)
        relative.append(np.clip(np.log(mean) - changes[:, 0], -MAX_LOG_RATIO, MAX_LOG_RATIO))
        additive.append((mean - level) / (24.0 * scale))
    return np.stack(relative + additive, axis=1).astype(np.float32)


def simulate_unknown_weather(arms_dir: str, run: str, which: str = "own", seed: int = 0,
                             members: int = MEMBER_YEARS) -> str:
    """
    Writes ``simesp_<label>.pkl``: simulation features that do not use the horizon's weather.

    For every row of the cohort's forecast table: the change features from
    :func:`unknown_weather_changes` and the simulation's current error (which never used the
    horizon). The tree forecaster reads them as the feature group ``simesp:<label>``.

    :param arms_dir: Cohort directory.
    :type arms_dir: str
    :param run: Pretraining run name.
    :type run: str
    :param which: Whose code each basin is simulated with (see :func:`basin_codes`).
    :type which: str, optional
    :param seed: Seed of shuffled codes, defaults to 0.
    :type seed: int, optional
    :param members: Other years tried on each side, defaults to ``MEMBER_YEARS``.
    :type members: int, optional
    :return: The label, ``<run>`` or ``<run>@<which>``.
    :rtype: str
    """
    cache = _daily_cache(arms_dir)
    data = pd.read_pickle(os.path.join(arms_dir, "tabular.pkl"))[["site_id", "date", "q0"]]
    stored = pd.read_pickle(os.path.join(arms_dir, "sim_%s.pkl" % (
        run if which == "own" else "%s@%s" % (run, which))))["sim"]
    reader, decoder, report = load_run(arms_dir, run)
    codes = basin_codes(arms_dir, reader, report, which, seed)
    block = np.full((len(data), len(FEATURES)), np.nan, dtype=np.float32)
    column = {name: position for position, name in enumerate(FEATURES)}
    bands = list(tabular.BANDS)
    dates, q0 = pd.to_datetime(data["date"]).to_numpy(), data["q0"].to_numpy(np.float64)
    started = time.time()
    for site, rows in data.groupby("site_id", sort=False).indices.items():
        if site not in cache["daily"] or site not in codes.index:
            continue
        daily = cache["daily"][site]
        issue = ((dates[rows] - daily.index[0].to_datetime64()) // np.timedelta64(1, "D")).astype(np.int64)
        cutoff = int(np.searchsorted(daily.index.to_numpy(), np.datetime64(tabular.CUTOFF)))
        code = torch.tensor(codes.loc[site].to_numpy(dtype=np.float32))
        changes = unknown_weather_changes(decoder, daily, code, issue, cutoff, members)
        values = unknown_weather_features(changes, cache["scale"][site])
        for position, band in enumerate(bands):
            block[rows, column[f"sim_dlog_{band}"]] = values[:, position]
            block[rows, column[f"sim_add_{band}"]] = values[:, len(bands) + position]
            block[rows, column[f"sim_dq_{band}"]] = q0[rows] * np.expm1(values[:, position].astype(np.float64))
        error = basin_features(stored[site], daily.q_mm, cache["scale"][site]).reindex(dates[rows])
        block[rows, column["sim_err0"]] = error["sim_err0"].to_numpy()
        block[rows, column["sim_err7"]] = error["sim_err7"].to_numpy()
    label = run if which == "own" else "%s@%s" % (run, which)
    pd.to_pickle({"features": block, "run": run, "codes": which, "members": members,
                  "rows": len(data)}, os.path.join(arms_dir, "simesp_%s.pkl" % label))
    print("simulated %d rows without horizon weather with %s in %.0f s"
          % (int(np.isfinite(block[:, 0]).sum()), label, time.time() - started), flush=True)
    return label


def unknown_weather_block(data: pd.DataFrame, arms_dir: str, label: str) -> np.ndarray:
    """
    Loads the ``simesp:<label>`` feature group for the cohort's forecast table.

    :param data: The forecast table the features were written for.
    :type data: pd.DataFrame
    :param arms_dir: Cohort directory holding ``simesp_<label>.pkl``.
    :type arms_dir: str
    :param label: A label written by :func:`simulate_unknown_weather`.
    :type label: str
    :return: Array of shape (rows, len(FEATURES)).
    :rtype: np.ndarray
    """
    stored = pd.read_pickle(os.path.join(arms_dir, "simesp_%s.pkl" % label))
    if stored["rows"] != len(data):
        raise ValueError("simesp_%s.pkl was written for a table of %d rows, not %d"
                         % (label, stored["rows"], len(data)))
    return stored["features"]


def load_run(arms_dir: str, run: str):
    """
    Rebuilds a pretraining run's reader and decoder from its report and saved weights.

    :param arms_dir: Cohort directory.
    :type arms_dir: str
    :param run: Name of a ``train`` run (reads the gauge record) or an ``align`` run (reads the
        static description).
    :type run: str
    :return: The reader, the decoder and the run's report. The decoder of a ``--blind`` run
        comes back wrapped so that it ignores whatever code it is handed, as in its training.
    :rtype: Tuple[torch.nn.Module, torch.nn.Module, Dict]
    """
    with open(os.path.join(arms_dir, "reports", "pretrain_%s.json" % run)) as file:
        report = json.load(file)
    report.setdefault("readers", ["history"])
    hidden = report.get("hidden")
    if "teacher" in report:
        with open(os.path.join(arms_dir, "reports", "pretrain_%s.json" % report["teacher"])) as file:
            hidden = json.load(file)["hidden"]
    state = torch.load(os.path.join(arms_dir, "behaviour_%s.pt" % run), weights_only=True)
    first_layer = state["encoder"].get("encoders.statics.net.0.weight")
    reader, decoder = behaviour.build_models(
        report["code_dim"], report["dim"], report["depth"], hidden, report["dropout"],
        n_statics=0 if first_layer is None else first_layer.shape[1],
        gauged="history" in report["readers"])
    reader.load_state_dict(state["encoder"]), decoder.load_state_dict(state["decoder"])
    if report.get("blind"):
        # Its code weights were never trained: any code but zeros would be noise.
        decoder = behaviour.CodeBlind(decoder)
    return reader.eval(), decoder.eval(), report


def basin_codes(arms_dir: str, reader: torch.nn.Module, report: Dict, which: str = "own",
                seed: int = 0) -> pd.DataFrame:
    """
    Writes the code each basin is simulated with.

    Codes are read from the record before the cutoff only (or from the static description for
    an ``align`` run), exactly as the run's context bank was written.

    :param arms_dir: Cohort directory.
    :type arms_dir: str
    :param reader: The run's reader.
    :type reader: torch.nn.Module
    :param report: The run's report.
    :type report: Dict
    :param which: ``own`` (each basin's code), ``mean`` (every basin gets the mean code of the
        training basins), ``none`` (zeros, for a run whose decoder never saw a code),
        ``shuffled`` (the code of another basin of its split), ``other_state`` (the code of a
        basin of its split in another state) or ``donor`` (the code of the nearest training
        basin other than itself). Defaults to ``own``.
    :type which: str, optional
    :param seed: Seed of the shuffle, defaults to 0.
    :type seed: int, optional
    :return: Codes indexed by site id, one row per basin of the cohort.
    :rtype: pd.DataFrame
    """
    if which not in CODES:
        raise ValueError("which must be one of %s but got %s" % (CODES, which))
    if len(report["readers"]) > 1:
        raise NotImplementedError("simulation is set up for runs with a single reader, not %s"
                                  % report["readers"])
    cache = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))
    split = cache["split"]
    sites = list(split)
    train_sites = sorted(site for site in sites if split[site] == "train")
    if "history" not in report["readers"]:
        statics = behaviour.load_static_banks(arms_dir, report["static_banks"], train_sites)
        statics = statics.reindex(sites).fillna(0.0).astype(np.float32)
        codes = behaviour.write_codes(reader, cache["daily"], sites, 1, 1, statics=statics)
    else:
        codes = behaviour.write_codes(reader, cache["daily"], sites, report["n_read"],
                                      report.get("read_passes", READ_PASSES))
    mean = codes.loc[codes.index.intersection(train_sites)].mean()
    codes = codes.reindex(sites)
    codes = codes.where(codes.notna().all(axis=1), mean, axis=1)
    if which == "mean":
        codes.loc[:, :] = np.tile(mean.to_numpy(), (len(codes), 1))
    elif which == "none":
        codes.loc[:, :] = 0.0
    elif which == "shuffled":
        codes = shuffle_within_split(codes, split, seed)
    elif which == "other_state":
        with open(cache["manifest"]) as file:
            states = {str(basin["site_id"]): str(basin["state"])
                      for basin in json.load(file)["basins"]}
        codes = shuffle_within_split(codes, split, seed, apart=states)
    elif which == "donor":
        codes = nearest_training_codes(codes, tabular.load_bank(arms_dir, "location"),
                                       train_sites)
    return codes


def nearest_training_codes(codes: pd.DataFrame, location: pd.DataFrame,
                           train_sites: List[str]) -> pd.DataFrame:
    """
    Gives every basin the code of the nearest training basin other than itself.

    This is the classic way to describe an ungauged basin: borrow from the closest gauged one.

    :param codes: Codes indexed by site id.
    :type codes: pd.DataFrame
    :param location: Coordinates on one scale for both axes, indexed by site id.
    :type location: pd.DataFrame
    :param train_sites: The basins a code may be borrowed from.
    :type train_sites: List[str]
    :return: The borrowed codes, indexed like ``codes``.
    :rtype: pd.DataFrame
    """
    donors = [site for site in train_sites if site in codes.index and site in location.index]
    donor_place = location.loc[donors].to_numpy(np.float64)
    unplaced = [site for site in codes.index if site not in location.index]
    if unplaced:
        raise ValueError("%d basins have no location to find a donor from, e.g. %s"
                         % (len(unplaced), unplaced[:3]))
    borrowed = codes.copy()
    for site in codes.index:
        distance = ((donor_place - location.loc[site].to_numpy(np.float64)) ** 2).sum(axis=1)
        distance[[donor == site for donor in donors]] = np.inf
        borrowed.loc[site] = codes.loc[donors[int(np.argmin(distance))]].to_numpy()
    return borrowed


def shuffle_within_split(codes: pd.DataFrame, split: Dict[str, str], seed: int,
                         apart: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    Gives every basin the code of a different basin of its own split.

    :param codes: Codes indexed by site id.
    :type codes: pd.DataFrame
    :param split: Site id -> split label.
    :type split: Dict[str, str]
    :param seed: Seed of the shuffle.
    :type seed: int
    :param apart: Site id -> group (a state, a region); when given, the other basin is drawn
        from a different group, so it cannot be a neighbour. Defaults to None.
    :type apart: Dict[str, str], optional
    :return: The codes, moved between basins; no basin keeps its own when its split has two.
    :rtype: pd.DataFrame
    """
    rng = np.random.default_rng(seed)
    shuffled = codes.copy()
    for label in sorted(set(split.values())):
        members = [site for site in codes.index if split[site] == label]
        if len(members) < 2:
            continue
        if apart is not None:
            for site in members:
                others = [other for other in members if apart[other] != apart[site]]
                if not others:
                    raise ValueError("no basin of split %s lies outside group %r of %s"
                                     % (label, apart[site], site))
                donor = others[int(rng.integers(len(others)))]
                shuffled.loc[site] = codes.loc[donor].to_numpy()
            continue
        order = rng.permutation(len(members))
        shift = int(rng.integers(1, len(members)))
        donors = [members[order[(rank + shift) % len(members)]] for rank in np.argsort(order)]
        shuffled.loc[members] = codes.loc[donors].to_numpy()
    return shuffled


def other_year_codes(reader: torch.nn.Module, daily: Dict[str, pd.DataFrame], sites: List[str],
                     n_read: int, passes: int = READ_PASSES, batch_size: int = 256
                     ) -> Dict[str, Dict[pd.Timestamp, torch.Tensor]]:
    """
    Writes, for every water year of every basin, the code read from the basin's other years.

    A code written from the whole record has read the flow of the year it is then used to
    simulate. For a later year, which is what a forecast is issued in, it has not. These codes
    put the earlier years on the same footing.

    :param reader: A reader of the gauge record.
    :type reader: torch.nn.Module
    :param daily: Daily records before the cutoff, by site id.
    :type daily: Dict[str, pd.DataFrame]
    :param sites: The basins to write codes for.
    :type sites: List[str]
    :param n_read: Water years the reader sees at a time.
    :type n_read: int
    :param passes: Groups of years averaged per code, defaults to ``READ_PASSES``.
    :type passes: int, optional
    :param batch_size: Groups per forward pass, defaults to 256.
    :type batch_size: int, optional
    :return: Site id -> (1 October of a water year -> code). A basin with a single usable year
        has no entry for it.
    :rtype: Dict[str, Dict[pd.Timestamp, torch.Tensor]]
    """
    from flood_forecast.meta_models.behaviour_pretrain import encode_code
    source = behaviour.WaterYearDataset(daily, [], n_read=n_read)
    keys, groups = [], []
    for site in sites:
        if site not in daily:
            continue
        years = behaviour.water_years(daily[site])
        members = {year: source.member(site, year) for year in years}
        for year in years:
            others = [other for other in years if other != year]
            for group in range(passes if others else 0):
                picked = [others[(group * n_read + step) % len(others)] for step in range(n_read)]
                keys.append((site, year))
                groups.append(np.stack([members[other] for other in picked]))
    codes: Dict[str, Dict[pd.Timestamp, torch.Tensor]] = {}
    reader.eval()
    with torch.no_grad():
        written = [encode_code(reader, {"history": torch.from_numpy(np.stack(groups[start:start + batch_size]))})
                   for start in range(0, len(groups), batch_size)]
    if written:
        table = pd.DataFrame(torch.cat(written).numpy())
        table["site"], table["year"] = [key[0] for key in keys], [key[1] for key in keys]
        for (site, year), rows in table.groupby(["site", "year"]):
            values = rows.drop(columns=["site", "year"]).to_numpy(np.float32).mean(axis=0)
            codes.setdefault(site, {})[year] = torch.from_numpy(values)
    return codes


def simulate(arms_dir: str, run: str, which: str = "own", seed: int = 0,
             daily: Optional[Dict[str, pd.DataFrame]] = None,
             out_of_sample: Optional[str] = None) -> str:
    """
    Simulates every basin's record with a run's decoder and writes ``sim_<label>.pkl``.

    With ``out_of_sample`` the simulation of a basin never comes from a run fitted on it: a
    basin of manifest fold k is simulated by the run named ``out_of_sample.format(fold=k)``
    (trained with ``--leave-out-folds k``), and every other basin by ``run``. Codes read from
    the gauge record are then also written, for each water year before the cutoff, from the
    basin's other years (:func:`other_year_codes`). This is what a forecaster stacked on the
    simulation should be fitted on; the label is ``<run>@oos``.

    :param arms_dir: Cohort directory.
    :type arms_dir: str
    :param run: Pretraining run name.
    :type run: str
    :param which: Whose code each basin is simulated with (see :func:`basin_codes`).
    :type which: str, optional
    :param seed: Seed of the shuffle, defaults to 0.
    :type seed: int, optional
    :param daily: Whole daily records by site id; read from the cache when None.
    :type daily: Dict[str, pd.DataFrame], optional
    :param out_of_sample: Name pattern, with ``{fold}``, of the runs that left each manifest
        fold out. Defaults to None (every basin is simulated by ``run``).
    :type out_of_sample: str, optional
    :return: The label: ``<run>``, ``<run>@<which>`` or ``<run>@oos``.
    :rtype: str
    """
    if daily is None:
        daily = pd.read_pickle(os.path.join(arms_dir, DAILY))["daily"]
    started = time.time()
    members = {run: None}                       # run name -> the basins it simulates (None: rest)
    if out_of_sample:
        if which != "own":
            raise ValueError("out-of-sample simulation is defined for each basin's own code")
        cache = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))
        with open(cache["manifest"]) as file:
            folds = {str(basin["site_id"]): basin.get("fold") for basin in json.load(file)["basins"]}
        for fold in sorted({fold for site, fold in folds.items() if cache["split"][site] == "train"}):
            members[out_of_sample.format(fold=fold)] = {site for site in folds if folds[site] == fold}
    taken = set().union(*[sites for sites in members.values() if sites])
    simulated, overrun = {}, {}
    for name, chosen in members.items():
        reader, decoder, report = load_run(arms_dir, name)
        codes = basin_codes(arms_dir, reader, report, which, seed)
        wanted = [site for site in codes.index
                  if site in daily and (site in chosen if chosen is not None else site not in taken)]
        by_year = {}
        if out_of_sample and "history" in report["readers"]:
            early = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))["daily"]
            by_year = other_year_codes(reader, early, wanted, report["n_read"])
        for site in wanted:
            code = torch.tensor(codes.loc[site].to_numpy(dtype=np.float32))
            series, beyond = simulate_basin(decoder, daily[site], code,
                                            year_codes=by_year.get(site))
            simulated[site] = pd.Series(series, index=daily[site].index)
            overrun[site] = pd.Series(beyond, index=daily[site].index)
    label = run + "@oos" if out_of_sample else run if which == "own" else "%s@%s" % (run, which)
    pd.to_pickle({"sim": simulated, "overrun": overrun, "run": run, "codes": which,
                  "seed": seed}, os.path.join(arms_dir, "sim_%s.pkl" % label))
    print("simulated %d basins with %s in %.0f s"
          % (len(simulated), label, time.time() - started), flush=True)
    return label


def basin_features(simulated: pd.Series, flow_mm: pd.Series, scale: float,
                   overrun: Optional[pd.Series] = None) -> pd.DataFrame:
    """
    Turns one basin's simulated series into per-day forecast features.

    For an issue day and each lead band: ``sim_dlog`` is the simulated change in log flow from
    the issue day to the band mean, and ``sim_add`` the same change in mm, in the forecaster's
    standardized flow units. ``sim_err0`` and ``sim_err7`` are the observed minus the simulated
    log flow on the issue day and over the week ending on it.

    :param simulated: Simulated log flow (mm per day) by day.
    :type simulated: pd.Series
    :param flow_mm: Observed flow (mm per day) on the same days.
    :type flow_mm: pd.Series
    :param scale: The basin's flow scale in mm per hour.
    :type scale: float
    :param overrun: The previous block's run on the first days of each block (see
        :func:`simulate_basin`); used for the horizon of forecasts issued before a restart.
        Defaults to None (the horizon is read from the stitched series).
    :type overrun: pd.Series, optional
    :return: A frame with the ``sim_dlog_*``, ``sim_add_*`` and ``sim_err*`` columns.
    :rtype: pd.DataFrame
    """
    level = np.exp(simulated.astype(np.float64))
    same_run = level if overrun is None else np.exp(overrun.astype(np.float64)).fillna(level)
    frame = pd.DataFrame(index=simulated.index)
    for band, (first, last) in tabular.BANDS.items():
        length = last - first + 1
        ahead = level.shift(-last).rolling(length, min_periods=length).mean()
        if overrun is not None:
            # A horizon that crosses a restart is read from the run the issue day belongs to.
            crossing = overrun.notna().shift(-last).rolling(last, min_periods=1).max() > 0
            crossing &= overrun.isna()
            continued = same_run.shift(-last).rolling(length, min_periods=length).mean()
            ahead = ahead.where(~crossing, continued)
        frame[f"sim_dlog_{band}"] = (np.log(ahead) - simulated).clip(-MAX_LOG_RATIO, MAX_LOG_RATIO)
        frame[f"sim_add_{band}"] = (ahead - level) / (24.0 * scale)
    error = np.log(flow_mm.astype(np.float64) + behaviour.FLOW_FLOOR_MM) - simulated
    frame["sim_err0"] = error
    frame["sim_err7"] = error.rolling(7, min_periods=4).mean()
    return frame.astype(np.float32)


@functools.lru_cache(maxsize=2)
def _daily_cache(arms_dir: str) -> Dict:
    return pd.read_pickle(os.path.join(arms_dir, DAILY))


def feature_block(data: pd.DataFrame, arms_dir: str, label: str) -> np.ndarray:
    """
    Builds the ``sim:<label>`` feature group for the rows of a forecast table.

    :param data: Forecast table with ``site_id``, ``date`` (the issue day) and ``q0``.
    :type data: pd.DataFrame
    :param arms_dir: Cohort directory holding ``sim_<label>.pkl`` and the daily cache.
    :type arms_dir: str
    :param label: A label written by :func:`simulate`.
    :type label: str
    :return: Array of shape (rows, len(FEATURES)), NaN where a basin has no simulation.
    :rtype: np.ndarray
    """
    stored = pd.read_pickle(os.path.join(arms_dir, "sim_%s.pkl" % label))
    simulated, overrun = stored["sim"], stored.get("overrun", {})
    cache = _daily_cache(arms_dir)
    block = np.full((len(data), len(FEATURES)), np.nan, dtype=np.float32)
    columns = [name for name in FEATURES if "_dq_" not in name]
    positions = {name: FEATURES.index(name) for name in FEATURES}
    dates = pd.to_datetime(data["date"]).to_numpy()
    q0 = data["q0"].to_numpy(np.float64)
    for site, rows in data.groupby("site_id", sort=False).indices.items():
        if site not in simulated:
            continue
        frame = basin_features(simulated[site], cache["daily"][site].q_mm, cache["scale"][site],
                               overrun.get(site))
        values = frame.reindex(dates[rows])
        for name in columns:
            block[rows, positions[name]] = values[name].to_numpy()
        for band in tabular.BANDS:
            ratio = np.expm1(values[f"sim_dlog_{band}"].to_numpy(np.float64))
            block[rows, positions[f"sim_dq_{band}"]] = q0[rows] * ratio
    return block


def temperature_offset_block(data: pd.DataFrame, arms_dir: str) -> np.ndarray:
    """
    The temperature correction from gauge to basin elevation (deg C), one column.

    The decoder's temperature carries this correction and the tree forecaster's does not, so a
    simulation arm knows the basin's elevation offset. This group gives the trees that number
    alone, as the control for it.

    :param data: Forecast table with ``site_id``.
    :type data: pd.DataFrame
    :param arms_dir: Cohort directory holding the pre-cutoff cache (which names the manifest).
    :type arms_dir: str
    :return: Array of shape (rows, 1).
    :rtype: np.ndarray
    """
    manifest_path = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))["manifest"]
    with open(manifest_path) as file:
        offsets = {str(basin["site_id"]): float(basin.get("temp_offset_c") or 0.0)
                   for basin in json.load(file)["basins"]}
    return data["site_id"].map(offsets).to_numpy(np.float32)[:, None]


def depth_block(data: pd.DataFrame, arms_dir: str) -> np.ndarray:
    """
    The observed issue-day flow as a depth (log mm per day), one column.

    The simulation error features hand the forecaster the basin's absolute runoff depth, which
    the standardized flow features do not carry. This group gives that depth alone, as the
    control for how much of a simulation arm's gain is just the depth.

    :param data: Forecast table with ``site_id`` and ``date``.
    :type data: pd.DataFrame
    :param arms_dir: Cohort directory holding the daily cache.
    :type arms_dir: str
    :return: Array of shape (rows, 1).
    :rtype: np.ndarray
    """
    cache = _daily_cache(arms_dir)
    block = np.full((len(data), 1), np.nan, dtype=np.float32)
    dates = pd.to_datetime(data["date"]).to_numpy()
    for site, rows in data.groupby("site_id", sort=False).indices.items():
        if site in cache["daily"]:
            flow = cache["daily"][site].q_mm.reindex(dates[rows]).to_numpy(np.float64)
            block[rows, 0] = np.log(flow + behaviour.FLOW_FLOOR_MM)
    return block


def main() -> None:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    cacher = commands.add_parser("daily", help="cache every basin's whole daily record")
    cacher.add_argument("--arms-dir", required=True)
    cacher.add_argument("--workers", type=int, default=6)
    simulator = commands.add_parser("simulate", help="simulate every basin with a run's decoder")
    simulator.add_argument("--arms-dir", required=True)
    simulator.add_argument("--run", nargs="+", required=True, help="pretraining run name(s)")
    simulator.add_argument("--codes", nargs="+", choices=CODES, default=["own"])
    simulator.add_argument("--seed", type=int, default=0, help="seed of the shuffled codes")
    simulator.add_argument("--out-of-sample", default=None,
                           help="name pattern with {fold} of the runs that left each fold out")
    simulator.add_argument("--unknown-weather", action="store_true",
                           help="also write simesp_<label>.pkl: features that do not use the "
                                "horizon's weather (other years' weather stands in)")
    args = parser.parse_args()
    if args.command == "daily":
        build_daily(args.arms_dir, args.workers)
    else:
        daily = pd.read_pickle(os.path.join(args.arms_dir, DAILY))["daily"]
        for run in args.run:
            for which in args.codes:
                simulate(args.arms_dir, run, which, args.seed, daily, args.out_of_sample)
                if args.unknown_weather:
                    simulate_unknown_weather(args.arms_dir, run, which, args.seed)


if __name__ == "__main__":
    main()
