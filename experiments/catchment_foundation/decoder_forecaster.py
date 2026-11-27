"""
A neural forecaster from Flow Forecast parts, conditioned on a description of the basin.

A recurrent decoder (Flow Forecast's ``DriverConditionedDecoder``) reads, day by day, the
weather and the gauge's observed flow up to the issue day, then the realized weather of the 14
days after it, and writes the change in flow from the issue day for each of those days. A small
reader turns a per-basin description (a learned code bank, the hand-built numbers, the physical
statics, or nothing) into the code the decoder is conditioned on. Training is Flow Forecast's
``pretrain_behaviour`` loop; only the windows and the scoring are defined here.

It is trained on exactly the rows the tree forecaster is fitted on and scored on exactly its
rows, with the same target (change of band-mean flow in the basin's standardized units), so the
two can be paired basin by basin (``compare``).

Commands
    train     fit one forecaster and write its forecasts for every evaluation row
    compare   score forecasters next to tree arms, paired with a reference arm

Run from the Water root with Flow Forecast on the path, for example::

    python -m experiments.catchment_foundation.decoder_forecaster train \
        --arms-dir experiments/catchment_foundation/calibration/<cohort> \
        --name gru_code_s0 --seed 0 --description behaviour_aux_s0
"""
import argparse
import json
import os
import sys
import time
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

from experiments.catchment_foundation import decoder_simulation as simulation
from experiments.catchment_foundation import pretrain_behaviour_code as behaviour
from experiments.catchment_foundation import tabular_forecast_baseline as tabular

if os.environ.get("FF_REPO"):
    sys.path.insert(0, os.environ["FF_REPO"])

HORIZON = simulation.HORIZON_DAYS
N_DRIVERS = 9                 # five weather channels, three of observed flow and a known-flow flag
TARGET_CLIP = 3.0             # as the trees: a handful of floods would otherwise own the loss
HISTORY_CLIP = 8.0


class ForecastWindows(Dataset):
    """
    Serves forecast windows: ``history_days`` up to the issue day and the 14 days after it.

    ``drivers`` has shape (history_days + 14, 9): the five weather channels of
    ``pretrain_behaviour_code.weather_channels`` for every day (realized weather after the issue
    day, as the tree forecaster has it); the observed flow minus the issue-day flow in the
    basin's standardized units, and a flag for a known flow, both zero after the issue day; and
    the issue-day flow as a depth (log mm per day) and in standardized units, repeated. ``target``
    is the flow minus the issue-day flow for the 14 days after the issue day (NaN before).
    ``statics`` is the basin's description.
    """

    def __init__(self, daily: Dict[str, pd.DataFrame], scale: Dict[str, float], rows: pd.DataFrame,
                 description: pd.DataFrame, history_days: int = 180, samples: Optional[int] = None,
                 seed: int = 0, clip_target: Optional[float] = TARGET_CLIP):
        """
        Initializes the dataset.

        :param daily: Whole daily records by site id (``q_mm``, ``p``, ``t``, ``pet``), no gaps.
        :type daily: Dict[str, pd.DataFrame]
        :param scale: Flow scale in mm per hour by site id.
        :type scale: Dict[str, float]
        :param rows: The issue days: a frame with ``site_id`` and ``date``.
        :type rows: pd.DataFrame
        :param description: Per-basin description indexed by site id.
        :type description: pd.DataFrame
        :param history_days: Days read up to and including the issue day, defaults to 180.
        :type history_days: int, optional
        :param samples: Windows per pass, drawn at random from the rows; None serves every row
            once, in order. Defaults to None.
        :type samples: int, optional
        :param seed: Seed of the random draws, defaults to 0.
        :type seed: int, optional
        :param clip_target: Bound on the target change; None leaves it as it is.
        :type clip_target: float, optional
        """
        self.history_days, self.samples, self.clip_target = history_days, samples, clip_target
        self.rng = np.random.default_rng(seed)
        self.description = description
        self.weather, self.flow, self.first = {}, {}, {}
        sites = rows["site_id"].to_numpy()
        for site in pd.unique(sites):
            frame = daily[site]
            self.weather[site] = behaviour.weather_channels(frame)
            self.flow[site] = frame.q_mm.to_numpy(np.float64) / (24.0 * scale[site])
            self.first[site] = frame.index[0]
        self.sites = sites
        first = pd.Series(self.first).reindex(sites).to_numpy()
        elapsed = pd.to_datetime(rows["date"]).to_numpy() - first
        self.positions = (elapsed // np.timedelta64(1, "D")).astype(np.int64)
        self.depth_scale = {site: 24.0 * scale[site] for site in self.weather}

    def __len__(self) -> int:
        return len(self.sites) if self.samples is None else self.samples

    def __getitem__(self, index: int) -> Dict[str, torch.Tensor]:
        row = index if self.samples is None else int(self.rng.integers(len(self.sites)))
        site, issue = self.sites[row], int(self.positions[row])
        weather, flow = self.weather[site], self.flow[site]
        length = self.history_days + HORIZON
        begin, stop = issue - self.history_days + 1, issue + HORIZON + 1
        drivers = np.zeros((length, N_DRIVERS), dtype=np.float32)
        target = np.full(length, np.nan, dtype=np.float32)
        source = slice(max(begin, 0), min(stop, len(flow)))
        placed = slice(source.start - begin, source.stop - begin)
        drivers[placed, :5] = weather[source]
        level = flow[issue]
        change = flow[source] - level
        known = np.isfinite(change) & (np.arange(source.start, source.stop) <= issue)
        drivers[placed, 5] = np.where(known, np.clip(change, -HISTORY_CLIP, HISTORY_CLIP), 0.0)
        drivers[placed, 6] = known
        drivers[:, 7] = np.log(level * self.depth_scale[site] + behaviour.FLOW_FLOOR_MM)
        drivers[:, 8] = np.log1p(max(level, 0.0))
        ahead = np.arange(source.start, source.stop) > issue
        future = np.where(ahead, change, np.nan)
        if self.clip_target is not None:
            future = np.clip(future, -self.clip_target, self.clip_target)
        target[placed] = future
        return {"statics": torch.tensor(self.description.loc[site].to_numpy(dtype=np.float32)),
                "drivers": torch.from_numpy(drivers), "target": torch.from_numpy(target)}


def build_models(description_width: int, code_dim: int = 32, dim: int = 64, hidden: int = 64,
                 dropout: float = 0.1):
    """
    Builds the description reader and the forecasting decoder from Flow Forecast parts.

    :param description_width: Width of the per-basin description.
    :type description_width: int
    :param code_dim: Width of the code the decoder is conditioned on, defaults to 32.
    :type code_dim: int, optional
    :param dim: Width of the reader, defaults to 64.
    :type dim: int, optional
    :param hidden: Recurrent state width of the decoder, defaults to 64.
    :type hidden: int, optional
    :param dropout: Dropout of the reader, defaults to 0.1.
    :type dropout: float, optional
    :return: The reader and the decoder.
    :rtype: Tuple[torch.nn.Module, torch.nn.Module]
    """
    from flood_forecast.meta_models.behaviour_pretrain import DriverConditionedDecoder
    reader, _ = behaviour.build_models(code_dim, dim, 1, hidden, dropout,
                                       n_statics=description_width, gauged=False)
    return reader, DriverConditionedDecoder(N_DRIVERS, code_dim, hidden_dim=hidden)


def band_means(daily_change: np.ndarray) -> Dict[str, np.ndarray]:
    """
    Averages forecasts of the 14 days after the issue day over the lead bands.

    :param daily_change: Array of shape (rows, 14): the forecast change on days 1 to 14.
    :type daily_change: np.ndarray
    :return: Band name -> array of shape (rows,).
    :rtype: Dict[str, np.ndarray]
    """
    return {band: daily_change[:, first - 1:last].mean(axis=1)
            for band, (first, last) in tabular.BANDS.items()}


def predict(reader: torch.nn.Module, decoder: torch.nn.Module, windows: ForecastWindows,
            batch_size: int = 512, device: str = "cpu") -> np.ndarray:
    """
    Forecasts every window of a dataset.

    :param reader: The description reader.
    :type reader: torch.nn.Module
    :param decoder: The forecasting decoder.
    :type decoder: torch.nn.Module
    :param windows: The windows, served once each in order.
    :type windows: ForecastWindows
    :param batch_size: Windows per forward pass, defaults to 512.
    :type batch_size: int, optional
    :param device: Torch device, defaults to "cpu".
    :type device: str, optional
    :return: Array of shape (windows, 14): the forecast change on days 1 to 14.
    :rtype: np.ndarray
    """
    from flood_forecast.meta_models.behaviour_pretrain import encode_code
    reader.to(device).eval(), decoder.to(device).eval()
    loader = torch.utils.data.DataLoader(windows, batch_size=batch_size, shuffle=False)
    forecasts = []
    with torch.no_grad():
        for batch in loader:
            code = encode_code(reader, {"statics": batch["statics"].to(device)})
            forecasts.append(decoder(batch["drivers"].to(device), code)[:, -HORIZON:].cpu().numpy())
    return np.concatenate(forecasts)


def train(arms_dir: str, name: str, description: Sequence[str] = (), seed: int = 0,
          epochs: int = 30, samples_per_epoch: int = 60000, batch_size: int = 256,
          lr: float = 1e-3, weight_decay: float = 1e-4, code_dim: int = 32, dim: int = 64,
          hidden: int = 64, dropout: float = 0.1, history_days: int = 180,
          validation_fraction: float = 0.15, validation_windows: int = 12000,
          device: str = "cpu", verbose: bool = True) -> Dict:
    """
    Fits one forecaster on the tree forecaster's fitting rows and forecasts its scoring rows.

    :param arms_dir: Cohort directory (``tabular.pkl``, the daily cache, the context banks).
    :type arms_dir: str
    :param name: Run name: ``forecaster_<name>.pt``, ``forecast_<name>.pkl`` and a report.
    :type name: str
    :param description: Context banks that describe the basin (as in a tree arm); empty for a
        forecaster that knows nothing about the basin. Defaults to empty.
    :type description: Sequence[str], optional
    :param seed: Seed of the validation basins, the draws and the weights, defaults to 0.
    :type seed: int, optional
    :param epochs: Passes, defaults to 30.
    :type epochs: int, optional
    :param samples_per_epoch: Windows drawn per pass, defaults to 60000.
    :type samples_per_epoch: int, optional
    :param batch_size: Windows per step, defaults to 256.
    :type batch_size: int, optional
    :param lr: AdamW learning rate, defaults to 1e-3.
    :type lr: float, optional
    :param weight_decay: AdamW weight decay, defaults to 1e-4.
    :type weight_decay: float, optional
    :param code_dim: Width of the code, defaults to 32.
    :type code_dim: int, optional
    :param dim: Width of the reader, defaults to 64.
    :type dim: int, optional
    :param hidden: Recurrent state width, defaults to 64.
    :type hidden: int, optional
    :param dropout: Reader dropout, defaults to 0.1.
    :type dropout: float, optional
    :param history_days: Days read up to the issue day, defaults to 180.
    :type history_days: int, optional
    :param validation_fraction: Share of the training basins held out to pick the epoch, the
        same basins as a pretraining run of this seed. Defaults to 0.15.
    :type validation_fraction: float, optional
    :param validation_windows: Windows of those basins scored each epoch, defaults to 12000.
    :type validation_windows: int, optional
    :param device: Torch device, defaults to "cpu".
    :type device: str, optional
    :param verbose: Print one line per epoch, defaults to True.
    :type verbose: bool, optional
    :return: The run's report.
    :rtype: Dict
    """
    from flood_forecast.meta_models.behaviour_pretrain import pretrain_behaviour
    data = pd.read_pickle(os.path.join(arms_dir, "tabular.pkl"))
    cache = pd.read_pickle(os.path.join(arms_dir, simulation.DAILY))
    split = pd.read_pickle(os.path.join(arms_dir, behaviour.CACHE))["split"]
    train_sites, fit_sites, validation_sites = behaviour.split_basins(split, seed, validation_fraction)
    sites = [site for site in split if site in cache["daily"]]
    if description:
        described = behaviour.load_static_banks(arms_dir, list(description), train_sites)
        described = described.reindex(sites).fillna(0.0).astype(np.float32)
    else:
        described = pd.DataFrame(np.zeros((len(sites), 1), dtype=np.float32), index=sites)
    settled = pd.to_datetime(data["date"]) < pd.Timestamp(tabular.CUTOFF) - pd.Timedelta(days=HORIZON)
    usable = data.site_id.isin(sites) & (data.period == "fit") & (data.split == "train") & settled
    fit_rows = data.loc[usable & data.site_id.isin(fit_sites), ["site_id", "date"]]
    validation_rows = data.loc[usable & data.site_id.isin(validation_sites), ["site_id", "date"]]
    validation_rows = validation_rows.iloc[::max(1, len(validation_rows) // validation_windows)]
    torch.manual_seed(seed)
    options = {"history_days": history_days}
    fit = ForecastWindows(cache["daily"], cache["scale"], fit_rows, described, samples=samples_per_epoch,
                          seed=seed, **options)
    validation = ForecastWindows(cache["daily"], cache["scale"], validation_rows, described, **options)
    reader, decoder = build_models(described.shape[1], code_dim, dim, hidden, dropout)
    trained = behaviour.CodeBlind(decoder) if not description else decoder
    started = time.time()
    history = pretrain_behaviour(reader, trained, fit, epochs=epochs, batch_size=batch_size, lr=lr,
                                 weight_decay=weight_decay, device=device,
                                 validation_dataset=validation, verbose=verbose)
    minutes = (time.time() - started) / 60.0
    scored = data.site_id.isin(sites) & data.period.isin(["2022", "2023+"])
    windows = ForecastWindows(cache["daily"], cache["scale"], data.loc[scored, ["site_id", "date"]],
                              described, clip_target=None, **options)
    forecasts = band_means(predict(reader, trained, windows, device=device))
    stored = {}
    for band, values in forecasts.items():
        column = np.full(len(data), np.nan, dtype=np.float32)
        column[scored.to_numpy()] = values
        stored[band] = column
    pd.to_pickle(stored, os.path.join(arms_dir, "forecast_%s.pkl" % name))
    torch.save({"reader": reader.state_dict(), "decoder": decoder.state_dict()},
               os.path.join(arms_dir, "forecaster_%s.pt" % name))
    report = {"name": name, "description": list(description), "seed": seed, "epochs": epochs,
              "samples_per_epoch": samples_per_epoch, "batch_size": batch_size, "lr": lr,
              "weight_decay": weight_decay, "code_dim": code_dim, "dim": dim, "hidden": hidden,
              "dropout": dropout, "history_days": history_days,
              "description_width": int(described.shape[1]), "fit_basins": len(fit_sites),
              "fit_rows": int(len(fit_rows)), "validation_basins": len(validation_sites),
              "validation_windows": int(len(validation_rows)), "minutes": round(minutes, 2),
              "best_epoch": history.get("best_epoch", [epochs])[0],
              "series_loss": history["series"], "validation_loss": history.get("validation")}
    os.makedirs(os.path.join(arms_dir, "reports"), exist_ok=True)
    with open(os.path.join(arms_dir, "reports", "forecaster_%s.json" % name), "w") as file:
        json.dump(report, file, indent=2)
    return report


def compare(arms_dir: str, forecasters: List[str], tree_arms: List[str], fit_subsample: int = 3,
            seed: int = 0, reference: Optional[str] = None, also_pair: Sequence[str] = (),
            output: Optional[str] = None) -> Dict:
    """
    Scores neural forecasters next to tree arms on the same rows, paired with a reference arm.

    :param arms_dir: Cohort directory.
    :type arms_dir: str
    :param forecasters: Names of ``train`` runs; each is scored as the arm ``gru:<name>``.
    :type forecasters: List[str]
    :param tree_arms: Tree arms to fit as in ``tabular_forecast_baseline.run``.
    :type tree_arms: List[str]
    :param fit_subsample: Fit the trees on every n-th fitting row, defaults to 3.
    :type fit_subsample: int, optional
    :param seed: Seed of the trees, defaults to 0.
    :type seed: int, optional
    :param reference: Arm every other arm is paired with; defaults to the first tree arm.
    :type reference: str, optional
    :param also_pair: Further arms to pair every arm with, defaults to none.
    :type also_pair: Sequence[str], optional
    :param output: Where to write the report, defaults to None.
    :type output: str, optional
    :return: The report, as ``tabular_forecast_baseline.score`` builds it.
    :rtype: Dict
    """
    from sklearn.ensemble import HistGradientBoostingRegressor
    data = pd.read_pickle(os.path.join(arms_dir, "tabular.pkl"))
    fit_rows = ((data.period == "fit") & (data.split == "train")).to_numpy()
    fit_index = np.flatnonzero(fit_rows)[::fit_subsample]
    predictions = {}
    for arm in tree_arms:
        matrix, categorical = tabular.features(data, tabular.arm_groups(arm), arms_dir, fit_rows)
        for band in tabular.BANDS:
            model = HistGradientBoostingRegressor(
                max_iter=300, learning_rate=0.06, max_leaf_nodes=31, min_samples_leaf=40,
                l2_regularization=1.0, random_state=seed,
                categorical_features=categorical if any(categorical) else None)
            model.fit(matrix[fit_index], data[f"y_{band}"].to_numpy()[fit_index].clip(-3, 3))
            predictions[(arm, band)] = model.predict(matrix)
        print("fitted %s" % arm, flush=True)
    for name in forecasters:
        stored = pd.read_pickle(os.path.join(arms_dir, "forecast_%s.pkl" % name))
        for band in tabular.BANDS:
            predictions[("gru:" + name, band)] = np.nan_to_num(stored[band].astype(np.float64))
    reference = reference or tree_arms[0]
    report = {"reference": reference, "seed": seed,
              "scores": tabular.score(data, predictions, reference, also_pair=also_pair)}
    held = report["scores"]["holdout"]["2023+"]
    print("\nheld-out basins, 2023+: %d basins (median skill %% | error cut vs %s)"
          % (held["basins"], reference))
    for arm in dict.fromkeys(arm for arm, _ in predictions):
        cells = [held["bands"][band][arm] for band in tabular.BANDS]
        line = "  %-58s" % arm + " /".join("%6.1f" % cell["median_basin_skill_pct"] for cell in cells)
        if "error_cut_vs_reference_pct" in cells[0]:
            line += "  | " + " / ".join("%+5.1f (%+.0f,%+.0f)" % (
                cell["error_cut_vs_reference_pct"], *cell["error_cut_ci95"]) for cell in cells)
        print(line)
    if output:
        with open(output, "w") as file:
            json.dump(report, file, indent=1)
    return report


def main() -> None:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    trainer = commands.add_parser("train", help="fit one forecaster and write its forecasts")
    trainer.add_argument("--arms-dir", required=True)
    trainer.add_argument("--name", required=True)
    trainer.add_argument("--description", nargs="*", default=[],
                         help="context banks describing the basin; none for a blind forecaster")
    trainer.add_argument("--seed", type=int, default=0)
    trainer.add_argument("--epochs", type=int, default=30)
    trainer.add_argument("--samples-per-epoch", type=int, default=60000)
    trainer.add_argument("--batch-size", type=int, default=256)
    trainer.add_argument("--lr", type=float, default=1e-3)
    trainer.add_argument("--weight-decay", type=float, default=1e-4)
    trainer.add_argument("--code-dim", type=int, default=32)
    trainer.add_argument("--dim", type=int, default=64)
    trainer.add_argument("--hidden", type=int, default=64)
    trainer.add_argument("--dropout", type=float, default=0.1)
    trainer.add_argument("--history-days", type=int, default=180)
    trainer.add_argument("--device", default="cpu")
    comparer = commands.add_parser("compare", help="score forecasters next to tree arms")
    comparer.add_argument("--arms-dir", required=True)
    comparer.add_argument("--forecasters", nargs="*", default=[])
    comparer.add_argument("--tree-arms", nargs="+", required=True)
    comparer.add_argument("--fit-subsample", type=int, default=3)
    comparer.add_argument("--seed", type=int, default=0)
    comparer.add_argument("--reference", default=None)
    comparer.add_argument("--also-pair", nargs="*", default=[])
    comparer.add_argument("--output", default=None)
    args = parser.parse_args()
    options = {key: value for key, value in vars(args).items() if key != "command"}
    if args.command == "train":
        report = train(**options)
        print(json.dumps({key: value for key, value in report.items()
                          if key not in ("series_loss", "validation_loss")}, indent=2))
    else:
        compare(**options)


if __name__ == "__main__":
    main()
