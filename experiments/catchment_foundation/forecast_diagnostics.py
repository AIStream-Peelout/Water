"""
Evaluation-only checks of whether a multi-basin forecaster can reward a better catchment context.

Nothing here trains a model. Three questions, one subcommand each:

``context``
    Does a trained checkpoint react to its catchment context? The same evaluation windows are
    re-run with the context rows permuted across basins, replaced by the cohort mean, and zeroed.
    Works for ``HybridGR4MultiBasin`` and ``CrossformerMultiBasin`` checkpoints.

``headroom``
    How much forecast skill can per-basin GR4/snow parameters buy the hybrid at all? The parameter
    head is replaced by a lookup table (calibrated per basin, one shared set, calibrated but
    shuffled across basins, the mid-range set an untrained head emits) and the forcing is pure
    physics. The gap between "calibrated" and "shared" bounds what any static context can add
    through the parameter head.

``audit``
    Is a finished run's headline number trustworthy? Puts the validation number that drove early
    stopping next to what persistence scores on the same windows, reports how much of the pooled
    evaluation error a single basin / five windows hold, and can pair the run with another one
    basin by basin (bootstrap over basins).

All skill numbers are ``100 * (1 - MSE_model / MSE_persistence)`` in mm/hr, as in ``evaluate.py``.
Per-basin statistics (median basin, share of basins beating persistence, paired wins) are the
robust readings; pooled skill is printed for continuity and is dominated by a few basins.

Run from the Water root with Flow Forecast on the path, for example::

    FF_REPO=/path/to/flow-forecast python experiments/catchment_foundation/forecast_diagnostics.py \
        context --run-dir experiments/catchment_foundation/runs/<run>
"""
import argparse
import glob
import json
import os
import sys
from typing import Dict, List, Optional, Tuple

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..")))
if os.environ.get("FF_REPO"):
    sys.path.insert(0, os.environ["FF_REPO"])

BANDS = {"day1-3": (0, 72), "day4-7": (72, 168), "day8-14": (168, 336), "all": (0, 336)}
PARAMETER_NAMES = ("X1", "X2", "X3", "X4", "Df", "Tmax", "Tmin")
EXPERIMENT_MARKER = os.path.join("experiments", "catchment_foundation") + os.sep


def resolve_path(path: str) -> str:
    """
    Resolves a path stored in an old run config.

    Runs made before the experiment moved from Flow Forecast to Water store absolute paths into
    the old checkout; those are re-rooted at this directory.

    :param path: Path as stored (absolute, or relative to the repository root).
    :type path: str
    :return: An existing path where one can be found, otherwise the input.
    :rtype: str
    """
    if os.path.exists(path):
        return path
    if EXPERIMENT_MARKER in path:
        candidate = os.path.join(HERE, path.split(EXPERIMENT_MARKER, 1)[1])
        if os.path.exists(candidate):
            return candidate
    candidate = os.path.join(HERE, "..", "..", path)
    return candidate if os.path.exists(candidate) else path


def basin_error_sums(series: np.ndarray, observed: np.ndarray, basin_index: np.ndarray,
                     n_basins: int, start: int, stop: int) -> np.ndarray:
    """
    Sums each basin's per-window mean squared error over a lead band.

    :param series: Forecasts of shape (windows, horizon).
    :type series: np.ndarray
    :param observed: Observations of the same shape.
    :type observed: np.ndarray
    :param basin_index: Basin number of every window.
    :type basin_index: np.ndarray
    :param n_basins: Number of basins.
    :type n_basins: int
    :param start: First lead hour of the band.
    :type start: int
    :param stop: One past the last lead hour of the band.
    :type stop: int
    :return: Per-basin sums of window MSE, shape (n_basins,).
    :rtype: np.ndarray
    """
    window_mse = ((series[:, start:stop] - observed[:, start:stop]) ** 2).mean(axis=1)
    return np.bincount(basin_index, weights=window_mse, minlength=n_basins)


def skill_summary(model_error: np.ndarray, persistence_error: np.ndarray) -> Dict[str, float]:
    """
    Summarizes skill versus persistence from per-basin error sums.

    :param model_error: Per-basin error sums of the forecast.
    :type model_error: np.ndarray
    :param persistence_error: Per-basin error sums of persistence on the same windows.
    :type persistence_error: np.ndarray
    :return: Pooled skill, median/quartile basin skill, the share of basins beating persistence
        and the largest single-basin share of the pooled persistence error.
    :rtype: Dict[str, float]
    """
    skill = 100.0 * (1.0 - model_error / np.maximum(persistence_error, 1e-30))
    return {
        "pooled_skill_pct": float(100.0 * (1.0 - model_error.sum() / persistence_error.sum())),
        "median_basin_skill_pct": float(np.median(skill)),
        "p25_basin_skill_pct": float(np.percentile(skill, 25)),
        "p75_basin_skill_pct": float(np.percentile(skill, 75)),
        "basins_beating_persistence_pct": float(100.0 * (skill > 0).mean()),
        "top_basin_share_of_pooled_error_pct":
            float(100.0 * persistence_error.max() / persistence_error.sum()),
    }


def paired_summary(error_a: np.ndarray, error_b: np.ndarray, persistence_error: np.ndarray,
                   n_boot: int = 5000, seed: int = 0) -> Dict[str, float]:
    """
    Compares two forecasts basin by basin, resampling basins for the intervals.

    :param error_a: Per-basin error sums of forecast A.
    :type error_a: np.ndarray
    :param error_b: Per-basin error sums of forecast B on the same windows.
    :type error_b: np.ndarray
    :param persistence_error: Per-basin persistence error sums (for the pooled skill difference).
    :type persistence_error: np.ndarray
    :param n_boot: Bootstrap resamples over basins, defaults to 5000.
    :type n_boot: int, optional
    :param seed: Bootstrap seed, defaults to 0.
    :type seed: int, optional
    :return: Share of basins where A is better, the median per-basin MSE reduction of A relative
        to B and the pooled-skill difference (A minus B), each with a 95% interval.
    :rtype: Dict[str, float]
    """
    rng = np.random.default_rng(seed)
    n = len(error_a)
    log_ratio = np.log(np.maximum(error_b, 1e-30) / np.maximum(error_a, 1e-30))

    def reduction(values: np.ndarray) -> float:
        return float(100.0 * (1.0 - np.exp(-np.median(values))))

    def pooled_difference(index: np.ndarray) -> float:
        total = persistence_error[index].sum()
        return float(100.0 * (error_b[index].sum() - error_a[index].sum()) / total)

    draws = rng.integers(0, n, size=(n_boot, n))
    reductions = np.array([reduction(log_ratio[row]) for row in draws])
    pooled = np.array([pooled_difference(row) for row in draws])
    return {
        "basins": int(n),
        "a_better_basins_pct": float(100.0 * (error_a < error_b).mean()),
        "median_mse_reduction_pct": reduction(log_ratio),
        "median_mse_reduction_ci95": [float(np.percentile(reductions, 2.5)),
                                      float(np.percentile(reductions, 97.5))],
        "pooled_skill_difference_points": pooled_difference(np.arange(n)),
        "pooled_skill_difference_ci95": [float(np.percentile(pooled, 2.5)),
                                         float(np.percentile(pooled, 97.5))],
    }


def _build_loader(manifest_path: str, dataset: Dict, basin_split: str, start_date: str,
                  end_date: Optional[str], stride: int):
    """Builds the split loader exactly as the run's config describes its windows."""
    from flood_forecast.preprocessing.pytorch_loaders import MultiBasinWindowLoader
    return MultiBasinWindowLoader(
        manifest_path, dataset["forecast_history"], dataset["forecast_length"],
        dataset["target_col"], dataset["relevant_cols"], scaled_cols=dataset.get("scaled_cols"),
        start_date=start_date, end_date=end_date, basin_split=basin_split, window_stride=stride,
        min_valid_fraction=dataset.get("min_valid_fraction", 0.95),
        require_pretrained_embedding=dataset.get("require_pretrained_embedding", False),
        max_basins=dataset.get("max_basins"))


def _collect(model, loader, batch_size: int = 64, forecasts_only: bool = False) -> Dict:
    """Runs a forecaster over every window; returns mm/hr arrays and each window's basin."""
    import torch
    from torch.utils.data import DataLoader
    local = {position: index for index, position in enumerate(loader.basin_positions)}
    spinup = loader.forecast_history
    sims, observed, persistence, basins = [], [], [], []
    with torch.no_grad():
        for src, trg in DataLoader(loader, batch_size=batch_size, shuffle=False):
            positions = src[:, 0, -1].long()
            scale = model.flow_scales[positions].unsqueeze(1)
            sims.append((model(src) * scale).double())
            if not forecasts_only:
                observed.append((trg[:, :, 0] * scale).double())
                persistence.append(src[:, spinup - 1, 0:1].expand(-1, trg.shape[1]).double())
                basins.append(torch.tensor([local[int(p)] for p in positions]))
    result = {"sim": torch.cat(sims).numpy()}
    if not forecasts_only:
        result.update(obs=torch.cat(observed).numpy(), persist=torch.cat(persistence).numpy(),
                      basin=torch.cat(basins).numpy(), sites=list(loader.basin_site_ids))
    return result


def _band_table(sims: Dict[str, np.ndarray], reference: Dict, compare_to: str) -> Dict:
    """Per-band skill summaries for several forecasts of the same windows, paired to one."""
    n_basins = len(reference["sites"])
    report = {}
    for band, (start, stop) in BANDS.items():
        persistence_error = basin_error_sums(reference["persist"], reference["obs"],
                                             reference["basin"], n_basins, start, stop)
        errors = {label: basin_error_sums(sim, reference["obs"], reference["basin"], n_basins,
                                          start, stop) for label, sim in sims.items()}
        report[band] = {}
        for label, error in errors.items():
            entry = skill_summary(error, persistence_error)
            if label != compare_to:
                entry["versus_" + compare_to] = paired_summary(errors[compare_to], error,
                                                               persistence_error)
            report[band][label] = entry
    return report


def _print_table(report: Dict, compare_to: str) -> None:
    """Prints one row per condition and band."""
    for band, conditions in report.items():
        print("\n%s" % band)
        print("  %-24s %8s %8s %10s   %s"
              % ("condition", "pooled", "median", "beat pers", "%s better on" % compare_to))
        for label, entry in conditions.items():
            versus = entry.get("versus_" + compare_to)
            print("  %-24s %8.1f %8.1f %9.0f%%   %s" % (
                label, entry["pooled_skill_pct"], entry["median_basin_skill_pct"],
                entry["basins_beating_persistence_pct"],
                "" if versus is None else "%.0f%% of basins" % versus["a_better_basins_pct"]))


def _load_run_model(run_dir: str, checkpoint: Optional[str]) -> Tuple[object, Dict, str]:
    """Rebuilds a run's forecaster from its config and loads its best checkpoint."""
    import torch
    from flood_forecast.ode.physics import forecast_training
    with open(os.path.join(run_dir, "config.json")) as file:
        config = json.load(file)
    manifest_path = resolve_path(config["model_params"]["basin_info_path"])
    params = dict(config["model_params"], basin_info_path=manifest_path)
    model = getattr(forecast_training, config["model_name"])(**params)
    state = torch.load(checkpoint or os.path.join(run_dir, "checkpoint.pth"), map_location="cpu",
                       weights_only=True)
    model.load_state_dict(state)
    model.eval()
    return model, config, manifest_path


def context_sensitivity(run_dir: str, checkpoint: Optional[str], basin_split: str,
                        start_date: str, stride: int, seed: int) -> Dict:
    """
    Re-runs a checkpoint's evaluation windows under context interventions.

    :param run_dir: A completed run directory (``config.json`` and ``checkpoint.pth``).
    :type run_dir: str
    :param checkpoint: Optional state_dict path; defaults to the run's best checkpoint.
    :type checkpoint: str, optional
    :param basin_split: Manifest split label of the basins to evaluate.
    :type basin_split: str
    :param start_date: First evaluation date.
    :type start_date: str
    :param stride: Hours between evaluation windows.
    :type stride: int
    :param seed: Seed of the context permutation.
    :type seed: int
    :return: Per-band skill for every intervention and how far the forecasts moved.
    :rtype: Dict
    """
    import torch
    model, config, manifest_path = _load_run_model(run_dir, checkpoint)
    if getattr(model, "fixed_context", None) is None:
        raise SystemExit("This run was trained without catchment context.")
    loader = _build_loader(manifest_path, config["dataset_params"], basin_split, start_date,
                           None, stride)
    positions = torch.tensor(loader.basin_positions)
    original = model.basin_context(positions).detach().clone()
    permutation = torch.randperm(len(positions), generator=torch.Generator().manual_seed(seed))
    conditions = {
        "ordered": original,
        "shuffled": original[permutation],
        "cohort_mean": original.mean(dim=0, keepdim=True).expand_as(original).clone(),
        "zeros": torch.zeros_like(original),
    }
    saved = (model.fixed_context.clone(), model.has_fixed_context.clone())
    reference, sims = None, {}
    for label, context in conditions.items():
        with torch.no_grad():
            model.fixed_context[positions] = context
            model.has_fixed_context[positions] = True
        output = _collect(model, loader, forecasts_only=reference is not None)
        reference = reference or output
        sims[label] = output["sim"]
    with torch.no_grad():
        model.fixed_context.copy_(saved[0])
        model.has_fixed_context.copy_(saved[1])
    departure = float(np.sqrt(((sims["ordered"] - reference["persist"]) ** 2).mean()))
    return {
        "run_dir": os.path.abspath(run_dir), "model": config["model_name"],
        "basin_split": basin_split, "basins": len(reference["sites"]),
        "windows": int(reference["sim"].shape[0]),
        "rms_departure_from_persistence_mm_hr": departure,
        "forecast_change_pct_of_departure": {
            label: float(100.0 * np.sqrt(((sim - sims["ordered"]) ** 2).mean()) / departure)
            for label, sim in sims.items() if label != "ordered"},
        "bands": _band_table(sims, reference, "ordered"),
    }


def _parameter_tables(sites: List[str], calibrated: Dict[str, Dict], head,
                      extra: Dict[str, Dict], seed: int) -> Dict[str, Dict[str, List[float]]]:
    """Builds the per-site parameter sets of every headroom condition."""
    import torch
    values = np.array([[calibrated[site][name] for name in PARAMETER_NAMES] for site in sites])
    shared = np.median(values, axis=0)
    rng = np.random.default_rng(seed)
    permutation = rng.permutation(len(sites))
    while len(sites) > 1 and (permutation == np.arange(len(sites))).any():
        permutation = rng.permutation(len(sites))
    middle = ((torch.cat([head.lower, head.snow_lower]) +
               torch.cat([head.upper, head.snow_upper])) / 2.0).numpy()
    midpoint = list(middle[:6]) + [middle[5] - middle[6]]  # Tmin = Tmax - delta
    tables = {
        "calibrated": {site: list(values[i]) for i, site in enumerate(sites)},
        "shared_median": {site: list(shared) for site in sites},
        "calibrated_shuffled": {site: list(values[permutation[i]])
                                for i, site in enumerate(sites)},
        "midpoint_untrained_head": {site: midpoint for site in sites},
    }
    for label, lookup in extra.items():
        tables[label] = {site: [lookup[site][name] for name in PARAMETER_NAMES]
                         for site in sites}
    return tables


def parameter_headroom(manifest_path: str, calibration_path: str, extra: Dict[str, str],
                       production_init_fraction: float, start_date: str, stride: int,
                       seed: int, work_dir: str) -> Dict:
    """
    Scores the pure-physics hybrid forecast under different GR4/snow parameter assignments.

    :param manifest_path: Forecast manifest with the basins' forcing and snow series.
    :type manifest_path: str
    :param calibration_path: ``calibrate_fleet.py`` output (site -> parameters, status).
    :type calibration_path: str
    :param extra: Further parameter files by label (``parameters`` per site), e.g. seed means.
    :type extra: Dict[str, str]
    :param production_init_fraction: Production-store fraction of X1 at the spin-up start.
    :type production_init_fraction: float
    :param start_date: First evaluation date.
    :type start_date: str
    :param stride: Hours between evaluation windows.
    :type stride: int
    :param seed: Seed of the shuffled assignment.
    :type seed: int
    :param work_dir: Directory for the relabelled manifest copy.
    :type work_dir: str
    :return: Per-band skill of every parameter assignment, each paired with "calibrated".
    :rtype: Dict
    """
    import torch
    from flood_forecast.ode.physics.forecast_training import HybridGR4MultiBasin
    from experiments.catchment_foundation.run_training import (
        HORIZON_HOURS, PHYS_COLS, RELEVANT_COLS, SCALED_COLS, SPINUP_HOURS)

    with open(manifest_path) as file:
        manifest = json.load(file)
    with open(calibration_path) as file:
        calibrated = {site: entry["parameters"] for site, entry in json.load(file).items()
                      if entry.get("status", "ok") == "ok" and "parameters" in entry}
    extra_tables = {}
    for label, path in extra.items():
        with open(path) as file:
            extra_tables[label] = {site: entry["parameters"]
                                   for site, entry in json.load(file).items()}
    eligible = set(calibrated).intersection(*[set(table) for table in extra_tables.values()])
    manifest["embedding_path"] = None
    for basin in manifest["basins"]:
        basin["has_embedding"] = False
        basin["split"] = "headroom" if basin["site_id"] in eligible else "unused"
    os.makedirs(work_dir, exist_ok=True)
    work_manifest = os.path.join(work_dir, "headroom_manifest.json")
    with open(work_manifest, "w") as file:
        json.dump(manifest, file)

    columns = RELEVANT_COLS + ["snodas_swe_mm"] + PHYS_COLS
    dataset = {"forecast_history": SPINUP_HOURS, "forecast_length": HORIZON_HOURS,
               "target_col": ["cfs"], "relevant_cols": columns, "scaled_cols": SCALED_COLS}
    loader = _build_loader(work_manifest, dataset, "headroom", start_date, None, stride)
    model = HybridGR4MultiBasin(
        n_time_series=len(columns) + 1, spinup_length=SPINUP_HOURS,
        forecast_length=HORIZON_HOURS, raw_temp_index=columns.index("temp_lapse_k"),
        raw_sw_index=columns.index("sw_raw"), basin_info_path=work_manifest, snow=True,
        swe_index=columns.index("snodas_swe_mm"),
        phys_indices=[columns.index(column) for column in PHYS_COLS], anchored=True,
        use_multiplier=False, use_asos_gate=False, normalize_context=False,
        production_store_init_fraction=production_init_fraction)
    model.eval()
    n_manifest = len(manifest["basins"])
    position = {basin["site_id"]: i for i, basin in enumerate(manifest["basins"])}
    with torch.no_grad():  # the context only carries the basin's manifest position
        model.fixed_context.zero_()
        model.fixed_context[:, 0] = torch.arange(n_manifest, dtype=torch.float32)
        model.has_fixed_context[:] = True

    class TableHead(torch.nn.Module):
        """Stands in for the parameter head: looks parameters up by manifest position."""

        def __init__(self, table: torch.Tensor):
            super().__init__()
            self.register_buffer("table", table)

        def forward(self, context: torch.Tensor) -> torch.Tensor:
            return self.table[context[:, 0].round().long()]

    sites = list(loader.basin_site_ids)
    tables = _parameter_tables(sites, calibrated, model.hybrid.parameter_head, extra_tables,
                               seed)
    reference, sims = None, {}
    for label, lookup in tables.items():
        table = torch.zeros(n_manifest, len(PARAMETER_NAMES))
        for site in sites:
            table[position[site]] = torch.tensor([float(v) for v in lookup[site]])
        model.hybrid.parameter_head = TableHead(table)
        output = _collect(model, loader, batch_size=128, forecasts_only=reference is not None)
        reference = reference or output
        sims[label] = output["sim"]
    return {
        "manifest": os.path.abspath(manifest_path),
        "calibration": os.path.abspath(calibration_path),
        "production_init_fraction": production_init_fraction, "basins": len(sites),
        "windows": int(reference["sim"].shape[0]),
        "bands": _band_table(sims, reference, "calibrated"),
    }


def run_audit(run_dir: str, versus: Optional[str]) -> Dict:
    """
    Checks a finished run's validation trajectory and the concentration of its pooled skill.

    :param run_dir: A completed run directory.
    :type run_dir: str
    :param versus: Optional second run evaluated on the same windows, paired basin by basin.
    :type versus: str, optional
    :return: Validation numbers next to persistence's, and per-split concentration statistics.
    :rtype: Dict
    """
    import torch
    from torch.utils.data import DataLoader
    with open(os.path.join(run_dir, "config.json")) as file:
        config = json.load(file)
    dataset = config["dataset_params"]
    manifest_path = resolve_path(dataset["validation_path"])
    loader = _build_loader(manifest_path, dataset, dataset["valid_basin_split"],
                           dataset["valid_start_date"], dataset["valid_end_date"],
                           dataset["valid_window_stride"])
    with open(manifest_path) as file:
        scales = torch.tensor([float(b["flow_scale_mm_hr"]) for b in json.load(file)["basins"]])
    window_error = []
    for src, trg in DataLoader(loader, batch_size=256, shuffle=False):
        persistence = src[:, dataset["forecast_history"] - 1, 0] / scales[src[:, 0, -1].long()]
        window_error.append(((trg[:, :, 0] - persistence.unsqueeze(1)) ** 2).mean(dim=1))
    window_error = torch.cat(window_error).double().numpy()
    history = []
    for path in glob.glob(os.path.join(run_dir, "*.json")):
        with open(path) as file:
            content = json.load(file)
        if isinstance(content, dict) and "run" in content:
            history = [float(epoch["validation_loss"]) for epoch in content["run"]]
    ordered = np.sort(window_error)[::-1]
    report = {
        "run_dir": os.path.abspath(run_dir),
        "validation": {
            # The trainer's validation number is the SUM of per-window standardized MSE.
            "windows": int(len(window_error)),
            "persistence_would_score": float(window_error.sum()),
            "model_by_epoch": history,
            "best_epoch": int(np.argmin(history)) if history else None,
            "best_vs_persistence_pct":
                float(100.0 * (min(history) / window_error.sum() - 1.0)) if history else None,
            "top5_windows_share_of_persistence_error_pct":
                float(100.0 * ordered[:5].sum() / ordered.sum()),
        },
        "evaluation": {},
    }
    for cases_path in sorted(glob.glob(os.path.join(run_dir, "eval_*",
                                                    "forecast_gallery_cases.json"))):
        split = os.path.basename(os.path.dirname(cases_path))[len("eval_"):]
        sites, model_error, persistence_error, windows = _case_errors(cases_path)
        top_windows = np.sort(windows)[::-1]
        entry = skill_summary(model_error, persistence_error)
        entry.update(basins=len(sites), windows=int(len(windows)),
                     top5_windows_share_of_pooled_error_pct=float(
                         100.0 * top_windows[:5].sum() / top_windows.sum()))
        if versus:
            other_path = os.path.join(versus, "eval_" + split, "forecast_gallery_cases.json")
            if os.path.exists(other_path):
                other_sites, other_error, other_persistence, _ = _case_errors(other_path)
                if other_sites != sites or not np.allclose(other_persistence, persistence_error):
                    raise SystemExit("%s: the two runs did not score the same windows" % split)
                entry["versus"] = dict(paired_summary(model_error, other_error,
                                                      persistence_error),
                                       other_run=os.path.abspath(versus))
        report["evaluation"][split] = entry
    return report


def _case_errors(cases_path: str) -> Tuple[List[str], np.ndarray, np.ndarray, np.ndarray]:
    """Reads all-lead per-window errors saved by ``evaluate.py`` and sums them by basin."""
    with open(cases_path) as file:
        cases = json.load(file)["all_cases"]
    sites = sorted({case["site_id"] for case in cases})
    index = {site: i for i, site in enumerate(sites)}
    basin = np.array([index[case["site_id"]] for case in cases])
    model = np.array([case["metrics"]["mse_mm_hr2"] for case in cases])
    persistence = np.array([case["metrics"]["persistence_mse_mm_hr2"] for case in cases])
    return (sites, np.bincount(basin, weights=model, minlength=len(sites)),
            np.bincount(basin, weights=persistence, minlength=len(sites)), persistence)


def _parse_labelled_path(specification: str) -> Tuple[str, str]:
    """Parses ``label=/path`` CLI values."""
    label, separator, path = specification.partition("=")
    if not separator or not label or not path:
        raise argparse.ArgumentTypeError("use label=/path/to/file.json")
    return label, path


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)

    context = commands.add_parser("context", help="context interventions on a checkpoint")
    context.add_argument("--run-dir", required=True)
    context.add_argument("--checkpoint", default=None)
    context.add_argument("--basin-split", default="train")
    context.add_argument("--start-date", default="2023-01-01")
    context.add_argument("--stride", type=int, default=336)
    context.add_argument("--seed", type=int, default=0)

    headroom = commands.add_parser("headroom", help="hybrid skill by parameter assignment")
    headroom.add_argument("--manifest",
                          default=os.path.join(HERE, "manifests", "cout_manifest_v3.json"))
    headroom.add_argument("--calibration",
                          default=os.path.join(HERE, "calibration", "cout_calibration_v2.json"))
    headroom.add_argument("--extra", action="append", type=_parse_labelled_path, default=[],
                          help="label=path of another per-site parameter file")
    headroom.add_argument("--production-init-fraction", type=float, default=0.3)
    headroom.add_argument("--start-date", default="2023-01-01")
    headroom.add_argument("--stride", type=int, default=336)
    headroom.add_argument("--seed", type=int, default=42)
    headroom.add_argument("--work-dir", default=os.path.join(HERE, "calibration", "headroom"))

    audit = commands.add_parser("audit", help="validation reference and pooled concentration")
    audit.add_argument("--run-dir", required=True)
    audit.add_argument("--versus", default=None)

    for sub in (context, headroom, audit):
        sub.add_argument("--output", default=None, help="Optional JSON report path")
    args = parser.parse_args()

    if args.command == "context":
        report = context_sensitivity(args.run_dir, args.checkpoint, args.basin_split,
                                     args.start_date, args.stride, args.seed)
        print("%s: %d basins, %d windows" % (report["model"], report["basins"],
                                             report["windows"]))
        _print_table(report["bands"], "ordered")
        print("\nforecast change as a share of the model's own departure from persistence:")
        for label, value in report["forecast_change_pct_of_departure"].items():
            print("  %-12s %.1f%%" % (label, value))
    elif args.command == "headroom":
        report = parameter_headroom(args.manifest, args.calibration, dict(args.extra),
                                    args.production_init_fraction, args.start_date,
                                    args.stride, args.seed, args.work_dir)
        print("production-store init fraction %.2f: %d basins, %d windows"
              % (report["production_init_fraction"], report["basins"], report["windows"]))
        _print_table(report["bands"], "calibrated")
    else:
        report = run_audit(args.run_dir, args.versus)
        validation = report["validation"]
        print("validation (%d windows): persistence would score %.1f; model by epoch %s"
              % (validation["windows"], validation["persistence_would_score"],
                 [round(value, 1) for value in validation["model_by_epoch"]]))
        for split, entry in report["evaluation"].items():
            print("%s: %d basins, %d windows; pooled %.1f, median basin %.1f, %.0f%% of basins "
                  "beat persistence; top basin holds %.0f%% and top 5 windows %.0f%% of the "
                  "pooled error"
                  % (split, entry["basins"], entry["windows"], entry["pooled_skill_pct"],
                     entry["median_basin_skill_pct"], entry["basins_beating_persistence_pct"],
                     entry["top_basin_share_of_pooled_error_pct"],
                     entry["top5_windows_share_of_pooled_error_pct"]))
            if "versus" in entry:
                versus = entry["versus"]
                print("   versus the other run: better on %.0f%% of %d basins; pooled skill "
                      "difference %.1f points (95%% interval %.1f to %.1f)"
                      % (versus["a_better_basins_pct"], versus["basins"],
                         versus["pooled_skill_difference_points"],
                         *versus["pooled_skill_difference_ci95"]))
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w") as file:
            json.dump(report, file, indent=1)


if __name__ == "__main__":
    main()
