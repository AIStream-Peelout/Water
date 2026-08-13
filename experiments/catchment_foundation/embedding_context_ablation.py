"""Test whether a trained hybrid forecast actually uses its catchment contexts.

This is an inference-time intervention, not a retraining comparison.  It loads one completed
``HybridGR4MultiBasin`` checkpoint and re-runs exactly the same evaluation windows after replacing
the selected basins' context rows with one of:

* the checkpoint's original contexts;
* a deterministic permutation of those contexts across basins;
* their cohort mean; or
* site-matched vectors from another embedding bank.

Prediction deltas answer the narrow causal question "does this fitted checkpoint react to basin
identity?"  They do not by themselves rank two embedding-training recipes, because the downstream
head was co-adapted to the bank used during training.
"""
import argparse
import copy
import json
import os
import sys
from typing import Dict, Iterable, Tuple

import numpy as np
import torch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from experiments.catchment_foundation.evaluate import collect_split_outputs, pooled_metrics
from flood_forecast.preprocessing.pytorch_loaders import MultiBasinWindowLoader
from flood_forecast.time_model import PyTorchForecast


PARAMETER_NAMES = ("X1", "X2", "X3", "X4", "Df", "Tmax", "Tmin")


def _parse_bank(specification: str) -> Tuple[str, str]:
    """Parses ``label=/path/to/bank.pt`` CLI values."""
    label, separator, path = specification.partition("=")
    if not separator or not label or not path:
        raise argparse.ArgumentTypeError("embedding banks must use label=/path/to/bank.pt")
    return label, path


def _make_loader(manifest_path: str, params: Dict, basin_split: str,
                 eval_stride: int) -> MultiBasinWindowLoader:
    """Builds a 2023 evaluation loader restricted to pretrained-embedding basins."""
    dataset = params["dataset_params"]
    return MultiBasinWindowLoader(
        manifest_path, dataset["forecast_history"], dataset["forecast_length"],
        dataset["target_col"], dataset["relevant_cols"],
        scaled_cols=dataset.get("scaled_cols"), start_date="2023-01-01",
        basin_split=basin_split, window_stride=eval_stride,
        min_valid_fraction=dataset.get("min_valid_fraction", 0.95),
        require_pretrained_embedding=True)


def _bank_contexts(path: str, sites: Iterable[str]) -> torch.Tensor:
    """Returns site-matched context rows from an external bank."""
    bank = torch.load(path, map_location="cpu", weights_only=True)
    lookup = {str(site): index for index, site in enumerate(bank["site_ids"])}
    missing = [site for site in sites if site not in lookup]
    if missing:
        raise ValueError("Bank %s is missing %d requested sites: %s" %
                         (path, len(missing), ", ".join(missing[:10])))
    return torch.stack([bank["embeddings"][lookup[site]] for site in sites])


def _context_diagnostics(model, positions: torch.Tensor) -> Dict:
    """Summarizes emitted parameters and the optional static ASOS gate by basin."""
    with torch.no_grad():
        contexts = model.basin_context(positions)
        emitted = model.hybrid.parameter_head(contexts).cpu()
        gate_net = getattr(model.hybrid.forcing_generator, "gate_net", None)
        gates = torch.sigmoid(gate_net(contexts)).flatten().cpu() if gate_net else None
    parameters = {}
    for column, name in enumerate(PARAMETER_NAMES):
        values = emitted[:, column].numpy()
        parameters[name] = {
            "min": float(values.min()), "median": float(np.median(values)),
            "max": float(values.max()), "std": float(values.std()),
        }
    result = {"parameters": parameters}
    if gates is not None:
        values = gates.numpy()
        result["asos_gate"] = {
            "min": float(values.min()), "median": float(np.median(values)),
            "max": float(values.max()), "std": float(values.std()),
        }
    return result


def _prediction_vector(outputs: Dict[str, Dict]) -> torch.Tensor:
    """Flattens site-sorted predictions for paired intervention comparisons."""
    return torch.cat([outputs[site]["sim"].reshape(-1) for site in sorted(outputs)])


def run_ablation(run_dir: str, checkpoint: str, manifest_path: str,
                 split_names: Iterable[str], banks: Dict[str, str], seed: int,
                 eval_stride: int) -> Dict:
    """Runs all context interventions and returns a JSON-serializable report."""
    with open(os.path.join(run_dir, "config.json")) as file:
        params = json.load(file)
    params = copy.deepcopy(params)
    params["wandb"] = False
    params["model_params"]["basin_info_path"] = manifest_path
    for key in ("training_path", "validation_path", "test_path"):
        params["dataset_params"][key] = manifest_path

    wrapper = PyTorchForecast(params["model_name"], manifest_path, manifest_path,
                              manifest_path, params)
    state = torch.load(checkpoint, map_location=wrapper.device, weights_only=True)
    wrapper.model.load_state_dict(state)
    model = wrapper.model
    original_context = model.fixed_context.detach().clone()
    generator = torch.Generator().manual_seed(seed)

    report = {
        "run_dir": os.path.abspath(run_dir),
        "checkpoint": os.path.abspath(checkpoint),
        "manifest": os.path.abspath(manifest_path),
        "seed": seed,
        "eval_stride": eval_stride,
        "note": ("Inference-time sensitivity only; external-bank results do not rank representation "
                 "training recipes because the fitted head co-adapted to its original bank."),
        "splits": {},
    }
    split_map = {"gauged_2023": "train", "basin_valid_2023": "basin_valid",
                 "ungauged_2023": "holdout"}
    for split_name in split_names:
        basin_split = split_map[split_name]
        loader = _make_loader(manifest_path, params, basin_split, eval_stride)
        positions = torch.tensor(loader.basin_positions, dtype=torch.long,
                                 device=model.fixed_context.device)
        sites = loader.basin_site_ids
        cohort = original_context[positions].clone()
        order = torch.randperm(len(positions), generator=generator)
        conditions = {
            "ordered": cohort,
            "shuffled": cohort[order],
            "cohort_mean": cohort.mean(dim=0, keepdim=True).expand_as(cohort),
        }
        for label, path in banks.items():
            conditions[label] = _bank_contexts(path, sites).to(model.fixed_context.device)

        split_report = {"n_basins": len(sites), "n_windows": len(loader),
                        "site_ids": sites, "conditions": {}}
        ordered_predictions = None
        for label, contexts in conditions.items():
            model.fixed_context.copy_(original_context)
            model.fixed_context[positions] = contexts
            outputs = collect_split_outputs(model, loader)
            predictions = _prediction_vector(outputs)
            if ordered_predictions is None:
                ordered_predictions = predictions
            delta = predictions - ordered_predictions
            split_report["conditions"][label] = {
                "pooled_metrics": pooled_metrics(outputs),
                "prediction_delta_vs_ordered": {
                    "mae_mm_hr": float(delta.abs().mean()),
                    "rmse_mm_hr": float(torch.sqrt(torch.mean(delta ** 2))),
                    "max_abs_mm_hr": float(delta.abs().max()),
                },
                "context_diagnostics": _context_diagnostics(model, positions),
            }
        model.fixed_context.copy_(original_context)
        report["splits"][split_name] = split_report
    return report


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--split", action="append", dest="splits",
                        choices=("gauged_2023", "basin_valid_2023", "ungauged_2023"))
    parser.add_argument("--bank", action="append", type=_parse_bank, default=[])
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--eval-stride", type=int, default=336)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    config_path = os.path.join(args.run_dir, "config.json")
    with open(config_path) as file:
        config = json.load(file)
    manifest_path = args.manifest or config["dataset_params"]["training_path"]
    checkpoint = args.checkpoint or os.path.join(args.run_dir, "checkpoint.pth")
    splits = args.splits or ["gauged_2023", "basin_valid_2023"]
    report = run_ablation(args.run_dir, checkpoint, manifest_path, splits,
                          dict(args.bank), args.seed, args.eval_stride)
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
        with open(args.output, "w") as file:
            file.write(rendered + "\n")


if __name__ == "__main__":
    main()
