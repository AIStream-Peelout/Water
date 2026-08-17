"""
Milestone retraining of the catchment embedding encoder over one or more collected states.

Per the project policy, the contrastive stage is retrained at each data milestone (new states
or recovered gauges) and every run writes a fresh versioned artifact directory containing the
encoder weights, the frozen embedding bank (``embeddings_<fusion>.pt`` with ``site_ids`` +
``embeddings``, the format consumed by the flow-forecast manifests), a training summary and
the interactive explorer HTML. State .npz records stay in their per-state directories; this
script only symlinks them into a combined view for training.
"""
import argparse
import json
import os
import sys
import time
from typing import List

import pandas as pd
import torch

sys.path.insert(0, os.environ.get("FF_REPO", "/Users/isaac/Documents/GitHub/flow-forecast"))


def build_combined_dir(state_dirs: List[str], combined_dir: str) -> int:
    """
    Symlinks every state's .npz records into one training directory.

    :param state_dirs: Per-state record directories (each holding <site>.npz files).
    :type state_dirs: List[str]
    :param combined_dir: Directory to populate with symlinks.
    :type combined_dir: str
    :return: The number of linked records.
    :rtype: int
    """
    os.makedirs(combined_dir, exist_ok=True)
    linked = 0
    for state_dir in state_dirs:
        for name in sorted(os.listdir(state_dir)):
            if not name.endswith(".npz"):
                continue
            target = os.path.join(combined_dir, name)
            if not os.path.exists(target):
                os.symlink(os.path.abspath(os.path.join(state_dir, name)), target)
            linked += 1
    return linked


def combine_gauges_csv(scrape_dirs: List[str], output_path: str) -> None:
    """
    Concatenates per-state gauges.csv files (station names for the explorer popups).

    :param scrape_dirs: Per-state scrape directories containing gauges.csv.
    :type scrape_dirs: List[str]
    :param output_path: Combined CSV output path.
    :type output_path: str
    :return: None
    :rtype: None
    """
    frames = [pd.read_csv(os.path.join(d, "gauges.csv"), dtype={"site_no": str})
              for d in scrape_dirs if os.path.exists(os.path.join(d, "gauges.csv"))]
    pd.concat(frames).drop_duplicates("site_no").to_csv(output_path, index=False)


def main() -> None:
    """
    CLI entry point: trains requested fusion variants and extracts embedding banks.

    :return: None
    :rtype: None
    """
    from flood_forecast.multi_models.catchment_embedding import CatchmentEncoder
    from flood_forecast.multi_models.contrastive_pretrain import (
        extract_embeddings, pretrain_catchment_encoder)
    from flood_forecast.preprocessing.catchment_loader import CatchmentEmbeddingDataset

    parser = argparse.ArgumentParser(description="Retrain catchment embeddings at a milestone.")
    parser.add_argument("--states", nargs="+", default=["CO", "UT"])
    parser.add_argument("--data-root", default=os.path.join("pilot_data", "embedding_dataset"))
    parser.add_argument("--scrape-root", default=os.path.join("pilot_data", "scrapes"))
    parser.add_argument("--output-dir", required=True,
                        help="Versioned artifact directory, e.g. "
                             "pilot_data/embedding_dataset/COUT_v2")
    parser.add_argument("--fusions", nargs="+", default=["concat", "cross_attention"])
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--lr", type=float, default=3e-4)
    parser.add_argument("--device", default="mps" if torch.backends.mps.is_available()
                        else "cpu")
    parser.add_argument("--n-history-samples", type=int, default=4,
                        help="History windows averaged per site during extraction "
                             "(random_window mode; hourly_panel is deterministic)")
    parser.add_argument("--history-mode", default="random_window",
                        choices=["random_window", "hourly_panel"],
                        help="Legacy random daily windows, or deterministic hourly "
                             "seasonal/extreme panels (requires panel records)")
    parser.add_argument("--cross-year", action="store_true",
                        help="Train with different-year panel views as positives "
                             "(hourly_panel only)")
    parser.add_argument("--blocked-batches", action="store_true",
                        help="Batch site-number-adjacent gauges together (harder negatives)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--no-wandb", action="store_true")
    args = parser.parse_args()
    torch.manual_seed(args.seed)

    os.makedirs(args.output_dir, exist_ok=True)
    combined_dir = os.path.join(args.output_dir, "combined_records")
    state_dirs = [os.path.join(args.data_root, state) for state in args.states]
    n_records = build_combined_dir(state_dirs, combined_dir)
    combine_gauges_csv([os.path.join(args.scrape_root, state) for state in args.states],
                       os.path.join(args.output_dir, "gauges.csv"))
    print("linked %d records from %s" % (n_records, ", ".join(args.states)))

    wandb_run = None
    if not args.no_wandb:
        import wandb
        wandb_run = wandb.init(project="catchment-foundation",
                               name="embed_" + os.path.basename(args.output_dir),
                               config={"states": args.states, "n_records": n_records,
                                       "epochs": args.epochs, "batch_size": args.batch_size,
                                       "lr": args.lr, "fusions": args.fusions})

    summary = {"states": args.states, "n_records": n_records,
               "config": {"epochs": args.epochs, "batch_size": args.batch_size, "lr": args.lr,
                          "device": args.device, "history_mode": args.history_mode,
                          "cross_year": args.cross_year, "blocked_batches": args.blocked_batches,
                          "seed": args.seed, "data_root": args.data_root}}
    for fusion in args.fusions:
        dataset = CatchmentEmbeddingDataset(combined_dir, seed=args.seed,
                                            history_mode=args.history_mode,
                                            cross_year_views=args.cross_year)
        sample = dataset[0]
        panel = args.history_mode == "hourly_panel"
        encoder = CatchmentEncoder(image_size=tuple(sample["image"].shape[1:]),
                                   image_channels=sample["image"].shape[0],
                                   static_features=dataset.static_features,
                                   history_features=sample["history"].shape[-1],
                                   history_len=sample["history"].shape[-2],
                                   history_mode="panel" if panel else "sequence",
                                   fusion=fusion)
        start = time.time()
        losses = pretrain_catchment_encoder(encoder, dataset, epochs=args.epochs,
                                            batch_size=args.batch_size, lr=args.lr,
                                            device=args.device, wandb_run=wandb_run,
                                            cross_year_views=args.cross_year,
                                            blocked_batches=args.blocked_batches,
                                            seed=args.seed)
        # Extraction always uses the deterministic canonical panel (no cross-year sampling).
        extract_dataset = CatchmentEmbeddingDataset(combined_dir, seed=args.seed,
                                                    history_mode=args.history_mode) \
            if args.cross_year else dataset
        site_ids, embeddings = extract_embeddings(encoder, extract_dataset, device=args.device,
                                                  n_history_samples=args.n_history_samples)
        torch.save(encoder.state_dict(),
                   os.path.join(args.output_dir, "encoder_%s.pt" % fusion))
        torch.save({"site_ids": site_ids, "embeddings": embeddings},
                   os.path.join(args.output_dir, "embeddings_%s.pt" % fusion))
        summary[fusion] = {"first_loss": losses[0], "final_loss": losses[-1],
                           "minutes": round((time.time() - start) / 60.0, 2),
                           "n_sites": len(site_ids)}
        print("%s: loss %.3f -> %.3f (%d sites)"
              % (fusion, losses[0], losses[-1], len(site_ids)))
    with open(os.path.join(args.output_dir, "training_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    if wandb_run is not None:
        wandb_run.summary.update(summary)
        wandb_run.finish()


if __name__ == "__main__":
    main()
