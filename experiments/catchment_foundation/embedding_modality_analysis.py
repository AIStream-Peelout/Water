"""
Modality attribution for a trained catchment encoder: what actually forms the representation.

The InfoNCE objective in :mod:`flood_forecast.meta_models.contrastive_train` aligns the
per-modality *contrastive heads*; the fused ``projection`` MLP that produces the bank is never in
the loss (verified: its weights are bit-identical to init after training). The bank is therefore
a random non-linear map of ``[vision_pooled | tabular_pooled | history_pooled]`` under a joint
LayerNorm, so "how much is each modality used" reduces to four measurable questions answered
here for one trained encoder:

1. **Magnitude share** — how much of the concat's across-site variance each tower's block owns
   (the block that dominates the joint LayerNorm dominates the random projection).
2. **Knockout sensitivity** — replace one tower's block by its site-mean (or a permutation
   across sites) and measure how far the fused embedding moves and whether it still retrieves
   its own site (identity retention).
3. **Cross-modal retrieval** — top-1 accuracy of the contrastive-head projections at
   retrieving the same site across modality pairs: what the loss actually optimized, and how
   much site identity each modality carries on its own.
4. **Per-tower signature probes** — ridge R-squared to flow signatures from each tower's
   pooled features, from the random-projection bank, and from alternative banks built from
   the *trained* representations (L2-normalized concat of pooled towers; concat of contrastive
   projections). The alternative banks are saved next to the encoder for reuse.

Example::

    FF_REPO=... python embedding_modality_analysis.py \\
        --version-dir pilot_data/embedding_dataset_hourly_pre2022/COUT_v4c_pre2022 \\
        --states CO UT --output calibration/modality_coutv4c_pre2022.json
"""
import argparse
import json
import os
import sys
from typing import Dict, List, Tuple

import numpy as np
import torch

sys.path.insert(0, os.environ.get("FF_REPO", "/Users/isaac/Documents/GitHub/flow-forecast"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from embedding_probes import build_signature_table, ridge_probe_r2  # noqa: E402

WATER_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MODALITIES = ("vision", "tabular", "history")
INPUT_KEYS = {"vision": "image", "tabular": "static", "history": "history"}


def seasonal_members(history: torch.Tensor) -> torch.Tensor:
    """
    Drops flood/drought panel members, keeping the four seasonal slices (the training view).

    :param history: Panel history of shape (batch, n_members, member_len, 6) whose last two
        channels are the flood/drought member flags.
    :type history: torch.Tensor
    :return: History restricted to members with both flags off.
    :rtype: torch.Tensor
    """
    keep = (history[:, :, 0, 4] == 0) & (history[:, :, 0, 5] == 0)
    n_keep = int(keep[0].sum())
    return history[keep].reshape(history.shape[0], n_keep, *history.shape[2:])


@torch.no_grad()
def encode_towers(encoder, dataset, device: str, batch_size: int = 32,
                  seasonal_only: bool = False, alt_dataset=None) -> Dict[str, torch.Tensor]:
    """
    Runs every site through the encoder, keeping per-tower pooled features and projections.

    :param encoder: The trained CatchmentEncoder (eval mode).
    :type encoder: CatchmentEncoder
    :param dataset: The canonical-panel embedding dataset (extraction view).
    :type dataset: CatchmentEmbeddingDataset
    :param device: Torch device string.
    :type device: str
    :param batch_size: Inference batch size, defaults to 32.
    :type batch_size: int, optional
    :param seasonal_only: Drop the flood/drought members from the canonical history so the
        extraction view matches the seasonal-only cross-year training view, defaults to False.
    :type seasonal_only: bool, optional
    :param alt_dataset: Optional cross-year dataset (same site order) whose "history_alt" view
        is encoded for the history->history_alt retrieval test, defaults to None.
    :type alt_dataset: CatchmentEmbeddingDataset, optional
    :return: Dict with "pooled_<m>", "proj_<m>" (contrastive head), "embedding" (fused bank
        output) and optionally "proj_history_alt", each of shape (n_sites, dim), on CPU.
    :rtype: Dict[str, torch.Tensor]
    """
    loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=False)
    alt_iter = iter(torch.utils.data.DataLoader(alt_dataset, batch_size=batch_size,
                                                shuffle=False)) if alt_dataset else None
    collected: Dict[str, List[torch.Tensor]] = {}
    for batch in loader:
        inputs = {name: batch[key].to(device) for name, key in INPUT_KEYS.items()}
        if seasonal_only:
            inputs["history"] = seasonal_members(inputs["history"])
        outputs = {name: tower(inputs[name]) for name, tower in encoder.encoders.items()}
        pooled = {name: out.mean(dim=1) if out.dim() == 3 else out
                  for name, out in outputs.items()}
        fused = torch.cat([pooled[name] for name in encoder.encoders], dim=-1)
        parts = {"embedding": encoder.projection(fused)}
        for name in MODALITIES:
            parts["pooled_" + name] = pooled[name]
            parts["proj_" + name] = encoder.contrastive_heads[name](pooled[name])
        if alt_iter is not None:
            alt = encoder.encoders["history"](next(alt_iter)["history_alt"].to(device))
            alt = alt.mean(dim=1) if alt.dim() == 3 else alt
            parts["proj_history_alt"] = encoder.contrastive_heads["history"](alt)
        for key, value in parts.items():
            collected.setdefault(key, []).append(value.detach().cpu())
    return {key: torch.cat(values) for key, values in collected.items()}


def variance_share(pooled: Dict[str, torch.Tensor]) -> Dict[str, float]:
    """
    Fraction of the concat's across-site variance owned by each tower's block.

    :param pooled: Modality -> pooled features (n_sites, dim).
    :type pooled: Dict[str, torch.Tensor]
    :return: Modality -> variance share (sums to 1).
    :rtype: Dict[str, float]
    """
    totals = {name: float(feat.var(dim=0, unbiased=False).sum()) for name, feat in pooled.items()}
    grand = sum(totals.values()) or 1.0
    return {name: round(value / grand, 3) for name, value in totals.items()}


@torch.no_grad()
def knockout(encoder, pooled: Dict[str, torch.Tensor], embedding: torch.Tensor,
             seed: int = 0) -> Dict[str, Dict[str, float]]:
    """
    Measures how the fused embedding responds to removing one tower's information.

    :param encoder: The encoder (for its projection head).
    :type encoder: CatchmentEncoder
    :param pooled: Modality -> pooled features (n_sites, dim), CPU.
    :type pooled: Dict[str, torch.Tensor]
    :param embedding: The original fused embeddings (n_sites, embedding_dim).
    :type embedding: torch.Tensor
    :param seed: Permutation seed, defaults to 0.
    :type seed: int, optional
    :return: Modality -> {"mean_fill_cosine", "mean_fill_identity", "permute_cosine",
        "permute_identity"}: cosine to the original embedding and top-1 self-retrieval rate.
    :rtype: Dict[str, Dict[str, float]]
    """
    projection = encoder.projection.cpu()
    reference = torch.nn.functional.normalize(embedding, dim=-1)
    generator = torch.Generator().manual_seed(seed)
    n_sites = embedding.shape[0]
    results = {}
    for name in MODALITIES:
        results[name] = {}
        for mode in ("mean_fill", "permute"):
            blocks = dict(pooled)
            if mode == "mean_fill":
                blocks[name] = pooled[name].mean(dim=0, keepdim=True).expand_as(pooled[name])
            else:
                blocks[name] = pooled[name][torch.randperm(n_sites, generator=generator)]
            fused = torch.cat([blocks[m] for m in encoder.encoders], dim=-1)
            altered = torch.nn.functional.normalize(projection(fused), dim=-1)
            cosine = (altered * reference).sum(-1).mean().item()
            nearest = (altered @ reference.T).argmax(dim=1)
            identity = (nearest == torch.arange(n_sites)).float().mean().item()
            results[name][mode + "_cosine"] = round(cosine, 3)
            results[name][mode + "_identity"] = round(identity, 3)
    return results


def retrieval_accuracy(anchor: torch.Tensor, positive: torch.Tensor,
                       ks: Tuple[int, ...] = (1, 5)) -> Dict[str, float]:
    """
    Top-k accuracy of retrieving each row's positive among all rows by cosine similarity.

    :param anchor: Anchor features (n, d).
    :type anchor: torch.Tensor
    :param positive: Positive features (n, d), row-aligned.
    :type positive: torch.Tensor
    :param ks: The k values to report, defaults to (1, 5).
    :type ks: Tuple[int, ...], optional
    :return: {"top1": ..., "top5": ...} plus "median_rank".
    :rtype: Dict[str, float]
    """
    a = torch.nn.functional.normalize(anchor, dim=-1)
    p = torch.nn.functional.normalize(positive, dim=-1)
    scores = a @ p.T
    target = torch.arange(a.shape[0])
    ranks = (scores > scores[target, target].unsqueeze(1)).sum(dim=1)
    out = {"top%d" % k: round((ranks < k).float().mean().item(), 3) for k in ks}
    out["median_rank"] = float(ranks.float().median().item()) + 1.0
    return out


def main() -> None:
    """
    CLI entry point.

    :return: None
    :rtype: None
    """
    from flood_forecast.multi_models.catchment_embedding import CatchmentEncoder
    from flood_forecast.preprocessing.catchment_loader import CatchmentEmbeddingDataset

    parser = argparse.ArgumentParser(description="Modality attribution of a catchment encoder")
    parser.add_argument("--version-dir", required=True,
                        help="Artifact dir holding encoder_concat.pt and combined_records/")
    parser.add_argument("--states", nargs="+", default=["CO", "UT"])
    parser.add_argument("--scrape-root", default=os.path.join(WATER_ROOT, "pilot_data",
                                                              "scrapes"))
    parser.add_argument("--output", required=True)
    parser.add_argument("--fusion", default="concat")
    parser.add_argument("--device", default="cpu",
                        help="Inference is light; default cpu leaves MPS to training runs")
    parser.add_argument("--seasonal-only", action="store_true",
                        help="Extract from the 4 seasonal members only (training view) instead "
                             "of the 6-member canonical panel")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    records = os.path.join(args.version_dir, "combined_records")
    dataset = CatchmentEmbeddingDataset(records, seed=args.seed, history_mode="hourly_panel")
    alt_dataset = CatchmentEmbeddingDataset(records, seed=args.seed,
                                            history_mode="hourly_panel", cross_year_views=True)
    sample = dataset[0]
    encoder = CatchmentEncoder(image_size=tuple(sample["image"].shape[1:]),
                               image_channels=sample["image"].shape[0],
                               static_features=dataset.static_features,
                               history_features=sample["history"].shape[-1],
                               history_len=sample["history"].shape[-2],
                               history_mode="panel", fusion=args.fusion)
    encoder.load_state_dict(torch.load(os.path.join(args.version_dir,
                                                    "encoder_%s.pt" % args.fusion),
                                       map_location="cpu"))
    encoder = encoder.to(args.device).eval()

    features = encode_towers(encoder, dataset, args.device, seasonal_only=args.seasonal_only,
                             alt_dataset=alt_dataset)
    encoder = encoder.cpu()
    site_ids = list(dataset.site_ids)
    pooled = {name: features["pooled_" + name] for name in MODALITIES}
    report: Dict = {"version_dir": args.version_dir, "n_sites": len(site_ids),
                    "history_view": "seasonal_only" if args.seasonal_only else "canonical6"}
    suffix = "_seasonal" if args.seasonal_only else ""

    report["variance_share_pooled"] = variance_share(pooled)
    report["mean_norm_pooled"] = {name: round(float(feat.norm(dim=-1).mean()), 3)
                                  for name, feat in pooled.items()}
    report["knockout"] = knockout(encoder, pooled, features["embedding"], seed=args.seed)

    pairs = [("vision", "history"), ("vision", "tabular"), ("tabular", "history"),
             ("history", "history_alt")]
    report["cross_modal_retrieval"] = {
        "%s->%s" % (a, b): retrieval_accuracy(features["proj_" + a], features["proj_" + b])
        for a, b in pairs}
    report["cross_modal_retrieval"]["chance_top1"] = round(1.0 / len(site_ids), 4)

    # Alternative banks built from the trained parts of the network.
    normalized = [torch.nn.functional.normalize(pooled[m], dim=-1) for m in MODALITIES]
    banks = {"random_projection": features["embedding"],
             "pooled_concat_l2": torch.cat(normalized, dim=-1),
             "contrastive_concat": torch.cat(
                 [torch.nn.functional.normalize(features["proj_" + m], dim=-1)
                  for m in MODALITIES], dim=-1)}
    for name in MODALITIES:
        banks["pooled_" + name] = pooled[name]
        banks["proj_" + name] = features["proj_" + name]
    for name in ("pooled_concat_l2", "contrastive_concat", "pooled_history"):
        torch.save({"site_ids": site_ids, "embeddings": banks[name]},
                   os.path.join(args.version_dir, "embeddings_%s%s.pt" % (name, suffix)))

    scrape_roots = {s: os.path.join(args.scrape_root, s) for s in args.states}
    table, kept = build_signature_table(site_ids, scrape_roots)
    report["n_probed"] = len(table)
    report["probe_r2"] = {}
    for name, bank in banks.items():
        matrix = torch.nn.functional.normalize(bank, dim=-1).numpy()[kept]
        report["probe_r2"][name] = ridge_probe_r2(matrix, table, seed=args.seed)

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)
    print("n_sites %d (probed %d)" % (len(site_ids), len(table)))
    print("variance share (pooled concat):", report["variance_share_pooled"])
    print("knockout:", json.dumps(report["knockout"], indent=1))
    print("cross-modal retrieval:", json.dumps(report["cross_modal_retrieval"], indent=1))
    header = ["bank"] + list(table.columns)
    print("\t".join(header))
    for name, r2 in report["probe_r2"].items():
        print("\t".join([name] + ["%.3f" % r2[c] for c in table.columns]))


if __name__ == "__main__":
    main()
