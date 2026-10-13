"""
What does the embedding space look like? Clusters, neighborhoods and a 2-D map.

Answers three questions about a saved bank, quantitatively:

1. **Do clusters make hydrologic sense?** k-means on the L2-normalized bank; each cluster is
   characterized by state composition, hydrologic signatures (melt fraction, flashiness, BFI,
   size) and physical statics (elevation, snow share of precipitation, precipitation).
2. **Is the geometry regime or geography?** Agreement (NMI / adjusted Rand) of embedding
   clusters with (a) state labels and (b) regime clusters built from signatures alone, plus
   k-nearest-neighbor purity for both labelings, plus the mean signature distance to embedding
   neighbors versus random pairs.
3. **Where do held-out basins land?** For banks with held-out sites, their nearest TRAINING
   neighbors' signature distance, compared with training basins' own leave-one-out neighbors.

Writes a JSON report and a 4-panel t-SNE figure (colored by state, melt fraction, size, BFI).

Example::

    python embedding_cluster_analysis.py --embedding-bank <dir>/embeddings_concat.pt \\
        --records-dir <dir>/combined_records --states CO UT WY FL PA --k 6 \\
        --output calibration/clusters_<name>.json --figure calibration/clusters_<name>.png
"""
import argparse
import json
import os
import sys
from typing import Dict, List

import numpy as np
import pandas as pd
import torch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from embedding_probes import build_signature_table, load_statics  # noqa: E402

WATER_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
REGIME_SIGNATURES = ["melt_fraction", "rb_flashiness", "bfi", "cv", "diurnal_strength",
                     "fdc_slope", "high_low_ratio"]
STATIC_COLUMNS = ["DRAIN_SQKM", "ELEV_MEAN_M_BASIN", "SNOW_PCT_PRECIP", "PPTAVG_BASIN"]


def knn_purity(features: np.ndarray, labels: np.ndarray, k: int = 10) -> float:
    """
    Mean fraction of each point's k nearest neighbors (cosine) sharing its label.

    :param features: L2-normalized features (n, d).
    :type features: np.ndarray
    :param labels: Integer labels (n,).
    :type labels: np.ndarray
    :param k: Neighbors per point, defaults to 10.
    :type k: int, optional
    :return: Mean purity in [0, 1].
    :rtype: float
    """
    similarity = features @ features.T
    np.fill_diagonal(similarity, -np.inf)
    neighbors = np.argsort(-similarity, axis=1)[:, :k]
    return float((labels[neighbors] == labels[:, None]).mean())


def neighbor_signature_distance(features: np.ndarray, signatures: np.ndarray,
                                query: np.ndarray, pool: np.ndarray, k: int = 10) -> float:
    """
    Mean standardized-signature distance from query points to their k nearest pool points.

    :param features: L2-normalized features (n, d).
    :type features: np.ndarray
    :param signatures: Standardized signature matrix (n, s).
    :type signatures: np.ndarray
    :param query: Boolean mask of query points.
    :type query: np.ndarray
    :param pool: Boolean mask of candidate neighbors (a query point is never its own neighbor).
    :type pool: np.ndarray
    :param k: Neighbors per point, defaults to 10.
    :type k: int, optional
    :return: Mean Euclidean distance in signature space.
    :rtype: float
    """
    pool_index = np.flatnonzero(pool)
    similarity = features[query] @ features[pool_index].T
    query_index = np.flatnonzero(query)
    for row, q in enumerate(query_index):
        same = np.flatnonzero(pool_index == q)
        if same.size:
            similarity[row, same] = -np.inf
    neighbors = pool_index[np.argsort(-similarity, axis=1)[:, :k]]
    distances = np.linalg.norm(signatures[query_index][:, None, :] - signatures[neighbors],
                               axis=-1)
    return float(distances.mean())


def main() -> None:
    """
    CLI entry point.

    :return: None
    :rtype: None
    """
    from sklearn.cluster import KMeans
    from sklearn.manifold import TSNE
    from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score

    parser = argparse.ArgumentParser(description="Cluster / neighborhood analysis of a bank")
    parser.add_argument("--embedding-bank", required=True)
    parser.add_argument("--records-dir", required=True)
    parser.add_argument("--states", nargs="+", default=["CO", "UT", "WY", "FL", "PA"])
    parser.add_argument("--split", default=os.path.join(WATER_ROOT, "experiments",
                                                        "catchment_foundation", "splits",
                                                        "basin_split_v1.json"))
    parser.add_argument("--scrape-root", default=os.path.join(WATER_ROOT, "pilot_data", "scrapes"))
    parser.add_argument("--k", type=int, default=6)
    parser.add_argument("--knn", type=int, default=10)
    parser.add_argument("--output", required=True)
    parser.add_argument("--figure", default=None)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    bank = torch.load(args.embedding_bank, weights_only=False)
    site_ids = list(bank["site_ids"])
    features = torch.nn.functional.normalize(bank["embeddings"].float(), dim=-1).numpy()
    holdout = set(bank.get("holdout_sites") or [])
    with open(args.split) as handle:
        split_sites = json.load(handle)["sites"]
    states = np.array([split_sites.get(s, {}).get("state", "?") for s in site_ids])

    roots = {s: os.path.join(args.scrape_root, s) for s in args.states}
    table, kept = build_signature_table(site_ids, roots)
    features, states = features[kept], states[kept]
    site_ids = [site_ids[i] for i in kept]
    is_holdout = np.array([s in holdout for s in site_ids])
    statics, names = load_statics(args.records_dir, site_ids)
    statics = pd.DataFrame(statics, columns=names, index=site_ids)[STATIC_COLUMNS]
    table = table.replace([np.inf, -np.inf], np.nan)
    regime = table[REGIME_SIGNATURES].fillna(table[REGIME_SIGNATURES].median())
    regime_z = ((regime - regime.mean()) / regime.std().replace(0, 1)).to_numpy()

    embedding_clusters = KMeans(args.k, n_init=10, random_state=args.seed).fit_predict(features)
    regime_clusters = KMeans(args.k, n_init=10, random_state=args.seed).fit_predict(regime_z)
    state_codes = pd.factorize(states)[0]

    clusters = []
    for c in range(args.k):
        mask = embedding_clusters == c
        clusters.append({
            "cluster": c, "n": int(mask.sum()),
            "n_holdout": int((mask & is_holdout).sum()),
            "state_share": {s: round(float((states[mask] == s).mean()), 2) for s in args.states
                            if (states[mask] == s).any()},
            "signatures_mean": {s: round(float(table[s][mask].mean()), 3)
                                for s in ["melt_fraction", "rb_flashiness", "bfi", "cv",
                                          "diurnal_strength", "log_mean_flow"]},
            "statics_median": {s: round(float(statics[s][mask].median()), 1)
                               for s in STATIC_COLUMNS},
        })

    report: Dict = {
        "bank": args.embedding_bank, "n_sites": len(site_ids), "k": args.k,
        "clusters": clusters,
        "agreement": {
            "embedding_vs_state": {
                "nmi": round(float(normalized_mutual_info_score(state_codes, embedding_clusters)), 3),
                "ari": round(float(adjusted_rand_score(state_codes, embedding_clusters)), 3)},
            "embedding_vs_regime_clusters": {
                "nmi": round(float(normalized_mutual_info_score(regime_clusters, embedding_clusters)), 3),
                "ari": round(float(adjusted_rand_score(regime_clusters, embedding_clusters)), 3)},
            "regime_clusters_vs_state": {
                "nmi": round(float(normalized_mutual_info_score(state_codes, regime_clusters)), 3)},
        },
        "knn_purity": {
            "state": round(knn_purity(features, state_codes, args.knn), 3),
            "state_chance": round(float(sum((states == s).mean() ** 2 for s in args.states)), 3),
            "regime_cluster": round(knn_purity(features, regime_clusters, args.knn), 3),
            "regime_cluster_chance": round(float(sum(
                (regime_clusters == c).mean() ** 2 for c in range(args.k))), 3),
        },
    }
    rng = np.random.default_rng(args.seed)
    random_pairs = rng.integers(0, len(site_ids), (2000, 2))
    random_distance = float(np.linalg.norm(regime_z[random_pairs[:, 0]] - regime_z[random_pairs[:, 1]],
                                           axis=1).mean())
    everyone = np.ones(len(site_ids), dtype=bool)
    report["neighbor_regime_distance"] = {
        "embedding_neighbors_all": round(neighbor_signature_distance(
            features, regime_z, everyone, everyone, args.knn), 3),
        "random_pairs": round(random_distance, 3),
    }
    # Same-state neighbors only: does the embedding find regime within geography?
    same_state = []
    for s in args.states:
        mask = states == s
        if mask.sum() > args.knn + 1:
            same_state.append(neighbor_signature_distance(features, regime_z, mask, mask, args.knn))
    report["neighbor_regime_distance"]["embedding_neighbors_within_state"] = round(
        float(np.mean(same_state)), 3)
    within_random = []
    for s in args.states:
        idx = np.flatnonzero(states == s)
        if idx.size > 2:
            pairs = rng.choice(idx, (500, 2))
            within_random.append(np.linalg.norm(regime_z[pairs[:, 0]] - regime_z[pairs[:, 1]],
                                                axis=1).mean())
    report["neighbor_regime_distance"]["random_pairs_within_state"] = round(
        float(np.mean(within_random)), 3)
    if is_holdout.any():
        report["holdout"] = {
            "n_holdout": int(is_holdout.sum()),
            "heldout_to_training_neighbors": round(neighbor_signature_distance(
                features, regime_z, is_holdout, ~is_holdout, args.knn), 3),
            "training_to_training_neighbors": round(neighbor_signature_distance(
                features, regime_z, ~is_holdout, ~is_holdout, args.knn), 3),
            "heldout_neighbor_state_purity": round(float(np.mean([
                (states[np.flatnonzero(~is_holdout)][np.argsort(
                    -(features[i] @ features[~is_holdout].T))[:args.knn]] == states[i]).mean()
                for i in np.flatnonzero(is_holdout)])), 3),
        }

    with open(args.output, "w") as handle:
        json.dump(report, handle, indent=2)
    print(json.dumps({k: v for k, v in report.items() if k != "clusters"}, indent=1))
    print("%-4s %5s %-28s %6s %6s %6s %6s %8s %7s %6s" % (
        "c", "n", "states", "melt", "flash", "bfi", "size", "elev_m", "snow%", "km2"))
    for c in clusters:
        print("%-4d %5d %-28s %6.2f %6.2f %6.2f %6.2f %8.0f %7.0f %6.0f" % (
            c["cluster"], c["n"],
            " ".join("%s%.0f" % (s, 100 * v) for s, v in sorted(c["state_share"].items(),
                                                                key=lambda kv: -kv[1])),
            c["signatures_mean"]["melt_fraction"], c["signatures_mean"]["rb_flashiness"],
            c["signatures_mean"]["bfi"], c["signatures_mean"]["log_mean_flow"],
            c["statics_median"]["ELEV_MEAN_M_BASIN"], c["statics_median"]["SNOW_PCT_PRECIP"],
            c["statics_median"]["DRAIN_SQKM"]))

    if args.figure:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        xy = TSNE(2, perplexity=30, random_state=args.seed, init="pca").fit_transform(features)
        fig, axes = plt.subplots(2, 2, figsize=(13, 11))
        palette = {s: c for s, c in zip(args.states, ["#1f77b4", "#ff7f0e", "#2ca02c",
                                                     "#d62728", "#9467bd"])}
        ax = axes[0, 0]
        for s in args.states:
            m = states == s
            ax.scatter(xy[m, 0], xy[m, 1], s=14, c=palette[s], label=s, alpha=0.8)
        if is_holdout.any():
            ax.scatter(xy[is_holdout, 0], xy[is_holdout, 1], s=40, facecolors="none",
                       edgecolors="k", linewidths=0.6, label="held out")
        ax.legend(fontsize=8); ax.set_title("state (rings: held-out basins)")
        for ax, (label, values, cmap) in zip(
                [axes[0, 1], axes[1, 0], axes[1, 1]],
                [("melt fraction (Apr-Jul share)", table["melt_fraction"].to_numpy(), "viridis"),
                 ("log mean flow (size)", table["log_mean_flow"].to_numpy(), "magma"),
                 ("baseflow index", table["bfi"].to_numpy(), "cividis")]):
            lo, hi = np.nanpercentile(values, [2, 98])
            sc = ax.scatter(xy[:, 0], xy[:, 1], s=14, c=np.clip(values, lo, hi), cmap=cmap)
            fig.colorbar(sc, ax=ax, shrink=0.8); ax.set_title(label)
        for ax in axes.flat:
            ax.set_xticks([]); ax.set_yticks([])
        fig.suptitle("t-SNE of %s (%d gauges)" % (os.path.basename(os.path.dirname(args.embedding_bank)),
                                                  len(site_ids)))
        fig.tight_layout()
        fig.savefig(args.figure, dpi=130)
        print("figure:", args.figure)


if __name__ == "__main__":
    main()
