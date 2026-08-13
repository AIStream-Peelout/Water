# Catchment Embedding — Status & Handoff (2026-08-13)

Self-contained handoff for a fresh chat focused on the gauge-representation (embedding) work.

## Goal
One vector per USGS gauge from three modalities — Sentinel-2 imagery + GAGES-II static
attributes + streamflow history — aligned by contrastive (InfoNCE) learning. It is the **backbone
of both forecasting tracks**: the hybrid model's hypernetwork reads GR4 parameters from it, and the
pure-ML Crossformer consumes it as per-basin context. Standalone value: cluster/retrieve
hydrologically similar basins. Paper potential: "gauge representation learning."

## Repo rule (important)
Hydrology-specific **experiments + data code → Water**; reusable **NN/framework code → flow-forecast (FF)**.
The `experiments/catchment_foundation/` tree now lives in **Water** (git worktree
`water-catchment-experiments`).

### Code locations
Water:
- `experiments/catchment_foundation/embedding_probes.py` — signature probe suite
- `experiments/catchment_foundation/pretrain_parameter_head.py`, `calibrate_fleet.py` — warm-start (forecasting side)
- `experiments/catchment_foundation/calibration/` — probe result JSONs
- `train_catchment_embeddings.py` — contrastive retrain CLI (`--history-mode hourly_panel --cross-year --blocked-batches`)
- `embedding_dataset.py` — collect Sentinel+static+history records per state
- `build_panel_records.py` — build hourly seasonal/extreme panels from scraped CSVs
- `embedding_visualizer.py` — interactive t-SNE explorer (Genizah-style)

FF (reusable pieces):
- `flood_forecast/preprocessing/catchment_loader.py` — `CatchmentEmbeddingDataset` (`random_window` + `hourly_panel` modes, `cross_year_views`)
- `flood_forecast/multi_models/catchment_embedding.py` — `CatchmentEncoder` (tri-modal; `fusion` concat/cross_attention; `history_mode` sequence/panel)
- `flood_forecast/meta_models/multimodal_encoder.py` — `PanelSequenceEncoder`, `KeyBlockedBatchSampler`
- `flood_forecast/meta_models/contrastive_train.py` — `pretrain_encoder` (view_aliases, blocked batches)

## Version history
- **v0/v1** (daily, random 1-yr window, GLOBAL standardization): clusters recover regime; vision-only PC1 ≈0.65 with elevation. Diagnosed: encodes **size**, not regime.
- **v2 (COUT_v2, daily)**: the bank warmstart_v1 used.
- **v3/v3b (COUT_v3, hourly panels)**: 6×92-day slices (4 seasons + flood + drought), per-SITE standardized, day-of-year encoding. Removed the size shortcut but regime still flat.
- **v4 (COUT_v4, + cross-year positives + site-adjacent blocked negatives)**: BUG — both views shared the single-year flood/drought slices → memorization shortcut open → probes flat.
- **v4b (COUT_v4b, fix: training views seasonal-only from DISJOINT years)**: **first real regime signal. CURRENT BEST.**

## Probe suite (standing eval — rerun after every embedding change)
Signature R² (ridge, 5-fold CV): embedding → flow-regime signatures computed from pre-2022 flow.

| Version | n | log-mean-flow (size) | melt frac | flashiness | BFI |
|---|---|---|---|---|---|
| v2 daily | 152 | **0.70** | 0.06 | 0.12 | 0.10 |
| v3b panel | 143 | ~0.00 | 0.08 | 0.07 | 0.02 |
| **v4b** | 143 | 0.08 | **0.24** | 0.12 | 0.02 |

Tercile classification (v4b): melt linear **0.60**, BFI **0.55** (chance 0.33); kNN ≈ linear (signal is linear-readable).
Cosine structure (v4b): Boulder Ck↔Arkansas vs ↔front-range contrast improved ~5× vs v3b; continental-divide separation still weak.
Parameter probe (embedding → calibrated GR4 params): **still ≈0** — v4b X1 0.04, X2 0.19, others ≤0 (n=112). Leading hypothesis: equifinality of calibration targets, not embedding failure — **untested** (test: multi-seed calibration agreement).

## Key findings
1. v0–v2 encode river **size**, not behavior (global flow standardization made magnitude the easy InfoNCE discriminator).
2. Per-site standardization + hourly panels + calendar anchor were necessary but NOT sufficient (v3b still flat).
3. The unlock was **closing view-overlap**: cross-year positives (disjoint years) + hard (site-adjacent) negatives → v4b regime signal; loss floor rose ~4× (task genuinely harder).
4. Signal is now **weak but real and linear-readable** (melt 0.24, most others <0.15) — headroom remains.

## DATA — scraped vs embedded (the scaling opportunity)
Embedding model has only ever seen **CO+UT → 143 panel sites**. The rest of the fleet is scraped but NOT embedded:

| State | Scraped (hourly CSV) | Embedding records (img+static+hist) | Panel records |
|---|---|---|---|
| CO | 346 | 163 | 61 |
| UT | 163 | 90 | 82 |
| FL | 407 | 0 | 0 |
| WY | 108 | 0 | 0 |
| PA | ~150→growing | 0 | 0 |

Fleet ≈ **1,024+ basins, ~180M+ hourly rows**, all on GCS. Regimes are genuinely diverse (FL flat/humid rain-driven; CO/UT/WY snowmelt; PA Appalachian).

**To scale the embedding to the full fleet (biggest available lever, 143 → ~1000 sites):**
1. `Water/embedding_dataset.py --state FL` / `WY` / `PA` (+ re-run CO) → collects Sentinel patches + GAGES-II statics + daily history (needed for the image/static modalities). Currently 0 for FL/WY/PA.
2. `Water/build_panel_records.py --states FL WY PA CO` → adds hourly seasonal/extreme panels.
3. `Water/train_catchment_embeddings.py --history-mode hourly_panel --cross-year --blocked-batches` → retrain on the full multi-state set.
4. Rerun the probe suite. This is where the CLIP-style scaling hypothesis (more data + more diverse regimes → sharper embeddings, bigger negative pools finally useful) becomes a fair test at ~1000 sites instead of 143.

## Artifacts
- Current best bank: `gs://flow_hydro_2_data/claude_data/pilot_data/embedding_dataset_hourly/COUT_v4b/embeddings_concat.pt` (+ local in Water)
- Explorer HTML: `COUT_v4b/` (and `COUT_v3b/`) `catchment_embeddings_*.html`
- Probe JSONs: `Water/experiments/catchment_foundation/calibration/signature_probe_cout*.json`

## Open questions / next steps (priority order)
1. **Scale to the full ~1000-basin fleet** (collect records for FL/WY/PA/expanded-CO, retrain) — highest leverage.
2. Is the parameter-probe ≈0 the embedding or calibration equifinality? Test with multi-seed calibration agreement.
3. Bigger negative pools / memory bank at fleet scale (batch 64 was negative-starved at 143 sites).
4. Multi-season Sentinel positives (currently 1 scene/basin).
5. Leakage fix for papers: history tower currently sees post-2022 flow; rebuild banks with a train-end cutoff for clean "ungauged-2023" evaluation.
