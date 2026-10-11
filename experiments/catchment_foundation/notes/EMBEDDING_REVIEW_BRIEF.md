# Catchment embedding — review brief for a fresh look (2026-10-11)

Self-contained. Written for a reviewer (human or model) who has not seen this project. The
author of the work to date is an AI assistant working with Isaac; this document is deliberately
candid about mistakes and negative results. Detailed logs with every table:
`EMBEDDING_V4C_LOG.md` (sections 1–10b) and the earlier `EMBEDDING_HANDOFF.md`.

## 1. Goal

Learn **one vector per USGS stream gauge** ("catchment embedding") from three modalities —
Sentinel-2 imagery, GAGES-II static attributes, and streamflow history — such that the vector
encodes the basin's **hydrologic regime** (how it responds: snowmelt-driven vs rain-driven,
flashy vs damped, baseflow-dominated, …), not merely its identity or size.

Consumers of the embedding:
- the hybrid physics model's hypernetwork reads GR4 rainfall–runoff parameters from it;
- the pure-ML Crossformer forecaster takes it as per-basin context;
- standalone: retrieve/cluster hydrologically similar basins; paper on "gauge representation
  learning".

Implicit requirement that has NOT been designed for so far (see §7): for **ungauged** basins
only imagery and statics exist at inference time, so an embedding that lives mostly in the
flow-history tower is of limited use there.

## 2. Data (all local under `Water/pilot_data/`, scraped from USGS/NWIS, GAGES-II, Sentinel-2)

- Fleet: 5 states (CO, UT, WY, FL, PA), 1,177 gauges with hourly flow + weather CSVs.
- Embedding records: 678 gauges (GAGES-II static vector of 125 numeric attributes; one clear
  summer Sentinel-2 L1C patch, 4 bands at 10 m, 128 px = 1.28 km; 15-year daily flow).
- Panel records (`pilot_data/embedding_dataset_hourly_pre2022/`): 555 gauges whose hourly flow
  before 2022-01-01 supports six 92-day hourly slices — the best-observed window of each season
  (up to 3 different years per season, so two views of a site can use disjoint years), the
  window around the record peak ("flood"), the lowest 92-day mean ("drought"). Flow is
  log1p + per-site standardized; channels: standardized log-flow, observed mask, sin/cos
  day-of-year, flood flag, drought flag. Cutoff 2022-01-01 keeps 2022+ for clean evaluation.
- Regional-context imagery (new, Sept 2026): for 675 gauges, 512 px at 50 m (25.6 km) with 6
  bands (10 m bands + SWIR B11/B12), one summer and one winter scene, merged into 553 of the
  panel records as `image_regional` / `image_regional_alt` (uint16).
- Hydrologic signatures (probe targets) computed from pre-2022 hourly flow per gauge:
  log-mean-flow (size), Richards-Baker flashiness, melt fraction (Apr–Jul share), baseflow
  index, CV, flow-duration slope, high/low ratio, diurnal strength.

## 3. Model and objective (flow-forecast repo, branch stack `fusion-contrastive-training` →
`regional-vision-modality` → `linear-fusion`; PRs #916, #917, #918)

`CatchmentEncoder` (`flood_forecast/multi_models/catchment_embedding.py`):
- vision tower: ViT (`ImagePatchEncoder`, patch 16, dim 128, depth 4, 4 heads) on the reach
  image; optional `vision_regional` tower (patch 32) on the regional image, or the regional
  image can replace the reach image as the vision input (`vision_source="regional"`);
- tabular tower: MLP on the 125 statics;
- history tower: `PanelSequenceEncoder` — a conv-tokenized transformer encodes each 92-day
  hourly slice to one token; tokens are mean-pooled (training views use the 4 seasonal slices
  only; flood/drought are single-year and were a memorization shortcut);
- fusion: pooled towers are L2-normalized (`normalize_towers`), concatenated, and mapped by a
  LayerNorm→Linear head to the 256-d bank (`fusion_head="linear"`; the original MLP head is
  kept as an option);
- each tower has a 128-d contrastive head; the fused embedding has its own.

Objective (`flood_forecast/meta_models/contrastive_train.py`): symmetric InfoNCE, temperature
0.07, averaged over pairs — vision↔history, vision↔tabular, tabular↔history, history↔history
(a different-year panel of the same site), fused↔each tower, fused(view A)↔fused(view B) with
per-sample **modality dropout** (p = 0.5) on the fused views so no block is guaranteed shared
across the two views. Negatives: the other sites in the batch; batches are "blocked" by USGS
site number so neighbors (same river network) are mutual negatives. Adam 3e-4, 300 epochs,
batch 64 or 128, trained from scratch on Apple MPS (fleet run ≈ 1 h at batch 128).

## 4. Evaluation protocol (Water repo, `experiments/catchment_foundation/`)

- `embedding_probes.py`: L2-normalized bank → ridge regression (α = 10, 5-fold CV) to each
  signature; reports out-of-fold R². Also tercile classification (linear and kNN).
- `embedding_modality_analysis.py`: per-tower probes, variance share of each tower in the
  concat, knockout sensitivity of the fused bank to each tower (mean-fill / permute), cross-
  modal retrieval top-1, and alternative banks (tower concat, single towers).
- `eval_embedding_milestone.sh`: the standing comparison — fleet-wide probes and probes on a
  **fixed 206-site CO/UT subset** so banks trained on different site sets compare like for like.
- Protocol: ≥ 3 seeds per configuration; seed noise is ±0.02–0.05 R² per signature at n ≈ 200.
- Caveat (important, see §7): every probe number is on sites the encoder was trained on.
  There is no held-out-basin evaluation yet.

## 5. Chronology of results (ridge-probe R², same 206 CO/UT sites unless noted; 3-seed means)

| version | change | size | flash | melt | BFI | read |
|---|---|---|---|---|---|---|
| v0–v2 (daily, global standardization) | — | 0.70 | 0.12 | 0.06 | 0.10 | encodes river size, not regime |
| v3b (hourly panels, per-site standardization) | — | ~0 | 0.07 | 0.08 | 0.02 | size removed; regime still flat |
| v4b (cross-year positives, blocked negatives; 143 sites) | first regime signal | 0.08 | 0.12 | 0.24 | 0.02 | weak but real |
| v4c (206 sites, pre-2022 cutoff) | more sites; cutoff is free | 0.17 | 0.18 | 0.22 | 0.05 | +sites helps a little |
| v5 (fleet, 555 sites, same recipe) | scale | 0.22 | 0.15 | 0.17 | 0.05 | **no like-for-like gain from scale** |
| — | discovery: fusion head was never trained (bank = random projection of towers, 88% vision by variance; regime signal lives in the history tower) | | | | | |
| v6 (fusion trained, fused(A)↔fused(B)) | | 0.22 | 0.14 | 0.15 | 0.05 | shortcut: views share image+statics, fusion suppressed history |
| v7 (modality dropout + fused↔tower pairs) | | 0.32 | 0.20 | 0.23 | 0.09 | fusion uses all towers |
| v8ctl (linear head, batch 128, fleet) | | **0.40** | **0.30** | 0.26 | 0.11 | best fleet bank; fleet-wide 0.48/0.45/0.27/0.21 |
| v8 (v8ctl + regional image as 4th tower) | | 0.33 | 0.27 | 0.26 | 0.12 | 4th tower dilutes the code |
| v9w (CO/UT-only training, batch 128, linear; width 256/384/512) | | 0.34/0.31/0.33 | 0.34 | **0.44** | 0.15/0.13/0.13 | width flat; **region-only training beats fleet on its own region** |
| v9c/v9r/v9t (batch 64, width 512: control / regional image as THE vision input / 4-tower) | fixed α=10 | 0.27/0.26/0.22 | 0.27/0.28/0.23 | 0.38/0.37/0.32 | 0.09/0.12/0.10 | regional-as-vision: small BFI gain only; 4th tower hurts |
| same, nested-CV ridge α (see addendum) | | 0.59/0.61/0.54 | 0.48/0.48/0.42 | 0.51/0.53/0.45 | 0.10/0.14/0.10 | same reading at the corrected scale |

Other established facts:
- The calibrated-GR4-parameter probe is ≈ 0 because the targets are not identifiable:
  3-seed recalibration gives cross-seed ICC 0.45–0.55 for routing/snow parameters (X2 0.86) at
  identical NSE. Probe R² tracks ICC. Use multi-seed-mean targets; signatures are the real test.
- Cross-modal retrieval of the site is ≈ 99% top-1 for every tower pair (chance 0.2%), i.e.
  the identity task is solved early; the loss floor is 0.04–0.09.
- The fused bank still reads regime worse than the encoder's own L2 tower concat (fleet v8ctl
  seed 42: melt 0.25 vs 0.38); the linear head narrowed but did not close this; widening did
  not help.
- Infrastructure lessons: v4b's 300 epochs were unrecorded; Sentinel scene selection chose
  orbit-edge slivers (fixed with footprint filtering: +100 gauges); regional imagery made
  training loader-bound until workers + uint16 storage + float16 transit were added; two
  concurrent MPS runs used 30 GB on a 36 GB laptop (now: one run at a time, capped).

## 6. What has been ruled out

1. Global flow standardization (encodes size). 2. Unaligned/leaky history (cutoff is free).
3. "More sites will fix it" (fleet training does not sharpen per-site representations).
4. "The fusion just needs training" (necessary, not sufficient). 5. "The code is too narrow"
(256 → 512 flat). 6. "A bigger image patch as an extra tower" (dilutes). Pending: the
regional image as the sole vision input.

## 7. Diagnosis of the stagnation (author's view — challenge it)

1. **The pretext task is saturated and is the wrong task.** Site identity is trivially
   solvable (99% retrieval) from any single tower, after which nothing in the objective asks
   the representation to organize by regime. Probes read whatever identity-discriminative
   features happen to correlate with regime; that is why everything plateaus at R² 0.3–0.45
   and why seeds scatter by ±0.05.
2. **Negatives are the lever that worked, and we have run out of it.** Every gain so far came
   from making positives/negatives harder (cross-year positives, site-adjacent negatives,
   modality dropout) — never from capacity or data volume. Region-only training beating fleet
   training says the same: cross-state negatives are easy.
3. **The regime signal lives in the history tower**, and the history tower is unavailable for
   ungauged basins. Current probes mostly measure history. If the embedding's purpose includes
   ungauged basins, the objective should be *distillation*: image + statics → predict the
   history-derived representation, with evaluation on image+static-only embeddings.
4. **All evaluation is transductive** (probe on training sites). A held-out-basin protocol is
   needed before any paper claim, and may change which configurations look best.
5. Imagery: a 1.28 km reach patch cannot see the catchment; a 25.6 km patch added as a fourth
   tower hurt. Whether catchment-scale imagery carries regime information at all is still open
   (pending runs), and terrain (DEM-derived elevation/slope/aspect) — the dominant control on
   snowmelt — is not an input except through a few GAGES-II statics.

## 8. Questions for the reviewer

1. Objective: given §7.1, what pretext/auxiliary task would force regime structure without
   making the signature probes circular? Candidates considered but not tried: masked-season
   modeling (predict a held-out seasonal slice's statistics from the others + image + statics);
   supervised-contrastive with pseudo-labels from history-only clustering; auxiliary regression
   to signatures *not* used in the probe set; a forecasting-style objective (predict next-season
   flow statistics). Which is most promising, and how would you keep evaluation honest?
2. Should the embedding target the ungauged case (image + statics → history-like code) as the
   primary design, with history used only as a teacher?
3. Negative mining: hydrology-aware hard negatives (nearest neighbors in history-tower space
   or in signature space) instead of site-number blocks — risk of collapsing exactly the
   structure we want to keep?
4. Evaluation: held-out basins (spatial CV by HUC or state); what probe set and what success
   threshold would be convincing for the paper?
5. Inputs: add a DEM/terrain modality? Replace statics with learned features? Multi-scene
   imagery (time series) rather than one summer + one winter?
6. Is the plateau itself informative — i.e. is R² ≈ 0.4 on these signatures near the ceiling
   given signature noise (BFI/flashiness are themselves noisy functionals of a 2-year record)?

## 9. Pointers

- Logs/tables: `EMBEDDING_V4C_LOG.md`; original handoff: `EMBEDDING_HANDOFF.md`.
- Training CLI: `Water/train_catchment_embeddings.py` (flags: `--history-mode hourly_panel
  --cross-year --blocked-batches --fusion-head --embedding-dim --vision-source --no-regional
  --fusion-dropout --num-workers`); queue runner `run_v9_queue.sh`; evaluation
  `eval_embedding_milestone.sh`, `embedding_probes.py`, `embedding_modality_analysis.py`.
- Banks: `pilot_data/embedding_dataset_hourly_pre2022/<VERSION>/embeddings_concat.pt`
  (`site_ids`, `embeddings`); encoders `encoder_concat.pt`; `training_summary.json` records the
  full config. Probe JSONs in `experiments/catchment_foundation/calibration/`.
- FF PRs: #916 (train the fusion; modality dropout), #917 (regional tower; loader policies;
  workers; float16 transit; regional as vision source), #918 (linear head, now default).

## Addendum (2026-10-11, after an external review)

Verified from the review: (1) the fixed ridge α=10 over-regularized unit-normalized banks —
fleet v8ctl seed 42 reads 0.69/0.61/0.68/0.29 (size/flash/melt/BFI) with nested-CV α versus
0.47/0.46/0.25/0.18 at α=10, so every table above understates absolute R² and the "≈0.4
ceiling" in §7 is withdrawn; (2) the blocked batch sampler under-exposes sites near the end of
the sorted order (offset drawn from [0, batch_size), short last block dropped); (3) the
attribution script does not restore all training-time options and must assert bank equality;
(4) a statics-only ridge baseline (0.78/0.54/0.79/0.31 fleet-wide) beats the fused bank on
size and seasonality in-sample; (5) per-site log standardization discards relative
variability. Agreed next order: repair measurement (nested α, baselines, reconstruction
checks), fix the sampler, freeze a held-out-basin split, then decide on a forcing-conditioned
forecasting-teacher objective with an image+statics student.
