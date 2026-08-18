# Embedding v4c work log (2026-08-13/14)

Continuation of the gauge-representation work from `EMBEDDING_HANDOFF.md`. Focus: leakage fix,
fleet scaling prep, and the two standing open questions (seed noise, calibration equifinality).

## 1. v4b's unrecorded hyperparameter: 300 epochs

The v4b bank was trained with `--epochs 300`, not the CLI default 30. Recovered from
`wandb/run-20260803_035341-la4xfz0z` (first-epoch loss 4.2310 matches v4b's
`training_summary.json` exactly; final 0.0823 ditto). A 30-epoch run stalls at ~3.7 —
undertrained, not broken. `train_catchment_embeddings.py` now records the full config
(epochs/batch/lr/device/mode/flags/seed/data_root) in every `training_summary.json`.

Timing: ~1.5 s/epoch per 143 sites on MPS; 300 epochs at ~200 sites ≈ 2–3 min.

## 2. Leakage A/B: the pre-2022 cutoff is free — adopted permanently

Panels rebuilt from today's scrape (grown since v3: only 7 CO/UT sites lack hourly CSVs now)
into two roots:

- `pilot_data/embedding_dataset_hourly_pre2022/` — `--end-date 2022-01-01` (clean), 206 sites
- `pilot_data/embedding_dataset_hourly_nocut/` — no cutoff (leaky control), 216 sites

Both trained with the exact v4b recipe (300 ep, concat, cross-year, blocked). Signature probe
R² (ridge 5-fold, signatures always computed from pre-2022 flow):

| signature      | v4b (143) | v4c nocut (216) | v4c pre2022 (206), seeds 42/43/44 |
|----------------|-----------|-----------------|------------------------------------|
| log_mean_flow  | 0.078     | 0.145           | 0.148 / 0.125 / 0.235              |
| rb_flashiness  | 0.118     | 0.165           | 0.212 / 0.156 / 0.169              |
| melt_fraction  | 0.239     | 0.241           | 0.185 / 0.215 / 0.245              |
| bfi            | 0.019     | 0.055           | 0.059 / 0.051 / 0.042              |
| cv             | −0.024    | 0.078           | 0.075 / 0.061 / 0.077              |

Conclusions:
- **Seed noise is ±0.02–0.05 R² per signature** at n≈200. Single-seed deltas below ~0.05 are
  not interpretable. Milestone comparisons now require ≥3 seeds (protocol change).
- The clean-vs-leaky difference is within seed noise → **the pre-2022 cutoff costs nothing
  measurable**. All future banks use `--end-date 2022-01-01`. The "probe numbers are an upper
  bound" caveat in `embedding_probes.py` is retired for v4c+ banks.
- The scrape-growth refresh alone (143 → ~210 sites) lifted flashiness/size/BFI/CV — a good
  omen for the fleet-scale hypothesis.

Artifacts: `COUT_v4c_pre2022{,_s43,_s44}/` and `COUT_v4c_nocut/` under their roots;
probe JSONs `calibration/signature_probe_coutv4c_{pre2022,nocut}.json`.

## 3. Sentinel scene-selection bug: cloud ranking favors orbit-edge slivers

PA's first sweep failed 39% of attempts on imagery (29% `no_valid_patch`, 10%
`no_sentinel_scenes`) vs ~8% for CO. Two root causes, both verified on real sites:

1. **Sliver scenes win the cloud sort.** Partial-coverage orbit-edge products report ~0%
   cloud (assessed over the sliver only). Site 01544000 (tile 17TQF, lon −78.02, at the UTM
   17/18 boundary): the 6 lowest-cloud scenes were all R140 slivers with 0% coverage at the
   gauge, while 31 of 61 window scenes (R097) fully cover it.
2. **Some canonical MGRS ids don't exist in ESA's grid.** Philadelphia sites map to 18SVK
   (39.97°N, just south of the 40°N S/T band boundary) — zero scenes in the whole bucket;
   the area is covered by neighbor tile 18TVK.

Fix in `sentinel_functions.py` + `embedding_dataset.py`, zero extra network cost (the
footprint lives in the same `MTD_MSIL1C.xml` already fetched for cloud cover):
`get_scene_metadata()` (cloud + footprint), `footprint_contains()` (point-in-polygon),
`candidate_tiles()` (own tile + band/zone neighbors). Collection now filters scenes to those
whose footprint contains the gauge before cloud-ranking, tries up to 5, and falls through to
neighbor tiles. New manifest status `no_covering_scene`; `tile` recorded on ok rows.
Both diagnosed sites now yield valid_fraction 1.0 patches. Failed manifest rows
(`no_valid_patch`/`no_sentinel_scenes`/`error`) are deleted per state after its sweep so the
resumable collector retries them under the fix (WY done; FL/PA pending sweep completion).

## 4. Equifinality confirmed: the parameter-probe ≈ 0 is target noise, not embedding failure

3-seed GR4-snow recalibration (seeds 101/202/303, `calibrate_fleet.py`) of the 24
best-calibrated basins (v2 NSE 0.73–0.91). Fit quality is seed-stable; parameters are not.
Spread = median across basins of within-basin (max−min) as a fraction of the bound range
(log-space for X1/X3/X4); ICC = between-basin variance fraction:

| param | spread | ICC  | | param | spread | ICC  |
|-------|--------|------|-|-------|--------|------|
| X2    | 0.044  | 0.96 | | Tmin  | 0.270  | 0.43 |
| X3    | 0.100  | 0.77 | | Df    | 0.314  | 0.50 |
| X1    | 0.149  | 0.75 | | Tmax  | 0.376  | 0.43 |
|       |        |      | | X4    | 0.406  | 0.42 |

Median cross-seed NSE spread: **0.027** at median NSE 0.799 (n=24).

The ICC ordering matches the old parameter-probe R² ordering (X2 0.19 > X1 0.04 > rest ≈ 0):
probes recover parameters exactly as well as the parameters are identifiable. Answering the
handoff's open question #2: **equifinality of the calibration targets, not embedding failure.**

Consequences:
- Warm-start head targets should be **multi-seed means** (3-seed mean lifts X4's effective
  ICC ≈ 0.42 → 0.68). Fleet-wide 3-seed calibration over all 117 basins with v2 NSE ≥ 0.3 is
  running (`calibration/equifinality_full_seed{101,202,303}.json`).
- Signatures stay the headline probe; parameter probes should use seed-mean targets and be
  read against the ICC ceiling.

Analysis: `calibration/equifinality24_analysis.json`; subset manifests
`manifest_equifinality24.json` / `manifest_equifinality_full.json`.

## 5. Fleet status toward v5

Hourly-CSV fleet: CO 346, UT 163, FL 402, PA 158 (growing), WY 108. Collection yield ≈ 45–55%
of attempts (dominant losses: not in GAGES-II, short daily history). v5 = all five states,
pre-2022 panels, 300 epochs × 3 seeds, then the probe suite vs the v4c baselines above.

Ops notes: FF panel-mode backbone lives on branch `foundation_model_hydro`, checked out at
`/Users/isaac/Documents/GitHub/ff-foundation` (worktree; main FF checkout stays on the MPS
branch). `train_catchment_embeddings.py` honors `FF_REPO` to point at it. Embedding training
runs use `--no-wandb` when unattended (no netrc login on this machine; wandb.init would hang).

## 6. v5 fleet retrain (555 sites, 5 states) — 2026-08-14/18

Recipe unchanged (300 ep, concat, cross-year, blocked, pre-2022 panels), seeds 42/43/44 at
batch 64 plus one batch-128 run. Artifacts: `pilot_data/embedding_dataset_hourly_pre2022/
FLEET_v5_{s42,s43,s44,b128_s42}/`. Probe JSONs `calibration/signature_probe_FLEET_v5_*.json`
(`*_coutsubset.json` = same bank probed on the 206 CO/UT sites only).

Signature R² (shipped `embeddings_concat.pt` banks):

| probe set                    | bank                | size  | flash | melt  | BFI   |
|------------------------------|---------------------|-------|-------|-------|-------|
| 555 fleet sites              | v5 b64, 3-seed mean | 0.276 | 0.278 | 0.231 | 0.140 |
| 555 fleet sites              | v5 b128 s42         | 0.364 | 0.366 | 0.270 | 0.174 |
| **same 206 CO/UT sites**     | v4c COUT-trained, 3-seed mean | 0.169 | 0.179 | 0.215 | 0.051 |
| **same 206 CO/UT sites**     | v5 b64, 3-seed mean | 0.224 | 0.152 | 0.174 | 0.050 |
| **same 206 CO/UT sites**     | v5 b128 s42         | 0.259 | 0.199 | 0.156 | 0.077 |

**Fleet training did not sharpen the representation of a given basin.** The higher fleet-wide
numbers are between-state signature variance (FL vs CO is trivially separable); on identical
CO/UT sites v5 ≈ v4c (size up, flashiness/melt slightly down). Batch 128 helps modestly and
consistently (single seed). Signature definitions were hardened for the fleet (clip negative
tidal cfs; log-ratio floor at 1% of mean) — all baselines re-probed under the same definitions.

## 7. Modality attribution: what actually forms the representation

`embedding_modality_analysis.py` (new). Verified first: **the fused `projection` MLP is
untrained** — bit-identical to its seed-42 init after training (max |Δ| = 0.0), because
InfoNCE only ever touches the per-modality contrastive heads. Every bank v0–v5 is a random
LayerNorm→Linear→GELU→Linear map of `[vision_pooled | tabular_pooled | history_pooled]`.

Consistent across v4c seeds and v5 (206 and 555 sites):

- **Variance share of the concat**: vision 0.88–0.91, history 0.09–0.12, tabular 0.005–0.011.
- **Knockout of the fused bank** (mean-fill one tower): vision → cosine 0.44–0.57 to the
  original, self-retrieval 11–27%; history → 0.94–0.96, 100%; tabular → 0.997, 100%.
  The shipped bank ≈ the vision embedding plus a small history perturbation; statics are
  effectively absent.
- **Cross-modal retrieval (what the loss optimizes)**: 0.94–0.998 top-1 among 206/555 sites
  (chance 0.5%/0.2%), incl. history↔other-year history 0.97–0.99. The objective is saturated
  as site identification (loss 0.05–0.06); regime content is a by-product, which is why more
  sites (v5) add easy negatives rather than learning pressure.
- **Extraction distribution shift**: with `--cross-year`, training views are seasonal-only
  (4 members) but canonical extraction feeds 6 members incl. flood/drought flags never seen in
  training. On the canonical view history-involving retrieval drops to 0.53–0.80; on the
  seasonal-only view it is 0.97–0.99. `--seasonal-only` extraction is the correct one.
- **Where the regime signal lives** (per-tower probes, seasonal-only view):

| bank (v4c s42, 206 CO/UT)     | size  | flash | melt  | BFI   |
|-------------------------------|-------|-------|-------|-------|
| shipped random projection     | 0.148 | 0.212 | 0.185 | 0.059 |
| pooled_concat_l2 (equal wt)   | 0.218 | 0.337 | 0.346 | 0.115 |
| history tower only            | 0.130 | 0.387 | 0.408 | 0.135 |
| tabular tower only            | 0.242 | 0.177 | 0.234 | 0.054 |
| vision tower only             | 0.184 | 0.244 | 0.206 | 0.071 |

| bank (v5 b128, 555 fleet)     | size  | flash | melt  | BFI   |
|-------------------------------|-------|-------|-------|-------|
| shipped random projection     | 0.365 | 0.384 | 0.307 | 0.179 |
| pooled_concat_l2 (equal wt)   | 0.502 | 0.542 | 0.522 | 0.276 |
| history tower only            | 0.353 | 0.565 | 0.501 | 0.245 |
| tabular tower only            | 0.534 | 0.383 | 0.467 | 0.265 |
| vision tower only             | 0.424 | 0.398 | 0.317 | 0.189 |

The history tower carries the regime signal (flashiness/melt/BFI), the tabular tower carries
size (and melt at fleet scale); vision is weakest on regime yet ~9:1 dominant in the bank.
The random fusion under-weights exactly the towers that matter. Alternative banks are saved
per version dir as `embeddings_{pooled_concat_l2,contrastive_concat,pooled_history}_seasonal.pt`.

**Free win, no retraining**: define the bank as the L2-normalized equal-weight concat of the
trained tower outputs, extracted seasonal-only. Same-206-site lift for v4c: melt 0.185→0.346,
flashiness 0.212→0.337, BFI 0.06→0.115, size 0.15→0.22. Fleet (v5 b128): melt 0.31→0.52,
flashiness 0.38→0.54, size 0.37→0.50, BFI 0.18→0.28.

**Model-side implications (would touch the FF backbone)**: (1) train the fusion — add an
InfoNCE term on the fused embedding between the two cross-year views, or drop `projection` and
make the bank the normalized concat; (2) balance tower magnitudes (per-tower normalization
before concat); (3) the identity objective is saturated — regime encoding needs harder
positives (multi-scene/season Sentinel views, augmentations) or a non-identity signal.
