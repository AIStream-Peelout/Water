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

## 8. Training the fusion — FF PR #916 (2026-09-03)

Decision: the untrained fusion is a defect, not a design choice. Fixed in flow-forecast on
branch `fusion-contrastive-training` (PR https://github.com/AIStream-Peelout/flow-forecast/pull/916,
base `foundation_model_hydro`; the ff-foundation worktree now tracks this branch):

- `contrastive_step(train_fusion=True)`: InfoNCE on the fused embedding through a new
  `fused_head` — fused(base views) vs fused(cross-year views substituted), so `projection`
  (and `cross_attention`) are in the loss graph. Without alias views the fused projection is
  paired with each modality projection instead. `train_fusion=False` = old behavior (ablation).
- `MultiModalEncoder.normalize_towers` (CatchmentEncoder default True): L2-normalize pooled
  towers before fusion, removing the 88/11/1 magnitude imbalance at the source.
- `CatchmentEmbeddingDataset(seasonal_only=True)`: extraction view = training view for
  cross-year encoders. `train_catchment_embeddings.py` now extracts seasonal-only whenever
  `--cross-year` is set (recorded in `training_summary.json` config).
- Regression test `tests/test_contrastive_fusion.py`: every named parameter receives a
  non-zero gradient through the training step for both fusion modes (8 tests; 33 existing
  tests for the touched modules still pass).

Live verification on the Water pipeline (5-epoch CO/UT smoke): `projection` weights moved by
4.2e-3 vs init — same order as the towers — where every previous checkpoint showed 0.0.
Pre-PR checkpoints (v4c, v5) lack `fused_head`; `embedding_modality_analysis.py` detects this
and loads them with `strict=False`, `normalize_towers=False`.

v6 = v5 protocol (555 sites, 300 ep, seeds 42/43/44 at batch 64 + one batch-128) on the
fused-training code with seasonal extraction. Results appended below when complete.

### 8a. v6 results (2026-09-08): the fusion trains, but the fused objective had a shortcut

v6 (`FLEET_v6_{s42,s43,s44,b128_s42}`, fused training + normalized towers + seasonal
extraction; loss 3.8 → 0.04–0.06, b128 0.16). Shipped-bank probe R²:

| probe set              | bank                | size  | flash | melt  | BFI   |
|------------------------|---------------------|-------|-------|-------|-------|
| 555 fleet sites        | v5 b64 3-seed mean  | 0.276 | 0.278 | 0.231 | 0.140 |
| 555 fleet sites        | v6 b64 3-seed mean  | 0.314 | 0.281 | 0.176 | 0.191 |
| 555 fleet sites        | v6 b128 s42         | 0.357 | 0.330 | 0.206 | 0.173 |
| same 206 CO/UT sites   | v5 b64 3-seed mean  | 0.224 | 0.152 | 0.174 | 0.050 |
| same 206 CO/UT sites   | v6 b64 3-seed mean  | 0.221 | 0.142 | 0.148 | 0.054 |

Mixed: size/BFI up fleet-wide, melt down, CO/UT subset flat. Corrected attribution (the
analysis script now uses the encoder's own `pool_towers`/`fuse`, so it describes the shipped
bank): variance share is balanced (0.35/0.33/0.33 — `normalize_towers` works), the
projection is trained, yet the fused bank is **least sensitive to history**: mean-fill
knockout cosine vision 0.77 / tabular 0.79 / history 0.91, and self-retrieval stays 100% with
history removed. Meanwhile the v6 history tower alone probes flashiness 0.48–0.51 and melt
0.38–0.48, and the equal-weight tower concat 0.43/0.35–0.43 — both far above the fused bank.

Mechanism: the fused InfoNCE pair was fused(base views) vs fused(cross-year views). A site
has one image and one static vector, so vision and tabular are *identical* across the two
views and only history differs — the projection is rewarded for matching on the shared
blocks and treating history as nuisance. Training the fusion was necessary but the pair
construction handed it a shortcut.

Fix (same PR branch): per-sample **modality dropout** on both fused views
(`drop_modalities`, default p=0.5, at least one modality kept) so no block is guaranteed
shared, plus fused↔each-tower InfoNCE pairs so the fused code must stay predictive of every
modality's identity code even when that modality is dropped from its own view. Water CLI:
`--fusion-dropout` (default 0.5) and `--no-train-fusion` (ablation). CO/UT A/B (dropout 0.5
vs 0, 3 seeds each, `COUT_v7a_d05_*` / `COUT_v7a_d0_*`) decides before any fleet run.

### 8b. A/B result (2026-09-08): trained fusion + modality dropout wins on the same sites

Same 206 CO/UT sites, shipped fused bank, 3-seed means (`COUT_v7a_d0_*`, `COUT_v7a_d05_*`):

| variant                                   | size  | flash | melt  | BFI   | cv    | diurnal |
|-------------------------------------------|-------|-------|-------|-------|-------|---------|
| v4c untrained fusion (random projection)  | 0.169 | 0.179 | 0.215 | 0.051 | 0.071 | 0.097   |
| v7a d0: trained fusion, fused↔tower pairs | 0.192 | 0.191 | 0.302 | 0.054 | 0.090 | 0.137   |
| **v7a d05: + modality dropout 0.5**       | 0.262 | 0.236 | 0.336 | 0.079 | 0.100 | 0.171   |

Every d05 seed beats every v4c seed on size and melt (d05 melt 0.318–0.354; v4c 0.185–0.245).
Attribution (d05 s42): knockout cosine vision 0.947 / tabular 0.937 / history 0.959, variance
share 0.34/0.34/0.32 — the fused bank uses all three towers. Remaining headroom: the fused
code still trails the best single tower on some signatures (d05 history tower alone: melt
0.426, flashiness 0.312; L2 concat 0.388/0.266), i.e. the identity objective is now the
ceiling, not the fusion. Fix committed to PR #916 (`fusion_modality_dropout=0.5` default,
fused↔tower pairs always on). Fleet run v7 = v5 protocol on this objective.

### 8c. v7 fleet (2026-09-08): fixed objective at 555 sites

`FLEET_v7_{s42,s43,s44,b128_s42}` = v5 protocol on the modality-dropout objective. Shipped
fused bank, probe R²:

| probe set              | bank                  | size  | flash | melt  | BFI   |
|------------------------|-----------------------|-------|-------|-------|-------|
| 555 fleet sites        | v6 b64 3-seed mean    | 0.314 | 0.281 | 0.176 | 0.191 |
| 555 fleet sites        | v7 b64 3-seed mean    | 0.312 | 0.266 | 0.128 | 0.165 |
| 555 fleet sites        | v7 b128 s42           | 0.369 | 0.394 | 0.170 | 0.211 |
| same 206 CO/UT sites   | v4c (COUT, untrained fusion) | 0.169 | 0.179 | 0.215 | 0.051 |
| same 206 CO/UT sites   | v6 b64 3-seed mean    | 0.221 | 0.142 | 0.148 | 0.054 |
| same 206 CO/UT sites   | **v7 b64 3-seed mean**| 0.318 | 0.201 | 0.227 | 0.090 |
| same 206 CO/UT sites   | v7 b128 s42           | 0.325 | 0.238 | 0.310 | 0.096 |

On the like-for-like CO/UT test v7 is the best fleet-trained bank so far (vs v6: size +0.10,
flash +0.06, melt +0.08, BFI +0.04) and beats the untrained-fusion baseline on size/melt/BFI.
Fleet-wide the b64 numbers are flat-to-lower than v6 (melt 0.176 → 0.128) while b128 is the
best fleet bank on size/flash/BFI — batch size matters more under the harder objective.

Standing gap: the 256-d fused code still reads regime worse than the SAME encoder's 384-d
L2 tower concat (fleet v7 s42: melt 0.126 vs 0.353, flash 0.271 vs 0.405; knockout is
balanced 0.95/0.95/0.97). The MLP fusion (LayerNorm→Linear→GELU→Linear) trained for identity
scrambles linearly-readable structure the towers carry. Candidate fix: a linear fusion
(LayerNorm→Linear, optionally residual to the concat) or shipping the L2 tower concat as the
bank while keeping the fused head as a training signal — decide by a CO/UT A/B (3 min/run).

## 9. Regional-context imagery + linear fusion head (2026-09-11)

The gauge-reach image is a 1.28 km, 10 m crop of a 110 km Sentinel-2 tile — full resolution,
tiny extent: 0.3% of the median catchment (530 km²). Vision therefore learned channel size
and identity, not regime. New second image modality: **25.6 km at 50 m (512 px), 6 bands
(10 m bands + SWIR B11/B12), summer scene + winter scene** of the same window (a hard
cross-season positive for the vision tower).

Code (all committed):
- Water `sentinel_functions.py`: `extract_patch(pixel_meters=, resampling=)` reads coarse
  windows via reduced-resolution JPEG2000 decode (23 s for 6 bands at 512 px); tile ordering
  by distance to tile center; `gcs_get()` retries; 404 metadata tolerated; GDAL HTTP retries.
- Water `embedding_dataset.py --regional --shard i/n`: `select_scene_patch()` (best-covered
  candidate, same-datatake mosaic fill, `accept_valid`), per-(tile, window) scene metadata
  cache, sharded resumable collection writing `<site>_regional.npz` sidecars
  (`image_regional`, `image_regional_alt`) + `manifest_regional[.shard<i>].csv`.
- Water `build_panel_records.py --merge-regional`: folds sidecars into panel records.
- FF PR #917 `regional-vision-modality` (base #916): `CatchmentEncoder(regional_image_size=,
  regional_channels=, regional_patch_size=32)` adds a `vision_regional` tower; loader serves
  `image_regional[_alt]`; trainer derives modality pairs/alias views from the encoder.
- FF PR #918 `linear-fusion` (base #917): `fusion_head="linear"` (LayerNorm→Linear) to keep
  tower structure linearly readable in the bank; Water CLI `--fusion-head`.

Runs: regional collection launched for all 5 states (8 shards, scene year 2025, overnight);
CO/UT A/B `COUT_v7b_linear_s{42,43,44}` (linear head) vs `COUT_v7a_d05_*` (MLP head).
v8 = regional tower + winning head + batch 128, 3 seeds, once sidecars are merged.

### 9a. Linear vs MLP fusion head (2026-09-11): linear adopted

Same 206 CO/UT sites, 3 seeds each, fused bank R²: MLP head (`COUT_v7a_d05_*`) size 0.262 /
flash 0.236 / melt 0.336 / BFI 0.079; **linear head** (`COUT_v7b_linear_*`) 0.265 / 0.261 /
0.368 / 0.067. Seed-42 attribution: the linear fused bank matches its own L2 tower concat
(0.256/0.287/0.327/0.088 vs 0.247/0.294/0.332/0.093) where the MLP bank trailed it
(melt 0.335 vs 0.388) — the fusion is no longer a lossy compression for linear readout.
`fusion_head="linear"` is now the CatchmentEncoder default (PR #918) and the Water CLI
default; `--fusion-head mlp` keeps the old head.

Regional collection: 8 shards, 0 errors in the first ~60 gauges, ~3.5 min/gauge/shard.
`run_v8_when_collected.sh` waits for the shards, merges sidecars, trains v8 (regional) vs
v8ctl (`--no-regional`), linear head, batch 128, 3 seeds each, and evaluates both vs v7.

### 9b. Regional collection done; v8 first attempt hit a loader collate bug (2026-09-12/23)

Collection: 675/678 gauges got summer+winter regional sidecars (2 `no_coordinates`, 1
`error`), median valid fraction 1.0 for both seasons, 12.5 h wall over 8 shards. Merge folded
regional arrays into 553/555 pre-2022 panel records.

The first v8 attempt failed in 40 s: the loader served `image_regional` per record, so the 2
records without a sidecar produced items with different keys and the default collate raised
`KeyError` on any batch containing them — for the regional run AND the `--no-regional`
control (which only skipped the tower). Framework fix in FF (`regional-vision-modality`,
commit 9a73200f, test added): `CatchmentEmbeddingDataset(regional="auto"|"require"|"ignore")`
resolves the policy once at construction — auto excludes records lacking the patch
(`excluded_sites`, warning), ignore is a true control. Water CLI `--no-regional` now passes
`regional="ignore"`; excluded sites are recorded in `training_summary.json`. `linear-fusion`
rebased onto the fix. v8 relaunched: regional runs train on 553 sites, control on 555.

### 9c. v8 relaunch was loader-bound: three fixes (2026-09-23/24)

The relaunched regional chain ran at ~3 min/epoch (control: 2.5 s). Diagnosis in order:
1. Single-threaded decoding of ~12 MB of regional imagery per record → FF `num_workers`
   (persistent workers) in `pretrain_encoder`/`extract_embeddings`; Water `--num-workers`.
   3× faster, still ~65 s/epoch.
2. zlib-compressed float32 records → panel records now stored uncompressed with uint16
   regional images (lossless for L1C DN; `--merge-regional --force-regional` converted all
   553). Record load 6 ms — but epoch time unchanged, so decompression was not the wall.
3. Profiling one batch of 128: regional tower forward+backward 0.4 s, batch load **22.6 s** —
   two float32 views = 3.2 GB per batch crossing worker→trainer shared memory. Fix (FF):
   loader serves regional images as unscaled float16 (`regional_half=True`) and the trainer
   scales on the device via `input_transforms` (`dataset.regional_transform`, wired
   automatically by the catchment wrappers). Batches now arrive in ~0 s (prefetched), device
   transfer + scaling 0.37 s. Expected ≈ 1 h per 300-epoch fleet seed.

All three are on `regional-vision-modality` (PR #917); `linear-fusion` (PR #918) rebased.
