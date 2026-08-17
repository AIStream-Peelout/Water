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
