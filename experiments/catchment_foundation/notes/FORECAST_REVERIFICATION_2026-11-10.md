# Forecast-side re-verification (2026-11-10)

Step 1 of `HANDOFF_2026-11-10.md` §10: the state of the two forecasters before any embedding
ablation. Read-only and evaluation-only — nothing was trained. Every number below comes from
`forecast_diagnostics.py` (`audit` / `context` / `headroom`) run against the FF worktree
`ff-foundation`; JSON outputs are in `calibration/forecast_diagnostics/` (local, not committed).

```bash
FF_REPO=/Users/isaac/Documents/GitHub/ff-foundation \
/Users/isaac/Documents/GitHub/flow-forecast/.venv/bin/python \
  experiments/catchment_foundation/forecast_diagnostics.py audit --run-dir experiments/catchment_foundation/runs/<run> [--versus <other run>]
  ... context --run-dir <run> [--basin-split basin_valid]
  ... headroom --production-init-fraction 0.3 --extra seed_mean=experiments/catchment_foundation/calibration/gr4_params_3seed_mean.json
```

Skill = `100 * (1 - MSE / MSE_persistence)`; "median" is the median basin; "beat" is the share of
basins with positive skill. Hindcasts throughout (realized horizon meteorology).

## 1. Bottom line

1. Neither forecaster has demonstrably learned. `audit` over all 50 runs with a validation
   history: of the 40 hybrid runs, the best epoch beats persistence in 8 (none of the 8 with at
   least 100 validation windows, where the median run is 53% worse) and is epoch 0 in 24; the
   two fleet-scale Crossformer runs are 2–3% better than persistence with no trend across epochs
   (10-basin runs reach 8–14% on 63 windows). Median within-window correlation between forecast
   and observation is about zero for both (−0.01 Crossformer full fleet, 0.01 hybrid ablation).
2. The one piece of downstream evidence quoted for the embedding (ordered vs shuffled v4b, hybrid,
   day 1–3 pooled 42.1% vs 25.0%) is not interpretable: one basin supplies 82% of the pooled error,
   the ordered run wins on 9 of 20 basins, and the two runs stopped at different epochs.
3. The Crossformer ignores its catchment context completely. The hybrid reacts to it, weakly, and
   stays below persistence either way.
4. Consequence: an embedding-context ablation on today's forecasters cannot rank representations.
   The forecasters and the measurement have to be repaired first (§6).

## 2. Do the pipelines run?

Yes. `tests.test_multi_basin_forecast`, `test_forecast_training`, `test_hybrid_gr4`,
`test_crossformer_foundation`: 64 tests pass on `ff-foundation`. `run_crossformer.py` and
`run_training.py` both train and evaluate end to end (4-basin smokes, 35 s and 85 s).

Corrections to the handoff's operational notes:
- Forecast runs live in `Water/experiments/catchment_foundation/runs/`, not in flow-forecast.
  Older run configs still hold absolute paths into the old checkout; the diagnostics re-root them.
- The forecast launchers do not read `FF_REPO`; run them with `PYTHONPATH=<ff-foundation>`.
  `flood_forecast` is a namespace package, so with that set, modules missing from the worktree
  are silently imported from the main checkout.
- Forecast code on this branch runs on CPU only (the MPS support is on another FF branch).
- Forecast manifests cover CO/UT only: `cout_manifest_v3.json` has 235 basins, 143 with the old
  v4b bank, 140 of those in `basin_split_v1`. Hourly forcing exists for 1,331 gauges in five
  states and panel records for 555, so the forecast cohort can be four to five times larger.
- The validation number in a run's history JSON is the **sum** of per-window standardized MSE
  (the trainer divides only what it sends to W&B), so it is not comparable with the train loss.

## 3. Audit of the headline runs

| run | validation: persistence / model best (epoch) | eval basins | pooled | median | beat | top basin's share of pooled error |
|---|---|---|---|---|---|---|
| hybrid `embedding_ablation_hourly_v4b_s42` (ordered) | 50.5 / 88.5 (0 of 5) | 20 held out | 36.7 | −105.2 | 20% | 82% |
| hybrid `…_shuffled_s42` | 50.5 / 72.8 (4 of 9) | 20 held out | 34.8 | −69.0 | 15% | 82% |
| hybrid `warmstart_v3` | 15.9 / 22.8 (0 of 5) | 10 held out | 16.9 | 15.1 | 70% | 70% |
| Crossformer `ladder11` (full fleet) | 323.3 / 315.4 (6 of 12) | 96 gauged | 27.1 | 3.8 | 68% | 17% |
| | | 15 unseen | −2.9 | −3.7 | 40% | 37% |
| Crossformer `ladder12` (embedded 49) | 142.6 / 138.4 (2 of 8) | 49 gauged | 23.6 | 7.1 | 78% | 50% |
| | | 15 unseen | −0.5 | −5.0 | 27% | 37% |

(all-lead skill, 2023+ windows.) Ordered vs shuffled, paired on the 20 basins: ordered better on
45%; pooled difference +1.9 points, 95% basin-bootstrap interval −98.7 to +9.0. For scale, a
hybrid given four gradient steps on four basins scores pooled day 1–3 +30.4% (median −17.6%) on
the same 785 windows; the trained arms scored 42.1% (median −40.5%) and 25.0% (median −66.2%).
Five windows hold 82% of the pooled error there, and five validation windows hold 68% of the
validation error that picks the checkpoint.

## 4. Does a trained forecaster use its context?

Same checkpoint, same windows, context rows replaced (`context`):

| checkpoint, basins | day 1–3 median: ordered / shuffled / cohort mean / zeros | ordered better than shuffled | forecast change |
|---|---|---|---|
| Crossformer `ladder12`, 49 gauged | −3.6 / −4.0 / −3.9 / −3.6 | 53% of basins | 0.4–0.8% |
| Crossformer `ladder11`, 96 gauged | 1.9 / 1.9 / 1.9 / 1.9 | 51% | 0.0% |
| hybrid ordered v4b, 93 training basins | −33.9 / −57.5 / −57.9 / −66.5 | 66% | 76–154% |
| hybrid ordered v4b, 20 held-out basins | −40.5 / −33.2 / −91.7 / −34.4 | 60% | 37–84% |

"Forecast change" is the RMS change of the forecast as a share of the model's own RMS departure
from persistence. The Crossformer's eight projected context channels have no effect on its output.
The hybrid's parameter head does use the embedding (the correct one beats a permuted one on
60–66% of basins), which is the only weakly positive evidence that context content matters.

## 5. How much can parameters buy the hybrid? (`headroom`)

Pure-physics hybrid (gridded precipitation, no multiplier, no station gate), parameter head
replaced by a table; 117 CO/UT basins with calibration NSE ≥ 0.3, 4,016 windows, 2023+.
Median basin skill, day 1–3 / all-lead (share of basins beating persistence, all-lead):

| parameters | production-store init 0.3 | init 0.1 | init 0.6 |
|---|---|---|---|
| calibrated per basin (`cout_calibration_v2`) | −17.3 / −75.8 (30%) | −13.8 / −11.7 (41%) | −73.5 / −357.6 (20%) |
| one shared set (median of calibrations) | −43.8 / −81.9 (26%) | −21.0 / −57.6 (32%) | −56.9 / −110.4 (26%) |
| calibrated, shuffled across basins | −66.8 / −133.8 (15%) | −32.0 / −65.2 (35%) | −181.7 / −330.5 (12%) |
| mid-range (what an untrained head emits) | −27.8 / −59.9 (32%) | −16.0 / −19.1 (39%) | −95.0 / −392.6 (4%) |
| 3-seed mean of calibrations | −24.6 / −82.4 (23%) | −21.8 / −33.7 (31%) | −96.0 / −303.3 (15%) |

Readings:
- Even with per-basin calibrated parameters the physics forecast loses to persistence on 59–80%
  of basins. No embedding can lift the hybrid above that through the parameter head.
- Calibrated beats shared on 61% of basins at day 1–3 and 56–61% all-lead at init 0.3 and 0.1
  (at 0.6 the shared set wins on 55–58%): real but small, and conditional on the initialization.
- One global constant, the initial production-store fraction, moves the median all-lead skill
  from −12% to −358%. State initialization after a 30-day spin-up, not parameters, dominates the
  hybrid's error. A static context cannot fix a state-estimation problem.
- The 3-seed mean of calibrations forecasts worse than a single calibration (which wins on
  55–65% of basins): the mean of equifinal optima is not an optimum. Seed means remain the more
  stable regression targets, but they are not better parameter sets.
- Caveat: the calibrations were fitted in continuous simulation, not in forecast mode, so
  "calibrated" is a strong reference rather than the exact per-basin optimum.
- On 745 event windows (observed peak ≥ 3× the flow at issue time; init 0.1) the calibrated
  physics produces a median 4% of the observed rise; the Crossformer galleries show the same miss.

## 6. What this changes

- §7 of the handoff ("suggestive that context content matters at short lead") is withdrawn; use
  §3–§4 above instead.
- §10 step 2 as written (five context arms × three seeds on the current forecasters) would return
  five indistinguishable arms for the Crossformer and five below-persistence arms for the hybrid.
- Required before it: per-basin headline metrics with basin-bootstrap intervals and a checkpoint
  criterion that a few windows cannot dominate; a forecaster that beats persistence at the median
  basin with no context; a positive control showing that a context known to be informative
  (physical statics plus flow signatures computed from the basin's own pre-2022 record) improves
  held-out-basin skill by more than seed noise; and a held-out cohort of well over 100 basins —
  a 62% paired win rate needs about 135 basins to be detected, the 20 used so far cannot.
- The forecast evaluation basins and the embedding split are still not aligned (14 of the 27
  forecast `holdout` basins were trained on by `FLEET_v10_f0_s42`). The fix is to derive forecast
  dev/test basins from `basin_split_v1` folds and hold both folds out of embedding training.
