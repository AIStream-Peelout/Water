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

## 7. Follow-up the same day: a context path the Crossformer cannot ignore

Why §4's Crossformer ignored its context: the eight projected context channels were not too
small to see (between-basin spread 0.3–1.0, the scale of the standardized meteorology), but the
flow series could only read them through cross-dimension attention and never learned to.
flow-forecast branch `crossformer-static-conditioning` (one commit on top of PR #919) adds
`Crossformer(static_dim=...)`: the projected context is added to every encoder token and decoder
query. Water: `run_crossformer.py --context-mode tokens`.

Same 12 training basins, 16,384 samples, seed and loss (Huber); only the context path differs
(`FLEET_v10_f0_s42` contexts, standardized on training basins; `forecast_diagnostics.py context`):

| context path | basins | forecast change when the context is permuted | correct beats permuted on |
|---|---|---|---|
| constant input series (old) | 12 trained on | 0.4% | 50% of basins (day 1–3) |
| | 11 held out (fold 0) | 1.0% | 55% |
| static conditioning (new) | 12 trained on | 46.6% | 92% (day 1–3), 75% (all leads) |
| | 11 held out (fold 0) | 37.0% | 45% |

The new path makes the model use its context. With only 12 training basins it uses it as an
identifier: nothing transfers to held-out basins, where the context-using model is also worse
(all-lead median −22% against −9% for the model that ignores context). Whether content transfers
with the full cohort, and against statics, shuffled and no-context arms, is what the queued
ablation tests (`prepare_context_ablation.py`, `run_context_ablation.py`; arms in
`calibration/context_ablation_cout_f0/`, 85 training and 55 held-out CO/UT basins).

Also found while timing runs: after training, the Flow Forecast trainer scores its own test
loader one window per forward pass. For the hybrid (about a second per pass regardless of batch
size) that was roughly 27 minutes per run on 85 basins, for a number the experiment never uses;
`run_training.py --lean-tuning` now bounds it to about one window per basin. At batch 128 the
hybrid trains at about 115 windows/s; earlier runs used batch 8 (about 8 windows/s).

**Fold roles from here on.** Fold 0 of `basin_split_v1` is the development fold: the only bank
with a held-out fold today (`FLEET_v10_f0_s42`) holds out fold 0, so the exploratory ablation
scores on it. Fold 1 is reserved as the final test fold and must not be scored until the
forecaster configuration is frozen; banks for the final comparison hold out both
(`train_catchment_embeddings.py --holdout-fold 0 1`). The five-state cohort for that run is
`manifests/fleet5_manifest_v1.json` (555 basins; built by `build_manifest.py
--sites-from-split`). Known data issues there before it is used: station precipitation `p01m`
averages 1.9× the gridded precipitation in PA and 1.27× in FL (near 1.0 in CO, UT, WY), 22 FL
gauges are tidal (more than 1% negative flow), PA SNODAS series start in July 2017 and FL has
none.

## 8. Exploratory context ablation on CO/UT, fold 0 held out: reading rules (written before results)

Queue `ctxf0` (`run_context_ablation.py`, launched 2026-11-10 23:46, no run evaluated when this
section was committed). Cohort: 140 CO/UT basins, 85 trained on (folds 1–4), 55 held out (fold 0,
38 CO and 17 UT); training before 2022, checkpoint selection on the training basins' 2022 windows,
scoring on 2023+. One seed per arm (42), plus a second seed of the no-context arm.

- Hybrid: pure-physics forcing (gridded precipitation, no multiplier, no station term — the
  station column over-counts rain, see below), production-store start 0.1, Huber loss, uniform
  sampling, batch 128, up to 10 epochs of 8,192 windows. The context reaches the forecast only
  through the GR4/snow parameter head.
- Crossformer: static conditioning (`--context-mode tokens`), Huber loss, uniform sampling, batch
  8, up to 8 epochs of 16,384 windows; one extra arm feeds the v10 context through the old
  constant-series path.
- Arms: none (zero context), `v10` (`FLEET_v10_f0_s42`: the encoder never trained on the held-out
  basins, but each held-out basin's own pre-2022 flow goes into its embedding), `v10_shuffled`
  (permuted within split labels), `v8ctl` (`FLEET_v8ctl_s42`: encoder trained on every basin, so
  not held-out evidence), `statics` (105 physical GAGES-II attributes), `statics_signatures`
  (+ 8 flow signatures from each basin's pre-2022 record), `learned_id` (a private trainable row
  per training basin; meaningful on trained-on basins only).

How the results will be read:
1. The unit is the basin. Primary set: the held-out basins; trained-on basins are secondary.
   Statistics per lead band: median basin skill, share of basins beating persistence, and for
   each pair of arms the share of basins where one has lower error and the median per-basin
   error reduction with a basin-bootstrap 95% interval (`forecast_diagnostics.py compare`).
2. Noise floor first: the two seeds of the no-context arm, paired the same way. An arm-versus-arm
   difference counts only if its interval excludes zero and it is clearly larger than that
   seed-to-seed difference, in the same direction in at least two of the three lead bands.
   Anything else is "not distinguishable at one seed" and waits for seeds 43 and 44.
3. Questions in order: does any context beat none on held-out basins (statics, statics plus
   signatures, v10); does the embedding's content matter (v10 against v10 shuffled); does the
   embedding beat the hand-built contexts; what is the ceiling on trained-on basins (learned
   rows against none); and for the Crossformer, does the context path matter (v10 through
   tokens against v10 through constant series).
4. Fold 0 is the development fold. Nothing here is a final test.

**Station precipitation is over-counted.** `weather_scraping_functions.process_asos_csv` sums
`p01m` over all reports in an hour, but `p01m` is cumulative since the hourly reset, so stations
reporting more than once an hour are counted several times (2,593 / 2,621 / 2,751 mm per year
against 1,251 / 1,122 / 945 gridded at three PA gauges; 743 against 341 at CO 07116500; hourly
values up to 307 mm), and an all-missing hour is written as 0. The hybrid's station gate would
add a share of that excess as rain and is a function of the context, so the hybrid arms run
without it. `p01m` remains one of the Crossformer's nine inputs (standardized per basin) in every
arm. Fixing the scraper and regenerating the column is a separate task.

## 9. Context ablation, hybrid arms (2026-11-11, one seed)

Eight runs of queue `ctxf0`, 8–15 min each on CPU. Reports:
`calibration/context_ablation_cout_f0/reports/hybrid_*.json` (`forecast_diagnostics.py compare`).
"Error cut" is the median per-basin reduction of MSE relative to the reference arm, with a
basin-bootstrap 95% interval; lead bands are day 1–3 / 4–7 / 8–14.

**The forecaster first.** With no context the head learns one parameter set for every basin:
X1 = 1,743 mm (bound 2,000), X4 = 105 h (bound 120), X2 = +4.4. A soil store that large at a
0.1 start passes about 1% of rain on as runoff, so the model has switched runoff generation
off; its forecasts have a median amplitude of 0.10 of the observed and a median correlation of
0.05 with it. The "persistence parity" it scores (held-out median skill +0.2 / +0.1 / +0.5%,
55% of basins beating persistence) is an imitation of persistence, not a forecast. Context arms
stay close to it (median X1 1,160–1,650 mm, amplitude 0.22–0.28, correlation 0.08–0.15). All
comparisons below are therefore small modulations around persistence.

**Seed noise** (no-context arm, seed 43 against 42): error cut +0.0 / +0.1 / +0.1% with intervals
inside ±0.8%; the win rate still moves to 53–58%, so win rates within about ten points of 50%
mean nothing at 53 basins. A one-parameter-set arm is unusually stable, so this understates the
seed noise of the context arms.

**Held-out basins (53), each arm against no context:**

| arm | error cut day 1–3 | day 4–7 | day 8–14 | better than none on (all leads) |
|---|---|---|---|---|
| statics | +0.2% (−2.0 to +3.4) | +5.0% (−2.4 to +9.7) | +1.3% (−3.6 to +7.0) | 57% of basins |
| statics + signatures | +2.9% (−0.8 to +5.8) | +3.4% (+0.2 to +10.1) | −0.3% (−3.0 to +7.8) | 53% |
| v10 embedding | +0.0% (−5.1 to +2.0) | −1.8% (−11.1 to +4.0) | −2.7% (−26.8 to +4.2) | 45% |
| v10 shuffled | +0.6% (−0.6 to +1.3) | −0.2% (−4.4 to +1.2) | +0.1% (−7.3 to +3.3) | 53% |
| v8ctl embedding (encoder saw these basins) | −0.1% (−2.9 to +3.4) | −0.2% (−4.2 to +1.4) | +0.2% (−1.4 to +3.9) | 49% |

By the rule of §8 no arm is distinguishable from no context on held-out basins.

**Embedding against the other arms, held-out basins:**
- v10 against v10 shuffled: +0.5% (−0.2 to +6.9) / −0.1% / −1.0%; better on 60 / 47 / 43% of
  basins. The embedding's content is not distinguishable from a permutation of it.
- v10 against statics + signatures: −1.9% (−4.9 to +0.7) / −4.7% (−16.3 to −1.3) / −4.6% (−15.4
  to −0.5); better on 40 / 30 / 34% of basins and on 23% for all leads (−6.3%, −19.8 to −2.2).
  v8ctl against statics + signatures: −2.6% / −10.7% (−29.2 to −1.5) / −3.0% (−8.8 to −0.6).
  This passes the rule of §8: as hybrid context on unseen basins the hand-built vector is better
  than either embedding. Statics alone and statics + signatures are indistinguishable.

**Trained-on basins (83):** nothing at day 1–7. At day 8–14 every per-basin context beats no
context: statics +6.3% (+1.5 to +13.0), statics + signatures +12.1% (+5.3 to +15.6), v8ctl +7.1%
(+1.4 to +14.6), learned rows +4.9% (+0.3 to +9.2), v10 +3.4% (−0.8 to +9.7) — and v10 shuffled
+10.0% (+6.1 to +13.3). A permuted embedding helping as much as anything is the identifier
effect: on basins it trained on, the head can attach parameters to any fixed distinct vector.

**Caveats.** One seed. Checkpoint selection differed between arms: the v10 run's validation was
best after its first epoch (161.1, then 169.7, 218.7, 162.4, stopped), while v10 shuffled ran
ten epochs and peaked at the eighth; validation of every context arm spikes (212–248 against a
best of 157–166), which says a learning rate of 1e-3 is too high for the head on standardized
contexts. The held-out fold is CO-heavy (38 CO, 17 UT) while training is UT-heavy (21 CO, 64 UT).
