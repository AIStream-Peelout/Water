# Catchment representation: plan and roadmap (2026-11-12)

Agreed with Isaac on 2026-11-12, after the forecast re-verification
(`FORECAST_REVERIFICATION_2026-11-10.md`, §13–§14). This replaces §10 steps 3–5 of
`HANDOFF_2026-11-10.md`.

## 1. What the representation is for

A general description of a river, in the way word and image embeddings are general: pretrain it
on a broad objective first, then fine-tune or align it to a specific task. Better forecasts stay
the main goal. The other uses named so far are classifying rivers, judging how a river responds
to extreme weather, and finding similar basins. A code trained only on the 14-day forecast target
is not the aim.

## 2. Why the current embedding (v10) falls short

- **Objective.** It is trained to tell gauges apart (InfoNCE on site identity, ~99% retrieval),
  which pushes similar basins apart. In the tree forecaster a shared description beats a private
  identifier even on the gauges it was fitted on: statics + signatures 39.7 / 52.9 / 60.7%
  median skill against 37.2 / 48.9 / 56.5% for a basin id (2023+, day 1–3 / 4–7 / 8–14).
- **Inputs.** The flow tower reads flow alone, standardized per basin. It never sees weather
  next to flow, so runoff per mm of rain, the rain-to-flow lag and snowmelt response are out of
  reach.
- **Size and loss.** 256 numbers for 403 training basins. On unseen basins a linear readout
  recovers 25–62% of the variance of the flow signatures and less of size and melt than raw
  statics do (0.44 against 0.74).

Result: as forecast context v10 adds nothing over no context and is 8–10% worse than statics +
eight flow signatures (§14 of the re-verification note).

## 3. Design

**Pretraining (general).** An encoder reads a gauge's pre-2022 flow together with its weather,
in physical units (mm over the basin, °C), and writes a code of 32 numbers (16 and 64 tested;
256 waits for far more basins). A decoder shared by all basins has to reproduce the flow from
the weather in years the encoder did not read: ordinary days, flood peaks, recessions, low flow,
snowmelt. This is the counterpart of masked-token prediction. Side targets read from the same
code: flow signatures, flood size per km², low-flow level. No contrast that treats every other
gauge as a negative.

**Alignment (per task).** The pretrained code is frozen and a small task-trained projection, or
a fine-tune of the encoder's last layers, adapts it. For forecasting the aligned code must beat
statics + signatures.

**Ungauged student.** Statics + imagery are aligned to the pretrained code, as image and text
towers are aligned in CLIP. Its bar is physical statics alone.

**One code, several readers (multimodal).** The embedding is one space, and each kind of data
about a basin has its own reader that writes into it: the gauged record (water years of flow
and weather), the physical statics, and the imagery (reach and regional patches). The readers
are fused as in Flow Forecast's `MultiModalEncoder`. In training, readers are switched off at
random per basin (modality dropout) and the decoder must still reproduce the flow from what is
left. A gauged basin uses every reader; an ungauged basin uses statics and imagery and lands in
the same space, so the ungauged student is the same encoder with the gauge reader off, not a
second model. The gauge reader is trained first because it defines what the code means; the
statics and imagery readers are then added to it. Imagery earns its place only if the code
without the gauge reader beats statics and location (rule 3).

**Where the analogy is weak.** Word and image embeddings generalize because they see millions
of distinct items. Here the items are basins and there are 403. Hours of record are plentiful;
distinct basins are not. Context began to transfer only between 85 and 403 basins, so adding
basins is part of the design, not an afterthought.

## 4. How "general" is measured

A fixed suite, built before any new model is trained. Every score is on the fold-0 held-out
basins with targets taken from 2022 onward (years no representation read), against the same
baselines: physical statics, statics + flow signatures, statics + flow and weather-response
signatures, v10, v10 shuffled, and location alone.

| Use | Test | Tool |
|---|---|---|
| Forecasting | Error cut in the tree forecaster, day 1–3 / 4–7 / 8–14 | `tabular_forecast_baseline.py` |
| Extremes | Flood size per km², response to the largest storms, low flow | `representation_benchmark.py` |
| River classification | Regime class from the later record | `representation_benchmark.py` |
| Similar basins | Nearest training basins as donors of behaviour and of flow | `representation_benchmark.py` |

Rules:

1. Frozen code: beat physical statics on every task. This is the generality test.
2. Aligned code: beat the hand-built gauged description on forecasting (§7: +7.5 / +8.3 /
   +9.8% error cut for 18 numbers). This is the gate; candidates that pass are ranked on the
   other three tasks.
3. Ungauged student: beat physical statics and location alone on forecasting (§7: about +3 /
   +3 / +4% each) and on the suite.
4. Fold 0 is the development fold. Fold 1 stays unscored until the design is frozen. Three
   seeds before any claim. In-sample probes are not evidence.

## 5. Order of work

1. Build the suite and the hand-built reference description (flow signatures plus
   weather-response signatures). Score the existing representations. This sets every bar and
   shows how much room is left above the hand-built description.
2. Pretrain the encoder of §3 on the daily record; score it frozen on the suite.
3. Align it to forecasting; score against statics + signatures.
4. Train the ungauged student; score against statics.
5. Add basins (a download job of hours; placement is Isaac's call), then repeat 2–4.
6. Only then return to the neural forecasters, which must first show they use the weather
   (`forecast_diagnostics.py inputs`).

Not planned: further tuning of the contrastive recipe (widths, heads, imagery variants).

## 6. Roadmap, longer term

- **Mapped flood extent and damage estimation.** Predict the inundated area around a reach for
  a given flow or storm, as a step toward estimating damage. Not possible with the data held
  now: it needs inundation labels. Candidate sources to check: USGS flood-inundation map
  libraries (extent by gauge stage at selected sites), satellite flood masks (Sentinel-1 based
  sets such as Sen1Floods11, the MODIS-based Global Flood Database), FEMA flood-hazard layers
  and flood-insurance claims for the damage side. The gauge record gives the flow or stage at
  the time of each mapped flood, and the reach imagery already collected gives the place.
- **External river classes.** Regulated against near-natural gauges and published regime or
  landscape classifications as further classification targets.
- **More modalities in the encoder.** Stage, water temperature and station weather once the
  station-precipitation fix is merged and the forcing is regenerated.

## 7. Step 1 results: the suite, the hand-built reference and every bar (2026-11-12)

Tools: `representation_benchmark.py` (`build`: one pass over the hourly files, 70 s; `run`: 4
min for 13 representations; `recover`: how much of one description another reproduces) and `tabular_forecast_baseline.py`, which now takes any `+`-joined
list of banks as an arm. Cohort: `calibration/context_ablation_fleet5_f0/`, 403 training and 123
held-out basins with a record after 2022. Reports: `reports/benchmark_seed0.json`,
`reports/tabular_seed{0,1,2}_response.json`.

**Hand-built reference.** Ten weather-response numbers from each basin's record before 2022
(`context_response.pt`): runoff per day, runoff ratio, aridity, snow share of precipitation
(with the manifest's elevation temperature correction), recession rate, flow rise per mm of
warm rain on the wettest days, rain-to-flow delay from the hourly record and how tight that
link is, the day by which half the year's flow has passed, and the share of no-flow days. With
the eight flow signatures that makes 18 numbers. Past flood and low-flow sizes
(`context_extremes.pt`) and location (`context_location.pt`) are separate banks.

**Suite, held-out basins, targets from 2022 on.** R² for the first four columns and for similar
behaviour; balanced accuracy over five regime classes (chance 0.20); median KGE of the flow
borrowed from the three nearest training basins.

| description | flood size | flood / mean flow | storm response | low flow | regime | similar behaviour | donor flow |
|---|---|---|---|---|---|---|---|
| physical statics (102) | 0.62 | 0.53 | 0.67 | 0.34 | 0.46 | 0.33 | 0.30 |
| location (2) | 0.37 | 0.18 | 0.50 | 0.09 | 0.40 | 0.31 | 0.24 |
| flow signatures (8) | 0.56 | 0.84 | 0.64 | 0.70 | 0.74 | 0.60 | 0.14 |
| weather response (10) | 0.81 | 0.60 | 0.92 | 0.85 | 0.69 | 0.68 | **0.43** |
| signatures + response (18) | 0.87 | 0.87 | 0.91 | 0.87 | 0.77 | **0.76** | 0.39 |
| statics + signatures (110) | 0.71 | 0.86 | 0.79 | 0.79 | 0.75 | 0.51 | 0.32 |
| statics + signatures + response (120) | 0.88 | 0.87 | 0.91 | 0.86 | **0.79** | 0.57 | 0.33 |
| the same + past extremes (123) | **0.92** | **0.88** | 0.91 | **0.93** | 0.74 | 0.60 | 0.34 |
| v10 embedding (256) | 0.48 | 0.46 | 0.63 | 0.62 | 0.47 | 0.23 | 0.06 |
| v10, 16 principal components | 0.36 | 0.23 | 0.45 | 0.30 | 0.38 | 0.14 | −0.13 |
| v10 shuffled | −0.00 | −0.00 | −0.00 | −0.01 | 0.20 | −0.18 | −0.40 |

Against physical statics, with basin-bootstrap 95% intervals: v10 is worse on flood size (−0.15,
interval −0.30 to −0.02) and donor flow (−0.24, −0.37 to −0.14), level on regime (+0.01) and
better only on low flow (+0.27, +0.13 to +0.42). The 18 hand-built numbers are better on every
task (+0.24 to +0.53 on extremes, +0.31 regime, +0.43 similar behaviour, +0.09 donor flow; every
interval excludes zero).

**Forecasting, held-out basins, 2023+.** Error cut against flow + weather + snowpack, day 1–3 /
4–7 / 8–14, mean of three forecaster seeds (range across seeds about ±1; seed-0 intervals in the
reports).

| context | error cut | median skill |
|---|---|---|
| none (reference) | | 40.1 / 49.5 / 58.7% |
| physical statics | +2.6 / +3.0 / +3.9% | 41.7 / 50.2 / 61.9% |
| location | +3.6 / +3.4 / +4.1% | 42.2 / 51.3 / 60.4% |
| weather response | +4.9 / +6.5 / +6.8% | 40.4 / 51.2 / 63.1% |
| statics + response | +4.2 / +6.0 / +7.9% | 41.0 / 51.2 / 62.7% |
| statics + signatures | +5.4 / +7.9 / +9.1% | 40.3 / 52.7 / 63.5% |
| signatures + response (18) | **+7.5 / +8.3 / +9.8%** | 42.3 / 54.1 / 63.8% |
| statics + signatures + response | +6.0 / +8.7 / +9.5% | 42.0 / 52.7 / 64.3% |
| the same + past extremes | +5.8 / +8.2 / +10.5% | 41.6 / 52.6 / 64.9% |
| v10 embedding | −2.1 / −0.9 / +1.0% | 36.1 / 46.6 / 59.4% |

The 2022 year agrees at day 4–14 (18 numbers +6.7 / +10.1%, v10 −2.4 / +1.3%); at day 1–3 no
context is reliably different from none there.

Readings:

1. **v10 fails the generality rule.** It does not beat physical statics on flood size, regime,
   similar behaviour or donor flow, and as forecast context it is level with no context in all
   three forecaster seeds. It reads each basin's flow record, yet recovers no more of the
   hand-built description on unseen basins than statics that never saw the flow (mean R² 0.43
   against 0.44 over the 21 numbers; `representation_benchmark.py recover`,
   `reports/recover_seed0.json`).
2. **Eighteen hand-built numbers are the bar.** They are the best description found on
   forecasting and on similarity, and near the best on extremes and regime. The frozen
   pretrained code has to match them; nothing learned has come close so far.
3. **Wide descriptions are poor similarity spaces.** Adding the 102 statics to the 18 numbers
   changes forecasting and extremes little and lowers similar behaviour from 0.76 to 0.57 and
   donor flow from 0.39 to 0.33. Similar-basin search is where a compact learned code has the
   clearest job.
4. **The forecasting gain from gauged context looks saturated near 6–10%.** Adding the response
   numbers to statics + signatures, or past extremes on top, moves the error cut by less than
   the spread between seeds. A learned code should be expected to match this in the tree
   forecaster, not to beat it by much. Its case rests on being one compact code for every task,
   on the ungauged student, and on being usable inside a neural forecaster.
5. **For ungauged basins, 102 physical statics tell the forecaster no more than two
   coordinates** (+2.6 / +3.0 / +3.9% against +3.6 / +3.4 / +4.1%). The ungauged bar therefore
   includes location. The room above it is the distance to the gauged description: 4–6 points
   of error cut, 0.54 to 0.88 mean R² on extremes, 0.46 to 0.77 on regime, 0.33 to 0.76 on
   similar behaviour. Statics recover climate and size well on unseen basins (aridity 0.91,
   snow share 0.89, mean flow 0.84) and storage and timing badly (rain-to-flow delay 0.01,
   flow-duration slope 0.02, recession rate 0.11, no-flow share 0.11, baseflow index 0.32).
   Those are what imagery or any other new input has to supply.

Caveats:

- A gauged basin's behaviour is stable across periods (rank correlation 0.93–0.98 for most
  numbers; 0.40 for the half-flow day and 0.52 for the delay). The extremes targets are
  therefore close to persistence for any description that contains the earlier value (storm
  response sits in the response bank; flood and low-flow sizes in the extremes bank). They
  discriminate mainly for descriptions that do not read the gauge.
- Donor flow is helped by shared weather: location alone scores 0.24.
- Regime classes are rule-based (no flow on over 5% of days; at least 30% snow and half the
  flow in April to July; upper terciles of flashiness and baseflow index among the rest) and
  so close to the signatures by construction. Published classes are on the roadmap.
- The suite used one readout seed (intervals are over basins) and v10 is one encoder seed.
- Snowpack files hold 54 days in total for every Pennsylvania basin and Florida has none, so
  the snowpack input is empty outside CO, UT and WY; a snowpack number was left out of the
  description for that reason.
- Not done: which of the 18 numbers carry the forecast gain (per-number importance).

## 8. Step 2, first result: a code pretrained on behaviour, read from the gauged record (2026-11-12)

**What was built.** Flow Forecast branch `behaviour-pretraining` (local, one commit above
`crossformer-static-conditioning`) adds `flood_forecast/meta_models/behaviour_pretrain.py`: the
existing `MultiModalEncoder` writes a code, a shared recurrent decoder must reproduce a target
series from its drivers and the code, optional side targets are read linearly from the code,
and modality dropout trains the code to work from any subset of readers. Water's
`pretrain_behaviour_code.py` feeds it the pre-2022 daily record in physical units (log flow in
mm per day, rain, temperature at basin elevation, evaporation demand, season): the reader sees
three water years, the decoder gets the weather of a fourth and must reproduce its flow.
Settings: 32-number code, 343 fitting and 60 validation basins (all from the 403 training
basins; the epoch is picked on the validation basins), 60 epochs, about 10 minutes on CPU and
under 1 GB. Two variants: reconstruction only, and reconstruction plus 14 behaviour numbers of
the fitting basins as side targets. Held-out basins are never trained on; their codes come from
their own pre-2022 record, which is what the hand-built description also reads.

**Scores.** 123 held-out basins, targets from 2022 on; mean of three seeds (each seed is a new
encoder, readout and forecaster). Forecast is the error cut against flow + weather + snow on
2023 onward, day 1–3 / 4–7 / 8–14.

| Description | Forecast error cut | Flood size | Flood ÷ mean flow | Storm response | Low flow | Regime | Similar behaviour | Donor flow |
|---|---|---|---|---|---|---|---|---|
| Physical statics (102) | +2.6 / +3.0 / +3.9% | 0.62 | 0.53 | 0.67 | 0.34 | 0.46 | 0.33 | 0.30 |
| Hand-built, gauged (18) | +7.5 / +8.3 / +9.8% | 0.87 | 0.87 | 0.91 | 0.87 | 0.77 | 0.76 | 0.39 |
| Pretrained code, reconstruction only (32) | +2.4 / +2.4 / +5.0% | 0.64 | 0.37 | 0.75 | 0.86 | 0.59 | 0.55 | 0.42 |
| Pretrained code, with side targets (32) | +5.3 / +6.5 / +8.4% | 0.74 | 0.69 | 0.82 | 0.87 | 0.65 | 0.62 | 0.43 |
| v10 (256), one encoder seed | −2.1 / −0.9 / +1.0% | 0.48 | 0.46 | 0.63 | 0.62 | 0.47 | 0.23 | 0.06 |

Readings:

1. **Rule 1 is met by the code with side targets.** It is above physical statics on all seven
   suite tasks in all three seeds; the 95% interval of the difference excludes zero in 20 of
   the 21 task-seed pairs (the exception is flood ÷ mean flow in one seed). It is the first
   learned representation here that helps the forecaster on unseen basins. Three seeds on fold
   0, so this is a development-fold result.
2. **Rule 2, the forecasting gate, is not met yet.** The frozen code is 1.4 to 2.2 points of
   error cut below the 18 hand-built numbers, and below them on floods, storm response, regime
   and similar behaviour (level on low flow). The gate applies to the aligned code (step 3),
   which has not been tried.
3. **The side targets carry much of the gain.** Reconstruction alone gives a code that beats
   statics on low flow, regime, similar behaviour and donor flow, ties on flood size, loses on
   flood ÷ mean flow (0.37 against 0.53) and helps the forecaster no more than statics. The
   loss is on log flow, which weights quiet days as much as floods. With side targets the code
   is in part a learned compression of the hand-built numbers.
4. **Donor flow is the one task where the code leads** (0.43 against 0.39; seeds 0.40 to
   0.47), in line with the reading in §7 that a compact code is a better similarity space.
5. **More epochs do not help; the run is limited by the number of basins.** At 150 epochs
   (one seed) the loss on the fitting basins keeps falling (0.42 to 0.36) while the validation
   basins move from 0.530 to 0.512. Downstream the suite is unchanged within the seed spread
   (regime 0.63 to 0.67, donor flow 0.40 to 0.47, the rest within 0.01) and the forecast gain
   is no better (+3.6 / +4.4 / +7.5% against +5.8 / +6.3 / +8.6% for the same seed at 60).
   *Corrected in §10: 2.7 times the basins do not lower this loss either. They improve the
   code on floods and regime, not the reconstruction and not the forecast.*

What this does not show: nothing about ungauged basins (this code reads the gauge), nothing
about imagery, and nothing on fold 1.

Next, in order: (a) add the statics and imagery readers with modality dropout and score the
code with the gauge reader off against statics and location (rule 3), done in §9; (b) align
the code to forecasting (rule 2); (c) more basins.

## 9. Step 2, second result: statics and reach-image readers added, trained together (2026-11-13)

**What was built.** Flow Forecast (`behaviour-pretraining`, commit `5d3518b7`, local):
`MultiModalEncoder.encode` and `extract_embeddings` take `modalities=[...]` and zero the other
readers exactly as the training dropout does; `pretrain_behaviour` takes `validation_subsets`
and picks the restored epoch on their mean. Water's `pretrain_behaviour_code.py` gained two
readers next to the gauged record: the static description (the `statics` and `location` banks
the baselines read, 104 numbers) and the reach image (four 10 m bands, 128 × 128 pixels at the
gauge, patches of 16, turned and mirrored at random in training). Readers are switched off with
probability 0.3 per training item and the epoch is chosen on the mean of the gauged and the
ungauged validation loss. One run writes five banks: all readers, the gauged record alone,
ungauged (statics + image), statics alone, image alone. Other settings as in §8 with side
targets. 13 minutes per seed on CPU, 1.2 GB. Both scoring tools gained `--also-pair`, which
compares every arm with further arms basin by basin.

**Scores.** 123 held-out basins, targets from 2022 on, mean of three seeds. Forecast is the
error cut against flow + weather + snow on 2023 onward, day 1–3 / 4–7 / 8–14.

| Description | Forecast error cut | Flood size | Flood ÷ mean flow | Storm response | Low flow | Regime | Similar behaviour | Donor flow |
|---|---|---|---|---|---|---|---|---|
| Physical statics (102) | +2.6 / +3.0 / +3.9% | 0.62 | 0.53 | 0.67 | 0.34 | 0.46 | 0.33 | 0.30 |
| Location (2) | +3.6 / +3.4 / +4.1% | 0.36 | 0.18 | 0.50 | 0.09 | 0.40 | 0.31 | 0.24 |
| Statics + location (104) | +3.6 / +4.1 / +5.0% | 0.65 | 0.49 | 0.64 | 0.34 | 0.47 | 0.33 | 0.28 |
| Gauge-only code of §8 (32) | +5.3 / +6.5 / +8.4% | 0.74 | 0.69 | 0.82 | 0.87 | 0.65 | 0.62 | 0.43 |
| Three readers, all on | +3.5 / +4.4 / +6.4% | 0.72 | 0.61 | 0.78 | 0.78 | 0.64 | 0.56 | 0.42 |
| Three readers, gauged record only | +3.1 / +3.5 / +6.0% | 0.72 | 0.57 | 0.81 | 0.87 | 0.65 | 0.60 | 0.43 |
| Three readers, ungauged (statics + image) | +1.6 / +3.4 / +4.7% | 0.58 | 0.52 | 0.56 | 0.18 | 0.44 | 0.34 | 0.26 |
| Three readers, statics only | +1.3 / +3.0 / +4.7% | 0.58 | 0.55 | 0.52 | 0.15 | 0.47 | 0.37 | 0.29 |
| Three readers, image only | −2.3 / −0.8 / +0.4% | 0.26 | −0.33 | 0.33 | −0.09 | 0.34 | 0.01 | 0.08 |
| Ungauged code handed to other basins | −2.4 / −2.1 / −3.6% | | | | | | | |

Decoder error on the 60 validation basins (mean squared error of log flow; training-fold
basins the run did not fit on). The first row was computed afterwards from the saved models
with every basin given the mean code; it is not in the run reports.

| Code written from | Seed 0 | Seed 1 | Seed 2 |
|---|---|---|---|
| Nothing (mean code) | 1.74 | 1.24 | 1.30 |
| Image | 1.64 | 1.29 | 1.21 |
| Statics | 1.15 | 0.91 | 0.85 |
| Statics + image | 1.18 | 0.87 | 0.88 |
| Gauged record | 0.62 | 0.56 | 0.49 |
| All three | 0.66 | 0.58 | 0.55 |
| Gauge-only run of §8, for reference | 0.51 | 0.51 | 0.43 |

Readings:

1. **Rule 3 is not met: the ungauged code ties raw statics and location, it does not beat
   them.** Paired basin by basin in the forecaster it is −0.2 / −0.2 / −0.2 points against
   statics, −1.5 / 0.0 / +1.3 against location and −1.6 / −0.8 / −0.5 against statics +
   location. Of those 27 band-seed intervals two exclude zero: one above (location, day
   8–14) and one below (statics + location, day 1–3). On the suite it is level with statics on five tasks and lower
   on storm response (0.56 against 0.67, interval below zero in two seeds) and low flow (0.18
   against 0.34, one seed). The code does describe the basin: handed to other basins of the
   same split it costs the forecaster 2 to 4 points instead of giving 2 to 5, and it removes
   about 30% of the validation error of the mean code. It just holds no more than its inputs.
2. **That is what the setup allows.** The ungauged code is a function of the statics, fitted
   on 343 basins, and is then read by a model fitted on 403. Nothing new enters, and 32
   numbers keep less than 104 for a readout that can use the 104 directly. An ungauged code
   can only win through inputs that add information, or by being trained on far more basins
   than the downstream model has.
3. **The reach image adds nothing here.** The image-only code sits at the mean-code level on
   the validation basins, is no better than a shuffled code in the forecaster, and adding the
   image reader to the statics reader changes nothing. Hand-built numbers from the same
   patches (band means and spreads, NDVI, NDWI, and for the regional patch NDMI, NDSI and the
   snow share; `representation_benchmark.py imagery`) show how much is there to find. Alone
   they reach 0.30 / 0.03 / 0.39 / 0.21 on the extremes (reach) and −0.1 / +1.8 / +2.1% in the
   forecaster (reach + regional), so the patches are not empty and the image reader, trained
   from scratch on 343 basins, extracts less than 13 band statistics do. Added to statics
   + location they move flood size by +0.02 and storm response by +0.05 (interval above zero
   in all three seeds), nothing else on the suite, and the forecaster by −0.7 / 0.0 / 0.0
   points. Even read perfectly, a patch centred on the gauge says little that the statics do
   not. A longer GPU run on the regional patches is not justified by this.
4. **Training the readers together cost the gauged path about 2 points of forecast gain**
   (+3.5 / +4.4 / +6.4% against +5.3 / +6.5 / +8.4% for the gauge-only code; paired −1.7 /
   −2.0 / −1.8). Two causes are visible. The ungauged validation loss bottoms early (epochs
   22, 42, 39) while the gauged one keeps falling (29, 53, 58), so one restored epoch (22, 58,
   39) undertrains the gauge reader; seed 0, restored at 22, gives +0.3 / +4.6 / +3.8%. And
   with all readers on the validation error is higher than with the gauged record alone in
   every seed: on a gauged basin the other two readers add noise. This run did not follow the
   order set in §3 (gauge reader first, the others added to it). The next version does.
5. **Statics + location is the ungauged bar in the forecaster** (+3.6 / +4.1 / +5.0%),
   slightly above either alone.

**Second version, in the order of §3: gauge reader first, statics reader fitted to it.**
`pretrain_behaviour_code.py align` loads the gauge-only run of §8 and leaves its reader and
decoder unchanged (the run report checks the decoder weights). A statics reader (statics +
location; no image reader, given reading 3) is trained on the same fitting basins so that the
frozen decoder reproduces a water year from its code and its code matches the basin's gauge
code. 5 minutes per seed on CPU. Two banks, both on the scale of the gauge bank: `student`
(every basin's code from statics) and `transfer` (gauge code for training basins, statics code
for held-out basins: what a model fitted on gauged basins sees when applied to ungauged ones).

| Description | Forecast error cut | Flood size | Flood ÷ mean flow | Storm response | Low flow | Regime | Similar behaviour | Donor flow |
|---|---|---|---|---|---|---|---|---|
| Physical statics (102) | +2.6 / +3.0 / +3.9% | 0.62 | 0.53 | 0.67 | 0.34 | 0.46 | 0.33 | 0.30 |
| Statics + location (104) | +3.6 / +4.1 / +5.0% | 0.65 | 0.49 | 0.64 | 0.34 | 0.47 | 0.33 | 0.28 |
| Gauge code of §8, unchanged | +5.3 / +6.5 / +8.4% | 0.74 | 0.69 | 0.82 | 0.87 | 0.65 | 0.62 | 0.43 |
| Statics code, every basin | +2.4 / +3.3 / +3.9% | 0.60 | 0.46 | 0.54 | 0.17 | 0.46 | 0.35 | 0.29 |
| Transfer: gauge code in training, statics code held out | +0.7 / +2.4 / +3.9% | 0.54 | 0.46 | 0.56 | 0.14 | 0.46 | 0.38 | 0.23 |

Validation error (as in the table above; seeds 0 / 1 / 2): mean code 1.77 / 1.43 / 1.26,
statics code 1.24 / 0.88 / 0.86, gauge code 0.51 / 0.51 / 0.43. The statics reader is at its
best after 7, 3 and 26 epochs and overfits from there.

6. **The gauged path is whole again and the two codes share a space.** A gauged basin gets
   the code of §8; an ungauged basin gets a code in the same space from its statics. Readouts
   and neighbour search fitted on gauge codes work on statics codes about as well as when
   fitted on statics codes: the transfer row is within the seed spread of the row above it,
   except the short forecast band (+0.7 against +2.4%) and donor flow (0.23 against 0.29).
   Looking up gauged donors for an ungauged basin is the one place the shared space leads
   statics (similar behaviour 0.38 against 0.33, interval above zero in two seeds).
7. **Rule 3 is still not met, for the reason in reading 2.** The statics code is −0.2 / 0.0 /
   −0.5 points against statics in the forecaster and level or lower on the suite (storm
   response 0.54 against 0.67, low flow 0.17 against 0.34). Fitting it separately changed
   nothing about what the statics know.

Where step 2 stands: one embedding, a gauge reader that meets rule 1 (§8), a statics reader
that puts ungauged basins in the same space at the level of raw statics, and no usable image
reader. The limits are the number of distinct basins (343 to fit on; §8 reading 5 and the
early overfitting of the statics reader) and what the ungauged inputs contain.

What this does not show: nothing on fold 1; no test of catchment-wide inputs (the patches are
centred on the gauge, and the statics hold no land cover, dams or withdrawals); no test of the
weather record of an ungauged basin as a reader; the aligned code has not been tuned (one
setting, code-matching weight 1).

Next, in order of expected effect: (a) more basins for pretraining (step 5 of §5), which is
the only lever that bears on both readers; (b) align the gauge code to forecasting (rule 2),
which needs no new data; (c) catchment-wide inputs for the ungauged readers (land cover, dams,
terrain over the basin and not a patch at the gauge), with the hand-built imagery numbers of
reading 3 as the first check of any new image source.

*§10 did (a) with the basins already on disk. It helped the gauge reader on the general tasks
only and did nothing for the statics reader, so "the only lever that bears on both readers"
was wrong.*

## 10. Step 5, first part: more basins without a download (2026-11-13)

**What was on disk.** The cohort of §7–§9 holds 533 basins because `basin_split_v1` took only
gauges with a panel record, and a panel record needs GAGES-II attributes, a cloud-free Sentinel
patch at the gauge and enough hourly coverage. Pretraining on the daily record needs none of
the three. Of the 1,586 gauges scraped in six states, 1,244 pass the manifest's checks
(drainage area, two years of flow before 2022, plausible runoff): 287 in Colorado, 281 in
Florida, 279 in Pennsylvania, 191 in Oregon, 121 in Utah, 85 in Wyoming. Oregon was scraped in
August and never used. The first part of step 5 therefore needed no download.

**What was built.**

- `build_basin_split.py --extend` and `splits/basin_split_v2.json`: v1 plus 689 gauges. No
  site of v1 moves. A new gauge joins the fold its HUC6 group already has, so it never trains
  next to a held-out neighbour; groups v1 never saw fill the lightest fold of their state. The
  additions are 79 gauges in fold 0, 151 in fold 1 and 459 in folds 2 to 4.
- `pretrain_behaviour_code.py pool`: the pre-2022 record of 598 extra basins (589 with two
  usable water years, 420 of those in GAGES-II). Left out: the cohort's own basins, 89 gauges
  whose HUC6 group holds a fold-0 site, and 24 tidal gauges (negative flow above 1%, the
  cohort's rule). `train --with-pool` and `align --with-pool` add the pool to the fitting
  basins and to nothing else.
- For the statics reader the pool carries the `statics` and `location` numbers of its basins on
  the cohort's scale; the cohort's rows are rebuilt alongside and must reproduce the banks on
  disk.

The comparison is matched by construction: the same 123 held-out basins, the same 60 validation
basins, readouts and forecasters fitted on the same 403 basins as in §8 and §9. Only the basins
the encoder is pretrained on change: 343 to 932 for the gauge reader, 343 to 763 for the
statics reader. Pooled runs use 30 epochs (1.36 times the gradient steps of 60 epochs on 343
basins). A wider model (reader and decoder width 128 for 64) was run with and without the pool
to separate width from basin count. On CPU at 1.2 GB per run: 14 minutes per seed at width 64
and 20 at width 128 with three runs at a time, 34 to 39 minutes when four ran at once.

**Scores.** 123 held-out basins, targets from 2022 on, mean of three seeds. Forecast is the
error cut against flow + weather + snow on 2023 onward, day 1–3 / 4–7 / 8–14. Validation is the
flow-reconstruction loss on the 60 validation basins.

| Description | Fitting basins | Forecast error cut | Flood size | Flood ÷ mean flow | Storm response | Low flow | Regime | Similar behaviour | Donor flow | Validation |
|---|---|---|---|---|---|---|---|---|---|---|
| Hand-built, gauged (18) | – | +7.5 / +8.3 / +9.8% | 0.87 | 0.87 | 0.91 | 0.87 | 0.77 | 0.76 | 0.39 | – |
| Gauge code of §8 (width 64) | 343 | +5.3 / +6.5 / +8.4% | 0.74 | 0.69 | 0.82 | 0.87 | 0.65 | 0.62 | 0.43 | 0.48 |
| Width 64, with pool | 932 | +4.3 / +7.3 / +8.5% | 0.80 | 0.70 | 0.85 | 0.87 | 0.68 | 0.61 | 0.44 | 0.49 |
| Width 128 | 343 | +4.9 / +6.3 / +8.3% | 0.74 | 0.68 | 0.84 | 0.87 | 0.65 | 0.61 | 0.42 | 0.47 |
| Width 128, with pool | 932 | +5.3 / +5.5 / +8.4% | 0.82 | 0.74 | 0.87 | 0.88 | 0.71 | 0.63 | 0.45 | 0.46 |
| Physical statics (102) | – | +2.6 / +3.0 / +3.9% | 0.62 | 0.53 | 0.67 | 0.34 | 0.46 | 0.33 | 0.30 | – |
| Statics + location (104) | – | +3.6 / +4.1 / +5.0% | 0.65 | 0.49 | 0.64 | 0.34 | 0.47 | 0.33 | 0.28 | – |
| Statics code of §9 | 343 | +2.4 / +3.3 / +3.9% | 0.60 | 0.46 | 0.54 | 0.17 | 0.46 | 0.35 | 0.29 | 0.99 |
| Statics code, with pool | 763 | +2.3 / +4.1 / +4.0% | 0.58 | 0.51 | 0.55 | 0.18 | 0.46 | 0.37 | 0.25 | 0.96 |

Readings:

1. **More basins improve the gauge code on the general tasks.** With the pool, flood size
   rises by 0.06 at width 64 and by 0.08 at width 128, and the 95% interval of the per-basin
   difference is above zero in all three seeds at both widths. Storm response rises by 0.03
   and 0.02 (two of three seeds), regime by 0.02 and 0.06. Width without the pool changes
   nothing. Width 128 with the pool is the best learned code on all seven tasks and halves the
   gap to the hand-built numbers on flood size (0.13 to 0.06, inside the interval in every
   seed), storm response (0.09 to 0.04) and regime (0.12 to 0.06). Flood ÷ mean flow and
   similar behaviour stay 0.13 below.
2. **The forecast gate does not move.** The four gauge codes are within noise of each other in
   the tree forecaster: paired differences from −0.8 to +0.9 points, the interval excluding
   zero in at most one seed. All four stay about 2 points below the hand-built numbers. Rule 2
   is no closer.
3. **The reconstruction loss on unseen basins hardly moves**: 0.48 to 0.49 at width 64, 0.47 to
   0.46 at width 128. Per basin at width 64 the pooled code is better on 31 to 36 of the 60
   validation basins and the median is 0.32 either way; five basins, mostly Florida canals,
   hold 22 to 34% of the summed loss. §8 reading 5 concluded from the flat loss at 150 epochs
   that the run was limited by the number of basins. That was an inference, not a test, and it
   was wrong for this loss. What the extra basins improve is what the code says about floods
   and regime, which a log-flow loss weights little.
4. **The statics reader gains nothing from 2.2 times the basins.** Flow loss from its code
   0.99 to 0.96 (the gauge code reaches 0.48). Paired with the §9 code: forecaster +0.1 / +0.7
   / −0.5 points and no suite task outside the interval; against statics + location +0.3 / 0.0
   / −1.0. The restored epochs are still early (10, 33 and 3). Rule 3 is still not met. The
   limit is what the 104 numbers hold, not how many basins they are fitted on.
5. **This is no case for a national download for pretraining.** 2.7 times the basins bought
   0.03 to 0.08 on three suite tasks and nothing in forecasts or for ungauged basins.

What this does not show:

- The readouts and the forecaster were still fitted on 403 basins. Whether they improve with
  more basins was not tested, and the earlier evidence for basin count came from there
  (context began to transfer between 85 and 403 basins, `FORECAST_REVERIFICATION` §13–§14).
- Pool basins get no code and are not scored. The 79 new fold-0 gauges of v2 are untouched and
  would double the held-out set; nothing on fold 1.
- The statics reader was fitted to the §8 gauge code, to isolate the effect of basins on it.
  It has not been refitted to the width-128 pooled code.
- 30 epochs with the pool; the width-128 pooled run of seed 2 was still improving at 30.
- The pool includes 169 gauges outside GAGES-II (often regulated or urban); a pool of
  reference-quality basins only was not tried.

Next, in order of expected effect on forecasts: (a) fit the forecaster and the readouts on the
larger basin set, scored on the same 123 held-out basins. Everything is on disk, but the
forecaster table must be rebuilt for all basins: Pennsylvania's snow series were backfilled
after the cohort's table was built, so every bar is re-based. (b) Align the gauge code to
forecasting (rule 2). (c) Catchment-wide inputs for the ungauged readers.
