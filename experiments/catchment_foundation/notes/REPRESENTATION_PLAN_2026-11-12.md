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
