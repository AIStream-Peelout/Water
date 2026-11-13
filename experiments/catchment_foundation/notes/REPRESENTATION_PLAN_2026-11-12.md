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
2. Aligned code: beat statics + signatures on forecasting (+6–9% error cut today). This is the
   gate; candidates that pass are ranked on the other three tasks.
3. Ungauged student: beat physical statics on forecasting (+3–5% today) and on the suite.
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
