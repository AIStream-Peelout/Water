# The catchment embedding improves forecasts when it is used through its decoder

2026-11-27. Five-state cohort, fold 0 held out (`calibration/context_ablation_fleet5_f0/`):
403 training basins, 123 scored held-out basins in five river regions, forecasts issued from
2023 on, three seeds. Fold 1 is still unscored.

## 1. What changed

Until now a basin's description reached the forecaster as constants: 32 numbers of a learned
code, or 18 hand-built numbers, appended to every row of the tree forecaster. The best such arm
cut held-out error by 7.5 / 8.3 / 9.8% (days 1-3 / 4-7 / 8-14).

Behaviour pretraining leaves a second thing besides the code: a decoder that turns daily
weather into flow for a given code. `decoder_simulation.py` runs that decoder over each basin's
whole record and gives the forecaster, per issue day and lead band, what the simulation says
will change, plus how far the simulation currently is from the observed flow. That cuts
held-out error by 14.7 / 13.2 / 17.7%, and by 17.7 / 16.1 / 19.3% together with the 18
hand-built numbers. Median held-out skill over persistence goes from 40 / 50 / 59% to
47 / 57 / 67%.

The decoder reads weather only, and only up to the day it writes. The forecaster already had
the realized weather of the horizon, so no arm sees more of the future than the reference does.

## 2. Forecast results

Tree forecaster (`tabular_forecast_baseline.py run --fit-subsample 3`), 123 held-out basins,
2023 and later, mean of three seeds. "Error cut" is the median basin's reduction in summed
squared error against the reference; the bracket counts the seeds whose basin-bootstrap 95%
interval excludes zero.

| forecaster inputs | median skill | error cut against the reference |
|---|---|---|
| reference: flow, weather, snowpack | 40.1 / 49.5 / 58.7% | |
| + issue-day flow as a depth (control) | 38.2 / 47.7 / 59.3% | +2.0 [1] / +1.7 [1] / +2.9 [2] |
| + 18 hand-built numbers (best arm before) | 42.3 / 54.1 / 63.8% | +7.5 [3] / +8.3 [3] / +9.8 [3] |
| + learned code as constants | 40.7 / 51.3 / 63.1% | +5.3 [3] / +6.5 [3] / +8.4 [3] |
| + simulation, decoder given no code | 42.2 / 48.7 / 60.9% | +7.7 [3] / +4.0 [3] / +5.8 [3] |
| + simulation, another state's code | 38.2 / 48.6 / 58.3% | +1.5 [1] / +0.5 [0] / +1.0 [1] |
| + simulation, nearest training basin's code | 42.1 / 49.3 / 57.9% | +10.2 [3] / +5.9 [3] / +7.7 [3] |
| + simulation, code read from statics only | 45.0 / 51.5 / 62.2% | +10.8 [3] / +5.8 [3] / +10.0 [3] |
| + simulation, decoder conditioned on the 18 numbers | 48.1 / 54.3 / 65.3% | +15.1 [3] / +13.6 [3] / +16.9 [3] |
| **+ simulation, learned code from the gauge record** | 46.7 / 55.5 / 66.3% | **+14.7 [3] / +13.2 [3] / +17.7 [3]** |
| + 18 numbers + simulation with no code | 44.9 / 54.3 / 63.1% | +15.3 [3] / +12.2 [3] / +12.8 [3] |
| **+ 18 numbers + simulation with the learned code** | 47.3 / 56.7 / 66.9% | **+17.7 [3] / +16.1 [3] / +19.3 [3]** |
| + statics and location | 40.9 / 51.4 / 62.5% | +3.6 [2] / +4.1 [3] / +5.0 [3] |
| + statics and location + simulation with no code | 42.2 / 51.6 / 62.4% | +8.0 [3] / +5.8 [3] / +5.8 [3] |
| + statics and location + simulation, statics code | 44.7 / 53.3 / 61.4% | +8.3 [3] / +6.8 [3] / +8.6 [3] |

Paired directly, basin by basin:

| comparison | error cut |
|---|---|
| simulation with the learned code against the 18 numbers as constants | +7.3 [2] / +6.3 [3] / +9.3 [3] |
| simulation with the learned code against simulation with no code | +7.6 [3] / +8.8 [3] / +9.6 [3] |
| simulation with the learned code against the decoder conditioned on the 18 numbers | −0.7 [0] / −0.4 [1] / +0.2 [1] |
| 18 numbers + learned-code simulation against 18 numbers + no-code simulation | +1.9 [0] / +4.2 [2] / +6.4 [3] |
| simulation with the statics code against simulation with no code | +2.0 [1] / +3.6 [2] / +3.3 [1] |
| statics, location + statics-code simulation against statics, location + no-code simulation | +0.2 [0] / +0.5 [0] / +1.8 [1] |
| simulation with the nearest basin's code against simulation with no code | +2.1 [0] / +1.3 [0] / −0.8 [0] |

Readings:
1. **The route matters more than the description.** A simulator that knows nothing about the
   basin already gives the trees as much as the 18 hand-built numbers do at days 1-3 (+7.7%).
2. **Knowing the basin is worth a further 8 to 10%**, in every band and seed. A code from
   another state removes the whole gain, so this is the basin's own behaviour, not a generic
   effect of feeding the trees a simulation.
3. **The learned code and the 18 hand-built numbers tie as the decoder's description.** The
   embedding now clearly helps forecasts, but nothing here shows it holds more than the
   hand-built numbers do. It was trained with 14 of them as side targets.
4. **The ungauged case is not solved.** A code read from statics alone adds 2 to 4% over the
   no-code simulation, mostly inside its intervals, and nothing once statics and location are
   also given to the trees. Borrowing the nearest gauged basin's code is no better.

### By region

Each state holds exactly one held-out river region, so the five columns are the five
independent tests this split offers. Median basin error cut, each basin's three seeds averaged
first (`by_state.py`); "pooled" is the cut in summed squared error over all 123 basins.

| comparison, band | all | CO (38) | FL (30) | PA (28) | UT (16) | WY (11) | basins better | pooled |
|---|---|---|---|---|---|---|---|---|
| learned-code simulation against the reference, days 1-3 | +13.8% | +10.4% | +4.6% | +30.1% | +5.6% | +23.9% | 77% | +15.3% |
| days 4-7 | +13.0% | +7.1% | +7.5% | +21.6% | +11.4% | +23.7% | 83% | +9.1% |
| days 8-14 | +17.3% | +16.8% | +6.1% | +33.3% | +16.2% | +19.7% | 84% | +13.1% |
| against the no-code simulation, days 1-3 | +6.8% | +6.7% | +4.8% | +11.2% | +4.9% | +23.0% | 67% | +5.3% |
| days 4-7 | +8.1% | +12.9% | +4.5% | +7.0% | +11.8% | +18.9% | 76% | +5.3% |
| days 8-14 | +9.9% | +13.3% | +4.3% | +12.2% | +9.9% | +13.4% | 81% | +7.3% |
| against the 18 numbers as constants, days 1-3 | +6.9% | −2.9% | +3.9% | +23.7% | +3.9% | +23.4% | 63% | +7.1% |
| days 4-7 | +4.1% | +2.6% | +1.9% | +12.9% | +1.1% | +19.7% | 67% | +1.5% |
| days 8-14 | +9.2% | +8.7% | +2.9% | +20.2% | −5.3% | +13.8% | 71% | +5.4% |
| against the decoder conditioned on the 18 numbers, days 1-3 | −0.5% | −3.1% | −5.0% | +0.7% | −0.8% | +12.0% | 48% | −2.3% |
| days 4-7 | −0.7% | −5.8% | −2.7% | +0.1% | +2.7% | +17.6% | 47% | −1.1% |
| days 8-14 | +0.6% | −3.3% | −0.4% | +2.4% | +0.6% | +17.2% | 54% | +1.7% |
| statics-code simulation against the no-code simulation, days 1-3 | +2.4% | −3.5% | +0.1% | +9.5% | +4.6% | +7.3% | 57% | +3.3% |
| days 4-7 | +2.2% | +2.5% | +0.4% | +4.5% | +4.6% | +11.0% | 59% | +1.8% |
| days 8-14 | +4.0% | −0.5% | +2.6% | +8.8% | +3.3% | +13.7% | 57% | +1.3% |

The gain over the reference and the gain over the no-code simulation are positive in all five
regions and all three bands. The gain is largest in Pennsylvania and Wyoming and smallest in
Florida. Against the 18 numbers as constants it holds in 13 of 15 region-band cells. The
statics code's edge over no code is small everywhere and negative in Colorado.

Checks the independent reviewer ran on seed 0 (its scripts, not rerun here): a reference
given the decoder's daily weather for each of the 14 days gains +4.5 / +2.8 / +1.1%, so the
simulation is not just finer weather; a per-basin seasonal climatology through the same
feature code gives +2.4 / +2.1 / +5.5%; the change features alone, which never read observed
flow, give +11.8 / +12.4 / +16.5%; and a simulation shifted by three days still gives
+7.5 / +14.1 / +14.2%, so much of the gain is the basin's slow state rather than storm timing.

### Out-of-sample simulations on the fitting rows (seed 0)

The trees are fitted on basins and years the decoder was trained on, with codes that had read
the year being simulated. To see whether that distorts the comparison, each training basin was
simulated by a decoder trained without its fold (the four training folds are whole river
regions), with every water year's code written from the basin's other years
(`simulate --out-of-sample`). On the fitting rows this lowers the simulation's median NSE of log
flow from 0.69 to 0.62, level with the held-out basins' 0.61.

| arm (seed 0; fitting rows whose targets pass the cutoff are now left out) | error cut against the reference |
|---|---|
| + temperature correction from gauge to basin elevation (control) | +0.4 (−1, +2) / +1.4 (0, +2) / +1.0 (−1, +2) |
| + simulation, learned code, in-sample on the fitting rows | +13.3 (+9, +18) / +13.1 (+10, +16) / +20.3 (+15, +24) |
| + simulation, learned code, out of sample on the fitting rows | +14.6 (+10, +20) / +13.7 (+10, +17) / +17.2 (+15, +23) |
| + 18 numbers + simulation, out of sample | +19.0 (+13, +23) / +14.2 (+11, +19) / +19.1 (+14, +26) |

Out of sample against in-sample, paired: +0.1 (−2, +2) / +0.6 (−2, +2) / +0.1 (−1, +1). The
optimism on the fitting rows did not matter, so the three-seed table above stands as it is.
The decoder's temperature carries a correction to basin elevation that the trees' weather lacks;
given to the trees as a feature it is worth about 1%.

## 3. Simulation alone, with no observed flow

The same decoders as pure simulators on the held-out basins after 2022: weather in, flow out,
no flow observation of any kind (mean of three seeds, per-basin scores).

| description given to the decoder | median NSE of log flow | median NSE | median KGE | basins with NSE of log flow above 0.5 |
|---|---|---|---|---|
| learned code from the gauge record | 0.59 | 0.42 | 0.37 | 60% |
| the 18 hand-built numbers | 0.56 | 0.41 | 0.35 | 59% |
| learned code from statics only | 0.14 | 0.30 | 0.26 | 32% |
| no description | 0.05 | 0.16 | 0.12 | 26% |
| nearest training basin's code | −0.36 | 0.02 | 0.03 | 24% |
| another state's code | −1.62 | −0.32 | −0.26 | 4% |

By state, NSE of log flow with the statics code against no description: CO −2.22 / −1.37,
FL 0.13 / 0.07, PA 0.70 / 0.32, UT −0.07 / −0.28, WY 0.62 / 0.34. The statics reader helps in
four regions and fails in Colorado's.

Decoder validation loss (log flow, 60 validation basins, seeds 0 / 1 / 2): learned code
0.505 / 0.507 / 0.431, decoder on the 18 numbers 0.506 / 0.493 / 0.425, no code
1.297 / 1.111 / 0.921.

## 4. A neural forecaster from the same parts (seed 0)

`decoder_forecaster.py`: Flow Forecast's `DriverConditionedDecoder` (a GRU) reads 180 days of
weather and observed flow up to the issue day, then the realized weather of the next 14 days,
and writes the daily change in flow. A small reader turns a frozen per-basin description into
the code it is conditioned on. It is trained with Flow Forecast's `pretrain_behaviour` loop on
the trees' fitting rows (30 passes of 60,000 windows, 12 minutes on the Mac's GPU) and scored
on the trees' rows with the trees' scoring. Held-out basins, 2023 and later:

| forecaster | median skill | error cut against the tree reference | against the GRU with no description |
|---|---|---|---|
| trees, reference | 38.7 / 48.4 / 57.6% | | |
| trees + learned-code simulation | 49.2 / 56.7 / 66.8% | +13.3 / +13.1 / +20.3% | +5.1 (+1, +8) / +4.0 (+1, +6) / +4.1 (+1, +8) |
| trees + 18 numbers + learned-code simulation | 52.2 / 56.9 / 68.2% | +15.6 / +16.6 / +21.7% | +7.7 (+5, +11) / +6.0 (+3, +10) / +6.5 (+2, +11) |
| GRU, no description | 41.7 / 53.7 / 64.7% | +11.7 (+3, +15) / +7.7 (+5, +12) / +13.7 (+9, +19) | |
| GRU, learned code | 43.7 / 54.3 / 65.8% | +10.8 (+6, +19) / +12.9 (+9, +20) / +16.2 (+13, +22) | +4.7 (+3, +7) / +4.4 (+1, +7) / +2.0 (0, +5) |
| GRU, 18 hand-built numbers | 48.2 / 56.3 / 67.7% | +12.9 (+9, +19) / +14.8 (+12, +20) / +20.5 (+15, +25) | +4.0 (0, +8) / +5.0 (+1, +7) / +5.6 (+2, +9) |

(The reference differs slightly from §2 because fitting rows whose targets pass the cutoff are
now left out.) Readings, on one seed:
1. A plain GRU forecaster, with no description of the basin, already beats the tree reference
   by 8 to 14% on basins it never saw. This is the neural forecaster the Crossformer was not.
2. Given the learned code it improves a further 2 to 5% on held-out basins (4 to 6% on
   trained-on ones, every interval above zero). This is the first neural forecaster here that
   forecasts better on unseen basins because of the embedding.
3. The hand-built numbers do as well as the learned code here too, and the GRU with them ties
   the best tree arm with the simulation features.

## 5. Why the earlier tests stalled

- A static description competes with 30 days of the gauge's own flow for the same few points:
  even a private identifier per basin was worth only 6-12% on trained-on basins.
- Trees split on single axes; a learned code is an arbitrary rotation. Its own decoder is the
  reader built for it.
- The neural forecaster the embedding was meant for was trained with a recipe under which it
  could not use its weather inputs (`FORECAST_REVERIFICATION_2026-11-10.md` §15).

## 6. What was built

- `decoder_simulation.py`: `daily` caches whole daily records; `simulate` runs a run's decoder
  over every basin in water-year blocks (each also run 14 days past its end, so a horizon that
  crosses 1 October is read from one run) with the basin's own code, zeros, the training mean,
  another basin's, another state's or the nearest training basin's; the feature group
  `sim:<label>` of the tree forecaster reads the result. `--out-of-sample` simulates training
  basins with decoders that left their fold out and codes written from the other years.
- `pretrain_behaviour_code.py train`: `--blind` (the decoder never sees a code),
  `--no-gauge-record --static-banks signatures response` (a decoder conditioned on the 18
  numbers directly), `--leave-out-folds`.
- `decoder_forecaster.py`: the GRU forecaster of §4 (`train`, `compare`).
- Features per issue day and lead band: simulated change in log flow; the same change in the
  forecaster's flow units; that ratio applied to the observed issue-day flow; and the observed
  minus simulated log flow on the issue day and over the week before.

## 7. Reviews

An independent agent attacked the plan before any run, and Kimi K3.1 Pro (through OpenCode)
reviewed the code afterwards. Neither found a path from observed flow after the issue day into
a held-out feature. What they changed:

- The training-mean code was a strawman for "no basin knowledge" (a decoder never trained on
  it); replaced by a decoder trained with no code.
- Added the decoder conditioned on the 18 numbers, the other-state and nearest-basin codes,
  the depth control and the no-flow table of §3.
- Horizons crossing 1 October were read across a decoder restart; fixed with the 14-day
  overrun.
- Still open from the reviews: the simulations on the tree-fitting rows are in-sample for the
  decoder and for the code (which has read the year it simulates). This cannot leak into
  held-out scores but changes how much the trees trust each arm. The out-of-sample option
  exists for this; its results are added below when in.
- 375 tree-fitting rows have targets reaching into January 2022 (every arm alike).

## 8. Limits

- Five held-out regions, one per state: basin-bootstrap intervals overstate independence.
- The weather of the horizon is the realized one, not a forecast.
- The gauge-record arms need a gauge record; §3's statics rows are the only ungauged result.
- Weather exists in the scraped files only for hours the gauge reported (60-99% of hours).
  Winter weather is missing for seasonal gauges, so the decoder cannot track their snowpack.
  A real ungauged application needs continuous forcing.
- Everything is on fold 0, which has guided every design choice.

## 9. Reproduction

    python experiments/catchment_foundation/decoder_simulation.py daily --arms-dir <cohort>
    python -m experiments.catchment_foundation.pretrain_behaviour_code train --arms-dir <cohort> \
        --name blind_s0 --seed 0 --blind
    python -m experiments.catchment_foundation.pretrain_behaviour_code train --arms-dir <cohort> \
        --name hand_s0 --seed 0 --no-gauge-record --static-banks signatures response
    python experiments/catchment_foundation/decoder_simulation.py simulate --arms-dir <cohort> \
        --run behaviour_aux_s0 --codes own other_state donor
    python experiments/catchment_foundation/decoder_simulation.py simulate --arms-dir <cohort> \
        --run blind_s0 --codes none
    python experiments/catchment_foundation/tabular_forecast_baseline.py run --data <cohort>/tabular.pkl \
        --arms-dir <cohort> --fit-subsample 3 --seed 0 --reference flow+weather+snow \
        --arms flow+weather+snow flow+weather+snow+sim:behaviour_aux_s0 flow+weather+snow+sim:blind_s0@none

Scripts, reviews and aggregated tables of this run: `calibration/embedding_forecast_2026-11-27/`
(local only).
