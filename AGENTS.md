# Water repository scope

Water is the hydrology application and research repository. It owns water-data collection,
hydrology preprocessing, catchment datasets, experiment configuration, scientific evaluation, and
reproducible analyses. It is not a general machine-learning framework.

## Relationship to Flow Forecast

Hydrology experiments in this repository use Flow Forecast as a dependency. Flow Forecast owns
reusable PyTorch models, neural-ODE/physics primitives, loaders, training loops, dated inference,
evaluation, Plotly/W&B logging, sweep support, interpretability, and deployment utilities.

Before implementing any of those capabilities here, inspect Flow Forecast's existing APIs,
especially `flood_forecast.trainer`, `time_model`, `pytorch_training`, `evaluator`,
`plot_functions`, and `deployment.inference`.

- Call Flow Forecast APIs from thin Water adapters.
- If a reusable capability is missing or broken, fix it in a focused Flow Forecast PR with tests
  and documentation, then update Water to consume it.
- Do not copy framework functions into Water or build a second training, plotting, dated-inference,
  checkpointing, or sweep system.
- Water-specific cohort selection, manifests, hydrology metrics, data joins, and experiment
  comparisons may remain here.

## Experiments and artifacts

Experiment source, configs, and experiment-specific tests belong under `experiments/`. Generated
manifests, run directories, logs, checkpoints, calibrated weights, W&B state, and galleries stay
local or in an artifact service and must not be committed.

An experiment that reveals a framework bug should receive two changes: a general fix and regression
test in Flow Forecast, followed by the smallest necessary adapter/config update in Water.
