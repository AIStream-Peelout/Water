#!/bin/bash
# Waits for the fleet_v1 pipeline process to exit, then launches the retuned fleet_v2 run.
EXPERIMENT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WATER_ROOT="$(cd "$EXPERIMENT_ROOT/../.." && pwd)"
PY="${FLOW_FORECAST_PYTHON:-python}"
while pgrep -f "run_training.py --name fleet_v1_swe" >/dev/null; do sleep 120; done
cd "$WATER_ROOT" || exit 1
"$PY" experiments/catchment_foundation/run_training.py --name fleet_v2_swe --swe \
    --epochs 30 --samples-per-epoch 16384 --lr 1e-3
echo "=== FLEET V2 DONE code=$? ==="
