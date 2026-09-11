#!/usr/bin/env bash
# v8 pipeline, unattended: wait for the regional-context collection shards to finish, merge
# the sidecars into the pre-2022 panel records, train v8 (regional tower vs no-regional
# control, linear fusion head, batch 128, 3 seeds each, two parallel chains), then run the
# standing milestone evaluation against v7.
#
# Usage: nohup experiments/catchment_foundation/run_v8_when_collected.sh > <log> 2>&1 &
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
WATER=$(cd "$HERE/../.." && pwd)
export FF_REPO=${FF_REPO:-/Users/isaac/Documents/GitHub/ff-foundation}
PY=${PY:-/Users/isaac/Documents/GitHub/flow-forecast/.venv/bin/python}
ROOT=$WATER/pilot_data/embedding_dataset_hourly_pre2022
STATES="CO UT WY FL PA"
cd "$WATER"

stamp() { echo "[$(date '+%F %T')] $*"; }

stamp "waiting for regional collection shards to exit"
while pgrep -f "embedding_dataset.py --state .* --regional" > /dev/null; do sleep 300; done
stamp "collection done:"
for s in $STATES; do
  echo "  $s sidecars: $(ls pilot_data/embedding_dataset/$s/*_regional.npz 2>/dev/null | wc -l | tr -d ' ')"
done

stamp "merging sidecars into panel records"
$PY -u build_panel_records.py --states $STATES --merge-regional --output-root "$ROOT" || exit 1

train() {  # name seed extra-args...
  local name=$1 seed=$2; shift 2
  $PY -u train_catchment_embeddings.py --states $STATES --data-root "$ROOT" \
      --scrape-root pilot_data/scrapes --output-dir "$ROOT/$name" --fusions concat \
      --epochs 300 --seed "$seed" --batch-size 128 --history-mode hourly_panel --cross-year \
      --blocked-batches --no-wandb "$@" > "$ROOT/train_$name.log" 2>&1
}

stamp "training v8 (regional) and v8 control (no regional), 3 seeds each, batch 128"
( for seed in 42 43 44; do train "FLEET_v8_s$seed" "$seed"; done ) &
( for seed in 42 43 44; do train "FLEET_v8ctl_s$seed" "$seed" --no-regional; done ) &
wait
for name in FLEET_v8_s42 FLEET_v8_s43 FLEET_v8_s44 FLEET_v8ctl_s42 FLEET_v8ctl_s43 FLEET_v8ctl_s44; do
  echo "  $name: $(tail -1 "$ROOT/train_$name.log")"
done

stamp "evaluating v8 vs v7 and control vs v7"
"$HERE/eval_embedding_milestone.sh" FLEET_v8 FLEET_v7 2>&1 | grep -v Warning
"$HERE/eval_embedding_milestone.sh" FLEET_v8ctl FLEET_v7 2>&1 | grep -v Warning
stamp "v8 pipeline complete"
