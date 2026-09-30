#!/usr/bin/env bash
# v9 A/B queue (CO/UT, batch 128, linear fusion head, 300 epochs, 3 seeds each), portable
# across machines (MacBook, Mac Studio, a CUDA box). Runs ONE training at a time, skips runs
# that already have a bank, and stops itself if its own memory footprint passes a budget.
#
#   v9w{256,384,512}  embedding-width sweep, no regional imagery           (~12 min/run on MPS)
#   v9r               regional 25.6 km patch as the vision input, dim 512  (~2 h/run on MPS)
#   v9t               reach patch + regional fourth tower, dim 512         (~2 h/run on MPS)
#
# Usage (from the Water repo root, on the machine that should do the work):
#   FF_REPO=/path/to/flow-forecast-worktree PY=/path/to/python \
#     nohup experiments/catchment_foundation/run_v9_queue.sh > v9_queue.log 2>&1 &
#
# Environment:
#   FF_REPO        flow-forecast checkout on branch linear-fusion (required)
#   PY             python with torch + flood_forecast deps (default: python3)
#   DEVICE         torch device: mps | cuda | cpu (default: the train script's own default)
#   ROOT           panel-record root (default: pilot_data/embedding_dataset_hourly_pre2022)
#   RUN_GROUPS     which groups to run, space separated (default: "w256 w384 w512 r t")
#   SEEDS          seeds (default: "42 43 44")
#   NUM_WORKERS    DataLoader workers for the no-regional runs (default 4); regional runs
#                  always use 2 — every prefetched regional batch is ~0.8 GB of shared memory
#   MEM_BUDGET_GB  stop the queue if the running training's footprint exceeds this (default 20)
#   MPS_WATERMARK  PYTORCH_MPS_HIGH_WATERMARK_RATIO for MPS runs (default 0.45; the PyTorch
#                  default lets the Metal cache grow far beyond what the model needs)
set -uo pipefail

: "${FF_REPO:?set FF_REPO to the flow-forecast checkout (branch linear-fusion)}"
export FF_REPO
PY=${PY:-python3}
ROOT=${ROOT:-pilot_data/embedding_dataset_hourly_pre2022}
GROUPS_TO_RUN=${RUN_GROUPS:-"w256 w384 w512 r t"}
SEEDS=${SEEDS:-"42 43 44"}
NUM_WORKERS=${NUM_WORKERS:-4}
MEM_BUDGET_GB=${MEM_BUDGET_GB:-20}
export PYTORCH_MPS_HIGH_WATERMARK_RATIO=${MPS_WATERMARK:-0.45}

stamp() { echo "[$(date '+%F %T')] $*"; }

footprint_gb() {  # total footprint in GB of a pid and its children (macOS); 0 elsewhere
  command -v footprint > /dev/null || { echo 0; return; }
  local pids="$1 $(pgrep -P "$1" | tr '\n' ' ')" total=0 value
  for pid in $pids; do
    value=$(footprint "$pid" 2> /dev/null | awk '/Footprint:/ {v=$5; u=$6; if (u=="GB") print v*1024; else if (u=="MB") print v; else if (u=="KB") print v/1024; exit}')
    total=$(awk -v a="$total" -v b="${value:-0}" 'BEGIN {print a+b}')
  done
  awk -v t="$total" 'BEGIN {printf "%.1f", t/1024}'
}

train() {  # name seed workers extra-args...
  local name=$1 seed=$2 workers=$3; shift 3
  if [ -f "$ROOT/$name/embeddings_concat.pt" ]; then stamp "$name already trained, skipping"; return 0; fi
  stamp "start $name"
  $PY -u train_catchment_embeddings.py --states CO UT --data-root "$ROOT" \
      --scrape-root pilot_data/scrapes --output-dir "$ROOT/$name" --fusions concat \
      --epochs 300 --seed "$seed" --batch-size 128 --num-workers "$workers" \
      --history-mode hourly_panel --cross-year --blocked-batches --no-wandb \
      ${DEVICE:+--device "$DEVICE"} "$@" > "$ROOT/train_${name#COUT_}.log" 2>&1 &
  local pid=$! used
  while kill -0 "$pid" 2> /dev/null; do
    sleep 60
    used=$(footprint_gb "$pid")
    if awk -v u="$used" -v b="$MEM_BUDGET_GB" 'BEGIN {exit !(u > b)}'; then
      stamp "MEMORY BUDGET EXCEEDED: $name at ${used} GB > ${MEM_BUDGET_GB} GB — stopping queue"
      pkill -P "$pid" 2> /dev/null; kill "$pid" 2> /dev/null
      exit 3
    fi
  done
  wait "$pid"; local status=$?
  stamp "end $name (exit $status, last: $(tail -1 "$ROOT/train_${name#COUT_}.log"))"
  return 0
}

stamp "v9 queue on $(hostname): groups [$GROUPS_TO_RUN], seeds [$SEEDS], budget ${MEM_BUDGET_GB} GB"
for group in $GROUPS_TO_RUN; do
  for seed in $SEEDS; do
    case $group in
      w256|w384|w512) train "COUT_v9${group}_s$seed" "$seed" "$NUM_WORKERS" --embedding-dim "${group#w}" --no-regional ;;
      r) train "COUT_v9r_s$seed" "$seed" 2 --embedding-dim 512 --vision-source regional ;;
      t) train "COUT_v9t_s$seed" "$seed" 2 --embedding-dim 512 ;;
      *) stamp "unknown group $group" ;;
    esac
  done
done
stamp "v9 queue complete"
