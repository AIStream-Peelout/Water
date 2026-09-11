#!/usr/bin/env bash
# Standing evaluation of one embedding milestone: signature probes fleet-wide and on the
# fixed 206-site CO/UT subset for every bank of the milestone, plus modality attribution
# (seasonal-only view) for the seed-42 banks, then one comparison table against a baseline.
#
# Usage: eval_embedding_milestone.sh <milestone prefix> [baseline prefix]
#   e.g. eval_embedding_milestone.sh FLEET_v6 FLEET_v5
# Banks are <root>/<prefix>_{s42,s43,s44,b128_s42}/embeddings_concat.pt.
set -euo pipefail

MILESTONE=${1:?milestone prefix, e.g. FLEET_v6}
BASELINE=${2:-}
HERE=$(cd "$(dirname "$0")" && pwd)
WATER=$(cd "$HERE/../.." && pwd)
export FF_REPO=${FF_REPO:-/Users/isaac/Documents/GitHub/ff-foundation}
PY=${PY:-/Users/isaac/Documents/GitHub/flow-forecast/.venv/bin/python}
ROOT=${ROOT:-$WATER/pilot_data/embedding_dataset_hourly_pre2022}
CAL=$HERE/calibration
PROBE=$HERE/embedding_probes.py
MODALITY=$HERE/embedding_modality_analysis.py
VARIANTS=(s42 s43 s44 b128_s42)

cd "$WATER"
for v in "${VARIANTS[@]}"; do
  bank=$ROOT/${MILESTONE}_$v/embeddings_concat.pt
  [ -f "$bank" ] || { echo "missing $bank"; continue; }
  $PY "$PROBE" --embedding-bank "$bank" --states CO UT WY FL PA \
      --output "$CAL/signature_probe_${MILESTONE}_$v.json" > /dev/null 2>&1 &
  $PY "$PROBE" --embedding-bank "$bank" --states CO UT \
      --output "$CAL/signature_probe_${MILESTONE}_${v}_coutsubset.json" > /dev/null 2>&1 &
done
for v in s42 b128_s42; do
  dir=$ROOT/${MILESTONE}_$v
  [ -d "$dir" ] || continue
  $PY "$MODALITY" --version-dir "$dir" --states CO UT WY FL PA --seasonal-only \
      --output "$CAL/modality_${MILESTONE}_${v}_seasonal.json" \
      > "$CAL/modality_${MILESTONE}_${v}_seasonal.log" 2>&1 &
done
wait

$PY - "$MILESTONE" "$BASELINE" "$CAL" <<'EOF'
import json, os, sys
import numpy as np

milestone, baseline, cal = sys.argv[1], sys.argv[2], sys.argv[3]
sigs = ["log_mean_flow", "rb_flashiness", "melt_fraction", "bfi", "cv", "diurnal_strength"]
variants = ["s42", "s43", "s44", "b128_s42"]


def load(prefix, variant, suffix=""):
    path = os.path.join(cal, "signature_probe_%s_%s%s.json" % (prefix, variant, suffix))
    return json.load(open(path)) if os.path.exists(path) else None


def block(title, suffix):
    print("\n%s  (probe set: %s)" % (title, "206 CO/UT sites" if suffix else "all 5 states"))
    print("%-30s %5s" % ("bank", "n") + "".join("%9s" % s[:8] for s in sigs))
    for prefix in [p for p in (baseline, milestone) if p]:
        seeds = [r for r in (load(prefix, v, suffix) for v in variants[:3]) if r]
        for v, r in zip(variants[:3], seeds):
            print("%-30s %5d" % ("%s %s" % (prefix, v), r["n_sites"])
                  + "".join("%9.3f" % r["probe_r2"][s] for s in sigs))
        if seeds:
            print("%-36s" % ("  %s 3-seed mean" % prefix)
                  + "".join("%9.3f" % np.mean([r["probe_r2"][s] for r in seeds]) for s in sigs))
        r = load(prefix, "b128_s42", suffix)
        if r:
            print("%-30s %5d" % ("%s b128_s42" % prefix, r["n_sites"])
                  + "".join("%9.3f" % r["probe_r2"][s] for s in sigs))


block("Signature probe R2", "")
block("Signature probe R2", "_coutsubset")
for v in ("s42", "b128_s42"):
    path = os.path.join(cal, "modality_%s_%s_seasonal.json" % (milestone, v))
    if not os.path.exists(path):
        continue
    r = json.load(open(path))
    print("\nModality attribution %s %s: variance share %s" % (milestone, v, r["variance_share_pooled"]))
    print("  knockout cosine:", {m: r["knockout"][m]["mean_fill_cosine"] for m in ("vision", "tabular", "history")})
    print("  %-20s" % "bank" + "".join("%9s" % s[:8] for s in sigs))
    for b in ("fused_bank", "pooled_concat_l2", "pooled_history", "pooled_tabular", "pooled_vision"):
        print("  %-20s" % b + "".join("%9.3f" % r["probe_r2"][b][s] for s in sigs))
EOF
