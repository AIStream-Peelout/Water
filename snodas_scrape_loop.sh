#!/bin/bash
# Resilient SNODAS scrape driver: waits out network outages (laptop sleep/wake, wifi drops),
# reruns the resume-safe scraper until every snow-season day in the target ranges has a daily
# JSON (fetched or marked missing upstream), then compiles the per-basin series.
cd "$(dirname "$0")"
PY=/Users/isaac/Documents/GitHub/flow-forecast/.venv/bin/python
# Date ranges are overridable for backfills, e.g.
#   SCRAPE_RANGE_LIST="2003-09-30:2019-09-30" ./snodas_scrape_loop.sh
RANGE_LIST="${SCRAPE_RANGE_LIST:-2022-12-01:2026-07-26 2019-10-01:2022-11-30}"
RANGES=""
for pair in $RANGE_LIST; do RANGES="$RANGES --range $pair"; done

remaining() {
  RANGE_LIST="$RANGE_LIST" $PY - <<'EOF'
import os
import sys
sys.path.insert(0, ".")
from datetime import datetime
from snodas_series_scrape import day_json_path, season_dates
count = 0
for pair in os.environ["RANGE_LIST"].split():
    start, end = pair.split(":")
    for day in season_dates(datetime.strptime(start, "%Y-%m-%d"),
                            datetime.strptime(end, "%Y-%m-%d")):
        if not os.path.exists(day_json_path(day)):
            count += 1
print(count)
EOF
}

for pass_num in $(seq 1 40); do
  until curl -s -m 10 --head https://noaadata.apps.nsidc.org/NOAA/G02158/ >/dev/null; do
    echo "NETWORK DOWN - waiting 60s"
    sleep 60
  done
  echo "SCRAPE PASS $pass_num"
  $PY snodas_series_scrape.py scrape $RANGES --workers 10
  left=$(remaining)
  echo "REMAINING DAYS: $left"
  if [ "$left" -eq 0 ]; then
    break
  fi
  sleep 60
done
$PY snodas_series_scrape.py compile && echo "SNODAS PIPELINE COMPLETE (remaining=$(remaining))"
