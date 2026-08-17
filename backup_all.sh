#!/bin/zsh
# Comprehensive periodic backup of ALL scraped/derived data to GCS.
#
# The scrape watchdog only mirrors the ACTIVE state; this daemon mirrors the whole pilot_data
# tree (every state's scrape, every SNODAS series, all embedding records/banks) on a fixed
# interval so nothing falls through the cracks if the laptop is lost. Uploads are incremental
# (backup_functions.py skips blobs whose byte size already matches), so every pass after the
# first is cheap -- only new/changed files move.
#
# Survives Claude-session death (detached). Does NOT survive reboot/battery-death -- relaunch
# after power-on:  cd Water && nohup ./backup_all.sh >/dev/null 2>&1 &
set -u
cd /Users/isaac/Documents/GitHub/Water
PY=/Users/isaac/Documents/GitHub/flow-forecast/.venv/bin/python
LOG=backup_all.log
INTERVAL=${1:-3600}   # seconds between full passes (default 1h)

sync() { [ -d "$1" ] && $PY backup_functions.py --dir "$1" --prefix "$1" >> $LOG 2>&1; }
log()  { echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> $LOG; }

log "backup_all daemon start (interval=${INTERVAL}s)"
while true; do
  log "=== pass start ==="
  # states discovered dynamically so new scrapes are picked up automatically
  for d in pilot_data/scrapes/*(/N) pilot_data/snodas_series/*(/N); do sync "$d"; done
  sync pilot_data/embedding_dataset
  sync pilot_data/embedding_dataset_hourly
  log "=== pass done ==="
  sleep "$INTERVAL"
done
