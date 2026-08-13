#!/bin/zsh
# Self-healing scrape watchdog.
#
# Keeps a state fleet scrape making progress through the failure modes that plague an
# unattended laptop on hotel WiFi: network/DNS outages, scrape crashes, and Claude-session
# death. Every loop it (1) keeps caffeinate alive, (2) restarts the state scrape if it is not
# running (a fresh run retries previously-failed gauges, so an outage batch is swept once the
# network returns), and (3) mirrors the state to GCS every 30 minutes so backups happen without
# a human. It backs off when a whole pass makes no progress (a sustained outage) so it does not
# hammer, and exits once the state is essentially complete (final backup first).
#
# It does NOT survive a full reboot / battery death (the watchdog process itself dies) -- relaunch
# it after power-on. It DOES survive session death and network outages, which are the common case.
#
# Usage: nohup ./scrape_watchdog.sh PA 300 >/dev/null 2>&1 &
set -u
STATE=${1:?usage: scrape_watchdog.sh STATE [target_completed]}
TARGET=${2:-300}
cd /Users/isaac/Documents/GitHub/Water
PY=/Users/isaac/Documents/GitHub/flow-forecast/.venv/bin/python
LOG=watchdog_${STATE}.log
REG=pilot_data/scrapes/$STATE/registry.json
SCRAPE_LOG=$(echo $STATE | tr 'A-Z' 'a-z')_scrape.log

count() { $PY -c "import json,os;p='$REG';r=json.load(open(p)) if os.path.exists(p) else {};print(sum(1 for v in r.values() if isinstance(v,dict) and v.get('status')=='$1'))" 2>/dev/null || echo 0; }
backup() { $PY backup_functions.py --dir pilot_data/scrapes/$STATE --prefix pilot_data/scrapes/$STATE >> $LOG 2>&1; }
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> $LOG; }

log "watchdog start (state=$STATE target=$TARGET)"
last_backup=0; last_done=-1; dry=0; restarts=0
while true; do
  if ! pgrep -f "caffeinate -dimsu" >/dev/null 2>&1; then nohup caffeinate -dimsu >/dev/null 2>&1 & fi
  done=$(count completed); failed=$(count failed); now=$(date +%s)

  if [ $((now - last_backup)) -ge 1800 ]; then backup; log "periodic backup ($done completed, $failed failed)"; last_backup=$now; fi

  if [ "$done" -ge "$TARGET" ]; then backup; log "STATE COMPLETE: $done completed, $failed failed -- final backup done, watchdog exiting"; break; fi

  if ! pgrep -f "state_scrape.py --state $STATE" >/dev/null 2>&1; then
    if [ "$restarts" -ge 80 ]; then log "giving up after 80 restarts ($done completed, $failed failed)"; break; fi
    if [ "$done" -le "$last_done" ]; then dry=$((dry+1)); else dry=0; fi
    last_done=$done
    if [ "$dry" -ge 2 ]; then log "no progress (dry=$dry) -- backoff 600s (likely network outage)"; sleep 600; fi
    PYTHONUNBUFFERED=1 nohup $PY state_scrape.py --state $STATE --no-backup >> $SCRAPE_LOG 2>&1 &
    restarts=$((restarts+1)); log "restarted scrape #$restarts ($done completed, $failed failed)"
  fi
  sleep 120
done
