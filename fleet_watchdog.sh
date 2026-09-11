#!/bin/zsh
# Unattended multi-state fleet scraper.
#
# Works through a QUEUE of states, keeping each one scraping until it is maxed out (no new
# completions across several passes = only dead/no-data gauges remain), then advances to the
# next state -- so it keeps accumulating NEW data for as long as it is left running, through
# the failure modes of an unattended laptop on flaky WiFi:
#   * network/DNS outage  -> detected (NWIS unreachable) => WAIT, do not burn the queue
#   * scrape crash / pass end -> restart (retries that state's failed gauges too)
#   * Claude-session death    -> detached, keeps running
# Snow states auto-launch their SNODAS companion at each pass end (state_scrape.py handles it).
# The separate backup_all.sh daemon mirrors everything to GCS hourly (auto-discovers new states).
#
# Does NOT survive reboot/battery-death. Relaunch after power-on:
#   cd Water && nohup ./fleet_watchdog.sh >/dev/null 2>&1 &
# Optionally pass a custom queue:  ./fleet_watchdog.sh OR CA NM ...
set -u
cd /Users/isaac/Documents/GitHub/Water
PY=/Users/isaac/Documents/GitHub/flow-forecast/.venv/bin/python
LOG=fleet_watchdog.log

if [ $# -gt 0 ]; then QUEUE=($@); else QUEUE=(PA OR NM NY GA CA MN TN AZ WA TX); fi

count() { $PY -c "import json,os;p='pilot_data/scrapes/$1/registry.json';r=json.load(open(p)) if os.path.exists(p) else {};print(sum(1 for v in r.values() if isinstance(v,dict) and v.get('status')=='$2'))" 2>/dev/null || echo 0; }
net_up() { [ "$(curl -s -m 15 -o /dev/null -w '%{http_code}' 'https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=CO&siteStatus=active&hasDataTypeCd=iv&parameterCd=00060' 2>/dev/null)" = "200" ]; }
log() { echo "$(date '+%F %T') $1" >> $LOG; }

log "fleet_watchdog start; queue: ${QUEUE[*]}"
for STATE in $QUEUE; do
  log "=== STATE $STATE begin ==="
  last_done=-1; dry=0; restarts=0
  while true; do
    if ! pgrep -f "caffeinate -dimsu" >/dev/null 2>&1; then nohup caffeinate -dimsu >/dev/null 2>&1 & fi
    if ! pgrep -f "state_scrape.py --state $STATE" >/dev/null 2>&1; then
      done=$(count $STATE completed); failed=$(count $STATE failed)
      # Distinguish "state maxed out" from "network is down" before deciding anything.
      if [ "$done" -le "$last_done" ] && ! net_up; then
        log "$STATE: no progress but NWIS unreachable -- network outage, waiting (done=$done)"
        sleep 300; continue
      fi
      if [ "$done" -le "$last_done" ]; then dry=$((dry+1)); else dry=0; fi
      last_done=$done
      if [ "$dry" -ge 3 ]; then log "$STATE maxed: $done completed, $failed failed ($restarts passes) -> advancing"; break; fi
      if [ "$restarts" -ge 60 ]; then log "$STATE restart cap: $done completed -> advancing"; break; fi
      [ "$dry" -ge 1 ] && sleep 300
      PYTHONUNBUFFERED=1 nohup $PY state_scrape.py --state $STATE --no-backup >> $(echo $STATE | tr 'A-Z' 'a-z')_scrape.log 2>&1 &
      restarts=$((restarts+1)); log "$STATE pass #$restarts started ($done completed, $failed failed)"
    fi
    sleep 180
  done
done
log "fleet_watchdog: all queued states processed"
