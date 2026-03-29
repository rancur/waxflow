#!/bin/bash
# Monitor Spotify-Lexicon parity and dispatch fixes
# Run as LaunchAgent every 30 minutes
# Logs to ~/.openclaw/logs/sls-monitor.log

set -euo pipefail

API_URL="http://192.168.1.221:8402"
LEXICON_URL="http://192.168.1.116:48624"
LOG_FILE="${HOME}/.openclaw/logs/sls-monitor.log"
STATE_FILE="${HOME}/.openclaw/logs/sls-monitor-state.json"

mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"; echo "$*"; }

# Fetch dashboard
DASHBOARD=$(curl -s --connect-timeout 10 "$API_URL/api/dashboard" 2>/dev/null)
if [ -z "$DASHBOARD" ]; then
    log "ERROR: API not responding"
    exit 1
fi

TOTAL=$(echo "$DASHBOARD" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['spotify_total'])")
SYNCED=$(echo "$DASHBOARD" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['lexicon_synced'])")
PCT=$(echo "$DASHBOARD" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['parity_pct'])")
ERRORS=$(echo "$DASHBOARD" | python3 -c "import json,sys; print(json.loads(sys.stdin.read()).get('by_pipeline_stage',{}).get('error',0))")
DOWNLOADING=$(echo "$DASHBOARD" | python3 -c "import json,sys; print(json.loads(sys.stdin.read()).get('by_pipeline_stage',{}).get('downloading',0))")
NEW=$(echo "$DASHBOARD" | python3 -c "import json,sys; print(json.loads(sys.stdin.read()).get('by_pipeline_stage',{}).get('new',0))")

log "PARITY: $SYNCED/$TOTAL ($PCT%) | errors=$ERRORS downloading=$DOWNLOADING new=$NEW"

# Check Nov 2025+ specifically
NOV_STATUS=$(curl -s "$API_URL/api/tracks?per_page=200" 2>/dev/null | python3 -c "
import json,sys
from collections import Counter
d=json.loads(sys.stdin.read())
stages = Counter()
for t in d['tracks']:
    if t.get('spotify_added_at','') >= '2025-11-01' and t.get('spotify_added_at','') < '2026-05-01':
        stages[t['pipeline_stage']] += 1
total = sum(stages.values())
complete = stages.get('complete',0)
print(f'{complete}/{total}')
" 2>/dev/null)
log "NOV25-APR26: $NOV_STATUS"

# Check Lexicon playlists have tracks
EMPTY_PLAYLISTS=$(curl -s "$API_URL/api/playlists" 2>/dev/null | python3 -c "
import json,sys
d=json.loads(sys.stdin.read())
empty = [p for p in d.get('playlists',[]) if p['track_count'] == 0 and p.get('lexicon_playlist_id')]
print(len(empty))
" 2>/dev/null)
log "EMPTY_PLAYLISTS: $EMPTY_PLAYLISTS"

# Check services health
LEXICON_OK=$(curl -s --connect-timeout 5 "$LEXICON_URL/v1/playlists" 2>/dev/null | python3 -c "import json,sys; print('ok' if 'data' in json.loads(sys.stdin.read()) else 'error')" 2>/dev/null || echo "error")
TIDARR_OK=$(curl -s --connect-timeout 5 "http://192.168.1.221:8484/api/queue/status" 2>/dev/null | python3 -c "import json,sys; print('ok')" 2>/dev/null || echo "error")
log "SERVICES: lexicon=$LEXICON_OK tidarr=$TIDARR_OK"

# Check for stuck downloads (downloading > 30 min with no progress)
WORKER_RUNNING=$(ssh -o ConnectTimeout=3 nas "/usr/local/bin/docker ps --filter name=sync-worker --format '{{.Status}}'" 2>/dev/null || echo "unknown")
log "WORKER: $WORKER_RUNNING"

# Save state for trend tracking
python3 -c "
import json, time
state = {
    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
    'total': $TOTAL,
    'synced': $SYNCED,
    'pct': $PCT,
    'errors': $ERRORS,
    'downloading': $DOWNLOADING,
    'new': $NEW,
    'services': {'lexicon': '$LEXICON_OK', 'tidarr': '$TIDARR_OK'},
}
with open('$STATE_FILE', 'w') as f:
    json.dump(state, f, indent=2)
"

# Issue detection
ISSUES=""

if [ "$LEXICON_OK" != "ok" ]; then
    ISSUES="${ISSUES}Lexicon API down. "
fi

if [ "$TIDARR_OK" != "ok" ]; then
    ISSUES="${ISSUES}Tidarr API down. "
fi

if [ "$ERRORS" -gt 500 ]; then
    ISSUES="${ISSUES}High error count ($ERRORS). "
fi

if echo "$WORKER_RUNNING" | grep -qi "exited\|dead"; then
    ISSUES="${ISSUES}Worker container not running. "
fi

if [ -n "$ISSUES" ]; then
    log "ISSUES DETECTED: $ISSUES"
    # Could dispatch repair agent here via openclaw
else
    log "STATUS: All clear"
fi
