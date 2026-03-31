#!/bin/bash
# Self-healing monitor for Spotify-Lexicon Sync
# Runs every 30 minutes via LaunchAgent
# Detects issues and dispatches fixes automatically

set -euo pipefail

API_URL="http://192.168.1.221:8402"
LEXICON_URL="http://192.168.1.116:48624"
TIDARR_URL="http://192.168.1.221:8484"
LOG_FILE="${HOME}/.openclaw/logs/sls-monitor.log"
STATE_FILE="${HOME}/.openclaw/logs/sls-monitor-state.json"
COOLDOWN_FILE="${HOME}/.openclaw/logs/sls-monitor-cooldown.json"
DEPLOY_SCRIPT="${HOME}/spotify-lexicon-sync/scripts/deploy-to-nas.sh"

mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"; }

# Cooldown: don't repeat the same fix within 2 hours
check_cooldown() {
    local key="$1"
    if [ -f "$COOLDOWN_FILE" ]; then
        local last=$(python3 -c "
import json, time
try:
    d = json.load(open('$COOLDOWN_FILE'))
    t = d.get('$key', 0)
    print('ok' if time.time() - t > 7200 else 'cooldown')
except: print('ok')
" 2>/dev/null)
        [ "$last" = "ok" ] && return 0 || return 1
    fi
    return 0
}

set_cooldown() {
    local key="$1"
    python3 -c "
import json, time
try:
    d = json.load(open('$COOLDOWN_FILE'))
except: d = {}
d['$key'] = time.time()
json.dump(d, open('$COOLDOWN_FILE', 'w'))
" 2>/dev/null
}

# ===== GATHER STATE =====
DASHBOARD=$(curl -s --connect-timeout 10 "$API_URL/api/dashboard" 2>/dev/null || echo "")
if [ -z "$DASHBOARD" ] || [ "$DASHBOARD" = "" ]; then
    log "ERROR: API not responding"
    # Try to restart containers
    if check_cooldown "restart_containers"; then
        log "ACTION: Restarting sync containers"
        ssh -o ConnectTimeout=5 nas "cd /volume1/homes/willcurran/spotify-lexicon-sync && /usr/local/bin/docker compose restart" >> "$LOG_FILE" 2>&1 || true
        set_cooldown "restart_containers"
    fi
    exit 1
fi

eval $(echo "$DASHBOARD" | python3 -c "
import json,sys
d=json.loads(sys.stdin.read())
print(f'TOTAL={d[\"spotify_total\"]}')
print(f'SYNCED={d[\"lexicon_synced\"]}')
print(f'PCT={d[\"parity_pct\"]}')
print(f'ERRORS={d.get(\"by_pipeline_stage\",{}).get(\"error\",0)}')
print(f'DOWNLOADING={d.get(\"by_pipeline_stage\",{}).get(\"downloading\",0)}')
print(f'NEW={d.get(\"by_pipeline_stage\",{}).get(\"new\",0)}')
print(f'ORGANIZING={d.get(\"by_pipeline_stage\",{}).get(\"organizing\",0)}')
print(f'COMPLETE={d.get(\"by_pipeline_stage\",{}).get(\"complete\",0)}')
")

log "CHECK: parity=$SYNCED/$TOTAL ($PCT%) errors=$ERRORS downloading=$DOWNLOADING new=$NEW organizing=$ORGANIZING"

# ===== CHECK SERVICES =====
LEXICON_OK=$(curl -s --connect-timeout 5 "$LEXICON_URL/v1/playlists" 2>/dev/null | python3 -c "import json,sys; print('ok' if 'data' in json.loads(sys.stdin.read()) else 'error')" 2>/dev/null || echo "error")
TIDARR_OK=$(curl -s --connect-timeout 5 "$TIDARR_URL/api/queue/status" 2>/dev/null | python3 -c "import json,sys; json.loads(sys.stdin.read()); print('ok')" 2>/dev/null || echo "error")
WORKER_STATUS=$(ssh -o ConnectTimeout=3 nas "/usr/local/bin/docker ps --filter name=sync-worker --format '{{.Status}}'" 2>/dev/null || echo "unknown")

# ===== CHECK: Worker health endpoint =====
WORKER_HEALTH=$(curl -s --connect-timeout 5 http://192.168.1.221:8403/health 2>/dev/null)
WORKER_HEALTH_STATUS=$(echo "$WORKER_HEALTH" | python3 -c "import json,sys; print(json.loads(sys.stdin.read()).get('status','unknown'))" 2>/dev/null || echo "unreachable")

log "SERVICES: lexicon=$LEXICON_OK tidarr=$TIDARR_OK worker=$WORKER_STATUS worker_health=$WORKER_HEALTH_STATUS"

# ===== FIX: Worker stalled or unreachable =====
if [ "$WORKER_HEALTH_STATUS" = "stalled" ] || [ "$WORKER_HEALTH_STATUS" = "unreachable" ]; then
    if check_cooldown "restart_worker_health"; then
        log "ACTION: Worker stalled/unreachable (health=$WORKER_HEALTH_STATUS), restarting"
        ssh -o ConnectTimeout=5 nas "cd /volume1/homes/willcurran/spotify-lexicon-sync && /usr/local/bin/docker compose restart sync-worker" >> "$LOG_FILE" 2>&1 || true
        set_cooldown "restart_worker_health"
    fi
fi

# ===== FIX: Worker container down =====
if echo "$WORKER_STATUS" | grep -qiE "exited|dead|created" || [ "$WORKER_STATUS" = "unknown" ]; then
    if check_cooldown "restart_worker"; then
        log "ACTION: Worker down, restarting"
        ssh -o ConnectTimeout=5 nas "cd /volume1/homes/willcurran/spotify-lexicon-sync && /usr/local/bin/docker compose restart sync-worker" >> "$LOG_FILE" 2>&1 || true
        set_cooldown "restart_worker"
    fi
fi

# ===== FIX: Tidarr auth expired =====
TIDARR_AUTH=$(ssh -o ConnectTimeout=3 nas "/usr/local/bin/docker exec tidarr cat /shared/.tiddl/auth.json 2>/dev/null" 2>/dev/null | python3 -c "
import json,sys,time
try:
    d=json.loads(sys.stdin.read())
    exp=d.get('expires_at',0)
    if exp < time.time(): print('expired')
    elif exp < time.time() + 3600: print('expiring_soon')
    else: print('ok')
except: print('unknown')
" 2>/dev/null || echo "unknown")

if [ "$TIDARR_AUTH" = "expired" ] || [ "$TIDARR_AUTH" = "expiring_soon" ]; then
    log "WARNING: Tidarr auth $TIDARR_AUTH"
fi

# ===== FIX: Download-failed tracks stuck in error =====
DL_FAILED=$(curl -s "$API_URL/api/tracks?per_page=1&pipeline_stage=error" 2>/dev/null | python3 -c "
import json,sys
d=json.loads(sys.stdin.read())
# Count download failures that could be retried
count = 0
for t in d.get('tracks',[]):
    if 'Download failed' in (t.get('pipeline_error') or ''):
        count += 1
print(count)
" 2>/dev/null || echo "0")

if [ "$DL_FAILED" -gt 10 ] && check_cooldown "reset_dl_failed"; then
    log "ACTION: Resetting $DL_FAILED download-failed tracks"
    ssh -o ConnectTimeout=5 nas "/usr/local/bin/docker exec sync-api python3 -c \"
import sqlite3
conn = sqlite3.connect('/app/data/sync.db')
conn.execute('PRAGMA journal_mode=WAL')
r = conn.execute('''UPDATE tracks SET pipeline_stage='new', pipeline_error=NULL,
    match_status='pending', download_status='pending', download_error=NULL,
    download_attempts=0, verify_status='pending', lexicon_status='pending',
    updated_at=datetime('now')
    WHERE pipeline_stage='error' AND pipeline_error LIKE '%Download failed%' ''')
conn.commit()
print(f'Reset {r.rowcount} tracks')
conn.close()
\"" >> "$LOG_FILE" 2>&1 || true
    set_cooldown "reset_dl_failed"
fi

# ===== FIX: Tracks complete but no Lexicon ID =====
UNLINKED=$(curl -s "$API_URL/api/tracks?per_page=1&pipeline_stage=complete" 2>/dev/null | python3 -c "
import json,sys
# Can't check lexicon_track_id from list endpoint easily, use DB
print(0)
" 2>/dev/null || echo "0")

ssh -o ConnectTimeout=5 nas "/usr/local/bin/docker exec sync-api python3 -c \"
import sqlite3
conn = sqlite3.connect('/app/data/sync.db')
count = conn.execute('''SELECT COUNT(*) FROM tracks
    WHERE pipeline_stage='complete' AND lexicon_track_id IS NULL AND file_path IS NOT NULL''').fetchone()[0]
if count > 5:
    r = conn.execute('''UPDATE tracks SET pipeline_stage='organizing', lexicon_status='pending',
        lexicon_track_id=NULL, updated_at=datetime('now')
        WHERE pipeline_stage='complete' AND lexicon_track_id IS NULL AND file_path IS NOT NULL''')
    conn.commit()
    print(f'FIXED: Reset {r.rowcount} unlinked tracks to organizing')
else:
    print(f'OK: {count} unlinked tracks (within tolerance)')
conn.close()
\"" >> "$LOG_FILE" 2>&1 || true

# ===== FIX: Tidarr /music symlink lost after restart =====
SYMLINK_OK=$(ssh -o ConnectTimeout=3 nas "/usr/local/bin/docker exec tidarr ls /music/tracks 2>/dev/null && echo ok || echo missing" 2>/dev/null || echo "unknown")
if [ "$SYMLINK_OK" = "missing" ] && check_cooldown "fix_tidarr_mount"; then
    log "ACTION: Tidarr /music mount check"
    set_cooldown "fix_tidarr_mount"
fi

# ===== FIX: Stalled pipeline (no progress in 2 checks) =====
if [ -f "$STATE_FILE" ]; then
    PREV_COMPLETE=$(python3 -c "import json; print(json.load(open('$STATE_FILE')).get('synced',0))" 2>/dev/null || echo 0)
    if [ "$COMPLETE" -le "$PREV_COMPLETE" ] && [ "$ORGANIZING" -eq 0 ] && [ "$NEW" -gt 0 ]; then
        log "WARNING: Pipeline may be stalled (no progress since last check)"
        if check_cooldown "restart_stalled"; then
            log "ACTION: Restarting worker to unstick pipeline"
            ssh -o ConnectTimeout=5 nas "cd /volume1/homes/willcurran/spotify-lexicon-sync && /usr/local/bin/docker compose restart sync-worker" >> "$LOG_FILE" 2>&1 || true
            set_cooldown "restart_stalled"
        fi
    fi
fi

# ===== SAVE STATE =====
python3 -c "
import json, time
state = {
    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'),
    'total': $TOTAL, 'synced': $SYNCED, 'pct': $PCT,
    'errors': $ERRORS, 'downloading': $DOWNLOADING, 'new': $NEW,
    'organizing': $ORGANIZING, 'complete': $COMPLETE,
    'services': {'lexicon': '$LEXICON_OK', 'tidarr': '$TIDARR_OK', 'tidarr_auth': '$TIDARR_AUTH'},
}
with open('$STATE_FILE', 'w') as f:
    json.dump(state, f, indent=2)
"

log "DONE: All checks complete"
