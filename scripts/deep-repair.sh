#!/bin/bash
# Deep repair: runs every 6 hours via OpenClaw cron
# Dispatches a Claude Code session to analyze and fix issues

set -euo pipefail

API_URL="http://192.168.1.221:8402"
LOG_FILE="${HOME}/.openclaw/logs/sls-deep-repair.log"
STATE_FILE="${HOME}/.openclaw/logs/sls-monitor-state.json"

mkdir -p "$(dirname "$LOG_FILE")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"; }

# Get current state
DASHBOARD=$(curl -s --connect-timeout 10 "$API_URL/api/dashboard" 2>/dev/null || echo "")
if [ -z "$DASHBOARD" ]; then
    log "API down, skipping deep repair"
    exit 0
fi

PCT=$(echo "$DASHBOARD" | python3 -c "import json,sys; print(json.loads(sys.stdin.read())['parity_pct'])")
ERRORS=$(echo "$DASHBOARD" | python3 -c "import json,sys; print(json.loads(sys.stdin.read()).get('by_pipeline_stage',{}).get('error',0))")

# Check if previous state exists
if [ -f "$STATE_FILE" ]; then
    PREV_PCT=$(python3 -c "import json; print(json.load(open('$STATE_FILE')).get('pct',0))" 2>/dev/null || echo 0)
else
    PREV_PCT=0
fi

log "Deep repair check: parity=$PCT% (prev=$PREV_PCT%) errors=$ERRORS"

# Conditions to dispatch repair:
# 1. Parity hasn't improved in 6 hours (stuck)
# 2. Error count > 300
# 3. Parity < 95% after 48 hours of operation
NEEDS_REPAIR=false
REASON=""

if python3 -c "import sys; sys.exit(0 if abs($PCT - $PREV_PCT) < 1.0 and $PCT < 90 else 1)" 2>/dev/null; then
    NEEDS_REPAIR=true
    REASON="Parity stuck at $PCT% (was $PREV_PCT%)"
fi

if [ "$ERRORS" -gt 300 ]; then
    NEEDS_REPAIR=true
    REASON="$REASON High error count: $ERRORS"
fi

if [ "$NEEDS_REPAIR" = true ]; then
    log "DISPATCHING REPAIR: $REASON"
    # Dispatch Claude Code via cc-spawn for deep analysis
    if command -v openclaw &> /dev/null; then
        openclaw agent --agent repair --message "REPAIR REQUEST from sls-deep-repair. Spotify-Lexicon Sync issues: $REASON. Project at ~/spotify-lexicon-sync. API at $API_URL. Check pipeline, fix errors, deploy. Logs at ~/.openclaw/logs/sls-monitor.log" >> "$LOG_FILE" 2>&1 || true
    fi
else
    log "All clear, no repair needed"
fi
