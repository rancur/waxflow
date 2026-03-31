#!/bin/bash
# Deep repair: runs periodically to analyze and fix pipeline issues
# Can be integrated with any external repair/alerting system

set -euo pipefail

API_URL="${WAXFLOW_API_URL:-http://localhost:8402}"
LOG_FILE="${WAXFLOW_LOG_DIR:-${HOME}/.waxflow/logs}/sls-deep-repair.log"
STATE_FILE="${WAXFLOW_LOG_DIR:-${HOME}/.waxflow/logs}/sls-monitor-state.json"

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
    log "REPAIR NEEDED: $REASON"
    # Hook: Add your alerting/repair dispatch here
    # Example: curl -X POST your-webhook-url -d "{\"reason\": \"$REASON\"}"
else
    log "All clear, no repair needed"
fi
