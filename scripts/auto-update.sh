#!/bin/bash
# Auto-update script for spotify-lexicon-sync
# Polls GitHub for new commits and rebuilds if found
# Install as cron: */5 * * * * /volume1/homes/willcurran/spotify-lexicon-sync/scripts/auto-update.sh

set -euo pipefail

REPO_DIR="${REPO_DIR:-/volume1/homes/willcurran/spotify-lexicon-sync}"
LOG_FILE="${REPO_DIR}/logs/auto-update.log"
LOCK_FILE="/tmp/sls-update.lock"

mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"; }

# Prevent concurrent runs
if [ -f "$LOCK_FILE" ]; then
    pid=$(cat "$LOCK_FILE" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        exit 0
    fi
fi
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

cd "$REPO_DIR"

# Check for signal file from web UI
SIGNAL_FILE="${REPO_DIR}/.update-requested"
if [ -f "$SIGNAL_FILE" ]; then
    log "Update requested via web UI"
    rm -f "$SIGNAL_FILE"
    git pull origin main >> "$LOG_FILE" 2>&1
    /usr/local/bin/docker compose up -d --build >> "$LOG_FILE" 2>&1
    log "Update complete (manual trigger)"
    exit 0
fi

# Check for new commits
git fetch origin main --quiet 2>/dev/null || { log "Failed to fetch"; exit 1; }

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" = "$REMOTE" ]; then
    exit 0
fi

log "New commits detected: $LOCAL -> $REMOTE"
git pull origin main >> "$LOG_FILE" 2>&1
/usr/local/bin/docker compose up -d --build >> "$LOG_FILE" 2>&1
log "Update complete: now at $(git rev-parse --short HEAD)"
