#!/bin/bash
# Auto-update script for spotify-lexicon-sync
# Checks the Docker volume for a signal file from the web UI
# NAS doesn't have git, so updates are pushed via rsync from Mac Mini
# Install as Synology Task Scheduler: every 5 min
# Or run from Mac Mini cron: */5 * * * * /path/to/auto-update.sh

set -euo pipefail

REPO_DIR="${REPO_DIR:-/volume1/homes/willcurran/spotify-lexicon-sync}"
LOG_FILE="${REPO_DIR}/logs/auto-update.log"
LOCK_FILE="/tmp/sls-update.lock"
DOCKER="/usr/local/bin/docker"

# Signal file is in the Docker volume
VOLUME_DATA=$(${DOCKER} volume inspect spotify-lexicon-sync_sync-data --format '{{.Mountpoint}}' 2>/dev/null || echo "")
SIGNAL_FILE="${VOLUME_DATA}/.update-requested"

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
if [ -n "$SIGNAL_FILE" ] && [ -f "$SIGNAL_FILE" ]; then
    log "Update requested via web UI"
    rm -f "$SIGNAL_FILE"
    ${DOCKER} compose up -d --build >> "$LOG_FILE" 2>&1
    log "Rebuild complete (web UI trigger)"
    exit 0
fi

log "No update needed"
