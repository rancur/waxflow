#!/bin/bash
# Deploy updates from Mac Mini to NAS
# Usage: ./scripts/deploy-to-nas.sh
# Can be installed as LaunchAgent or called manually

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
NAS_HOST="nas"  # SSH alias
NAS_PATH="/volume1/homes/willcurran/spotify-lexicon-sync"
DEPLOY_LOG="$REPO_DIR/deploy-history.log"

cd "$REPO_DIR"

# Read version from VERSION file
VERSION=$(cat VERSION 2>/dev/null || echo "0.0.0")
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

echo "[deploy] Version: $VERSION (SHA: $GIT_SHA)"
echo "[deploy] Syncing files to NAS..."
tar czf - --exclude='.next' --exclude='node_modules' --exclude='.env' --exclude='*.db' --exclude='.git' --exclude='__pycache__' . \
    | ssh "$NAS_HOST" "cd $NAS_PATH && tar xzf -"

echo "[deploy] Rebuilding containers on NAS (VERSION=$VERSION, GIT_SHA=$GIT_SHA)..."
ssh "$NAS_HOST" "cd $NAS_PATH && VERSION=$VERSION GIT_SHA=$GIT_SHA /usr/local/bin/docker compose build --build-arg GIT_SHA=$GIT_SHA && VERSION=$VERSION /usr/local/bin/docker compose up -d" 2>&1

echo "[deploy] Done."
ssh "$NAS_HOST" "/usr/local/bin/docker ps --filter name=sync --format 'table {{.Names}}\t{{.Status}}'"

# Log deployment
echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') v$VERSION ($GIT_SHA)" >> "$DEPLOY_LOG"
echo "[deploy] Logged to deploy-history.log"
