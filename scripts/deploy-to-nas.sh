#!/bin/bash
# Deploy updates from Mac Mini to NAS
# Usage: ./scripts/deploy-to-nas.sh
# Can be installed as LaunchAgent or called manually

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
NAS_HOST="nas"  # SSH alias
NAS_PATH="/volume1/homes/willcurran/spotify-lexicon-sync"

cd "$REPO_DIR"

echo "[deploy] Syncing files to NAS..."
tar czf - --exclude='.next' --exclude='node_modules' --exclude='.env' --exclude='*.db' --exclude='.git' --exclude='__pycache__' . \
    | ssh "$NAS_HOST" "cd $NAS_PATH && tar xzf -"

GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
echo "[deploy] Rebuilding containers on NAS (GIT_SHA=$GIT_SHA)..."
ssh "$NAS_HOST" "cd $NAS_PATH && GIT_SHA=$GIT_SHA /usr/local/bin/docker compose build --build-arg GIT_SHA=$GIT_SHA && /usr/local/bin/docker compose up -d" 2>&1

echo "[deploy] Done."
ssh "$NAS_HOST" "/usr/local/bin/docker ps --filter name=sync --format 'table {{.Names}}\t{{.Status}}'"
