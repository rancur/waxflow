#!/bin/bash
# Deploy updates to a remote Docker host (e.g., NAS)
# Usage: ./scripts/deploy-to-nas.sh
# Can be called manually or from CI/CD

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
REMOTE_HOST="${WAXFLOW_REMOTE_HOST:-nas}"  # SSH alias or hostname
REMOTE_PATH="${WAXFLOW_REMOTE_PATH:-/opt/waxflow}"
DEPLOY_LOG="$REPO_DIR/deploy-history.log"

cd "$REPO_DIR"

# Read version from VERSION file
VERSION=$(cat VERSION 2>/dev/null || echo "0.0.0")
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

echo "[deploy] Version: $VERSION (SHA: $GIT_SHA)"
echo "[deploy] Syncing files to remote host..."
tar czf - --exclude='.next' --exclude='node_modules' --exclude='.env' --exclude='*.db' --exclude='.git' --exclude='__pycache__' . \
    | ssh "$REMOTE_HOST" "cd $REMOTE_PATH && tar xzf -"

echo "[deploy] Rebuilding containers on remote host (VERSION=$VERSION, GIT_SHA=$GIT_SHA)..."
ssh "$REMOTE_HOST" "cd $REMOTE_PATH && VERSION=$VERSION GIT_SHA=$GIT_SHA docker compose build --build-arg GIT_SHA=$GIT_SHA && VERSION=$VERSION docker compose up -d" 2>&1

echo "[deploy] Done."
ssh "$REMOTE_HOST" "docker ps --filter name=sync --format 'table {{.Names}}\t{{.Status}}'"

# Log deployment
echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') v$VERSION ($GIT_SHA)" >> "$DEPLOY_LOG"
echo "[deploy] Logged to deploy-history.log"
