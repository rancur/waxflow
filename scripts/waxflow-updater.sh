#!/bin/sh
# WaxFlow updater — applies an update that the worker has already decided on.
#
# WHY THIS CONTAINER EXISTS
#   A container cannot restart or replace itself, so "auto-update" always needs
#   something on the host side. Previously that was scripts/auto-update.sh in the
#   user's crontab, which nothing installed — so `auto_update_enabled` was inert
#   for every install. And even when triggered it ran `docker compose up -d
#   --build` against the source already on disk, i.e. it rebuilt the SAME version.
#   Auto-update could therefore never actually update anything.
#
# SECURITY — read before changing the compose entry
#   This container mounts the Docker socket, which is root-equivalent on the host.
#   It therefore runs with NO NETWORK (`network_mode: none`) and never downloads
#   anything: the decision and the release metadata come from the worker (which
#   has network but no socket), and image layers are fetched by the DOCKER DAEMON
#   itself when we ask it to pull over the socket. Nothing with host-root access
#   ever talks to the internet. Keep it that way.
#
# WHAT IT DOES
#   Polls the shared data volume for .update-requested (written by
#   sync-worker/tasks/auto_update.py), then:
#     1. sanity-checks the requested tag (must look like a version)
#     2. asks the daemon to pull that tag for the three services
#     3. recreates ONLY those three (--no-deps, and never itself)
#     4. health-checks the API, and ROLLS BACK to the previous tag if it fails
#     5. records the outcome in .update-result for the UI/logs
#
# Rollback matters because this runs unattended at 3am by default. An update that
# half-applies and is never noticed is worse than one that never ran.

set -u

DATA_DIR="${WAXFLOW_DATA_DIR:-/data}"
PROJECT_DIR="${WAXFLOW_PROJECT_DIR:-/project}"
SIGNAL="$DATA_DIR/.update-requested"
RESULT="$DATA_DIR/.update-result"
STATE="$DATA_DIR/.update-state"
POLL="${WAXFLOW_UPDATE_POLL_SECONDS:-60}"
SERVICES="${WAXFLOW_UPDATE_SERVICES:-sync-api sync-worker sync-web}"
HEALTH_URL="${WAXFLOW_HEALTH_URL:-http://sync-api:8402/api/admin/health}"
HEALTH_TIMEOUT="${WAXFLOW_HEALTH_TIMEOUT:-180}"

log() { echo "[$(date '+%Y-%m-%dT%H:%M:%S')] $*"; }

result() {   # status, message, from, to
    cat > "$RESULT" <<EOF
{"status":"$1","message":"$2","from":"$3","to":"$4","at":"$(date -u '+%Y-%m-%dT%H:%M:%SZ')"}
EOF
}

# The health probe runs INSIDE the api container via the socket, because this
# container deliberately has no network of its own.
api_healthy() {
    docker exec waxflow-api python3 -c "
import urllib.request,sys
try:
    with urllib.request.urlopen('http://localhost:8402/api/admin/health', timeout=5) as r:
        sys.exit(0 if r.status == 200 else 1)
except Exception:
    sys.exit(1)
" >/dev/null 2>&1
}

# COMPOSE PROJECT NAME — must match the running stack.
#
# Compose derives the project name from the directory name, and this container
# mounts the project at /project, so it inferred "project" while the real stack
# was "waxflow". Different project name means compose tries to CREATE rather than
# RECREATE, and immediately hits:
#
#   Conflict. The container name "/waxflow-api" is already in use
#
# Detect it from the running container's own compose label so this is
# self-configuring regardless of where the project lives on disk.
detect_project() {
    docker inspect waxflow-api \
        --format '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null
}
PROJECT_NAME="${WAXFLOW_PROJECT_NAME:-$(detect_project)}"
[ -z "$PROJECT_NAME" ] && PROJECT_NAME="waxflow"

compose() {
    docker compose --project-name "$PROJECT_NAME" --project-directory "$PROJECT_DIR" "$@"
}

apply_tag() {   # tag -> pull + recreate the three services on that tag
    tag="$1"
    WAXFLOW_IMAGE_TAG="$tag" compose pull $SERVICES || return 1
    # shellcheck disable=SC2086
    WAXFLOW_IMAGE_TAG="$tag" compose up -d --no-deps $SERVICES || return 1
    return 0
}

log "updater started (poll=${POLL}s, project='$PROJECT_NAME', services='$SERVICES')"
if ! docker version >/dev/null 2>&1; then
    log "FATAL: cannot reach the Docker socket — is /var/run/docker.sock mounted?"
    exit 1
fi

while true; do
    if [ -f "$SIGNAL" ]; then
        TARGET=$(sed -n 's/.*"target_version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$SIGNAL" | head -1)
        CURRENT=$(sed -n 's/.*"current_version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$SIGNAL" | head -1)
        [ -f "$STATE" ] && PREV=$(cat "$STATE") || PREV="$CURRENT"
        rm -f "$SIGNAL"

        # Never let an unexpected string reach `docker pull`. The tag originates
        # from the GitHub API, so treat it as untrusted input.
        if ! echo "$TARGET" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
            log "REFUSE: target_version '$TARGET' is not a semver tag"
            result "refused" "target_version not semver: $TARGET" "$CURRENT" "$TARGET"
            sleep "$POLL"; continue
        fi

        log "applying update $CURRENT -> $TARGET (rollback tag: $PREV)"
        if apply_tag "$TARGET"; then
            waited=0
            healthy=0
            while [ "$waited" -lt "$HEALTH_TIMEOUT" ]; do
                if api_healthy; then healthy=1; break; fi
                sleep 5; waited=$((waited + 5))
            done
            if [ "$healthy" -eq 1 ]; then
                log "update OK: now on $TARGET (healthy after ${waited}s)"
                echo "$TARGET" > "$STATE"
                result "success" "updated to $TARGET" "$CURRENT" "$TARGET"
            else
                log "UNHEALTHY after ${HEALTH_TIMEOUT}s — rolling back to $PREV"
                if apply_tag "$PREV"; then
                    result "rolled_back" "unhealthy after ${HEALTH_TIMEOUT}s; restored $PREV" "$CURRENT" "$TARGET"
                    log "rollback to $PREV complete"
                else
                    result "failed" "update unhealthy AND rollback to $PREV failed" "$CURRENT" "$TARGET"
                    log "ROLLBACK FAILED — manual intervention needed"
                fi
            fi
        else
            log "pull/up failed for $TARGET — leaving the running stack untouched"
            result "failed" "pull or up failed for $TARGET" "$CURRENT" "$TARGET"
        fi
    fi
    sleep "$POLL"
done
