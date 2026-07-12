#!/bin/bash
# WaxFlow — REAL Lexicon DJ library-database backup (the safety net).
#
# WHY THIS EXISTS
#   Lexicon stores Will's entire DJ library — tracks, playlists, cue points,
#   tags, links — in a single SQLite DB on his Mac:
#     ~/Library/Application Support/lexicon/main.db   (WAL mode, ~150 MB)
#   That DB had NEVER been backed up. `sync-worker/tasks/backup_lexicon.py` only
#   pinged the Lexicon HTTP API and (falsely) recorded "verified"; Time Machine
#   skips ~/Music/Database; Wasabi HyperBackup covers the music FILES but not the
#   DB. This script is the actual backup and MUST run before any delicate WaxFlow
#   work touches the library.
#
# WHAT IT DOES  (NON-DESTRUCTIVE — only READS the DB, only WRITES new files)
#   1. On the Lexicon Mac: take a consistent SQLite *online* backup with
#      `sqlite3 ".backup"` — this does NOT lock or require quitting Lexicon and
#      captures the live WAL-committed state.
#   2. Verify it: `PRAGMA integrity_check` == "ok" AND Track row-count > 0.
#   3. gzip it, keep a rotated copy ON THE MAC (independent hardware #1).
#   4. Stream it to a dedicated dir ON THE NAS (independent hardware #2),
#      throttled + niced, and verify the copy (gunzip -t + sha256 match).
#   5. Rotate both sides to the last KEEP snapshots; write a heartbeat + log so a
#      silent failure is impossible (fail-loud, exit non-zero on any problem).
#
#   It NEVER modifies/deletes the source DB, never restarts a service, never
#   quits Lexicon, never reboots anything.
#
# RUNS ON: the ops Mac (openclaw / Barry) — the box that already SSHes into both
#   the Lexicon Mac and the NAS and hosts the sls-monitor LaunchAgents. It reads
#   the DB on the Lexicon Mac over SSH and pushes to the NAS over SSH.
#
# SCHEDULE: daily via LaunchAgent com.openclaw.waxflow-lexicon-backup, AND run
#   manually before any delicate library operation:  scripts/backup-lexicon-db.sh
#
# CONFIG (env overrides; sane defaults):
#   LEXICON_SSH        ssh target for the Lexicon Mac   (default willcurran@192.168.1.116)
#   LEXICON_DB         DB path on that Mac (remote $HOME expands remotely)
#   MAC_BACKUP_DIR     rotated copies on the Mac        (default ~/WaxFlow-Backups/lexicon-db)
#   NAS_SSH            ssh alias/target for the NAS     (default nas ; alias sets port 7844)
#   NAS_BACKUP_DIR     rotated copies on the NAS        (default /volume1/homes/willcurran/WaxFlow-Backups/lexicon-db)
#   KEEP               snapshots to retain each side    (default 14)
#   LOG_DIR            heartbeat + log dir on ops box   (default ~/.waxflow/logs)
#   SKIP_ON_HYPERBACKUP  1 = defer NAS push while a HyperBackup runs (default 1)

set -euo pipefail

LEXICON_SSH="${LEXICON_SSH:-willcurran@192.168.1.116}"
LEXICON_DB="${LEXICON_DB:-\$HOME/Library/Application Support/lexicon/main.db}"
MAC_BACKUP_DIR="${MAC_BACKUP_DIR:-\$HOME/WaxFlow-Backups/lexicon-db}"
NAS_SSH="${NAS_SSH:-nas}"
NAS_BACKUP_DIR="${NAS_BACKUP_DIR:-/volume1/homes/willcurran/WaxFlow-Backups/lexicon-db}"
KEEP="${KEEP:-14}"
LOG_DIR="${LOG_DIR:-$HOME/.waxflow/logs}"
SKIP_ON_HYPERBACKUP="${SKIP_ON_HYPERBACKUP:-1}"
SSH_OPTS="-o ConnectTimeout=20 -o ServerAliveInterval=15"

mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/lexicon-backup.log"
HEARTBEAT="$LOG_DIR/lexicon-backup-heartbeat.json"
TS="$(date +%Y%m%d-%H%M%S)"
BASENAME="lexicon-main-$TS.db.gz"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG" >&2; }

fail() {
    log "ERROR: $*"
    printf '{"ts":"%s","status":"error","error":%s}\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(printf '%s' "$1" | sed 's/"/\\"/g;s/^/"/;s/$/"/')" \
        > "$HEARTBEAT"
    exit 1
}

log "=== lexicon-db backup start ($TS) ==="

# ---- 1) Take + verify the online backup ON THE MAC, then gzip + rotate -------
# All of this runs remotely on the Lexicon Mac (fast local disk read of the DB).
# The remote script prints one line:  OK <sha256> <track_count> <gz_bytes> <gz_path>
# NOTE: the DB path contains a space ("Application Support"), and ssh hands the
# command to the Mac's LOGIN shell which word-splits unquoted args — so the
# space-containing paths are resolved INSIDE the (quoted) remote bash heredoc,
# with proper quoting, from the Mac's own $HOME. Only space-free values (TS,
# KEEP, and optional no-space overrides) are passed positionally.
# Capture the remote body into a variable WITHOUT a heredoc-inside-$() (macOS's
# bash 3.2 mis-parses that). `read -d ''` returns non-zero at EOF — expected.
IFS= read -r -d '' REMOTE_SCRIPT <<'REMOTE' || true
set -euo pipefail
TS="$1"; KEEP="$2"; DB_OVERRIDE="${3:-}"; DIR_OVERRIDE="${4:-}"
# Canonical Lexicon DB + Mac-side backup dir, resolved from the Mac's own $HOME
# (quotes protect the space in "Application Support"). Overridable only with a
# space-free absolute path.
DB="${DB_OVERRIDE:-$HOME/Library/Application Support/lexicon/main.db}"
DIR="${DIR_OVERRIDE:-$HOME/WaxFlow-Backups/lexicon-db}"
mkdir -p "$DIR"
[ -f "$DB" ] || { echo "REMOTE_ERR source DB not found: $DB" >&2; exit 3; }
OUT="$DIR/lexicon-main-$TS.db"
# Online backup — consistent snapshot, no lock, no need to quit Lexicon.
# Source opened read-only so the live DB is never written.
nice -n 19 sqlite3 "file:$DB?mode=ro" ".backup \"$OUT\"" >&2
# Verify on a plain path (a fresh .backup inherits WAL journal mode; a mode=ro
# open of a WAL file with no live -shm fails CANTOPEN, so use the plain path).
IC="$(sqlite3 "$OUT" 'PRAGMA integrity_check;')"
[ "$IC" = "ok" ] || { echo "REMOTE_ERR integrity_check=$IC" >&2; rm -f "$OUT"; exit 4; }
TC="$(sqlite3 "$OUT" 'SELECT COUNT(*) FROM Track;')"
[ "$TC" -gt 0 ] 2>/dev/null || { echo "REMOTE_ERR Track count=$TC" >&2; rm -f "$OUT"; exit 5; }
nice -n 19 gzip -6 "$OUT"
GZ="$OUT.gz"
SHA="$(shasum -a 256 "$GZ" | cut -d' ' -f1)"
BYTES="$(stat -f%z "$GZ")"
# Rotate: keep newest $KEEP on the Mac.
ls -1t "$DIR"/lexicon-main-*.db.gz 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f
echo "OK $SHA $TC $BYTES $GZ"
REMOTE

REMOTE_OUT="$(printf '%s' "$REMOTE_SCRIPT" | ssh $SSH_OPTS "$LEXICON_SSH" \
    bash -s "$TS" "$KEEP" "${LEXICON_DB_OVERRIDE:-}" "${MAC_BACKUP_DIR_OVERRIDE:-}")" \
    || fail "remote backup step failed (see log / stderr above)"

read -r TAG SHA TRACK_COUNT GZ_BYTES GZ_PATH <<<"$REMOTE_OUT"
[ "$TAG" = "OK" ] || fail "remote backup did not report OK: $REMOTE_OUT"
log "Mac backup OK: $GZ_PATH ($GZ_BYTES bytes, Track=$TRACK_COUNT, integrity ok, sha=$SHA)"

# ---- 2) Throttle gate: defer the NAS push if a HyperBackup is running --------
NAS_STATUS="pushed"
if [ "$SKIP_ON_HYPERBACKUP" = "1" ] && \
   ssh $SSH_OPTS "$NAS_SSH" 'pgrep -f -- "synoimgbkptool|aws_s3_ccpd" >/dev/null 2>&1'; then
    NAS_STATUS="deferred_hyperbackup"
    log "WARN: HyperBackup active on NAS — deferring NAS push (Mac copy is safe; next run retries)"
else
    # ---- 3) Stream Mac->NAS through this box (niced), then verify ------------
    ssh $SSH_OPTS "$NAS_SSH" "mkdir -p '$NAS_BACKUP_DIR'" || fail "cannot create NAS dir $NAS_BACKUP_DIR"
    DEST="$NAS_BACKUP_DIR/$BASENAME"
    # scp's sftp subsystem is disabled on this Synology, so stream over ssh cat.
    if nice -n 19 ssh $SSH_OPTS "$LEXICON_SSH" "cat \"$GZ_PATH\"" \
         | nice -n 19 ssh $SSH_OPTS "$NAS_SSH" "cat > '$DEST'"; then
        NAS_SHA="$(ssh $SSH_OPTS "$NAS_SSH" "gunzip -t '$DEST' && (command -v sha256sum >/dev/null && sha256sum '$DEST' | cut -d' ' -f1 || openssl dgst -sha256 '$DEST' | awk '{print \$NF}')")" \
            || fail "NAS copy failed gunzip -t / hash"
        [ "$NAS_SHA" = "$SHA" ] || fail "NAS sha mismatch: nas=$NAS_SHA mac=$SHA"
        # Rotate NAS side.
        ssh $SSH_OPTS "$NAS_SSH" "ls -1t '$NAS_BACKUP_DIR'/lexicon-main-*.db.gz 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f" || true
        log "NAS copy OK: $DEST (sha match)"
    else
        fail "streaming Mac->NAS failed"
    fi
fi

# ---- 4) Heartbeat (fail-loud freshness signal) ------------------------------
printf '{"ts":"%s","status":"ok","file":"%s","sha256":"%s","gz_bytes":%s,"track_count":%s,"integrity":"ok","mac_copy":"%s","nas_status":"%s","nas_dir":"%s","keep":%s}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$BASENAME" "$SHA" "$GZ_BYTES" "$TRACK_COUNT" \
    "$GZ_PATH" "$NAS_STATUS" "$NAS_BACKUP_DIR" "$KEEP" > "$HEARTBEAT"

log "=== lexicon-db backup done (nas_status=$NAS_STATUS) ==="
[ "$NAS_STATUS" = "pushed" ] || log "NOTE: offsite/NAS push was deferred; re-run when the NAS is idle."
exit 0
