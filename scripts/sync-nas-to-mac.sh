#!/bin/bash
# WaxFlow: ONE-WAY replication of the NAS library to the Lexicon host Mac.
#
#   NAS /volume1/music/Database  --->  ~/Music/Database
#   NAS /volume1/music/Input     --->  ~/Music/Input
#
# WHY THIS EXISTS (2026-08-08)
#   Synology Drive was two-way-syncing the ENTIRE music share (session: share
#   'music', remote '/', local ~/Music/, sync_direction=0, rename_conflict=1).
#   Three things went wrong, and all three are structural, not bad luck:
#     1. Engine DJ's library lives at ~/Music/Engine Library. Two-way sync of a
#        live 595 MB SQLite file produced 12 "_Conflict" copies (~9.4 GB) and left
#        the real m.db with 2 tracks in it.
#     2. ~/Music/SoundSwitch/default.ssproj/*.ssfile is rejected by the server
#        ('System error'), and Drive retried it forever — jamming the whole queue
#        so genuinely new tracks never arrived.
#     3. Syncing the full 1.1 TB share (DJ Will See 450 G, Processing 95 G, ...)
#        filled the Mac to 86%.
#
#   ONE-WAY is the entire point. A pull-only replica cannot produce a conflict
#   copy, cannot push a half-written Engine database back to the NAS, and cannot
#   be jammed by a local file the server won't accept.
#
# TRANSPORT — why this is a hybrid
#   Data moves over the SMB mount (/Volumes/music). Change DETECTION happens over
#   SSH, because the two costs are wildly different:
#     * full rsync scan over SMB ....... 5 m 46 s  (3,736 dirs x round-trips)
#     * `find -newermt` over SSH ....... 0.9 s     (NAS walks its own disk)
#   We cannot rsync over SSH: DSM refuses `rsync --server` for non-admin users
#   ("Permission denied, please try again"), which needs a Control Panel ->
#   File Services -> rsync toggle. Detection-over-SSH gets ~99% of the win
#   without touching DSM config.
#
#   INCREMENTAL pass (every run): ask the NAS which files changed since the last
#   successful pass, copy just those over SMB. Typically <2 s.
#   RECONCILE pass (every RECONCILE_SECONDS, default 6 h, and whenever state is
#   missing/stale/SSH is down): full rsync. This is the safety net that catches
#   anything detection missed — including /volume1/music/Database/Aktive, the one
#   directory of 3,736 that is mode 000 to the SSH user but readable over SMB.
#
# WHAT IT DOES NOT DO
#   * Never touches ~/Music/Engine Library — Engine's library is LOCAL ONLY.
#     (Engine stores track paths as '../Database/<Artist>/...', relative to the
#     Engine Library folder, so a NAS copy is meaningless as well as dangerous.)
#   * No --delete. A deletion is the one thing a sync bug cannot undo, so v1
#     never removes anything from the Mac. Revisit after a week of clean runs.
#   * No push. Nothing on the Mac is ever written back to the NAS.
#
# WHY --size-only
#   SMB mtimes round-trip unreliably, and library audio is immutable once the
#   worker has moved it into place (_move_to_library uses an atomic same-volume
#   rename). Size is a sufficient and much cheaper comparison than checksums over
#   200 GB. CAVEAT: a same-size re-encode or an in-place tag edit on the NAS will
#   NOT be detected by the reconcile pass. Don't edit tags NAS-side; do it in
#   Lexicon. (The incremental pass is mtime-based and *will* catch those.)
#
# PARTIAL FILES
#   rsync transfers to a temp name and renames into place, so a partially copied
#   file is never visible at its final path. If the worker is mid-write on the NAS
#   we may copy a short file; the next cycle sees a size/mtime mismatch and
#   re-copies. Phase 3's import gate (the heartbeat below) is what stops Lexicon
#   importing inside that window.
#
# Deployed on the Lexicon host Mac at ~/.waxflow/sync-nas-to-mac.sh, run every
# 120 s by LaunchAgent com.waxflow.sync-database. THIS repo copy is canonical:
#   scp scripts/sync-nas-to-mac.sh willcurran@192.168.1.116:.waxflow/sync-nas-to-mac.sh
#
# Usage: sync-nas-to-mac.sh [--dry-run] [--reconcile]

set -uo pipefail

SRC="${WAXFLOW_SYNC_SRC:-/Volumes/music}"
DST="${WAXFLOW_SYNC_DST:-$HOME/Music}"
NAS_SSH="${WAXFLOW_NAS_SSH:-nas-lan}"
NAS_ROOT="${WAXFLOW_NAS_ROOT:-/volume1/music}"
FOLDERS=("Database" "Input")
RECONCILE_SECONDS="${WAXFLOW_RECONCILE_SECONDS:-21600}"   # 6 h
DETECT_SLACK_SECONDS=300                                  # re-ask a little further back than strictly needed

STATE_DIR="$HOME/.waxflow"
LOG="$STATE_DIR/sync-nas-to-mac.log"
LOCK="$STATE_DIR/sync-nas-to-mac.lock"
STATE="$STATE_DIR/sync-nas-to-mac.state"
HEARTBEAT="$SRC/Input/.waxflow-sync-heartbeat"
MOUNT_HELPER="$STATE_DIR/ensure-music-mount.sh"
MAX_LOG_BYTES=$((5 * 1024 * 1024))

# GNU rsync if available: macOS ships openrsync, which silently drops -e and does
# not implement --files-from. Homebrew rsync is required for the incremental pass.
RSYNC="/opt/homebrew/bin/rsync"; [ -x "$RSYNC" ] || RSYNC="$(command -v rsync)"

DRY_RUN=0; FORCE_RECONCILE=0
for a in "$@"; do
    case "$a" in
        --dry-run)   DRY_RUN=1 ;;
        --reconcile) FORCE_RECONCILE=1 ;;
    esac
done

mkdir -p "$STATE_DIR"
ts() { date "+%Y-%m-%dT%H:%M:%S"; }
log() { echo "$(ts) $*" >>"$LOG"; }

# --- log rotation (the sibling mount-music.log grew to 728 KB with none) -------
if [ -f "$LOG" ] && [ "$(stat -f%z "$LOG" 2>/dev/null || echo 0)" -gt "$MAX_LOG_BYTES" ]; then
    mv -f "$LOG" "$LOG.1"
fi

# --- single instance ----------------------------------------------------------
# A slow reconcile (200 GB over SMB) must not have a second copy stacked on it.
if ! mkdir "$LOCK" 2>/dev/null; then
    if [ -f "$LOCK/pid" ] && kill -0 "$(cat "$LOCK/pid" 2>/dev/null)" 2>/dev/null; then
        exit 0   # previous run still going; not an error
    fi
    log "stale lock (pid $(cat "$LOCK/pid" 2>/dev/null || echo '?') gone) — reclaiming"
    rm -rf "$LOCK"; mkdir "$LOCK" 2>/dev/null || exit 0
fi
echo $$ >"$LOCK/pid"
trap 'rm -rf "$LOCK"' EXIT INT TERM

# --- precondition: the SMB mount must be healthy ------------------------------
[ -x "$MOUNT_HELPER" ] && "$MOUNT_HELPER" >/dev/null 2>&1
if ! mount | grep -q " on ${SRC} (smbfs"; then
    log "ABORT: ${SRC} is not mounted (ensure-music-mount.sh could not heal it)"
    exit 1
fi
LSERR="$(ls "$SRC" 2>&1 >/dev/null)"
if [ -n "$LSERR" ]; then
    case "$LSERR" in
        *"Operation not permitted"*)
            # macOS TCC, not a broken mount. The share is healthy; THIS process is
            # denied. Nothing the script can do about it, and retrying forever just
            # fills the log — say exactly how to fix it and stop.
            log "ABORT: TCC denies this process access to ${SRC} (mount is healthy). One-time fix: System Settings -> Privacy & Security -> Full Disk Access -> add /bin/bash, then: launchctl kickstart -k gui/\$(id -u)/com.waxflow.sync-database"
            ;;
        *)
            log "ABORT: ${SRC} unreadable (${LSERR})"
            ;;
    esac
    exit 1
fi

RSYNC_COMMON=(
    -rt --size-only --modify-window=2
    --no-perms --no-owner --no-group --omit-dir-times
    --exclude=.DS_Store --exclude=._* --exclude=@eaDir
    --exclude=.SynologyWorkingDirectory --exclude=#recycle
    --exclude=*_Conflict*          # never replicate Synology conflict artefacts
    --exclude=.waxflow-sync-heartbeat
)
[ "$DRY_RUN" -eq 1 ] && RSYNC_COMMON+=(--dry-run)

# --- decide pass type ---------------------------------------------------------
NOW=$(date +%s)
LAST_OK=0; LAST_RECONCILE=0
# shellcheck disable=SC1090
[ -f "$STATE" ] && . "$STATE" 2>/dev/null
MODE="incremental"
[ "$LAST_OK" -eq 0 ] && MODE="reconcile"
[ $((NOW - LAST_RECONCILE)) -ge "$RECONCILE_SECONDS" ] && MODE="reconcile"
[ "$FORCE_RECONCILE" -eq 1 ] && MODE="reconcile"

START_EPOCH=$NOW
TOTAL_FILES=0
STATUS="ok"

if [ "$MODE" = "incremental" ]; then
    # Ask the NAS what changed. -newermt with an absolute timestamp so clock skew
    # between the two hosts cannot silently narrow the window.
    SINCE=$(( LAST_OK - DETECT_SLACK_SECONDS ))
    SINCE_FMT=$(date -u -r "$SINCE" "+%Y-%m-%d %H:%M:%S")
    LIST="$STATE_DIR/.sync-changed.$$"
    # One SSH call per folder, cd'ing in first so `find .` yields paths already
    # relative to the folder; sed re-prefixes them to be relative to $SRC, which
    # is exactly what --files-from wants. (`find -printf` is GNU-only and this NAS
    # is busybox-ish, so the cd+sed form is the portable one.)
    # NOTE: newline-in-filename would break the line-oriented list. None exist in
    # this library, and the reconcile pass would catch such a file anyway.
    : >"$LIST"
    if ssh -o BatchMode=yes -o ConnectTimeout=10 "$NAS_SSH" true 2>/dev/null; then
        # The filter MUST live in the find, not in rsync: --files-from bypasses
        # rsync's --exclude rules for explicitly listed paths, so a Synology
        # @eaDir entry (extended-attribute streams that do not exist over SMB)
        # fails the whole pass with "link_stat ... No such file or directory".
        for f in "${FOLDERS[@]}"; do
            ssh -o BatchMode=yes -o ConnectTimeout=10 "$NAS_SSH" \
                "cd '$NAS_ROOT/$f' 2>/dev/null && find . -type f -newermt '$SINCE_FMT UTC' \
                    ! -path '*/@eaDir/*' ! -name '.DS_Store' ! -name '._*' \
                    ! -name '*@SynoEAStream' ! -name '*@SynoResource' \
                    ! -name '*_Conflict*' 2>/dev/null | sed 's|^\./|$f/|'" \
                >>"$LIST" 2>/dev/null
        done
        N=$(grep -c . "$LIST" 2>/dev/null || echo 0)
        if [ "$N" -gt 0 ]; then
            OUT=$("$RSYNC" "${RSYNC_COMMON[@]}" --files-from="$LIST" "$SRC/" "$DST/" 2>&1)
            RC=$?
            if [ $RC -ne 0 ]; then
                STATUS="error"
                log "ERROR incremental rsync rc=$RC: $(echo "$OUT" | tail -3 | tr '\n' ' ')"
            else
                TOTAL_FILES=$N
                log "incremental: $N changed file(s) since $SINCE_FMT UTC"
            fi
        fi
        rm -f "$LIST"
    else
        log "SSH detection unavailable — falling back to reconcile"
        rm -f "$LIST"
        MODE="reconcile"
    fi
fi

if [ "$MODE" = "reconcile" ]; then
    for folder in "${FOLDERS[@]}"; do
        if [ ! -d "$SRC/$folder" ]; then
            log "SKIP $folder — not present on the NAS"; continue
        fi
        mkdir -p "$DST/$folder"
        OUT=$("$RSYNC" "${RSYNC_COMMON[@]}" --stats "$SRC/$folder/" "$DST/$folder/" 2>&1)
        RC=$?
        # macOS openrsync says "Number of files transferred:"; GNU rsync 3.x says
        # "Number of regular files transferred:". Match either.
        N=$(echo "$OUT" | awk '/Number of (regular )?files transferred:/ {gsub(/,/,"",$NF); print $NF; exit}')
        N=${N:-0}
        TOTAL_FILES=$((TOTAL_FILES + N))
        if [ $RC -ne 0 ]; then
            STATUS="error"
            log "ERROR reconcile rsync $folder rc=$RC: $(echo "$OUT" | tail -3 | tr '\n' ' ')"
        fi
    done
fi

ELAPSED=$(( $(date +%s) - START_EPOCH ))
# Log every pass, not just the ones that moved data — a silent log is
# indistinguishable from a dead agent, which is exactly how the July 13 fix
# rotted unnoticed. Rotation above keeps this bounded.
log "$MODE pass complete: $TOTAL_FILES file(s) in ${ELAPSED}s (status=$STATUS)"

# --- persist state ------------------------------------------------------------
if [ "$DRY_RUN" -eq 0 ] && [ "$STATUS" = "ok" ]; then
    [ "$MODE" = "reconcile" ] && LAST_RECONCILE=$START_EPOCH
    printf 'LAST_OK=%s\nLAST_RECONCILE=%s\n' "$START_EPOCH" "$LAST_RECONCILE" >"$STATE"
fi

# --- heartbeat ----------------------------------------------------------------
# Written NAS-side, inside Input/, so the WaxFlow worker container can read it at
# /downloads/.waxflow-sync-heartbeat. The organizing stage holds any track whose
# file is newer than completed_at — that is what makes importing by LOCAL
# /Users/... path safe despite replication lag.
if [ "$DRY_RUN" -eq 0 ] && [ -d "$SRC/Input" ]; then
    # Truncate-in-place, NOT write-tmp-then-mv. The share has Synology's recycle
    # bin enabled, and every replace-by-rename was being captured as a deletion —
    # one #recycle entry every 120 s, forever. A torn read is harmless here:
    # sync_gate.py fails open on unparseable JSON by design.
    printf '%s\n' "{\"status\":\"$STATUS\",\"mode\":\"$MODE\",\"completed_at\":$(date +%s),\"completed_at_iso\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"files_transferred\":$TOTAL_FILES,\"elapsed_seconds\":$ELAPSED,\"host\":\"$(scutil --get LocalHostName 2>/dev/null || hostname)\",\"folders\":\"${FOLDERS[*]}\"}" \
        >"$HEARTBEAT" 2>/dev/null || true
fi

# --- Lexicon path-drift watchdog (read-only) ----------------------------------
# WHY: Lexicon CANONICALISES imported paths through the boot-volume symlink,
# rewriting the /Users/... path WaxFlow hands it into
# /Volumes/Macintosh HD/Users/... — and Engine DJ refuses /Volumes/* locations.
# Proven live on 2026-08-09: the first import after the path-contract cutover was
# stored with that prefix even though WaxFlow sent a clean /Users path.
#
# So the source-side fix is necessary but NOT sufficient, and
# scripts/repoint-lexicon-local.sh is NOT the one-shot we hoped for. This check
# makes the drift impossible to miss: it is the exact failure that went unnoticed
# from March to August and cost the Engine library.
#
# Read-only (mode=ro), so it is safe while Lexicon is running. Fixing still
# requires Lexicon to be quit — see the log line's instruction.
LEXICON_DB="$HOME/Library/Application Support/lexicon/main.db"
if [ -f "$LEXICON_DB" ]; then
    DRIFT=$(python3 - "$LEXICON_DB" <<'PY' 2>/dev/null
import sqlite3, sys
try:
    c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    print(c.execute(
        "SELECT COUNT(*) FROM Track WHERE location LIKE '/Volumes/%'"
    ).fetchone()[0])
except Exception:
    print("")
PY
)
    if [ -n "$DRIFT" ] && [ "$DRIFT" -gt 0 ] 2>/dev/null; then
        log "LEXICON PATH DRIFT: $DRIFT row(s) back on /Volumes/* — Engine DJ cannot see them. Quit Lexicon and run scripts/repoint-lexicon-local.sh --apply"
    fi
fi

[ "$STATUS" = "ok" ] || exit 1
exit 0
