#!/bin/bash
# WaxFlow v3 — Feature 8: NAS backup-aware throttling HOST-SIDE probe.
#
# WHY THIS EXISTS
#   The WaxFlow worker runs in a Docker container on the NAS. It can read the
#   host's aggregate iowait from /proc/stat (shared kernel) but it CANNOT see
#   the host-side Synology HyperBackup processes (they live in the host PID
#   namespace). So this tiny script runs ON THE NAS HOST, detects a running
#   HyperBackup, samples iowait, and publishes three signals into app_config —
#   the same key/value signal bus the worker already uses. tasks/throttle.py
#   (should_yield) consumes them.
#
# WHAT IT WRITES  (app_config in sync.db — the signal bus, nothing else)
#   nas_backup_active      "1" while a HyperBackup process is running, else "0"
#   nas_iowait_pct         host iowait percent over a short sampling window
#   nas_signal_updated_at  epoch seconds of this write (freshness / staleness)
#
# NON-DISRUPTIVE BY DESIGN
#   * READ-ONLY on the host: a pgrep + two reads of /proc/stat. No host state
#     is changed, no process is signalled, no backup is touched.
#   * The ONLY write is three UPSERTs into app_config (idempotent key/value).
#     It never touches tracks, playlists, the pipeline, or any file. Worst case
#     of a bug here is a stale/absent signal, which throttle.py fails OPEN on
#     (heavy ops proceed) — it can never stall the pipeline.
#   * Runs in well under a second. Intended cadence: every 60–120s via cron or a
#     systemd timer (finer-grained than monitor-parity.sh's 30-min self-heal
#     loop, which is why this is a separate probe rather than folded into it).
#
# DEPLOY (staged — NOT auto-deployed; coordinate with the worker rollout)
#   Copy to the NAS and schedule, e.g. crontab:
#     * * * * * /volume1/waxflow/scripts/nas-backup-probe.sh >/dev/null 2>&1
#   The signals are inert until Phase C flips backup_throttle_enabled=1.
#
# CONFIG (env overrides; sane Synology defaults)
#   DOCKER_CMD            docker binary                      (default: docker)
#   API_CONTAINER         container that can reach sync.db   (default: waxflow-api)
#   DB_PATH_IN_CONTAINER  sync.db path inside that container (default: /app/data/sync.db)
#   BACKUP_PROC_REGEX     pgrep pattern for HyperBackup      (default: synoimgbkptool|aws_s3_ccpd)
#   IOWAIT_SAMPLE_SECS    sampling window for iowait delta   (default: 1)
#   PROBE_LOG            optional log file                   (default: none/stderr)

set -euo pipefail

DOCKER_CMD="${DOCKER_CMD:-docker}"
API_CONTAINER="${API_CONTAINER:-waxflow-api}"
DB_PATH_IN_CONTAINER="${DB_PATH_IN_CONTAINER:-/app/data/sync.db}"
BACKUP_PROC_REGEX="${BACKUP_PROC_REGEX:-synoimgbkptool|aws_s3_ccpd}"
IOWAIT_SAMPLE_SECS="${IOWAIT_SAMPLE_SECS:-1}"
PROBE_LOG="${PROBE_LOG:-}"

log() {
    local line="[$(date '+%Y-%m-%d %H:%M:%S')] nas-backup-probe: $*"
    if [ -n "$PROBE_LOG" ]; then
        echo "$line" >> "$PROBE_LOG"
    else
        echo "$line" >&2
    fi
}

# ---- 1) Detect a running HyperBackup process (host PID namespace) ----------
# pgrep -f matches the full command line. Exit status 1 == "no match", which is
# normal and must NOT trip `set -e`.
if pgrep -f -- "$BACKUP_PROC_REGEX" >/dev/null 2>&1; then
    BACKUP_ACTIVE=1
else
    BACKUP_ACTIVE=0
fi

# ---- 2) Sample host iowait from /proc/stat ---------------------------------
# The cpu aggregate line is: cpu user nice system idle iowait irq softirq steal ...
# iowait is field 6 (index 5 after the "cpu" label). We take two samples and
# compute the iowait share of the delta so the number reflects the *current*
# window, not since-boot averages.
read_cpu_fields() {
    # Prints: <iowait_jiffies> <total_jiffies>
    awk '/^cpu /{
        total=0; for (i=2; i<=NF; i++) total+=$i;
        print $6, total; exit
    }' /proc/stat
}

IOWAIT_PCT=0
if [ -r /proc/stat ]; then
    S1=$(read_cpu_fields || echo "")
    sleep "$IOWAIT_SAMPLE_SECS"
    S2=$(read_cpu_fields || echo "")
    if [ -n "$S1" ] && [ -n "$S2" ]; then
        IOWAIT_PCT=$(awk -v s1="$S1" -v s2="$S2" 'BEGIN{
            split(s1, a, " "); split(s2, b, " ");
            di = b[1]-a[1]; dt = b[2]-a[2];
            if (dt <= 0) { print 0; }
            else { p = 100.0*di/dt; if (p<0) p=0; if (p>100) p=100; printf "%.0f", p; }
        }')
    fi
else
    log "WARN: /proc/stat not readable; reporting iowait=0"
fi

NOW="$(date +%s)"
log "backup_active=$BACKUP_ACTIVE iowait=${IOWAIT_PCT}% -> app_config"

# ---- 3) Publish signals into app_config (the only write) -------------------
# Done inside the container that owns sync.db, mirroring monitor-parity.sh's
# `docker exec <api> python3` pattern. UPSERT matches helpers.set_config exactly.
"$DOCKER_CMD" exec -i "$API_CONTAINER" python3 - "$DB_PATH_IN_CONTAINER" \
    "$BACKUP_ACTIVE" "$IOWAIT_PCT" "$NOW" <<'PY'
import sqlite3, sys
db_path, backup_active, iowait_pct, now = sys.argv[1:5]
conn = sqlite3.connect(db_path, timeout=30)
try:
    conn.execute("PRAGMA journal_mode=WAL")
    for key, value in (
        ("nas_backup_active", str(backup_active)),
        ("nas_iowait_pct", str(iowait_pct)),
        ("nas_signal_updated_at", str(now)),
    ):
        conn.execute(
            "INSERT INTO app_config (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = ?",
            (key, value, value),
        )
    conn.commit()
finally:
    conn.close()
PY

log "done"
