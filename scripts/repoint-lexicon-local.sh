#!/bin/bash
# WaxFlow — re-point Lexicon Track locations to canonical LOCAL
# /Users/willcurran/Music/Database/* paths so Engine DJ export includes ALL tracks.
#
# WHY
#   Engine DJ cannot ingest /Volumes/* locations (network/removable-style paths).
#   Historically Lexicon rows carried three non-canonical prefixes:
#     • /Volumes/Macintosh HD/Users/willcurran/Music/...  (symlink to /, LOCAL disk)
#     • /Volumes/music/...                                (SMB mount of the NAS share)
#     • /Users/willcurran/Music/<Artist>/...              (share ROOT replica, i.e.
#                                                          outside the library root)
#   The files already exist locally under /Users/willcurran/Music/Database, so
#   re-pointing fixes the export WITHOUT touching a single audio file.
#
# STATUS (2026-08-08): THIS SCRIPT IS NOW A ONE-SHOT, NOT A RECURRING CHORE.
#   It was written on 2026-07-13, fixed the problem, and then the problem came back
#   because nothing automated it and new imports kept arriving with /Volumes paths.
#   That root cause is now fixed at the source: lexicon_library_path is
#   /Users/willcurran/Music/Database and the worker writes into
#   MUSIC_LIBRARY_PATH=/music/Database, so every NEW track is canonical on arrival
#   (see sync-worker/tasks/process_pipeline.py::_container_to_mac_path and
#   tasks/sync_gate.py). Run this once to clean up the legacy rows. If it ever finds
#   work again, that is a REGRESSION SIGNAL — something reverted the path contract.
#
# WHAT IT DOES  (NON-DESTRUCTIVE to files; touches ONLY Track.location)
#     /Volumes/Macintosh HD/Users/...   -> /Users/...                    (strip prefix)
#     /Volumes/music/Database/<rest>    -> ~/Music/Database/<rest>
#     /Volumes/music/Input/<rest>       -> ~/Music/Input/<rest>
#     /Volumes/music/<rest>             -> ~/Music/Database/<rest>       (post-consolidation)
#     ~/Music/<Artist>/<rest>           -> ~/Music/Database/<Artist>/<rest>
#   ONLY if the resulting local file EXISTS on disk (os.path.isfile). If it is not
#   present (sync incomplete, or the folder deliberately stays at the share root —
#   e.g. Disorganized/), the track is LEFT UNCHANGED. Idempotent: rows already
#   canonical are skipped.
#   Updates ONLY the `location` column. `locationUnique` is Lexicon's immutable
#   import-identity key (it already legitimately diverges on ~5121 rows), so it is
#   deliberately NOT modified — this avoids the UNIQUE index entirely.
#
# SAFETY GATES (all enforced; refuses otherwise)
#   1. A fresh verified DB backup must exist (heartbeat integrity==ok, Track>0),
#      or pass --skip-backup-check only if you JUST ran scripts/backup-lexicon-db.sh.
#   2. Lexicon MUST be quit (a running Lexicon caches rows and would clobber or
#      contend with the write). The script refuses if it sees the Lexicon process.
#   3. Dry-run by default. Requires --apply to write. --limit N does a SMALL batch first.
#   4. PRAGMA integrity_check before and after; per-row old->new audit log.
#
# USAGE
#   Runs LOCALLY when invoked on the Lexicon host Mac, or over SSH from an ops box.
#   Local mode is auto-detected (the Lexicon DB is present); force with LEXICON_SSH=local.
#
#   scripts/backup-lexicon-db.sh                          # gate: fresh verified backup
#   # -> quit Lexicon <-
#   scripts/repoint-lexicon-local.sh                      # dry-run, full report
#   scripts/repoint-lexicon-local.sh --apply --limit 20   # small batch, then verify
#   scripts/repoint-lexicon-local.sh --apply              # full run
#
set -euo pipefail

LEXICON_SSH="${LEXICON_SSH:-willcurran@192.168.1.116}"
HEARTBEAT="${HEARTBEAT:-$HOME/.waxflow/logs/lexicon-backup-heartbeat.json}"
AUDIT_DIR="${AUDIT_DIR:-$HOME/.waxflow/logs}"
APPLY=0; LIMIT=0; SKIP_BACKUP_CHECK=0
while [ $# -gt 0 ]; do case "$1" in
  --apply) APPLY=1;;
  --limit) LIMIT="$2"; shift;;
  --skip-backup-check) SKIP_BACKUP_CHECK=1;;
  *) echo "unknown arg: $1" >&2; exit 2;;
esac; shift; done

# Local mode: we are already ON the Lexicon host, so skip SSH entirely.
LOCAL_DB="$HOME/Library/Application Support/lexicon/main.db"
if [ "$LEXICON_SSH" = "local" ] || [ -f "$LOCAL_DB" ]; then
    MODE="local"
else
    MODE="ssh"
fi
echo "[repoint] mode=$MODE apply=$APPLY limit=${LIMIT:-none}"

# Gate 1 — fresh verified backup
if [ "$SKIP_BACKUP_CHECK" -eq 0 ]; then
  if [ ! -f "$HEARTBEAT" ]; then echo "REFUSE: no backup heartbeat ($HEARTBEAT). Run scripts/backup-lexicon-db.sh first." >&2; exit 1; fi
  python3 - "$HEARTBEAT" <<'PY' || exit 1
import json,sys
h=json.load(open(sys.argv[1]))
assert h.get("status")=="ok" and h.get("integrity")=="ok" and int(h.get("track_count",0))>0, "backup heartbeat not verified"
print("backup gate OK: Track=%s integrity=%s file=%s"%(h["track_count"],h["integrity"],h["file"]))
PY
fi

# Gate 2 — Lexicon must be quit (enforced only for a real write; dry-run reads are safe)
if [ "$APPLY" -eq 1 ]; then
  if [ "$MODE" = "local" ]; then
    RUNNING=$(pgrep -x Lexicon >/dev/null 2>&1 && echo yes || (pgrep -f "Lexicon.app/Contents/MacOS/Lexicon" >/dev/null 2>&1 && echo yes || echo no))
  else
    RUNNING=$(ssh "$LEXICON_SSH" 'pgrep -x Lexicon >/dev/null 2>&1 || pgrep -f "Lexicon.app/Contents/MacOS/Lexicon" >/dev/null 2>&1' && echo yes || echo no)
  fi
  if [ "$RUNNING" = "yes" ]; then
    echo "REFUSE: Lexicon is RUNNING. Quit Lexicon fully, then re-run --apply." >&2
    exit 1
  fi
fi

TS=$(date +%Y%m%d-%H%M%S)
AUDIT="$AUDIT_DIR/repoint-lexicon-$TS.log"
mkdir -p "$AUDIT_DIR"
echo "audit -> $AUDIT"

read -r -d '' PYSRC <<'PY' || true
import sqlite3, os, sys, unicodedata
db, apply, limit = sys.argv[1], sys.argv[2]=="1", int(sys.argv[3])
HOME="/Users/willcurran"
MUSIC=HOME+"/Music"
LIB=MUSIC+"/Database"
INP=MUSIC+"/Input"

def newpath(loc):
    if loc.startswith("/Volumes/Macintosh HD/"): return loc[len("/Volumes/Macintosh HD"):]
    if loc.startswith("/Volumes/music/Database/"): return LIB+"/"+loc[len("/Volumes/music/Database/"):]
    if loc.startswith("/Volumes/music/Input/"):    return INP+"/"+loc[len("/Volumes/music/Input/"):]
    if loc.startswith("/Volumes/music/"):          return LIB+"/"+loc[len("/Volumes/music/"):]
    # share-ROOT replica: ~/Music/<Artist>/... but NOT already under Database/ or Input/
    if loc.startswith(MUSIC+"/") and not loc.startswith(LIB+"/") and not loc.startswith(INP+"/"):
        return LIB+"/"+loc[len(MUSIC+"/"):]
    return None

c=sqlite3.connect(db)
print("integrity_before=", c.execute("PRAGMA integrity_check").fetchone()[0])
rows=c.execute("SELECT id,location FROM Track ORDER BY id").fetchall()
changed=skipped_missing=already=0; n=0
for i,loc in rows:
    if not loc: continue
    np=newpath(loc)
    if np is None:
        already+=1
        continue
    if not os.path.isfile(np):
        skipped_missing+=1; print("SKIP_MISSING id=%d %s"%(i,np)); continue
    print("REPOINT id=%d\n   old=%s\n   new=%s"%(i,loc,np))
    if apply:
        c.execute("UPDATE Track SET location=? WHERE id=?", (np,i))
    changed+=1; n+=1
    if limit and n>=limit: break
if apply: c.commit()
print("integrity_after=", c.execute("PRAGMA integrity_check").fetchone()[0])
print("SUMMARY changed=%d skipped_missing=%d already_canonical=%d applied=%s limit=%s"%(changed,skipped_missing,already,apply,limit or "none"))
PY

if [ "$MODE" = "local" ]; then
    python3 -c "$PYSRC" "$LOCAL_DB" "$APPLY" "$LIMIT" | tee "$AUDIT"
else
    ssh "$LEXICON_SSH" "python3 - \"\$HOME/Library/Application Support/lexicon/main.db\" $APPLY $LIMIT" <<PYWRAP | tee "$AUDIT"
$PYSRC
PYWRAP
fi
echo "done. audit: $AUDIT"
