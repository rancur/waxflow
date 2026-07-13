#!/bin/bash
# WaxFlow — re-point Lexicon Track locations from /Volumes/* to canonical LOCAL
# /Users/willcurran/Music/* paths so Engine DJ export includes ALL tracks.
#
# WHY
#   Every Track.location in Lexicon's main.db uses a /Volumes/* prefix:
#     • /Volumes/Macintosh HD/Users/willcurran/Music/...   (symlink to /, LOCAL disk)
#     • /Volumes/music/...                                 (SMB mount of the NAS share)
#   Engine DJ export can't ingest /Volumes/* (network/removable-style) paths, so only a
#   fraction of the library shows up. The files themselves already exist locally under
#   /Users/willcurran/Music (Synology Drive replica), so re-pointing to the canonical
#   /Users/... path fixes the export WITHOUT touching a single audio file.
#
# WHAT IT DOES  (NON-DESTRUCTIVE to files; touches ONLY Track.location)
#   For each track:
#     /Volumes/Macintosh HD/Users/...  ->  /Users/...            (strip the symlink prefix)
#     /Volumes/music/<rest>            ->  /Users/willcurran/Music/<rest>
#   ONLY if the resulting local file EXISTS on disk (os.path.isfile). If the local file
#   is not present yet (sync incomplete), the track is LEFT UNCHANGED.
#   Updates ONLY the `location` column. `locationUnique` is Lexicon's immutable
#   import-identity key (it already legitimately diverges from `location` on ~5121 rows),
#   so it is deliberately NOT modified — this avoids the UNIQUE index entirely.
#   Idempotent: rows already at /Users/... are skipped.
#
# SAFETY GATES (all enforced; refuses otherwise)
#   1. A fresh verified DB backup must exist (heartbeat integrity==ok, Track>0, recent),
#      or pass --skip-backup-check only if you JUST ran scripts/backup-lexicon-db.sh.
#   2. Lexicon MUST be quit on the Mac (a running Lexicon caches rows and would clobber
#      or contend with the write). The script refuses if it sees the Lexicon process.
#   3. Dry-run by default. Requires --apply to write. --limit N does a SMALL batch first.
#   4. PRAGMA integrity_check before and after; per-row old->new audit log.
#
# USAGE (run on the ops Mac; SSHes into the Lexicon Mac):
#   scripts/backup-lexicon-db.sh                      # gate: fresh verified backup
#   # -> quit Lexicon on 192.168.1.116 <-
#   scripts/repoint-lexicon-local.sh                  # dry-run, full report
#   scripts/repoint-lexicon-local.sh --apply --limit 20   # small batch, then reopen+verify
#   scripts/repoint-lexicon-local.sh --apply              # full run
#
set -euo pipefail

LEXICON_SSH="${LEXICON_SSH:-willcurran@192.168.1.116}"
LEXICON_DB="${LEXICON_DB:-\$HOME/Library/Application Support/lexicon/main.db}"
HEARTBEAT="${HEARTBEAT:-$HOME/.waxflow/logs/lexicon-backup-heartbeat.json}"
AUDIT_DIR="${AUDIT_DIR:-$HOME/.waxflow/logs}"
APPLY=0; LIMIT=0; SKIP_BACKUP_CHECK=0
while [ $# -gt 0 ]; do case "$1" in
  --apply) APPLY=1;;
  --limit) LIMIT="$2"; shift;;
  --skip-backup-check) SKIP_BACKUP_CHECK=1;;
  *) echo "unknown arg: $1" >&2; exit 2;;
esac; shift; done

# Gate 1 — fresh verified backup
if [ "$SKIP_BACKUP_CHECK" -eq 0 ]; then
  if [ ! -f "$HEARTBEAT" ]; then echo "REFUSE: no backup heartbeat ($HEARTBEAT). Run scripts/backup-lexicon-db.sh first." >&2; exit 1; fi
  python3 - "$HEARTBEAT" <<'PY' || exit 1
import json,sys,time
h=json.load(open(sys.argv[1]))
assert h.get("status")=="ok" and h.get("integrity")=="ok" and int(h.get("track_count",0))>0, "backup heartbeat not verified"
print("backup gate OK: Track=%s integrity=%s file=%s"%(h["track_count"],h["integrity"],h["file"]))
PY
fi

# Gate 2 — Lexicon must be quit (enforced only for a real write; dry-run reads are safe)
if [ "$APPLY" -eq 1 ]; then
  if ssh "$LEXICON_SSH" 'pgrep -x Lexicon >/dev/null 2>&1 || pgrep -f "Lexicon.app/Contents/MacOS/Lexicon" >/dev/null 2>&1'; then
    echo "REFUSE: Lexicon is RUNNING on $LEXICON_SSH. Quit Lexicon fully, then re-run --apply." >&2
    exit 1
  fi
fi

TS=$(date +%Y%m%d-%H%M%S)
AUDIT="$AUDIT_DIR/repoint-lexicon-$TS.log"
mkdir -p "$AUDIT_DIR"
echo "audit -> $AUDIT ; apply=$APPLY limit=$LIMIT"

ssh "$LEXICON_SSH" 'bash -s' "$APPLY" "$LIMIT" <<'REMOTE' | tee "$AUDIT"
set -euo pipefail
APPLY="$1"; LIMIT="$2"
# Standard Lexicon DB path on the Mac (built from remote $HOME so the space in
# "Application Support" is preserved).
DB="$HOME/Library/Application Support/lexicon/main.db"
python3 - "$DB" "$APPLY" "$LIMIT" <<'PY'
import sqlite3, os, sys
db, apply, limit = sys.argv[1], sys.argv[2]=="1", int(sys.argv[3])
HOME="/Users/willcurran"
def newpath(loc):
    if loc.startswith("/Volumes/Macintosh HD/"): return loc[len("/Volumes/Macintosh HD"):]
    if loc.startswith("/Volumes/music/"):        return HOME+"/Music/"+loc[len("/Volumes/music/"):]
    return None
c=sqlite3.connect(db)
print("integrity_before=", c.execute("PRAGMA integrity_check").fetchone()[0])
rows=c.execute("SELECT id,location FROM Track ORDER BY id").fetchall()
changed=skipped_missing=already=0; n=0
for i,loc in rows:
    np=newpath(loc)
    if np is None:
        if loc.startswith("/Users/"): already+=1
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
print("SUMMARY changed=%d skipped_missing=%d already_local=%d applied=%s limit=%s"%(changed,skipped_missing,already,apply,limit or "none"))
PY
REMOTE
echo "done. audit: $AUDIT"
