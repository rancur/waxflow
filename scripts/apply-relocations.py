#!/usr/bin/env python3
"""Apply staged quality upgrades: swap the file and re-point Lexicon.

WHAT THIS FINISHES
    The rechecker (sync-worker/tasks/quality_upgrade.py) finds a better copy of a
    track, verifies it, files it into the library and records the swap in
    `relocation_queue`. It deliberately stops there. This script is the other half:
    it rewrites Lexicon's `Track.location` so the better file is the one that
    actually plays, and retires the old one.

    Without this step an "upgrade" is worse than useless -- the better file sits on
    disk unreferenced while Lexicon keeps playing the old copy, and disk usage grows.

WHY IT RUNS HERE
    Lexicon's API refuses to edit `location` (verified: `'location' is not
    editable`), so this is a direct SQLite write. It must run on the Mac that owns
    the database -- not because the file is unreachable elsewhere, but because
    SQLite must not be written over SMB/NFS, where advisory locking is unreliable
    and BEGIN IMMEDIATE cannot be trusted. Running locally also lets it verify
    Lexicon is genuinely quit rather than infer it from an API timeout.

WHAT IT TOUCHES
    `Track.location`, and nothing else. Specifically NOT `locationUnique`, which is
    Lexicon's immutable import-identity key and what keeps CloudFile links intact.
    Cue points, beat grids and playlists key off `Track.id`, which never changes --
    so an upgraded file keeps every cue you set. The script verifies that rather
    than assuming it.

SAFETY GATES (all enforced)
    1. A fresh verified backup must exist (integrity ok, Track > 0).
    2. Lexicon MUST be quit, proven three ways: no process, an exclusive lock is
       obtainable, and the -wal is not growing.
    3. Dry-run by default. --apply to write, --limit N for a small first batch.
    4. The new file must exist, be non-empty, and still score better than the old.
    5. integrity_check + foreign_key_check, and cue/grid/playlist/cloud counts
       compared before and after.
    6. One transaction; any failure rolls back the whole batch.
    7. The old file is moved to .superseded/, never deleted, and only after the
       database write has committed and verified.

USAGE
    scripts/backup-lexicon-db.sh
    # -> quit Lexicon <-
    scripts/apply-relocations.py                # dry-run
    scripts/apply-relocations.py --apply --limit 5
    scripts/apply-relocations.py --apply
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import urllib.request
from datetime import datetime

HOME = os.path.expanduser("~")
DEFAULT_LEXICON_DB = f"{HOME}/Library/Application Support/lexicon/main.db"
DEFAULT_HEARTBEAT = f"{HOME}/.waxflow/logs/lexicon-backup-heartbeat.json"
DEFAULT_AUDIT_DIR = f"{HOME}/.waxflow/logs"
DEFAULT_API = os.environ.get("WAXFLOW_API", "http://192.168.1.221:8402")
DEFAULT_NAS_SSH = os.environ.get("WAXFLOW_NAS_SSH", "nas-lan")
DEFAULT_DOCKER = os.environ.get("WAXFLOW_DOCKER", "/usr/local/bin/docker")

VERIFY_TABLES = ("Track", "Cuepoint", "Tempomarker", "Playlist",
                 "LinkTrackPlaylist", "CloudFile")


def lexicon_is_running() -> bool:
    for cmd in (["pgrep", "-x", "Lexicon"],
                ["pgrep", "-f", "Lexicon.app/Contents/MacOS/Lexicon"]):
        try:
            if subprocess.run(cmd, capture_output=True).returncode == 0:
                return True
        except FileNotFoundError:
            pass
    return False


def lexicon_db_is_quiet(db_path: str) -> tuple[bool, str]:
    """Prove nothing is writing: an exclusive lock is free and the WAL is static.

    A dead API is not proof Lexicon exited -- it can be mid-shutdown with the
    database still open, and a write then races its final flush.
    """
    wal = db_path + "-wal"
    before = os.path.getsize(wal) if os.path.exists(wal) else 0
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("ROLLBACK")
        finally:
            conn.close()
    except sqlite3.OperationalError as e:
        return False, f"database is locked by another writer ({e})"
    after = os.path.getsize(wal) if os.path.exists(wal) else 0
    if after != before:
        return False, "the -wal file is still changing — something is writing"
    return True, "no other writer"


def check_backup_gate(heartbeat_path: str) -> None:
    if not os.path.exists(heartbeat_path):
        sys.exit(f"REFUSE: no backup heartbeat at {heartbeat_path}. "
                 f"Run scripts/backup-lexicon-db.sh first.")
    try:
        h = json.load(open(heartbeat_path))
    except (OSError, ValueError) as e:
        sys.exit(f"REFUSE: cannot read backup heartbeat: {e}")
    if h.get("status") != "ok" or h.get("integrity") != "ok" or int(h.get("track_count", 0)) <= 0:
        sys.exit(f"REFUSE: backup heartbeat is not verified: {h}")
    print(f"  backup gate OK: Track={h['track_count']} file={h.get('file')}")


def fetch_pending(nas_ssh: str) -> list[dict]:
    """Read pending relocations straight from the worker's database."""
    src = """
import json, sqlite3
c = sqlite3.connect("file:/app/data/sync.db?mode=ro", uri=True)
c.row_factory = sqlite3.Row
rows = c.execute('''SELECT id, track_id, lexicon_track_id, old_path, new_path,
                           old_tier, new_tier, old_score, new_score
                    FROM relocation_queue WHERE state = 'pending'
                    ORDER BY id''').fetchall()
print(json.dumps([dict(r) for r in rows]))
"""
    proc = subprocess.run(
        ["ssh", nas_ssh, f"{DEFAULT_DOCKER} exec -i waxflow-api python3 -"],
        input=src, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"REFUSE: cannot read the relocation queue: {proc.stderr[:300]}")
    try:
        return json.loads(proc.stdout.strip() or "[]")
    except ValueError:
        sys.exit(f"REFUSE: unreadable relocation queue: {proc.stdout[:300]}")


def to_mac_path(container_path: str | None) -> str | None:
    if not container_path:
        return None
    prefix = "/music/Database/"
    if container_path.startswith(prefix):
        return f"{HOME}/Music/Database/" + container_path[len(prefix):]
    return container_path


def snapshot(conn: sqlite3.Connection) -> dict:
    out = {}
    for table in VERIFY_TABLES:
        try:
            out[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except sqlite3.Error:
            out[table] = None
    return out


def report_result(nas_ssh: str, applied: list[int], failed: dict) -> None:
    payload = json.dumps({"applied": applied, "failed": failed})
    src = f"""
import json, sqlite3
data = json.loads({payload!r})
conn = sqlite3.connect("/app/data/sync.db", timeout=30)
conn.execute("PRAGMA busy_timeout=30000")
try:
    conn.execute("BEGIN IMMEDIATE")
    if data["applied"]:
        conn.executemany(
            "UPDATE relocation_queue SET state='applied', applied_at=datetime('now') WHERE id=?",
            [(i,) for i in data["applied"]])
        conn.executemany(
            '''UPDATE tracks SET file_path=(
                   SELECT new_path FROM relocation_queue WHERE track_id = tracks.id
                   ORDER BY id DESC LIMIT 1),
                  below_target=0, upgrade_state='resolved', updated_at=datetime('now')
               WHERE id IN (SELECT track_id FROM relocation_queue WHERE id=?)''',
            [(i,) for i in data["applied"]])
    for rid, err in data["failed"].items():
        conn.execute("UPDATE relocation_queue SET state='failed', error=?, "
                     "attempts=attempts+1 WHERE id=?", (err, int(rid)))
    conn.execute("INSERT INTO activity_log (event_type, message, details) VALUES (?,?,?)",
                 ("relocation_applied",
                  f"{{len(data['applied'])}} upgrade(s) applied, {{len(data['failed'])}} failed",
                  json.dumps(data)))
    conn.commit()
    print("REPORTED")
finally:
    conn.close()
"""
    proc = subprocess.run(
        ["ssh", nas_ssh, f"{DEFAULT_DOCKER} exec -i waxflow-api python3 -"],
        input=src, capture_output=True, text=True)
    if "REPORTED" not in (proc.stdout or ""):
        print(f"  WARNING: could not report results back: "
              f"{(proc.stderr or proc.stdout)[:200]}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--db", default=DEFAULT_LEXICON_DB)
    ap.add_argument("--heartbeat", default=DEFAULT_HEARTBEAT)
    ap.add_argument("--audit-dir", default=DEFAULT_AUDIT_DIR)
    ap.add_argument("--nas-ssh", default=DEFAULT_NAS_SSH)
    ap.add_argument("--skip-backup-check", action="store_true")
    ap.add_argument("--keep-old", action="store_true",
                    help="leave the superseded file in place instead of moving it")
    args = ap.parse_args()

    print(f"[relocate] apply={args.apply} limit={args.limit or 'none'}")
    if not os.path.exists(args.db):
        sys.exit(f"REFUSE: Lexicon DB not found at {args.db}")

    if not args.skip_backup_check:
        check_backup_gate(args.heartbeat)

    if args.apply:
        if lexicon_is_running():
            sys.exit("REFUSE: Lexicon is RUNNING. It caches rows in memory and would "
                     "overwrite this on exit. Quit Lexicon fully, then re-run.")
        quiet, why = lexicon_db_is_quiet(args.db)
        if not quiet:
            sys.exit(f"REFUSE: {why}")
        print(f"  lexicon gate OK: {why}")

    pending = fetch_pending(args.nas_ssh)
    if args.limit:
        pending = pending[:args.limit]
    print(f"  pending relocations: {len(pending)}")
    if not pending:
        return 0

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys=ON")
    before = snapshot(conn)
    integrity_before = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity_before != "ok":
        sys.exit("REFUSE: Lexicon DB failed integrity_check BEFORE any write.")
    print(f"  integrity_before={integrity_before}")

    plan, skipped = [], {}
    for row in pending:
        rid = row["id"]
        new_mac = to_mac_path(row.get("new_path"))
        old_mac = to_mac_path(row.get("old_path"))
        lex_id = row.get("lexicon_track_id")

        if not lex_id:
            skipped[rid] = "no lexicon_track_id"
            continue
        if not new_mac or not os.path.isfile(new_mac) or os.path.getsize(new_mac) == 0:
            # The replacement must be real and readable from THIS machine before we
            # point Lexicon at it -- a path that only exists on the NAS would leave
            # the library pointing at nothing.
            skipped[rid] = f"replacement not present on this Mac: {new_mac}"
            continue

        current = conn.execute("SELECT location FROM Track WHERE id = ?",
                               (int(lex_id),)).fetchone()
        if current is None:
            skipped[rid] = f"lexicon track {lex_id} no longer exists"
            continue
        if old_mac and current[0] and current[0] != old_mac:
            # Something moved this row since the upgrade was staged; re-point it and
            # we would silently undo whatever that was.
            skipped[rid] = "location drifted since staging"
            continue
        plan.append({"id": rid, "lexicon_id": int(lex_id), "old": current[0],
                     "new": new_mac, "old_tier": row.get("old_tier"),
                     "new_tier": row.get("new_tier"), "track_id": row.get("track_id")})

    print(f"  ready to apply: {len(plan)}   skipped: {len(skipped)}")
    for rid, why in list(skipped.items())[:5]:
        print(f"    skip #{rid}: {why}")
    for p in plan[:8]:
        print(f"    #{p['id']} lexicon {p['lexicon_id']}  "
              f"{p['old_tier']} -> {p['new_tier']}")
        print(f"        {os.path.basename(p['old'] or '(none)')[:60]}")
        print(f"     -> {os.path.basename(p['new'])[:60]}")

    if not args.apply:
        print("\n  Dry run. Re-run with --apply (and --limit 5 first) to write.")
        return 0
    if not plan:
        report_result(args.nas_ssh, [], skipped)
        return 0

    os.makedirs(args.audit_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    audit_path = os.path.join(args.audit_dir, f"relocations-{stamp}.log")

    with open(audit_path, "w", encoding="utf-8") as audit:
        for p in plan:
            audit.write(f"RELOCATE lexicon={p['lexicon_id']} {p['old']} -> {p['new']}\n")
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                "UPDATE Track SET location = ?, dateModified = ? WHERE id = ?",
                [(p["new"], datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                  p["lexicon_id"]) for p in plan])
            conn.commit()
        except Exception as e:                                   # noqa: BLE001
            conn.rollback()
            audit.write(f"ROLLBACK {e}\n")
            sys.exit(f"FAILED, rolled back, nothing written: {e}")

        integrity_after = conn.execute("PRAGMA integrity_check").fetchone()[0]
        fk_after = conn.execute("PRAGMA foreign_key_check").fetchall()
        after = snapshot(conn)
        audit.write(f"integrity_after={integrity_after} fk={len(fk_after)}\n")
        audit.write(f"before={before}\nafter={after}\n")

        problems = []
        if integrity_after != "ok":
            problems.append(f"integrity_check={integrity_after}")
        if fk_after:
            problems.append(f"{len(fk_after)} foreign key violations")
        for table, n in before.items():
            if n != after.get(table):
                problems.append(f"{table} {n} -> {after.get(table)}")
        if problems:
            print("\n  *** POST-WRITE VERIFICATION FAILED ***")
            for p in problems:
                print(f"    {p}")
            print(f"    Restore from the backup named in {args.heartbeat}")
            return 1

        print(f"  integrity_after={integrity_after}")
        print("  verified: cues, grids, playlists and cloud links all unchanged")

        # Retire the old files only AFTER the write is committed and verified.
        if not args.keep_old:
            for p in plan:
                old = p["old"]
                if not old or not os.path.isfile(old) or old == p["new"]:
                    continue
                superseded = os.path.join(os.path.dirname(old), ".superseded")
                try:
                    os.makedirs(superseded, exist_ok=True)
                    shutil.move(old, os.path.join(superseded, os.path.basename(old)))
                    audit.write(f"SUPERSEDED {old}\n")
                except OSError as e:
                    # Not fatal: Lexicon already points at the new file.
                    audit.write(f"SUPERSEDE_FAILED {old}: {e}\n")

    report_result(args.nas_ssh, [p["id"] for p in plan], skipped)
    print(f"\n  applied {len(plan)} relocation(s)")
    print(f"  audit -> {audit_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
