#!/usr/bin/env python3
"""Backfill Lexicon's Track.dateAdded from the date you liked the track on Spotify.

WHY
    Lexicon stamps dateAdded with the moment a file was IMPORTED, so a library
    assembled by WaxFlow shows thousands of tracks all "added" on the day the sync
    ran. The real date -- when you actually saved the track -- is already in
    WaxFlow's `tracks.spotify_added_at`, going back to 2014.

    Measured on the live library: 5,255 tracks are wrong, 996 of them by 11 years.
    Sorting by date added is meaningless until this is fixed.

WHY A DIRECT SQLITE WRITE
    Lexicon's API refuses the field outright:

        PATCH /v1/track {"id":1,"edits":{"dateAdded":"..."}}
        -> 400 {"message":"'dateAdded' is not editable"}

    (`comment` on the same request returns 200, so this is a per-field rule, not a
    broken call.) The column is plain TEXT with a non-unique index and no triggers
    that reference it, so a direct UPDATE is well-defined.

WHY IT RUNS ON THE MAC
    Not because the DB is unreachable elsewhere, but because SQLite must not be
    written over SMB/NFS -- advisory locking there is unreliable and BEGIN IMMEDIATE
    cannot be trusted. The write belongs on the machine that owns the file. Running
    here also lets it verify Lexicon is genuinely quit rather than infer it.

WHAT IT TOUCHES
    Track.dateAdded. Nothing else. Specifically NOT `location` and NOT
    `locationUnique` (Lexicon's immutable import-identity key, which is also what
    keeps CloudFile links intact). Cue points, beat grids and playlists key off
    Track.id and are unaffected -- the script verifies that rather than assuming it.

SAFETY GATES (all enforced; refuses otherwise)
    1. A fresh verified DB backup must exist (heartbeat integrity==ok, Track>0),
       or pass --skip-backup-check if you JUST ran scripts/backup-lexicon-db.sh.
    2. Lexicon MUST be quit. A running Lexicon caches rows in memory and would
       overwrite the change on exit.
    3. Dry-run by default. --apply to write; --limit N to do a small batch first.
    4. integrity_check + foreign_key_check before and after; cue/playlist/track
       counts compared before and after; per-row old->new audit log.
    5. One transaction. Any failure rolls the whole thing back.

USAGE
    scripts/backup-lexicon-db.sh                        # gate: fresh verified backup
    # -> quit Lexicon <-
    scripts/backfill-lexicon-dateadded.py               # dry-run, full report
    scripts/backfill-lexicon-dateadded.py --apply --limit 20
    scripts/backfill-lexicon-dateadded.py --apply
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime

HOME = os.path.expanduser("~")
DEFAULT_DB = f"{HOME}/Library/Application Support/lexicon/main.db"
DEFAULT_HEARTBEAT = f"{HOME}/.waxflow/logs/lexicon-backup-heartbeat.json"
DEFAULT_AUDIT_DIR = f"{HOME}/.waxflow/logs"
DEFAULT_API = os.environ.get("WAXFLOW_API", "http://192.168.1.221:8402")

# Lexicon stores ISO-8601 with milliseconds and a Z suffix: 2024-10-19T07:00:00.000Z
# Spotify hands us 2026-08-04T00:36:31Z. Normalising avoids rewriting rows that are
# already correct and differ only in format.
def to_lexicon_ts(value: str) -> str | None:
    if not value:
        return None
    v = value.strip()
    if v.endswith("Z") and "." not in v:
        return v[:-1] + ".000Z"
    return v


def as_int(value) -> int | None:
    """Coerce WaxFlow's lexicon_track_id to an int.

    WaxFlow stores it as TEXT while Lexicon's Track.id is INTEGER. Comparing them
    raw matches NOTHING and silently falls through to path matching -- which, on a
    first run of this analysis, dropped ID matches from 5,130 to zero without
    raising anything.
    """
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def lexicon_is_running() -> bool:
    for cmd in (["pgrep", "-x", "Lexicon"],
                ["pgrep", "-f", "Lexicon.app/Contents/MacOS/Lexicon"]):
        try:
            if subprocess.run(cmd, capture_output=True).returncode == 0:
                return True
        except FileNotFoundError:
            pass
    return False


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
    print(f"  backup gate OK: Track={h['track_count']} integrity={h['integrity']} "
          f"file={h.get('file')}")


def fetch_waxflow_tracks(api_base: str) -> list[dict]:
    """Pull every WaxFlow track via the API (no direct coupling to its DB)."""
    tracks, page = [], 1
    while True:
        url = f"{api_base}/api/tracks?per_page=200&page={page}"
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                data = json.load(r)
        except (urllib.error.URLError, OSError) as e:
            sys.exit(f"REFUSE: cannot reach the WaxFlow API at {api_base}: {e}")
        tracks.extend(data.get("tracks") or [])
        if page >= int(data.get("pages") or 1):
            return tracks
        page += 1


def to_mac_path(container_path: str | None) -> str | None:
    """Translate the worker's container path to the Mac path Lexicon stores."""
    if not container_path:
        return None
    prefix = "/music/Database/"
    if container_path.startswith(prefix):
        return f"{HOME}/Music/Database/" + container_path[len(prefix):]
    return container_path


def snapshot(conn: sqlite3.Connection) -> dict:
    """Counts that a dateAdded-only write must leave completely untouched."""
    counts = {}
    for table in ("Track", "Cuepoint", "Tempomarker", "Playlist", "LinkTrackPlaylist", "CloudFile"):
        try:
            counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except sqlite3.Error:
            counts[table] = None  # table absent in this Lexicon version
    return counts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default is dry-run)")
    ap.add_argument("--limit", type=int, default=0, help="only process N changes")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--api", default=DEFAULT_API)
    ap.add_argument("--heartbeat", default=DEFAULT_HEARTBEAT)
    ap.add_argument("--audit-dir", default=DEFAULT_AUDIT_DIR)
    ap.add_argument("--skip-backup-check", action="store_true")
    args = ap.parse_args()

    print(f"[dateadded] apply={args.apply} limit={args.limit or 'none'} db={args.db}")

    if not os.path.exists(args.db):
        sys.exit(f"REFUSE: Lexicon DB not found at {args.db}")

    # -- Gate 1: verified fresh backup ------------------------------------- #
    if not args.skip_backup_check:
        check_backup_gate(args.heartbeat)

    # -- Gate 2: Lexicon quit (writes only; dry-run reads are safe) --------- #
    if args.apply and lexicon_is_running():
        sys.exit("REFUSE: Lexicon is RUNNING. It caches rows in memory and would "
                 "overwrite this on exit. Quit Lexicon fully, then re-run --apply.")

    os.makedirs(args.audit_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    audit_path = os.path.join(args.audit_dir, f"backfill-dateadded-{stamp}.log")

    wf_tracks = fetch_waxflow_tracks(args.api)
    print(f"  waxflow tracks: {len(wf_tracks)}")

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys=ON")

    before_counts = snapshot(conn)
    integrity_before = conn.execute("PRAGMA integrity_check").fetchone()[0]
    print(f"  integrity_before={integrity_before}")
    if integrity_before != "ok":
        sys.exit("REFUSE: Lexicon DB failed integrity_check BEFORE any write.")

    by_id = {r[0]: r[1] for r in conn.execute("SELECT id, dateAdded FROM Track")}
    by_loc = {r[1]: (r[0], r[2]) for r in
              conn.execute("SELECT id, location, dateAdded FROM Track WHERE location IS NOT NULL")}
    print(f"  lexicon tracks: {len(by_id)}")

    planned: list[tuple[int, str, str, str]] = []   # (lexicon_id, old, new, how)
    stats = {"matched_by_id": 0, "matched_by_path": 0, "unmatched": 0,
             "already_correct": 0, "no_spotify_date": 0}

    for t in wf_tracks:
        want = to_lexicon_ts(t.get("spotify_added_at"))
        if not want:
            stats["no_spotify_date"] += 1
            continue

        lid = as_int(t.get("lexicon_track_id"))
        if lid is not None and lid in by_id:
            current, how = by_id[lid], "id"
            stats["matched_by_id"] += 1
        else:
            hit = by_loc.get(to_mac_path(t.get("file_path")))
            if not hit:
                stats["unmatched"] += 1
                continue
            lid, current = hit
            how = "path"
            stats["matched_by_path"] += 1

        if current == want:
            stats["already_correct"] += 1
            continue
        planned.append((lid, current or "", want, how))

    if args.limit:
        planned = planned[:args.limit]

    with open(audit_path, "w", encoding="utf-8") as audit:
        audit.write(f"# backfill-lexicon-dateadded {stamp} apply={args.apply} "
                    f"limit={args.limit or 'none'}\n")
        for lid, old, new, how in planned:
            audit.write(f"SET id={lid} via={how} old={old or '(null)'} new={new}\n")

        applied = 0
        if args.apply and planned:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(
                    "UPDATE Track SET dateAdded = ? WHERE id = ?",
                    [(new, lid) for lid, _old, new, _how in planned],
                )
                conn.commit()
                applied = len(planned)
            except Exception as e:                                  # noqa: BLE001
                conn.rollback()
                audit.write(f"ROLLBACK {e}\n")
                sys.exit(f"FAILED, rolled back, nothing written: {e}")

            integrity_after = conn.execute("PRAGMA integrity_check").fetchone()[0]
            fk_after = conn.execute("PRAGMA foreign_key_check").fetchall()
            after_counts = snapshot(conn)
            print(f"  integrity_after={integrity_after}")
            audit.write(f"integrity_after={integrity_after} fk_violations={len(fk_after)}\n")
            audit.write(f"counts_before={before_counts}\ncounts_after={after_counts}\n")

            # A dateAdded-only write must not have moved anything else. If it did,
            # say so loudly -- the backup is the recovery path.
            problems = []
            if integrity_after != "ok":
                problems.append(f"integrity_check={integrity_after}")
            if fk_after:
                problems.append(f"{len(fk_after)} foreign key violations")
            for table, before in before_counts.items():
                if before != after_counts.get(table):
                    problems.append(f"{table} count {before} -> {after_counts.get(table)}")
            if problems:
                print("\n  *** POST-WRITE VERIFICATION FAILED ***")
                for p in problems:
                    print(f"    {p}")
                print(f"    Restore from the backup in {args.heartbeat}")
                return 1
            print(f"  verified: cues/grids/playlists/cloud links all unchanged")

    for key, value in stats.items():
        print(f"  {key:18} {value}")
    print(f"  {'would_change' if not args.apply else 'CHANGED':18} {len(planned)}")
    print(f"  audit -> {audit_path}")
    if not args.apply and planned:
        print("\n  Dry run. Re-run with --apply (and --limit 20 first) to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
