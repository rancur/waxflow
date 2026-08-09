#!/usr/bin/env python3
"""Merge Lexicon Track rows that point at the SAME file.

WHY
    Engine DJ's Track table declares `CONSTRAINT C_path UNIQUE (path)`, so it can
    hold at most one row per file. Lexicon had 5,852 rows covering only 5,611
    distinct files — 241 redundant rows that can NEVER sync to Engine. (The proof:
    the last good Engine export contained exactly 5,611 tracks.) Those extra rows
    are also a plausible contributor to the "SqliteError: FOREIGN KEY constraint
    failed" that Lexicon->Engine sync now fails with.

WHY NOT JUST DELETE THE EXTRAS
    Because both rows carry real data. Measured on this library: in 239 of the 241
    groups the row we would drop holds playlist memberships the surviving row does
    NOT have — 849 memberships that a naive DELETE would silently destroy. So we
    MIGRATE first, then delete.

WHAT IT DOES (per group of rows sharing one `location`)
    1. Pick the KEEP row: most playlist links + cuepoints, tie-broken by row id.
    2. Re-point every LinkTrackPlaylist row of the other rows onto the KEEP row,
       skipping playlists the KEEP row already belongs to (no duplicate entries in
       a playlist), preserving `position`.
    3. DELETE the redundant Track rows. Every child table (Cuepoint, Tempomarker,
       LinkTrackPlaylist, LinkTagTrack, CloudFile, AlbumartPreview, Waveform)
       declares ON DELETE CASCADE from Track, so their leftovers go with them.
       PRAGMA foreign_keys=ON is set explicitly — SQLite defaults it OFF, and
       without it the cascade silently does not happen.

    Cuepoints/tempo markers/waveforms are NOT merged: the surviving row is chosen
    for having the richest set, and merging cue positions between two analyses
    would produce nonsense.

SAFETY
    * Refuses to run while Lexicon is open (it caches rows and would clobber us).
    * Dry run by default; --apply to write; --limit N for a small first batch.
    * integrity_check AND foreign_key_check before and after.
    * Verifies afterwards that no playlist membership was lost.

USAGE
    ./merge-duplicate-lexicon-rows.py                 # dry run + full report
    ./merge-duplicate-lexicon-rows.py --apply --limit 10
    ./merge-duplicate-lexicon-rows.py --apply
"""
from __future__ import annotations

import argparse
import collections
import os
import sqlite3
import subprocess
import sys
import unicodedata

DB = os.path.expanduser("~/Library/Application Support/lexicon/main.db")


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s or "")


def lexicon_running() -> bool:
    r = subprocess.run(
        ["pgrep", "-f", "Lexicon.app/Contents/MacOS/Lexicon"],
        capture_output=True, text=True,
    )
    return r.returncode == 0 and bool(r.stdout.strip())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--db", default=DB)
    args = ap.parse_args()

    if not os.path.isfile(args.db):
        print(f"REFUSE: no Lexicon DB at {args.db}", file=sys.stderr)
        return 1
    if args.apply and lexicon_running():
        print("REFUSE: Lexicon is running. Quit it fully, then re-run --apply.", file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.db, timeout=30)
    conn.execute("PRAGMA foreign_keys=ON")   # REQUIRED: cascades are off by default

    print("integrity_before :", conn.execute("PRAGMA integrity_check").fetchone()[0])
    print("fk_check_before  :", len(list(conn.execute("PRAGMA foreign_key_check"))), "violation(s)")

    groups: dict[str, list[int]] = collections.defaultdict(list)
    for tid, loc in conn.execute("SELECT id, location FROM Track"):
        if loc:
            groups[nfc(loc)].append(tid)
    dupes = {k: v for k, v in groups.items() if len(v) > 1}

    def playlists(tid: int) -> dict[int, int]:
        return {p: pos for p, pos in
                conn.execute("SELECT playlistId, position FROM LinkTrackPlaylist WHERE trackId=?", (tid,))}

    def cues(tid: int) -> int:
        return conn.execute("SELECT COUNT(*) FROM Cuepoint WHERE trackId=?", (tid,)).fetchone()[0]

    # Snapshot EVERY (playlist, file) membership in the library, not just the ones
    # inside duplicate groups. Scoping this to `dupes` produced a spurious
    # "LOST 15" on a run where the full-library comparison proved nothing was lost
    # — a verification that cries wolf is worse than none, because the next real
    # loss gets waved through.
    def all_memberships(c) -> set:
        return {(pid, nfc(loc)) for pid, loc in c.execute(
            "SELECT l.playlistId, t.location FROM LinkTrackPlaylist l "
            "JOIN Track t ON t.id = l.trackId WHERE t.location IS NOT NULL")}

    before_pairs = all_memberships(conn)

    migrated = removed = skipped_existing = 0
    processed = 0
    for loc, ids in sorted(dupes.items()):
        ranked = sorted(ids, key=lambda t: (len(playlists(t)) + cues(t), t), reverse=True)
        keep, drops = ranked[0], ranked[1:]
        keep_pls = set(playlists(keep))
        for d in drops:
            for pid, pos in playlists(d).items():
                if pid in keep_pls:
                    skipped_existing += 1
                    continue
                if args.apply:
                    conn.execute(
                        "UPDATE LinkTrackPlaylist SET trackId=? WHERE trackId=? AND playlistId=?",
                        (keep, d, pid),
                    )
                keep_pls.add(pid)
                migrated += 1
            if args.apply:
                conn.execute("DELETE FROM Track WHERE id=?", (d,))
            removed += 1
        processed += 1
        if args.limit and processed >= args.limit:
            break

    if args.apply:
        conn.commit()

    print()
    print(f"duplicate groups           : {len(dupes)}")
    print(f"groups processed           : {processed}")
    print(f"playlist links migrated    : {migrated}")
    print(f"links already on keep row  : {skipped_existing}")
    print(f"redundant Track rows {'removed' if args.apply else 'to remove'} : {removed}")
    print(f"mode                       : {'APPLY' if args.apply else 'DRY RUN'}")

    print()
    print("integrity_after  :", conn.execute("PRAGMA integrity_check").fetchone()[0])
    print("fk_check_after   :", len(list(conn.execute("PRAGMA foreign_key_check"))), "violation(s)")
    total = conn.execute("SELECT COUNT(*) FROM Track").fetchone()[0]
    distinct = conn.execute("SELECT COUNT(DISTINCT location) FROM Track WHERE location IS NOT NULL").fetchone()[0]
    print(f"Track rows       : {total}   distinct locations: {distinct}")

    if args.apply:
        after_pairs = all_memberships(conn)
        lost = before_pairs - after_pairs
        print(f"playlist memberships before {len(before_pairs)} / after {len(after_pairs)} / LOST {len(lost)}")
        if lost:
            print("  WARNING: memberships lost:", list(lost)[:5], file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
