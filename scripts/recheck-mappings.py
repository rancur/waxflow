#!/usr/bin/env python3
"""Re-adjudicate Lexicon mappings that were made before the matching gates existed.

WHY
    252 Lexicon tracks are each claimed by more than one Spotify track. None are
    legitimate duplicates -- they have different Spotify IDs, different ISRCs, 249
    have different titles and 221 differ in duration by more than five seconds.
    Every one is a mis-match made by a matcher that compared title and artist and
    nothing else.

    2.13.0-2.15.1 closed those holes, but only for NEW matches. Nothing re-examines
    a mapping once it is made, so the existing wrong ones sit there looking
    complete. This applies the current gates retroactively.

WHAT IT DECIDES
    For each contested Lexicon row, every claimant is re-tested against the file
    Lexicon actually points at:

        duration gate  -- within `--tolerance` seconds (default 5)
        version gate   -- the title and the FILE must not name different cuts

    exactly one survivor  -> it keeps the mapping, the others are unmapped
    no survivors          -> all are unmapped; none of them is this file
    several survivors     -> left alone and reported. Guessing here would be the
                             same mistake that created the mess.

WHAT "UNMAP" MEANS
    Clearing `lexicon_track_id`, `file_path`, `match_source` and sending the track
    back to `pipeline_stage='new'`, so the pipeline re-resolves it with the gates
    active. It does NOT touch Lexicon's database, delete any audio file, or change
    anything in Lexicon itself -- only WaxFlow's opinion about which file is which.

USAGE
    scripts/recheck-mappings.py                     # dry-run report
    scripts/recheck-mappings.py --json out.json     # also write the decisions
    scripts/recheck-mappings.py --apply             # apply via the NAS container
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.request

HOME = os.path.expanduser("~")
DEFAULT_LEXICON_DB = f"{HOME}/Library/Application Support/lexicon/main.db"
DEFAULT_API = os.environ.get("WAXFLOW_API", "http://192.168.1.221:8402")
DEFAULT_NAS_SSH = os.environ.get("WAXFLOW_NAS_SSH", "nas-lan")
DEFAULT_DOCKER = os.environ.get("WAXFLOW_DOCKER", "/usr/local/bin/docker")

# Kept in step with sync-worker/tasks/process_pipeline.py::_VERSION_TOKENS.
VERSION_TOKENS = (
    "remix", "bootleg", "vip", "mashup", "rework", "flip", "edit",
    "extended", "radio", "dub", "instrumental", "acoustic", "live",
)


def version_tokens(text: str) -> frozenset:
    return frozenset(t for t in VERSION_TOKENS if t in (text or "").lower())


def extract_descriptor(title: str) -> str:
    if not title:
        return ""
    parts = re.findall(r"[\(\[]([^\)\]]+)[\)\]]", title)
    tail = re.search(r"\s+-\s+(.+)$", re.sub(r"[\(\[][^\)\]]*[\)\]]", "", title))
    if tail:
        parts.append(tail.group(1))
    return " ".join(parts)


def path_version_tokens(path: str) -> frozenset:
    if not path:
        return frozenset()
    stem = os.path.splitext(os.path.basename(path))[0]
    parent = os.path.basename(os.path.dirname(path))
    return version_tokens(f"{parent} {stem}")


def versions_conflict(title: str, path: str) -> bool:
    a, b = version_tokens(extract_descriptor(title)), path_version_tokens(path)
    if not a or not b:
        return False
    return not (a & b)


def durations_match(duration_ms, lexicon_seconds, tolerance: float) -> bool:
    if not duration_ms or not lexicon_seconds:
        return True                       # unknown on either side -> cannot judge
    return abs(duration_ms / 1000.0 - float(lexicon_seconds)) <= tolerance


def fetch_waxflow_tracks(api_base: str) -> list[dict]:
    tracks, page = [], 1
    while True:
        url = f"{api_base}/api/tracks?per_page=200&page={page}"
        with urllib.request.urlopen(url, timeout=60) as r:
            data = json.load(r)
        tracks.extend(data.get("tracks") or [])
        if page >= int(data.get("pages") or 1):
            return tracks
        page += 1


def as_int(value):
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def adjudicate(wf_tracks, lex_by_id, tolerance):
    groups = collections.defaultdict(list)
    for t in wf_tracks:
        lid = as_int(t.get("lexicon_track_id"))
        if lid is not None:
            groups[lid].append(t)

    decisions = {"unmap": [], "keep": [], "ambiguous": [], "orphaned": []}
    for lid, claimants in groups.items():
        lex = lex_by_id.get(lid)
        if lex is None:
            # WaxFlow points at a Lexicon row that no longer exists.
            decisions["orphaned"].extend(
                {"track_id": c["id"], "title": c.get("title"), "lexicon_track_id": lid}
                for c in claimants)
            continue
        if len(claimants) == 1:
            continue                       # uncontested; leave it entirely alone

        _lex_title, lex_duration, lex_location = lex
        survivors, rejected = [], []
        for c in claimants:
            dur_ok = durations_match(c.get("duration_ms"), lex_duration, tolerance)
            ver_ok = not versions_conflict(c.get("title") or "", lex_location or "")
            (survivors if (dur_ok and ver_ok) else rejected).append(
                {"track_id": c["id"], "title": c.get("title"),
                 "duration_ms": c.get("duration_ms"), "match_source": c.get("match_source"),
                 "duration_ok": dur_ok, "version_ok": ver_ok})

        entry = {"lexicon_track_id": lid, "lexicon_title": _lex_title,
                 "lexicon_duration": lex_duration, "lexicon_location": lex_location,
                 "survivors": survivors, "rejected": rejected}
        if len(survivors) == 1:
            decisions["keep"].append(entry)
            decisions["unmap"].extend(r["track_id"] for r in rejected)
        elif not survivors:
            decisions["ambiguous"].append({**entry, "reason": "no claimant fits this file"})
            decisions["unmap"].extend(r["track_id"] for r in rejected)
        else:
            decisions["ambiguous"].append({**entry, "reason": "several claimants fit"})
    return decisions


# Piped to `python3 -` inside the API container. The ids are baked in as a literal
# rather than sent on stdin, because stdin is where the interpreter reads the
# SCRIPT from -- it cannot carry both.
APPLY_SRC = r"""
import json, sqlite3
ids = __IDS__
conn = sqlite3.connect("/app/data/sync.db", timeout=30)
conn.execute("PRAGMA busy_timeout=30000")
try:
    conn.execute("BEGIN IMMEDIATE")
    conn.executemany(
        '''UPDATE tracks SET
               lexicon_track_id = NULL,
               file_path        = NULL,
               match_source     = NULL,
               match_status     = 'pending',
               match_confidence = NULL,
               download_status  = 'pending',
               download_source  = NULL,
               download_attempts = 0,
               verify_status    = 'pending',
               lexicon_status   = 'pending',
               pipeline_stage   = 'new',
               pipeline_error   = NULL,
               updated_at       = datetime('now')
           WHERE id = ?''', [(i,) for i in ids])
    conn.executemany("DELETE FROM fallback_attempts WHERE track_id = ?", [(i,) for i in ids])
    conn.executemany("DELETE FROM source_attempts   WHERE track_id = ?", [(i,) for i in ids])
    conn.execute(
        "INSERT INTO activity_log (event_type, message, details) VALUES (?,?,?)",
        ("mapping_recheck",
         f"{len(ids)} mis-mapped track(s) unmapped for re-resolution",
         json.dumps({"track_ids": ids[:500], "count": len(ids)})))
    conn.commit()
    print(f"APPLIED {len(ids)}")
except Exception as e:
    conn.rollback()
    print(f"ROLLED BACK: {e}")
    raise
finally:
    conn.close()
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--tolerance", type=float, default=5.0)
    ap.add_argument("--lexicon-db", default=DEFAULT_LEXICON_DB)
    ap.add_argument("--api", default=DEFAULT_API)
    ap.add_argument("--nas-ssh", default=DEFAULT_NAS_SSH)
    ap.add_argument("--json", dest="json_out")
    ap.add_argument("--limit", type=int, default=0, help="cap how many are unmapped")
    args = ap.parse_args()

    if not os.path.exists(args.lexicon_db):
        sys.exit(f"REFUSE: Lexicon DB not found at {args.lexicon_db}")

    lex = sqlite3.connect(f"file:{args.lexicon_db}?mode=ro", uri=True)
    lex_by_id = {r[0]: (r[1], r[2], r[3])
                 for r in lex.execute("SELECT id, title, duration, location FROM Track")}
    wf = fetch_waxflow_tracks(args.api)
    print(f"  waxflow tracks: {len(wf)}   lexicon tracks: {len(lex_by_id)}")

    d = adjudicate(wf, lex_by_id, args.tolerance)
    if args.limit:
        d["unmap"] = d["unmap"][:args.limit]

    print(f"\n  contested rows resolved to one claimant : {len(d['keep'])}")
    print(f"  rows left alone (ambiguous)             : {len(d['ambiguous'])}")
    print(f"  waxflow tracks to unmap and re-resolve  : {len(d['unmap'])}")
    print(f"  mappings pointing at a missing row      : {len(d['orphaned'])}")

    print("\n  examples of what would be unmapped:")
    for entry in d["keep"][:5]:
        print(f"    lexicon {entry['lexicon_track_id']}  "
              f"{(entry['lexicon_title'] or '')[:40]}  ({entry['lexicon_duration'] and round(entry['lexicon_duration'])}s)")
        for s in entry["survivors"]:
            print(f"       KEEP   wf#{s['track_id']:<5} {(s['title'] or '')[:44]}")
        for r in entry["rejected"]:
            why = "duration" if not r["duration_ok"] else "version"
            print(f"       unmap  wf#{r['track_id']:<5} {(r['title'] or '')[:44]}  [{why}]")

    if args.json_out:
        with open(args.json_out, "w") as fh:
            json.dump(d, fh, indent=1)
        print(f"\n  decisions -> {args.json_out}")

    if not args.apply:
        print("\n  Dry run. Re-run with --apply to unmap and re-queue.")
        return 0

    if not d["unmap"]:
        print("\n  nothing to do")
        return 0

    print(f"\n  applying: unmapping {len(d['unmap'])} tracks on the NAS...")
    src = APPLY_SRC.replace("__IDS__", json.dumps(d["unmap"]))
    proc = subprocess.run(
        ["ssh", args.nas_ssh, f"{DEFAULT_DOCKER} exec -i waxflow-api python3 -"],
        input=src, capture_output=True, text=True)
    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    print(f"  {out or err[:500]}")
    if "APPLIED" not in out:
        print("  *** apply did NOT confirm success ***")
        return 1
    print("\n  Unmapped tracks re-enter the pipeline within ~10s and will re-resolve\n"
          "  with the duration and version gates active.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
