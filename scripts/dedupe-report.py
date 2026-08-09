#!/usr/bin/env python3
"""WaxFlow — duplicate analysis across Lexicon, the library, and the pre-WaxFlow archive.

READ-ONLY. Produces a report and CSVs. Deletes nothing, moves nothing.

Three populations are reported separately, because conflating them is what makes
"how many duplicates do I have?" unanswerable:

  A. MULTI-ROW  — several Lexicon Track rows pointing at ONE file. Nothing to delete
                  on disk; the question is whether to merge the rows (cues/playlist
                  links live on the row, not the file).
  B. MULTI-FILE — one recording present as several DISTINCT files. These are the real
                  duplicates, and the ones where quality actually differs.
  C. ARCHIVE    — files under the pre-WaxFlow archive (Processing/, Disorganized/)
                  that match a Lexicon track. This is where Beatport/Bandcamp
                  purchases live, so it is where an UPGRADE is most likely: a bought
                  WAV/AIFF beating a Tidal FLAC, or a lossless original of something
                  WaxFlow only found lossy.

QUALITY RANKING (best first), applied within each group:
    genuinely lossless > higher sample rate > higher bit depth > higher bitrate >
    larger file > purchased-source path > earliest dateAdded
Lossy-from-lossless transcodes are NOT detected here — that needs a spectral check
(sync-worker/tasks/lossless_verify.py::spectral_cutoff) and is deliberately out of
scope for a read-only report.

USAGE
    ./dedupe-report.py                       # A + B (fast, metadata only)
    ./dedupe-report.py --archive             # also scan Processing/ + Disorganized/ (slow)
    ./dedupe-report.py --probe               # ffprobe every candidate for real codec info
    ./dedupe-report.py --out ~/WaxFlow-Backups
"""
from __future__ import annotations

import argparse
import collections
import csv
import os
import re
import sqlite3
import subprocess
import sys
import unicodedata

LEXICON_DB = os.path.expanduser("~/Library/Application Support/lexicon/main.db")
ARCHIVE_ROOTS = ["/Volumes/music/Processing", "/Volumes/music/Disorganized"]
AUDIO_EXT = {".flac", ".m4a", ".mp3", ".wav", ".aiff", ".aif", ".alac", ".aac", ".ogg"}
LOSSLESS_EXT = {".flac", ".wav", ".aiff", ".aif", ".alac"}
PURCHASE_HINTS = ("beatport", "bandcamp", "juno", "traxsource", "qobuz", "purchase", "bought")


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s or "")


def norm_key(artist: str, title: str) -> tuple[str, str]:
    """Loose match key: lowercase alphanumerics only, remix/edit suffixes retained.

    Retaining remix info matters — 'Punk' and 'Punk (Vocal Extended)' are different
    records and must not collapse into one another.
    """
    def clean(s: str) -> str:
        s = nfc(s).lower()
        s = re.sub(r"\b(feat|ft|featuring|with)\b.*", "", s)
        return re.sub(r"[^a-z0-9]", "", s)
    return clean(artist), clean(title)


TRACKNO_RE = re.compile(r"^\s*\d{1,4}\s*[.\-_)]\s+")
DATE_SUFFIX_RE = re.compile(r"\s+-\s+\d{1,2}\s+\d{4}\s*$")


def split_archive_stem(stem: str) -> tuple[str, str]:
    """Split an archive filename stem into (artist, title).

    The pre-WaxFlow folders are exports from playlist tools, so the dominant shape
    is "NNN. Artist - Title" — a naive split on the first ' - ' yields an artist of
    "1041. Tiesto", which matches nothing. Purchased files sometimes also carry a
    trailing purchase-date suffix ("... - 02 2025"). Strip both before splitting,
    or the whole archive scan silently reports ~0 matches.
    """
    s = TRACKNO_RE.sub("", stem)
    s = DATE_SUFFIX_RE.sub("", s)
    if " - " in s:
        a, t = s.split(" - ", 1)
        return a.strip(), t.strip()
    return "", s.strip()


def probe(path: str) -> dict:
    """ffprobe a file for real codec facts. Returns {} on any failure."""
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_streams",
             "-select_streams", "a:0", path],
            capture_output=True, text=True, timeout=30,
        )
        import json as _json
        st = _json.loads(out.stdout).get("streams", [{}])[0]
        return {
            "codec": st.get("codec_name", ""),
            "sample_rate": int(st.get("sample_rate") or 0),
            "bit_depth": int(st.get("bits_per_raw_sample") or 0),
            "bitrate": int(st.get("bit_rate") or 0),
            "duration": float(st.get("duration") or 0),
        }
    except Exception:
        return {}


MIX_TOKENS = (
    "remix", "vip", "v.i.p", "edit", "bootleg", "mashup", "rework", "flip",
    "extended", "radio", "club", "dub", "instrumental", "acoustic", "live",
    "original mix", "intro", "outro",
)


def mix_tokens(path: str) -> frozenset:
    """Remix/edit descriptors present in a filename OR its parent folder.

    Lexicon's `title` frequently keeps the original track name even when the file
    is a specific remix, so two rows can share a title while the files are
    genuinely different records. Comparing path descriptors catches that, and it
    is the difference between a real duplicate and a false positive.

    The parent folder matters as much as the filename: WaxFlow's layout puts the
    release in the directory, so the Maduk remix of "Stay" lands at
    ".../Stay (Maduk Remix)/Delta Heavy, Dirty Audio, Holly - Stay 4M88.flac" —
    the remix tag is in the FOLDER, not the file.
    """
    stem = os.path.splitext(os.path.basename(path))[0].lower()
    parent = os.path.basename(os.path.dirname(path)).lower()
    hay = f"{parent} {stem}"
    return frozenset(t for t in MIX_TOKENS if t in hay)


def archive_verdict(cand: dict, lex: dict) -> tuple[str, str]:
    """Compare an archive file against what Lexicon currently has.

    quality_key() must NOT be used here. Lexicon rows carry bitrate/sampleRate from
    Lexicon's own scan, while archive files carry none unless --probe ran, so a
    tuple compare makes every archive file lose on sample_rate=0 and reports ~0
    upgrades. Compare only on what is genuinely known for BOTH sides: container
    losslessness and file size.
    """
    a_ext = os.path.splitext(cand["path"])[1].lower()
    l_ext = os.path.splitext(lex["path"])[1].lower()
    a_lossless, l_lossless = a_ext in LOSSLESS_EXT, l_ext in LOSSLESS_EXT
    a_sz, l_sz = cand.get("size", 0), lex.get("size", 0) or 0

    if a_lossless and not l_lossless:
        return "UPGRADE", f"archive is lossless ({a_ext}), library is lossy ({l_ext})"
    if not a_lossless and l_lossless:
        return "no better", f"library already lossless ({l_ext})"
    # same class -> size is the only honest signal without probing
    if l_sz and a_sz > l_sz * 1.05:
        return "LARGER", f"{human(a_sz)} vs {human(l_sz)} (same class, {a_ext}/{l_ext})"
    if l_sz and a_sz < l_sz * 0.95:
        return "no better", f"smaller ({human(a_sz)} vs {human(l_sz)})"
    return "equivalent", f"~same size ({human(a_sz)} vs {human(l_sz)})"


def quality_key(rec: dict) -> tuple:
    """Sort key — higher is better. Mirrors the ranking documented above."""
    ext = os.path.splitext(rec["path"])[1].lower()
    lossless = 1 if ext in LOSSLESS_EXT else 0
    purchased = 1 if any(h in rec["path"].lower() for h in PURCHASE_HINTS) else 0
    return (
        lossless,
        rec.get("sample_rate", 0),
        rec.get("bit_depth", 0),
        rec.get("bitrate", 0),
        rec.get("size", 0),
        purchased,
    )


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", action="store_true", help="also scan Processing/ and Disorganized/")
    ap.add_argument("--probe", action="store_true", help="ffprobe candidates for real codec info")
    ap.add_argument("--out", default=os.path.expanduser("~/WaxFlow-Backups"))
    args = ap.parse_args()

    if not os.path.isfile(LEXICON_DB):
        print(f"REFUSE: Lexicon DB not found at {LEXICON_DB}", file=sys.stderr)
        return 1
    os.makedirs(args.out, exist_ok=True)

    conn = sqlite3.connect(f"file:{LEXICON_DB}?mode=ro", uri=True)
    tracks = [
        dict(id=r[0], title=r[1] or "", artist=r[2] or "", path=nfc(r[3] or ""),
             bitrate=int(r[4] or 0), sample_rate=int(r[5] or 0),
             duration=float(r[6] or 0), size=int(r[7] or 0), added=r[8] or "")
        for r in conn.execute(
            "SELECT id,title,artist,location,bitrate,sampleRate,duration,sizeBytes,dateAdded FROM Track"
        )
    ]
    print(f"Lexicon tracks: {len(tracks)}\n")

    # ---- A. several Lexicon rows -> one file --------------------------------
    by_path = collections.defaultdict(list)
    for t in tracks:
        if t["path"]:
            by_path[t["path"]].append(t)
    multi_row = {p: v for p, v in by_path.items() if len(v) > 1}
    extra_rows = sum(len(v) - 1 for v in multi_row.values())

    # ---- B. one recording -> several distinct files -------------------------
    by_key = collections.defaultdict(list)
    for t in tracks:
        by_key[norm_key(t["artist"], t["title"])].append(t)
    multi_file = {}
    for k, v in by_key.items():
        paths = {t["path"] for t in v if t["path"]}
        if len(paths) > 1:
            multi_file[k] = v

    # ---- C. archive candidates ---------------------------------------------
    archive_hits = collections.defaultdict(list)
    if args.archive:
        print("scanning archive roots (this walks ~100 GB over SMB)...")
        for root in ARCHIVE_ROOTS:
            if not os.path.isdir(root):
                print(f"  skip {root} (not mounted)")
                continue
            n = 0
            for dp, dn, fn in os.walk(root):
                dn[:] = [d for d in dn if d != "@eaDir"]
                for f in fn:
                    if f.startswith(".") or os.path.splitext(f)[1].lower() not in AUDIO_EXT:
                        continue
                    n += 1
                    a, t = split_archive_stem(os.path.splitext(f)[0])
                    key = norm_key(a, t)
                    if key in by_key and key != ("", ""):
                        full = os.path.join(dp, f)
                        try:
                            sz = os.path.getsize(full)
                        except OSError:
                            continue
                        archive_hits[key].append({"path": nfc(full), "size": sz})
            print(f"  {root}: {n} audio files scanned")

    # ---- optional real codec facts -----------------------------------------
    if args.probe:
        targets = [t for v in multi_file.values() for t in v]
        targets += [r for v in archive_hits.values() for r in v]
        print(f"\nffprobing {len(targets)} candidate files...")
        for i, rec in enumerate(targets, 1):
            if rec.get("path") and os.path.exists(rec["path"]):
                rec.update(probe(rec["path"]))
            if i % 50 == 0:
                print(f"  {i}/{len(targets)}")

    # ---- report -------------------------------------------------------------
    print("\n" + "=" * 72)
    print("A. MULTI-ROW — several Lexicon rows, ONE file (nothing to delete on disk)")
    print("=" * 72)
    print(f"  files with >1 row : {len(multi_row)}")
    print(f"  redundant rows    : {extra_rows}")

    print("\n" + "=" * 72)
    print("B. MULTI-FILE — one recording, SEVERAL distinct files (the real duplicates)")
    print("=" * 72)
    reclaim = 0
    rows_b = []
    true_dupes, mix_variants = [], []
    for k, v in sorted(multi_file.items()):
        uniq = {}
        for t in v:
            if t["path"]:
                uniq.setdefault(t["path"], t)
        ranked = sorted(uniq.values(), key=quality_key, reverse=True)
        # If the filenames carry DIFFERENT remix descriptors these are almost
        # certainly separate records that merely share a Lexicon title.
        token_sets = {mix_tokens(r["path"]) for r in ranked}
        differing_mix = len(token_sets) > 1
        (mix_variants if differing_mix else true_dupes).append((k, ranked))
        keep, drop = ranked[0], ranked[1:]
        if not differing_mix:
            reclaim += sum(d.get("size", 0) for d in drop)
        for r in ranked:
            rows_b.append({
                "group": f"{k[0]}|{k[1]}",
                "verdict": ("DIFFERENT MIX - review" if differing_mix
                            else ("KEEP" if r is keep else "candidate")),
                "artist": r["artist"], "title": r["title"], "path": r["path"],
                "ext": os.path.splitext(r["path"])[1], "bytes": r.get("size", 0),
                "bitrate": r.get("bitrate", 0), "sample_rate": r.get("sample_rate", 0),
                "codec": r.get("codec", ""), "added": r.get("added", ""),
                "mix_tokens": "|".join(sorted(mix_tokens(r["path"]))),
            })
    print(f"  groups sharing artist+title : {len(multi_file)}")
    print(f"    likely TRUE duplicates    : {len(true_dupes)}   reclaimable {human(reclaim)}")
    print(f"    different mixes (keep!)   : {len(mix_variants)}   <- filenames carry different remix tags")
    print("\n  --- likely true duplicates ---")
    for k, ranked in true_dupes[:15]:
        print(f"\n  {ranked[0]['artist']} — {ranked[0]['title']}")
        for i, r in enumerate(ranked):
            tag = "KEEP " if i == 0 else "  dup"
            print(f"    {tag}{os.path.splitext(r['path'])[1]:>5} {human(r.get('size',0)):>9} "
                  f"{r.get('sample_rate',0) or '?'}Hz {r.get('bitrate',0) or '?'}kbps  "
                  f"...{r['path'][-58:]}")

    rows_c = []
    if args.archive:
        print("\n" + "=" * 72)
        print("C. ARCHIVE — pre-WaxFlow files matching a Lexicon track (upgrade candidates)")
        print("=" * 72)
        verdict_counts = collections.Counter()
        for k, cands in sorted(archive_hits.items()):
            lex = sorted((t for t in by_key[k] if t["path"]), key=quality_key, reverse=True)
            if not lex:
                continue
            best_lex = lex[0]
            for c in cands:
                verdict, why = archive_verdict(c, best_lex)
                verdict_counts[verdict] += 1
                rows_c.append({
                    "group": f"{k[0]}|{k[1]}", "verdict": verdict, "reason": why,
                    "artist": best_lex["artist"], "title": best_lex["title"],
                    "archive_path": c["path"], "archive_bytes": c["size"],
                    "archive_ext": os.path.splitext(c["path"])[1],
                    "lexicon_path": best_lex["path"], "lexicon_bytes": best_lex.get("size", 0),
                    "lexicon_ext": os.path.splitext(best_lex["path"])[1],
                })
        print(f"  archive files matching a Lexicon track : {sum(len(v) for v in archive_hits.values())}")
        for v, n in verdict_counts.most_common():
            print(f"    {v:<12} {n}")
        for label in ("UPGRADE", "LARGER"):
            hits = [r for r in rows_c if r["verdict"] == label]
            if not hits:
                continue
            print(f"\n  --- {label} ({len(hits)}) ---")
            for r in hits[:10]:
                print(f"  {r['artist']} — {r['title']}")
                print(f"    have    {r['lexicon_ext']:>5} {human(r['lexicon_bytes']):>9}  ...{r['lexicon_path'][-54:]}")
                print(f"    archive {r['archive_ext']:>5} {human(r['archive_bytes']):>9}  ...{r['archive_path'][-54:]}")

    # ---- CSVs ---------------------------------------------------------------
    outs = []
    if rows_b:
        p = os.path.join(args.out, "dedupe-B-multifile.csv")
        with open(p, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows_b[0].keys())); w.writeheader(); w.writerows(rows_b)
        outs.append(p)
    if rows_c:
        p = os.path.join(args.out, "dedupe-C-archive-upgrades.csv")
        with open(p, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows_c[0].keys())); w.writeheader(); w.writerows(rows_c)
        outs.append(p)
    if multi_row:
        p = os.path.join(args.out, "dedupe-A-multirow.csv")
        with open(p, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh); w.writerow(["path", "row_count", "track_ids"])
            for path, v in sorted(multi_row.items()):
                w.writerow([path, len(v), ";".join(str(t["id"]) for t in v)])
        outs.append(p)

    print("\nwrote:")
    for p in outs:
        print("  ", p)
    print("\nNOTHING WAS DELETED OR MOVED. Review the CSVs before any action.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
