#!/usr/bin/env python3
"""WaxFlow — consolidate stray artist folders from the NAS music share ROOT into Database/.

WHY
    The worker's _move_to_library() writes to MUSIC_LIBRARY_PATH == container /music,
    which was bind-mounted to the share ROOT (/volume1/music) rather than the actual
    library root (/volume1/music/Database). Result: 339 audio files across 189 artist
    folders live at the share root, outside the library Lexicon and Engine DJ read.

    This script moves them into Database/, preserving the relative layout so the
    Lexicon repoint is a pure prefix insertion:

        /Users/willcurran/Music/<Artist>/<rel>
     -> /Users/willcurran/Music/Database/<Artist>/<rel>

    Phase 3 then re-points the container mount at /volume1/music/Database, so
    tracks.file_path values of the form /music/<Artist>/<rel> keep resolving to the
    same bytes they resolved to before. The two changes MUST ship together.

TRANSPORT
    Runs on the Lexicon host Mac over the SMB mount. It cannot run over SSH: DSM
    gives the SSH user no write access to Database/ (it is owned by PlexMediaServer),
    while the SMB share grants it via ACL. Source and destination are the same share,
    so each move is a server-side rename — no data crosses the network.

COLLISIONS (never destructive)
    identical size  -> byte-compare; if truly identical the root copy is moved to
                       #waxflow-quarantine/ (NOT deleted) and logged
    different size  -> both kept; the incoming file gets a '__fromroot' suffix and is
                       flagged in the manifest for the Phase 6 dedupe pass

KEEP AT ROOT (never touched)
    Database, Input, DJ Will See, Disorganized, Engine Library, Engine Library Backup,
    Friends Recordings, Processing, rekordbox, SoundSwitch, Music, #recycle, @eaDir

USAGE
    ./consolidate-share-root.py                 # dry run, full report (default)
    ./consolidate-share-root.py --apply         # perform the moves
    ./consolidate-share-root.py --apply --limit 20
    ./consolidate-share-root.py --prune         # after --apply: remove now-empty root
                                                # artist folders (metadata-only leftovers)
"""
from __future__ import annotations

import argparse
import filecmp
import os
import sys
import unicodedata
from datetime import datetime, timezone

SHARE = os.environ.get("WAXFLOW_SHARE", "/Volumes/music")
LIBRARY = "Database"
QUARANTINE = "#waxflow-quarantine"
MANIFEST_DIR = os.path.expanduser("~/WaxFlow-Backups")

KEEP_AT_ROOT = {
    "Database", "Input", "DJ Will See", "Disorganized", "Engine Library",
    "Engine Library Backup", "Friends Recordings", "Processing", "rekordbox",
    "SoundSwitch", "Music", "#recycle", "@eaDir", QUARANTINE,
}
AUDIO_EXT = {".flac", ".m4a", ".mp3", ".wav", ".aiff", ".aif", ".ogg", ".alac", ".aac"}


def nfc(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def is_audio(name: str) -> bool:
    return os.path.splitext(name)[1].lower() in AUDIO_EXT


def collect(share: str):
    """Yield (artist, relpath) for every audio file in a non-keep root folder."""
    for entry in sorted(os.listdir(share)):
        if entry in KEEP_AT_ROOT or entry.startswith("."):
            continue
        folder = os.path.join(share, entry)
        if not os.path.isdir(folder):
            continue
        for dirpath, dirnames, filenames in os.walk(folder):
            dirnames[:] = [d for d in dirnames if d != "@eaDir"]
            for fn in sorted(filenames):
                if fn.startswith(".") or not is_audio(fn):
                    continue
                full = os.path.join(dirpath, fn)
                yield entry, os.path.relpath(full, share)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="perform moves (default: dry run)")
    ap.add_argument("--prune", action="store_true", help="remove now-empty root artist folders")
    ap.add_argument("--limit", type=int, default=0, help="only process the first N files")
    ap.add_argument("--share", default=SHARE)
    args = ap.parse_args()

    share = args.share
    if not os.path.isdir(os.path.join(share, LIBRARY)):
        print(f"REFUSE: {share}/{LIBRARY} not found — is the SMB share mounted?", file=sys.stderr)
        return 1

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    os.makedirs(MANIFEST_DIR, exist_ok=True)
    manifest_path = os.path.join(MANIFEST_DIR, f"consolidate-share-root-{stamp}.tsv")

    rows, n_clean, n_ident, n_diff, moved_bytes = [], 0, 0, 0, 0

    for artist, rel in collect(share):
        src = os.path.join(share, rel)
        dst_rel = os.path.join(LIBRARY, rel)
        dst = os.path.join(share, dst_rel)
        try:
            size = os.path.getsize(src)
        except OSError as e:
            print(f"  SKIP unreadable {rel}: {e}", file=sys.stderr)
            continue

        action = "move"
        if os.path.exists(dst):
            dsize = os.path.getsize(dst)
            if dsize == size and filecmp.cmp(src, dst, shallow=False):
                # true duplicate: quarantine the root copy rather than delete it
                dst_rel = os.path.join(QUARANTINE, rel)
                dst = os.path.join(share, dst_rel)
                action = "quarantine-identical"
                n_ident += 1
            else:
                base, ext = os.path.splitext(dst_rel)
                dst_rel = f"{base}__fromroot{ext}"
                dst = os.path.join(share, dst_rel)
                action = "keep-both-differs"
                n_diff += 1
        else:
            n_clean += 1
            moved_bytes += size

        rows.append((action, size, nfc(rel), nfc(dst_rel)))
        if args.limit and len(rows) >= args.limit:
            break

    print(f"share            : {share}")
    print(f"mode             : {'APPLY' if args.apply else 'DRY RUN'}")
    print(f"clean moves      : {n_clean}  ({moved_bytes / 1e9:.2f} GB)")
    print(f"identical dupes  : {n_ident}  -> {QUARANTINE}/ (kept, not deleted)")
    print(f"differing dupes  : {n_diff}  -> kept both, '__fromroot' suffix")
    print(f"total            : {len(rows)}")
    print(f"manifest         : {manifest_path}\n")

    if n_diff:
        print("Files kept side by side (review in the Phase 6 dedupe pass):")
        for a, s, r, d in rows:
            if a == "keep-both-differs":
                print(f"   {r}\n     -> {d}")
        print()

    errors = 0
    with open(manifest_path, "w", encoding="utf-8") as mf:
        mf.write("action\tbytes\tnas_old\tnas_new\tmac_old\tmac_new\n")
        for action, size, rel, dst_rel in rows:
            mac_old = f"/Users/willcurran/Music/{rel}"
            mac_new = f"/Users/willcurran/Music/{dst_rel}"
            mf.write(f"{action}\t{size}\t{rel}\t{dst_rel}\t{mac_old}\t{mac_new}\n")

            if not args.apply:
                continue
            src = os.path.join(share, rel)
            dst = os.path.join(share, dst_rel)
            try:
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                if os.path.exists(dst) and action == "move":
                    print(f"  SKIP (appeared since scan): {rel}", file=sys.stderr)
                    continue
                os.rename(src, dst)      # same share -> server-side rename
            except OSError as e:
                errors += 1
                print(f"  ERROR moving {rel}: {e}", file=sys.stderr)

    if args.apply:
        print(f"applied. errors={errors}")

    if args.prune:
        # Loop until stable: over SMB, os.listdir can return a stale snapshot, so a
        # single pass reliably leaves a tail of folders behind (observed: 189 found,
        # 156 removed, 33 left). rmtree is idempotent here, so just re-scan until a
        # pass removes nothing.
        pruned_total = 0
        for _ in range(10):
            n = prune_pass(share, args.apply)
            pruned_total += n
            if n == 0:
                break
        print(f"prune: {pruned_total} empty root artist folder(s) "
              f"{'removed' if args.apply else 'would be removed'}")

    return 1 if errors else 0


def prune_pass(share: str, apply: bool) -> int:
    """One prune sweep. Returns how many folders were (or would be) removed."""
    import shutil

    pruned = 0
    for entry in sorted(os.listdir(share)):
        if entry in KEEP_AT_ROOT or entry.startswith("."):
            continue
        folder = os.path.join(share, entry)
        if not os.path.isdir(folder):
            continue
        # only prune when nothing but Synology/macOS metadata remains
        leftover = []
        for dirpath, dirnames, filenames in os.walk(folder):
            if "@eaDir" in dirpath:
                continue
            leftover += [f for f in filenames if not f.startswith(".")]
        if leftover:
            print(f"  KEEP {entry} — still holds {len(leftover)} non-metadata file(s)")
            continue
        if apply:
            try:
                shutil.rmtree(folder)
                pruned += 1
            except OSError as e:
                print(f"  ERROR pruning {entry}: {e}", file=sys.stderr)
        else:
            pruned += 1
    return pruned


if __name__ == "__main__":
    sys.exit(main())
