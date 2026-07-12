"""Plex / Plexamp mirror (WaxFlow v3 — Feature 4).

Mirrors what WaxFlow already syncs into Lexicon over to the Plex server that runs
ON the NAS (``http://192.168.1.221:32400``) and reads the SAME ``/volume1/music``
tree, so the monthly ``MM. Month YYYY`` playlists show up in Plexamp.

Three responsibilities, all idempotent:

  1. SCAN — when new files land, issue targeted PATH-SCOPED library refreshes
     (``PUT /library/sections/{id}/refresh?path=…``), one per unique parent
     directory, batched/debounced. NEVER a global full scan (storm risk).
  2. MATCH — map each WaxFlow track to its Plex ``ratingKey``, by file path
     first (deterministic — worker ``/music/…`` == Plex ``/volume1/music/…``),
     falling back to a normalized artist+title search. The ratingKey is cached
     in the ``plex_sync`` table so later runs skip the lookup.
  3. MIRROR — for every ``MM. Month YYYY`` monthly playlist, reconcile a Plex
     audio playlist of the same name so its membership EQUALS the monthly list
     (add missing, remove extras). Running twice makes no changes.

This module is a READ-ONLY consumer of audio files: it never moves, renames, or
writes a file. The only writes are to Plex's own playlist/scan state and to the
WaxFlow ``plex_sync`` cache table.

INERT by default: gated behind the ``plex_sync_enabled`` app_config flag
(default off) and NOT wired into ``worker.py``'s task loop — Phase C wires it in
during a quiet window.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import unicodedata

from tasks.helpers import get_config, get_db, log_activity
from tasks.plex_client import PlexClient

log = logging.getLogger("worker.plex_sync")

# A monthly playlist is named like "07. July 2026" (see process_pipeline
# _organize_track). Only these are mirrored into Plex.
MONTHLY_RE = re.compile(r"^\d{2}\. [A-Z][a-z]+ \d{4}$")

# Defaults for the container<->Plex path translation. The worker writes to
# container ``/music`` which is the NAS ``/volume1/music`` that Plex indexes.
DEFAULT_CONTAINER_PREFIX = "/music"
DEFAULT_SERVER_PREFIX = "/volume1/music"

# How many unique directories to scan per cycle (debounce/batch the scan storm).
DEFAULT_SCAN_BATCH = 25


# ------------------------------------------------------------------ config


class PlexConfig:
    """Resolved Plex config pulled from app_config (all values optional)."""

    def __init__(self, db_path: str):
        self.enabled = get_config(db_path, "plex_sync_enabled") == "1"
        self.token = get_config(db_path, "plex_token") or ""
        self.url = get_config(db_path, "plex_url") or ""
        self.section_id = get_config(db_path, "plex_music_section_id") or ""
        self.container_prefix = (
            get_config(db_path, "plex_music_container_prefix") or DEFAULT_CONTAINER_PREFIX
        )
        self.server_prefix = (
            get_config(db_path, "plex_music_server_prefix") or DEFAULT_SERVER_PREFIX
        )
        try:
            self.scan_batch = int(get_config(db_path, "plex_scan_batch") or DEFAULT_SCAN_BATCH)
        except (TypeError, ValueError):
            self.scan_batch = DEFAULT_SCAN_BATCH

    def usable(self) -> bool:
        return bool(self.token and self.url and self.section_id)


# ------------------------------------------------------------ path + fuzzy


def container_to_plex_path(file_path: str, container_prefix: str, server_prefix: str) -> str | None:
    """Translate a worker-container audio path into the path Plex indexes it by.

    ``/music/<rel>`` -> ``/volume1/music/<rel>``. Returns None when ``file_path``
    is not under the container prefix (e.g. a Lexicon-host ``/Volumes/…`` path),
    signalling the caller to fall back to fuzzy artist+title matching.
    """
    if not file_path:
        return None
    cp = container_prefix.rstrip("/")
    if file_path == cp or file_path.startswith(cp + "/"):
        rel = file_path[len(cp):].lstrip("/")
        return f"{server_prefix.rstrip('/')}/{rel}"
    return None


def _normalize(text: str | None) -> str:
    """Casefold + strip accents/punctuation/feat-noise for fuzzy comparison."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    # Drop featured-artist noise and bracketed qualifiers that Plex/Spotify differ on.
    text = re.sub(r"\b(feat|ft|featuring|with)\b.*", " ", text)
    text = re.sub(r"[\(\[].*?[\)\]]", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _fuzzy_pick(candidates: list[dict], artist: str | None, title: str | None) -> str | None:
    """Choose the Plex track whose normalized (artist,title) matches best.

    Requires the title to match exactly (normalized) and the artist to match or
    be a substring either way — conservative to avoid mis-mapping a wrong track.
    """
    want_a, want_t = _normalize(artist), _normalize(title)
    if not want_t:
        return None
    best = None
    for c in candidates:
        c_t = _normalize(c.get("title"))
        if c_t != want_t:
            continue
        c_a = _normalize(c.get("artist"))
        if want_a and c_a and not (want_a == c_a or want_a in c_a or c_a in want_a):
            continue
        # Exact title match (+ compatible artist) is good enough; first wins.
        best = c.get("rating_key")
        if c_a == want_a:
            break
    return best


# ------------------------------------------------------------- plex_sync IO


def _get_track_rating_key(db_path: str, track_id: int) -> str | None:
    with get_db(db_path) as conn:
        row = conn.execute(
            "SELECT rating_key FROM plex_sync WHERE track_id = ? AND rating_key IS NOT NULL "
            "AND playlist_id IS NULL ORDER BY id LIMIT 1",
            (track_id,),
        ).fetchone()
        return row["rating_key"] if row else None


def _upsert_track_mapping(db_path: str, track_id: int, rating_key: str) -> None:
    with get_db(db_path) as conn:
        row = conn.execute(
            "SELECT id FROM plex_sync WHERE track_id = ? AND playlist_id IS NULL", (track_id,)
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE plex_sync SET rating_key = ?, scan_state = 'matched', "
                "updated_at = datetime('now') WHERE id = ?",
                (rating_key, row["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO plex_sync (track_id, rating_key, scan_state) VALUES (?, ?, 'matched')",
                (track_id, rating_key),
            )


def _upsert_playlist_mapping(db_path: str, playlist_id: int, rating_key: str) -> None:
    with get_db(db_path) as conn:
        row = conn.execute(
            "SELECT id FROM plex_sync WHERE playlist_id = ? AND track_id IS NULL", (playlist_id,)
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE plex_sync SET rating_key = ?, updated_at = datetime('now') WHERE id = ?",
                (rating_key, row["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO plex_sync (playlist_id, rating_key, scan_state) VALUES (?, ?, 'mirrored')",
                (playlist_id, rating_key),
            )


# ------------------------------------------------------------------ stages


def _build_path_index(client: PlexClient, section_id: str) -> dict[str, str]:
    """One pass over the section: {plex_file_path: rating_key}."""
    index: dict[str, str] = {}
    for t in client.iter_section_tracks(section_id):
        if t.get("file"):
            index[t["file"]] = t["rating_key"]
    return index


def match_tracks(db_path: str, client: PlexClient, cfg: PlexConfig, limit: int = 500) -> dict:
    """Map WaxFlow tracks -> Plex ratingKeys (path first, fuzzy fallback).

    Only considers complete tracks with a file_path that are not already mapped.
    Returns ``{matched_path, matched_fuzzy, unmatched}`` counts.
    """
    with get_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT t.id, t.file_path, t.artist, t.title
            FROM tracks t
            WHERE t.file_path IS NOT NULL AND t.file_path != ''
              AND NOT EXISTS (
                SELECT 1 FROM plex_sync p
                WHERE p.track_id = t.id AND p.playlist_id IS NULL
                  AND p.rating_key IS NOT NULL
              )
            ORDER BY t.id LIMIT ?
            """,
            (limit,),
        ).fetchall()
    tracks = [dict(r) for r in rows]
    if not tracks:
        return {"matched_path": 0, "matched_fuzzy": 0, "unmatched": 0}

    index = _build_path_index(client, cfg.section_id)
    matched_path = matched_fuzzy = unmatched = 0
    for t in tracks:
        plex_path = container_to_plex_path(t["file_path"], cfg.container_prefix, cfg.server_prefix)
        rating_key = index.get(plex_path) if plex_path else None
        if rating_key:
            _upsert_track_mapping(db_path, t["id"], rating_key)
            matched_path += 1
            continue
        # Fuzzy fallback: search Plex by artist+title.
        query = " ".join(x for x in (t.get("artist"), t.get("title")) if x).strip()
        rating_key = None
        if query:
            candidates = client.search_tracks(cfg.section_id, query)
            rating_key = _fuzzy_pick(candidates, t.get("artist"), t.get("title"))
        if rating_key:
            _upsert_track_mapping(db_path, t["id"], rating_key)
            matched_fuzzy += 1
        else:
            unmatched += 1
    return {"matched_path": matched_path, "matched_fuzzy": matched_fuzzy, "unmatched": unmatched}


def scan_new_imports(db_path: str, client: PlexClient, cfg: PlexConfig) -> dict:
    """Issue PATH-SCOPED refreshes for directories holding newly-imported files.

    Targets tracks that have a container file_path but no Plex mapping yet.
    Batches by UNIQUE parent directory (dedup/debounce) and caps the batch so a
    burst of imports never fans out into a scan storm. Never a global refresh.
    Returns ``{scanned_dirs, skipped}``.
    """
    with get_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT t.id, t.file_path
            FROM tracks t
            WHERE t.file_path IS NOT NULL AND t.file_path != ''
              AND t.pipeline_stage = 'complete'
              AND NOT EXISTS (
                SELECT 1 FROM plex_sync p
                WHERE p.track_id = t.id AND p.playlist_id IS NULL
                  AND p.rating_key IS NOT NULL
              )
            ORDER BY t.updated_at DESC
            """,
        ).fetchall()

    # Collapse to unique Plex-side parent directories.
    dirs: list[str] = []
    seen: set[str] = set()
    for r in rows:
        plex_path = container_to_plex_path(r["file_path"], cfg.container_prefix, cfg.server_prefix)
        if not plex_path:
            continue
        parent = os.path.dirname(plex_path)
        if parent and parent not in seen:
            seen.add(parent)
            dirs.append(parent)

    to_scan = dirs[: cfg.scan_batch]
    scanned = 0
    for d in to_scan:
        if client.refresh_path(cfg.section_id, d):
            scanned += 1
    return {"scanned_dirs": scanned, "skipped": max(0, len(dirs) - len(to_scan))}


def _resolve_playlist_rating_keys(db_path: str, playlist_id: int) -> list[str]:
    """Ordered, de-duplicated Plex ratingKeys for a monthly playlist's members."""
    with get_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT p.rating_key
            FROM playlist_tracks pt
            JOIN plex_sync p ON p.track_id = pt.track_id
              AND p.playlist_id IS NULL AND p.rating_key IS NOT NULL
            WHERE pt.playlist_id = ?
            ORDER BY pt.position IS NULL, pt.position, pt.id
            """,
            (playlist_id,),
        ).fetchall()
    out: list[str] = []
    seen: set[str] = set()
    for r in rows:
        rk = r["rating_key"]
        if rk and rk not in seen:
            seen.add(rk)
            out.append(rk)
    return out


def _reconcile_playlist(
    client: PlexClient, title: str, desired_keys: list[str], existing_by_title: dict[str, str]
) -> tuple[str | None, dict]:
    """Make the Plex audio playlist ``title`` contain EXACTLY ``desired_keys``.

    Creates the playlist if missing, adds missing members, removes extras. A
    second call with the same desired set makes zero changes (idempotent).
    Returns ``(playlist_rating_key, {created, added, removed})``.
    """
    stats = {"created": 0, "added": 0, "removed": 0}
    if not desired_keys:
        return existing_by_title.get(title), stats

    pl_key = existing_by_title.get(title)
    if not pl_key:
        pl_key = client.create_audio_playlist(title, desired_keys)
        stats["created"] = 1
        return pl_key, stats

    current = client.playlist_items(pl_key)
    current_keys = {c["rating_key"] for c in current}
    desired_set = set(desired_keys)

    to_add = [k for k in desired_keys if k not in current_keys]
    if to_add:
        client.add_playlist_items(pl_key, to_add)
        stats["added"] = len(to_add)

    for c in current:
        if c["rating_key"] not in desired_set:
            if client.remove_playlist_item(pl_key, c["playlist_item_id"]):
                stats["removed"] += 1
    return pl_key, stats


def mirror_playlists(db_path: str, client: PlexClient, cfg: PlexConfig) -> dict:
    """Mirror ALL ``MM. Month YYYY`` monthly playlists into Plex audio playlists.

    Membership of each Plex playlist is reconciled to equal the WaxFlow monthly
    list. Fully idempotent. Returns aggregate ``{playlists, created, added, removed, empty}``.
    """
    with get_db(db_path) as conn:
        rows = conn.execute(
            "SELECT id, playlist_name FROM playlists ORDER BY year, month"
        ).fetchall()
    monthly = [(r["id"], r["playlist_name"]) for r in rows if MONTHLY_RE.match(r["playlist_name"] or "")]
    if not monthly:
        return {"playlists": 0, "created": 0, "added": 0, "removed": 0, "empty": 0}

    existing_by_title = {p["title"]: p["rating_key"] for p in client.list_audio_playlists()}

    agg = {"playlists": 0, "created": 0, "added": 0, "removed": 0, "empty": 0}
    for playlist_id, title in monthly:
        desired = _resolve_playlist_rating_keys(db_path, playlist_id)
        if not desired:
            agg["empty"] += 1
            continue
        pl_key, stats = _reconcile_playlist(client, title, desired, existing_by_title)
        if pl_key:
            _upsert_playlist_mapping(db_path, playlist_id, pl_key)
            existing_by_title[title] = pl_key
        agg["playlists"] += 1
        agg["created"] += stats["created"]
        agg["added"] += stats["added"]
        agg["removed"] += stats["removed"]
    return agg


# --------------------------------------------------------------- entrypoint


def _run_plex_sync(db_path: str) -> dict | None:
    """Synchronous body: scan -> match -> mirror. Returns aggregate stats or None
    when disabled/unconfigured. Safe to call repeatedly."""
    cfg = PlexConfig(db_path)
    if not cfg.enabled:
        return None
    if not cfg.usable():
        log.info("Plex sync enabled but not configured (missing token/url/section); skipping")
        return None

    client = PlexClient(cfg.url, cfg.token)
    try:
        scan = scan_new_imports(db_path, client, cfg)
        match = match_tracks(db_path, client, cfg)
        mirror = mirror_playlists(db_path, client, cfg)
    finally:
        client.close()

    result = {"scan": scan, "match": match, "mirror": mirror}
    if match["matched_path"] or match["matched_fuzzy"] or mirror["created"] or mirror["added"] or mirror["removed"]:
        log_activity(
            db_path, "plex_sync", None,
            f"Plex mirror: matched {match['matched_path']}+{match['matched_fuzzy']}, "
            f"playlists {mirror['playlists']} (+{mirror['created']} new, "
            f"+{mirror['added']}/-{mirror['removed']} members)",
            result,
        )
    log.info("Plex sync cycle: %s", result)
    return result


async def plex_sync(db_path: str) -> dict | None:
    """Async entry point for the Plex mirror task (INERT until wired in Phase C)."""
    return await asyncio.to_thread(_run_plex_sync, db_path)
