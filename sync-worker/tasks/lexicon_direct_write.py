"""Direct Lexicon SQLite writer for *link-only* tracks (Phase 2 — kill the 500/hr bottleneck).

WHY
    Lexicon exposes no editable-location API and its per-track HTTP path caps sync at
    ~400-700 tracks/hr (each organize = find + membership-check + add-to-playlist + tag).
    For a *link-only* track — one that ALREADY has a Lexicon Track row (a real
    ``lexicon_track_id``; the audio file is already present, no file move, no import) —
    filing it into its monthly playlist and tagging it is nothing but two small row writes.
    Writing those directly into Lexicon's SQLite DB is 10x+ faster.

WHAT IT MIRRORS  (byte-for-byte the end state the HTTP path produces)
    The API path (``process_pipeline._organize_track`` for ``already_in_lexicon`` tracks) does
    exactly two data writes for a link-only track:
      1. ``PATCH /v1/playlist-tracks {id: playlistId, trackIds:[trackId]}``  -> a row in
         ``LinkTrackPlaylist(playlistId, trackId, position)`` appended at the end of the
         playlist, deduped on the unique ``(trackId, playlistId)`` index.
      2. ``PATCH /v1/track {id, edits:{comment:"[sls:<spotify_id>]"}}``      -> ``Track.comment``
         set to the traceability tag (and ``dateModified`` bumped).
    This module reproduces precisely those two writes and NOTHING else. It never touches
    ``location``, never moves/deletes files, never inserts/deletes a Track row, never creates
    or deletes a Playlist. Playlist/folder *creation* stays on the safe API path (rare,
    structural); this module only resolves an EXISTING monthly playlist by name.

SAFETY
    * Source of truth for the FTS index is a DB trigger (``Track_FTS_Update``), so a raw
      ``UPDATE Track`` keeps full-text search consistent exactly as Lexicon's own writes do.
    * All writes for a batch run in a single ``BEGIN IMMEDIATE`` transaction (atomic).
    * ``foreign_keys=ON`` so a bad trackId/playlistId is rejected, never silently orphaned.
    * ``INSERT OR IGNORE`` on the link makes re-filing idempotent (mirrors the membership check).
    * The comment write is diff-guarded: skipped when already equal, so it is idempotent and
      only bumps ``dateModified`` when something actually changes.
    * Caller MUST ensure Lexicon is quit (WAL checkpointed) before applying to the LIVE DB.

This is the writer only. Choosing direct-write vs the API path is gated by the
``direct_write_enabled`` config flag (default off); the API path is always preserved as the
fallback.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone

_MONTHS = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _now_iso() -> str:
    """Lexicon's dateModified format, e.g. '2026-07-12T17:53:32.123Z'."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{datetime.now(tz=timezone.utc).microsecond // 1000:03d}Z"


def monthly_playlist_name(added_at: str) -> str:
    """Map a Spotify added_at ISO timestamp to Lexicon's monthly playlist name.

    Mirrors process_pipeline._organize_track: 'MM. Month YYYY'. Falls back to the
    current UTC month when added_at is missing/unparseable (same as the API path).
    """
    try:
        dt = datetime.fromisoformat((added_at or "").replace("Z", "+00:00"))
    except (ValueError, TypeError):
        dt = datetime.now(tz=timezone.utc)
    return f"{dt.month:02d}. {_MONTHS[dt.month]} {dt.year}"


def resolve_monthly_playlist_id(conn: sqlite3.Connection, added_at: str) -> int | None:
    """Look up an EXISTING monthly playlist's id by name (type='2'). None if absent.

    A None return means the playlist does not exist yet -> the caller must route that
    track to the API path (which creates the folder/playlist). This module never creates
    playlists.
    """
    name = monthly_playlist_name(added_at)
    row = conn.execute(
        "SELECT id FROM Playlist WHERE name = ? AND type = '2' LIMIT 1", (name,)
    ).fetchone()
    return int(row[0]) if row else None


@dataclass
class LinkSpec:
    """One link-only track to file. lexicon_track_id + playlist_id are REAL existing ids."""
    lexicon_track_id: int
    playlist_id: int
    spotify_id: str
    waxflow_track_id: int | None = None  # for audit back-reference into sync.db


@dataclass
class WriteResult:
    total: int = 0
    linked: int = 0            # LinkTrackPlaylist rows inserted
    link_already: int = 0      # already in playlist (no-op)
    comment_set: int = 0       # Track.comment updated
    comment_already: int = 0   # comment already correct (no-op)
    errors: list = field(default_factory=list)
    audit: list = field(default_factory=list)  # (track_id, op, before, after)


def _comment_tag(spotify_id: str) -> str:
    return f"[sls:{spotify_id}]"


def is_direct_write_enabled(db_path: str) -> bool:
    """Feature flag: is link-only direct-write selected over the API path?

    Default OFF. The API path (process_pipeline._organize_track) is always the fallback
    and is never removed. Reads app_config.direct_write_enabled from the WaxFlow sync.db.
    Kept here (not in helpers) so the flag lives with the writer it gates.
    """
    try:
        from tasks.helpers import get_config
    except Exception:  # pragma: no cover - import shape differs outside the worker pkg
        return False
    return str(get_config(db_path, "direct_write_enabled") or "0").lower() in ("1", "true", "on", "yes")


def apply_link_only_writes(
    lexicon_db_path: str,
    specs: list,
    dry_run: bool = False,
) -> WriteResult:
    """Apply the two link-only writes for each spec, atomically, then integrity-check.

    Returns a WriteResult with per-op counts + an audit trail. Raises on integrity failure
    (caller restores from the pre-op backup). When dry_run=True the transaction is rolled
    back after collecting the would-be counts (used for scratch/copy validation timing).
    """
    result = WriteResult(total=len(specs))
    conn = sqlite3.connect(lexicon_db_path, timeout=60)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("BEGIN IMMEDIATE")
        now = _now_iso()
        for spec in specs:
            tid = int(spec.lexicon_track_id)
            pid = int(spec.playlist_id)
            tag = _comment_tag(spec.spotify_id)

            # ---- 1) playlist link (append at end, dedup on unique (trackId,playlistId)) ----
            exists = conn.execute(
                "SELECT 1 FROM LinkTrackPlaylist WHERE trackId=? AND playlistId=?",
                (tid, pid),
            ).fetchone()
            if exists:
                result.link_already += 1
            else:
                cur = conn.execute(
                    """INSERT OR IGNORE INTO LinkTrackPlaylist (playlistId, trackId, position)
                       VALUES (?, ?, (SELECT COALESCE(MAX(position), -1) + 1
                                        FROM LinkTrackPlaylist WHERE playlistId = ?))""",
                    (pid, tid, pid),
                )
                if cur.rowcount:
                    result.linked += 1
                    result.audit.append((tid, "link", None, f"playlist:{pid}"))

            # ---- 2) comment tag (diff-guarded; mirrors API end state) ----------------------
            row = conn.execute("SELECT comment FROM Track WHERE id=?", (tid,)).fetchone()
            if row is None:
                result.errors.append((tid, "track_missing"))
                continue
            before = row[0]
            if before == tag:
                result.comment_already += 1
            else:
                conn.execute(
                    "UPDATE Track SET comment=?, dateModified=? WHERE id=?",
                    (tag, now, tid),
                )
                result.comment_set += 1
                result.audit.append((tid, "comment", before, tag))

        if dry_run:
            conn.execute("ROLLBACK")
        else:
            conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        conn.close()
        raise

    # Integrity + FK check AFTER commit (separate connection semantics not needed; same conn).
    ic = conn.execute("PRAGMA integrity_check").fetchone()[0]
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    conn.close()
    if ic != "ok":
        raise RuntimeError(f"integrity_check failed after direct-write: {ic}")
    if fk:
        raise RuntimeError(f"foreign_key_check found violations after direct-write: {fk[:5]}")
    return result
