"""WaxFlow v3 — additive schema foundation (Phase A).

This module lays down the NEW tables and NEW ``tracks`` columns that the nine
v3 features build on. It is deliberately ADDITIVE-ONLY and INERT:

  * ADDITIVE — every statement is ``CREATE TABLE IF NOT EXISTS`` or a guarded
    ``ALTER TABLE ... ADD COLUMN`` (nullable/defaulted). There is NO ``tracks``
    table rebuild, NO CHECK-constraint change, and NO data migration. That keeps
    the migration cheap and non-locking, which matters because it may run while
    the live worker is finishing a sync under memory pressure.
  * INERT — nothing here is wired into the worker task loop yet. No feature reads
    or writes these tables at runtime. They exist so Phase B/C code can slot in
    behind default-off flags without a second schema change.
  * IDEMPOTENT — safe to call on every worker cycle (mirrors the existing
    ``lossless_upgrade.ensure_schema`` pattern). Re-running is a pure no-op.

The identical DDL is mirrored in ``sync-api/init_db.py`` so the API-side init and
the worker converge on exactly the same schema regardless of which one first
touches a fresh (or existing pre-v3) database.

The forward table for the source-plugin retry log is ``source_attempts`` (a
per-source, per-track attempt log with exponential backoff). The legacy
``fallback_attempts`` table is intentionally left UNTOUCHED — the live Soulseek
fallback still reads and writes it — so both coexist during the transition.
"""

from __future__ import annotations

import logging

from tasks.helpers import get_db

log = logging.getLogger("worker.v3_schema")

# Bumped whenever the additive v3 schema surface changes. Recorded in
# direct_write_audit rows so a written-back change can be tied to the schema it
# was produced under. Phase A == 1.
V3_SCHEMA_VERSION = 1

# The new tables introduced by the v3 foundation. Exposed for tests + tooling.
V3_TABLES = (
    "wanted",
    "source_attempts",
    "purchase_links",
    "import_queue",
    "plex_sync",
    "direct_write_audit",
    "mac_availability",
)

# The new (nullable) columns added to the existing ``tracks`` table.
V3_TRACK_COLUMNS = (
    ("sourceability", "TEXT"),
    ("wanted_id", "INTEGER"),
)

# All CREATE-IF-NOT-EXISTS DDL. Kept in one script so it applies atomically and
# reads as the canonical shape of the v3 tables. Mirrored verbatim in init_db.py.
_V3_DDL = """
-- Per-track "we still want a (better) copy of this" ledger. One row tracks the
-- desired quality target for a track and the cross-source acquisition state,
-- decoupled from the single-shot pipeline_stage on tracks.
CREATE TABLE IF NOT EXISTS wanted (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER REFERENCES tracks(id),
    state TEXT NOT NULL DEFAULT 'wanted',
    quality_target TEXT,
    reason TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    last_attempt_at TEXT,
    last_source TEXT,
    best_result_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Generalizes fallback_attempts into a per-source, per-track attempt log with
-- exponential backoff (see SourceBackoff in tasks/sources/base.py). fallback_attempts
-- is left intact for the live Soulseek path; source_attempts is the forward table.
CREATE TABLE IF NOT EXISTS source_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER REFERENCES tracks(id),
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    search_query TEXT,
    result_count INTEGER,
    attempt_no INTEGER NOT NULL DEFAULT 1,
    backoff_seconds INTEGER,
    next_retry_at TEXT,
    attempted_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Generated buy/download links for tracks that cannot be auto-acquired
-- (SEARCH_LINK-only sources). dedup_key collapses repeat generations of the
-- same effective link so a track is not spammed with duplicates.
CREATE TABLE IF NOT EXISTS purchase_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER REFERENCES tracks(id),
    source TEXT NOT NULL,
    url TEXT,
    format_hint TEXT,
    price TEXT,
    confidence REAL,
    dedup_key TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    first_generated_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_refreshed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Queue of acquired files awaiting import into Lexicon on the Mac side (op =
-- import/relocate/etc.). held_reason parks an item that is blocked (e.g. Mac
-- unreachable) without losing it.
CREATE TABLE IF NOT EXISTS import_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER REFERENCES tracks(id),
    mac_path TEXT,
    playlist_target TEXT,
    op TEXT NOT NULL DEFAULT 'import',
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    held_reason TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Maps tracks/playlists to their Plex ratingKey and records library-scan state
-- so a future Plex mirror can reconcile without re-scanning everything.
CREATE TABLE IF NOT EXISTS plex_sync (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER REFERENCES tracks(id),
    playlist_id INTEGER REFERENCES playlists(id),
    rating_key TEXT,
    scan_state TEXT NOT NULL DEFAULT 'pending',
    last_scanned_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Audit trail for direct writes to the Lexicon database (the future
-- direct-write path): the schema version in force, the pre-write backup, the
-- before/after snapshot, and whether the API fallback had to be used.
CREATE TABLE IF NOT EXISTS direct_write_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER REFERENCES tracks(id),
    schema_version TEXT,
    backup_path TEXT,
    op TEXT,
    before_json TEXT,
    after_json TEXT,
    fallback_used INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Rolling samples of Lexicon-host (Mac) availability: raw reachability, whether
-- the SMB music mount is present, and whether the Lexicon API answered. Used to
-- decide when it is safe to push imports rather than hold them.
CREATE TABLE IF NOT EXISTS mac_availability (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reachable INTEGER,
    smb_mounted INTEGER,
    api_ok INTEGER,
    detail TEXT,
    sampled_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Dedup + lookup indexes (all IF NOT EXISTS -> idempotent).
CREATE UNIQUE INDEX IF NOT EXISTS idx_purchase_links_dedup
    ON purchase_links(dedup_key);
CREATE INDEX IF NOT EXISTS idx_source_attempts_track_source
    ON source_attempts(track_id, source);
CREATE INDEX IF NOT EXISTS idx_wanted_track ON wanted(track_id);
CREATE INDEX IF NOT EXISTS idx_import_queue_state ON import_queue(state);
"""


def ensure_v3_schema(db_path: str) -> None:
    """Idempotently create the additive v3 tables/columns.

    Additive-only: CREATE TABLE IF NOT EXISTS + guarded nullable ADD COLUMN. No
    table rebuild, no CHECK-constraint change, no data migration. Safe to call on
    every worker cycle and safe to run repeatedly (pure no-op after the first).
    """
    with get_db(db_path) as conn:
        conn.executescript(_V3_DDL)

    # Guarded, nullable ADD COLUMNs on tracks. A nullable/defaulted ADD COLUMN is
    # cheap and non-locking in SQLite and needs no table rebuild.
    with get_db(db_path) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(tracks)").fetchall()}
        for name, decl in V3_TRACK_COLUMNS:
            if name not in cols:
                conn.execute(f"ALTER TABLE tracks ADD COLUMN {name} {decl}")
                log.info("schema: added tracks.%s", name)
