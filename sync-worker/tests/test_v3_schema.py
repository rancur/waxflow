"""Tests for the WaxFlow v3 additive schema foundation (Phase A).

Proves the migration:
  * creates every new v3 table + both new nullable tracks columns + the indexes,
  * is ADDITIVE (leaves the legacy fallback_attempts table untouched),
  * is IDEMPOTENT (a second run is a pure no-op, and the table shapes are stable),
  * is MIRRORED by sync-api/init_db.py (same structural shape, column-for-column).

No network / no external deps.
"""

import os
import sqlite3
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import v3_schema  # noqa: E402


def _base_db() -> str:
    """A minimal pre-v3 DB: the parent tables the v3 FKs reference + the legacy
    fallback_attempts table (to prove it is left intact)."""
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE NOT NULL,
            title TEXT, artist TEXT
        );
        CREATE TABLE playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            playlist_name TEXT NOT NULL, year INTEGER, month INTEGER
        );
        CREATE TABLE fallback_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_id INTEGER, source TEXT, status TEXT,
            error TEXT, search_query TEXT, result_count INTEGER,
            attempted_at TEXT DEFAULT (datetime('now'))
        );
        INSERT INTO tracks (spotify_id, title, artist) VALUES ('sp1', 'T', 'A');
        INSERT INTO fallback_attempts (track_id, source, status) VALUES (1, 'tidal', 'no_match');
        """
    )
    conn.commit()
    conn.close()
    return path


def _tables(path: str) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
    finally:
        conn.close()


def _columns(path: str, table: str) -> dict:
    """{col_name: (type, notnull, dflt, pk)} for structural comparison."""
    conn = sqlite3.connect(path)
    try:
        return {
            r[1]: (r[2], r[3], r[4], r[5])
            for r in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    finally:
        conn.close()


def _indexes(path: str) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        ).fetchall()}
    finally:
        conn.close()


class TestV3SchemaCreation(unittest.TestCase):
    def setUp(self):
        self.db = _base_db()

    def tearDown(self):
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(self.db + suffix)
            except OSError:
                pass

    def test_creates_all_new_tables(self):
        before = _tables(self.db)
        for t in v3_schema.V3_TABLES:
            self.assertNotIn(t, before, f"{t} should not pre-exist")
        v3_schema.ensure_v3_schema(self.db)
        after = _tables(self.db)
        for t in v3_schema.V3_TABLES:
            self.assertIn(t, after, f"{t} should be created")

    def test_creates_new_tracks_columns(self):
        v3_schema.ensure_v3_schema(self.db)
        cols = _columns(self.db, "tracks")
        self.assertIn("sourceability", cols)
        self.assertIn("wanted_id", cols)
        # both nullable (notnull flag == 0)
        self.assertEqual(cols["sourceability"][1], 0)
        self.assertEqual(cols["wanted_id"][1], 0)

    def test_creates_indexes(self):
        v3_schema.ensure_v3_schema(self.db)
        idx = _indexes(self.db)
        for expected in (
            "idx_purchase_links_dedup",
            "idx_source_attempts_track_source",
            "idx_wanted_track",
            "idx_import_queue_state",
        ):
            self.assertIn(expected, idx)

    def test_fallback_attempts_untouched(self):
        """ADDITIVE: legacy table + its data survive verbatim."""
        before_cols = _columns(self.db, "fallback_attempts")
        conn = sqlite3.connect(self.db)
        before_rows = conn.execute("SELECT COUNT(*) FROM fallback_attempts").fetchone()[0]
        conn.close()
        v3_schema.ensure_v3_schema(self.db)
        self.assertEqual(_columns(self.db, "fallback_attempts"), before_cols)
        conn = sqlite3.connect(self.db)
        after_rows = conn.execute("SELECT COUNT(*) FROM fallback_attempts").fetchone()[0]
        conn.close()
        self.assertEqual(before_rows, after_rows)

    def test_idempotent_second_run_is_noop(self):
        v3_schema.ensure_v3_schema(self.db)
        snap_tables = _tables(self.db)
        snap_cols = {t: _columns(self.db, t) for t in v3_schema.V3_TABLES}
        snap_track_cols = _columns(self.db, "tracks")
        snap_idx = _indexes(self.db)
        # Second (and third) run must not raise and must not change anything.
        v3_schema.ensure_v3_schema(self.db)
        v3_schema.ensure_v3_schema(self.db)
        self.assertEqual(_tables(self.db), snap_tables)
        for t in v3_schema.V3_TABLES:
            self.assertEqual(_columns(self.db, t), snap_cols[t])
        self.assertEqual(_columns(self.db, "tracks"), snap_track_cols)
        self.assertEqual(_indexes(self.db), snap_idx)

    def test_idempotent_when_columns_already_present(self):
        """If a pre-v3 DB already has the columns, ADD COLUMN must be skipped."""
        conn = sqlite3.connect(self.db)
        conn.execute("ALTER TABLE tracks ADD COLUMN sourceability TEXT")
        conn.execute("ALTER TABLE tracks ADD COLUMN wanted_id INTEGER")
        conn.commit()
        conn.close()
        v3_schema.ensure_v3_schema(self.db)  # must not raise "duplicate column"
        cols = _columns(self.db, "tracks")
        self.assertIn("sourceability", cols)
        self.assertIn("wanted_id", cols)


class TestInitDbMirror(unittest.TestCase):
    """The worker ensure_v3_schema and sync-api/init_db.py must produce the same
    v3 structural shape (mirrored migrations)."""

    def _run_init_db(self) -> str:
        import importlib

        api_dir = os.path.join(os.path.dirname(SYNC_WORKER_DIR), "sync-api")
        path = tempfile.mktemp(suffix=".db")
        old_env = os.environ.get("SLS_DB_PATH")
        old_path = list(sys.path)
        os.environ["SLS_DB_PATH"] = path
        if api_dir not in sys.path:
            sys.path.insert(0, api_dir)
        try:
            # Fresh import so it binds to the just-set SLS_DB_PATH via db.py.
            for mod in ("init_db", "db"):
                if mod in sys.modules:
                    del sys.modules[mod]
            init_db = importlib.import_module("init_db")
            init_db.init()
        finally:
            sys.path[:] = old_path
            if old_env is None:
                os.environ.pop("SLS_DB_PATH", None)
            else:
                os.environ["SLS_DB_PATH"] = old_env
            for mod in ("init_db", "db"):
                sys.modules.pop(mod, None)
        return path

    def test_init_db_creates_v3_tables_and_columns(self):
        path = self._run_init_db()
        try:
            tables = _tables(path)
            for t in v3_schema.V3_TABLES:
                self.assertIn(t, tables, f"init_db must create {t}")
            tcols = _columns(path, "tracks")
            self.assertIn("sourceability", tcols)
            self.assertIn("wanted_id", tcols)
        finally:
            for suffix in ("", "-wal", "-shm"):
                try:
                    os.remove(path + suffix)
                except OSError:
                    pass

    def test_worker_and_api_shapes_match(self):
        api_db = self._run_init_db()
        worker_db = _base_db()
        try:
            v3_schema.ensure_v3_schema(worker_db)
            # Every v3 table must have an identical column shape in both DBs.
            for t in v3_schema.V3_TABLES:
                self.assertEqual(
                    _columns(api_db, t), _columns(worker_db, t),
                    f"{t} shape differs between init_db and ensure_v3_schema",
                )
            # And the two new tracks columns must match structurally too.
            api_tcols = _columns(api_db, "tracks")
            worker_tcols = _columns(worker_db, "tracks")
            for c in ("sourceability", "wanted_id"):
                self.assertEqual(api_tcols[c], worker_tcols[c], f"tracks.{c} shape differs")
        finally:
            for base in (api_db, worker_db):
                for suffix in ("", "-wal", "-shm"):
                    try:
                        os.remove(base + suffix)
                    except OSError:
                        pass


if __name__ == "__main__":
    unittest.main()
