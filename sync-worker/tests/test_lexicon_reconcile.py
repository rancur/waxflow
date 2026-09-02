"""Tests for the deleted-in-Lexicon reconciler (tasks/lexicon_reconcile.py).

Contract under test:
  * a complete track whose Lexicon row is gone is TOMBSTONED (ignored + protected +
    tombstones row) and its NAS master moved into the share's recycle bin,
  * an archived Lexicon row counts as deleted,
  * a track still present in Lexicon is untouched,
  * a 'lexicon_existing' (linked, never downloaded) track is tombstoned but its
    file is left alone,
  * the pass is a no-op while the Mac is asleep / Lexicon unavailable,
  * an incomplete listing (page failure) produces ZERO writes,
  * a suspiciously small library refuses to run (sanity fuse),
  * tombstones are terminal for the worker's re-arm selectors (hunter, catch-up,
    retry_unmatched, quality upgrader all select other stages),
  * the same-crap-match guard diverts a colliding candidate to review,
  * purge deletes only WaxFlow-trashed files, only after purge_after,
  * restore un-trashes the file, closes the tombstone and re-arms from 'new'.

No network: the availability probe and the Lexicon listing are stubbed.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import lexicon_reconcile as lr  # noqa: E402
from tasks import v3_schema  # noqa: E402
from tasks.helpers import get_db  # noqa: E402


def _db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT, title TEXT, artist TEXT, album TEXT, isrc TEXT,
            tidal_id TEXT, spotify_added_at TEXT, file_path TEXT, file_hash_sha256 TEXT,
            match_source TEXT, match_status TEXT, download_source TEXT,
            download_status TEXT, verify_status TEXT,
            lexicon_track_id TEXT, lexicon_status TEXT,
            pipeline_stage TEXT DEFAULT 'complete', pipeline_error TEXT,
            is_protected INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now', '-1 hour'))
        );
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT, track_id INTEGER, message TEXT, details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE file_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT UNIQUE NOT NULL, isrc TEXT, title TEXT, artist TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    v3_schema.ensure_v3_schema(path)  # adds tombstones + the v3 tables
    return path


def _avail(state="available"):
    return lambda db_path, **kw: SimpleNamespace(
        state=state, lexicon_available=(state == "available")
    )


def _listing(ids, archived=()):
    return lambda api_url: (set(ids), set(archived))


class _Base(unittest.TestCase):
    def setUp(self):
        self.db = _db()
        # A fake share root with a Database/ library dir, so trash paths resolve.
        self.root = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.root, "Database", "Artist"))
        self._env = mock.patch.dict(os.environ, {"MUSIC_SHARE_ROOT": self.root})
        self._env.start()

    def tearDown(self):
        self._env.stop()

    def _file(self, name="Artist - Song.flac") -> str:
        p = os.path.join(self.root, "Database", "Artist", name)
        with open(p, "wb") as f:
            f.write(b"FLAC")
        with get_db(self.db) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO file_index (file_path, isrc) VALUES (?, ?)",
                (p, "ISRC1"),
            )
        return p

    def _track(self, lexicon_id, file_path=None, **kw) -> int:
        fields = dict(
            spotify_id=f"sp{lexicon_id}", title="Song", artist="Artist", isrc="ISRC1",
            tidal_id=f"t{lexicon_id}", file_path=file_path, match_source="isrc",
            download_source="tidal", lexicon_track_id=str(lexicon_id),
            pipeline_stage="complete", lexicon_status="synced",
        )
        fields.update(kw)
        cols = ", ".join(fields)
        with get_db(self.db) as conn:
            cur = conn.execute(
                f"INSERT INTO tracks ({cols}) VALUES ({', '.join('?' * len(fields))})",
                list(fields.values()),
            )
            return cur.lastrowid

    def _row(self, tid) -> dict:
        with get_db(self.db) as conn:
            return dict(conn.execute("SELECT * FROM tracks WHERE id = ?", (tid,)).fetchone())

    def _tombs(self):
        with get_db(self.db) as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM tombstones ORDER BY id")]

    def _run(self, live_ids, archived=(), state="available"):
        return lr.run_reconcile(
            self.db, probe_fn=_avail(state), fetch_fn=_listing(live_ids, archived)
        )


class TestReconcile(_Base):
    def test_missing_lexicon_row_is_tombstoned_and_file_trashed(self):
        f = self._file()
        gone = self._track(11, f)
        kept = self._track(12, None)
        out = self._run(live_ids=[12] + list(range(1000, 1200)))
        self.assertEqual(out["deleted"], 1)
        t = self._row(gone)
        self.assertEqual(t["pipeline_stage"], "ignored")
        self.assertEqual(t["is_protected"], 1)
        self.assertTrue(t["pipeline_error"].startswith(lr.TOMBSTONE_ERROR_PREFIX))
        self.assertEqual(self._row(kept)["pipeline_stage"], "complete")
        tb = self._tombs()
        self.assertEqual(len(tb), 1)
        self.assertEqual(tb[0]["track_id"], gone)
        self.assertEqual(tb[0]["tidal_id"], "t11")
        self.assertFalse(os.path.exists(f), "master must leave the library")
        self.assertTrue(os.path.exists(tb[0]["trashed_path"]))
        self.assertIn(f"/{lr.RECYCLE_DIRNAME}/Database/Artist/", tb[0]["trashed_path"])
        with get_db(self.db) as conn:
            self.assertIsNone(
                conn.execute("SELECT 1 FROM file_index WHERE file_path = ?", (f,)).fetchone(),
                "file_index must forget the trashed file",
            )
        self.assertTrue(lr.is_tombstoned(self.db, gone))
        self.assertFalse(lr.is_tombstoned(self.db, kept))

    def test_archived_counts_as_deleted(self):
        tid = self._track(21, None)
        out = self._run(live_ids=[21] + list(range(1000, 1200)), archived=[21])
        self.assertEqual(out["archived"], 1)
        self.assertEqual(self._row(tid)["pipeline_stage"], "ignored")
        self.assertEqual(self._tombs()[0]["reason"], lr.REASON_ARCHIVED)

    def test_linked_library_track_keeps_its_file(self):
        f = self._file("owned-by-will.flac")
        tid = self._track(31, f, match_source="lexicon_existing", download_source="lexicon_existing")
        self._run(live_ids=list(range(1000, 1200)))
        self.assertEqual(self._row(tid)["pipeline_stage"], "ignored")
        self.assertTrue(os.path.exists(f), "WaxFlow never owned this file; must not move it")
        self.assertIsNone(self._tombs()[0]["trashed_path"])

    def test_noop_while_mac_asleep(self):
        tid = self._track(41, None)
        out = self._run(live_ids=[], state="asleep")
        self.assertEqual(out["status"], "skipped")
        self.assertEqual(self._row(tid)["pipeline_stage"], "complete")
        self.assertEqual(self._tombs(), [])

    def test_incomplete_listing_writes_nothing(self):
        tid = self._track(51, None)

        def boom(api_url):
            raise lr.LexiconListingIncomplete("page 3: HTTP 500")

        out = lr.run_reconcile(self.db, probe_fn=_avail(), fetch_fn=boom)
        self.assertEqual(out["status"], "skipped")
        self.assertEqual(self._row(tid)["pipeline_stage"], "complete")
        self.assertEqual(self._tombs(), [])

    def test_tiny_library_refuses(self):
        tid = self._track(61, None)
        out = self._run(live_ids=[1, 2, 3])  # < min_library
        self.assertEqual(out["status"], "refused")
        self.assertEqual(self._row(tid)["pipeline_stage"], "complete")

    def test_disabled_flag(self):
        with get_db(self.db) as conn:
            conn.execute("INSERT INTO app_config VALUES ('lexicon_reconcile_enabled', '0')")
        self._track(71, None)
        self.assertEqual(self._run(live_ids=[])["status"], "disabled")

    def test_idempotent_second_pass(self):
        gone = self._track(81, self._file())
        live = list(range(1000, 1200))
        self._run(live)
        self._run(live)
        self.assertEqual(len(self._tombs()), 1)

    def test_batch_bound(self):
        with get_db(self.db) as conn:
            conn.execute("INSERT INTO app_config VALUES ('tombstone_batch', '2')")
        for i in range(5):
            self._track(90 + i, None)
        out = self._run(live_ids=list(range(1000, 1200)))
        self.assertEqual(out["deleted"], 2)

    def test_listing_pages_and_aborts_on_partial(self):
        class _Resp:
            def __init__(self, status, payload):
                self.status_code = status
                self._p = payload

            def json(self):
                return self._p

        calls = []

        class _Client:
            def __init__(self, *a, **k):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def get(self, path, params=None):
                calls.append(params["offset"])
                if params["offset"] == 0:
                    tracks = [{"id": i, "archived": 0} for i in range(1000)]
                    return _Resp(200, {"data": {"tracks": tracks, "total": 1500}})
                return _Resp(500, {})

        with mock.patch.object(lr.httpx, "Client", _Client):
            with self.assertRaises(lr.LexiconListingIncomplete):
                lr.fetch_lexicon_ids("http://x")
        self.assertEqual(calls, [0, 1000])


class TestTerminalEverywhere(_Base):
    """A tombstone is in 'ignored'; every worker re-arm selector must skip it."""

    def test_worker_rearm_selectors_never_touch_ignored(self):
        import inspect
        from tasks import hunter, import_catchup, retry_unmatched, metadata_fallback
        from tasks import quality_upgrade, lossless_upgrade
        for mod, needle in (
            (hunter, "UNSOURCED_STAGES = (\"error\",)"),
            (import_catchup, "pipeline_stage = 'error'"),
            (retry_unmatched, "pipeline_stage = 'error'"),
            (metadata_fallback, "pipeline_stage = 'error'"),
            (quality_upgrade, "pipeline_stage = 'complete'"),
            (lossless_upgrade, "pipeline_stage = 'complete'"),
        ):
            src = inspect.getsource(mod)
            self.assertIn(needle, src, f"{mod.__name__} selector changed — re-check tombstone safety")
            self.assertNotIn("pipeline_stage = 'ignored'", src)
            self.assertNotIn("pipeline_stage IN ('ignored'", src)


class TestGuard(_Base):
    def test_colliding_tidal_match_is_diverted_to_review(self):
        from tasks import process_pipeline as pp
        gone = self._track(101, self._file(), tidal_id="T-CRAP", isrc="ISRC-CRAP")
        self._run(live_ids=list(range(1000, 1200)))
        other = self._track(102, None, spotify_id="spX", tidal_id=None, isrc="OTHER",
                            pipeline_stage="matching", lexicon_track_id=None)
        blocked = pp._tombstone_blocks(self.db, self._row(other), tidal_id="T-CRAP", via="tidal_search")
        self.assertTrue(blocked)
        t = self._row(other)
        self.assertEqual(t["pipeline_stage"], "needs_import_review")
        self.assertIn("tombstone_match", t["pipeline_error"])

    def test_non_colliding_match_passes(self):
        from tasks import process_pipeline as pp
        self._track(111, None, tidal_id="T-1")
        self._run(live_ids=list(range(1000, 1200)))
        other = self._track(112, None, spotify_id="spY", tidal_id=None, pipeline_stage="matching",
                            lexicon_track_id=None)
        self.assertFalse(pp._tombstone_blocks(self.db, self._row(other), tidal_id="T-other", via="x"))
        self.assertEqual(self._row(other)["pipeline_stage"], "matching")

    def test_restored_tombstone_no_longer_collides(self):
        gone = self._track(121, self._file(), tidal_id="T-R")
        self._run(live_ids=list(range(1000, 1200)))
        lr.restore_tombstone(self.db, gone)
        self.assertIsNone(lr.find_tombstone_collision(self.db, tidal_id="T-R"))


class TestPurgeAndRestore(_Base):
    def test_purge_only_after_window_and_only_own_files(self):
        gone = self._track(131, self._file())
        self._run(live_ids=list(range(1000, 1200)))
        tb = self._tombs()[0]
        self.assertEqual(lr.purge_expired(self.db), 0, "not due yet")
        self.assertTrue(os.path.exists(tb["trashed_path"]))
        past = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with get_db(self.db) as conn:
            conn.execute("UPDATE tombstones SET purge_after = ? WHERE id = ?", (past, tb["id"]))
        self.assertEqual(lr.purge_expired(self.db), 1)
        self.assertFalse(os.path.exists(tb["trashed_path"]))
        self.assertIsNotNone(self._tombs()[0]["purged_at"])
        # A tombstone whose trashed_path is NOT in the recycle bin is never deleted.
        stray = os.path.join(self.root, "Database", "Artist", "stray.flac")
        open(stray, "wb").write(b"x")
        with get_db(self.db) as conn:
            conn.execute(
                "INSERT INTO tombstones (track_id, reason, trashed_path, purge_after) VALUES (?, 'x', ?, ?)",
                (gone, stray, past),
            )
        lr.purge_expired(self.db)
        self.assertTrue(os.path.exists(stray))

    def test_restore_untrashes_and_rearms(self):
        f = self._file()
        gone = self._track(141, f)
        self._run(live_ids=list(range(1000, 1200)))
        self.assertFalse(os.path.exists(f))
        out = lr.restore_tombstone(self.db, gone)
        self.assertEqual(out["restored_file"], f)
        self.assertTrue(os.path.exists(f))
        t = self._row(gone)
        self.assertEqual(t["pipeline_stage"], "new")
        self.assertEqual(t["is_protected"], 0)
        self.assertEqual(t["file_path"], f)
        self.assertIsNotNone(self._tombs()[0]["restored_at"])
        self.assertFalse(lr.is_tombstoned(self.db, gone))
        with self.assertRaises(ValueError):
            lr.restore_tombstone(self.db, gone)


if __name__ == "__main__":
    unittest.main()
