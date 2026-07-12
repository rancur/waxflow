"""Tests for the WaxFlow v3 Plex/Plexamp mirror (Feature 4).

Proves behaviour, not shape:

  * plex_client issues the exact PATH-SCOPED refresh (never a global scan) and
    the correct create / add / remove playlist requests, driven through an
    ``httpx.MockTransport`` fake Plex server that actually mutates state;
  * track matching resolves by file PATH first and falls back to fuzzy
    artist+title search when the path is not under the container prefix;
  * monthly-playlist reconcile is IDEMPOTENT — a second run makes zero changes
    and produces no duplicate memberships — and it both ADDS missing members and
    REMOVES stale ones so membership equals the WaxFlow monthly list.

No real network: every Plex call is served by FakePlexServer in-process.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from urllib.parse import parse_qs

import httpx

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import plex_sync, v3_schema  # noqa: E402
from tasks.plex_client import PlexClient  # noqa: E402


# ---------------------------------------------------------------- fake Plex


class FakePlexServer:
    """An in-memory Plex server good enough to drive the mirror end to end.

    Serves the handful of endpoints plex_client uses and mutates real state so
    create/add/remove and idempotency can be asserted against observable results.
    """

    def __init__(self):
        # rating_key -> {"file", "artist", "title"}
        self.tracks: dict[str, dict] = {}
        # playlist rating_key -> {"title", "items": [ {playlist_item_id, rating_key} ]}
        self.playlists: dict[str, dict] = {}
        self.machine_id = "MID123"
        self._next_pl = 1000
        self._next_item = 1
        # Observability
        self.refresh_calls: list[tuple[str, str]] = []  # (section_id, path)
        self.global_refresh_calls = 0
        self.create_calls = 0

    # -- seed helpers --
    def add_track(self, rating_key, file, artist, title):
        self.tracks[str(rating_key)] = {"file": file, "artist": artist, "title": title}

    def add_playlist(self, title, rating_keys):
        key = str(self._next_pl)
        self._next_pl += 1
        items = []
        for rk in rating_keys:
            items.append({"playlist_item_id": str(self._next_item), "rating_key": str(rk)})
            self._next_item += 1
        self.playlists[key] = {"title": title, "items": items}
        return key

    # -- transport handler --
    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = dict(request.url.params)
        method = request.method

        def js(container):
            return httpx.Response(200, json={"MediaContainer": container})

        if path == "/identity":
            return js({"machineIdentifier": self.machine_id})

        if path == "/library/sections" and method == "GET":
            return js({"Directory": [
                {"key": "4", "type": "artist", "title": "Music"},
            ]})

        if path.endswith("/all") and "/library/sections/" in path:
            start = int(params.get("X-Plex-Container-Start", 0))
            size = int(params.get("X-Plex-Container-Size", 200))
            all_keys = list(self.tracks.keys())
            page = all_keys[start:start + size]
            meta = []
            for rk in page:
                t = self.tracks[rk]
                meta.append({
                    "ratingKey": rk, "title": t["title"], "grandparentTitle": t["artist"],
                    "Media": [{"Part": [{"file": t["file"]}]}],
                })
            return js({"size": len(page), "totalSize": len(all_keys), "Metadata": meta})

        if path.endswith("/search") and "/library/sections/" in path:
            query = (params.get("query") or "").lower()
            qtokens = set(query.split())
            meta = []
            for rk, t in self.tracks.items():
                hay = f"{t['artist']} {t['title']}".lower()
                if qtokens and all(tok in hay for tok in qtokens):
                    meta.append({
                        "ratingKey": rk, "title": t["title"], "grandparentTitle": t["artist"],
                        "Media": [{"Part": [{"file": t["file"]}]}],
                    })
            return js({"size": len(meta), "Metadata": meta})

        if path.endswith("/refresh") and "/library/sections/" in path and method == "PUT":
            section_id = path.split("/library/sections/")[1].split("/")[0]
            if "path" in params:
                self.refresh_calls.append((section_id, params["path"]))
            else:
                self.global_refresh_calls += 1
            return httpx.Response(200)

        if path == "/playlists" and method == "GET":
            meta = [{"ratingKey": k, "title": v["title"], "playlistType": "audio"}
                    for k, v in self.playlists.items()]
            return js({"size": len(meta), "Metadata": meta})

        if path == "/playlists" and method == "POST":
            self.create_calls += 1
            title = params.get("title")
            keys = _uri_keys(params.get("uri", ""))
            new_key = self.add_playlist(title, keys)
            return js({"Metadata": [{"ratingKey": new_key, "title": title}]})

        if path.startswith("/playlists/") and path.endswith("/items"):
            pl_key = path.split("/playlists/")[1].split("/")[0]
            pl = self.playlists.get(pl_key)
            if method == "GET":
                meta = [{"ratingKey": it["rating_key"], "playlistItemID": it["playlist_item_id"],
                         "title": self.tracks.get(it["rating_key"], {}).get("title", "")}
                        for it in (pl["items"] if pl else [])]
                return js({"size": len(meta), "Metadata": meta})
            if method == "PUT":
                for rk in _uri_keys(params.get("uri", "")):
                    pl["items"].append({"playlist_item_id": str(self._next_item), "rating_key": rk})
                    self._next_item += 1
                return httpx.Response(200)

        # DELETE /playlists/{key}/items/{itemid}
        if path.startswith("/playlists/") and "/items/" in path and method == "DELETE":
            parts = path.split("/")
            pl_key, item_id = parts[2], parts[4]
            pl = self.playlists.get(pl_key)
            if pl:
                pl["items"] = [it for it in pl["items"] if it["playlist_item_id"] != item_id]
            return httpx.Response(200)

        return httpx.Response(404, json={"MediaContainer": {}})

    def client(self) -> PlexClient:
        return PlexClient(
            "http://fake:32400", "TESTTOKEN",
            transport=httpx.MockTransport(self.handler),
            machine_id=self.machine_id,
        )


def _uri_keys(uri: str) -> list[str]:
    if "/library/metadata/" not in uri:
        return []
    return [k for k in uri.split("/library/metadata/")[1].split(",") if k]


# ------------------------------------------------------------------- DB seed


def _make_db() -> str:
    path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE NOT NULL,
            title TEXT, artist TEXT, file_path TEXT,
            pipeline_stage TEXT DEFAULT 'complete',
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folder_name TEXT, playlist_name TEXT NOT NULL,
            year INTEGER, month INTEGER
        );
        CREATE TABLE playlist_tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            playlist_id INTEGER NOT NULL,
            track_id INTEGER NOT NULL,
            position INTEGER,
            UNIQUE(playlist_id, track_id)
        );
        CREATE TABLE app_config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL, track_id INTEGER,
            message TEXT NOT NULL, details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        """
    )
    conn.commit()
    conn.close()
    v3_schema.ensure_v3_schema(path)
    return path


def _cfg(**over) -> plex_sync.PlexConfig:
    """Build a PlexConfig without touching a DB (direct attribute set)."""
    cfg = plex_sync.PlexConfig.__new__(plex_sync.PlexConfig)
    cfg.enabled = True
    cfg.token = "T"
    cfg.url = "http://fake:32400"
    cfg.section_id = "4"
    cfg.container_prefix = "/music"
    cfg.server_prefix = "/volume1/music"
    cfg.scan_batch = 25
    for k, v in over.items():
        setattr(cfg, k, v)
    return cfg


def _insert_track(db, spotify_id, title, artist, file_path, stage="complete"):
    conn = sqlite3.connect(db)
    cur = conn.execute(
        "INSERT INTO tracks (spotify_id, title, artist, file_path, pipeline_stage) VALUES (?,?,?,?,?)",
        (spotify_id, title, artist, file_path, stage),
    )
    tid = cur.lastrowid
    conn.commit()
    conn.close()
    return tid


def _insert_monthly(db, name, track_ids, year=2026, month=7):
    conn = sqlite3.connect(db)
    cur = conn.execute(
        "INSERT INTO playlists (folder_name, playlist_name, year, month) VALUES (?,?,?,?)",
        (str(year), name, year, month),
    )
    pid = cur.lastrowid
    for pos, tid in enumerate(track_ids):
        conn.execute(
            "INSERT INTO playlist_tracks (playlist_id, track_id, position) VALUES (?,?,?)",
            (pid, tid, pos),
        )
    conn.commit()
    conn.close()
    return pid


# --------------------------------------------------------------------- tests


class TestPathTranslation(unittest.TestCase):
    def test_container_maps_to_plex(self):
        self.assertEqual(
            plex_sync.container_to_plex_path("/music/Database/A/x.flac", "/music", "/volume1/music"),
            "/volume1/music/Database/A/x.flac",
        )

    def test_non_container_path_returns_none(self):
        # A Lexicon-host path is not under the container prefix -> fuzzy fallback.
        self.assertIsNone(
            plex_sync.container_to_plex_path("/Volumes/music/A/x.flac", "/music", "/volume1/music")
        )

    def test_empty_returns_none(self):
        self.assertIsNone(plex_sync.container_to_plex_path("", "/music", "/volume1/music"))


class TestFuzzyMatch(unittest.TestCase):
    def test_normalize_strips_accents_and_feat(self):
        self.assertEqual(plex_sync._normalize("Beyoncé (feat. Jay-Z)"), "beyonce")
        self.assertEqual(plex_sync._normalize("Song [Extended Mix]"), "song")

    def test_fuzzy_pick_exact_title_and_artist(self):
        cands = [
            {"rating_key": "1", "artist": "Wrong", "title": "Different"},
            {"rating_key": "2", "artist": "GOJII", "title": "When I Look At U"},
        ]
        self.assertEqual(plex_sync._fuzzy_pick(cands, "♥ GOJII ♥", "WHEN I LOOK AT U"), "2")

    def test_fuzzy_pick_rejects_wrong_title(self):
        cands = [{"rating_key": "1", "artist": "GOJII", "title": "Some Other Song"}]
        self.assertIsNone(plex_sync._fuzzy_pick(cands, "GOJII", "When I Look At U"))


class TestMatchTracks(unittest.TestCase):
    def setUp(self):
        self.db = _make_db()
        self.srv = FakePlexServer()

    def tearDown(self):
        for s in ("", "-wal", "-shm"):
            try:
                os.remove(self.db + s)
            except OSError:
                pass

    def test_path_match_and_fuzzy_fallback(self):
        # Track A: path maps directly to a Plex file.
        ta = _insert_track(self.db, "spA", "Song A", "Artist A", "/music/A/songA.flac")
        self.srv.add_track("11", "/volume1/music/A/songA.flac", "Artist A", "Song A")
        # Track B: Lexicon-host path (won't map) but matches via search.
        tb = _insert_track(self.db, "spB", "Song B", "Artist B", "/Volumes/music/B/songB.flac")
        self.srv.add_track("22", "/volume1/music/B/songB.flac", "Artist B", "Song B")
        # Track C: no Plex counterpart at all.
        _insert_track(self.db, "spC", "Ghost", "Nobody", "/music/C/ghost.flac")

        with self.srv.client() as client:
            res = plex_sync.match_tracks(self.db, client, _cfg())

        self.assertEqual(res["matched_path"], 1)
        self.assertEqual(res["matched_fuzzy"], 1)
        self.assertEqual(res["unmatched"], 1)
        self.assertEqual(plex_sync._get_track_rating_key(self.db, ta), "11")
        self.assertEqual(plex_sync._get_track_rating_key(self.db, tb), "22")

    def test_match_is_idempotent_skips_already_mapped(self):
        ta = _insert_track(self.db, "spA", "Song A", "Artist A", "/music/A/songA.flac")
        self.srv.add_track("11", "/volume1/music/A/songA.flac", "Artist A", "Song A")
        with self.srv.client() as client:
            plex_sync.match_tracks(self.db, client, _cfg())
            res2 = plex_sync.match_tracks(self.db, client, _cfg())
        # Second pass: already mapped -> nothing to do, no duplicate rows.
        self.assertEqual(res2["matched_path"], 0)
        conn = sqlite3.connect(self.db)
        n = conn.execute("SELECT COUNT(*) FROM plex_sync WHERE track_id = ?", (ta,)).fetchone()[0]
        conn.close()
        self.assertEqual(n, 1)


class TestScan(unittest.TestCase):
    def setUp(self):
        self.db = _make_db()
        self.srv = FakePlexServer()

    def tearDown(self):
        for s in ("", "-wal", "-shm"):
            try:
                os.remove(self.db + s)
            except OSError:
                pass

    def test_path_scoped_scan_dedups_dirs_never_global(self):
        # Two tracks in the same dir + one in another -> two unique refreshes.
        _insert_track(self.db, "s1", "T1", "A", "/music/Database/Artist1/t1.flac")
        _insert_track(self.db, "s2", "T2", "A", "/music/Database/Artist1/t2.flac")
        _insert_track(self.db, "s3", "T3", "A", "/music/Database/Artist2/t3.flac")
        with self.srv.client() as client:
            res = plex_sync.scan_new_imports(self.db, client, _cfg())

        self.assertEqual(res["scanned_dirs"], 2)
        scanned_paths = {p for _s, p in self.srv.refresh_calls}
        self.assertEqual(scanned_paths, {
            "/volume1/music/Database/Artist1", "/volume1/music/Database/Artist2",
        })
        # HARD invariant: never a global (path-less) refresh.
        self.assertEqual(self.srv.global_refresh_calls, 0)

    def test_scan_batch_caps_fanout(self):
        for i in range(5):
            _insert_track(self.db, f"s{i}", f"T{i}", "A", f"/music/D/dir{i}/t.flac")
        with self.srv.client() as client:
            res = plex_sync.scan_new_imports(self.db, client, _cfg(scan_batch=2))
        self.assertEqual(res["scanned_dirs"], 2)
        self.assertEqual(res["skipped"], 3)
        self.assertEqual(len(self.srv.refresh_calls), 2)


class TestMirrorReconcile(unittest.TestCase):
    def setUp(self):
        self.db = _make_db()
        self.srv = FakePlexServer()

    def tearDown(self):
        for s in ("", "-wal", "-shm"):
            try:
                os.remove(self.db + s)
            except OSError:
                pass

    def _seed_three_mapped_tracks(self):
        ids = []
        for i, rk in enumerate(("11", "22", "33"), start=1):
            tid = _insert_track(self.db, f"sp{i}", f"Song {i}", f"Artist {i}",
                                f"/music/M/song{i}.flac")
            self.srv.add_track(rk, f"/volume1/music/M/song{i}.flac", f"Artist {i}", f"Song {i}")
            plex_sync._upsert_track_mapping(self.db, tid, rk)
            ids.append(tid)
        return ids

    def test_creates_playlist_with_membership(self):
        ids = self._seed_three_mapped_tracks()
        _insert_monthly(self.db, "07. July 2026", ids)
        with self.srv.client() as client:
            res = plex_sync.mirror_playlists(self.db, client, _cfg())
        self.assertEqual(res["playlists"], 1)
        self.assertEqual(res["created"], 1)
        # The created Plex playlist has exactly the 3 members.
        pl = next(iter(self.srv.playlists.values()))
        self.assertEqual({it["rating_key"] for it in pl["items"]}, {"11", "22", "33"})

    def test_reconcile_is_idempotent_no_dupes(self):
        ids = self._seed_three_mapped_tracks()
        _insert_monthly(self.db, "07. July 2026", ids)
        with self.srv.client() as client:
            plex_sync.mirror_playlists(self.db, client, _cfg())
            res2 = plex_sync.mirror_playlists(self.db, client, _cfg())
        # Second run: no create, no add, no remove.
        self.assertEqual(res2["created"], 0)
        self.assertEqual(res2["added"], 0)
        self.assertEqual(res2["removed"], 0)
        pl = next(iter(self.srv.playlists.values()))
        # Exactly one Plex playlist, membership unchanged, NO duplicate items.
        self.assertEqual(len(self.srv.playlists), 1)
        keys = [it["rating_key"] for it in pl["items"]]
        self.assertEqual(sorted(keys), ["11", "22", "33"])
        self.assertEqual(len(keys), len(set(keys)))

    def test_reconcile_adds_missing_and_removes_stale(self):
        ids = self._seed_three_mapped_tracks()
        pid = _insert_monthly(self.db, "07. July 2026", ids)
        # Pre-existing Plex playlist has one desired member (11), one stale (99).
        self.srv.add_track("99", "/volume1/music/M/stale.flac", "Stale", "Stale")
        self.srv.add_playlist("07. July 2026", ["11", "99"])
        with self.srv.client() as client:
            res = plex_sync.mirror_playlists(self.db, client, _cfg())
        self.assertEqual(res["created"], 0)
        self.assertEqual(res["added"], 2)    # 22, 33 added
        self.assertEqual(res["removed"], 1)  # 99 removed
        pl = next(iter(self.srv.playlists.values()))
        self.assertEqual({it["rating_key"] for it in pl["items"]}, {"11", "22", "33"})
        # Playlist mapping cached in plex_sync.
        conn = sqlite3.connect(self.db)
        row = conn.execute(
            "SELECT rating_key FROM plex_sync WHERE playlist_id = ? AND track_id IS NULL", (pid,)
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)

    def test_non_monthly_playlists_ignored(self):
        ids = self._seed_three_mapped_tracks()
        _insert_monthly(self.db, "My Faves", ids)  # not MM. Month YYYY
        with self.srv.client() as client:
            res = plex_sync.mirror_playlists(self.db, client, _cfg())
        self.assertEqual(res["playlists"], 0)
        self.assertEqual(self.srv.create_calls, 0)

    def test_empty_monthly_skipped_no_create(self):
        # Monthly playlist whose members have no Plex mapping yet -> skip, no create.
        tid = _insert_track(self.db, "spX", "X", "X", "/music/M/x.flac")
        _insert_monthly(self.db, "08. August 2026", [tid])
        with self.srv.client() as client:
            res = plex_sync.mirror_playlists(self.db, client, _cfg())
        self.assertEqual(res["empty"], 1)
        self.assertEqual(res["created"], 0)
        self.assertEqual(self.srv.create_calls, 0)


class TestDisabledGate(unittest.TestCase):
    def setUp(self):
        self.db = _make_db()

    def tearDown(self):
        for s in ("", "-wal", "-shm"):
            try:
                os.remove(self.db + s)
            except OSError:
                pass

    def test_run_returns_none_when_disabled(self):
        # Default: plex_sync_enabled unset -> disabled -> inert no-op.
        self.assertIsNone(plex_sync._run_plex_sync(self.db))

    def test_run_returns_none_when_enabled_but_unconfigured(self):
        conn = sqlite3.connect(self.db)
        conn.execute("INSERT INTO app_config (key, value) VALUES ('plex_sync_enabled','1')")
        conn.commit()
        conn.close()
        self.assertIsNone(plex_sync._run_plex_sync(self.db))


if __name__ == "__main__":
    unittest.main()
