"""Tests for lexicon_direct_write — the link-only direct SQLite writer.

Builds a minimal Lexicon-shaped DB (Track + Playlist + LinkTrackPlaylist + the FTS
content table & update trigger) and asserts the writer reproduces exactly the two
link-only writes the HTTP path makes, idempotently, keeping FTS + integrity consistent.
"""

import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tasks.lexicon_direct_write import (  # noqa: E402
    LinkSpec,
    apply_link_only_writes,
    monthly_playlist_name,
    resolve_monthly_playlist_id,
)


def _make_lexicon_db(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE Track (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL, artist TEXT NOT NULL, albumTitle TEXT NOT NULL,
            comment TEXT NOT NULL, location TEXT NOT NULL,
            dateModified TEXT NOT NULL
        );
        CREATE TABLE Playlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, type TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE LinkTrackPlaylist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            playlistId INTEGER NOT NULL, trackId INTEGER NOT NULL, position INTEGER NOT NULL,
            FOREIGN KEY(trackId) REFERENCES Track(id) ON DELETE CASCADE,
            FOREIGN KEY(playlistId) REFERENCES Playlist(id) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX LinkTrackPlaylist_PlaylistIdTrackId
            ON LinkTrackPlaylist(trackId, playlistId);
        CREATE VIRTUAL TABLE Track_FTS USING fts5(
            title, artist, albumTitle, content='Track', content_rowid='id');
        CREATE TRIGGER Track_FTS_Insert AFTER INSERT ON Track BEGIN
            INSERT INTO Track_FTS(rowid, title, artist, albumTitle)
            VALUES (NEW.id, NEW.title, NEW.artist, NEW.albumTitle);
        END;
        CREATE TRIGGER Track_FTS_Update AFTER UPDATE ON Track BEGIN
            INSERT INTO Track_FTS(Track_FTS, rowid, title, artist, albumTitle)
            VALUES ('delete', OLD.id, OLD.title, OLD.artist, OLD.albumTitle);
            INSERT INTO Track_FTS(rowid, title, artist, albumTitle)
            VALUES (NEW.id, NEW.title, NEW.artist, NEW.albumTitle);
        END;
        """
    )
    # two tracks already in Lexicon (link-only), one monthly playlist that pre-exists
    conn.execute("INSERT INTO Track(id,title,artist,albumTitle,comment,location,dateModified)"
                 " VALUES (10,'Song A','Artist A','Album A','','/m/a.flac','2020-01-01T00:00:00.000Z')")
    conn.execute("INSERT INTO Track(id,title,artist,albumTitle,comment,location,dateModified)"
                 " VALUES (11,'Song B','Artist B','Album B','old note','/m/b.flac','2020-01-01T00:00:00.000Z')")
    conn.execute("INSERT INTO Playlist(id,name,type) VALUES (5,'01. January 2016','2')")
    # a track already present in the playlist to exercise position append
    conn.execute("INSERT INTO Track(id,title,artist,albumTitle,comment,location,dateModified)"
                 " VALUES (9,'Seed','S','S','','/m/s.flac','2020-01-01T00:00:00.000Z')")
    conn.execute("INSERT INTO LinkTrackPlaylist(playlistId,trackId,position) VALUES (5,9,0)")
    conn.commit()
    conn.close()


def test_monthly_playlist_name():
    assert monthly_playlist_name("2016-01-11T20:16:01Z") == "01. January 2016"
    assert monthly_playlist_name("2014-12-01T00:00:00Z") == "12. December 2014"


def test_apply_link_only_writes(tmp_path):
    db = str(tmp_path / "lex.db")
    _make_lexicon_db(db)

    with sqlite3.connect(db) as c:
        assert resolve_monthly_playlist_id(c, "2016-01-11T20:16:01Z") == 5
        assert resolve_monthly_playlist_id(c, "2099-05-01T00:00:00Z") is None

    specs = [
        LinkSpec(lexicon_track_id=10, playlist_id=5, spotify_id="SPOT10"),
        LinkSpec(lexicon_track_id=11, playlist_id=5, spotify_id="SPOT11"),
    ]
    res = apply_link_only_writes(db, specs)
    assert res.linked == 2 and res.comment_set == 2 and not res.errors

    conn = sqlite3.connect(db)
    # links appended after the seed (position 0) -> 1 and 2, unique per playlist
    rows = conn.execute("SELECT trackId, position FROM LinkTrackPlaylist WHERE playlistId=5 "
                        "ORDER BY position").fetchall()
    assert rows == [(9, 0), (10, 1), (11, 2)]
    # comment set to the exact [sls:] tag (overwrites 'old note' on track 11, mirrors API)
    assert conn.execute("SELECT comment FROM Track WHERE id=10").fetchone()[0] == "[sls:SPOT10]"
    assert conn.execute("SELECT comment FROM Track WHERE id=11").fetchone()[0] == "[sls:SPOT11]"
    # FTS stayed consistent through the comment UPDATE trigger
    assert conn.execute("SELECT count(*) FROM Track_FTS('Artist')").fetchone()[0] >= 1
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    conn.close()


def test_idempotent_rerun(tmp_path):
    db = str(tmp_path / "lex.db")
    _make_lexicon_db(db)
    specs = [LinkSpec(lexicon_track_id=10, playlist_id=5, spotify_id="SPOT10")]
    apply_link_only_writes(db, specs)
    res2 = apply_link_only_writes(db, specs)  # second run must be a full no-op
    assert res2.linked == 0 and res2.link_already == 1
    assert res2.comment_set == 0 and res2.comment_already == 1
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT count(*) FROM LinkTrackPlaylist WHERE trackId=10").fetchone()[0] == 1
    conn.close()
