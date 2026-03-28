#!/usr/bin/env python3
"""Initialize the sync database with all required tables."""

from db import get_db


def init():
    with get_db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE NOT NULL,
            spotify_uri TEXT,
            spotify_added_at TEXT,
            title TEXT,
            artist TEXT,
            album TEXT,
            duration_ms INTEGER,
            isrc TEXT,
            spotify_popularity INTEGER,
            match_status TEXT NOT NULL DEFAULT 'pending'
                CHECK(match_status IN ('pending','matched','mismatched','manual','failed')),
            match_source TEXT,
            match_confidence REAL,
            tidal_id TEXT,
            download_status TEXT NOT NULL DEFAULT 'pending'
                CHECK(download_status IN ('pending','queued','downloading','complete','failed','skipped')),
            download_source TEXT,
            download_attempts INTEGER NOT NULL DEFAULT 0,
            download_error TEXT,
            file_path TEXT,
            file_hash_sha256 TEXT,
            verify_status TEXT NOT NULL DEFAULT 'pending'
                CHECK(verify_status IN ('pending','pass','fail','skipped')),
            verify_codec TEXT,
            verify_sample_rate INTEGER,
            verify_bit_depth INTEGER,
            verify_is_genuine_lossless INTEGER,
            chromaprint TEXT,
            fingerprint_match_score REAL,
            lexicon_status TEXT NOT NULL DEFAULT 'pending'
                CHECK(lexicon_status IN ('pending','synced','skipped','error')),
            lexicon_track_id TEXT,
            lexicon_playlist_id TEXT,
            pipeline_stage TEXT NOT NULL DEFAULT 'new'
                CHECK(pipeline_stage IN ('new','matching','downloading','verifying','organizing','complete','error')),
            pipeline_error TEXT,
            is_protected INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folder_name TEXT,
            playlist_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            lexicon_folder_id TEXT,
            lexicon_playlist_id TEXT,
            track_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(year, month)
        );

        CREATE TABLE IF NOT EXISTS playlist_tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            playlist_id INTEGER NOT NULL REFERENCES playlists(id),
            track_id INTEGER NOT NULL REFERENCES tracks(id),
            position INTEGER,
            added_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(playlist_id, track_id)
        );

        CREATE TABLE IF NOT EXISTS lexicon_backups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            backup_path TEXT NOT NULL,
            backup_size_bytes INTEGER,
            trigger TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS download_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_id INTEGER NOT NULL REFERENCES tracks(id),
            priority INTEGER NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'tidarr'
                CHECK(source IN ('tidarr','beatport','bandcamp')),
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending','queued','downloading','complete','failed')),
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            error TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            started_at TEXT,
            completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS fallback_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_id INTEGER NOT NULL REFERENCES tracks(id),
            source TEXT NOT NULL,
            status TEXT NOT NULL,
            error TEXT,
            search_query TEXT,
            result_count INTEGER,
            attempted_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS activity_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            track_id INTEGER REFERENCES tracks(id),
            message TEXT NOT NULL,
            details TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        -- Default config values (insert or ignore to preserve existing)
        INSERT OR IGNORE INTO app_config (key, value) VALUES
            ('spotify_poll_interval_seconds', '300'),
            ('lexicon_backup_before_sync', '1'),
            ('max_concurrent_downloads', '2'),
            ('fingerprint_min_score', '0.85'),
            ('music_library_path', '/music');
        """)
    print("Database initialized successfully.")


if __name__ == "__main__":
    init()
