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
                CHECK(pipeline_stage IN ('new','matching','downloading','verifying','organizing','complete','error','waiting','ignored')),
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
            source TEXT NOT NULL DEFAULT 'tiddl'
                CHECK(source IN ('tiddl','tidarr','beatport','bandcamp')),
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
            ('fingerprint_min_score', '0.70'),
            ('music_library_path', '/music'),
            ('activity_log_retention_days', '365'),
            ('synology_sync_delay_seconds', '3'),
            ('auto_analyze_enabled', '1'),
            ('lexicon_post_processing', 'analyze,cues,tags,cloud'),
            ('sync_mode', 'scan'),
            ('webhook_url', ''),
            ('lexicon_library_path', '/music/library'),
            ('lexicon_input_path', '/music/downloads'),
            ('tidal_download_quality', 'max'),
            ('downloads_path', '/downloads'),
            ('lexicon_api_url', ''),
            ('tidarr_url', ''),
            ('plex_uid', '1000'),
            ('plex_gid', '1000'),
            ('retry_search_interval_seconds', '43200'),
            ('lexicon_legacy_path_prefixes', ''),
            ('analyze_interval_seconds', '3600'),
            ('analyze_batch_size', '20'),
            ('analyze_total_processed', '0'),
            ('auto_update_enabled', '0'),
            ('auto_update_schedule', 'daily_3am'),
            ('auto_backup_before_update', '1'),
            ('last_update_check', ''),
            ('auto_playlists_enabled', '1'),
            ('auto_playlists_genres', '1'),
            ('auto_playlists_energy', '1'),
            ('auto_playlists_danceability', '1'),
            ('auto_playlists_popularity', '1'),
            ('auto_playlists_happiness', '1'),
            ('auto_playlists_rating', '1'),
            ('auto_playlists_bpm', '1'),
            ('auto_playlists_key', '1'),
            ('auto_playlists_interval_seconds', '86400'),
            ('auto_playlists_last_run', ''),
            ('auto_playlists_created_ids', '{}'),
            ('auto_playlists_rebuild', '0');
        """)
    # Migration: add 'waiting' to pipeline_stage CHECK constraint
    # SQLite can't ALTER CHECK constraints, so we recreate the table if needed
    with get_db() as conn:
        # Check if the 'waiting' value is already allowed
        try:
            conn.execute("UPDATE tracks SET pipeline_stage = 'waiting' WHERE 0")  # no-op, just tests constraint
        except Exception:
            # Constraint doesn't include 'waiting' -- need to migrate
            print("Migrating tracks table to add 'waiting' pipeline stage...")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tracks_new (
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
                        CHECK(pipeline_stage IN ('new','matching','downloading','verifying','organizing','complete','error','waiting','ignored')),
                    pipeline_error TEXT,
                    is_protected INTEGER NOT NULL DEFAULT 0,
                    notes TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                INSERT OR IGNORE INTO tracks_new SELECT * FROM tracks;

                DROP TABLE tracks;

                ALTER TABLE tracks_new RENAME TO tracks;
            """)
            print("Migration complete.")

    # Migration: add 'ignored' to pipeline_stage CHECK constraint
    with get_db() as conn:
        try:
            conn.execute("UPDATE tracks SET pipeline_stage = 'ignored' WHERE 0")  # no-op, just tests constraint
        except Exception:
            print("Migrating tracks table to add 'ignored' pipeline stage...")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tracks_new (
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
                        CHECK(pipeline_stage IN ('new','matching','downloading','verifying','organizing','complete','error','waiting','ignored')),
                    pipeline_error TEXT,
                    is_protected INTEGER NOT NULL DEFAULT 0,
                    notes TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                INSERT OR IGNORE INTO tracks_new SELECT * FROM tracks;

                DROP TABLE tracks;

                ALTER TABLE tracks_new RENAME TO tracks;
            """)
            print("Migration complete.")

    # Migration: add 'tiddl' to download_queue source CHECK constraint
    with get_db() as conn:
        try:
            conn.execute("UPDATE download_queue SET source = 'tiddl' WHERE 0")  # no-op, just tests constraint
        except Exception:
            print("Migrating download_queue table to add 'tiddl' source...")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS download_queue_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    track_id INTEGER NOT NULL REFERENCES tracks(id),
                    priority INTEGER NOT NULL DEFAULT 0,
                    source TEXT NOT NULL DEFAULT 'tiddl'
                        CHECK(source IN ('tiddl','tidarr','beatport','bandcamp')),
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','queued','downloading','complete','failed')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    error TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    started_at TEXT,
                    completed_at TEXT
                );

                INSERT OR IGNORE INTO download_queue_new SELECT * FROM download_queue;

                DROP TABLE download_queue;

                ALTER TABLE download_queue_new RENAME TO download_queue;
            """)
            print("Migration complete.")

    print("Database initialized successfully.")


if __name__ == "__main__":
    init()
