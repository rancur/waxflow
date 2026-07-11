# Changelog

## 2.6.0 — Direct-to-library import (bypass watch/incoming/Done) + sync-lag tolerance

Fixes the root cause of "new songs never enter the Lexicon library": imports were
POSTed with a music path Lexicon's host Mac could not read, so `POST /v1/tracks`
returned HTTP 200 but imported **0 tracks** (28 tracks were stuck this way), while
files piled up in Lexicon's Incoming awaiting a manual "Done" that has no headless
API.

### Fixed
- **Import now targets the Mac-LOCAL path, landing tracks straight in the library.**
  The Lexicon host reads its music from the Mac's internal disk
  (`/Volumes/Macintosh HD/Users/willcurran/Music/...`), NOT from the SMB share
  (`/Volumes/music/...`, which is not even mounted). WaxFlow's path-mapping config
  (`lexicon_library_path`, `lexicon_input_path`) must point at the Mac-local prefix.
  Verified end-to-end: `POST /v1/tracks` with the Mac-local location imports the
  track with `incoming=false` — directly into the searchable library, no watch
  folder, no Incoming queue, no manual Done. The prior SMB path was proven to
  import 0 tracks (the empty-import signature).

### Added
- **Sync-lag tolerance for empty imports.** A freshly-downloaded file lands on the
  NAS first and only reaches the Lexicon host after Synology Drive replicates
  `/volume1/music` → `/Users/willcurran/Music` (seconds-to-minutes). A `POST` in
  that window legitimately imports 0 tracks because the file is not on the Mac yet.
  `_process_organizing` now treats an empty import as **transient** — it keeps
  retrying in `organizing` (self-healing) until the file syncs — and only escalates
  to the loud `[lexicon_import_empty]` mount-down error once the file has stayed
  empty past a grace window (`lexicon_empty_import_grace_seconds`, default 30 min).
  The first-seen timestamp is persisted in `pipeline_error` via an
  `[empty_since:<ts>]` marker so the window survives worker restarts, and is cleared
  automatically on a successful import.

## 2.5.0 — Import-health detection, watch-folder canary & non-music filter

Makes silent Lexicon-import failures impossible to miss, and keeps non-music out
of the DJ library.

### Added
- **Empty-import is now a hard, distinct failure.** When `POST /v1/tracks`
  returns HTTP 200 but imports **0 tracks** (the signature of a Lexicon host that
  has lost the NAS music mount), `_lexicon_find_or_import` now raises
  `LexiconImportEmpty` instead of silently returning `None`. The track is set to a
  clearly-messaged error state tagged `[lexicon_import_empty]`, and the failure is
  counted and surfaced (it is no longer swallowed as success or a generic error).
- **Proactive import-health canary** (`sync-worker/tasks/lexicon_health.py`, runs
  every 15 min). For the current watch-folder architecture it verifies the two
  NAS-side dependencies of the import flow **before** real imports fail:
  (1) WaxFlow can write to the staging/watch dir (`/downloads` → NAS
  `/volume1/music/Input` → Lexicon's watch folder), and (2) the Lexicon API is
  reachable. On failure it raises a loud, specific status.
- **Import health on the API** — `GET /api/admin/health` now returns
  `import_health`, `lexicon_mount_ok`, `import_health_detail`,
  `import_health_checked_at`, and `lexicon_import_empty_count`. Overall `status`
  degrades to `degraded` when imports are broken, so a Kuma/HTTP check on the
  endpoint pages instead of hiding.
- **Self-heal monitor** (`scripts/monitor-parity.sh`) reads `import_health` and
  logs a `CRITICAL` line + optional webhook page (`WAXFLOW_ALERT_WEBHOOK`) when
  imports are silently failing. It deliberately does **not** restart containers
  for this (a restart cannot remount the Mac's NAS share).
- **Non-music ingest filter** (`sync-worker/tasks/nonmusic_filter.py`) — Spotify
  items that are not music are skipped at poll time: podcast episodes / audiobook
  objects (`type != track`), audiobook/spoken-word keywords (LibriVox, audiobook,
  `chapter N`, unabridged, …), and anything over a configurable duration cap
  (default 30 min). Skips are logged as `nonmusic_skipped`, never silently.

### Config keys
- `nonmusic_filter_enabled` (default `1`), `nonmusic_max_duration_ms`
  (default `1800000` = 30 min)
- `lexicon_canary_interval_seconds` (default `900`)
- `lexicon_watch_dir` (default `/downloads`) — staging/watch dir the canary checks

### Tests
- `sync-worker/tests/test_import_health.py` — empty-import detection, organize
  routing, watch-folder canary, and the health recorder's transition-gated paging.
- `sync-worker/tests/test_nonmusic_filter.py` — non-music rule coverage.

### Notes / unchanged
- The dedup/link/import guards and the Drift/Drifting matcher are untouched and
  continue to pass.
