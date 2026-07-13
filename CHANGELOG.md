# Changelog

## 2.9.0 — Backup identification fallback for "no match" tracks (MusicBrainz + AcoustID scaffold)

Recovers liked tracks that show **"no match"** in Match Review because Spotify
removed the song, WITHOUT depending on the live Spotify track — using only the
metadata WaxFlow cached at like-time (ISRC, title, artist, album, duration_ms).
Answers the companion question in the runbook: rejecting a match keeps the file on
disk (only the DB `file_path` pointer is cleared), re-arms the track from `new`, and
never touches Lexicon.

### Added — MusicBrainz metadata/ISRC re-resolution fallback (`tasks/metadata_fallback.py`)
- New worker task (default **ON**, `metadata_fallback_enabled`) that scans unmatched
  tracks (`match_status='failed' / pipeline_stage='error'`) and re-resolves each via
  **MusicBrainz** (free, keyless): ISRC → recording → canonical title/artist + the
  recording's **full ISRC set across every release**; falls back to a duration-tie-
  broken recording search when the cached ISRC isn't catalogued.
- Re-attempts a match with the enriched metadata in priority order: **(0) already-owned
  local file** under `/music` (alt-ISRC then canonical name — the highest-value,
  Tidal-independent, no-download recovery), **(1) Tidal by alternate ISRC**, **(2) Tidal
  by canonical name**. A track pulled from Spotify under one ISRC frequently still
  exists locally or on Tidal under a different release/ISRC of the same recording.
- Recovered matches are surfaced in **Match Review** (`match_status='mismatched'`,
  `match_source='musicbrainz_local' | 'musicbrainz_isrc' | 'musicbrainz_search'`) as
  fallback-sourced proposals for human approve/reject — **never auto-imported**.
- Non-destructive (proposes a match; never deletes/moves files or writes Lexicon),
  idempotent + non-looping (one `source_attempts(source='musicbrainz')` row per track
  with exponential backoff), and **hunter-safe** (a `wanted` row `state='review'`
  shields the proposal from the missing-track hunter's re-arm).

### Added — Acoustic-fingerprint fallback scaffold (`tasks/acoustid_fallback.py`)
- Chromaprint/AcoustID identification of local candidate files, **config-gated and OFF**
  (`acoustid_fallback_enabled`). `fpcalc` is present in the worker image; provisioning
  is complete except for a free AcoustID key — seed `acoustid_api_key` in `app_config`
  and flip the flag (both read live, **no redeploy**) to activate. No key is fabricated;
  the task is an explicit logged no-op until provisioned.

### Web
- Match Review labels the new fallback sources and shows a **FALLBACK** badge so a
  recovered match is never mistaken for a live-Spotify-confirmed one. (Ships on the
  next web build; recovered matches surface regardless.)

## 2.8.1 — Match Review: fix 500 + side-by-side audio preview

Fixes the intermittent `API error: 500` in Match Review and adds an A/B audio
preview (Spotify + matched local file) to each review card. API-and-web only; the
worker soak is untouched.

### Fixed — Match Review 500
- **Root cause:** `sync-api/db.py` opened SQLite with WAL but no `busy_timeout`, so
  while the worker soak held a write lock the API's approve/reject write raised
  `database is locked` immediately → 500. Added `sqlite3.connect(..., timeout=30)`
  and `PRAGMA busy_timeout=5000` so the API waits for the lock instead of erroring.
- **Resilience:** `/api/matching/review` now guards per-row serialization in a
  try/except — one malformed row (e.g. a NULL in a required column) is skipped and
  logged instead of 500-ing the whole review list. Response includes `skipped`.

### Added — Side-by-side audio preview in Match Review
- **`GET /api/matching/{track_id}/file`** streams the matched local audio file with
  HTTP Range support (Starlette `FileResponse` → 206, seekable `<audio>`), a correct
  audio content-type, and path-safety: the resolved real path must stay inside
  `MUSIC_LIBRARY_PATH` (rejects `..`/symlink escape with 403).
- **Web:** each review card now shows two players side by side — a Spotify **embed
  iframe** (`open.spotify.com/embed/track/{id}`, reliable despite 2024 preview_url
  removals) labeled "Spotify" with an "Open in Spotify" link, and an `<audio>`
  labeled "Your file" pointed at the stream endpoint. Both degrade gracefully when
  the Spotify id or local file is missing.

## Unreleased — Phase 3: Sleep-tolerant sync + real-time flow-on-like

Makes the sync survive the Lexicon Mac going to sleep, and cuts like→Lexicon latency
from the 300s poll to minutes. Reuses the v3 scaffold tables (`mac_availability`,
`import_queue`) — no schema rebuild. All new behaviour is behind default-OFF config
flags, so it is INERT until the batched deploy flips it on. Non-destructive: only
enqueues + applies via the existing safe organize path (incl. the Phase 2
direct-write when enabled); never deletes.

### Added — `tasks/mac_availability.py` (availability detector)
- One detector for "is it safe to push Lexicon work right now", recording rolling
  samples into `mac_availability`. Distinguishes **asleep** (Mac unreachable — TCP
  reachability port closed) from **lexicon_down** (Mac up, TCP open, but Lexicon API
  not answering) from **available** (API 200). Reuses the canary's `/v1/playlists`
  probe. Registered as a cheap 60s worker sampler (pure observability).

### Added — `tasks/offline_queue.py` (durable offline import queue)
- When Lexicon is unavailable, organizing tracks are ENQUEUED into `import_queue`
  (durable in sync.db) and left parked in `organizing` — the NAS side (poll/match/
  download) keeps running; no error flood, no lost work. On wake, `drain()` applies
  each item oldest-first through the safe `_organize_track` path, idempotently
  (playlist membership check / INSERT OR IGNORE / diff-guarded comment ⇒ no
  double-apply), with exponential backoff on failure and a clean stop if Lexicon is
  lost mid-drain. Survives worker restarts (queue is on disk). Gated by
  `offline_queue_enabled` (default off). Heartbeat counts on `/stats`.

### Changed — `tasks/poll_spotify.py` (real-time flow-on-like)
- Spotify's Web API has **no push/webhook for saved/liked tracks** (confirmed), so
  "real-time" = a tighter poll with cheap change-detection: incremental polls use a
  small configurable page (`spotify_incremental_page_size`, default 20) and break at
  the newest-first cutoff, so a "nothing new" tight poll costs a **single tiny API
  call**. Interval stays configurable (`spotify_poll_interval_seconds`) — drop to
  ~30-60s at deploy for minutes-not-hours latency. Added Retry-After-aware 429
  backoff so a short interval can't hammer the API. Full backfill still uses big pages.

### Schema — additive only
- `import_queue.next_retry_at` (nullable) added via guarded ADD COLUMN in
  `v3_schema.ensure_v3_schema` + mirrored in `sync-api/init_db.py`. Cheap, non-locking,
  idempotent — no table rebuild.

### Tests
- `test_mac_availability.py`, `test_offline_queue.py`, `test_poll_fastpoll.py` cover
  asleep/lexicon-down/available detection, enqueue/drain idempotency, restart-survival,
  backoff + mid-drain loss, and tighter-poll change-detection + 429 backoff.

## Unreleased — Phase 0: REAL Lexicon library-DB backup (the safety net)

Closes the scariest gap in the whole system: Will's entire DJ library — tracks,
playlists, cue points, tags, links — lives in ONE SQLite DB on his Mac
(`~/Library/Application Support/lexicon/main.db`, WAL, ~150 MB) and had **never been
backed up**. The old `backup_lexicon.py` only pinged the Lexicon API and *falsely*
logged "backup verified"; Time Machine skips `~/Music/Database`; Wasabi HyperBackup
covers music FILES but not this DB. This lands a real, verified, two-location backup
that gates all later delicate work.

### Added — `scripts/backup-lexicon-db.sh` (the real backup)
- Runs on the ops Mac (the only host that can SSH both the Lexicon Mac and the NAS).
  NON-DESTRUCTIVE: only reads the DB, only writes new files; never quits Lexicon or
  restarts anything.
- Consistent **SQLite online backup** (`sqlite3 "file:$DB?mode=ro" ".backup"`) — no
  lock, no app-quit, captures live WAL-committed state. Verifies
  `PRAGMA integrity_check == ok` **and** `Track > 0`, then gzips.
- Two rotated copies (`KEEP=14`): **Mac** `~/WaxFlow-Backups/lexicon-db/` and **NAS**
  `/volume1/homes/willcurran/WaxFlow-Backups/lexicon-db/`; NAS copy verified by
  `gunzip -t` + sha256 match.
- Low-perf: `nice -n 19` throughout; **defers the NAS push while a HyperBackup runs**
  (Mac copy still taken, next run retries). Streams over `ssh cat` (Synology scp/sftp
  subsystem is disabled). Fail-loud heartbeat JSON + log under `~/.waxflow/logs/`.
- Scheduled daily via LaunchAgent `com.openclaw.waxflow-lexicon-backup` (plist template
  added); also run manually before any delicate op.

### Changed — `tasks/backup_lexicon.py` is now honest
- No longer INSERTs a phantom "backup verified" row from an API ping. Records a
  truthful `lexicon_api_probe` liveness event only and points at the external real
  backup. `scripts/backup-lexicon.sh` annotated as a container-side no-op fast-path.

### Verified
- Initial backup taken 2026-07-12: `integrity_check = ok`; Track≈5714, Playlist 310,
  Cuepoint 28777, LinkTrackPlaylist 47345; two sha-matched copies (Mac + NAS).

## Unreleased — v3 Feature 4: Plex/Plexamp mirror (additive, inert)

Mirrors what WaxFlow syncs into Lexicon over to the Plex server that runs **on the NAS**
(`http://192.168.1.221:32400`, same `/volume1/music` tree) so the monthly `MM. Month YYYY`
playlists show up in Plexamp. **Additive and inert**: gated behind the default-off
`plex_sync_enabled` flag and **not wired** into `worker.py`'s loop (Phase C wires it in a
quiet window). A READ-ONLY consumer of audio files — it only writes Plex's own playlist/scan
state and the WaxFlow `plex_sync` cache table; it never moves or rewrites a file.

### Added — `tasks/plex_client.py` (thin httpx Plex client, no `plexapi` dep)
- `X-Plex-Token` + JSON client wrapping only the endpoints the mirror needs: list sections,
  paginate a music section's tracks, **path-scoped** library refresh, section search, and
  audio-playlist list/items/create/add/remove. Constructor takes an injectable transport so
  tests drive real request shapes through a mock Plex server.

### Added — `tasks/plex_sync.py` (mirror task)
- **Scan**: targeted `PUT /library/sections/{id}/refresh?path=…` per unique parent directory,
  batched/debounced (`plex_scan_batch`, default 25). **Never** a global full scan (storm risk).
- **Match**: WaxFlow track → Plex `ratingKey` by file **path first** (container `/music/…` ==
  Plex `/volume1/music/…`), falling back to a normalized artist+title search; result cached in
  `plex_sync`.
- **Mirror**: reconciles ALL `MM. Month YYYY` monthly playlists into Plex audio playlists so
  membership **equals** the monthly list (create/add/remove). Fully idempotent — a second run
  makes zero changes and creates no duplicate memberships.

### Added — config (`init_db.py`, all default-off/generic)
- `plex_sync_enabled` (`0`), `plex_url`, `plex_music_section_id` (empty, env-seeded like
  `lexicon_api_url`), `plex_music_container_prefix` (`/music`), `plex_music_server_prefix`
  (`/volume1/music`), `plex_scan_batch` (`25`). The **`plex_token` is never committed** — it is
  self-generated from the server's `Preferences.xml`, stored in 1Password
  ("Plex — WaxFlow token (Barry)"), and seeded into the live `app_config` out of band.

### Tests — `tests/test_plex_sync.py`
- Mock Plex server (`httpx.MockTransport`): path-scoped scan (dedups dirs, never global; batch
  cap), path+fuzzy matching, playlist create/reconcile (add missing + remove stale), and
  idempotency (run-twice → no changes, no duplicates). Disabled/unconfigured gate is a no-op.

## 2.8.0 — v3 Phase A foundation (additive schema + source-plugin abstraction)

Foundation for the WaxFlow v3 build. Everything here is **additive and inert**: new
tables/columns and new modules behind default-off flags, wired into **nothing** in the
live worker loop yet. No runtime behavior changes — the Tidal/Soulseek refactor is a
pure, characterization-proven pass-through. It "bakes in" at the next coordinated deploy.

### Added — additive v3 schema (`tasks/v3_schema.ensure_v3_schema`, mirrored in `init_db.py`)
- Seven new tables (all `CREATE TABLE IF NOT EXISTS`): `wanted`, `source_attempts`,
  `purchase_links`, `import_queue`, `plex_sync`, `direct_write_audit`, `mac_availability`,
  plus supporting indexes.
- Two new **nullable** `tracks` columns (guarded `ALTER TABLE ADD COLUMN`): `sourceability`,
  `wanted_id`.
- **Additive-only**: no `tracks` rebuild, no CHECK-constraint change, no data migration.
  Idempotent (safe to re-run). The legacy `fallback_attempts` table is left **intact** —
  `source_attempts` is the forward per-source attempt log.

### Added — source-plugin abstraction (`tasks/sources/`)
- `base.py`: `SourceCapability` enum, `TrackQuery`/`SourceResult` dataclasses, the `Source`
  base class, and a shared `SourceBackoff` helper (exponential backoff on `source_attempts`).
- `registry.py`: `all_sources()`, `acquire_sources()`/`link_sources()` (priority-sorted),
  `get_source()`, `enabled_acquire_sources()`; enable/disable per source via `app_config`.
- `tidal.py` / `soulseek.py`: wrap the **existing** Tidal (`_tidal_search` +
  `_download_track_via_tiddl`) and Soulseek (`soulseek_fallback`) logic behind the `Source`
  interface. The pipeline's call-sites now route through these adapters with **zero**
  behavior change (the adapters delegate to the same implementations).

### Tests
- +35 tests (schema creation/idempotency + init_db mirror, registry ordering/backoff/toggle,
  and **characterization** tests proving the source adapters are byte-identical to the
  inline Tidal/Soulseek code — same returns, same HTTP requests, same subprocess argv/dest).

## 2.7.0 — Lossy-only auto-upgrade re-check

Some liked tracks are kept as a **lossy** copy because, at import time, no genuinely
lossless copy existed anywhere — Tidal offered only lossy AAC and Soulseek had no FLAC
(e.g. "Mob Tactics - Labyrinth", "Annix x Mefjus - Shai Hulud VIP"). Will's standard is
lossless everywhere it is obtainable, so rather than leave those lossy forever, WaxFlow
now keeps the lossy as a placeholder and periodically **re-checks** whether a lossless
copy has since appeared — swapping it in automatically if one has.

### Added
- **Marker + detection** (`tasks/lossless_upgrade.py`): two lightweight columns on
  `tracks` — `lossless_upgrade_pending` (0/1) and `last_upgrade_check` (ISO ts), added
  idempotently (`ALTER TABLE ADD COLUMN`, no table rebuild, mirrored in `init_db.py`).
  `mark_pending()` conservatively flags `complete` tracks that are **not** genuinely
  lossless (verified-lossy, or a plainly lossy file extension). A track whose file looks
  lossless, or is `is_protected`, is never marked.
- **Throttled periodic re-check** wired into the worker loop: each track is re-checked at
  most once every N days (default 7) and each cycle processes a small bounded batch
  (default 2). Off in `scan` mode and behind `lossless_upgrade_enabled`. NAS-friendly by
  design (slow loop + per-track throttle + tiny batch).
- **Re-source through the existing gate:** a due track is re-attempted through a fresh
  Tidal-lossless search+download and then the Soulseek fallback, **every candidate gated
  by `lossless_verify`** (same fake-FLAC/lossy protection as the live pipeline).
- **In-place swap, dedup-safe:** on a verified-lossless source, the **existing** Lexicon
  track is relocated to the new file in place (same track id — no new track, so no
  duplicate), self-verified by reading the location back, then the marker is cleared.

### Guarantee
- **Never leaves Will with neither:** a lossy track is never removed or demoted unless a
  genuinely-lossless replacement has been sourced, verified, **and** confirmably installed
  in Lexicon. If nothing lossless is found, or the relocate can't be confirmed, the lossy
  is kept untouched and the freshly-sourced copy is discarded — no false "upgraded" state.
- New tests cover the marker, the throttle, the swap-on-lossless-found path, and the
  never-remove-without-replacement guard (14 tests).

> Committed but **not** deployed to the running image (NAS was under HyperBackup + a
> backfill was finishing). Bakes in at the next coordinated `docker compose build`.

## 2.6.2 — Downloaded tracks actually reach Lexicon (Synology ACL + SMB delivery)

Fixes the show-critical bug where freshly-**downloaded** tracks never reached the
Lexicon Mac (April & June 2026 completely missing, May partial) while **linked**
tracks appeared fine.

### Root cause
The worker placed each finished download with `shutil.move` + `os.chmod`. On the
Synology NAS, the `/volume1/music` share carries an inheritable
`user:SynologyDrive:allow` ACL that a fresh file inherits — but **any mode change
(`os.chmod`, or `shutil.move`/`copy2`'s `copystat`) strips that ACL and converts
the file to POSIX "Linux mode", which Synology Drive Server cannot see.** Stranded
files never synced to the Mac, so Lexicon could not import them. Linked tracks need
no file move, so they were unaffected. A month was "completely missing" when all of
its likes needed downloads. (Proven empirically 2026-07-11: identical file WITH
chmod = "Linux mode" + not synced; WITHOUT chmod = inherited ACL + synced.)

### Fixed
- **ACL-preserving placement:** `_download_track_via_tiddl` now uses a data-only
  `shutil.copyfile` + `os.remove` (never `shutil.move`) and drops the `os.chmod`
  calls. `os.chown` (which does NOT strip the ACL) still sets the Plex owner. Fresh
  downloads keep the inherited Synology ACL and propagate to the Mac's `~/Music`
  Synology replica.
- **Live SMB delivery for Lexicon import:** default `lexicon_library_path` is now
  `/Volumes/music` (and `lexicon_input_path` `/Volumes/music/Input`) — the Mac's
  live SMB view of the NAS share. SMB reflects the NAS filesystem instantly (no
  sync lag, no ACL/change-event dependency), so downloads are importable the moment
  they are written. A self-healing launchd agent keeps `/Volumes/music` mounted.
- **Regression guard:** extracted `_container_to_mac_path()` with tests pinning the
  `/music -> /Volumes/music` mapping (the exact bug), and updated the import-health
  canary + grace-window docs to the SMB delivery model.

## 2.6.1 — Resumable, lock-resilient Spotify liked-songs backfill

Makes the full liked-songs backfill actually **complete** under real load. WaxFlow's
DB held only ~998 of Will's ~5,550 all-time Spotify likes: the incremental poller
stops at the first track added at/before `last_spotify_poll` (likes come back
newest-first), so once that cutoff is set it never walks back into older history.
The one-shot `backfill_all_liked` mode (which ignores the cutoff and paginates the
whole library) existed but, when triggered, could not finish — the burst of INSERTs
raced the pipeline's concurrent writes and the poll task crashed on
`database is locked`, and every worker restart re-walked from offset 0.

### Fixed
- **Lock-resilient inserts:** backfill INSERTs now retry transient
  `database is locked` (short escalating backoff) instead of crashing the poll task
  and aborting the walk. Unrelated `OperationalError`s still propagate immediately.
- **Resumable backfill:** the page offset is checkpointed to `app_config`
  (`backfill_offset`) after each page and resumed on restart, so redeploys/crashes
  continue the walk instead of restarting from 0.
- **Completion-gated flag clear:** the one-shot `backfill_all_liked` flag is cleared
  only when the walk reaches the end of the library. A backfill that exits early on a
  Spotify API error keeps the flag set (and the offset persisted) so the next cycle
  resumes and finishes, rather than silently dropping to incremental with a partial
  library. Each track is still inserted with its real `spotify_added_at`.

Dedup/link-vs-import-vs-review guards are unchanged (existing tests green): the
backfill relies on them to LINK most likes to Will's existing Lexicon library
without downloading or duplicating.

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
