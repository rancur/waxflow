# Changelog

## 2.14.0 — retroactive dateAdded from your Spotify liked dates

Lexicon stamps `dateAdded` with the moment a file was imported, so a library
assembled by WaxFlow shows thousands of tracks "added" on the day the sync ran.
The real date is already in `tracks.spotify_added_at`, going back to 2014.

Measured on the live library: **5,255 tracks are wrong, 996 of them by 11 years.**
Sorting by date added is meaningless until that is fixed.

`scripts/backfill-lexicon-dateadded.py` corrects it. Lexicon's API refuses the
field outright -- `PATCH /v1/track` with `dateAdded` returns
`'dateAdded' is not editable`, while `comment` on the same request returns 200 --
so this is a direct SQLite write, gated the same way `repoint-lexicon-local.sh` is:

  * a fresh **verified** backup must exist (integrity ok, Track > 0)
  * **Lexicon must be quit** -- a running Lexicon caches rows and would overwrite
    the change on exit
  * **dry-run by default**; `--apply`, and `--limit N` for a small first batch
  * `integrity_check` + `foreign_key_check` before and after
  * cue point, beat grid, playlist and cloud-link counts compared before and after,
    because a `dateAdded`-only write must not move anything else
  * one transaction, rolled back whole on any failure, with a per-row audit log

It touches `Track.dateAdded` and nothing else -- in particular not `location` and
not `locationUnique`, Lexicon's immutable import-identity key.

It runs on the Mac. Not because the database is unreachable elsewhere, but because
SQLite must not be written over SMB/NFS, where advisory locking is unreliable and
`BEGIN IMMEDIATE` cannot be trusted. Running locally also lets it verify Lexicon is
genuinely quit instead of inferring it from an API timeout.

### One bug worth naming

WaxFlow stores `lexicon_track_id` as TEXT; Lexicon's `Track.id` is INTEGER.
Comparing them raw matches **nothing** and silently degrades to path matching --
which took ID matches from 5,130 to zero without raising a thing. Found by
checking a dry-run's match counts rather than trusting them. Both the coercion and
that failure mode are now covered by tests.

## 2.13.2 — fix: /api/dashboard returned 500

The service-health section of `get_dashboard()` runs *after* the
`with get_db() as conn:` block has exited, so the connection still in lexical
scope there is already closed. 2.13.0 added a Soulseek row that read from
`app_config` using it, and the app's front page started returning:

    {"detail":"Cannot operate on a closed database."}

Nothing caught it. The unit tests passed, all four containers came up healthy,
and `/api/admin/health` was green -- because none of them actually requested the
page. **The dashboard endpoint had no test at all.** It has one now, and it
reproduces the exact 500 when the fix is reverted.

The health helper opens its own connection instead of borrowing one whose
lifetime it does not control.

Also: the Soulseek status detail claimed "logged in as ?" because slskd's
`/api/v0/server` has no username field. It now reports the server address and
connection state, which is what the response actually contains.

## 2.13.1 — fix: coverage measured only the first 1000 tracks

Lexicon caps `GET /v1/tracks` at 1000 rows per request and rejects any larger
limit outright. The coverage task read the unpaginated response, so on a
5,611-track library it silently measured the oldest fifth and reported confident
percentages for it.

This is the failure mode a percentage is worst at showing: a full 1000 tracks came
back, every number looked plausible, and the only tell was the log line reading
`1000 active tracks` for a library with 5,611. Caught by watching the first run
after deploying 2.13.0 rather than by anything the code reported.

Now paged via `limit`/`offset` against the reported `total`, with a bounded page
count. Tests cover the exact-multiple-of-page-size boundary, which is where a
naive loop stops one page early or runs one too many.

## 2.13.0 — visible failures, real retries, and the wrong-version fix

Three problems that were reported as feature requests turned out to be something
other than they looked. Measuring them first is what made this release small.

### The Missing Tracks page was never empty — it was erroring

`/upload` asked for `per_page=500` from an endpoint capped at 200. The API replied
with a 422 explaining exactly that, and the page rendered a clean empty table.

The reason it survived so long is `api.ts`: it threw away the response body and
raised `Error("API error: 422")`, so every caller's `catch { setRows([]) }` turned a
loud, specific server error into "you have no missing tracks". The client now keeps
the status and FastAPI's `detail`, retries only transport faults and 5xx (never a
4xx, which is deterministic), and the page pages through all 270 errors instead of
truncating at the cap.

### "Fingerprint mismatch" involved no fingerprints

The category matched on duration, not on the chromaprint WaxFlow computes and
stores. 108 of the 109 tracks in it came from one path: the file-index title+artist
matcher, which accepted any file whose title and artist agreed — so an extended
mix, a radio edit or a live take all matched. Verify then rejected the file on
duration and parked the track as an error.

Measured across 265 such matches on the live library: 155 land within 5s of the
Spotify duration and 110 exceed it, 95 of those by more than 15s. The two
populations separate cleanly, so title+artist matches now require the durations to
agree within 5s (configurable; fails open when either duration is unknown, as ~0.4%
of indexed files have none). The category is renamed **Wrong Version**, which is
what it always was.

### Post-processing was working the whole time

Of 400 tracks imported since July: 400 had BPM, 399 had cue points, 394 artwork,
399 cloud-backed — and only 4 tracks in the entire library lacked cues. Nothing was
broken; there was no way to see it. The dashboard now shows coverage for cue
points, beat grids, BPM, key, genre and tags, sampled hourly by the worker.

It also honestly reports what it *cannot* measure: Lexicon's API exposes no artwork
or cloud-upload field, so those are named as unavailable rather than guessed at.

One real bug did surface here: **"Auto-Analyze After Sync" silently disabled cue
generation, tag lookup and cloud upload too**, despite each having its own checkbox
in Settings and the toggle's own description mentioning only BPM/key detection.
Turning off analysis now turns off exactly analysis.

### Bulk retry

`POST /api/tracks/bulk-retry` takes explicit ids or a category name resolved
server-side by the same classifier that renders the count — so "Retry All 47"
acts on those 47, not on a set that drifted. The Errors page gains per-category
"Retry All" alongside the existing "Ignore All".

It also **clears `fallback_attempts`**, without which a retry was quietly inert:
`already_attempted()` treats any prior row as "we tried this", so the track reset,
walked back down the pipeline, and failed identically without ever re-contacting
Soulseek. The existing single-track retry had the same defect and is fixed too.

Bulk operations now write one summary activity row instead of one per track;
ignoring a category used to insert thousands and bury the dashboard feed.

### Soulseek in service health

`is_logged_in()` existed and was never surfaced. Because sync-api cannot import
worker code and has no slskd credentials, the worker probes every 120s and persists
the verdict to `app_config` for the API to serve — the pattern `lexicon_health`
already uses. It distinguishes logged-out (slskd answers, but searches return
nothing) from unreachable, and never-configured from broken. A verdict older than
15 minutes reports as unknown rather than repeating a stale "ok".

The dashboard now renders whatever services the API reports rather than a hardcoded
list of three.

### Dashboard drill-down

Clicking a month's green/red/grey segment opens that exact set of tracks. The
`month` filter is a half-open range rather than `substr(spotify_added_at, 1, 7)`, so
it can use an index — and `tracks` had no index at all beyond its implicit primary
key one, so `spotify_added_at`, `pipeline_stage` and `lexicon_status` now have them.

Segment widths come from flex-grow on raw counts; three independently rounded
percentages could total 101% and overflow the bar.

### Also

- The dashboard health probe called `GET /v1/tracks` — up to 1000 full track
  records — every 10 seconds, just to prove Lexicon answers. It now calls
  `/v1/playlists`, as `lexicon_health` already did.
- The nav error badge polled every errored track in full for a single integer;
  there is now a counts-only summary endpoint.
- **CI runs the tests.** The repo had ~20 test files and no workflow that executed
  them. It runs pytest (not `unittest`, which silently collects zero tests from the
  bare-function suites and reports success), plus a web typecheck and a real
  `next build`.
- Fixed two order-dependency bugs the new CI immediately exposed: test modules were
  fighting over `db.py`'s module-level `DB_PATH`, which is decided by whichever test
  imports it first.
- The Tidal token-refresh test had been failing for anyone running the suite outside
  the container. `tiddl` is installed by `sync-worker/Dockerfile` but is absent from
  `requirements.txt`, so the function under test bailed out on missing credentials
  before reaching the code the test was asserting on. CI now mirrors the Dockerfile.


## 2.12.4 — fix: the updater used the wrong compose project name

The updater mounts the project at `/project`, and Compose derives the project name
from the directory name — so it inferred `project` while the running stack was
`waxflow`. A different project name means Compose tries to CREATE rather than
RECREATE, so every apply died instantly with:

    Conflict. The container name "/waxflow-api" is already in use

**Auto-update would have failed on every single run.**

Found by forcing an apply of the current version rather than waiting for a real
release to expose it. The failure was safe — the stack stayed up and healthy on
2.12.3 and `.update-result` correctly recorded `failed`, which is the fail-safe
behaving as designed — but nothing would ever have updated.

The project name is now detected from the running container's own
`com.docker.compose.project` label, so it is correct wherever the project lives on
disk. `WAXFLOW_PROJECT_NAME` overrides it.


## 2.12.3 — build release images on native runners, not QEMU

The first version of `release-images.yml` cross-built arm64 through
`docker/setup-qemu-action`. Measured on the 2.12.2 release:

| image | build time |
|---|---|
| api | 6 min |
| worker | 9 min |
| **web (Next.js)** | **46+ min, abandoned** |

Emulating a Node build is pathologically slow, and it would have made **every**
release take ~50 minutes — which defeats the point of publishing images so that
updates are fast.

Each architecture now builds on a runner of that architecture (`ubuntu-latest`
and `ubuntu-24.04-arm`, free for public repos) and the two are joined into one
multi-arch manifest. Per-arch jobs push **by digest only**; the human-readable
tags are attached by the merge job once both architectures exist, so a tag never
points at a half-published set.


## 2.12.2 — fix: WaxFlow could not be built from a fresh clone

`sync-web/public/` was never committed, but `sync-web/Dockerfile` COPYs it out of
the builder stage. So a clean checkout failed:

    failed to compute cache key: failed to calculate checksum of ref ... /app/public

**Nobody could build WaxFlow from a fresh clone.** It only worked for people whose
working copy already happened to have the directory — which is why it went
unnoticed until CI built from a clean checkout for the first time and the web
image failed while api and worker succeeded.

Fixed by tracking `sync-web/public/.gitkeep` and adding `RUN mkdir -p /app/public`
so the COPY cannot fail even if the directory is absent from the build context.

Also documented, because it looked alarming during review: `NEXT_PUBLIC_API_URL`
is a build arg, but the frontend calls a RELATIVE `/api` and `next.config.js`
rewrites it server-side from `INTERNAL_API_URL` at RUNTIME. Nothing user-specific
is baked into the web image, which is what lets one published image serve every
install.


## 2.12.1 — "Update Now" actually requests an update

`POST /api/admin/update` wrote the literal string `requested at <timestamp>` into
the signal file. The updater added in 2.12.0 reads `target_version` out of that
file as JSON and refuses anything that is not semver — correctly, since the tag
comes from the GitHub API and reaches `docker pull`. So the button parsed to an
empty target and was refused every time: **"Update Now" silently did nothing.**

It now resolves the latest release from GitHub itself (rather than trusting the
caller with a tag), writes the same JSON shape `tasks/auto_update.py` writes, and
returns `up_to_date` instead of queueing a no-op when you are already current.
`?force=true` re-applies the current version to recover a half-applied update.

### Added
- `GET /api/admin/update-result` — the outcome of the last update, including
  rollbacks, so "Update Now" can report what happened instead of being a button
  that reports nothing.

### Fixed
- `scripts/deploy-to-nas.sh` extracted a tar, which adds and overwrites but never
  DELETES. Files removed upstream lingered on the remote forever — the dead
  services deleted in 2.11.0 were still on the NAS afterwards. It now prunes
  tracked-but-absent files first, so a deploy mirrors the repo. Host-local files
  (`.env`, `docker-compose.override.yml`, logs) are explicitly protected.
- `_read_version()` replaces three inlined copies of the same VERSION read in
  `routes/admin.py`, one of which had drifted.
- Example Plex URLs in docstrings no longer use a real LAN address.


## 2.12.0 — Auto-update actually works

`auto_update_enabled` has existed for a while. It could never have worked. Three
independent reasons, each sufficient on its own:

1. **The version check used a string compare.** `"2.11.0" > "2.9.0"` is `False`
   (at index 2, `"1" < "9"`), so every x.9 -> x.10+ upgrade was invisible. The
   reverse was `True`, meaning the check could have offered a **downgrade** as an
   update. Present in both `routes/admin.py` and `tasks/auto_update.py`.
2. **Nothing applied the update.** `scripts/auto-update.sh` had to be installed in
   the host's crontab by hand; nothing shipped it, so the signal file the worker
   wrote was never read by anything.
3. **Even when triggered, it did not update.** The script ran
   `docker compose up -d --build` against the source already on disk — a rebuild
   of the same version.

### Added — `waxflow-updater`
A container cannot restart itself, so applying an update needs host-side Docker
access. This is the only place WaxFlow asks for it, and it is deliberately
constrained:

- **`network_mode: none`.** It never downloads anything. The worker (network, no
  socket) decides the target version; the Docker *daemon* fetches image layers
  when asked over the socket. Nothing with host-root access talks to the internet.
- **Rollback.** After applying, it health-checks the API and restores the previous
  tag if the new version does not come up. This runs unattended at 3am by default;
  an update that half-applies and is never noticed is worse than one that never ran.
- **Input is treated as untrusted.** The target tag comes from the GitHub API and
  is refused unless it matches semver, so nothing unexpected reaches `docker pull`.

Delete the service from `docker-compose.yml` if you would rather not grant socket
access; everything else keeps working.

### Added — published images
`.github/workflows/release-images.yml` builds and pushes
`ghcr.io/<owner>/waxflow-{api,worker,web}` (linux/amd64 + linux/arm64) on every
published release, and refuses to publish if the tag disagrees with `VERSION`.

Updating is now a pull, not a rebuild. The rebuild path took ~25 minutes for the
worker image on a Synology NAS and wedged the Docker daemon once — unacceptable
for an unattended 3am job. `build:` blocks remain, so `docker compose up -d
--build` still works offline and for forks (`WAXFLOW_REGISTRY`).

### Changed
- `auto_update_enabled` defaults to `1` for **new** installs. `INSERT OR IGNORE`
  means existing deployments keep whatever they already had.
- Compose images are `${WAXFLOW_REGISTRY:-ghcr.io/rancur}/waxflow-*:${WAXFLOW_IMAGE_TAG:-${VERSION:-latest}}`.

### Tests
`tests/test_auto_update_version.py` — 7 cases pinning the comparison, including
the exact regression (2.9 -> 2.10/2.11 must be newer) and that a downgrade is
never offered.


## 2.11.0 — Path contract, one-way replication, and a pile of real bugs

Everything here was found by *running* the system during a live incident, not by
reading it. Several are bugs any deployment would hit.

### Fixed — bugs that affect every install
- **`MUSIC_LIBRARY_PATH` was hardcoded in `docker-compose.yml`.** Both services
  pinned `/music`, so setting it in `.env` silently did nothing. Now
  `${MUSIC_LIBRARY_PATH:-/music/Database}`. Pointing the library root at a
  SUBDIRECTORY of the bind mount keeps the share root clean and leaves Plex path
  translation intact — remapping the mount instead would break every existing
  `file_path` row.
- **The update banner was inverted.** `admin.py` compared versions as strings, so
  `"2.9.0" > "2.10.1"` was `True` and no update was ever offered across a
  major.minor boundary. Replaced with a numeric-tuple compare.
- **The API reported the wrong version.** `main.py` hardcoded `2.1.0` while
  `VERSION` said `2.10.1`. It now reads `/app/VERSION`, baked in at build time.
- **`busy_timeout` contradicted the connect timeout.** 5 s against `timeout=30`,
  and `busy_timeout` is what governs — so `PATCH /api/settings` still returned
  `{"detail":"database is locked"}` whenever the worker was mid-index. Both 30 s.
- **`deep-repair.sh` could never alert anyone.** It computed a repair verdict and
  then dispatched to a commented-out example. Wired to `WAXFLOW_ALERT_WEBHOOK`.
- **CodeQL scanned Python only**; `sync-web`'s TypeScript went unanalysed. (Held
  back from this PR — needs a `workflow`-scoped token.)

### Added — one-way NAS -> Mac replication (optional)
`scripts/sync-nas-to-mac.sh` + LaunchAgent. Pull-only, so conflict copies are
structurally impossible. Hybrid transport: change detection over SSH (~0.9 s,
the NAS walks its own disk) with an SMB transfer, versus 5 m 46 s for a full SMB
scan; 6 h reconcile as the safety net. `tasks/sync_gate.py` holds each import
until the file has landed locally, and **fails open** on every degenerate case —
a gate that can deadlock the pipeline is worse than the lag it prevents.

### Added — tools
- `scripts/merge-duplicate-lexicon-rows.py` — Engine DJ's `Track.path` is UNIQUE,
  so Lexicon rows beyond the distinct-file count can NEVER sync and can leave a
  part-applied sync failing with `FOREIGN KEY constraint failed`. Migrates
  playlist memberships onto the surviving row *before* deleting; a plain delete
  would have destroyed 849 memberships on the library this was written against.
- `scripts/consolidate-share-root.py` — moves stray artist folders from a share
  root into the library root. Non-destructive on collisions.
- `scripts/dedupe-report.py` — read-only duplicate/quality analysis. Reads remix
  descriptors from the parent folder as well as the filename, so different mixes
  are not reported as duplicates.

### Fixed — macOS agent robustness
- `ensure-music-mount.sh` treated **any** failed `ls` as a stale handle and
  unmounted. From a launchd agent the share cannot be remounted (`mount volume`
  has no keychain access; `mount_smbfs` returns `Authentication error`), so it
  destroyed a working mount that only a human in Finder could restore. It now
  never unmounts by default, and distinguishes macOS TCC's `Operation not
  permitted` — which means the mount is fine and *this process* is denied — from
  a real stale handle.
- The share is addressed by IP/hostname, **not** a Bonjour service-instance name.
  `NAME._smb._tcp.local` resolves only via service discovery; when that goes
  stale, mounts hang forever instead of failing.
- `osascript mount volume` is now watchdog-wrapped. It blocks indefinitely
  waiting on a credential dialog that never appears under launchd, which wedged
  both agents and stopped replication entirely.

### Changed
- Host-specific values moved out of the scripts into `~/.waxflow/waxflow.conf`.
- `lexicon_library_path` / `lexicon_input_path` seed to the SMB default and can be
  set via `LEXICON_LIBRARY_PATH` / `LEXICON_INPUT_PATH` — no username in defaults.
- `bump-version.sh` updates every version source, not just `VERSION`.
- README gains "Local paths and Engine DJ", including the two things that cost the
  most time here: never put an Engine library inside a two-way sync, and Engine
  holds at most one row per file.

### Removed
- `sync-api/services/{matcher,downloader,verifier}.py` — 463 lines referenced
  nowhere; that logic lives in `sync-worker/tasks/`.
- `scripts/backup-lexicon.sh` — self-documented no-op; `backup-lexicon-db.sh` is
  the real one.

### Corrected
An earlier diagnosis held that Engine DJ refuses `/Volumes/*` locations. **It does
not.** Tested against a real Engine library: all 40 rows carrying a
`/Volumes/Macintosh HD/` prefix were present. That prefix is a symlink to `/` and
resolves fine. The missing-tracks symptom was caused by two-way sync destroying
the Engine database, not by path format.


## 2.10.0 — Sleep-tolerance catch-up: rescue downloaded-but-not-imported tracks

Closes the last sleep/wake gap that stranded freshly-downloaded tracks with a real
file on the NAS but no entry in Lexicon. The always-on NAS worker downloads fine
while the Lexicon-host Mac sleeps; the *import* (the only stage that talks to the
Mac) is where sleep bites. Phase 3's offline queue already HOLDS import work
*proactively* when a pre-check says the Mac is unavailable — but a call that PASSES
the pre-check (SSH port open, `GET /v1/playlists` == 200) and then fails mid-import
as the Mac slips into sleep (`database is locked`, `timed out`, empty import once
the SMB mount drops) landed the track in a **terminal** `pipeline_stage='error'`
with `download_status='complete'` and no `lexicon_track_id` — and nothing ever
retried it. Found live: 5 such orphans, every one timestamped overnight / early-AM
(NGHTMRE "Hold Me Close", Goo Goo Dolls "Iris", Jeff Buckley "Hallelujah", The Maine
"I Wanna Love You", Ludwig Göransson "Hades").

### Added — sleep-tolerance catch-up pass (`tasks/import_catchup.py`, worker task, default ON)
- Runs every `import_catchup_interval_seconds` (default 900s) but **only when Lexicon
  is available** — so it naturally fires on the first cycle after the Mac wakes (the
  "on-wake scan"), and is a pure no-op while asleep.
- Finds tracks stranded in `error` with `download_status='complete'`, no
  `lexicon_track_id`, and a **transient Lexicon/Mac-unavailability** error signature
  (db-locked / timed-out / empty-import / mount-down / connection), whose file still
  exists on disk, and **re-arms** each to its correct earlier stage (`verifying` for a
  verify-stage lock, otherwise `organizing`). The real re-import then runs through the
  normal, fully-guarded pipeline (lossless gate, ISRC match guard, empty-import grace,
  dedup existence check) — all idempotent, so a track already in Lexicon links instead
  of duplicating. The pass itself performs **no** Lexicon/file write.
- **Bounded, no hot loop:** only revives an error settled for at least
  `import_catchup_min_age_seconds` (default 300s) and at most
  `import_catchup_max_attempts` (default 6) times per track, via the new
  `tracks.catchup_attempts` counter — a genuinely-broken track is retried a handful of
  times then left alone. Non-transient errors (not-lossless, fingerprint-too-low) are
  never touched.
- Emits `import_catchup_revived` / `import_catchup_pass` activity events for observability.

### Schema (additive, idempotent)
- `tracks.catchup_attempts INTEGER NOT NULL DEFAULT 0` (guarded ADD COLUMN, mirrored in
  `sync-api/init_db.py`; `V3_SCHEMA_VERSION` → 2). No table rebuild, no data migration.

### Tests
- `tests/test_import_catchup.py` — 10 cases covering revive-to-organizing, verify-stage
  re-entry, empty-import signature, non-transient left-alone, missing-file skip, the
  on-wake no-op gate, already-in-Lexicon untouched, the attempts cap, the min-age
  window, and the disable flag.

### Config keys (all read live)
- `import_catchup_enabled` (default ON), `import_catchup_interval_seconds` (900),
  `import_catchup_min_age_seconds` (300), `import_catchup_max_attempts` (6).

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
