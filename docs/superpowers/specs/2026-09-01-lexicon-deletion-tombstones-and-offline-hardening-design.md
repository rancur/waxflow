# Lexicon deletion tombstones + offline hardening — design (2026-09-01)

## Problem

1. **Deleting a track in Lexicon is invisible to WaxFlow.** The WaxFlow row stays
   `complete`, pointing at a Lexicon `Track.id` that no longer exists. Any re-arm path
   (Retry, Bulk Retry, hunter, catch-up, `recheck-mappings --apply`, unignore) sends it
   back through the pipeline and re-downloads / re-imports the very match the user just
   rejected; the quality rechecker keeps "upgrading" it; `file_index` still offers the
   NAS master to the next like of that song. Lexicon on this install is configured
   `trackDeletePermanently=true, trackDeleteFromDisk=true`, so the Mac file is deleted —
   but the NAS master survives (the NAS→Mac rsync is pull-only, no `--delete`) and the
   6-hourly reconcile **copies the deleted file back onto the Mac**. Measured 2026-09-01:
   34 complete tracks point at missing Lexicon rows; 27 still have a NAS master.
2. **Sleep tolerance is built (2.10.x, `offline_queue_enabled=1`) but has never held a
   single track in production**, and four Lexicon-touching tasks are not availability-
   gated (`lexicon_health` canary, `analyze_tracks`, `create_playlists`,
   `_trigger_lexicon_post_processing_batch`) so a deliberately-off Mac produces
   `lexicon_unreachable` pages and error noise.

## Design

### A. Tombstones (`tasks/lexicon_reconcile.py`, new; default ON)

* Runs every `lexicon_reconcile_interval_seconds` (default 900) **only when
  `mac_availability.probe()` says available** (no-op while asleep — never mistake an
  unreachable Lexicon for a deleted library).
* Builds the live Lexicon id set by paging `GET /v1/tracks` (`limit`/`offset`, 1000/pg,
  against reported `total`; a page failure aborts the pass — a partial set must never
  produce tombstones). Also collects `archived=1` ids.
* For every `pipeline_stage='complete'` track whose `lexicon_track_id` is absent from
  the set (or archived) → **tombstone**:
  * `tracks`: `pipeline_stage='ignored'`, `is_protected=1`, `lexicon_status='skipped'`,
    `pipeline_error='deleted_in_lexicon:<iso ts>'`.
  * new row in `tombstones(track_id, spotify_id, isrc, tidal_id, lexicon_track_id,
    file_path, file_hash_sha256, reason, trashed_path, created_at, purge_after)`.
  * NAS master (`file_path` under `/music`) → moved to the Synology Recycle Bin
    `/music/#recycle/<relative path>` (the share already has it on; the Mac rsync
    already excludes `#recycle`). `trashed_path` + `purge_after = now+30d` recorded;
    a purge step deletes only files WaxFlow itself trashed, once `purge_after` passes.
    The `file_index` row for the old path is deleted.
  * Safety: never touches Lexicon; never deletes a file it did not trash; a track whose
    `match_source='lexicon_existing'` (pre-existing library track WaxFlow only linked)
    is tombstoned but its file is **not** touched (WaxFlow never downloaded it).
  * Activity event `lexicon_deleted_tombstoned`; summary `lexicon_reconcile_pass`.
* **Terminal everywhere.** `is_tombstoned(track)` = `pipeline_stage='ignored'` AND a
  `tombstones` row. Refused by: `/tracks/{id}/retry`, `/tracks/bulk-retry`,
  `/tracks/{id}/unignore`, `/matching/{id}/reject`, hunter, import_catchup,
  retry_unmatched, metadata_fallback (all already select only `error`, asserted by
  tests), quality/lossless upgraders (select only `complete`). The only way back is
  `POST /tracks/{id}/restore` (new; also restores the trashed file if still present).
* **Same-crap-match guard.** At match acceptance (`_match_track` Tidal hit,
  `_check_existing_by_isrc`, `_check_existing_in_library`) a candidate whose
  `tidal_id`, `isrc` or `file_path`/`file_hash` matches any tombstone is not
  auto-imported: the track is routed to `needs_import_review` with a
  `tombstone_match:` reason so the user decides.
* Dashboard/Errors page: "Deleted in Lexicon" section (count from `tombstones`),
  per-row **Restore**. `/api/tracks/errors` gains `deleted_in_lexicon: [...]`.

### B. Offline hardening

* `lexicon_health.run_canary`: when the probe says `asleep`, record `mac_asleep`
  (ok=True, informational) instead of paging `lexicon_unreachable`. `lexicon_down`
  (Mac up, Lexicon quit) still pages after a grace of
  `lexicon_down_page_after_seconds` (default 1800).
* `analyze_tracks`, `create_playlists`, `_trigger_lexicon_post_processing_batch`:
  early-return when not available (no warning spam).
* Verified live (2026-09-01 night): Mac shut down with liked tracks in flight →
  downloads complete, tracks parked in `import_queue`, zero `error`, zero pages; Mac
  boot → drained, imported, filed into the monthly playlist.

### Schema (additive, mirrored in `init_db.py` and `v3_schema.py`)

```
CREATE TABLE IF NOT EXISTS tombstones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER REFERENCES tracks(id),
    spotify_id TEXT, isrc TEXT, tidal_id TEXT, lexicon_track_id TEXT,
    file_path TEXT, file_hash_sha256 TEXT,
    reason TEXT NOT NULL,
    trashed_path TEXT, purge_after TEXT, purged_at TEXT,
    restored_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_tombstones_track ON tombstones(track_id);
CREATE INDEX IF NOT EXISTS idx_tombstones_tidal ON tombstones(tidal_id);
CREATE INDEX IF NOT EXISTS idx_tombstones_isrc  ON tombstones(isrc);
```

### Config (read live)
`lexicon_reconcile_enabled` (1), `lexicon_reconcile_interval_seconds` (900),
`tombstone_trash_enabled` (1), `tombstone_purge_days` (30),
`lexicon_down_page_after_seconds` (1800).

### Tests
`tests/test_lexicon_reconcile.py`: tombstone on missing id; archived counts as
deleted; no-op when unavailable; partial page failure aborts with zero writes;
`lexicon_existing` tracks keep their file; trash move + file_index removal; purge only
after `purge_after` and only WaxFlow-trashed paths; restore path. `test_guards`:
tombstone match routes to review. API: retry/bulk-retry/unignore refuse tombstones;
restore works. Canary: asleep → no page.

### Release
v2.19.0 — PR → main → `gh release create v2.19.0` (images build on release, not tag)
→ `waxflow-updater` applies → confirm in the live DB that `lexicon_reconcile_pass`
fired and the 34 known orphans became tombstones.
