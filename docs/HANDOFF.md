# Handoff — quality profiles, matching, and what's left

**Written 2026-08-10. Live instance is on v2.18.0.** Everything described as shipped
is deployed and verified against the real library, not just tested.

This document is written for a fresh agent picking the work up cold. Read
"Ground truth" first — several of the facts in it were expensive to establish and
three of them contradict what the code's own comments used to claim.

---

## Where things stand

| | |
|---|---|
| Version | 2.17.0 |
| Parity | 94.93% (5,163 / 5,439) |
| Errors | 265 — `wrong_version` 129, `no_tidal_match` 57, `other` 70, `lexicon_sync_failed` 3, `not_lossless` 5, `download_failed` 1 |
| Library scoring | 135 lossless, 43 at 320k, 7 24-bit, 4,978 not yet scored |
| Automatic upgrades | **On and self-applying** for same-container upgrades (845 of 939 scored tracks). Container changes (94) still stage for the manual relocator. |
| Relocator | Write path **verified** against a copy of the live DB (see below); has not yet applied a real staged upgrade |
| Library scoring | Backfilling now, ~40 tracks/min. ~932 tracks have stale paths and are marked `missing-file` |

Tests: 360 in `sync-worker`, 51 in `sync-api`, all passing. CI runs them on every
push (`.github/workflows/tests.yml`).

---

## The goal, in the user's words

> Like how Radarr and Sonarr have quality levels — find the absolutely highest
> quality and work our way down through a set of levels (which should be in the
> settings so the user has visibility). A rechecker that can seamlessly replace the
> low quality with higher quality if the system automatically finds it, replaces the
> file and updates Lexicon, so we always have the highest quality files.

Most of that is built. The last mile — actually turning it on — is not.

---

## What is built and shipped

### Quality profile (v2.17.0)

Three settings, in **Settings → Quality Profile**:

```
floor    never accept worse than this      default 320k
cutoff   stop hunting once reached          default lossless
target   ask for this first                 default hi-res
```

Downloads walk the ladder **down** (`hi-res → 24-bit → lossless → 320k`) and take
the first tier that yields a verified file. The rechecker walks it **up**: anything
below the cutoff stays on the hunt. The cutoff is what makes the hunt terminate.

- `sync-worker/tasks/quality.py` — the ladder, scoring, and profile resolution.
  **Mirrored byte-for-byte to `sync-api/quality.py`** because sync-api cannot import
  worker code. `test_quality.py::MirrorTest` fails if they drift — edit both.
- `GET /api/quality/profile` — the profile plus a live histogram.

### Tiered search (v2.17.0)

`soulseek_fallback.rank_candidates(..., tier=)` accepts the extensions and size band
appropriate to a tier and pre-filters on slskd's reported bitRate/bitDepth/
sampleRate. `search_best_available()` drives the walk. **Passing no tier preserves
the old `.flac`-only behaviour exactly**, which is what keeps the existing tests
honest.

### The rechecker (v2.17.0)

`sync-worker/tasks/quality_upgrade.py`. Finds tracks below the cutoff, hunts only
*above* what they already have, verifies through the existing fake-FLAC gate, and
requires a strictly better score before staging into `relocation_queue`. Registered
in `worker.py` on a 6h timer. It stages; it never swaps.

### The relocator (v2.17.0, never yet run for real)

`scripts/apply-relocations.py`. Swaps the file and re-points Lexicon's
`Track.location`. Gates: verified fresh backup; Lexicon proven quit three ways (no
process, exclusive lock obtainable, `-wal` static); dry-run default; integrity + FK
checks; cue/grid/playlist/cloud counts compared before and after; one transaction;
old file moved to `.superseded/` only after the write commits.

### Retroactive dateAdded (v2.14.x, applied)

`scripts/backfill-lexicon-dateadded.py` corrected **5,255 tracks**, 996 of them by
11 years. The library now spans 2014→2026. Already applied; the script is idempotent
and safe to re-run.

### Matching correctness (v2.13.0 → v2.15.2, applied)

Four matching paths existed; only one was gated. All four now apply a **duration
gate** (±5s, configurable, fails open when either duration is unknown) and a
**version gate** (title vs FILE PATH, both sides must name a version).

`scripts/recheck-mappings.py` re-adjudicated the existing damage: 256 tracks were
unmapped and re-resolved. 56 groups remain genuinely ambiguous and were left alone.

### Phase 1 (v2.13.x, applied)

Missing Tracks fix, bulk retry, Soulseek in service health, dashboard month
drill-down, post-processing coverage, and CI that actually runs the tests.

---

## What is left

### 1. Watch the first real in-place upgrade land

**Most upgrades no longer need a human at all** (v2.18.0). A FLAC replacing a FLAC
keeps its filename, so the better bytes are written over the old file, Lexicon's
`location` never changes, and there is nothing to rewrite and nothing to quit.
Measured here: **845 of 939 scored tracks** are in that position.

The remaining 94 are lossy (m4a) and must change container, so those still stage
into `relocation_queue` and need the relocator with Lexicon quit.

Nothing has come through the in-place path yet only because the rechecker is still
waiting on Soulseek to actually find something better. Watch for it:

    SELECT * FROM activity_log WHERE event_type = 'upgrade_applied' ORDER BY id DESC;

Verify the first one by hand: the file at the original path should be bigger/better,
`.superseded/` should hold the original, and the track's cues should be intact.

### 2. Apply the container-changing upgrades (still manual)

For the m4a -> flac cases, a real `Track.location` write against the live database
has still never happened.

The write path itself IS verified. It was exercised against an online `.backup`
copy of the live 5,714-track database, on a real track with real cue points:

    integrity_check      ok
    fk violations        0
    location changed     yes
    locationUnique kept  yes   (CloudFile links depend on this)
    cues                 4 -> 4
    table counts         all unchanged

So the mechanism is proven; what remains is running it for real, which needs
Lexicon quit and therefore a human:

1. `LEXICON_SSH=local NAS_SSH=nas-lan bash scripts/backup-lexicon-db.sh`
2. Confirm something is staged:
   `SELECT * FROM relocation_queue WHERE state='pending'`
3. **Quit Lexicon.**
4. `scripts/apply-relocations.py` — dry run, read the plan.
5. `scripts/apply-relocations.py --apply --limit 5`, then check cue counts are
   unchanged and the upgraded tracks play.
6. Full run.

Treat the first real run as an experiment with a verified backup, not a routine
deploy. It moves audio files and writes to the library database.

Note the stranding guard in `process_pipeline._upgrade_replacement_available`:
with `relocation_enabled=0`, hunts are not queued at all. That is deliberate --
finding an upgrade nothing can install leaves the better file on disk unreferenced
while Lexicon keeps playing the worse copy, because `_lexicon_find_or_import`
short-circuits on the existing `lexicon_track_id`.

### 2. Let the score backfill finish

`quality_upgrade.backfill_scores()` drains a bounded batch each cycle and is
running now. It matters because the rechecker cannot evaluate a track whose
current quality is unknown, and 4,978 of 5,163 had no score.

**~932 tracks are marked `missing-file`** — their `file_path` does not resolve in
the container. That is a real, separate problem worth investigating: those tracks
read as `complete` but their audio is not where the database says it is.

Two operational settings were raised to drain the backlog faster and should be put
back once it is done: `quality_score_batch` 800 (default 250) and
`quality_upgrade_interval_seconds` 180 (default 21600).

### 3. Let the score backfill finish

- **129 `wrong_version`** — the gates now prevent new ones. These predate them and
  need re-sourcing. `POST /api/tracks/bulk-retry {"category":"wrong_version"}`
  clears `fallback_attempts` and re-runs them; do it in chunks, the pipeline polls
  every 10s.
- **57 `no_tidal_match`** — genuinely absent from Tidal. Soulseek is the path.
- **70 `other`** — never categorised; worth reading a sample before acting.
- **56 ambiguous mappings** and **34 pointing at deleted Lexicon rows** — reported
  by `recheck-mappings.py`, deliberately untouched.

### 4. Phase 3 — the UI overhaul (not started)

Planned in detail, nothing built:

- Four hand-rolled tables duplicate row markup and formatters
  (`tracks:421`, `errors:257`, `upload:186`, `downloads:463`) → one shared
  `DataTable`.
- `app/types.ts` is stale; `strict: true` is already on, so fixing it surfaces many
  errors at once.
- **Downloads is not unified**: `download_queue` only ever receives `tiddl`/`tidarr`
  (its `source` column has a CHECK constraint that cannot hold `soulseek`).
  Soulseek writes to `fallback_attempts`. Fix with a read-side
  `download_events` VIEW over the three tables — no rebuild.
- `/upload` duplicates `/errors`; the `wanted` ledger has no UI at all.
- Dead code: `app/components/TrackRow.tsx` is imported by nothing.

---

## Ground truth

Facts established by probing the live system. Several contradict what the code
previously assumed.

**Lexicon's API refuses to edit `location`, `bpm` and `dateAdded`.** Verified:
`PATCH /v1/track {"edits":{"dateAdded":...}}` → `'dateAdded' is not editable`, while
`comment` on the same request returns 200. Anything touching those columns must be a
direct SQLite write.

**A `location`-only write is safe.** `Track.location` is *not* uniquely indexed —
the UNIQUE is on `locationUnique`, which we never touch. `Cuepoint`, `Tempomarker`
and `Waveform` key off `Track.id`; `CloudFile` keys off `locationUnique`. **Cues,
beat grids and cloud links survive a relocation.** Verify it anyway — every script
here compares counts before and after.

**SQLite must not be written over SMB/NFS.** Advisory locking is unreliable there
and `BEGIN IMMEDIATE` cannot be trusted. This — not reachability — is why the
writers run on the Mac. The Lexicon DB is local to the Mac and read freely.

**`/v1/tracks` caps at 1000 rows** and rejects larger limits. Paginate with
`limit`/`offset` against the reported `total`.

**Lexicon reports `duration` in seconds** (a float, despite the INTEGER column).
WaxFlow stores `duration_ms`.

**WaxFlow stores `lexicon_track_id` as TEXT; Lexicon's `Track.id` is INTEGER.**
Comparing them raw matches *nothing* and silently degrades to path matching. This
cost an entire analysis pass — 5,130 ID matches read as zero with nothing raised.

**ffprobe reports AIFF bit depth in `bits_per_sample`, not `bits_per_raw_sample`.**
Without the fallback all 974 AIFFs read as 0-bit and drop a tier.

**Bitrate must be `max(stream, format)`.** VBR MP3 often reports only one, and for a
lossy file the bitrate *is* the quality decision.

**`/v1/control` accepts no parameters and has no select-all.** Post-processing can
only act on Lexicon's GUI selection. It works anyway — do not try to "fix" it.

**Schema DDL is duplicated** in `sync-api/init_db.py` and
`sync-worker/tasks/v3_schema.py`. Both containers apply it and either may boot
first. **Add columns to both.** And never put a `CREATE INDEX` on a new column in
the same `executescript` as the DDL — on an existing database the column does not
exist yet, the statement raises, and *every statement after it is skipped*. Add the
index after the guarded `ALTER TABLE`.

---

## How to work on this safely

**Cutting a release:** the image workflow triggers on `release: published`, NOT on
push. Committing a version bump without `gh release create` means no images are
ever built and the update silently has nothing to pull.

**Changing a task interval requires a worker restart.** `run_task` reads the
interval when it sleeps, so a config change does not wake a task already sleeping
its old interval — this looked exactly like the backfill silently stalling.

**Deploying:** push a tag, wait for images, then `POST /api/admin/update`. Wait for
the images **by tag**, not by polling the latest workflow run — `gh run list
--limit 1` returns the previous completed run and exits immediately, which already
caused one failed update (the fail-safe caught it and left the stack untouched):

```bash
curl -s "https://ghcr.io/token?scope=repository:rancur/waxflow-worker:pull&service=ghcr.io"   # then HEAD the manifest for the tag
```

**The lesson worth carrying:** four separate times in this work, code that passed
tests and deployed cleanly **did nothing at all**.

- Coverage measured only the first 1,000 tracks of 5,612 and reported confident
  percentages.
- The quality floor was added at the organizing stage, but verify rejects lossy
  files first — it was unreachable and `below_target` stayed at 0.
- The duration gate was added to two of four matching paths; 14 of 20 test tracks
  went straight back to the same wrong row.
- A Soulseek health row 500'd the entire dashboard on a closed connection, and the
  dashboard endpoint had no test.

Every one was caught by checking whether the thing *actually happened in
production*, never by the test suite. After deploying, query the database and
confirm the new code path was taken.

---

## Key files

```
sync-worker/tasks/
  quality.py               ladder, scoring, profile   (MIRROR to sync-api/quality.py)
  quality_upgrade.py       the rechecker
  soulseek_fallback.py     tiered search, import gate
  process_pipeline.py      matching gates, verify, the stranding guard
  v3_schema.py             schema  (MIRROR to sync-api/init_db.py)

scripts/
  apply-relocations.py         swap file + re-point Lexicon  (never run for real)
  recheck-mappings.py          re-adjudicate mappings
  backfill-lexicon-dateadded.py
  backup-lexicon-db.sh         run FIRST before any direct write

docs/HANDOFF.md            this file
CHANGELOG.md               every decision, with the measurement behind it
```

The plan this came from is at
`~/.claude/plans/i-have-a-github-cuddly-balloon.md` on the owner's machine.
