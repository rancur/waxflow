"""Tests for the quality ladder.

Two classes of failure matter here:

  * Misreading a real file's metadata. Every probe dict below is a REAL ffprobe
    result from this library -- the AIFFs that report bit depth only in
    `bits_per_sample`, and the AAC files that measure 320000/326415.
  * Ordering that is not strict. `is_upgrade` is a plain `>` on `score`, so if two
    genuinely different qualities can tie, the upgrade watcher either churns the
    same track forever or silently refuses a real improvement.
"""

import hashlib
import os
import sys
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks import quality  # noqa: E402

# Real probe results measured on the live library.
FLAC_16 = {"codec": "flac", "sample_rate": 44100, "bit_depth": 16, "bit_rate": 900_000}
AIFF_16 = {"codec": "pcm_s16be", "sample_rate": 44100, "bit_depth": 16, "bit_rate": 1_411_200}
AIFF_24 = {"codec": "pcm_s24be", "sample_rate": 44100, "bit_depth": 24, "bit_rate": 2_116_800}
HIRES = {"codec": "flac", "sample_rate": 96000, "bit_depth": 24, "bit_rate": 4_600_000}
AAC_320 = {"codec": "aac", "sample_rate": 44100, "bit_depth": 0, "bit_rate": 326_415}
MP3_320 = {"codec": "mp3", "sample_rate": 44100, "bit_depth": 0, "bit_rate": 320_000}
MP3_V0 = {"codec": "mp3", "sample_rate": 44100, "bit_depth": 0, "bit_rate": 245_000}
MP3_128 = {"codec": "mp3", "sample_rate": 44100, "bit_depth": 0, "bit_rate": 128_000}


class LadderTest(unittest.TestCase):
    def test_tiers(self):
        for probe, tier in (
            (HIRES, quality.TIER_HIRES),
            (AIFF_24, quality.TIER_24BIT),
            (FLAC_16, quality.TIER_LOSSLESS),
            (AIFF_16, quality.TIER_LOSSLESS),
            (AAC_320, quality.TIER_LOSSY_HIGH),
            (MP3_320, quality.TIER_LOSSY_HIGH),
            (MP3_V0, quality.TIER_BELOW),
            (MP3_128, quality.TIER_BELOW),
        ):
            self.assertEqual(quality.score_probe(probe).tier, tier, probe["codec"])

    def test_the_aiff_bit_depth_trap(self):
        # ffprobe reports AIFF depth in bits_per_sample, not bits_per_raw_sample.
        # If that arrives as 0, all 974 AIFFs in this library drop a tier.
        s = quality.score_probe({"codec": "pcm_s24be", "sample_rate": 44100,
                                 "bit_depth": 0, "bit_rate": 2_116_800})
        self.assertEqual(s.bit_depth, 24, "depth must be recovered from the codec name")
        self.assertEqual(s.tier, quality.TIER_24BIT)

    def test_the_measured_aac_files_clear_the_floor(self):
        # These 95 files currently sit in the error queue. At 326415 bps they are
        # genuine 320k and should import as below-target, not be refused.
        s = quality.score_probe(AAC_320)
        self.assertTrue(s.meets_floor)
        self.assertFalse(s.meets_target)

    def test_v0_stays_below_the_floor(self):
        # ~245k VBR is not 320k; the tolerance must not stretch that far.
        self.assertFalse(quality.score_probe(MP3_V0).meets_floor)

    def test_slightly_under_320k_is_still_accepted(self):
        # Real 320k rips measure a little under; refusing them helps nobody.
        self.assertTrue(quality.score_probe(
            {"codec": "mp3", "sample_rate": 44100, "bit_rate": 312_000}).meets_floor)

    def test_unknown_codec_is_treated_as_lossy(self):
        s = quality.score_probe({"codec": "something_new", "sample_rate": 44100,
                                 "bit_rate": 1_000_000})
        self.assertFalse(s.lossless)
        self.assertEqual(s.tier, quality.TIER_LOSSY_HIGH)

    def test_downsampled_lossless_is_not_target_quality(self):
        s = quality.score_probe({"codec": "flac", "sample_rate": 22050,
                                 "bit_depth": 16, "bit_rate": 400_000})
        self.assertFalse(s.meets_target)

    def test_missing_bitrate_does_not_crash(self):
        s = quality.score_probe({"codec": "mp3", "sample_rate": 44100})
        self.assertEqual(s.tier, quality.TIER_BELOW)
        self.assertTrue(s.reasons)

    def test_empty_probe_is_below_floor(self):
        self.assertEqual(quality.score_probe({}).tier, quality.TIER_BELOW)


class OrderingTest(unittest.TestCase):
    def test_strictly_increasing_up_the_ladder(self):
        scores = [quality.score_probe(p).score
                  for p in (MP3_128, MP3_320, FLAC_16, AIFF_24, HIRES)]
        self.assertEqual(scores, sorted(scores))
        self.assertEqual(len(set(scores)), len(scores), "ties break is_upgrade()")

    def test_upgrade_requires_a_strict_improvement(self):
        flac = quality.score_probe(FLAC_16)
        self.assertFalse(quality.is_upgrade(flac, quality.score_probe(FLAC_16)))
        self.assertFalse(quality.is_upgrade(flac, quality.score_probe(MP3_320)))
        self.assertTrue(quality.is_upgrade(quality.score_probe(MP3_320), flac))

    def test_hires_counts_as_an_upgrade_over_cd_quality(self):
        self.assertTrue(quality.is_upgrade(quality.score_probe(FLAC_16),
                                           quality.score_probe(HIRES)))

    def test_never_upgrade_to_something_below_the_floor(self):
        self.assertFalse(quality.is_upgrade(quality.score_probe(FLAC_16),
                                            quality.score_probe(MP3_128)))
        self.assertFalse(quality.is_upgrade(None, quality.score_probe(MP3_128)))

    def test_no_current_quality_accepts_anything_at_or_above_the_floor(self):
        self.assertTrue(quality.is_upgrade(None, quality.score_probe(MP3_320)))


class MirrorTest(unittest.TestCase):
    def test_worker_and_api_copies_are_identical(self):
        """sync-api cannot import worker code, so quality.py is duplicated.

        A silent drift between the two would mean the API and the worker disagree
        about what "good enough" means, which is worse than either rule alone.
        """
        repo = os.path.dirname(SYNC_WORKER_DIR)
        worker = os.path.join(repo, "sync-worker", "tasks", "quality.py")
        api = os.path.join(repo, "sync-api", "quality.py")
        self.assertTrue(os.path.exists(api), "sync-api/quality.py is missing")
        digest = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()  # noqa: E731
        self.assertEqual(digest(worker), digest(api),
                         "sync-worker/tasks/quality.py and sync-api/quality.py "
                         "have drifted — edit BOTH")



class ProfileTest(unittest.TestCase):
    """The Radarr model: floor (never accept worse), cutoff (stop upgrading),
    target (ask for this first). Search walks DOWN, upgrading walks UP."""

    def _get(self, **kw):
        return lambda key: kw.get(key)

    def test_defaults_are_sensible(self):
        p = quality.resolve_profile(self._get())
        self.assertEqual(p["floor"], quality.TIER_LOSSY_HIGH)
        self.assertEqual(p["cutoff"], quality.TIER_LOSSLESS)
        self.assertEqual(p["target"], quality.TIER_HIRES)

    def test_search_order_is_best_first_down_to_the_floor(self):
        p = quality.resolve_profile(self._get())
        names = [quality.TIER_NAMES[t] for t in quality.search_ladder(p)]
        self.assertEqual(names, ["hi-res", "24-bit", "lossless", "320k"])

    def test_raising_the_floor_shortens_the_search(self):
        p = quality.resolve_profile(self._get(quality_floor_tier="lossless"))
        names = [quality.TIER_NAMES[t] for t in quality.search_ladder(p)]
        self.assertEqual(names, ["hi-res", "24-bit", "lossless"])
        self.assertNotIn("320k", names)

    def test_cutoff_decides_when_to_stop_hunting(self):
        lossless = quality.score_probe(FLAC_16)
        mp3 = quality.score_probe(MP3_320)

        happy_with_cd = quality.resolve_profile(self._get(quality_cutoff_tier="lossless"))
        self.assertFalse(quality.needs_upgrade(lossless, happy_with_cd))
        self.assertTrue(quality.needs_upgrade(mp3, happy_with_cd))

        chasing_hires = quality.resolve_profile(self._get(quality_cutoff_tier="hi-res"))
        self.assertTrue(quality.needs_upgrade(lossless, chasing_hires),
                        "a hi-res cutoff must keep hunting past CD quality")

    def test_nothing_yet_always_needs_an_upgrade(self):
        self.assertTrue(quality.needs_upgrade(None, quality.resolve_profile(self._get())))

    def test_impossible_profiles_are_forced_into_order(self):
        # floor above cutoff would search for nothing at all.
        p = quality.resolve_profile(self._get(quality_floor_tier="hi-res",
                                              quality_cutoff_tier="320k"))
        self.assertGreaterEqual(p["cutoff"], p["floor"])
        self.assertGreaterEqual(p["target"], p["cutoff"])
        self.assertTrue(quality.search_ladder(p))

    def test_unknown_names_fall_back_rather_than_crash(self):
        p = quality.resolve_profile(self._get(quality_cutoff_tier="banana"))
        self.assertEqual(p["cutoff"], quality.DEFAULT_PROFILE["cutoff"])

    def test_common_aliases_resolve(self):
        for alias, tier in (("flac", quality.TIER_LOSSLESS), ("mp3", quality.TIER_LOSSY_HIGH),
                            ("hires", quality.TIER_HIRES), ("24bit", quality.TIER_24BIT)):
            self.assertEqual(quality.tier_from_name(alias), tier, alias)

    def test_description_is_ui_ready(self):
        d = quality.describe_profile(quality.resolve_profile(self._get()))
        self.assertEqual(d["cutoff"], "lossless")
        self.assertEqual(len(d["tiers"]), 4)
        self.assertTrue(all("accepted" in t and "stops_upgrading" in t for t in d["tiers"]))

if __name__ == "__main__":
    unittest.main()
