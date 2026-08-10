"""Tests for the Lexicon dateAdded backfill helpers.

The script writes directly to the Lexicon database, so the parts that decide WHAT
to write are the parts worth pinning:

  * `as_int` exists because WaxFlow stores lexicon_track_id as TEXT while Lexicon's
    Track.id is INTEGER. Comparing them raw matches nothing and silently degrades
    to path matching -- on the first run of this analysis that dropped ID matches
    from 5,130 to zero, and nothing raised.
  * `to_lexicon_ts` exists so a row whose date is already correct but formatted
    differently is not rewritten on every run.

Loaded by path: the script lives in scripts/ and is not an importable package.
"""

import importlib.util
import os
import sys
import unittest

_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "scripts", "backfill-lexicon-dateadded.py",
)
_spec = importlib.util.spec_from_file_location("backfill_dateadded", _SCRIPT)
bf = importlib.util.module_from_spec(_spec)
sys.modules["backfill_dateadded"] = bf
_spec.loader.exec_module(bf)


class AsIntTest(unittest.TestCase):
    def test_string_ids_from_waxflow_coerce(self):
        # The actual regression: WaxFlow hands back '11287', Lexicon keys on 11287.
        self.assertEqual(bf.as_int("11287"), 11287)
        self.assertEqual(bf.as_int(11287), 11287)
        self.assertEqual(bf.as_int(" 11287 "), 11287)

    def test_unusable_values_are_none_not_exceptions(self):
        for value in (None, "", "  ", "abc", "12.5", [], {}):
            self.assertIsNone(bf.as_int(value), f"{value!r}")

    def test_none_never_collides_with_a_real_id(self):
        # `lid in by_id` must not accidentally hit on a falsy id.
        self.assertIsNone(bf.as_int(None))
        self.assertNotEqual(bf.as_int("0"), None)
        self.assertEqual(bf.as_int("0"), 0)


class TimestampTest(unittest.TestCase):
    def test_spotify_format_becomes_lexicon_format(self):
        self.assertEqual(bf.to_lexicon_ts("2026-08-04T00:36:31Z"),
                         "2026-08-04T00:36:31.000Z")

    def test_already_lexicon_format_is_unchanged(self):
        # Idempotence: this is what stops every run rewriting every row.
        stamp = "2024-10-19T07:00:00.000Z"
        self.assertEqual(bf.to_lexicon_ts(stamp), stamp)

    def test_empty_is_none(self):
        for value in (None, "", "   "):
            self.assertIn(bf.to_lexicon_ts(value), (None, ""))

    def test_conversion_is_stable_under_repetition(self):
        once = bf.to_lexicon_ts("2014-07-13T23:06:10Z")
        self.assertEqual(bf.to_lexicon_ts(once), once)


class MacPathTest(unittest.TestCase):
    def test_container_path_maps_to_the_mac_library(self):
        got = bf.to_mac_path("/music/Database/Bonobo/Drift.flac")
        self.assertEqual(got, f"{bf.HOME}/Music/Database/Bonobo/Drift.flac")

    def test_non_container_paths_pass_through(self):
        native = f"{bf.HOME}/Music/Database/A/b.flac"
        self.assertEqual(bf.to_mac_path(native), native)

    def test_none_is_handled(self):
        self.assertIsNone(bf.to_mac_path(None))

    def test_other_music_subtrees_are_not_rewritten_into_database(self):
        # /music/Input/ is a real, different location; silently relocating it into
        # Database/ would match the wrong Lexicon row.
        self.assertEqual(bf.to_mac_path("/music/Input/x.flac"), "/music/Input/x.flac")


if __name__ == "__main__":
    unittest.main()
