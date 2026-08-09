"""Version comparison for the auto-update check.

This is the bug that made auto-update dead on arrival: the check was a plain
string compare, and string ordering is not version ordering across a component
boundary. `"2.11.0" > "2.9.0"` is False because at index 2, "1" < "9" — so every
x.9 -> x.10+ upgrade was invisible, and the reverse comparison was True, meaning
a DOWNGRADE could be offered as an update.

Kept as a standalone module (no `tasks.helpers` import, which drags in spotipy)
so it runs anywhere.
"""

import os
import sys
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)


def _load():
    """Load only the pure helpers, skipping the module's heavy imports."""
    src = open(os.path.join(SYNC_WORKER_DIR, "tasks", "auto_update.py")).read()
    start = src.index("def _version_tuple(")
    end = src.index("def _is_right_time(")
    ns: dict = {}
    exec(compile(src[start:end], "auto_update_helpers", "exec"), ns)
    return ns["_is_newer"], ns["_version_tuple"]


_is_newer, _version_tuple = _load()


class VersionCompareTest(unittest.TestCase):
    def test_the_regression_that_broke_auto_update(self):
        # Both of these are False under a string compare.
        self.assertTrue(_is_newer("2.10.1", "2.9.0"))
        self.assertTrue(_is_newer("2.11.0", "2.9.0"))

    def test_never_offers_a_downgrade(self):
        # String compare said True here, i.e. it would have "updated" backwards.
        self.assertFalse(_is_newer("2.9.0", "2.11.0"))
        self.assertFalse(_is_newer("1.0.0", "2.0.0"))

    def test_equal_is_not_newer(self):
        self.assertFalse(_is_newer("2.11.0", "2.11.0"))

    def test_ordinary_increments(self):
        self.assertTrue(_is_newer("2.11.1", "2.11.0"))
        self.assertTrue(_is_newer("3.0.0", "2.99.99"))
        self.assertFalse(_is_newer("2.11.0", "2.11.1"))

    def test_suffixes_and_v_prefix_do_not_explode(self):
        self.assertEqual(_version_tuple("2.11.0-rc1"), (2, 11, 0))
        self.assertTrue(_is_newer("2.12.0-beta", "2.11.0"))

    def test_missing_or_empty_is_never_newer(self):
        for latest, current in (("", "2.11.0"), ("2.11.0", ""), ("", ""), (None, "2.11.0")):
            self.assertFalse(_is_newer(latest, current), f"{latest!r} vs {current!r}")

    def test_differing_component_counts(self):
        self.assertTrue(_is_newer("2.11", "2.9.9"))
        self.assertFalse(_is_newer("2.11", "2.11.0"))


if __name__ == "__main__":
    unittest.main()
