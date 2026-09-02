"""Every task module that is supposed to run on a schedule must actually be
registered in worker.main(). tasks/auto_update.py sat unregistered from 2.12.0 to
2.19.0 -- the 3am auto-update never fired and nobody noticed. This pins the list.
"""

import os
import re
import sys
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

REQUIRED = (
    "poll_spotify", "process_pipeline", "parity_check", "retry_unmatched",
    "index_library", "create_playlists", "lexicon_health_check", "mac_availability",
    "import_catchup", "lexicon_reconcile", "auto_update", "quality_upgrade",
    "hunter", "metadata_fallback",
)


class TestWorkerRegistration(unittest.TestCase):
    def test_every_scheduled_task_is_registered(self):
        src = open(os.path.join(SYNC_WORKER_DIR, "worker.py")).read()
        registered = set(re.findall(r'run_task\("([a-z_]+)"', src))
        missing = [n for n in REQUIRED if n not in registered]
        self.assertEqual(missing, [], f"unregistered worker tasks: {missing}")


if __name__ == "__main__":
    unittest.main()
