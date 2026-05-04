"""pytest configuration: wire up sys.path and stub heavy optional dependencies."""

import sys
import os
from unittest.mock import MagicMock

# Stub out spotipy so worker modules can be imported without it installed
for _mod in ("spotipy", "spotipy.oauth2"):
    sys.modules.setdefault(_mod, MagicMock())

# Allow `import tasks.xxx` from the sync-worker directory
_worker_dir = os.path.join(os.path.dirname(__file__), "..", "sync-worker")
if _worker_dir not in sys.path:
    sys.path.insert(0, os.path.abspath(_worker_dir))
