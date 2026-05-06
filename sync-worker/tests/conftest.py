import sys
import os
import types
from unittest.mock import MagicMock

# Allow imports from sync-worker root (e.g. `from tasks.process_pipeline import ...`)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub out heavy runtime dependencies so pure functions can be unit-tested
# without Docker / installed packages.
_httpx_stub = types.ModuleType("httpx")
_httpx_stub.Client = MagicMock
sys.modules.setdefault("httpx", _httpx_stub)

_spotipy_stub = types.ModuleType("spotipy")
_spotipy_stub.Spotify = MagicMock
sys.modules.setdefault("spotipy", _spotipy_stub)
sys.modules.setdefault("spotipy.oauth2", types.ModuleType("spotipy.oauth2"))

# Stub tasks.helpers so process_pipeline can be imported
_helpers_stub = types.ModuleType("tasks.helpers")
_helpers_stub.LEXICON_API_URL = "http://localhost:48624"
_helpers_stub.MUSIC_LIBRARY_PATH = "/music"
_helpers_stub.TIDARR_URL = "http://tidarr:8484"
_helpers_stub.get_config = MagicMock(return_value=None)
_helpers_stub.get_db = MagicMock()
_helpers_stub.get_tracks_by_stage = MagicMock(return_value=[])
_helpers_stub.log_activity = MagicMock()
_helpers_stub.sanitize_filename = MagicMock(side_effect=lambda s: s)
_helpers_stub.set_config = MagicMock()
_helpers_stub.update_track = MagicMock()
sys.modules["tasks.helpers"] = _helpers_stub
