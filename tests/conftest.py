"""Stub external worker dependencies so tests can import process_pipeline."""
import sys
import os
import types
from unittest.mock import MagicMock

# Stub spotipy before any worker import
_spotipy = types.ModuleType("spotipy")
_spotipy.Spotify = MagicMock
_spotipy.oauth2 = types.ModuleType("spotipy.oauth2")
_spotipy.oauth2.SpotifyOAuth = MagicMock
sys.modules.setdefault("spotipy", _spotipy)
sys.modules.setdefault("spotipy.oauth2", _spotipy.oauth2)

# Stub httpx
_httpx = types.ModuleType("httpx")
_httpx.Client = MagicMock
sys.modules.setdefault("httpx", _httpx)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "sync-worker"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "sync-worker", "tasks"))
