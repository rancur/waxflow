from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime


# --- Track models ---

class TrackBase(BaseModel):
    spotify_id: str
    spotify_uri: Optional[str] = None
    spotify_added_at: Optional[str] = None
    title: Optional[str] = None
    artist: Optional[str] = None
    album: Optional[str] = None
    duration_ms: Optional[int] = None
    isrc: Optional[str] = None
    spotify_popularity: Optional[int] = None


class TrackOut(TrackBase):
    id: int
    match_status: str = "pending"
    match_source: Optional[str] = None
    match_confidence: Optional[float] = None
    tidal_id: Optional[str] = None
    download_status: str = "pending"
    download_source: Optional[str] = None
    download_attempts: int = 0
    download_error: Optional[str] = None
    file_path: Optional[str] = None
    file_hash_sha256: Optional[str] = None
    verify_status: str = "pending"
    verify_codec: Optional[str] = None
    verify_sample_rate: Optional[int] = None
    verify_bit_depth: Optional[int] = None
    verify_is_genuine_lossless: Optional[bool] = None
    chromaprint: Optional[str] = None
    fingerprint_match_score: Optional[float] = None
    lexicon_status: str = "pending"
    lexicon_track_id: Optional[str] = None
    lexicon_playlist_id: Optional[str] = None
    pipeline_stage: str = "new"
    pipeline_error: Optional[str] = None
    is_protected: bool = False
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        from_attributes = True


class TrackUpdate(BaseModel):
    is_protected: Optional[bool] = None
    notes: Optional[str] = None
    match_status: Optional[str] = None
    tidal_id: Optional[str] = None
    file_path: Optional[str] = None


class TrackListResponse(BaseModel):
    tracks: list[TrackOut]
    total: int
    page: int
    per_page: int
    pages: int


class ParityResponse(BaseModel):
    spotify_total: int
    lexicon_synced: int
    missing: int
    mismatched: int
    lexicon_only: int
    parity_pct: float


# --- Playlist models ---

class PlaylistOut(BaseModel):
    id: int
    folder_name: Optional[str] = None
    playlist_name: str
    year: int
    month: int
    lexicon_folder_id: Optional[str] = None
    lexicon_playlist_id: Optional[str] = None
    track_count: int = 0
    created_at: Optional[str] = None

    class Config:
        from_attributes = True


class PlaylistDetailOut(PlaylistOut):
    tracks: list[TrackOut] = []


# --- Download models ---

class DownloadQueueItem(BaseModel):
    id: int
    track_id: int
    priority: int = 0
    source: str = "tiddl"
    status: str = "pending"
    attempts: int = 0
    max_attempts: int = 3
    error: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    track: Optional[TrackOut] = None

    class Config:
        from_attributes = True


class DownloadStatsResponse(BaseModel):
    total: int
    pending: int
    queued: int
    downloading: int
    complete: int
    failed: int
    avg_download_time_seconds: Optional[float] = None
    estimated_remaining_seconds: Optional[float] = None
    method: str = "tiddl"
    tiddl_available: bool = False
    tidarr_reachable: bool = False


# --- Matching models ---

class ManualMatchRequest(BaseModel):
    tidal_id: Optional[str] = None
    file_path: Optional[str] = None


# --- Lexicon models ---

class LexiconStatusResponse(BaseModel):
    connected: bool
    base_url: str
    last_sync: Optional[str] = None
    track_count: Optional[int] = None


class LexiconBackupOut(BaseModel):
    id: int
    backup_path: str
    backup_size_bytes: Optional[int] = None
    trigger: Optional[str] = None
    created_at: Optional[str] = None

    class Config:
        from_attributes = True


# --- Dashboard models ---

class ServiceHealth(BaseModel):
    name: str
    status: str  # "ok" | "error"
    latency_ms: Optional[float] = None
    error: Optional[str] = None


class DashboardResponse(BaseModel):
    spotify_total: int
    lexicon_synced: int
    parity_pct: float
    by_pipeline_stage: dict[str, int]
    by_match_status: dict[str, int]
    by_download_status: dict[str, int]
    by_verify_status: dict[str, int]
    by_lexicon_status: dict[str, int]
    recent_activity: list[dict[str, Any]]
    services: list[ServiceHealth]


# --- Activity log ---

class ActivityLogEntry(BaseModel):
    id: int
    event_type: str
    track_id: Optional[int] = None
    message: str
    details: Optional[str] = None
    created_at: Optional[str] = None

    class Config:
        from_attributes = True


# --- Config models ---

class ConfigUpdate(BaseModel):
    settings: dict[str, str]


class SpotifyStatusResponse(BaseModel):
    authenticated: bool
    token_valid: bool
    token_expiry: Optional[str] = None
    last_poll: Optional[str] = None


# --- Admin models ---

class HealthResponse(BaseModel):
    status: str
    database: str
    uptime_seconds: Optional[float] = None


class VersionResponse(BaseModel):
    version: Optional[str] = None
    git_sha: Optional[str] = None
    build_date: Optional[str] = None
