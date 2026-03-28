export interface Track {
  id: string
  spotify_id: string
  title: string
  artist: string
  album: string
  duration_ms: number
  added_at: string
  pipeline_stage: PipelineStage
  match_status: MatchStatus
  download_status: DownloadStatus
  verified: boolean
  in_lexicon: boolean
  file_path?: string
  confidence_score?: number
  error_message?: string
}

export type PipelineStage =
  | 'pending'
  | 'matching'
  | 'downloading'
  | 'verifying'
  | 'complete'
  | 'error'

export type MatchStatus =
  | 'unmatched'
  | 'matched'
  | 'mismatched'
  | 'approved'
  | 'rejected'

export type DownloadStatus =
  | 'pending'
  | 'queued'
  | 'downloading'
  | 'complete'
  | 'failed'

export interface DashboardData {
  parity: ParityData
  stages: StageBreakdown
  activity: ActivityEvent[]
  health: ServiceHealth
}

export interface ParityData {
  synced: number
  total: number
  percentage: number
}

export interface StageBreakdown {
  missing: number
  downloading: number
  verifying: number
  mismatched: number
  complete: number
  error: number
}

export interface ActivityEvent {
  id: string
  timestamp: string
  type: 'download' | 'match' | 'verify' | 'error' | 'sync' | 'upload'
  message: string
}

export interface ServiceHealth {
  spotify: ServiceStatus
  tidarr: ServiceStatus
  lexicon: ServiceStatus
}

export interface ServiceStatus {
  connected: boolean
  last_check: string
  latency_ms?: number
}

export interface MatchCandidate {
  track_id: string
  spotify_title: string
  spotify_artist: string
  spotify_album: string
  spotify_duration_ms: number
  candidate_title: string
  candidate_artist: string
  candidate_path: string
  confidence: number
}

export interface DownloadQueueItem {
  id: string
  track_id: string
  title: string
  artist: string
  status: DownloadStatus
  progress: number
  speed_kbps?: number
  error_message?: string
  started_at?: string
}

export interface DownloadStats {
  total_downloaded: number
  total_failed: number
  avg_speed_kbps: number
  queue_size: number
}

export interface Playlist {
  id: string
  name: string
  year?: number
  month?: number
  track_count: number
  synced: boolean
  children?: Playlist[]
  tracks?: Track[]
}

export interface AppSettings {
  poll_interval_seconds: number
  fingerprint_threshold: number
  auto_download: boolean
  auto_verify: boolean
  download_quality: string
  max_concurrent_downloads: number
}

export interface LexiconBackup {
  id: string
  filename: string
  created_at: string
  size_bytes: number
}

export interface VersionInfo {
  version: string
  git_sha: string
  build_date: string
}

export interface TracksResponse {
  tracks: Track[]
  total: number
  page: number
  per_page: number
}

export interface DownloadsResponse {
  active: DownloadQueueItem[]
  queued: DownloadQueueItem[]
  failed: DownloadQueueItem[]
  stats: DownloadStats
}
