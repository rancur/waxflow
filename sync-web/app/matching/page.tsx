'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'

interface ReviewStats {
  total_mismatched: number
  pending_review: number
  approved: number
  rejected: number
}

interface ReviewTrack {
  track: {
    id: number
    title?: string
    artist?: string
    album?: string
    duration_ms?: number
    spotify_id: string
    [key: string]: any
  }
  spotify_title?: string
  spotify_artist?: string
  spotify_album?: string
  spotify_duration_s?: number
  spotify_duration_ms?: number
  spotify_isrc?: string
  spotify_added_at?: string
  spotify_popularity?: number
  file_path?: string
  verify_codec?: string
  verify_sample_rate?: number
  verify_bit_depth?: number
  verify_is_genuine_lossless?: boolean
  match_source?: string
  match_confidence?: number
  tidal_id?: string
  fingerprint_match_score?: number
  pipeline_stage?: string
  pipeline_error?: string
  download_status?: string
  duration_diff_seconds?: number
  title_similarity?: string
  artist_similarity?: string
}

interface ReviewResponse {
  tracks: ReviewTrack[]
  total: number
  stats: ReviewStats
}

function formatDuration(seconds: number): string {
  if (!seconds) return '--:--'
  const m = Math.floor(seconds / 60)
  const s = Math.floor(seconds % 60)
  return `${m}:${s.toString().padStart(2, '0')}`
}

function formatDate(dateStr?: string): string {
  if (!dateStr) return '--'
  try {
    const d = new Date(dateStr)
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
  } catch {
    return dateStr
  }
}

function confidencePercent(c: number | undefined | null): number {
  return Math.round((c ?? 0) * 100)
}

function confidenceColor(c: number): string {
  if (c >= 0.9) return 'text-emerald-400'
  if (c >= 0.7) return 'text-amber-400'
  return 'text-red-400'
}

function confidenceBgColor(c: number): string {
  if (c >= 0.9) return 'bg-emerald-500'
  if (c >= 0.7) return 'bg-amber-500'
  return 'bg-red-500'
}

function confidenceBorderColor(c: number): string {
  if (c >= 0.9) return 'border-emerald-500/30'
  if (c >= 0.7) return 'border-amber-500/30'
  return 'border-red-500/30'
}

function confidenceGlow(c: number): string {
  if (c >= 0.9) return 'glow-green'
  if (c >= 0.7) return 'glow-amber'
  return 'glow-red'
}

function durationDiffBadge(diffSeconds: number | null | undefined): { text: string; color: string } {
  if (diffSeconds == null) return { text: 'N/A', color: 'text-slate-500' }
  const abs = Math.abs(diffSeconds)
  const sign = diffSeconds >= 0 ? '+' : '-'
  const text = `${sign}${abs.toFixed(0)}s`
  if (abs < 3) return { text, color: 'text-emerald-400' }
  if (abs < 10) return { text, color: 'text-amber-400' }
  return { text, color: 'text-red-400' }
}

function similarityBadge(sim: string | undefined): { label: string; color: string; icon: string } {
  switch (sim) {
    case 'exact':
      return { label: 'Exact', color: 'text-emerald-400 bg-emerald-500/10', icon: '\u2713' }
    case 'partial':
      return { label: 'Partial', color: 'text-amber-400 bg-amber-500/10', icon: '~' }
    case 'different':
      return { label: 'Different', color: 'text-red-400 bg-red-500/10', icon: '\u2717' }
    default:
      return { label: 'Unknown', color: 'text-slate-500 bg-slate-500/10', icon: '?' }
  }
}

function matchSourceLabel(source: string | undefined): string {
  if (!source) return 'Unknown'
  const labels: Record<string, string> = {
    isrc: 'ISRC Lookup',
    search: 'Metadata Search',
    manual: 'Manual',
    fingerprint: 'Acoustic Fingerprint',
    file_index: 'File Index',
    tidal_search: 'Tidal Search',
  }
  return labels[source] || source
}

function truncatePath(path: string | undefined, maxLen = 50): string {
  if (!path) return 'N/A'
  if (path.length <= maxLen) return path
  return '...' + path.slice(-maxLen)
}

function StatCard({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div className="card flex flex-col items-center justify-center py-4 px-3 min-w-0">
      <span className={`text-2xl font-bold tabular-nums ${color}`}>{value}</span>
      <span className="text-xs text-slate-500 mt-1 text-center">{label}</span>
    </div>
  )
}

export default function MatchingPage() {
  const [tracks, setTracks] = useState<ReviewTrack[]>([])
  const [stats, setStats] = useState<ReviewStats>({ total_mismatched: 0, pending_review: 0, approved: 0, rejected: 0 })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [actionLoading, setActionLoading] = useState<number | null>(null)
  const [skippedIds, setSkippedIds] = useState<Set<number>>(new Set())

  const fetchData = useCallback(async () => {
    try {
      const result = await apiFetch<ReviewResponse>('/matching/review')
      setTracks(result.tracks || [])
      setStats(result.stats || { total_mismatched: 0, pending_review: 0, approved: 0, rejected: 0 })
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch matches')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchData()
  }, [fetchData])

  const handleAction = async (trackId: number, action: 'approve' | 'reject' | 'skip') => {
    if (action === 'skip') {
      setSkippedIds((prev) => new Set(prev).add(trackId))
      return
    }

    setActionLoading(trackId)
    try {
      await apiFetch(`/matching/${trackId}/${action}`, { method: 'POST' })
      setTracks((prev) => prev.filter((c) => c.track?.id !== trackId))
      // Update stats locally
      setStats((prev) => ({
        ...prev,
        total_mismatched: Math.max(0, prev.total_mismatched - 1),
        pending_review: Math.max(0, prev.pending_review - 1),
        approved: action === 'approve' ? prev.approved + 1 : prev.approved,
        rejected: action === 'reject' ? prev.rejected + 1 : prev.rejected,
      }))
    } catch (err) {
      setError(err instanceof Error ? err.message : `Failed to ${action}`)
    } finally {
      setActionLoading(null)
    }
  }

  const visibleTracks = tracks.filter((t) => !skippedIds.has(t.track?.id))
  const skippedCount = skippedIds.size

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Match Review</h1>
        <p className="text-sm text-slate-500 mt-1">
          {loading ? 'Loading...' : 'Review mismatched tracks and approve or reject matches'}
        </p>
      </div>

      {/* Summary Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Total Mismatched" value={stats.total_mismatched} color="text-slate-200" />
        <StatCard label="Pending Review" value={stats.pending_review} color="text-amber-400" />
        <StatCard label="Approved" value={stats.approved} color="text-emerald-400" />
        <StatCard label="Rejected" value={stats.rejected} color="text-red-400" />
      </div>

      {/* Error banner */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4 flex items-center justify-between">
          <p className="text-sm text-red-400">{error}</p>
          <button onClick={fetchData} className="btn-secondary text-xs">Retry</button>
        </div>
      )}

      {/* Skipped notice */}
      {skippedCount > 0 && (
        <div className="bg-slate-800/50 border border-slate-700 rounded-xl px-5 py-3 flex items-center justify-between">
          <p className="text-sm text-slate-400">{skippedCount} track{skippedCount > 1 ? 's' : ''} skipped</p>
          <button
            onClick={() => setSkippedIds(new Set())}
            className="text-xs text-slate-400 hover:text-slate-200 underline underline-offset-2"
          >
            Show all
          </button>
        </div>
      )}

      {/* Loading skeleton */}
      {loading ? (
        <div className="space-y-4">
          {Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="card">
              <div className="h-48 skeleton rounded-lg" />
            </div>
          ))}
        </div>
      ) : visibleTracks.length === 0 ? (
        /* Empty state */
        <div className="card text-center py-20">
          <div className="text-5xl mb-4 text-emerald-500/60">{'\u2713'}</div>
          <p className="text-lg text-slate-300 font-medium">No tracks need review</p>
          <p className="text-sm text-slate-500 mt-2">All matches look good!</p>
          {skippedCount > 0 && (
            <button
              onClick={() => setSkippedIds(new Set())}
              className="btn-secondary text-sm mt-4"
            >
              Show {skippedCount} skipped track{skippedCount > 1 ? 's' : ''}
            </button>
          )}
        </div>
      ) : (
        /* Comparison Cards */
        <div className="space-y-5">
          {visibleTracks.map((c) => {
            const track = c.track || {} as any
            const trackId = track.id
            const confidence = c.match_confidence ?? 0
            const fpScore = c.fingerprint_match_score
            const isProcessing = actionLoading === trackId
            const titleSim = similarityBadge(c.title_similarity)
            const artistSim = similarityBadge(c.artist_similarity)
            const durDiff = durationDiffBadge(c.duration_diff_seconds)

            return (
              <div
                key={trackId}
                className={`card ${confidenceGlow(confidence)} border ${confidenceBorderColor(confidence)} transition-all duration-300`}
              >
                {/* Card Header */}
                <div className="flex items-center justify-between mb-5">
                  <div className="flex items-center gap-3">
                    <span className="text-lg">{confidence < 0.7 ? '\u26A0\uFE0F' : confidence < 0.9 ? '\u2753' : '\u2705'}</span>
                    <div>
                      <span className="text-sm text-slate-400">
                        {confidence < 0.7 ? 'Likely Mismatch' : confidence < 0.9 ? 'Possible Mismatch' : 'Probable Match'}
                      </span>
                      <span className="mx-2 text-slate-700">|</span>
                      <span className={`text-lg font-bold tabular-nums ${confidenceColor(confidence)}`}>
                        {confidencePercent(confidence)}%
                      </span>
                    </div>
                  </div>

                  {/* Action buttons — top right */}
                  <div className="flex gap-2">
                    <button
                      className="btn-primary text-sm"
                      disabled={isProcessing}
                      onClick={() => handleAction(trackId, 'approve')}
                    >
                      {isProcessing ? '...' : 'Approve'}
                    </button>
                    <button
                      className="btn-danger text-sm"
                      disabled={isProcessing}
                      onClick={() => handleAction(trackId, 'reject')}
                    >
                      Reject
                    </button>
                    <button
                      className="btn-secondary text-sm"
                      disabled={isProcessing}
                      onClick={() => handleAction(trackId, 'skip')}
                    >
                      Skip
                    </button>
                  </div>
                </div>

                {/* Confidence bar */}
                <div className="w-full h-1.5 bg-slate-800 rounded-full overflow-hidden mb-5">
                  <div
                    className={`h-full rounded-full transition-all duration-500 ${confidenceBgColor(confidence)}`}
                    style={{ width: `${Math.max(confidence * 100, 2)}%` }}
                  />
                </div>

                {/* Side-by-side comparison */}
                <div className="grid md:grid-cols-2 gap-4">
                  {/* Spotify side */}
                  <div>
                    <div className="flex items-center gap-2 mb-3">
                      <span className="w-2.5 h-2.5 rounded-full bg-green-500"></span>
                      <span className="text-xs font-semibold text-green-400 uppercase tracking-wider">Spotify</span>
                    </div>
                    <div className="bg-slate-800/60 rounded-lg p-4 space-y-3 h-full">
                      <Row label="Title" value={c.spotify_title} />
                      <Row label="Artist" value={c.spotify_artist} />
                      <Row label="Album" value={c.spotify_album} />
                      <Row label="Duration" value={formatDuration(c.spotify_duration_s ?? 0)} />
                      <Row label="ISRC" value={c.spotify_isrc} mono />
                      <Row label="Added" value={formatDate(c.spotify_added_at)} />
                      {c.spotify_popularity != null && (
                        <Row label="Popularity" value={`${c.spotify_popularity}/100`} />
                      )}
                    </div>
                  </div>

                  {/* Matched file side */}
                  <div>
                    <div className="flex items-center gap-2 mb-3">
                      <span className="w-2.5 h-2.5 rounded-full bg-blue-500"></span>
                      <span className="text-xs font-semibold text-blue-400 uppercase tracking-wider">Matched File</span>
                    </div>
                    <div className="bg-slate-800/60 rounded-lg p-4 space-y-3 h-full">
                      <Row label="Title" value={track.title || '--'} />
                      <Row label="Artist" value={track.artist || '--'} />
                      <Row label="Album" value={track.album || '--'} />
                      <Row
                        label="Codec"
                        value={
                          [
                            c.verify_codec || '?',
                            c.verify_bit_depth ? `${c.verify_bit_depth}-bit` : null,
                            c.verify_sample_rate ? `${(c.verify_sample_rate / 1000).toFixed(1)}kHz` : null,
                          ]
                            .filter(Boolean)
                            .join(' / ')
                        }
                      />
                      <Row label="File" value={truncatePath(c.file_path, 45)} mono title={c.file_path} />
                      {fpScore != null && (
                        <Row
                          label="Fingerprint"
                          value={`${(fpScore * 100).toFixed(1)}%`}
                          valueColor={confidenceColor(fpScore)}
                        />
                      )}
                      {c.tidal_id && <Row label="Tidal ID" value={c.tidal_id} mono />}
                    </div>
                  </div>
                </div>

                {/* Comparison details footer */}
                <div className="mt-4 pt-4 border-t border-slate-800">
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                    {/* Duration diff */}
                    <div className="flex flex-col">
                      <span className="text-xs text-slate-500 mb-1">Duration Diff</span>
                      <span className={`font-mono font-medium ${durDiff.color}`}>{durDiff.text}</span>
                    </div>

                    {/* Title match */}
                    <div className="flex flex-col">
                      <span className="text-xs text-slate-500 mb-1">Title Match</span>
                      <span className={`inline-flex items-center gap-1.5 text-xs font-medium px-2 py-0.5 rounded-full w-fit ${titleSim.color}`}>
                        <span>{titleSim.icon}</span>
                        {titleSim.label}
                      </span>
                    </div>

                    {/* Artist match */}
                    <div className="flex flex-col">
                      <span className="text-xs text-slate-500 mb-1">Artist Match</span>
                      <span className={`inline-flex items-center gap-1.5 text-xs font-medium px-2 py-0.5 rounded-full w-fit ${artistSim.color}`}>
                        <span>{artistSim.icon}</span>
                        {artistSim.label}
                      </span>
                    </div>

                    {/* Match source */}
                    <div className="flex flex-col">
                      <span className="text-xs text-slate-500 mb-1">Match Source</span>
                      <span className="text-slate-300">{matchSourceLabel(c.match_source)}</span>
                    </div>
                  </div>
                </div>

                {/* Pipeline error */}
                {c.pipeline_error && (
                  <div className="mt-3 bg-red-500/10 border border-red-500/20 rounded-lg px-4 py-2">
                    <p className="text-xs text-red-400 font-mono">{c.pipeline_error}</p>
                  </div>
                )}

                {/* External search links */}
                <div className="mt-3 flex gap-2 flex-wrap">
                  <SearchLink
                    label="Beatport"
                    url={`https://www.beatport.com/search?q=${encodeURIComponent(`${c.spotify_artist || ''} ${c.spotify_title || ''}`.trim())}`}
                  />
                  <SearchLink
                    label="Bandcamp"
                    url={`https://bandcamp.com/search?q=${encodeURIComponent(`${c.spotify_artist || ''} ${c.spotify_title || ''}`.trim())}`}
                  />
                  <SearchLink
                    label="SoundCloud"
                    url={`https://soundcloud.com/search?q=${encodeURIComponent(`${c.spotify_artist || ''} ${c.spotify_title || ''}`.trim())}`}
                  />
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

function Row({
  label,
  value,
  mono,
  title,
  valueColor,
}: {
  label: string
  value?: string | null
  mono?: boolean
  title?: string
  valueColor?: string
}) {
  return (
    <div>
      <p className="text-xs text-slate-500">{label}</p>
      <p
        className={`text-sm ${valueColor || 'text-slate-200'} ${mono ? 'font-mono text-xs' : ''} break-words`}
        title={title}
      >
        {value || '--'}
      </p>
    </div>
  )
}

function SearchLink({ label, url }: { label: string; url: string }) {
  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className="text-xs text-slate-500 hover:text-slate-300 border border-slate-700 hover:border-slate-600 rounded-md px-2.5 py-1 transition-colors"
    >
      Search {label} {'\u2197'}
    </a>
  )
}
