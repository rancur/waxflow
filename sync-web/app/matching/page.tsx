'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'
import { MatchCandidate } from '../types'

function formatDuration(ms: number): string {
  const m = Math.floor(ms / 60_000)
  const s = Math.floor((ms % 60_000) / 1000)
  return `${m}:${s.toString().padStart(2, '0')}`
}

function confidenceColor(c: number): string {
  if (c >= 0.9) return 'bg-emerald-500'
  if (c >= 0.7) return 'bg-amber-500'
  return 'bg-red-500'
}

export default function MatchingPage() {
  const [candidates, setCandidates] = useState<MatchCandidate[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [actionLoading, setActionLoading] = useState<string | null>(null)

  const fetchCandidates = useCallback(async () => {
    try {
      const result = await apiFetch<MatchCandidate[]>('/matching/review')
      setCandidates(result)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch matches')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchCandidates()
  }, [fetchCandidates])

  const handleAction = async (trackId: string, action: 'approve' | 'reject' | 'skip') => {
    setActionLoading(trackId)
    try {
      await apiFetch(`/matching/${trackId}/${action}`, { method: 'POST' })
      setCandidates((prev) => prev.filter((c) => c.track_id !== trackId))
    } catch (err) {
      setError(err instanceof Error ? err.message : `Failed to ${action}`)
    } finally {
      setActionLoading(null)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Match Review</h1>
        <p className="text-sm text-slate-500 mt-1">
          {loading ? '...' : `${candidates.length} tracks need review`}
        </p>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4 flex items-center justify-between">
          <p className="text-sm text-red-400">{error}</p>
          <button onClick={fetchCandidates} className="btn-secondary text-xs">Retry</button>
        </div>
      )}

      {loading ? (
        <div className="space-y-4">
          {Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="card">
              <div className="h-32 skeleton rounded-lg" />
            </div>
          ))}
        </div>
      ) : candidates.length === 0 ? (
        <div className="card text-center py-16">
          <div className="text-4xl mb-3 text-slate-600">&#10003;</div>
          <p className="text-slate-400">No tracks need review</p>
          <p className="text-xs text-slate-600 mt-1">All matches look good</p>
        </div>
      ) : (
        <div className="space-y-4">
          {candidates.map((c) => (
            <div key={c.track_id} className="card">
              {/* Confidence bar */}
              <div className="flex items-center gap-3 mb-4">
                <div className="flex-1 h-2 bg-slate-800 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${confidenceColor(c.confidence)}`}
                    style={{ width: `${c.confidence * 100}%` }}
                  />
                </div>
                <span className="text-sm font-mono text-slate-400 tabular-nums w-14 text-right">
                  {(c.confidence * 100).toFixed(1)}%
                </span>
              </div>

              {/* Comparison */}
              <div className="grid md:grid-cols-2 gap-6">
                {/* Spotify side */}
                <div className="space-y-2">
                  <p className="text-xs font-semibold text-green-400 uppercase tracking-wider">Spotify</p>
                  <div className="bg-slate-800/50 rounded-lg p-4 space-y-2">
                    <div>
                      <p className="text-xs text-slate-500">Title</p>
                      <p className="text-sm text-slate-200 font-medium">{c.spotify_title}</p>
                    </div>
                    <div>
                      <p className="text-xs text-slate-500">Artist</p>
                      <p className="text-sm text-slate-300">{c.spotify_artist}</p>
                    </div>
                    <div>
                      <p className="text-xs text-slate-500">Album</p>
                      <p className="text-sm text-slate-300">{c.spotify_album}</p>
                    </div>
                    <div>
                      <p className="text-xs text-slate-500">Duration</p>
                      <p className="text-sm text-slate-300">{formatDuration(c.spotify_duration_ms)}</p>
                    </div>
                  </div>
                </div>

                {/* Candidate side */}
                <div className="space-y-2">
                  <p className="text-xs font-semibold text-purple-400 uppercase tracking-wider">Matched File</p>
                  <div className="bg-slate-800/50 rounded-lg p-4 space-y-2">
                    <div>
                      <p className="text-xs text-slate-500">Title</p>
                      <p className="text-sm text-slate-200 font-medium">{c.candidate_title}</p>
                    </div>
                    <div>
                      <p className="text-xs text-slate-500">Artist</p>
                      <p className="text-sm text-slate-300">{c.candidate_artist}</p>
                    </div>
                    <div className="col-span-2">
                      <p className="text-xs text-slate-500">Path</p>
                      <p className="text-sm text-slate-300 font-mono text-xs truncate">{c.candidate_path}</p>
                    </div>
                  </div>
                </div>
              </div>

              {/* Actions */}
              <div className="flex gap-3 mt-4 pt-4 border-t border-slate-800">
                <button
                  className="btn-primary text-sm"
                  disabled={actionLoading === c.track_id}
                  onClick={() => handleAction(c.track_id, 'approve')}
                >
                  {actionLoading === c.track_id ? 'Processing...' : 'Approve'}
                </button>
                <button
                  className="btn-danger text-sm"
                  disabled={actionLoading === c.track_id}
                  onClick={() => handleAction(c.track_id, 'reject')}
                >
                  Reject
                </button>
                <button
                  className="btn-secondary text-sm"
                  disabled={actionLoading === c.track_id}
                  onClick={() => handleAction(c.track_id, 'skip')}
                >
                  Skip
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
