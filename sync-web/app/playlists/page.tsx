'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'

interface PlaylistData {
  id: number
  folder_name: string
  playlist_name: string
  year: number
  month: number
  lexicon_folder_id: string | null
  lexicon_playlist_id: string | null
  track_count: number
  created_at: string
}

export default function PlaylistsPage() {
  const [playlists, setPlaylists] = useState<PlaylistData[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [expandedYear, setExpandedYear] = useState<number | null>(null)

  const fetchPlaylists = useCallback(async () => {
    try {
      const result = await apiFetch<any>('/playlists')
      setPlaylists(result.playlists || [])
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch playlists')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchPlaylists() }, [fetchPlaylists])

  const byYear: Record<number, PlaylistData[]> = {}
  for (const pl of playlists) {
    if (!byYear[pl.year]) byYear[pl.year] = []
    byYear[pl.year].push(pl)
  }
  const sortedYears = Object.keys(byYear).map(Number).sort((a, b) => b - a)
  for (const year of sortedYears) {
    byYear[year].sort((a, b) => a.month - b.month)
  }

  const totalTracks = playlists.reduce((sum, pl) => sum + pl.track_count, 0)

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Playlists</h1>
        <p className="text-sm text-slate-500 mt-1">
          {playlists.length} playlists, {totalTracks} tracks total
        </p>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {loading ? (
        <div className="space-y-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <div key={i} className="card"><div className="h-12 skeleton rounded" /></div>
          ))}
        </div>
      ) : playlists.length === 0 ? (
        <div className="card text-center py-12">
          <p className="text-slate-500">No playlists created yet.</p>
        </div>
      ) : (
        <div className="space-y-2">
          {sortedYears.map(year => (
            <div key={year} className="card p-0 overflow-hidden">
              <button
                className="w-full flex items-center justify-between px-5 py-4 hover:bg-slate-800/50 transition-colors"
                onClick={() => setExpandedYear(expandedYear === year ? null : year)}
              >
                <div className="flex items-center gap-3">
                  <svg
                    className={`w-4 h-4 text-slate-500 transition-transform ${expandedYear === year ? 'rotate-90' : ''}`}
                    fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                  </svg>
                  <span className="text-lg font-bold text-slate-200">{year}</span>
                </div>
                <div className="flex items-center gap-4">
                  <span className="text-xs text-slate-500">{byYear[year].length} playlists</span>
                  <span className="text-sm font-medium text-emerald-400">
                    {byYear[year].reduce((s, p) => s + p.track_count, 0)} tracks
                  </span>
                </div>
              </button>
              {expandedYear === year && (
                <div className="border-t border-slate-800">
                  {byYear[year].map(pl => (
                    <div
                      key={pl.id}
                      className="flex items-center justify-between px-5 py-3 border-b border-slate-800/50 last:border-0 hover:bg-slate-800/30"
                    >
                      <div className="flex items-center gap-3">
                        <svg className="w-4 h-4 text-slate-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M9 9l10.5-3m0 6.553v3.75a2.25 2.25 0 01-1.632 2.163l-1.32.377a1.803 1.803 0 11-.99-3.467l2.31-.66a2.25 2.25 0 001.632-2.163zm0 0V2.25L9 5.25v10.303m0 0v3.75a2.25 2.25 0 01-1.632 2.163l-1.32.377a1.803 1.803 0 01-.99-3.467l2.31-.66A2.25 2.25 0 009 15.553z" />
                        </svg>
                        <span className="text-sm text-slate-300">{pl.playlist_name}</span>
                      </div>
                      <div className="flex items-center gap-4">
                        <span className="text-sm font-medium text-slate-400 tabular-nums">{pl.track_count} tracks</span>
                        {pl.lexicon_playlist_id ? (
                          <span className="text-xs text-emerald-400">Synced</span>
                        ) : (
                          <span className="text-xs text-slate-600">Pending</span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
