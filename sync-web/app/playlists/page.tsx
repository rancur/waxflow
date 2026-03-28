'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'
import { Playlist, Track } from '../types'
import StatusBadge from '../components/StatusBadge'

function PlaylistNode({
  playlist,
  depth = 0,
  onSelect,
  selectedId,
}: {
  playlist: Playlist
  depth?: number
  onSelect: (p: Playlist) => void
  selectedId?: string
}) {
  const [expanded, setExpanded] = useState(depth === 0)
  const hasChildren = playlist.children && playlist.children.length > 0

  return (
    <div>
      <div
        className={`flex items-center gap-2 px-3 py-2 rounded-lg cursor-pointer transition-colors ${
          selectedId === playlist.id
            ? 'bg-emerald-500/10 border border-emerald-500/20'
            : 'hover:bg-slate-800/50 border border-transparent'
        }`}
        style={{ paddingLeft: `${depth * 16 + 12}px` }}
        onClick={() => {
          onSelect(playlist)
          if (hasChildren) setExpanded(!expanded)
        }}
      >
        {hasChildren && (
          <svg
            className={`w-3.5 h-3.5 text-slate-500 transition-transform ${expanded ? 'rotate-90' : ''}`}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
          </svg>
        )}
        {!hasChildren && <div className="w-3.5" />}
        <span className="text-sm text-slate-300 flex-1">{playlist.name}</span>
        <span className="text-xs text-slate-600 tabular-nums">{playlist.track_count}</span>
        {playlist.synced && (
          <span className="text-emerald-400 text-xs">&#10003;</span>
        )}
      </div>
      {expanded && hasChildren && (
        <div>
          {playlist.children!.map((child) => (
            <PlaylistNode
              key={child.id}
              playlist={child}
              depth={depth + 1}
              onSelect={onSelect}
              selectedId={selectedId}
            />
          ))}
        </div>
      )}
    </div>
  )
}

export default function PlaylistsPage() {
  const [playlists, setPlaylists] = useState<Playlist[]>([])
  const [selected, setSelected] = useState<Playlist | null>(null)
  const [tracks, setTracks] = useState<Track[]>([])
  const [loading, setLoading] = useState(true)
  const [tracksLoading, setTracksLoading] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fetchPlaylists = useCallback(async () => {
    try {
      const result = await apiFetch<Playlist[]>('/playlists')
      setPlaylists(result)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch playlists')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchPlaylists()
  }, [fetchPlaylists])

  const handleSelect = async (playlist: Playlist) => {
    setSelected(playlist)
    if (!playlist.children || playlist.children.length === 0) {
      setTracksLoading(true)
      try {
        const result = await apiFetch<{ tracks: Track[] }>(`/playlists/${playlist.id}/tracks`)
        setTracks(result.tracks)
      } catch {
        setTracks([])
      } finally {
        setTracksLoading(false)
      }
    }
  }

  const handleSync = async () => {
    if (!selected) return
    setSyncing(true)
    try {
      await apiFetch(`/playlists/${selected.id}/sync`, { method: 'POST' })
      fetchPlaylists()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Sync failed')
    } finally {
      setSyncing(false)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Playlists</h1>
        <p className="text-sm text-slate-500 mt-1">Lexicon playlist structure</p>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4 flex items-center justify-between">
          <p className="text-sm text-red-400">{error}</p>
          <button onClick={fetchPlaylists} className="btn-secondary text-xs">Retry</button>
        </div>
      )}

      <div className="grid lg:grid-cols-3 gap-6">
        {/* Tree */}
        <div className="card p-2 lg:col-span-1 max-h-[70vh] overflow-y-auto">
          {loading ? (
            <div className="space-y-2 p-3">
              {Array.from({ length: 8 }).map((_, i) => (
                <div key={i} className="h-8 skeleton rounded" />
              ))}
            </div>
          ) : playlists.length === 0 ? (
            <div className="py-8 text-center text-sm text-slate-500">No playlists found</div>
          ) : (
            playlists.map((p) => (
              <PlaylistNode
                key={p.id}
                playlist={p}
                onSelect={handleSelect}
                selectedId={selected?.id}
              />
            ))
          )}
        </div>

        {/* Detail */}
        <div className="lg:col-span-2">
          {selected ? (
            <div className="card">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h2 className="text-lg font-semibold text-slate-200">{selected.name}</h2>
                  <p className="text-xs text-slate-500">{selected.track_count} tracks</p>
                </div>
                <button
                  className="btn-primary text-sm"
                  disabled={syncing}
                  onClick={handleSync}
                >
                  {syncing ? 'Syncing...' : 'Sync to Lexicon'}
                </button>
              </div>

              {tracksLoading ? (
                <div className="space-y-2">
                  {Array.from({ length: 5 }).map((_, i) => (
                    <div key={i} className="h-10 skeleton rounded" />
                  ))}
                </div>
              ) : tracks.length > 0 ? (
                <div className="divide-y divide-slate-800/50 max-h-96 overflow-y-auto">
                  {tracks.map((track, idx) => (
                    <div key={track.id} className="flex items-center gap-3 py-2.5">
                      <span className="text-xs text-slate-600 w-6 text-right tabular-nums">
                        {idx + 1}
                      </span>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm text-slate-300 truncate">{track.title}</p>
                        <p className="text-xs text-slate-500 truncate">{track.artist}</p>
                      </div>
                      <StatusBadge status={track.pipeline_stage} />
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-slate-500 text-center py-8">
                  Select a leaf playlist to view tracks
                </p>
              )}
            </div>
          ) : (
            <div className="card text-center py-16">
              <div className="text-3xl mb-3 text-slate-700">&#9835;</div>
              <p className="text-slate-500">Select a playlist to view details</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
