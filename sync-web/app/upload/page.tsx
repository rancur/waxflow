'use client'

import { useState, useCallback, useEffect, useRef } from 'react'
import { apiFetch, apiUpload } from '../api'
import StatusBadge from '../components/StatusBadge'

interface MissingTrack {
  id: number
  spotify_id: string
  title: string
  artist: string
  album: string
  pipeline_stage: string
  match_status: string
  pipeline_error: string | null
  spotify_added_at: string | null
}

type SortKey = 'title' | 'artist' | 'pipeline_error' | 'spotify_added_at'
type SortDir = 'asc' | 'desc'

export default function MissingTracksPage() {
  const [tracks, setTracks] = useState<MissingTrack[]>([])
  const [loading, setLoading] = useState(true)
  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('spotify_added_at')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [retrying, setRetrying] = useState<Set<number>>(new Set())
  const [uploading, setUploading] = useState<Set<number>>(new Set())
  const [toast, setToast] = useState<string | null>(null)
  const fileInputRefs = useRef<Record<number, HTMLInputElement | null>>({})

  const fetchTracks = useCallback(async () => {
    setLoading(true)
    try {
      const result = await apiFetch<any>('/tracks?pipeline_stage=error&per_page=500')
      setTracks(result.tracks || [])
    } catch {
      setTracks([])
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchTracks() }, [fetchTracks])

  // Auto-dismiss toast
  useEffect(() => {
    if (!toast) return
    const t = setTimeout(() => setToast(null), 3000)
    return () => clearTimeout(t)
  }, [toast])

  const filtered = tracks.filter(t => {
    if (!search) return true
    const q = search.toLowerCase()
    return t.title?.toLowerCase().includes(q) || t.artist?.toLowerCase().includes(q)
  })

  const sorted = [...filtered].sort((a, b) => {
    let aVal = a[sortKey] ?? ''
    let bVal = b[sortKey] ?? ''
    if (sortKey === 'spotify_added_at') {
      aVal = aVal || '0'
      bVal = bVal || '0'
    }
    const cmp = String(aVal).localeCompare(String(bVal), undefined, { sensitivity: 'base' })
    return sortDir === 'asc' ? cmp : -cmp
  })

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  const sortIndicator = (key: SortKey) => {
    if (sortKey !== key) return null
    return sortDir === 'asc' ? ' \u25B2' : ' \u25BC'
  }

  const handleRetry = async (id: number) => {
    setRetrying(prev => new Set(prev).add(id))
    try {
      await apiFetch(`/tracks/${id}/retry`, { method: 'POST' })
      setToast('Track queued for retry')
      fetchTracks()
    } catch {
      setToast('Retry failed')
    } finally {
      setRetrying(prev => {
        const next = new Set(prev)
        next.delete(id)
        return next
      })
    }
  }

  const handleUpload = async (id: number, file: File) => {
    setUploading(prev => new Set(prev).add(id))
    try {
      const formData = new FormData()
      formData.append('file', file)
      await apiUpload(`/uploads/${id}`, formData)
      setToast('Upload complete')
      fetchTracks()
    } catch {
      setToast('Upload failed')
    } finally {
      setUploading(prev => {
        const next = new Set(prev)
        next.delete(id)
        return next
      })
    }
  }

  const searchUrl = (base: string, artist: string, title: string) => {
    const q = encodeURIComponent(`${artist} ${title}`)
    return base.replace('{q}', q)
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100">Missing Tracks</h1>
          <p className="text-sm text-slate-500 mt-1">
            {filtered.length} track{filtered.length !== 1 ? 's' : ''} with errors
          </p>
        </div>
        <input
          type="text"
          placeholder="Search by title or artist..."
          className="input-field w-72"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
      </div>

      {toast && (
        <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-xl px-5 py-3">
          <p className="text-sm text-emerald-400">{toast}</p>
        </div>
      )}

      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm text-left">
            <thead>
              <tr className="border-b border-slate-800 text-xs uppercase tracking-wider text-slate-500">
                <th className="px-4 py-3 cursor-pointer hover:text-slate-300" onClick={() => toggleSort('title')}>
                  Title{sortIndicator('title')}
                </th>
                <th className="px-4 py-3 cursor-pointer hover:text-slate-300" onClick={() => toggleSort('artist')}>
                  Artist{sortIndicator('artist')}
                </th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3 cursor-pointer hover:text-slate-300" onClick={() => toggleSort('pipeline_error')}>
                  Error Reason{sortIndicator('pipeline_error')}
                </th>
                <th className="px-4 py-3 cursor-pointer hover:text-slate-300" onClick={() => toggleSort('spotify_added_at')}>
                  Added{sortIndicator('spotify_added_at')}
                </th>
                <th className="px-4 py-3 text-right">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {loading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i}>
                    <td colSpan={6} className="px-4 py-3"><div className="h-8 skeleton rounded" /></td>
                  </tr>
                ))
              ) : sorted.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-4 py-12 text-center text-slate-500">
                    {search ? 'No matching tracks' : 'No missing tracks'}
                  </td>
                </tr>
              ) : (
                sorted.map(track => (
                  <tr key={track.id} className="hover:bg-slate-800/40 transition-colors">
                    <td className="px-4 py-3 text-slate-200 font-medium max-w-[200px] truncate">
                      {track.title}
                    </td>
                    <td className="px-4 py-3 text-slate-400 max-w-[160px] truncate">
                      {track.artist}
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={track.match_status} />
                    </td>
                    <td className="px-4 py-3 text-red-400/80 text-xs max-w-[220px] truncate" title={track.pipeline_error || ''}>
                      {track.pipeline_error || '\u2014'}
                    </td>
                    <td className="px-4 py-3 text-slate-500 text-xs whitespace-nowrap">
                      {track.spotify_added_at
                        ? new Date(track.spotify_added_at).toLocaleDateString()
                        : '\u2014'}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center justify-end gap-2">
                        {/* Upload */}
                        <input
                          ref={el => { fileInputRefs.current[track.id] = el }}
                          type="file"
                          accept=".flac,.aiff,.wav,.m4a"
                          className="hidden"
                          onChange={e => {
                            const f = e.target.files?.[0]
                            if (f) handleUpload(track.id, f)
                            e.target.value = ''
                          }}
                        />
                        <button
                          className="btn-primary text-xs px-2.5 py-1"
                          disabled={uploading.has(track.id)}
                          onClick={() => fileInputRefs.current[track.id]?.click()}
                          title="Upload file"
                        >
                          {uploading.has(track.id) ? 'Uploading...' : 'Upload'}
                        </button>

                        {/* Retry */}
                        <button
                          className="text-xs px-2.5 py-1 rounded-lg border border-amber-500/30 bg-amber-500/10 text-amber-400 hover:bg-amber-500/20 transition-colors disabled:opacity-50"
                          disabled={retrying.has(track.id)}
                          onClick={() => handleRetry(track.id)}
                          title="Retry matching"
                        >
                          {retrying.has(track.id) ? 'Retrying...' : 'Retry'}
                        </button>

                        {/* External search links */}
                        <a
                          href={searchUrl('https://www.beatport.com/search?q={q}', track.artist, track.title)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-xs px-2 py-1 rounded-lg border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-600 transition-colors"
                          title="Search Beatport"
                        >
                          Beatport
                        </a>
                        <a
                          href={searchUrl('https://bandcamp.com/search?q={q}', track.artist, track.title)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-xs px-2 py-1 rounded-lg border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-600 transition-colors"
                          title="Search Bandcamp"
                        >
                          Bandcamp
                        </a>
                        <a
                          href={searchUrl('https://soundcloud.com/search/sounds?q={q}', track.artist, track.title)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-xs px-2 py-1 rounded-lg border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-600 transition-colors"
                          title="Search SoundCloud"
                        >
                          SoundCloud
                        </a>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
