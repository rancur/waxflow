'use client'

import { useState, useCallback, useEffect } from 'react'
import { apiFetch } from '../api'

interface ErrorTrack {
  id: number
  spotify_id: string
  title: string
  artist: string
  album: string
  pipeline_stage: string
  pipeline_error: string | null
  spotify_added_at: string | null
  verify_codec: string | null
  file_path: string | null
  download_attempts: number
}

interface CategoryInfo {
  key: string
  label: string
  color: string
  borderColor: string
  bgColor: string
  textColor: string
  description: string
}

const CATEGORIES: CategoryInfo[] = [
  {
    key: 'not_lossless',
    label: 'Not Lossless',
    color: 'red',
    borderColor: 'border-red-500/30',
    bgColor: 'bg-red-500/10',
    textColor: 'text-red-400',
    description: 'Tracks where verify found AAC/MP3 codec instead of lossless',
  },
  {
    key: 'no_tidal_match',
    label: 'No Tidal Match',
    color: 'amber',
    borderColor: 'border-amber-500/30',
    bgColor: 'bg-amber-500/10',
    textColor: 'text-amber-400',
    description: 'Tracks not found on Tidal',
  },
  {
    key: 'download_failed',
    label: 'Download Failed',
    color: 'blue',
    borderColor: 'border-blue-500/30',
    bgColor: 'bg-blue-500/10',
    textColor: 'text-blue-400',
    description: 'Tracks that failed to download after max retries',
  },
  {
    key: 'lexicon_sync_failed',
    label: 'Lexicon Sync Failed',
    color: 'purple',
    borderColor: 'border-purple-500/30',
    bgColor: 'bg-purple-500/10',
    textColor: 'text-purple-400',
    description: 'Tracks with files that could not be added to Lexicon',
  },
  {
    key: 'fingerprint_mismatch',
    label: 'Fingerprint Mismatch',
    color: 'orange',
    borderColor: 'border-orange-500/30',
    bgColor: 'bg-orange-500/10',
    textColor: 'text-orange-400',
    description: 'Tracks where the downloaded file might be the wrong version',
  },
  {
    key: 'other',
    label: 'Other Errors',
    color: 'slate',
    borderColor: 'border-slate-500/30',
    bgColor: 'bg-slate-500/10',
    textColor: 'text-slate-400',
    description: 'Uncategorized errors',
  },
]

interface ErrorsResponse {
  categories: Record<string, ErrorTrack[]>
  ignored: ErrorTrack[]
  total_errors: number
  total_ignored: number
}

export default function ErrorsPage() {
  const [data, setData] = useState<ErrorsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [search, setSearch] = useState('')
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set())
  const [ignoredExpanded, setIgnoredExpanded] = useState(false)
  const [selectedTracks, setSelectedTracks] = useState<Set<number>>(new Set())
  const [actionLoading, setActionLoading] = useState<Set<number>>(new Set())
  const [bulkLoading, setBulkLoading] = useState<string | null>(null)
  const [toast, setToast] = useState<string | null>(null)

  const fetchData = useCallback(async () => {
    try {
      const result = await apiFetch<ErrorsResponse>('/tracks/errors')
      setData(result)
      // Auto-expand sections that have tracks
      const nonEmpty = new Set<string>()
      for (const cat of CATEGORIES) {
        if ((result.categories[cat.key]?.length ?? 0) > 0) {
          nonEmpty.add(cat.key)
        }
      }
      setExpandedSections(prev => prev.size === 0 ? nonEmpty : prev)
    } catch {
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchData() }, [fetchData])

  useEffect(() => {
    if (!toast) return
    const t = setTimeout(() => setToast(null), 3000)
    return () => clearTimeout(t)
  }, [toast])

  const toggleSection = (key: string) => {
    setExpandedSections(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const filterTracks = (tracks: ErrorTrack[]) => {
    if (!search) return tracks
    const q = search.toLowerCase()
    return tracks.filter(t =>
      t.title?.toLowerCase().includes(q) ||
      t.artist?.toLowerCase().includes(q) ||
      (t.pipeline_error || '').toLowerCase().includes(q)
    )
  }

  const handleIgnore = async (trackId: number) => {
    setActionLoading(prev => new Set(prev).add(trackId))
    try {
      await apiFetch(`/tracks/${trackId}/ignore`, { method: 'POST' })
      setToast('Track ignored')
      fetchData()
    } catch {
      setToast('Failed to ignore track')
    } finally {
      setActionLoading(prev => {
        const next = new Set(prev)
        next.delete(trackId)
        return next
      })
    }
  }

  const handleUnignore = async (trackId: number) => {
    setActionLoading(prev => new Set(prev).add(trackId))
    try {
      await apiFetch(`/tracks/${trackId}/unignore`, { method: 'POST' })
      setToast('Track un-ignored')
      fetchData()
    } catch {
      setToast('Failed to un-ignore track')
    } finally {
      setActionLoading(prev => {
        const next = new Set(prev)
        next.delete(trackId)
        return next
      })
    }
  }

  const handleRetry = async (trackId: number) => {
    setActionLoading(prev => new Set(prev).add(trackId))
    try {
      await apiFetch(`/tracks/${trackId}/retry`, { method: 'POST' })
      setToast('Track queued for retry')
      fetchData()
    } catch {
      setToast('Retry failed')
    } finally {
      setActionLoading(prev => {
        const next = new Set(prev)
        next.delete(trackId)
        return next
      })
    }
  }

  const handleBulkIgnore = async (categoryKey: string) => {
    const tracks = filterTracks(data?.categories[categoryKey] || [])
    const selected = tracks.filter(t => selectedTracks.has(t.id))
    const toIgnore = selected.length > 0 ? selected : tracks
    if (toIgnore.length === 0) return

    setBulkLoading(categoryKey)
    try {
      await apiFetch('/tracks/bulk-ignore', {
        method: 'POST',
        body: JSON.stringify(toIgnore.map(t => t.id)),
      })
      setToast(`${toIgnore.length} track${toIgnore.length !== 1 ? 's' : ''} ignored`)
      setSelectedTracks(new Set())
      fetchData()
    } catch {
      setToast('Bulk ignore failed')
    } finally {
      setBulkLoading(null)
    }
  }

  const toggleTrackSelection = (trackId: number) => {
    setSelectedTracks(prev => {
      const next = new Set(prev)
      if (next.has(trackId)) next.delete(trackId)
      else next.add(trackId)
      return next
    })
  }

  const toggleAllInCategory = (categoryKey: string) => {
    const tracks = filterTracks(data?.categories[categoryKey] || [])
    const allSelected = tracks.every(t => selectedTracks.has(t.id))
    setSelectedTracks(prev => {
      const next = new Set(prev)
      if (allSelected) {
        tracks.forEach(t => next.delete(t.id))
      } else {
        tracks.forEach(t => next.add(t.id))
      }
      return next
    })
  }

  const searchUrl = (base: string, artist: string, title: string) => {
    const q = encodeURIComponent(`${artist} ${title}`)
    return base.replace('{q}', q)
  }

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return '\u2014'
    return new Date(dateStr).toLocaleDateString()
  }

  const renderTrackRow = (track: ErrorTrack, options: { showIgnore?: boolean; showUnignore?: boolean; showSearchLinks?: boolean }) => {
    const isLoading = actionLoading.has(track.id)
    return (
      <tr key={track.id} className="hover:bg-slate-800/40 transition-colors">
        {options.showIgnore && (
          <td className="px-3 py-3">
            <input
              type="checkbox"
              checked={selectedTracks.has(track.id)}
              onChange={() => toggleTrackSelection(track.id)}
              className="rounded border-slate-600 bg-slate-800 text-emerald-500 focus:ring-emerald-500/50"
            />
          </td>
        )}
        <td className="px-4 py-3 text-slate-200 font-medium max-w-[200px] truncate" title={track.title}>
          {track.title}
        </td>
        <td className="px-4 py-3 text-slate-400 max-w-[160px] truncate" title={track.artist}>
          {track.artist}
        </td>
        <td className="px-4 py-3 text-red-400/80 text-xs max-w-[240px] truncate" title={track.pipeline_error || ''}>
          {track.pipeline_error || '\u2014'}
          {track.verify_codec && (
            <span className="ml-2 text-slate-500">({track.verify_codec})</span>
          )}
        </td>
        <td className="px-4 py-3 text-slate-500 text-xs whitespace-nowrap">
          {formatDate(track.spotify_added_at)}
        </td>
        <td className="px-4 py-3">
          <div className="flex items-center justify-end gap-1.5">
            {options.showIgnore && (
              <>
                <button
                  className="text-xs px-2.5 py-1 rounded-lg border border-amber-500/30 bg-amber-500/10 text-amber-400 hover:bg-amber-500/20 transition-colors disabled:opacity-50"
                  disabled={isLoading}
                  onClick={() => handleRetry(track.id)}
                >
                  {isLoading ? '...' : 'Retry'}
                </button>
                <button
                  className="text-xs px-2.5 py-1 rounded-lg border border-slate-600 bg-slate-700/50 text-slate-300 hover:bg-slate-600/50 transition-colors disabled:opacity-50"
                  disabled={isLoading}
                  onClick={() => handleIgnore(track.id)}
                >
                  {isLoading ? '...' : 'Ignore'}
                </button>
              </>
            )}
            {options.showUnignore && (
              <button
                className="text-xs px-2.5 py-1 rounded-lg border border-emerald-500/30 bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20 transition-colors disabled:opacity-50"
                disabled={isLoading}
                onClick={() => handleUnignore(track.id)}
              >
                {isLoading ? '...' : 'Un-ignore'}
              </button>
            )}
            {options.showSearchLinks && (
              <>
                <a
                  href={searchUrl('https://www.beatport.com/search?q={q}', track.artist, track.title)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs px-2 py-1 rounded-lg border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-600 transition-colors"
                >
                  BP
                </a>
                <a
                  href={searchUrl('https://bandcamp.com/search?q={q}', track.artist, track.title)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs px-2 py-1 rounded-lg border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-600 transition-colors"
                >
                  BC
                </a>
                <a
                  href={searchUrl('https://soundcloud.com/search/sounds?q={q}', track.artist, track.title)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-xs px-2 py-1 rounded-lg border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-600 transition-colors"
                >
                  SC
                </a>
              </>
            )}
          </div>
        </td>
      </tr>
    )
  }

  const renderCategory = (cat: CategoryInfo) => {
    const tracks = filterTracks(data?.categories[cat.key] || [])
    const count = tracks.length
    if (count === 0 && search) return null
    const isExpanded = expandedSections.has(cat.key)
    const allSelected = count > 0 && tracks.every(t => selectedTracks.has(t.id))
    const someSelected = tracks.some(t => selectedTracks.has(t.id))
    const showSearchLinks = cat.key === 'no_tidal_match'

    return (
      <div key={cat.key} className={`rounded-xl border ${cat.borderColor} overflow-hidden`}>
        {/* Header */}
        <button
          onClick={() => toggleSection(cat.key)}
          className={`w-full flex items-center justify-between px-5 py-4 ${cat.bgColor} hover:brightness-110 transition-all`}
        >
          <div className="flex items-center gap-3">
            <svg
              className={`w-4 h-4 ${cat.textColor} transition-transform ${isExpanded ? 'rotate-90' : ''}`}
              fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
            >
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
            </svg>
            <h3 className={`text-sm font-semibold ${cat.textColor}`}>{cat.label}</h3>
            <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border ${cat.borderColor} ${cat.bgColor} ${cat.textColor}`}>
              {count}
            </span>
          </div>
          <p className="text-xs text-slate-500 hidden sm:block">{cat.description}</p>
        </button>

        {/* Content */}
        {isExpanded && (
          <div className="bg-slate-900/60">
            {count === 0 ? (
              <p className="px-5 py-8 text-center text-sm text-slate-500">No tracks in this category</p>
            ) : (
              <>
                {/* Bulk actions bar */}
                <div className="flex items-center gap-3 px-5 py-2 border-b border-slate-800/50">
                  <button
                    onClick={() => handleBulkIgnore(cat.key)}
                    disabled={bulkLoading === cat.key}
                    className="text-xs px-3 py-1.5 rounded-lg border border-slate-600 bg-slate-700/50 text-slate-300 hover:bg-slate-600/50 transition-colors disabled:opacity-50"
                  >
                    {bulkLoading === cat.key
                      ? 'Ignoring...'
                      : someSelected
                        ? `Ignore Selected (${tracks.filter(t => selectedTracks.has(t.id)).length})`
                        : `Ignore All ${count}`}
                  </button>
                </div>

                <div className="overflow-x-auto">
                  <table className="w-full text-sm text-left">
                    <thead>
                      <tr className="border-b border-slate-800 text-xs uppercase tracking-wider text-slate-500">
                        <th className="px-3 py-2 w-8">
                          <input
                            type="checkbox"
                            checked={allSelected}
                            ref={(el) => { if (el) el.indeterminate = someSelected && !allSelected }}
                            onChange={() => toggleAllInCategory(cat.key)}
                            className="rounded border-slate-600 bg-slate-800 text-emerald-500 focus:ring-emerald-500/50"
                          />
                        </th>
                        <th className="px-4 py-2">Title</th>
                        <th className="px-4 py-2">Artist</th>
                        <th className="px-4 py-2">Error</th>
                        <th className="px-4 py-2">Added</th>
                        <th className="px-4 py-2 text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-800/50">
                      {tracks.map(track => renderTrackRow(track, {
                        showIgnore: true,
                        showSearchLinks,
                      }))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        )}
      </div>
    )
  }

  const totalErrors = data?.total_errors ?? 0
  const totalIgnored = data?.total_ignored ?? 0
  const filteredIgnored = filterTracks(data?.ignored || [])

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100">Errored Tracks</h1>
          <p className="text-sm text-slate-500 mt-1">
            {totalErrors} error{totalErrors !== 1 ? 's' : ''} across all categories
            {totalIgnored > 0 && (
              <span className="text-slate-600"> &middot; {totalIgnored} ignored</span>
            )}
          </p>
        </div>
        <input
          type="text"
          placeholder="Search by title, artist, or error..."
          className="input-field w-80"
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
      </div>

      {/* Toast */}
      {toast && (
        <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-xl px-5 py-3">
          <p className="text-sm text-emerald-400">{toast}</p>
        </div>
      )}

      {/* Loading state */}
      {loading ? (
        <div className="space-y-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="h-16 skeleton rounded-xl" />
          ))}
        </div>
      ) : (
        <>
          {/* Category sections */}
          <div className="space-y-4">
            {CATEGORIES.map(cat => renderCategory(cat))}
          </div>

          {/* Ignored section */}
          {totalIgnored > 0 && (
            <div className="rounded-xl border border-slate-700/50 overflow-hidden mt-8">
              <button
                onClick={() => setIgnoredExpanded(!ignoredExpanded)}
                className="w-full flex items-center justify-between px-5 py-4 bg-slate-800/30 hover:bg-slate-800/50 transition-all"
              >
                <div className="flex items-center gap-3">
                  <svg
                    className={`w-4 h-4 text-slate-500 transition-transform ${ignoredExpanded ? 'rotate-90' : ''}`}
                    fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                  >
                    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                  </svg>
                  <h3 className="text-sm font-semibold text-slate-500">Ignored</h3>
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border border-slate-700 bg-slate-800/50 text-slate-500">
                    {filteredIgnored.length}
                  </span>
                </div>
                <p className="text-xs text-slate-600 hidden sm:block">Tracks you have dismissed</p>
              </button>

              {ignoredExpanded && (
                <div className="bg-slate-900/40">
                  {filteredIgnored.length === 0 ? (
                    <p className="px-5 py-8 text-center text-sm text-slate-600">
                      {search ? 'No matching ignored tracks' : 'No ignored tracks'}
                    </p>
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm text-left">
                        <thead>
                          <tr className="border-b border-slate-800 text-xs uppercase tracking-wider text-slate-500">
                            <th className="px-4 py-2">Title</th>
                            <th className="px-4 py-2">Artist</th>
                            <th className="px-4 py-2">Error</th>
                            <th className="px-4 py-2">Added</th>
                            <th className="px-4 py-2 text-right">Actions</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-800/50">
                          {filteredIgnored.map(track => renderTrackRow(track, {
                            showUnignore: true,
                          }))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  )
}
