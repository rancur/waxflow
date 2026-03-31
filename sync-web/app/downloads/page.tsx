'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'
import StatusBadge from '../components/StatusBadge'

const STATS_POLL = 3_000
const TABLE_POLL = 10_000
const PER_PAGE = 50

interface DownloadItem {
  id: number
  track_id: number
  source: string
  status: string
  attempts: number
  max_attempts: number
  error: string | null
  created_at: string | null
  started_at: string | null
  completed_at: string | null
  track_title: string
  track_artist: string
  track_album: string
  file_path: string | null
  codec: string | null
  sample_rate: number | null
  bit_depth: number | null
}

interface RecentItem {
  id: number
  track_id: number
  track_title: string
  track_artist: string
  track_album: string
  file_path: string | null
  codec: string | null
  sample_rate: number | null
  bit_depth: number | null
  completed_at: string
}

interface ActiveDownload {
  id: number
  track_id: number
  source: string
  status: string
  title: string | null
  artist: string | null
  album: string | null
  tidal_id: string | null
  started_at: string | null
  attempts: number
}

interface DownloadStats {
  total: number
  pending: number
  queued: number
  downloading: number
  complete: number
  failed: number
  skipped: number
  avg_download_time_seconds: number | null
  estimated_remaining_seconds: number | null
  method: string
  tiddl_available: boolean
  tidarr_reachable: boolean
}

interface TidalStatus {
  connected: boolean
  user_id?: string
  country?: string
  expires_at?: number
  expired?: boolean
  hours_left?: number
  source?: string
}

function formatDuration(seconds: number | null | undefined): string {
  if (!seconds) return '--'
  if (seconds < 60) return `${Math.round(seconds)}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  return `${h}h ${m}m`
}

function formatTime(iso: string | null): string {
  if (!iso) return '--'
  try {
    return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  } catch {
    return '--'
  }
}

function formatDate(iso: string | null): string {
  if (!iso) return '--'
  try {
    return new Date(iso).toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  } catch {
    return '--'
  }
}

function sourceLabel(source: string): string {
  const labels: Record<string, string> = {
    tiddl: 'Tidal (FLAC)',
    tidarr: 'Tidal (FLAC)',
    beatport: 'Beatport',
    bandcamp: 'Bandcamp',
  }
  return labels[source] || source
}

function codecLabel(codec: string | null, sampleRate: number | null, bitDepth: number | null): string {
  if (!codec) return '--'
  const parts = [codec.toUpperCase()]
  if (bitDepth) parts.push(`${bitDepth}-bit`)
  if (sampleRate) parts.push(`${(sampleRate / 1000).toFixed(1)}kHz`)
  return parts.join(' / ')
}

function fileName(path: string | null): string {
  if (!path) return '--'
  const parts = path.split('/')
  return parts[parts.length - 1] || path
}

export default function DownloadsPage() {
  const [items, setItems] = useState<DownloadItem[]>([])
  const [totalItems, setTotalItems] = useState(0)
  const [totalPages, setTotalPages] = useState(1)
  const [stats, setStats] = useState<DownloadStats | null>(null)
  const [activeDownloads, setActiveDownloads] = useState<ActiveDownload[]>([])
  const [tidalStatus, setTidalStatus] = useState<TidalStatus | null>(null)
  const [recent, setRecent] = useState<RecentItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const [statusFilter, setStatusFilter] = useState<string>('')
  const [sortBy, setSortBy] = useState('created_at')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [expandedRow, setExpandedRow] = useState<number | null>(null)

  const fetchStats = useCallback(async () => {
    try {
      const [s, active] = await Promise.all([
        apiFetch<DownloadStats>('/downloads/stats'),
        apiFetch<{ active: ActiveDownload[] }>('/downloads/active'),
      ])
      setStats(s)
      setActiveDownloads(active.active || [])
    } catch {}
  }, [])

  const fetchTidalStatus = useCallback(async () => {
    try {
      const s = await apiFetch<TidalStatus>('/tidal/status')
      setTidalStatus(s)
    } catch {}
  }, [])

  const fetchTable = useCallback(async () => {
    try {
      const params = new URLSearchParams({
        page: String(page),
        per_page: String(PER_PAGE),
        sort_by: sortBy,
        sort_dir: sortDir,
      })
      if (statusFilter) params.set('status', statusFilter)
      if (search) params.set('search', search)

      const [result, recentResult] = await Promise.all([
        apiFetch<any>(`/downloads?${params}`),
        apiFetch<{ items: RecentItem[] }>('/downloads/recent?limit=10'),
      ])
      setItems(result.items || [])
      setTotalItems(result.total || 0)
      setTotalPages(result.total_pages || 1)
      setRecent(recentResult.items || [])
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch downloads')
    } finally {
      setLoading(false)
    }
  }, [page, search, statusFilter, sortBy, sortDir])

  // Stats + active downloads polling (3s)
  useEffect(() => {
    fetchStats()
    const interval = setInterval(fetchStats, STATS_POLL)
    return () => clearInterval(interval)
  }, [fetchStats])

  // Tidal status polling (30s)
  useEffect(() => {
    fetchTidalStatus()
    const interval = setInterval(fetchTidalStatus, 30_000)
    return () => clearInterval(interval)
  }, [fetchTidalStatus])

  // Table polling (10s)
  useEffect(() => {
    fetchTable()
    const interval = setInterval(fetchTable, TABLE_POLL)
    return () => clearInterval(interval)
  }, [fetchTable])

  const handleRetry = async (trackId: number) => {
    try {
      await apiFetch(`/downloads/${trackId}/retry`, { method: 'POST' })
      fetchTable()
      fetchStats()
    } catch {}
  }

  const handleSort = (col: string) => {
    if (sortBy === col) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(col)
      setSortDir('desc')
    }
    setPage(1)
  }

  const handleSearch = () => {
    setSearch(searchInput)
    setPage(1)
  }

  // Progress: resolved = downloaded + library matched, out of grand total
  const totalTracks = stats ? stats.total + stats.skipped : 0
  const resolvedTracks = stats ? stats.complete + stats.skipped : 0
  const progressPct = totalTracks > 0 ? Math.round((resolvedTracks / totalTracks) * 100) : 0
  const allDownloadsDone = stats ? stats.pending === 0 && stats.queued === 0 && stats.downloading === 0 : false

  const SortIcon = ({ col }: { col: string }) => {
    if (sortBy !== col) return <span className="text-slate-600 ml-1">&#x25B4;&#x25BE;</span>
    return <span className="text-emerald-400 ml-1">{sortDir === 'asc' ? '\u25B4' : '\u25BE'}</span>
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-100">Downloads</h1>
          <p className="text-sm text-slate-500 mt-1">
            {totalItems.toLocaleString()} items total
            {stats?.avg_download_time_seconds ? ` \u00b7 avg ${stats.avg_download_time_seconds}s per track` : ''}
            {stats ? ` \u00b7 via ${stats.tiddl_available ? 'tiddl (direct)' : stats.tidarr_reachable ? 'Tidarr (legacy)' : 'no downloader'}` : ''}
          </p>
        </div>
        {tidalStatus && (
          <div className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium ${
            tidalStatus.connected && !tidalStatus.expired
              ? 'bg-emerald-500/10 border border-emerald-500/30 text-emerald-400'
              : 'bg-red-500/10 border border-red-500/30 text-red-400'
          }`}>
            <div className={`w-2 h-2 rounded-full ${
              tidalStatus.connected && !tidalStatus.expired ? 'bg-emerald-400' : 'bg-red-400'
            }`} />
            {tidalStatus.connected && !tidalStatus.expired
              ? `Tidal: Connected (${tidalStatus.hours_left}h left)`
              : tidalStatus.connected && tidalStatus.expired
                ? 'Tidal: Token expired'
                : 'Tidal: Not connected'
            }
          </div>
        )}
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {/* --- Top Section: Live Stats Bar --- */}
      <div className="space-y-3">
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          {/* Library Matched (skipped) */}
          <div className="card text-center border-violet-500/20">
            <p className="text-3xl font-bold text-violet-400 tabular-nums">
              {stats?.skipped?.toLocaleString() ?? '-'}
            </p>
            <p className="text-xs text-slate-500 font-medium mt-1">Library Matched</p>
          </div>
          {/* Queue */}
          <div className="card text-center border-amber-500/20">
            <p className="text-3xl font-bold text-amber-400 tabular-nums">
              {stats ? (stats.pending + stats.queued).toLocaleString() : '-'}
            </p>
            <p className="text-xs text-slate-500 font-medium mt-1">Queued</p>
          </div>
          {/* Downloading */}
          <div className="card text-center border-blue-500/20">
            <div className="flex items-center justify-center gap-2">
              <div className={`w-2 h-2 rounded-full ${stats?.downloading ? 'bg-blue-400 animate-pulse' : 'bg-slate-600'}`} />
              <p className="text-3xl font-bold text-blue-400 tabular-nums">
                {stats?.downloading ?? '-'}
              </p>
            </div>
            <p className="text-xs text-slate-500 font-medium mt-1">Downloading</p>
          </div>
          {/* Complete */}
          <div className="card text-center border-emerald-500/20">
            <p className="text-3xl font-bold text-emerald-400 tabular-nums">
              {stats?.complete?.toLocaleString() ?? '-'}
            </p>
            <p className="text-xs text-slate-500 font-medium mt-1">Downloaded</p>
          </div>
          {/* Failed */}
          <div className="card text-center border-red-500/20">
            <p className="text-3xl font-bold text-red-400 tabular-nums">
              {stats?.failed?.toLocaleString() ?? '-'}
            </p>
            <p className="text-xs text-slate-500 font-medium mt-1">Failed</p>
          </div>
        </div>

        {/* Progress bar */}
        {stats && stats.total > 0 && (
          <div className="card !py-3">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs text-slate-400 font-medium">Download Progress</span>
              <span className="text-xs text-slate-400 tabular-nums">
                {allDownloadsDone && stats.failed === 0 ? (
                  <span className="text-emerald-400">All downloads complete</span>
                ) : (
                  <>{stats.complete.toLocaleString()} downloaded + {stats.skipped.toLocaleString()} library matched = {resolvedTracks.toLocaleString()} / {totalTracks.toLocaleString()} ({progressPct}%)</>
                )}
              </span>
            </div>
            <div className="w-full h-2 bg-slate-800 rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all duration-500 ${
                  allDownloadsDone && stats.failed === 0
                    ? 'bg-gradient-to-r from-emerald-500 to-emerald-400'
                    : 'bg-gradient-to-r from-blue-500 to-emerald-400'
                }`}
                style={{ width: `${progressPct}%` }}
              />
            </div>
            {stats.estimated_remaining_seconds ? (
              <p className="text-xs text-slate-500 mt-2 text-right">
                Est. remaining: <span className="text-slate-300 tabular-nums">{formatDuration(stats.estimated_remaining_seconds)}</span>
              </p>
            ) : allDownloadsDone && stats.failed === 0 ? null : stats.downloading === 0 && (stats.pending + stats.queued) > 0 ? (
              <p className="text-xs text-slate-500 mt-2 text-right">
                All downloads paused &mdash; {(stats.pending + stats.queued).toLocaleString()} tracks in queue
              </p>
            ) : null}
          </div>
        )}
      </div>

      {/* --- Middle Section: Currently Downloading --- */}
      <div className={`card border ${activeDownloads.length > 0 ? 'border-blue-500/30 bg-blue-500/5' : 'border-slate-800'}`}>
        <div className="flex items-center gap-3 mb-3">
          <div className={`w-2.5 h-2.5 rounded-full ${activeDownloads.length > 0 ? 'bg-blue-400 animate-pulse' : 'bg-slate-600'}`} />
          <h2 className="text-sm font-semibold text-blue-400">Currently Downloading</h2>
        </div>
        {activeDownloads.length > 0 ? (
          <div className="space-y-3">
            {activeDownloads.map(dl => (
              <div key={dl.id} className="flex items-start gap-4">
                <div className="flex-1 min-w-0">
                  <p className="text-lg text-slate-100 font-semibold truncate">{dl.title || 'Unknown Track'}</p>
                  <p className="text-sm text-slate-400 truncate">{dl.artist || 'Unknown Artist'}</p>
                  <p className="text-xs text-slate-500 mt-1">{dl.album}</p>
                  <div className="flex items-center gap-4 mt-3">
                    <span className="text-xs text-slate-500">
                      Method: <span className="text-slate-300">{sourceLabel(dl.source)}</span>
                    </span>
                    {dl.started_at && (
                      <span className="text-xs text-slate-500">
                        Started: <span className="text-slate-300 tabular-nums">{formatTime(dl.started_at)}</span>
                      </span>
                    )}
                  </div>
                </div>
                <div className="shrink-0 flex items-center gap-2">
                  <svg className="animate-spin h-5 w-5 text-blue-400" viewBox="0 0 24 24" fill="none">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-slate-500 italic">Idle &mdash; no active downloads</p>
        )}
      </div>

      {/* --- Main Section: Download Queue Table --- */}
      <div className="card p-0 overflow-hidden">
        <div className="px-4 py-3 border-b border-slate-800 flex flex-col sm:flex-row items-start sm:items-center gap-3">
          <h2 className="text-sm font-semibold text-slate-200">Download Queue</h2>
          <div className="flex flex-1 items-center gap-2 w-full sm:w-auto">
            <input
              className="input-field text-xs !py-1.5 flex-1 sm:max-w-xs"
              placeholder="Search title, artist, album..."
              value={searchInput}
              onChange={e => setSearchInput(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSearch()}
            />
            <button className="btn-secondary text-xs !py-1.5 !px-3" onClick={handleSearch}>
              Search
            </button>
            <select
              className="select-field text-xs !py-1.5"
              value={statusFilter}
              onChange={e => { setStatusFilter(e.target.value); setPage(1) }}
            >
              <option value="">All Status</option>
              <option value="pending">Pending</option>
              <option value="queued">Queued</option>
              <option value="downloading">Downloading</option>
              <option value="complete">Complete</option>
              <option value="failed">Failed</option>
            </select>
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-800">
                <th className="table-header cursor-pointer" onClick={() => handleSort('title')}>
                  Title <SortIcon col="title" />
                </th>
                <th className="table-header cursor-pointer" onClick={() => handleSort('artist')}>
                  Artist <SortIcon col="artist" />
                </th>
                <th className="table-header cursor-pointer" onClick={() => handleSort('status')}>
                  Status <SortIcon col="status" />
                </th>
                <th className="table-header">Source</th>
                <th className="table-header cursor-pointer" onClick={() => handleSort('attempts')}>
                  Attempts <SortIcon col="attempts" />
                </th>
                <th className="table-header cursor-pointer" onClick={() => handleSort('started_at')}>
                  Started <SortIcon col="started_at" />
                </th>
                <th className="table-header cursor-pointer" onClick={() => handleSort('completed_at')}>
                  Completed <SortIcon col="completed_at" />
                </th>
                <th className="table-header">Actions</th>
              </tr>
            </thead>
            <tbody>
              {items.map(item => (
                <>
                  <tr
                    key={item.id}
                    className={`border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors ${
                      item.status === 'downloading' ? 'bg-blue-500/5' : ''
                    } ${item.status === 'failed' ? 'bg-red-500/5' : ''}`}
                    onClick={() => item.error ? setExpandedRow(expandedRow === item.id ? null : item.id) : null}
                  >
                    <td className="table-cell font-medium text-slate-200 max-w-[200px] truncate">
                      {item.track_title || '--'}
                    </td>
                    <td className="table-cell text-slate-400 max-w-[160px] truncate">
                      {item.track_artist || '--'}
                    </td>
                    <td className="table-cell">
                      <StatusBadge status={item.status} />
                    </td>
                    <td className="table-cell text-slate-500 text-xs">
                      {sourceLabel(item.source)}
                    </td>
                    <td className="table-cell text-slate-400 tabular-nums text-center">
                      {item.attempts}/{item.max_attempts}
                    </td>
                    <td className="table-cell text-slate-500 tabular-nums text-xs">
                      {formatDate(item.started_at)}
                    </td>
                    <td className="table-cell text-slate-500 tabular-nums text-xs">
                      {formatDate(item.completed_at)}
                    </td>
                    <td className="table-cell">
                      {item.status === 'failed' && (
                        <button
                          className="text-xs text-amber-400 hover:text-amber-300 px-2 py-1 border border-amber-500/30 rounded hover:bg-amber-500/10 transition-colors"
                          onClick={e => { e.stopPropagation(); handleRetry(item.track_id) }}
                        >
                          Retry
                        </button>
                      )}
                    </td>
                  </tr>
                  {expandedRow === item.id && item.error && (
                    <tr key={`${item.id}-error`} className="bg-red-500/5">
                      <td colSpan={8} className="px-4 py-2">
                        <p className="text-xs text-red-400 font-mono break-all">{item.error}</p>
                      </td>
                    </tr>
                  )}
                </>
              ))}
              {items.length === 0 && !loading && (
                <tr>
                  <td colSpan={8} className="table-cell text-center text-slate-500 py-8">
                    No downloads found
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="px-4 py-3 border-t border-slate-800 flex items-center justify-between">
            <p className="text-xs text-slate-500">
              Page {page} of {totalPages} ({totalItems.toLocaleString()} items)
            </p>
            <div className="flex items-center gap-1">
              <button
                className="btn-secondary text-xs !py-1 !px-2 disabled:opacity-30"
                disabled={page <= 1}
                onClick={() => setPage(1)}
              >
                First
              </button>
              <button
                className="btn-secondary text-xs !py-1 !px-2 disabled:opacity-30"
                disabled={page <= 1}
                onClick={() => setPage(p => p - 1)}
              >
                Prev
              </button>
              <button
                className="btn-secondary text-xs !py-1 !px-2 disabled:opacity-30"
                disabled={page >= totalPages}
                onClick={() => setPage(p => p + 1)}
              >
                Next
              </button>
              <button
                className="btn-secondary text-xs !py-1 !px-2 disabled:opacity-30"
                disabled={page >= totalPages}
                onClick={() => setPage(totalPages)}
              >
                Last
              </button>
            </div>
          </div>
        )}
      </div>

      {/* --- Bottom Section: Recently Completed --- */}
      {recent.length > 0 && (
        <div className="card p-0 overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800">
            <h2 className="text-sm font-semibold text-emerald-400">Recently Completed</h2>
          </div>
          <div className="divide-y divide-slate-800/50">
            {recent.map(item => (
              <div key={item.id} className="flex items-center gap-4 px-4 py-2.5">
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-slate-200 font-medium truncate">{item.track_title}</p>
                  <p className="text-xs text-slate-500 truncate">{item.track_artist} &mdash; {item.track_album}</p>
                </div>
                <div className="shrink-0 text-right">
                  <p className="text-xs text-slate-400 tabular-nums">
                    {codecLabel(item.codec, item.sample_rate, item.bit_depth)}
                  </p>
                  <p className="text-xs text-slate-600 truncate max-w-[200px]" title={item.file_path || ''}>
                    {fileName(item.file_path)}
                  </p>
                </div>
                <span className="text-xs text-slate-500 tabular-nums shrink-0">
                  {formatDate(item.completed_at)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Loading skeleton */}
      {loading && items.length === 0 && (
        <div className="card">
          <div className="space-y-3">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="h-12 skeleton rounded" />
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
