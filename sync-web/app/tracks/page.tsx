'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'
import StatusBadge from '../components/StatusBadge'

const PER_PAGE_OPTIONS = [50, 100, 200]
const DEFAULT_PER_PAGE = 100

type SortDir = 'asc' | 'desc'
type SortKey =
  | 'title' | 'artist' | 'album' | 'duration_ms' | 'spotify_added_at'
  | 'pipeline_stage' | 'match_status' | 'download_status' | 'verify_status'
  | 'verify_codec' | 'lexicon_status' | 'match_confidence'

interface TrackData {
  id: number
  spotify_id: string
  title: string | null
  artist: string | null
  album: string | null
  duration_ms: number | null
  spotify_added_at: string | null
  pipeline_stage: string
  match_status: string
  match_source: string | null
  match_confidence: number | null
  tidal_id: string | null
  download_status: string
  download_source: string | null
  download_error: string | null
  download_attempts: number
  file_path: string | null
  verify_status: string
  verify_codec: string | null
  verify_sample_rate: number | null
  verify_bit_depth: number | null
  verify_is_genuine_lossless: boolean | null
  chromaprint: string | null
  fingerprint_match_score: number | null
  lexicon_status: string
  lexicon_track_id: string | null
  pipeline_error: string | null
  isrc: string | null
  is_protected: boolean
}

function formatDuration(ms: number): string {
  const minutes = Math.floor(ms / 60_000)
  const seconds = Math.floor((ms % 60_000) / 1000)
  return `${minutes}:${seconds.toString().padStart(2, '0')}`
}

function formatDate(iso: string | null): string {
  if (!iso) return '\u2014'
  const d = new Date(iso)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

function formatQuality(codec: string | null, sampleRate: number | null, bitDepth: number | null): string {
  if (!codec) return '\u2014'
  const parts: string[] = [codec.toUpperCase()]
  if (bitDepth) parts.push(`${bitDepth}-bit`)
  if (sampleRate) {
    const khz = sampleRate >= 1000 ? `${(sampleRate / 1000).toFixed(sampleRate % 1000 === 0 ? 0 : 1)}kHz` : `${sampleRate}Hz`
    parts.push(khz)
  }
  return parts.join(' / ')
}

function qualityColor(codec: string | null): string {
  if (!codec) return 'text-slate-500'
  const upper = codec.toUpperCase()
  if (upper === 'FLAC' || upper === 'AIFF') return 'text-emerald-400'
  if (upper === 'ALAC' || upper === 'WAV' || upper === 'APE' || upper === 'WV') return 'text-amber-400'
  return 'text-red-400'
}

function truncatePath(path: string | null, max: number = 30): string {
  if (!path) return '\u2014'
  if (path.length <= max) return path
  return '\u2026' + path.slice(path.length - max + 1)
}

function SortArrow({ active, dir }: { active: boolean; dir: SortDir }) {
  if (!active) return <span className="text-slate-700 ml-1">\u2195</span>
  return <span className="text-emerald-400 ml-1">{dir === 'asc' ? '\u2191' : '\u2193'}</span>
}

function CheckMark() {
  return <span className="text-emerald-400 text-sm">\u2713</span>
}
function XMark() {
  return <span className="text-red-400 text-sm">\u2717</span>
}
function Dash() {
  return <span className="text-slate-600">\u2014</span>
}

export default function TracksPage() {
  const [tracks, setTracks] = useState<TrackData[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [perPage, setPerPage] = useState(DEFAULT_PER_PAGE)
  const [search, setSearch] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const [stageFilter, setStageFilter] = useState('')
  const [matchFilter, setMatchFilter] = useState('')
  const [downloadFilter, setDownloadFilter] = useState('')
  const [verifyFilter, setVerifyFilter] = useState('')
  const [lexiconFilter, setLexiconFilter] = useState('')
  const [sortBy, setSortBy] = useState<SortKey>('spotify_added_at')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [expandedId, setExpandedId] = useState<number | null>(null)
  const [actionLoading, setActionLoading] = useState<number | null>(null)

  const activeFilterCount = [stageFilter, matchFilter, downloadFilter, verifyFilter, lexiconFilter].filter(Boolean).length

  const fetchTracks = useCallback(async () => {
    setLoading(true)
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        per_page: perPage.toString(),
        sort_by: sortBy,
        sort_dir: sortDir,
      })
      if (search) params.set('search', search)
      if (stageFilter) params.set('pipeline_stage', stageFilter)
      if (matchFilter) params.set('status', matchFilter)
      if (downloadFilter) params.set('download_status', downloadFilter)
      if (verifyFilter) params.set('verify_status', verifyFilter)
      if (lexiconFilter) params.set('lexicon_status', lexiconFilter)

      const result = await apiFetch<any>(`/tracks?${params}`)
      setTracks(result.tracks)
      setTotal(result.total)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch tracks')
    } finally {
      setLoading(false)
    }
  }, [page, perPage, search, stageFilter, matchFilter, downloadFilter, verifyFilter, lexiconFilter, sortBy, sortDir])

  useEffect(() => { fetchTracks() }, [fetchTracks])

  // Debounced search
  useEffect(() => {
    const timer = setTimeout(() => {
      setSearch(searchInput)
      setPage(1)
    }, 300)
    return () => clearTimeout(timer)
  }, [searchInput])

  const totalPages = Math.ceil(total / perPage)

  const handleSort = (key: SortKey) => {
    if (sortBy === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(key)
      setSortDir('asc')
    }
    setPage(1)
  }

  const handleRetry = async (trackId: number) => {
    setActionLoading(trackId)
    try {
      await apiFetch(`/tracks/${trackId}/retry`, { method: 'POST' })
      await fetchTracks()
    } catch { /* ignore */ }
    setActionLoading(null)
  }

  const handleIgnore = async (trackId: number) => {
    setActionLoading(trackId)
    try {
      await apiFetch(`/tracks/${trackId}/ignore`, { method: 'POST' })
      await fetchTracks()
    } catch { /* ignore */ }
    setActionLoading(null)
  }

  const clearFilters = () => {
    setStageFilter('')
    setMatchFilter('')
    setDownloadFilter('')
    setVerifyFilter('')
    setLexiconFilter('')
    setSearchInput('')
    setSearch('')
    setPage(1)
  }

  const startIdx = (page - 1) * perPage + 1
  const endIdx = Math.min(page * perPage, total)

  // Page number range for pagination
  const pageNumbers: number[] = []
  const maxPageButtons = 7
  if (totalPages <= maxPageButtons) {
    for (let i = 1; i <= totalPages; i++) pageNumbers.push(i)
  } else {
    pageNumbers.push(1)
    let start = Math.max(2, page - 2)
    let end = Math.min(totalPages - 1, page + 2)
    if (page <= 3) { start = 2; end = 5 }
    if (page >= totalPages - 2) { start = totalPages - 4; end = totalPages - 1 }
    if (start > 2) pageNumbers.push(-1) // ellipsis
    for (let i = start; i <= end; i++) pageNumbers.push(i)
    if (end < totalPages - 1) pageNumbers.push(-2) // ellipsis
    pageNumbers.push(totalPages)
  }

  const ThCell = ({ label, sortKey, minW }: { label: string; sortKey?: SortKey; minW?: string }) => (
    <th
      className={`table-header whitespace-nowrap select-none ${sortKey ? 'cursor-pointer hover:text-slate-200' : ''}`}
      style={minW ? { minWidth: minW } : undefined}
      onClick={sortKey ? () => handleSort(sortKey) : undefined}
    >
      {label}
      {sortKey && <SortArrow active={sortBy === sortKey} dir={sortDir} />}
    </th>
  )

  return (
    <div className="flex flex-col h-[calc(100vh-48px)] lg:h-screen">
      {/* Top bar: title + filters */}
      <div className="flex-none px-4 py-3 border-b border-slate-800 bg-slate-900/80 backdrop-blur-sm">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-4">
            <h1 className="text-lg font-bold text-slate-100">Track Library</h1>
            <span className="text-xs text-slate-500">{total.toLocaleString()} tracks</span>
          </div>
          <div className="flex items-center gap-2">
            <select
              className="select-field text-xs py-1 px-2"
              value={perPage}
              onChange={(e) => { setPerPage(Number(e.target.value)); setPage(1) }}
            >
              {PER_PAGE_OPTIONS.map(n => (
                <option key={n} value={n}>{n} per page</option>
              ))}
            </select>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <div className="flex-1 min-w-[200px] max-w-sm">
            <input
              type="text"
              placeholder="Search title, artist, album..."
              className="input-field text-sm py-1.5"
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
            />
          </div>
          <select className="select-field text-xs py-1.5" value={stageFilter} onChange={(e) => { setStageFilter(e.target.value); setPage(1) }}>
            <option value="">All Stages</option>
            <option value="new">New</option>
            <option value="matching">Matching</option>
            <option value="downloading">Downloading</option>
            <option value="verifying">Verifying</option>
            <option value="organizing">Organizing</option>
            <option value="complete">Complete</option>
            <option value="error">Error</option>
            <option value="ignored">Ignored</option>
          </select>
          <select className="select-field text-xs py-1.5" value={matchFilter} onChange={(e) => { setMatchFilter(e.target.value); setPage(1) }}>
            <option value="">All Match</option>
            <option value="pending">Pending</option>
            <option value="matched">Matched</option>
            <option value="mismatched">Mismatched</option>
            <option value="failed">Failed</option>
            <option value="manual">Manual</option>
          </select>
          <select className="select-field text-xs py-1.5" value={downloadFilter} onChange={(e) => { setDownloadFilter(e.target.value); setPage(1) }}>
            <option value="">All Download</option>
            <option value="pending">Pending</option>
            <option value="queued">Queued</option>
            <option value="downloading">Downloading</option>
            <option value="complete">Complete</option>
            <option value="failed">Failed</option>
          </select>
          <select className="select-field text-xs py-1.5" value={verifyFilter} onChange={(e) => { setVerifyFilter(e.target.value); setPage(1) }}>
            <option value="">All Verify</option>
            <option value="pending">Pending</option>
            <option value="pass">Pass</option>
            <option value="fail">Fail</option>
          </select>
          <select className="select-field text-xs py-1.5" value={lexiconFilter} onChange={(e) => { setLexiconFilter(e.target.value); setPage(1) }}>
            <option value="">All Lexicon</option>
            <option value="pending">Pending</option>
            <option value="synced">Synced</option>
            <option value="error">Error</option>
            <option value="skipped">Skipped</option>
          </select>
          {activeFilterCount > 0 && (
            <button
              onClick={clearFilters}
              className="text-xs text-slate-400 hover:text-slate-200 px-2 py-1.5 rounded border border-slate-700 hover:border-slate-600 transition-colors"
            >
              Clear {activeFilterCount} filter{activeFilterCount > 1 ? 's' : ''}
            </button>
          )}
        </div>
      </div>

      {/* Error banner */}
      {error && (
        <div className="flex-none bg-red-500/10 border-b border-red-500/30 px-4 py-2 flex items-center justify-between">
          <p className="text-sm text-red-400">{error}</p>
          <button onClick={fetchTracks} className="btn-secondary text-xs py-1 px-2">Retry</button>
        </div>
      )}

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="w-full border-collapse">
          <thead className="sticky top-0 z-10 bg-slate-900 shadow-[0_1px_0_0_rgba(51,65,85,1)]">
            <tr>
              <ThCell label="Title" sortKey="title" minW="200px" />
              <ThCell label="Artist" sortKey="artist" minW="140px" />
              <ThCell label="Album" sortKey="album" minW="140px" />
              <ThCell label="Duration" sortKey="duration_ms" minW="70px" />
              <ThCell label="Added" sortKey="spotify_added_at" minW="100px" />
              <ThCell label="Pipeline" sortKey="pipeline_stage" minW="90px" />
              <ThCell label="Match" sortKey="match_status" minW="100px" />
              <ThCell label="Download" sortKey="download_status" minW="90px" />
              <ThCell label="Quality" sortKey="verify_codec" minW="140px" />
              <ThCell label="Lossless" minW="60px" />
              <ThCell label="Lexicon" sortKey="lexicon_status" minW="60px" />
              <ThCell label="File" minW="120px" />
            </tr>
          </thead>
          <tbody>
            {loading ? (
              Array.from({ length: 15 }).map((_, i) => (
                <tr key={i} className="border-b border-slate-800/50">
                  {Array.from({ length: 12 }).map((_, j) => (
                    <td key={j} className="px-4 py-2"><div className="w-full h-4 skeleton" /></td>
                  ))}
                </tr>
              ))
            ) : tracks.length === 0 ? (
              <tr>
                <td colSpan={12} className="text-center py-16 text-slate-500 text-sm">
                  No tracks found
                </td>
              </tr>
            ) : (
              tracks.map((track) => (
                <TrackTableRow
                  key={track.id}
                  track={track}
                  expanded={expandedId === track.id}
                  onToggle={() => setExpandedId(expandedId === track.id ? null : track.id)}
                  onRetry={() => handleRetry(track.id)}
                  onIgnore={() => handleIgnore(track.id)}
                  actionLoading={actionLoading === track.id}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex-none px-4 py-2 border-t border-slate-800 bg-slate-900/80 backdrop-blur-sm flex items-center justify-between">
        <p className="text-xs text-slate-500">
          {total > 0 ? `Showing ${startIdx.toLocaleString()}\u2013${endIdx.toLocaleString()} of ${total.toLocaleString()} tracks` : 'No tracks'}
        </p>
        {totalPages > 1 && (
          <div className="flex items-center gap-1">
            <PagBtn label="\u00AB" disabled={page <= 1} onClick={() => setPage(1)} />
            <PagBtn label="\u2039" disabled={page <= 1} onClick={() => setPage(page - 1)} />
            {pageNumbers.map((pn, i) =>
              pn < 0 ? (
                <span key={`e${i}`} className="px-1 text-slate-600 text-xs">\u2026</span>
              ) : (
                <PagBtn
                  key={pn}
                  label={String(pn)}
                  active={pn === page}
                  onClick={() => setPage(pn)}
                />
              )
            )}
            <PagBtn label="\u203A" disabled={page >= totalPages} onClick={() => setPage(page + 1)} />
            <PagBtn label="\u00BB" disabled={page >= totalPages} onClick={() => setPage(totalPages)} />
          </div>
        )}
      </div>
    </div>
  )
}

function PagBtn({ label, disabled, active, onClick }: { label: string; disabled?: boolean; active?: boolean; onClick: () => void }) {
  return (
    <button
      disabled={disabled}
      onClick={onClick}
      className={`min-w-[28px] h-7 px-1.5 text-xs rounded transition-colors ${
        active
          ? 'bg-emerald-600 text-white font-bold'
          : disabled
            ? 'text-slate-700 cursor-not-allowed'
            : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800'
      }`}
    >
      {label}
    </button>
  )
}

function TrackTableRow({
  track,
  expanded,
  onToggle,
  onRetry,
  onIgnore,
  actionLoading,
}: {
  track: TrackData
  expanded: boolean
  onToggle: () => void
  onRetry: () => void
  onIgnore: () => void
  actionLoading: boolean
}) {
  return (
    <>
      <tr
        className={`border-b border-slate-800/50 cursor-pointer transition-colors ${
          expanded ? 'bg-slate-800/40' : 'hover:bg-slate-800/20'
        }`}
        onClick={onToggle}
      >
        <td className="px-4 py-2 text-sm">
          <span className="text-slate-100 font-medium">{track.title || 'Unknown'}</span>
        </td>
        <td className="px-4 py-2 text-sm text-slate-400">{track.artist || 'Unknown'}</td>
        <td className="px-4 py-2 text-sm text-slate-500">{track.album || '\u2014'}</td>
        <td className="px-4 py-2 text-sm text-slate-400 tabular-nums">
          {track.duration_ms ? formatDuration(track.duration_ms) : '\u2014'}
        </td>
        <td className="px-4 py-2 text-sm text-slate-500 whitespace-nowrap">
          {formatDate(track.spotify_added_at)}
        </td>
        <td className="px-4 py-2 text-sm">
          <StatusBadge status={track.pipeline_stage} />
        </td>
        <td className="px-4 py-2 text-sm">
          <div className="flex items-center gap-1.5">
            <StatusBadge status={track.match_status} />
            {track.match_confidence != null && (
              <span className="text-[10px] text-slate-500">{(track.match_confidence * 100).toFixed(0)}%</span>
            )}
          </div>
        </td>
        <td className="px-4 py-2 text-sm">
          <StatusBadge status={track.download_status} />
        </td>
        <td className={`px-4 py-2 text-sm whitespace-nowrap ${qualityColor(track.verify_codec)}`}>
          {formatQuality(track.verify_codec, track.verify_sample_rate, track.verify_bit_depth)}
        </td>
        <td className="px-4 py-2 text-sm text-center">
          {track.verify_is_genuine_lossless === true ? <CheckMark /> : track.verify_is_genuine_lossless === false ? <XMark /> : <Dash />}
        </td>
        <td className="px-4 py-2 text-sm text-center">
          {track.lexicon_status === 'synced' && track.lexicon_track_id ? <CheckMark /> : track.lexicon_status === 'error' ? <XMark /> : <Dash />}
        </td>
        <td className="px-4 py-2 text-sm text-slate-600 font-mono text-xs" title={track.file_path || ''}>
          {truncatePath(track.file_path)}
        </td>
      </tr>
      {expanded && (
        <tr className="border-b border-slate-800/50">
          <td colSpan={12} className="px-6 py-4 bg-slate-800/20">
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-x-6 gap-y-3 text-sm">
              <DetailItem label="Spotify ID" value={track.spotify_id} mono />
              <DetailItem label="ISRC" value={track.isrc} mono />
              <DetailItem label="Tidal ID" value={track.tidal_id} mono />
              <DetailItem label="Match Source" value={track.match_source} />
              <DetailItem
                label="Match Confidence"
                value={track.match_confidence != null ? `${(track.match_confidence * 100).toFixed(1)}%` : null}
              />
              <DetailItem label="Download Source" value={track.download_source} />
              <div className="col-span-2">
                <DetailItem label="File Path" value={track.file_path} mono />
              </div>
              {track.chromaprint && (
                <div className="col-span-2">
                  <DetailItem
                    label="Chromaprint"
                    value={track.chromaprint.length > 60 ? track.chromaprint.slice(0, 60) + '\u2026' : track.chromaprint}
                    mono
                  />
                </div>
              )}
              {track.fingerprint_match_score != null && (
                <DetailItem label="Fingerprint Score" value={`${(track.fingerprint_match_score * 100).toFixed(1)}%`} />
              )}
              {(track.pipeline_error || track.download_error) && (
                <div className="col-span-2 lg:col-span-3">
                  <p className="text-[10px] text-red-400 uppercase tracking-wider mb-0.5">Error</p>
                  <p className="text-red-300 text-xs font-mono">{track.pipeline_error || track.download_error}</p>
                </div>
              )}
            </div>
            <div className="flex gap-2 mt-4 pt-3 border-t border-slate-800">
              <button
                onClick={(e) => { e.stopPropagation(); onRetry() }}
                disabled={actionLoading}
                className="btn-secondary text-xs py-1 px-3"
              >
                {actionLoading ? 'Working...' : 'Retry Pipeline'}
              </button>
              {track.pipeline_stage !== 'ignored' && (
                <button
                  onClick={(e) => { e.stopPropagation(); onIgnore() }}
                  disabled={actionLoading}
                  className="btn-danger text-xs py-1 px-3"
                >
                  Ignore Track
                </button>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

function DetailItem({ label, value, mono }: { label: string; value: string | null | undefined; mono?: boolean }) {
  return (
    <div>
      <p className="text-[10px] text-slate-500 uppercase tracking-wider mb-0.5">{label}</p>
      <p className={`text-slate-300 text-xs ${mono ? 'font-mono' : ''} break-all`}>{value || '\u2014'}</p>
    </div>
  )
}
