'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'
import { Track, TracksResponse, PipelineStage, MatchStatus } from '../types'
import TrackRow from '../components/TrackRow'

const PER_PAGE = 50

export default function TracksPage() {
  const [tracks, setTracks] = useState<Track[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [stageFilter, setStageFilter] = useState<string>('')
  const [matchFilter, setMatchFilter] = useState<string>('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchTracks = useCallback(async () => {
    setLoading(true)
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        per_page: PER_PAGE.toString(),
      })
      if (search) params.set('q', search)
      if (stageFilter) params.set('pipeline_stage', stageFilter)
      if (matchFilter) params.set('match_status', matchFilter)

      const result = await apiFetch<TracksResponse>(`/tracks?${params}`)
      setTracks(result.tracks)
      setTotal(result.total)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch tracks')
    } finally {
      setLoading(false)
    }
  }, [page, search, stageFilter, matchFilter])

  useEffect(() => {
    fetchTracks()
  }, [fetchTracks])

  // Debounced search
  const [searchInput, setSearchInput] = useState('')
  useEffect(() => {
    const timer = setTimeout(() => {
      setSearch(searchInput)
      setPage(1)
    }, 300)
    return () => clearTimeout(timer)
  }, [searchInput])

  const totalPages = Math.ceil(total / PER_PAGE)

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Track Library</h1>
        <p className="text-sm text-slate-500 mt-1">
          {total.toLocaleString()} tracks total
        </p>
      </div>

      {/* Filters */}
      <div className="flex flex-col sm:flex-row gap-3">
        <div className="flex-1">
          <input
            type="text"
            placeholder="Search title or artist..."
            className="input-field"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
          />
        </div>
        <select
          className="select-field"
          value={stageFilter}
          onChange={(e) => { setStageFilter(e.target.value); setPage(1) }}
        >
          <option value="">All Stages</option>
          <option value="pending">Pending</option>
          <option value="matching">Matching</option>
          <option value="downloading">Downloading</option>
          <option value="verifying">Verifying</option>
          <option value="complete">Complete</option>
          <option value="error">Error</option>
        </select>
        <select
          className="select-field"
          value={matchFilter}
          onChange={(e) => { setMatchFilter(e.target.value); setPage(1) }}
        >
          <option value="">All Match Status</option>
          <option value="unmatched">Unmatched</option>
          <option value="matched">Matched</option>
          <option value="mismatched">Mismatched</option>
          <option value="approved">Approved</option>
          <option value="rejected">Rejected</option>
        </select>
      </div>

      {/* Error */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4 flex items-center justify-between">
          <p className="text-sm text-red-400">{error}</p>
          <button onClick={fetchTracks} className="btn-secondary text-xs">Retry</button>
        </div>
      )}

      {/* Table */}
      <div className="card overflow-hidden p-0">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b border-slate-800">
                <th className="table-header">Title / Album</th>
                <th className="table-header">Artist</th>
                <th className="table-header">Match</th>
                <th className="table-header">Download</th>
                <th className="table-header">Verified</th>
                <th className="table-header">Lexicon</th>
                <th className="table-header w-8"></th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                Array.from({ length: 10 }).map((_, i) => (
                  <tr key={i} className="border-b border-slate-800/50">
                    <td className="table-cell"><div className="w-40 h-5 skeleton" /></td>
                    <td className="table-cell"><div className="w-24 h-5 skeleton" /></td>
                    <td className="table-cell"><div className="w-20 h-5 skeleton" /></td>
                    <td className="table-cell"><div className="w-20 h-5 skeleton" /></td>
                    <td className="table-cell"><div className="w-8 h-5 skeleton" /></td>
                    <td className="table-cell"><div className="w-8 h-5 skeleton" /></td>
                    <td className="table-cell"></td>
                  </tr>
                ))
              ) : tracks.length === 0 ? (
                <tr>
                  <td colSpan={7} className="text-center py-12 text-slate-500 text-sm">
                    No tracks found
                  </td>
                </tr>
              ) : (
                tracks.map((track) => <TrackRow key={track.id} track={track} />)
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-slate-500">
            Page {page} of {totalPages}
          </p>
          <div className="flex gap-2">
            <button
              className="btn-secondary text-sm"
              disabled={page <= 1}
              onClick={() => setPage(page - 1)}
            >
              Previous
            </button>
            <button
              className="btn-secondary text-sm"
              disabled={page >= totalPages}
              onClick={() => setPage(page + 1)}
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
