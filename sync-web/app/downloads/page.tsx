'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'
import StatusBadge from '../components/StatusBadge'

const POLL_INTERVAL = 5_000

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
}

export default function DownloadsPage() {
  const [items, setItems] = useState<DownloadItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchDownloads = useCallback(async () => {
    try {
      const result = await apiFetch<any>('/downloads')
      setItems(result.items || [])
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch downloads')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchDownloads()
    const interval = setInterval(fetchDownloads, POLL_INTERVAL)
    return () => clearInterval(interval)
  }, [fetchDownloads])

  const handleRetry = async (trackId: number) => {
    try {
      await apiFetch(`/downloads/${trackId}/retry`, { method: 'POST' })
      fetchDownloads()
    } catch {}
  }

  const active = items.filter(i => i.status === 'downloading')
  const queued = items.filter(i => i.status === 'queued' || i.status === 'pending')
  const failed = items.filter(i => i.status === 'failed')
  const complete = items.filter(i => i.status === 'complete')

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Downloads</h1>
        <p className="text-sm text-slate-500 mt-1">{items.length} items in queue</p>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card text-center">
          <p className="text-2xl font-bold text-blue-400">{active.length}</p>
          <p className="text-xs text-slate-500">Active</p>
        </div>
        <div className="card text-center">
          <p className="text-2xl font-bold text-slate-400">{queued.length}</p>
          <p className="text-xs text-slate-500">Queued</p>
        </div>
        <div className="card text-center">
          <p className="text-2xl font-bold text-emerald-400">{complete.length}</p>
          <p className="text-xs text-slate-500">Complete</p>
        </div>
        <div className="card text-center">
          <p className="text-2xl font-bold text-red-400">{failed.length}</p>
          <p className="text-xs text-slate-500">Failed</p>
        </div>
      </div>

      {/* Active Downloads */}
      {active.length > 0 && (
        <div className="card p-0 overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800">
            <h2 className="text-sm font-semibold text-blue-400">Active Downloads</h2>
          </div>
          {active.map(item => (
            <div key={item.id} className="flex items-center gap-4 px-4 py-3 border-b border-slate-800/50">
              <div className="flex-1 min-w-0">
                <p className="text-sm text-slate-200 font-medium truncate">{item.track_title}</p>
                <p className="text-xs text-slate-500">{item.track_artist}</p>
              </div>
              <StatusBadge status={item.status} />
            </div>
          ))}
        </div>
      )}

      {/* Failed Downloads */}
      {failed.length > 0 && (
        <div className="card p-0 overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800">
            <h2 className="text-sm font-semibold text-red-400">Failed ({failed.length})</h2>
          </div>
          {failed.slice(0, 20).map(item => (
            <div key={item.id} className="flex items-center gap-4 px-4 py-3 border-b border-slate-800/50">
              <div className="flex-1 min-w-0">
                <p className="text-sm text-slate-200 font-medium truncate">{item.track_title}</p>
                <p className="text-xs text-slate-500">{item.track_artist}</p>
                {item.error && <p className="text-xs text-red-400 mt-1 truncate">{item.error}</p>}
              </div>
              <span className="text-xs text-slate-500">Attempt {item.attempts}/{item.max_attempts}</span>
              <button
                className="text-xs text-amber-400 hover:text-amber-300 px-2 py-1 border border-amber-500/30 rounded"
                onClick={() => handleRetry(item.track_id)}
              >
                Retry
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Recent Complete */}
      {complete.length > 0 && (
        <div className="card p-0 overflow-hidden">
          <div className="px-4 py-3 border-b border-slate-800">
            <h2 className="text-sm font-semibold text-emerald-400">Recently Complete ({complete.length})</h2>
          </div>
          {complete.slice(0, 10).map(item => (
            <div key={item.id} className="flex items-center gap-4 px-4 py-3 border-b border-slate-800/50">
              <div className="flex-1 min-w-0">
                <p className="text-sm text-slate-200 font-medium truncate">{item.track_title}</p>
                <p className="text-xs text-slate-500">{item.track_artist}</p>
              </div>
              <StatusBadge status="complete" />
            </div>
          ))}
        </div>
      )}

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
