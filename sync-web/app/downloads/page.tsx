'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'
import { DownloadsResponse, DownloadQueueItem } from '../types'
import StatusBadge from '../components/StatusBadge'

const POLL_INTERVAL = 5_000

function DownloadItem({ item, onRetry }: { item: DownloadQueueItem; onRetry?: (id: string) => void }) {
  return (
    <div className="flex items-center gap-4 px-4 py-3 hover:bg-slate-800/30 transition-colors">
      <div className="flex-1 min-w-0">
        <p className="text-sm text-slate-200 font-medium truncate">{item.title}</p>
        <p className="text-xs text-slate-500 truncate">{item.artist}</p>
      </div>
      <div className="w-32 hidden sm:block">
        <StatusBadge status={item.status} />
      </div>
      {item.status === 'downloading' && (
        <div className="w-32 flex items-center gap-2">
          <div className="flex-1 h-1.5 bg-slate-800 rounded-full overflow-hidden">
            <div
              className="h-full bg-blue-500 rounded-full transition-all"
              style={{ width: `${item.progress}%` }}
            />
          </div>
          <span className="text-xs text-slate-500 tabular-nums w-10 text-right">
            {item.progress}%
          </span>
        </div>
      )}
      {item.speed_kbps && (
        <span className="text-xs text-slate-500 tabular-nums hidden md:block">
          {item.speed_kbps > 1000
            ? `${(item.speed_kbps / 1000).toFixed(1)} MB/s`
            : `${item.speed_kbps} KB/s`}
        </span>
      )}
      {item.status === 'failed' && onRetry && (
        <button
          className="text-xs text-amber-400 hover:text-amber-300 transition-colors"
          onClick={() => onRetry(item.id)}
        >
          Retry
        </button>
      )}
    </div>
  )
}

export default function DownloadsPage() {
  const [data, setData] = useState<DownloadsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchDownloads = useCallback(async () => {
    try {
      const result = await apiFetch<DownloadsResponse>('/downloads')
      setData(result)
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

  const handleRetry = async (id: string) => {
    try {
      await apiFetch(`/downloads/${id}/retry`, { method: 'POST' })
      fetchDownloads()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Retry failed')
    }
  }

  const stats = data?.stats

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Downloads</h1>
        <p className="text-sm text-slate-500 mt-1">Download queue and status</p>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4 flex items-center justify-between">
          <p className="text-sm text-red-400">{error}</p>
          <button onClick={fetchDownloads} className="btn-secondary text-xs">Retry</button>
        </div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Downloaded', value: stats?.total_downloaded, color: 'text-emerald-400' },
          { label: 'Failed', value: stats?.total_failed, color: 'text-red-400' },
          { label: 'In Queue', value: stats?.queue_size, color: 'text-blue-400' },
          {
            label: 'Avg Speed',
            value: stats?.avg_speed_kbps
              ? stats.avg_speed_kbps > 1000
                ? `${(stats.avg_speed_kbps / 1000).toFixed(1)} MB/s`
                : `${stats.avg_speed_kbps} KB/s`
              : '-',
            color: 'text-slate-300',
            raw: true,
          },
        ].map((s) => (
          <div key={s.label} className="card">
            {loading ? (
              <>
                <div className="w-16 h-8 skeleton mb-2" />
                <div className="w-20 h-4 skeleton" />
              </>
            ) : (
              <>
                <p className={`text-2xl font-bold tabular-nums ${s.color}`}>
                  {(s as any).raw ? s.value : (s.value ?? 0).toLocaleString()}
                </p>
                <p className="text-xs text-slate-500 mt-1">{s.label}</p>
              </>
            )}
          </div>
        ))}
      </div>

      {/* Active Downloads */}
      <div className="card p-0 overflow-hidden">
        <div className="px-5 py-3 border-b border-slate-800">
          <h2 className="text-sm font-semibold text-slate-300">
            Active Downloads
            {data?.active && (
              <span className="ml-2 text-xs text-slate-500">({data.active.length})</span>
            )}
          </h2>
        </div>
        {loading ? (
          <div className="p-4 space-y-3">
            {Array.from({ length: 3 }).map((_, i) => (
              <div key={i} className="h-12 skeleton rounded" />
            ))}
          </div>
        ) : data?.active && data.active.length > 0 ? (
          <div className="divide-y divide-slate-800/50">
            {data.active.map((item) => (
              <DownloadItem key={item.id} item={item} />
            ))}
          </div>
        ) : (
          <div className="py-8 text-center text-sm text-slate-500">No active downloads</div>
        )}
      </div>

      {/* Queued */}
      <div className="card p-0 overflow-hidden">
        <div className="px-5 py-3 border-b border-slate-800">
          <h2 className="text-sm font-semibold text-slate-300">
            Queued
            {data?.queued && (
              <span className="ml-2 text-xs text-slate-500">({data.queued.length})</span>
            )}
          </h2>
        </div>
        {loading ? (
          <div className="p-4 space-y-3">
            {Array.from({ length: 3 }).map((_, i) => (
              <div key={i} className="h-12 skeleton rounded" />
            ))}
          </div>
        ) : data?.queued && data.queued.length > 0 ? (
          <div className="divide-y divide-slate-800/50">
            {data.queued.map((item) => (
              <DownloadItem key={item.id} item={item} />
            ))}
          </div>
        ) : (
          <div className="py-8 text-center text-sm text-slate-500">Queue empty</div>
        )}
      </div>

      {/* Failed */}
      {data?.failed && data.failed.length > 0 && (
        <div className="card p-0 overflow-hidden">
          <div className="px-5 py-3 border-b border-red-500/20 bg-red-500/5">
            <h2 className="text-sm font-semibold text-red-400">
              Failed ({data.failed.length})
            </h2>
          </div>
          <div className="divide-y divide-slate-800/50">
            {data.failed.map((item) => (
              <div key={item.id}>
                <DownloadItem item={item} onRetry={handleRetry} />
                {item.error_message && (
                  <p className="px-4 pb-3 text-xs text-red-400/70">{item.error_message}</p>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
