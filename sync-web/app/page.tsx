'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from './api'
import { DashboardData } from './types'
import ParityMeter from './components/ParityMeter'
import ActivityFeed from './components/ActivityFeed'

const POLL_INTERVAL = 10_000

export default function DashboardPage() {
  const [data, setData] = useState<DashboardData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchDashboard = useCallback(async () => {
    try {
      const result = await apiFetch<DashboardData>('/dashboard')
      setData(result)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch dashboard')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchDashboard()
    const interval = setInterval(fetchDashboard, POLL_INTERVAL)
    return () => clearInterval(interval)
  }, [fetchDashboard])

  const stages = data?.stages
  const health = data?.health

  return (
    <div className="space-y-8">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Dashboard</h1>
        <p className="text-sm text-slate-500 mt-1">Library sync overview</p>
      </div>

      {/* Error banner */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4 flex items-center justify-between">
          <p className="text-sm text-red-400">{error}</p>
          <button onClick={fetchDashboard} className="btn-secondary text-xs">
            Retry
          </button>
        </div>
      )}

      {/* Parity Score - Hero */}
      <div className="card flex justify-center">
        <ParityMeter data={data?.parity} loading={loading} />
      </div>

      {/* Stage Breakdown */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        {[
          { label: 'Missing', value: stages?.missing, color: 'text-red-400', bg: 'bg-red-500/10 border-red-500/20' },
          { label: 'Downloading', value: stages?.downloading, color: 'text-blue-400', bg: 'bg-blue-500/10 border-blue-500/20' },
          { label: 'Verifying', value: stages?.verifying, color: 'text-amber-400', bg: 'bg-amber-500/10 border-amber-500/20' },
          { label: 'Mismatched', value: stages?.mismatched, color: 'text-orange-400', bg: 'bg-orange-500/10 border-orange-500/20' },
          { label: 'Complete', value: stages?.complete, color: 'text-emerald-400', bg: 'bg-emerald-500/10 border-emerald-500/20' },
          { label: 'Error', value: stages?.error, color: 'text-red-400', bg: 'bg-red-500/10 border-red-500/20' },
        ].map((stat) => (
          <div key={stat.label} className={`rounded-xl border p-4 ${stat.bg}`}>
            {loading ? (
              <>
                <div className="w-12 h-8 skeleton mb-2" />
                <div className="w-16 h-4 skeleton" />
              </>
            ) : (
              <>
                <p className={`text-2xl font-bold tabular-nums ${stat.color}`}>
                  {(stat.value ?? 0).toLocaleString()}
                </p>
                <p className="text-xs text-slate-500 mt-1">{stat.label}</p>
              </>
            )}
          </div>
        ))}
      </div>

      {/* Activity + Health */}
      <div className="grid lg:grid-cols-3 gap-6">
        {/* Activity Feed */}
        <div className="lg:col-span-2 card">
          <h2 className="text-sm font-semibold text-slate-300 mb-4">Recent Activity</h2>
          <ActivityFeed events={data?.activity} loading={loading} />
        </div>

        {/* Service Health */}
        <div className="card">
          <h2 className="text-sm font-semibold text-slate-300 mb-4">Service Health</h2>
          <div className="space-y-4">
            {[
              { name: 'Spotify', status: health?.spotify },
              { name: 'Tidarr', status: health?.tidarr },
              { name: 'Lexicon', status: health?.lexicon },
            ].map((svc) => (
              <div key={svc.name} className="flex items-center justify-between py-2">
                <div className="flex items-center gap-3">
                  <div
                    className={`w-2.5 h-2.5 rounded-full ${
                      loading
                        ? 'bg-slate-600 animate-pulse'
                        : svc.status?.connected
                        ? 'bg-emerald-400'
                        : 'bg-red-400'
                    }`}
                  />
                  <span className="text-sm text-slate-300">{svc.name}</span>
                </div>
                {!loading && svc.status && (
                  <span className="text-xs text-slate-500 tabular-nums">
                    {svc.status.latency_ms !== undefined
                      ? `${svc.status.latency_ms}ms`
                      : svc.status.connected
                      ? 'OK'
                      : 'Down'}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
