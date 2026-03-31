'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from './api'
import ParityMeter from './components/ParityMeter'
import ActivityFeed from './components/ActivityFeed'

const POLL_INTERVAL = 10_000

interface MonthlyRow {
  month: string
  total: number
  complete: number
  errors: number
}

interface RawDashboard {
  spotify_total: number
  lexicon_synced: number
  parity_pct: number
  by_pipeline_stage: Record<string, number>
  by_match_status: Record<string, number>
  by_download_status: Record<string, number>
  by_verify_status: Record<string, number>
  by_lexicon_status: Record<string, number>
  recent_activity: Array<{
    id: number
    event_type: string
    track_id: number | null
    message: string
    details: any
    created_at: string
  }>
  services: Array<{
    name: string
    status: string
    latency_ms: number
    error: string | null
  }>
}

export default function DashboardPage() {
  const [data, setData] = useState<RawDashboard | null>(null)
  const [monthly, setMonthly] = useState<MonthlyRow[]>([])
  const [syncMode, setSyncMode] = useState<string>('full')
  const [switchingMode, setSwitchingMode] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchDashboard = useCallback(async () => {
    try {
      const [result, monthlyResult, modeResult] = await Promise.all([
        apiFetch<RawDashboard>('/dashboard'),
        apiFetch<{ months: MonthlyRow[] }>('/dashboard/monthly'),
        apiFetch<{ sync_mode: string }>('/admin/sync-mode'),
      ])
      setData(result)
      setMonthly(monthlyResult.months?.slice(0, 24) ?? [])
      setSyncMode(modeResult.sync_mode || 'scan')
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch dashboard')
    } finally {
      setLoading(false)
    }
  }, [])

  const handleStartDownloads = async () => {
    setSwitchingMode(true)
    try {
      const result = await apiFetch<{ sync_mode: string; tracks_queued: number }>('/admin/sync-mode', {
        method: 'POST',
        body: JSON.stringify({ mode: 'full' }),
      })
      setSyncMode('full')
      fetchDashboard()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to enable downloads')
    } finally {
      setSwitchingMode(false)
    }
  }

  useEffect(() => {
    fetchDashboard()
    const interval = setInterval(fetchDashboard, POLL_INTERVAL)
    return () => clearInterval(interval)
  }, [fetchDashboard])

  const stages = data?.by_pipeline_stage || {}
  const services = data?.services || []
  const getService = (name: string) => services.find(s => s.name === name)

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Dashboard</h1>
        <p className="text-sm text-slate-500 mt-1">Library sync overview</p>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4 flex items-center justify-between">
          <p className="text-sm text-red-400">{error}</p>
          <button onClick={fetchDashboard} className="text-xs text-slate-400 hover:text-white px-3 py-1.5 rounded-lg border border-slate-700 hover:border-slate-600">
            Retry
          </button>
        </div>
      )}

      {/* Scan Mode Banner */}
      {!loading && syncMode === 'scan' && (
        <div className="rounded-xl border border-blue-500/30 bg-blue-500/5 p-6">
          <div className="flex items-start gap-3">
            <span className="text-2xl">📋</span>
            <div className="flex-1">
              <h2 className="text-base font-semibold text-blue-300">Library Scan Mode</h2>
              <p className="text-sm text-slate-400 mt-1">
                Matching your Spotify library against existing Lexicon tracks. No downloads will start until you enable them.
              </p>
              <div className="flex items-center gap-6 mt-4 text-sm">
                <span className="text-slate-300">
                  <span className="font-semibold text-emerald-400">{(stages.complete ?? 0).toLocaleString()}</span>
                  <span className="text-slate-500"> matched</span>
                </span>
                <span className="text-slate-300">
                  <span className="font-semibold text-amber-400">{(stages.waiting ?? 0).toLocaleString()}</span>
                  <span className="text-slate-500"> missing</span>
                </span>
                <span className="text-slate-300">
                  <span className="font-semibold text-red-400">{(stages.error ?? 0).toLocaleString()}</span>
                  <span className="text-slate-500"> errors</span>
                </span>
              </div>
              <button
                onClick={handleStartDownloads}
                disabled={switchingMode}
                className="mt-4 px-5 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium transition-colors disabled:opacity-50"
              >
                {switchingMode ? 'Enabling...' : 'Start Downloads'}
              </button>
              <p className="text-xs text-slate-600 mt-2">Begin downloading missing tracks via Tidal</p>
            </div>
          </div>
        </div>
      )}

      {/* Parity Score */}
      <div className="card flex justify-center">
        {loading ? (
          <div className="flex flex-col items-center justify-center py-8">
            <div className="w-48 h-48 rounded-full skeleton" />
            <div className="mt-4 w-32 h-6 skeleton" />
          </div>
        ) : (
          <ParityMeter
            data={{
              synced: data?.lexicon_synced ?? 0,
              total: data?.spotify_total ?? 0,
              percentage: data?.parity_pct ?? 0,
            }}
            loading={false}
          />
        )}
      </div>

      {/* Stage Breakdown */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-4">
        {[
          { label: 'New', value: stages.new, color: 'text-slate-400', bg: 'bg-slate-500/10 border-slate-500/20' },
          { label: 'Matching', value: stages.matching, color: 'text-blue-400', bg: 'bg-blue-500/10 border-blue-500/20' },
          { label: 'Waiting', value: stages.waiting, color: 'text-purple-400', bg: 'bg-purple-500/10 border-purple-500/20' },
          { label: 'Downloading', value: stages.downloading, color: 'text-cyan-400', bg: 'bg-cyan-500/10 border-cyan-500/20' },
          { label: 'Verifying', value: stages.verifying, color: 'text-amber-400', bg: 'bg-amber-500/10 border-amber-500/20' },
          { label: 'Complete', value: stages.complete, color: 'text-emerald-400', bg: 'bg-emerald-500/10 border-emerald-500/20' },
          { label: 'Error', value: stages.error, color: 'text-red-400', bg: 'bg-red-500/10 border-red-500/20' },
          { label: 'Ignored', value: stages.ignored, color: 'text-slate-500', bg: 'bg-slate-500/5 border-slate-700/30' },
        ].map((stat) => (
          <div key={stat.label} className={`rounded-xl border p-4 ${stat.bg}`}>
            <p className={`text-2xl font-bold tabular-nums ${stat.color}`}>
              {(stat.value ?? 0).toLocaleString()}
            </p>
            <p className="text-xs text-slate-500 mt-1">{stat.label}</p>
          </div>
        ))}
      </div>

      {/* Monthly Progress */}
      {monthly.length > 0 && (
        <div className="card">
          <h2 className="text-sm font-semibold text-slate-300 mb-4">Monthly Progress</h2>
          <div className="space-y-2">
            {monthly.map((m) => {
              const pct = m.total > 0 ? Math.round((m.complete / m.total) * 100) : 0
              const errPct = m.total > 0 ? Math.round((m.errors / m.total) * 100) : 0
              const remaining = m.total - m.complete - m.errors
              const remainPct = m.total > 0 ? Math.round((remaining / m.total) * 100) : 0
              const [year, mon] = m.month.split('-')
              const label = new Date(Number(year), Number(mon) - 1).toLocaleDateString('en-US', { month: 'short', year: 'numeric' })
              const glowColor = pct > 90 ? '0 0 8px rgba(34,197,94,0.4)' : pct >= 50 ? '0 0 8px rgba(245,158,11,0.3)' : '0 0 8px rgba(239,68,68,0.3)'
              const borderColor = pct > 90 ? 'border-emerald-500/30' : pct >= 50 ? 'border-amber-500/30' : 'border-red-500/30'
              return (
                <div key={m.month} className="flex items-center gap-3">
                  <span className="text-xs text-slate-400 w-20 shrink-0 tabular-nums">{label}</span>
                  <div
                    className={`flex-1 h-5 rounded-full overflow-hidden border ${borderColor}`}
                    style={{ background: 'rgb(30,41,59)', boxShadow: glowColor }}
                  >
                    <div className="flex h-full">
                      {pct > 0 && (
                        <div
                          style={{ width: `${pct}%` }}
                          className="bg-emerald-500 transition-all duration-500"
                        />
                      )}
                      {errPct > 0 && (
                        <div
                          style={{ width: `${errPct}%` }}
                          className="bg-red-500 transition-all duration-500"
                        />
                      )}
                      {remainPct > 0 && (
                        <div
                          style={{ width: `${remainPct}%` }}
                          className="bg-slate-600 transition-all duration-500"
                        />
                      )}
                    </div>
                  </div>
                  <span className="text-xs text-slate-500 w-16 shrink-0 text-right tabular-nums">
                    {m.complete}/{m.total}
                  </span>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Match + Download stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card text-center">
          <p className="text-3xl font-bold text-emerald-400">{data?.by_match_status?.matched?.toLocaleString() ?? '—'}</p>
          <p className="text-xs text-slate-500 mt-1">Matched</p>
        </div>
        <div className="card text-center">
          <p className="text-3xl font-bold text-red-400">{data?.by_match_status?.failed?.toLocaleString() ?? '—'}</p>
          <p className="text-xs text-slate-500 mt-1">Match Failed</p>
        </div>
        <div className="card text-center">
          <p className="text-3xl font-bold text-emerald-400">{data?.by_download_status?.complete?.toLocaleString() ?? '—'}</p>
          <p className="text-xs text-slate-500 mt-1">Downloaded</p>
        </div>
        <div className="card text-center">
          <p className="text-3xl font-bold text-emerald-400">{data?.by_verify_status?.pass?.toLocaleString() ?? '—'}</p>
          <p className="text-xs text-slate-500 mt-1">Verified Lossless</p>
        </div>
      </div>

      {/* Activity + Health */}
      <div className="grid lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 card">
          <h2 className="text-sm font-semibold text-slate-300 mb-4">Recent Activity</h2>
          {loading ? (
            <div className="space-y-3">
              {Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-lg skeleton" />
                  <div className="flex-1">
                    <div className="w-3/4 h-4 skeleton mb-1" />
                    <div className="w-1/4 h-3 skeleton" />
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <ActivityFeed
              events={(data?.recent_activity || []).map(e => ({
                id: String(e.id),
                timestamp: e.created_at,
                type: e.event_type.includes('error') ? 'error' as const
                  : e.event_type.includes('download') ? 'download' as const
                  : e.event_type.includes('match') ? 'match' as const
                  : e.event_type.includes('verify') ? 'verify' as const
                  : e.event_type.includes('sync') || e.event_type.includes('lexicon') ? 'sync' as const
                  : 'download' as const,
                message: e.message,
              }))}
              loading={false}
            />
          )}
        </div>

        <div className="card">
          <h2 className="text-sm font-semibold text-slate-300 mb-4">Service Health</h2>
          <div className="space-y-4">
            {(['spotify', 'tidal', 'lexicon'] as const).map((name) => {
              const svc = getService(name)
              const spotifyOk = name === 'spotify' ? data !== null : undefined
              const isOk = svc ? svc.status === 'ok' : spotifyOk
              return (
                <div key={name} className="flex items-center justify-between py-2">
                  <div className="flex items-center gap-3">
                    <div className={`w-2.5 h-2.5 rounded-full ${
                      loading ? 'bg-slate-600 animate-pulse'
                        : isOk ? 'bg-emerald-400'
                        : 'bg-red-400'
                    }`} />
                    <span className="text-sm text-slate-300 capitalize">{name}</span>
                  </div>
                  {!loading && svc && (
                    <span className="text-xs text-slate-500 tabular-nums">
                      {svc.latency_ms ? `${Math.round(svc.latency_ms)}ms` : svc.status}
                    </span>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      </div>
    </div>
  )
}
