'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'

export default function SettingsPage() {
  const [settings, setSettings] = useState<Record<string, string>>({})
  const [backups, setBackups] = useState<any[]>([])
  const [version, setVersion] = useState<any>(null)
  const [spotifyStatus, setSpotifyStatus] = useState<any>(null)
  const [health, setHealth] = useState<any>(null)
  const [dashboard, setDashboard] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  const fetchAll = useCallback(async () => {
    try {
      const [settingsRes, backupsRes, versionRes, spotifyRes, healthRes, dashboardRes] = await Promise.allSettled([
        apiFetch<any>('/settings'),
        apiFetch<any>('/lexicon/backups'),
        apiFetch<any>('/admin/version'),
        apiFetch<any>('/spotify/status'),
        apiFetch<any>('/admin/health'),
        apiFetch<any>('/dashboard'),
      ])

      if (settingsRes.status === 'fulfilled') setSettings(settingsRes.value.settings || {})
      if (backupsRes.status === 'fulfilled') setBackups(backupsRes.value.backups || backupsRes.value || [])
      if (versionRes.status === 'fulfilled') setVersion(versionRes.value)
      if (spotifyRes.status === 'fulfilled') setSpotifyStatus(spotifyRes.value)
      if (healthRes.status === 'fulfilled') setHealth(healthRes.value)
      if (dashboardRes.status === 'fulfilled') setDashboard(dashboardRes.value)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load settings')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchAll() }, [fetchAll])

  const handleSave = async () => {
    setSaving(true)
    setError(null)
    try {
      await apiFetch('/settings', {
        method: 'PATCH',
        body: JSON.stringify({ settings }),
      })
      setSuccess('Settings saved')
      setTimeout(() => setSuccess(null), 3000)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  const handleConnectSpotify = () => {
    window.open('/api/spotify/auth', '_blank')
  }

  const handleCreateBackup = async () => {
    try {
      await apiFetch('/lexicon/backup', { method: 'POST' })
      setSuccess('Backup created')
      fetchAll()
      setTimeout(() => setSuccess(null), 3000)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Backup failed')
    }
  }

  const updateSetting = (key: string, value: string) => {
    setSettings(prev => ({ ...prev, [key]: value }))
  }

  if (loading) {
    return (
      <div className="space-y-6">
        <h1 className="text-2xl font-bold text-slate-100">Settings</h1>
        <div className="space-y-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="card"><div className="h-24 skeleton rounded" /></div>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Settings</h1>
        <p className="text-sm text-slate-500 mt-1">Configuration and maintenance</p>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}
      {success && (
        <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-xl px-5 py-4">
          <p className="text-sm text-emerald-400">{success}</p>
        </div>
      )}

      {/* Spotify Connection */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Spotify Connection</h2>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className={`w-3 h-3 rounded-full ${spotifyStatus?.authenticated ? 'bg-emerald-400' : 'bg-red-400'}`} />
            <span className="text-sm text-slate-300">
              {spotifyStatus?.authenticated ? 'Connected' : 'Not connected'}
            </span>
            {spotifyStatus?.last_poll && (
              <span className="text-xs text-slate-500">
                Last poll: {new Date(spotifyStatus.last_poll).toLocaleString()}
              </span>
            )}
          </div>
          <button className="btn-primary text-sm" onClick={handleConnectSpotify}>
            {spotifyStatus?.authenticated ? 'Reconnect' : 'Connect Spotify'}
          </button>
        </div>
      </div>

      {/* Service Health */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Service Health</h2>
        <div className="space-y-3">
          {/* Spotify */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className={`w-2.5 h-2.5 rounded-full ${spotifyStatus?.authenticated ? 'bg-emerald-400' : 'bg-red-400'}`} />
              <span className="text-sm text-slate-300">Spotify</span>
            </div>
            <span className="text-xs text-slate-500">
              {spotifyStatus?.authenticated ? 'Connected' : 'Disconnected'}
            </span>
          </div>

          {/* Tidarr */}
          {(() => {
            const tidarr = dashboard?.services?.find((s: any) => s.name === 'tidarr')
            const tidarrUrl = settings.tidarr_url || 'http://192.168.1.221:8484'
            return (
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className={`w-2.5 h-2.5 rounded-full ${tidarr?.status === 'ok' ? 'bg-emerald-400' : 'bg-red-400'}`} />
                  <span className="text-sm text-slate-300">Tidarr</span>
                  <span className="text-xs text-slate-600 font-mono">{tidarrUrl}</span>
                </div>
                <span className="text-xs text-slate-500">
                  {tidarr?.status === 'ok'
                    ? `Connected (${tidarr.latency_ms}ms)`
                    : tidarr?.error || 'Unknown'}
                </span>
              </div>
            )
          })()}

          {/* Lexicon */}
          {(() => {
            const lexicon = dashboard?.services?.find((s: any) => s.name === 'lexicon')
            const lexiconUrl = settings.lexicon_api_url || 'http://192.168.1.116:48624'
            return (
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className={`w-2.5 h-2.5 rounded-full ${lexicon?.status === 'ok' ? 'bg-emerald-400' : 'bg-red-400'}`} />
                  <span className="text-sm text-slate-300">Lexicon</span>
                  <span className="text-xs text-slate-600 font-mono">{lexiconUrl}</span>
                </div>
                <span className="text-xs text-slate-500">
                  {lexicon?.status === 'ok'
                    ? `Connected (${lexicon.latency_ms}ms)`
                    : lexicon?.error || 'Unknown'}
                </span>
              </div>
            )
          })()}

          {/* Database */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className={`w-2.5 h-2.5 rounded-full ${health?.database === 'ok' ? 'bg-emerald-400' : 'bg-red-400'}`} />
              <span className="text-sm text-slate-300">Database</span>
            </div>
            <span className="text-xs text-slate-500">
              {health?.database === 'ok' ? 'OK' : health?.database || 'Unknown'}
            </span>
          </div>

          {/* Uptime */}
          {health?.uptime_seconds != null && (
            <div className="flex items-center justify-between pt-2 border-t border-slate-800">
              <span className="text-xs text-slate-500">Uptime</span>
              <span className="text-xs text-slate-400 font-mono">
                {(() => {
                  const s = health.uptime_seconds
                  const d = Math.floor(s / 86400)
                  const h = Math.floor((s % 86400) / 3600)
                  const m = Math.floor((s % 3600) / 60)
                  return d > 0 ? `${d}d ${h}h ${m}m` : h > 0 ? `${h}h ${m}m` : `${m}m`
                })()}
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Configuration */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Configuration</h2>
        <div className="grid md:grid-cols-2 gap-4">
          <div>
            <label className="block text-xs text-slate-500 mb-1">Poll Interval (seconds)</label>
            <input
              type="number"
              className="input-field"
              value={settings.spotify_poll_interval_seconds || '300'}
              onChange={(e) => updateSetting('spotify_poll_interval_seconds', e.target.value)}
            />
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Fingerprint Min Score</label>
            <input
              type="number"
              step="0.01"
              className="input-field"
              value={settings.fingerprint_min_score || '0.85'}
              onChange={(e) => updateSetting('fingerprint_min_score', e.target.value)}
            />
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Max Concurrent Downloads</label>
            <input
              type="number"
              className="input-field"
              value={settings.max_concurrent_downloads || '2'}
              onChange={(e) => updateSetting('max_concurrent_downloads', e.target.value)}
            />
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Backup Before Sync</label>
            <select
              className="select-field w-full"
              value={settings.lexicon_backup_before_sync || '1'}
              onChange={(e) => updateSetting('lexicon_backup_before_sync', e.target.value)}
            >
              <option value="1">Enabled</option>
              <option value="0">Disabled</option>
            </select>
          </div>
        </div>
        <div className="mt-4 pt-4 border-t border-slate-800 flex justify-end">
          <button className="btn-primary text-sm" disabled={saving} onClick={handleSave}>
            {saving ? 'Saving...' : 'Save Settings'}
          </button>
        </div>
      </div>

      {/* Parity Stats */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Parity Stats</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center">
          <div>
            <p className="text-2xl font-bold text-slate-200">{settings.parity_total_tracks || '0'}</p>
            <p className="text-xs text-slate-500">Total Tracks</p>
          </div>
          <div>
            <p className="text-2xl font-bold text-emerald-400">{settings.parity_synced || '0'}</p>
            <p className="text-xs text-slate-500">Synced</p>
          </div>
          <div>
            <p className="text-2xl font-bold text-amber-400">{settings.parity_in_progress || '0'}</p>
            <p className="text-xs text-slate-500">In Progress</p>
          </div>
          <div>
            <p className="text-2xl font-bold text-red-400">{settings.parity_errors || '0'}</p>
            <p className="text-xs text-slate-500">Errors</p>
          </div>
        </div>
      </div>

      {/* Lexicon Backups */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold text-slate-300">Lexicon Backups</h2>
          <button className="btn-secondary text-sm" onClick={handleCreateBackup}>Create Backup</button>
        </div>
        {Array.isArray(backups) && backups.length > 0 ? (
          <div className="divide-y divide-slate-800">
            {backups.map((backup: any, i: number) => (
              <div key={backup.id || i} className="flex items-center justify-between py-3">
                <div>
                  <p className="text-sm text-slate-300">{backup.backup_path || backup.filename}</p>
                  <p className="text-xs text-slate-500">{backup.created_at}</p>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-slate-500 text-center py-6">No backups yet</p>
        )}
      </div>

      {/* Version */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Version</h2>
        <div className="flex items-center justify-between">
          <div className="text-sm text-slate-400">
            <span className="font-mono text-xs">{version?.version || '?'}{version?.git_sha ? ` (${version.git_sha})` : ''}</span>
          </div>
          <button
            className="btn-secondary text-sm"
            onClick={() => apiFetch('/admin/update', { method: 'POST' }).then(() => {
              setSuccess('Update requested')
              setTimeout(() => setSuccess(null), 3000)
            }).catch(() => setError('Update request failed'))}
          >
            Request Update
          </button>
        </div>
      </div>
    </div>
  )
}
