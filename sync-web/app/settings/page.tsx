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
  const [tidalStatus, setTidalStatus] = useState<any>(null)
  const [tidalAuth, setTidalAuth] = useState<{
    state: 'idle' | 'waiting_for_code' | 'showing_code' | 'polling' | 'success' | 'error'
    verification_uri?: string
    user_code?: string
    error?: string
    interval?: number
  }>({ state: 'idle' })
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  const fetchAll = useCallback(async () => {
    try {
      const [settingsRes, backupsRes, versionRes, spotifyRes, healthRes, dashboardRes, tidalRes] = await Promise.allSettled([
        apiFetch<any>('/settings'),
        apiFetch<any>('/lexicon/backups'),
        apiFetch<any>('/admin/version'),
        apiFetch<any>('/spotify/status'),
        apiFetch<any>('/admin/health'),
        apiFetch<any>('/dashboard'),
        apiFetch<any>('/tidal/status'),
      ])

      if (settingsRes.status === 'fulfilled') setSettings(settingsRes.value.settings || {})
      if (backupsRes.status === 'fulfilled') setBackups(backupsRes.value.backups || backupsRes.value || [])
      if (versionRes.status === 'fulfilled') setVersion(versionRes.value)
      if (spotifyRes.status === 'fulfilled') setSpotifyStatus(spotifyRes.value)
      if (healthRes.status === 'fulfilled') setHealth(healthRes.value)
      if (dashboardRes.status === 'fulfilled') setDashboard(dashboardRes.value)
      if (tidalRes.status === 'fulfilled') setTidalStatus(tidalRes.value)
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

  const handleConnectTidal = async () => {
    setTidalAuth({ state: 'waiting_for_code' })
    try {
      const data = await apiFetch<any>('/tidal/auth/start', { method: 'POST' })
      if (data.error) {
        setTidalAuth({ state: 'error', error: data.error })
        return
      }
      setTidalAuth({
        state: 'showing_code',
        verification_uri: data.verification_uri,
        user_code: data.user_code,
        interval: data.interval || 5,
      })
      // Start polling
      const pollInterval = (data.interval || 5) * 1000
      const maxAttempts = Math.ceil((data.expires_in || 300) / (data.interval || 5))
      let attempts = 0
      const poll = setInterval(async () => {
        attempts++
        if (attempts > maxAttempts) {
          clearInterval(poll)
          setTidalAuth({ state: 'error', error: 'Authorization timed out. Please try again.' })
          return
        }
        try {
          const result = await apiFetch<any>('/tidal/auth/poll', { method: 'POST' })
          if (result.status === 'authorized') {
            clearInterval(poll)
            setTidalAuth({ state: 'success' })
            setSuccess(`Tidal connected (user ${result.user_id})`)
            setTimeout(() => setSuccess(null), 5000)
            // Refresh tidal status
            const fresh = await apiFetch<any>('/tidal/status')
            setTidalStatus(fresh)
            setTimeout(() => setTidalAuth({ state: 'idle' }), 5000)
          } else if (result.status === 'error') {
            clearInterval(poll)
            setTidalAuth({ state: 'error', error: result.error })
          }
          // 'pending' -> keep polling
        } catch {
          clearInterval(poll)
          setTidalAuth({ state: 'error', error: 'Polling failed' })
        }
      }, pollInterval)
    } catch (err) {
      setTidalAuth({ state: 'error', error: err instanceof Error ? err.message : 'Failed to start auth' })
    }
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

      {/* Tidal Connection */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Tidal Connection</h2>
        <div className="space-y-4">
          {/* Status row */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className={`w-3 h-3 rounded-full ${
                tidalStatus?.connected
                  ? tidalStatus?.expired ? 'bg-amber-400' : 'bg-emerald-400'
                  : 'bg-red-400'
              }`} />
              <span className="text-sm text-slate-300">
                {tidalStatus?.connected
                  ? tidalStatus?.expired
                    ? 'Token expired'
                    : `Connected (user ${tidalStatus.user_id})`
                  : 'Not connected'}
              </span>
              {tidalStatus?.connected && !tidalStatus?.expired && (
                <span className="text-xs text-slate-500">
                  Expires in {tidalStatus.hours_left}h
                </span>
              )}
            </div>
            <button
              className="btn-primary text-sm"
              onClick={handleConnectTidal}
              disabled={tidalAuth.state === 'waiting_for_code' || tidalAuth.state === 'polling' || tidalAuth.state === 'showing_code'}
            >
              {tidalAuth.state === 'waiting_for_code' ? 'Starting...'
                : tidalAuth.state === 'showing_code' || tidalAuth.state === 'polling' ? 'Waiting...'
                : tidalStatus?.connected && !tidalStatus?.expired ? 'Reconnect' : 'Connect Tidal'}
            </button>
          </div>

          {/* Device code flow UI */}
          {(tidalAuth.state === 'showing_code' || tidalAuth.state === 'polling') && (
            <div className="bg-slate-800/50 border border-slate-700 rounded-lg px-5 py-4 space-y-3">
              <p className="text-sm text-slate-300">
                Go to{' '}
                <a
                  href={tidalAuth.verification_uri?.startsWith('http') ? tidalAuth.verification_uri : `https://${tidalAuth.verification_uri}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-400 underline hover:text-blue-300"
                >
                  {tidalAuth.verification_uri}
                </a>
              </p>
              {tidalAuth.user_code && (
                <div className="flex items-center gap-3">
                  <span className="text-sm text-slate-400">Enter code:</span>
                  <code className="text-lg font-bold text-slate-100 bg-slate-700 px-3 py-1 rounded tracking-wider">
                    {tidalAuth.user_code}
                  </code>
                </div>
              )}
              <p className="text-xs text-slate-500 animate-pulse">
                Waiting for authorization...
              </p>
            </div>
          )}

          {/* Success */}
          {tidalAuth.state === 'success' && (
            <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-lg px-5 py-3">
              <p className="text-sm text-emerald-400">Tidal authorized successfully.</p>
            </div>
          )}

          {/* Error */}
          {tidalAuth.state === 'error' && (
            <div className="bg-red-500/10 border border-red-500/30 rounded-lg px-5 py-3">
              <p className="text-sm text-red-400">{tidalAuth.error}</p>
            </div>
          )}
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

          {/* Tidal */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className={`w-2.5 h-2.5 rounded-full ${
                tidalStatus?.connected && !tidalStatus?.expired ? 'bg-emerald-400'
                : tidalStatus?.connected && tidalStatus?.expired ? 'bg-amber-400'
                : 'bg-red-400'
              }`} />
              <span className="text-sm text-slate-300">Tidal</span>
            </div>
            <span className="text-xs text-slate-500">
              {tidalStatus?.connected
                ? tidalStatus?.expired ? 'Token expired' : `Connected (${tidalStatus.hours_left}h left)`
                : 'Disconnected'}
            </span>
          </div>

          {/* Tidal Downloader (optional) */}
          {(() => {
            const tidal = dashboard?.services?.find((s: any) => s.name === 'tidal')
            const tidalUrl = settings.tidarr_url || 'http://192.168.1.221:8484'
            return (
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className={`w-2.5 h-2.5 rounded-full ${tidal?.status === 'ok' ? 'bg-emerald-400' : 'bg-slate-600'}`} />
                  <span className="text-sm text-slate-300">Tidal Downloader</span>
                  <span className="text-xs text-slate-600 font-mono">{tidalUrl}</span>
                </div>
                <span className="text-xs text-slate-500">
                  {tidal?.status === 'ok'
                    ? `Connected (${tidal.latency_ms}ms)`
                    : 'Optional'}
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

      {/* Sync Mode */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Sync Mode</h2>
        <div className="flex items-center justify-between">
          <div>
            <p className="text-sm text-slate-300">
              {settings.sync_mode === 'full' ? 'Full Sync' : 'Scan Only'}
            </p>
            <p className="text-xs text-slate-500 mt-1">
              {settings.sync_mode === 'full'
                ? 'Matching, downloading, verifying, and organizing tracks.'
                : 'Only matching against existing library. No downloads.'}
            </p>
          </div>
          <select
            className="select-field"
            value={settings.sync_mode || 'scan'}
            onChange={(e) => updateSetting('sync_mode', e.target.value)}
          >
            <option value="scan">Scan Only</option>
            <option value="full">Full Sync</option>
          </select>
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

      {/* Lexicon Post-Processing */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-2">Lexicon Post-Processing</h2>
        <p className="text-xs text-slate-500 mb-4">
          Actions triggered automatically after a track is synced to Lexicon.
          These use Lexicon&apos;s control API and operate on the UI-selected tracks &mdash;
          they work best when Lexicon is open.
        </p>
        <div className="space-y-3">
          {([
            { key: 'analyze', label: 'Analyze (BPM / Key)', desc: 'Detect BPM, key, and waveform via TrackBrowser_Analyze', disabled: false },
            { key: 'cues', label: 'Cue Point Generator', desc: 'Auto-generate cue points via TrackBrowser_CuePointGenerator', disabled: false },
            { key: 'tags', label: 'Tag Lookup', desc: 'Look up genre/energy tags via TrackBrowser_TagLookup', disabled: false },
            { key: 'cloud', label: 'Cloud Backup Upload', desc: 'Upload to Lexicon Cloud via CloudFileBackup_UploadSelected', disabled: false },
            { key: 'artwork', label: 'Album Art', desc: 'Not yet available via API — must be done manually in Lexicon UI', disabled: true },
          ] as { key: string; label: string; desc: string; disabled: boolean }[]).map(action => {
            const current = (settings.lexicon_post_processing || 'analyze,cues,tags,cloud').split(',').map(s => s.trim())
            const isEnabled = !action.disabled && current.includes(action.key)
            const toggle = () => {
              if (action.disabled) return
              const next = isEnabled
                ? current.filter(a => a !== action.key)
                : [...current, action.key]
              updateSetting('lexicon_post_processing', next.filter(Boolean).join(','))
            }
            return (
              <div key={action.key} className="flex items-center justify-between">
                <div>
                  <p className={`text-sm ${action.disabled ? 'text-slate-600' : 'text-slate-300'}`}>{action.label}</p>
                  <p className="text-xs text-slate-600">{action.desc}</p>
                </div>
                <button
                  type="button"
                  disabled={action.disabled}
                  onClick={toggle}
                  className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                    action.disabled ? 'bg-slate-800 cursor-not-allowed opacity-40'
                    : isEnabled ? 'bg-emerald-500' : 'bg-slate-700'
                  }`}
                >
                  <span className={`inline-block h-4 w-4 rounded-full bg-white transition-transform ${
                    isEnabled ? 'translate-x-6' : 'translate-x-1'
                  }`} />
                </button>
              </div>
            )
          })}
        </div>
        <div className="flex items-center justify-between mt-3 pt-3 border-t border-slate-800">
          <div>
            <p className="text-sm text-slate-300">Enable Post-Processing</p>
            <p className="text-xs text-slate-600">Master toggle &mdash; disabling turns off all actions above</p>
          </div>
          <button
            type="button"
            onClick={() => updateSetting('auto_analyze_enabled', settings.auto_analyze_enabled === '0' ? '1' : '0')}
            className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
              settings.auto_analyze_enabled === '0' ? 'bg-slate-700' : 'bg-emerald-500'
            }`}
          >
            <span className={`inline-block h-4 w-4 rounded-full bg-white transition-transform ${
              settings.auto_analyze_enabled === '0' ? 'translate-x-1' : 'translate-x-6'
            }`} />
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

      {/* Webhook */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Webhook Notifications</h2>
        <div>
          <label className="block text-xs text-slate-500 mb-1">Webhook URL (leave empty to disable)</label>
          <input
            type="url"
            className="input-field w-full"
            placeholder="https://example.com/webhook"
            value={settings.webhook_url || ''}
            onChange={(e) => updateSetting('webhook_url', e.target.value)}
          />
          <p className="text-xs text-slate-600 mt-1">
            Receives POST with track sync events and parity milestones.
          </p>
        </div>
      </div>

      {/* Export */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Export Sync Report</h2>
        <div className="flex items-center gap-3">
          <a
            href="/api/admin/export?format=csv"
            download="sync-report.csv"
            className="btn-secondary text-sm inline-block"
          >
            Export CSV
          </a>
          <a
            href="/api/admin/export?format=json"
            download="sync-report.json"
            className="btn-secondary text-sm inline-block"
          >
            Export JSON
          </a>
          <span className="text-xs text-slate-500 ml-auto">
            {dashboard?.total_tracks ? `${dashboard.total_tracks} tracks` : ''}
          </span>
        </div>
      </div>
    </div>
  )
}
