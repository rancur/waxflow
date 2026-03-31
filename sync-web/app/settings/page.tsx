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
  const [lexiconTest, setLexiconTest] = useState<{
    state: 'idle' | 'testing' | 'success' | 'error'
    latency?: number
    error?: string
  }>({ state: 'idle' })
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)
  const [showAllBackups, setShowAllBackups] = useState(false)

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
            const fresh = await apiFetch<any>('/tidal/status')
            setTidalStatus(fresh)
            setTimeout(() => setTidalAuth({ state: 'idle' }), 5000)
          } else if (result.status === 'error') {
            clearInterval(poll)
            setTidalAuth({ state: 'error', error: result.error })
          }
        } catch {
          clearInterval(poll)
          setTidalAuth({ state: 'error', error: 'Polling failed' })
        }
      }, pollInterval)
    } catch (err) {
      setTidalAuth({ state: 'error', error: err instanceof Error ? err.message : 'Failed to start auth' })
    }
  }

  const handleTestLexicon = async () => {
    setLexiconTest({ state: 'testing' })
    const start = Date.now()
    try {
      // Save current settings first so the backend uses the updated URL
      await apiFetch('/settings', {
        method: 'PATCH',
        body: JSON.stringify({ settings }),
      })
      const data = await apiFetch<any>('/lexicon/status')
      const latency = Date.now() - start
      if (data.connected) {
        setLexiconTest({ state: 'success', latency })
      } else {
        setLexiconTest({ state: 'error', error: 'Lexicon API unreachable at ' + (data.base_url || 'configured URL') })
      }
    } catch (err) {
      setLexiconTest({ state: 'error', error: err instanceof Error ? err.message : 'Connection failed' })
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

  // Helpers
  const lexiconService = dashboard?.services?.find((s: any) => s.name === 'lexicon')
  const visibleBackups = showAllBackups ? backups : backups.slice(0, 3)

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
        <p className="text-sm text-slate-500 mt-1">Connections, sync configuration, and maintenance</p>
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

      {/* ================================================================ */}
      {/* CONNECTION CARDS (grid of 3)                                      */}
      {/* ================================================================ */}

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {/* Spotify Connection */}
        <div className="card">
          <h2 className="text-sm font-semibold text-slate-300 mb-3">Spotify</h2>
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <div className={`w-2.5 h-2.5 rounded-full ${spotifyStatus?.authenticated ? 'bg-emerald-400' : 'bg-red-400'}`} />
              <span className="text-sm text-slate-300">
                {spotifyStatus?.authenticated ? 'Connected' : 'Not connected'}
              </span>
            </div>
            {spotifyStatus?.last_poll && (
              <p className="text-xs text-slate-500">
                Last poll: {new Date(spotifyStatus.last_poll).toLocaleString()}
              </p>
            )}
            <button className="btn-primary text-sm w-full" onClick={handleConnectSpotify}>
              {spotifyStatus?.authenticated ? 'Reconnect' : 'Connect'}
            </button>
          </div>
        </div>

        {/* Tidal Connection */}
        <div className="card">
          <h2 className="text-sm font-semibold text-slate-300 mb-3">Tidal</h2>
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <div className={`w-2.5 h-2.5 rounded-full ${
                tidalStatus?.connected
                  ? tidalStatus?.expired ? 'bg-amber-400' : 'bg-emerald-400'
                  : 'bg-red-400'
              }`} />
              <span className="text-sm text-slate-300">
                {tidalStatus?.connected
                  ? tidalStatus?.expired ? 'Token expired' : 'Connected'
                  : 'Not connected'}
              </span>
            </div>
            {tidalStatus?.connected && !tidalStatus?.expired && (
              <p className="text-xs text-slate-500">
                Expires in {tidalStatus.hours_left}h
              </p>
            )}
            <div>
              <label className="block text-xs text-slate-500 mb-1">Download Quality</label>
              <select
                className="select-field w-full"
                value={settings.tidal_download_quality || 'max'}
                onChange={(e) => updateSetting('tidal_download_quality', e.target.value)}
              >
                <option value="low">Low</option>
                <option value="normal">Normal</option>
                <option value="high">High</option>
                <option value="max">Max (Master/FLAC)</option>
              </select>
            </div>
            <button
              className="btn-primary text-sm w-full"
              onClick={handleConnectTidal}
              disabled={tidalAuth.state === 'waiting_for_code' || tidalAuth.state === 'polling' || tidalAuth.state === 'showing_code'}
            >
              {tidalAuth.state === 'waiting_for_code' ? 'Starting...'
                : tidalAuth.state === 'showing_code' || tidalAuth.state === 'polling' ? 'Waiting...'
                : tidalStatus?.connected && !tidalStatus?.expired ? 'Reconnect' : 'Connect'}
            </button>

            {/* Device code flow UI */}
            {(tidalAuth.state === 'showing_code' || tidalAuth.state === 'polling') && (
              <div className="bg-slate-800/50 border border-slate-700 rounded-lg px-4 py-3 space-y-2">
                <p className="text-xs text-slate-300">
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
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-slate-400">Code:</span>
                    <code className="text-sm font-bold text-slate-100 bg-slate-700 px-2 py-0.5 rounded tracking-wider">
                      {tidalAuth.user_code}
                    </code>
                  </div>
                )}
                <p className="text-xs text-slate-500 animate-pulse">Waiting for authorization...</p>
              </div>
            )}
            {tidalAuth.state === 'success' && (
              <div className="bg-emerald-500/10 border border-emerald-500/30 rounded-lg px-4 py-2">
                <p className="text-xs text-emerald-400">Authorized.</p>
              </div>
            )}
            {tidalAuth.state === 'error' && (
              <div className="bg-red-500/10 border border-red-500/30 rounded-lg px-4 py-2">
                <p className="text-xs text-red-400">{tidalAuth.error}</p>
              </div>
            )}
          </div>
        </div>

        {/* Lexicon Connection */}
        <div className="card">
          <h2 className="text-sm font-semibold text-slate-300 mb-3">Lexicon</h2>
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <div className={`w-2.5 h-2.5 rounded-full ${
                lexiconTest.state === 'success' ? 'bg-emerald-400'
                : lexiconTest.state === 'error' ? 'bg-red-400'
                : lexiconService?.status === 'ok' ? 'bg-emerald-400'
                : 'bg-slate-500'
              }`} />
              <span className="text-sm text-slate-300">
                {lexiconTest.state === 'success'
                  ? `Connected (${lexiconTest.latency}ms)`
                  : lexiconTest.state === 'error'
                    ? 'Connection failed'
                    : lexiconService?.status === 'ok'
                      ? `Connected (${lexiconService.latency_ms}ms)`
                      : 'Unknown'}
              </span>
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1">API URL</label>
              <input
                type="text"
                className="input-field w-full"
                placeholder="http://192.168.1.116:48624"
                value={settings.lexicon_api_url || ''}
                onChange={(e) => updateSetting('lexicon_api_url', e.target.value)}
              />
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1">Library Path</label>
              <input
                type="text"
                className="input-field w-full"
                placeholder="/Volumes/music/Database"
                value={settings.lexicon_library_path || ''}
                onChange={(e) => updateSetting('lexicon_library_path', e.target.value)}
              />
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1">Input Path</label>
              <input
                type="text"
                className="input-field w-full"
                placeholder="/Volumes/music/Input"
                value={settings.lexicon_input_path || ''}
                onChange={(e) => updateSetting('lexicon_input_path', e.target.value)}
              />
            </div>
            <p className="text-xs text-slate-600">For local setups, use http://localhost:48624</p>
            <button
              className="btn-secondary text-sm w-full"
              onClick={handleTestLexicon}
              disabled={lexiconTest.state === 'testing'}
            >
              {lexiconTest.state === 'testing' ? 'Testing...' : 'Test Connection'}
            </button>
            {lexiconTest.state === 'error' && (
              <p className="text-xs text-red-400">{lexiconTest.error}</p>
            )}
          </div>
        </div>
      </div>

      {/* ================================================================ */}
      {/* SYNC SETTINGS                                                     */}
      {/* ================================================================ */}

      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Sync Settings</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          <div>
            <label className="block text-xs text-slate-500 mb-1">Sync Mode</label>
            <select
              className="select-field w-full"
              value={settings.sync_mode || 'scan'}
              onChange={(e) => updateSetting('sync_mode', e.target.value)}
            >
              <option value="scan">Scan Only</option>
              <option value="full">Full Sync</option>
            </select>
            <p className="text-xs text-slate-600 mt-1">
              {settings.sync_mode === 'full'
                ? 'Match, download, verify, and organize.'
                : 'Match against existing library only.'}
            </p>
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Spotify Poll Interval (sec)</label>
            <input
              type="number"
              min="30"
              className="input-field w-full"
              value={settings.spotify_poll_interval_seconds || '300'}
              onChange={(e) => updateSetting('spotify_poll_interval_seconds', e.target.value)}
            />
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Max Concurrent Downloads</label>
            <input
              type="number"
              min="1"
              max="10"
              className="input-field w-full"
              value={settings.max_concurrent_downloads || '2'}
              onChange={(e) => updateSetting('max_concurrent_downloads', e.target.value)}
            />
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Retry Search Interval (hours)</label>
            <input
              type="number"
              min="1"
              className="input-field w-full"
              value={String(Math.round(Number(settings.retry_search_interval_seconds || '43200') / 3600))}
              onChange={(e) => updateSetting('retry_search_interval_seconds', String(Number(e.target.value) * 3600))}
            />
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">Fingerprint Threshold (0-1)</label>
            <input
              type="number"
              step="0.01"
              min="0"
              max="1"
              className="input-field w-full"
              value={settings.fingerprint_min_score || '0.85'}
              onChange={(e) => updateSetting('fingerprint_min_score', e.target.value)}
            />
          </div>
          <div>
            <label className="block text-xs text-slate-500 mb-1">SynologyDrive Sync Delay (sec)</label>
            <input
              type="number"
              min="0"
              className="input-field w-full"
              value={settings.synology_sync_delay_seconds || '3'}
              onChange={(e) => updateSetting('synology_sync_delay_seconds', e.target.value)}
            />
          </div>
        </div>

        {/* Advanced path settings (collapsible) */}
        <details className="mt-4 pt-4 border-t border-slate-800">
          <summary className="text-xs text-slate-500 cursor-pointer hover:text-slate-400">
            Advanced: Container paths &amp; legacy prefixes
          </summary>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-3">
            <div>
              <label className="block text-xs text-slate-500 mb-1">Music Library Path (container)</label>
              <input
                type="text"
                className="input-field w-full"
                placeholder="/music"
                value={settings.music_library_path || '/music'}
                onChange={(e) => updateSetting('music_library_path', e.target.value)}
              />
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1">Downloads Path (container)</label>
              <input
                type="text"
                className="input-field w-full"
                placeholder="/downloads"
                value={settings.downloads_path || '/downloads'}
                onChange={(e) => updateSetting('downloads_path', e.target.value)}
              />
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1">Tidarr URL</label>
              <input
                type="text"
                className="input-field w-full"
                placeholder="http://tidarr:8484"
                value={settings.tidarr_url || ''}
                onChange={(e) => updateSetting('tidarr_url', e.target.value)}
              />
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1">Plex UID / GID</label>
              <div className="flex gap-2">
                <input
                  type="number"
                  className="input-field w-full"
                  placeholder="UID"
                  value={settings.plex_uid || '297536'}
                  onChange={(e) => updateSetting('plex_uid', e.target.value)}
                />
                <input
                  type="number"
                  className="input-field w-full"
                  placeholder="GID"
                  value={settings.plex_gid || '297536'}
                  onChange={(e) => updateSetting('plex_gid', e.target.value)}
                />
              </div>
            </div>
            <div className="md:col-span-2">
              <label className="block text-xs text-slate-500 mb-1">Legacy Path Prefixes</label>
              <input
                type="text"
                className="input-field w-full"
                placeholder="/Volumes/Macintosh HD/Users/user/Music/Database/,/Users/user/Music/Database/"
                value={settings.lexicon_legacy_path_prefixes || ''}
                onChange={(e) => updateSetting('lexicon_legacy_path_prefixes', e.target.value)}
              />
              <p className="text-xs text-slate-600 mt-1">
                Comma-separated old path prefixes for matching tracks imported under a previous layout.
              </p>
            </div>
          </div>
        </details>
      </div>

      {/* ================================================================ */}
      {/* LEXICON POST-PROCESSING                                           */}
      {/* ================================================================ */}

      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-2">Lexicon Post-Processing</h2>
        <p className="text-xs text-slate-500 mb-4">
          Triggers after each sync batch. Keep Lexicon open for best results.
        </p>
        <div className="space-y-3">
          {([
            { key: 'analyze', label: 'Analyze (BPM / Key)', disabled: false },
            { key: 'cues', label: 'Cue Point Generator', disabled: false },
            { key: 'tags', label: 'Tag Lookup', disabled: false },
            { key: 'cloud', label: 'Cloud Backup Upload', disabled: false },
          ] as { key: string; label: string; disabled: boolean }[]).map(action => {
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
                <span className={`text-sm ${action.disabled ? 'text-slate-600' : 'text-slate-300'}`}>{action.label}</span>
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
            <p className="text-sm text-slate-300">Backup Before Sync</p>
            <p className="text-xs text-slate-600">Create a Lexicon backup before each sync run</p>
          </div>
          <button
            type="button"
            onClick={() => updateSetting('lexicon_backup_before_sync', settings.lexicon_backup_before_sync === '0' ? '1' : '0')}
            className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
              settings.lexicon_backup_before_sync === '0' ? 'bg-slate-700' : 'bg-emerald-500'
            }`}
          >
            <span className={`inline-block h-4 w-4 rounded-full bg-white transition-transform ${
              settings.lexicon_backup_before_sync === '0' ? 'translate-x-1' : 'translate-x-6'
            }`} />
          </button>
        </div>
        <div className="flex items-center justify-between mt-3 pt-3 border-t border-slate-800">
          <div>
            <p className="text-sm text-slate-300">Auto-Analyze After Sync</p>
            <p className="text-xs text-slate-600">Run BPM/key detection after each track is organized</p>
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

      {/* ================================================================ */}
      {/* DATABASE & BACKUP                                                 */}
      {/* ================================================================ */}

      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold text-slate-300">Database &amp; Backup</h2>
          <button className="btn-secondary text-sm" onClick={handleCreateBackup}>Create Backup</button>
        </div>
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-xs text-slate-500">Database Status</span>
            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 rounded-full ${health?.database === 'ok' ? 'bg-emerald-400' : 'bg-red-400'}`} />
              <span className="text-xs text-slate-400">{health?.database === 'ok' ? 'OK' : health?.database || 'Unknown'}</span>
            </div>
          </div>
          {backups.length > 0 && (
            <div className="flex items-center justify-between">
              <span className="text-xs text-slate-500">Last Backup</span>
              <span className="text-xs text-slate-400">{backups[0]?.created_at || 'N/A'}</span>
            </div>
          )}
        </div>
        {Array.isArray(backups) && backups.length > 0 && (
          <div className="mt-4 pt-4 border-t border-slate-800">
            <p className="text-xs text-slate-500 mb-2">Recent Backups</p>
            <div className="space-y-2">
              {visibleBackups.map((backup: any, i: number) => (
                <div key={backup.id || i} className="flex items-center justify-between">
                  <span className="text-xs text-slate-400 font-mono truncate max-w-[70%]">
                    {backup.backup_path || backup.filename}
                  </span>
                  <span className="text-xs text-slate-600">{backup.created_at}</span>
                </div>
              ))}
            </div>
            {backups.length > 3 && (
              <button
                className="text-xs text-blue-400 hover:text-blue-300 mt-2"
                onClick={() => setShowAllBackups(!showAllBackups)}
              >
                {showAllBackups ? 'Show fewer' : `View all ${backups.length} backups`}
              </button>
            )}
          </div>
        )}
      </div>

      {/* ================================================================ */}
      {/* NOTIFICATIONS                                                     */}
      {/* ================================================================ */}

      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Notifications</h2>
        <div>
          <label className="block text-xs text-slate-500 mb-1">Webhook URL</label>
          <input
            type="url"
            className="input-field w-full"
            placeholder="https://example.com/webhook"
            value={settings.webhook_url || ''}
            onChange={(e) => updateSetting('webhook_url', e.target.value)}
          />
          <p className="text-xs text-slate-600 mt-1">
            Receives POST with track sync events. Leave empty to disable.
          </p>
        </div>
      </div>

      {/* ================================================================ */}
      {/* GLOBAL SAVE                                                       */}
      {/* ================================================================ */}

      <div className="flex justify-end">
        <button className="btn-primary text-sm px-6" disabled={saving} onClick={handleSave}>
          {saving ? 'Saving...' : 'Save All Settings'}
        </button>
      </div>

      {/* ================================================================ */}
      {/* EXPORT & VERSION                                                  */}
      {/* ================================================================ */}

      <div className="card">
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
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
            {dashboard?.total_tracks && (
              <span className="text-xs text-slate-500">{dashboard.total_tracks} tracks</span>
            )}
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-slate-500 font-mono">
              {version?.version || '?'}{version?.git_sha ? ` (${version.git_sha})` : ''}
            </span>
            <button
              className="btn-secondary text-sm"
              onClick={() => apiFetch('/admin/update', { method: 'POST' }).then(() => {
                setSuccess('Update requested')
                setTimeout(() => setSuccess(null), 3000)
              }).catch(() => setError('Update request failed'))}
            >
              Check for Updates
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
