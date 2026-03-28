'use client'

import { useEffect, useState, useCallback } from 'react'
import { apiFetch } from '../api'
import { AppSettings, LexiconBackup, VersionInfo } from '../types'

export default function SettingsPage() {
  const [settings, setSettings] = useState<AppSettings | null>(null)
  const [backups, setBackups] = useState<LexiconBackup[]>([])
  const [version, setVersion] = useState<VersionInfo | null>(null)
  const [spotifyConnected, setSpotifyConnected] = useState(false)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [creatingBackup, setCreatingBackup] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)

  const fetchAll = useCallback(async () => {
    try {
      const [settingsRes, backupsRes, versionRes] = await Promise.allSettled([
        apiFetch<AppSettings>('/settings'),
        apiFetch<LexiconBackup[]>('/lexicon/backups'),
        apiFetch<VersionInfo>('/admin/version'),
      ])

      if (settingsRes.status === 'fulfilled') {
        setSettings(settingsRes.value)
        setSpotifyConnected(true)
      }
      if (backupsRes.status === 'fulfilled') setBackups(backupsRes.value)
      if (versionRes.status === 'fulfilled') setVersion(versionRes.value)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load settings')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchAll()
  }, [fetchAll])

  const handleSave = async () => {
    if (!settings) return
    setSaving(true)
    setError(null)
    setSuccess(null)
    try {
      await apiFetch('/settings', {
        method: 'PUT',
        body: JSON.stringify(settings),
      })
      setSuccess('Settings saved')
      setTimeout(() => setSuccess(null), 3000)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  const handleCreateBackup = async () => {
    setCreatingBackup(true)
    try {
      await apiFetch('/lexicon/backups', { method: 'POST' })
      const updated = await apiFetch<LexiconBackup[]>('/lexicon/backups')
      setBackups(updated)
      setSuccess('Backup created')
      setTimeout(() => setSuccess(null), 3000)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Backup failed')
    } finally {
      setCreatingBackup(false)
    }
  }

  const handleRestore = async (id: string) => {
    if (!confirm('Restore this backup? This will overwrite the current Lexicon database.')) return
    try {
      await apiFetch(`/lexicon/backups/${id}/restore`, { method: 'POST' })
      setSuccess('Backup restored')
      setTimeout(() => setSuccess(null), 3000)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Restore failed')
    }
  }

  const handleConnectSpotify = () => {
    window.open('/api/auth/spotify', '_blank')
  }

  const formatBytes = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
    return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  }

  if (loading) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-100">Settings</h1>
        </div>
        <div className="space-y-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="card">
              <div className="h-24 skeleton rounded" />
            </div>
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
            <div className={`w-3 h-3 rounded-full ${spotifyConnected ? 'bg-emerald-400' : 'bg-red-400'}`} />
            <span className="text-sm text-slate-300">
              {spotifyConnected ? 'Connected' : 'Not connected'}
            </span>
          </div>
          <button className="btn-primary text-sm" onClick={handleConnectSpotify}>
            {spotifyConnected ? 'Reconnect Spotify' : 'Connect Spotify'}
          </button>
        </div>
      </div>

      {/* Configuration */}
      {settings && (
        <div className="card">
          <h2 className="text-sm font-semibold text-slate-300 mb-4">Configuration</h2>
          <div className="grid md:grid-cols-2 gap-4">
            <div>
              <label className="block text-xs text-slate-500 mb-1">Poll Interval (seconds)</label>
              <input
                type="number"
                className="input-field"
                value={settings.poll_interval_seconds}
                onChange={(e) => setSettings({ ...settings, poll_interval_seconds: parseInt(e.target.value) || 60 })}
              />
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1">Fingerprint Threshold</label>
              <input
                type="number"
                step="0.01"
                className="input-field"
                value={settings.fingerprint_threshold}
                onChange={(e) => setSettings({ ...settings, fingerprint_threshold: parseFloat(e.target.value) || 0.8 })}
              />
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1">Download Quality</label>
              <select
                className="select-field w-full"
                value={settings.download_quality}
                onChange={(e) => setSettings({ ...settings, download_quality: e.target.value })}
              >
                <option value="flac">FLAC (Lossless)</option>
                <option value="320">MP3 320kbps</option>
                <option value="256">MP3 256kbps</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-slate-500 mb-1">Max Concurrent Downloads</label>
              <input
                type="number"
                className="input-field"
                value={settings.max_concurrent_downloads}
                onChange={(e) => setSettings({ ...settings, max_concurrent_downloads: parseInt(e.target.value) || 3 })}
              />
            </div>
            <div className="flex items-center gap-3">
              <button
                className={`relative w-10 h-5 rounded-full transition-colors ${
                  settings.auto_download ? 'bg-emerald-600' : 'bg-slate-700'
                }`}
                onClick={() => setSettings({ ...settings, auto_download: !settings.auto_download })}
              >
                <span
                  className={`absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white transition-transform ${
                    settings.auto_download ? 'translate-x-5' : ''
                  }`}
                />
              </button>
              <span className="text-sm text-slate-300">Auto-download</span>
            </div>
            <div className="flex items-center gap-3">
              <button
                className={`relative w-10 h-5 rounded-full transition-colors ${
                  settings.auto_verify ? 'bg-emerald-600' : 'bg-slate-700'
                }`}
                onClick={() => setSettings({ ...settings, auto_verify: !settings.auto_verify })}
              >
                <span
                  className={`absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white transition-transform ${
                    settings.auto_verify ? 'translate-x-5' : ''
                  }`}
                />
              </button>
              <span className="text-sm text-slate-300">Auto-verify</span>
            </div>
          </div>
          <div className="mt-4 pt-4 border-t border-slate-800 flex justify-end">
            <button className="btn-primary text-sm" disabled={saving} onClick={handleSave}>
              {saving ? 'Saving...' : 'Save Settings'}
            </button>
          </div>
        </div>
      )}

      {/* Lexicon Backups */}
      <div className="card">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold text-slate-300">Lexicon Backups</h2>
          <button
            className="btn-secondary text-sm"
            disabled={creatingBackup}
            onClick={handleCreateBackup}
          >
            {creatingBackup ? 'Creating...' : 'Create Backup'}
          </button>
        </div>
        {backups.length === 0 ? (
          <p className="text-sm text-slate-500 text-center py-6">No backups yet</p>
        ) : (
          <div className="divide-y divide-slate-800">
            {backups.map((backup) => (
              <div key={backup.id} className="flex items-center justify-between py-3">
                <div>
                  <p className="text-sm text-slate-300">{backup.filename}</p>
                  <p className="text-xs text-slate-500">
                    {new Date(backup.created_at).toLocaleString()} - {formatBytes(backup.size_bytes)}
                  </p>
                </div>
                <button
                  className="text-xs text-amber-400 hover:text-amber-300 transition-colors"
                  onClick={() => handleRestore(backup.id)}
                >
                  Restore
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Version */}
      <div className="card">
        <h2 className="text-sm font-semibold text-slate-300 mb-4">Version</h2>
        <div className="flex items-center justify-between">
          <div className="text-sm text-slate-400">
            {version ? (
              <>
                <span className="text-slate-300 font-mono">{version.version}</span>
                <span className="mx-2 text-slate-600">|</span>
                <span className="font-mono text-xs">{version.git_sha.slice(0, 7)}</span>
                <span className="mx-2 text-slate-600">|</span>
                <span className="text-xs">{new Date(version.build_date).toLocaleDateString()}</span>
              </>
            ) : (
              <span className="text-slate-500">Unknown</span>
            )}
          </div>
          <button
            className="btn-secondary text-sm"
            onClick={() => apiFetch('/admin/update-check', { method: 'POST' })}
          >
            Check for Updates
          </button>
        </div>
      </div>
    </div>
  )
}
