'use client'

import { useState, useCallback, useRef, useEffect } from 'react'
import { apiFetch, apiUpload } from '../api'
import StatusBadge from '../components/StatusBadge'

interface MissingTrack {
  id: number
  spotify_id: string
  title: string
  artist: string
  album: string
  pipeline_stage: string
  match_status: string
  pipeline_error: string | null
}

export default function UploadPage() {
  const [missingTracks, setMissingTracks] = useState<MissingTrack[]>([])
  const [loadingMissing, setLoadingMissing] = useState(true)
  const [selectedTrack, setSelectedTrack] = useState<MissingTrack | null>(null)
  const [file, setFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadResult, setUploadResult] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [dragOver, setDragOver] = useState(false)
  const [progress, setProgress] = useState(0)
  const [searchFilter, setSearchFilter] = useState('')
  const fileInputRef = useRef<HTMLInputElement>(null)

  const fetchMissing = useCallback(async () => {
    setLoadingMissing(true)
    try {
      // Get tracks that need manual files (match failed or download failed)
      const result = await apiFetch<any>('/tracks?pipeline_stage=error&per_page=200')
      setMissingTracks(result.tracks || [])
    } catch {
      setMissingTracks([])
    } finally {
      setLoadingMissing(false)
    }
  }, [])

  useEffect(() => { fetchMissing() }, [fetchMissing])

  const filteredMissing = missingTracks.filter(t => {
    if (!searchFilter) return true
    const q = searchFilter.toLowerCase()
    return (t.title?.toLowerCase().includes(q) || t.artist?.toLowerCase().includes(q))
  })

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setDragOver(false)
    const f = e.dataTransfer.files[0]
    if (f) setFile(f)
  }

  const handleUpload = async () => {
    if (!selectedTrack || !file) return
    setUploading(true)
    setError(null)
    setUploadResult(null)
    setProgress(0)

    try {
      const formData = new FormData()
      formData.append('file', file)

      const progressInterval = setInterval(() => {
        setProgress(prev => Math.min(prev + 10, 90))
      }, 200)

      const result = await apiUpload(`/uploads/${selectedTrack.id}`, formData)
      clearInterval(progressInterval)
      setProgress(100)
      setUploadResult(result)
      fetchMissing() // Refresh list
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed')
    } finally {
      setUploading(false)
    }
  }

  const reset = () => {
    setFile(null)
    setSelectedTrack(null)
    setUploadResult(null)
    setError(null)
    setProgress(0)
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Manual Upload</h1>
        <p className="text-sm text-slate-500 mt-1">
          {missingTracks.length} tracks need manual files
        </p>
      </div>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded-xl px-5 py-4">
          <p className="text-sm text-red-400">{error}</p>
        </div>
      )}

      {uploadResult ? (
        <div className="card text-center py-12">
          <div className="text-4xl mb-3 text-emerald-400">&#10003;</div>
          <p className="text-lg font-semibold text-slate-200 mb-2">Upload Complete</p>
          <p className="text-sm text-slate-400 mb-6">
            {selectedTrack?.title} by {selectedTrack?.artist}
          </p>
          <button className="btn-primary" onClick={reset}>Upload Another</button>
        </div>
      ) : (
        <div className="grid lg:grid-cols-2 gap-6">
          {/* Left: Missing tracks list */}
          <div className="card p-0 overflow-hidden">
            <div className="px-4 py-3 border-b border-slate-800">
              <h2 className="text-sm font-semibold text-red-400 mb-2">
                Tracks Needing Files ({filteredMissing.length})
              </h2>
              <input
                type="text"
                placeholder="Filter by title or artist..."
                className="input-field text-sm"
                value={searchFilter}
                onChange={(e) => setSearchFilter(e.target.value)}
              />
            </div>
            <div className="max-h-[500px] overflow-y-auto divide-y divide-slate-800/50">
              {loadingMissing ? (
                Array.from({ length: 5 }).map((_, i) => (
                  <div key={i} className="px-4 py-3"><div className="h-10 skeleton rounded" /></div>
                ))
              ) : filteredMissing.length === 0 ? (
                <div className="px-4 py-8 text-center text-sm text-slate-500">
                  {searchFilter ? 'No matches' : 'All tracks have files'}
                </div>
              ) : (
                filteredMissing.map(track => (
                  <button
                    key={track.id}
                    className={`w-full text-left px-4 py-3 hover:bg-slate-800/60 transition-colors ${
                      selectedTrack?.id === track.id ? 'bg-emerald-500/10 border-l-2 border-emerald-500' : ''
                    }`}
                    onClick={() => { setSelectedTrack(track); setFile(null); setUploadResult(null) }}
                  >
                    <div className="flex items-center justify-between">
                      <div className="min-w-0 flex-1">
                        <p className="text-sm text-slate-200 font-medium truncate">{track.title}</p>
                        <p className="text-xs text-slate-500 truncate">{track.artist}</p>
                      </div>
                      <StatusBadge status={track.match_status} />
                    </div>
                    {track.pipeline_error && (
                      <p className="text-xs text-red-400/70 mt-1 truncate">{track.pipeline_error}</p>
                    )}
                  </button>
                ))
              )}
            </div>
          </div>

          {/* Right: Upload area */}
          <div className="card">
            <h2 className="text-sm font-semibold text-slate-300 mb-4">
              {selectedTrack ? (
                <span>Upload file for: <span className="text-emerald-400">{selectedTrack.title}</span></span>
              ) : (
                'Select a track from the list'
              )}
            </h2>

            {selectedTrack && (
              <>
                <div className="bg-slate-800/50 rounded-lg px-4 py-3 mb-4">
                  <p className="text-sm text-slate-200 font-medium">{selectedTrack.title}</p>
                  <p className="text-xs text-slate-400">{selectedTrack.artist} - {selectedTrack.album}</p>
                </div>

                <div
                  className={`border-2 border-dashed rounded-xl p-8 text-center transition-colors cursor-pointer ${
                    dragOver ? 'border-emerald-500 bg-emerald-500/5'
                      : file ? 'border-emerald-500/30 bg-emerald-500/5'
                      : 'border-slate-700 hover:border-slate-600'
                  }`}
                  onDragOver={(e) => { e.preventDefault(); setDragOver(true) }}
                  onDragLeave={() => setDragOver(false)}
                  onDrop={handleDrop}
                  onClick={() => fileInputRef.current?.click()}
                >
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept=".flac,.aiff,.wav,.m4a"
                    className="hidden"
                    onChange={(e) => { if (e.target.files?.[0]) setFile(e.target.files[0]) }}
                  />
                  {file ? (
                    <div>
                      <div className="text-2xl mb-2 text-emerald-400">&#9835;</div>
                      <p className="text-sm text-slate-200 font-medium">{file.name}</p>
                      <p className="text-xs text-slate-500 mt-1">{(file.size / 1024 / 1024).toFixed(1)} MB</p>
                    </div>
                  ) : (
                    <div>
                      <div className="text-2xl mb-2 text-slate-600">&#8593;</div>
                      <p className="text-sm text-slate-400">Drop a lossless file here</p>
                      <p className="text-xs text-slate-600 mt-1">FLAC, AIFF, WAV, or M4A</p>
                    </div>
                  )}
                </div>

                {uploading && (
                  <div className="mt-4">
                    <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                      <div className="h-full bg-emerald-500 rounded-full transition-all duration-300" style={{ width: `${progress}%` }} />
                    </div>
                  </div>
                )}

                <button
                  className="btn-primary w-full mt-4"
                  disabled={!file || uploading}
                  onClick={handleUpload}
                >
                  {uploading ? 'Uploading...' : 'Upload & Verify'}
                </button>
              </>
            )}

            {!selectedTrack && (
              <div className="text-center py-12 text-slate-600">
                <div className="text-3xl mb-2">&#8592;</div>
                <p className="text-sm">Select a track from the list to upload a file</p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
