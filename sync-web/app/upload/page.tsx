'use client'

import { useState, useCallback, useRef } from 'react'
import { apiFetch, apiUpload } from '../api'
import { Track } from '../types'

export default function UploadPage() {
  const [search, setSearch] = useState('')
  const [searchResults, setSearchResults] = useState<Track[]>([])
  const [selectedTrack, setSelectedTrack] = useState<Track | null>(null)
  const [file, setFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadResult, setUploadResult] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [dragOver, setDragOver] = useState(false)
  const [progress, setProgress] = useState(0)
  const fileInputRef = useRef<HTMLInputElement>(null)

  // Search tracks
  const searchTracks = useCallback(async (q: string) => {
    if (q.length < 2) {
      setSearchResults([])
      return
    }
    try {
      const params = new URLSearchParams({ q, per_page: '10' })
      const result = await apiFetch<{ tracks: Track[] }>(`/tracks?${params}`)
      setSearchResults(result.tracks)
    } catch {
      setSearchResults([])
    }
  }, [])

  // Debounced search
  const [searchTimer, setSearchTimer] = useState<NodeJS.Timeout | null>(null)
  const handleSearchInput = (val: string) => {
    setSearch(val)
    if (searchTimer) clearTimeout(searchTimer)
    setSearchTimer(setTimeout(() => searchTracks(val), 300))
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setDragOver(false)
    const f = e.dataTransfer.files[0]
    if (f) setFile(f)
  }

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0]
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

      // Simulated progress (real progress would need XMLHttpRequest)
      const progressInterval = setInterval(() => {
        setProgress((prev) => Math.min(prev + 10, 90))
      }, 200)

      const result = await apiUpload(`/uploads/${selectedTrack.id}`, formData)
      clearInterval(progressInterval)
      setProgress(100)
      setUploadResult(result)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed')
    } finally {
      setUploading(false)
    }
  }

  const reset = () => {
    setFile(null)
    setSelectedTrack(null)
    setSearch('')
    setSearchResults([])
    setUploadResult(null)
    setError(null)
    setProgress(0)
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-100">Manual Upload</h1>
        <p className="text-sm text-slate-500 mt-1">Upload a FLAC file for a specific track</p>
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
          {uploadResult.verified !== undefined && (
            <div className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-slate-800 text-sm">
              <span className={uploadResult.verified ? 'text-emerald-400' : 'text-amber-400'}>
                {uploadResult.verified ? 'Verification passed' : 'Verification pending'}
              </span>
            </div>
          )}
          <div className="mt-6">
            <button className="btn-primary" onClick={reset}>Upload Another</button>
          </div>
        </div>
      ) : (
        <div className="grid lg:grid-cols-2 gap-6">
          {/* Step 1: Select Track */}
          <div className="card">
            <h2 className="text-sm font-semibold text-slate-300 mb-4">
              <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-slate-800 text-xs text-slate-400 mr-2">1</span>
              Select Track
            </h2>
            <input
              type="text"
              placeholder="Search for a track..."
              className="input-field mb-3"
              value={search}
              onChange={(e) => handleSearchInput(e.target.value)}
            />
            {searchResults.length > 0 && !selectedTrack && (
              <div className="max-h-64 overflow-y-auto rounded-lg border border-slate-700 divide-y divide-slate-800">
                {searchResults.map((track) => (
                  <button
                    key={track.id}
                    className="w-full text-left px-4 py-3 hover:bg-slate-800/60 transition-colors"
                    onClick={() => {
                      setSelectedTrack(track)
                      setSearch(`${track.title} - ${track.artist}`)
                      setSearchResults([])
                    }}
                  >
                    <p className="text-sm text-slate-200">{track.title}</p>
                    <p className="text-xs text-slate-500">{track.artist} - {track.album}</p>
                  </button>
                ))}
              </div>
            )}
            {selectedTrack && (
              <div className="flex items-center justify-between bg-emerald-500/10 border border-emerald-500/20 rounded-lg px-4 py-3">
                <div>
                  <p className="text-sm text-slate-200 font-medium">{selectedTrack.title}</p>
                  <p className="text-xs text-slate-400">{selectedTrack.artist}</p>
                </div>
                <button
                  className="text-xs text-slate-500 hover:text-slate-300"
                  onClick={() => { setSelectedTrack(null); setSearch('') }}
                >
                  Change
                </button>
              </div>
            )}
          </div>

          {/* Step 2: Upload File */}
          <div className="card">
            <h2 className="text-sm font-semibold text-slate-300 mb-4">
              <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-slate-800 text-xs text-slate-400 mr-2">2</span>
              Upload FLAC File
            </h2>
            <div
              className={`border-2 border-dashed rounded-xl p-8 text-center transition-colors cursor-pointer ${
                dragOver
                  ? 'border-emerald-500 bg-emerald-500/5'
                  : file
                  ? 'border-emerald-500/30 bg-emerald-500/5'
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
                accept=".flac"
                className="hidden"
                onChange={handleFileSelect}
              />
              {file ? (
                <div>
                  <div className="text-2xl mb-2 text-emerald-400">&#9835;</div>
                  <p className="text-sm text-slate-200 font-medium">{file.name}</p>
                  <p className="text-xs text-slate-500 mt-1">
                    {(file.size / 1024 / 1024).toFixed(1)} MB
                  </p>
                </div>
              ) : (
                <div>
                  <div className="text-2xl mb-2 text-slate-600">&#8593;</div>
                  <p className="text-sm text-slate-400">Drop a FLAC file here</p>
                  <p className="text-xs text-slate-600 mt-1">or click to browse</p>
                </div>
              )}
            </div>

            {/* Progress */}
            {uploading && (
              <div className="mt-4">
                <div className="h-2 bg-slate-800 rounded-full overflow-hidden">
                  <div
                    className="h-full bg-emerald-500 rounded-full transition-all duration-300"
                    style={{ width: `${progress}%` }}
                  />
                </div>
                <p className="text-xs text-slate-500 mt-2 text-center">{progress}%</p>
              </div>
            )}

            {/* Upload button */}
            <button
              className="btn-primary w-full mt-4"
              disabled={!selectedTrack || !file || uploading}
              onClick={handleUpload}
            >
              {uploading ? 'Uploading...' : 'Upload'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
