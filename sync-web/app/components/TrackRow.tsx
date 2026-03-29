'use client'

import { useState } from 'react'
import StatusBadge from './StatusBadge'

interface TrackData {
  id: number
  spotify_id: string
  title: string | null
  artist: string | null
  album: string | null
  duration_ms: number | null
  spotify_added_at: string | null
  pipeline_stage: string
  match_status: string
  match_confidence: number | null
  download_status: string
  download_error: string | null
  verify_status: string
  verify_codec: string | null
  lexicon_status: string
  file_path: string | null
  pipeline_error: string | null
  isrc: string | null
  tidal_id: string | null
}

function formatDuration(ms: number): string {
  const minutes = Math.floor(ms / 60_000)
  const seconds = Math.floor((ms % 60_000) / 1000)
  return `${minutes}:${seconds.toString().padStart(2, '0')}`
}

export default function TrackRow({ track }: { track: TrackData }) {
  const [expanded, setExpanded] = useState(false)

  return (
    <>
      <tr
        className="border-b border-slate-800/50 hover:bg-slate-800/30 cursor-pointer transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <td className="table-cell">
          <div>
            <p className="text-slate-200 font-medium">{track.title || 'Unknown'}</p>
            <p className="text-xs text-slate-500">{track.album || ''}</p>
          </div>
        </td>
        <td className="table-cell text-slate-400">{track.artist || 'Unknown'}</td>
        <td className="table-cell">
          <StatusBadge status={track.match_status} />
        </td>
        <td className="table-cell">
          <StatusBadge status={track.download_status || 'pending'} />
        </td>
        <td className="table-cell">
          {track.verify_status === 'pass' ? (
            <span className="text-emerald-400">&#10003;</span>
          ) : track.verify_status === 'fail' ? (
            <span className="text-red-400">&#10007;</span>
          ) : (
            <span className="text-slate-600">&#8212;</span>
          )}
        </td>
        <td className="table-cell">
          {track.lexicon_status === 'synced' ? (
            <span className="text-emerald-400">&#10003;</span>
          ) : track.lexicon_status === 'error' ? (
            <span className="text-red-400">&#10007;</span>
          ) : (
            <span className="text-slate-600">&#8212;</span>
          )}
        </td>
        <td className="table-cell text-slate-500">
          <svg
            className={`w-4 h-4 transition-transform ${expanded ? 'rotate-180' : ''}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
          </svg>
        </td>
      </tr>
      {expanded && (
        <tr className="border-b border-slate-800/50">
          <td colSpan={7} className="px-4 py-4 bg-slate-800/20">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
              <div>
                <p className="text-slate-500 text-xs mb-1">Pipeline Stage</p>
                <StatusBadge status={track.pipeline_stage} />
              </div>
              <div>
                <p className="text-slate-500 text-xs mb-1">Duration</p>
                <p className="text-slate-300">{track.duration_ms ? formatDuration(track.duration_ms) : '—'}</p>
              </div>
              <div>
                <p className="text-slate-500 text-xs mb-1">ISRC</p>
                <p className="text-slate-300 font-mono text-xs">{track.isrc || '—'}</p>
              </div>
              <div>
                <p className="text-slate-500 text-xs mb-1">Added to Spotify</p>
                <p className="text-slate-300">{track.spotify_added_at ? new Date(track.spotify_added_at).toLocaleDateString() : '—'}</p>
              </div>
              {track.match_confidence != null && (
                <div>
                  <p className="text-slate-500 text-xs mb-1">Match Confidence</p>
                  <p className="text-slate-300">{(track.match_confidence * 100).toFixed(1)}%</p>
                </div>
              )}
              {track.tidal_id && (
                <div>
                  <p className="text-slate-500 text-xs mb-1">Tidal ID</p>
                  <p className="text-slate-300 font-mono text-xs">{track.tidal_id}</p>
                </div>
              )}
              {track.verify_codec && (
                <div>
                  <p className="text-slate-500 text-xs mb-1">Codec</p>
                  <p className="text-slate-300 uppercase">{track.verify_codec}</p>
                </div>
              )}
              {track.file_path && (
                <div className="col-span-2">
                  <p className="text-slate-500 text-xs mb-1">File Path</p>
                  <p className="text-slate-300 font-mono text-xs truncate">{track.file_path}</p>
                </div>
              )}
              {(track.pipeline_error || track.download_error) && (
                <div className="col-span-2">
                  <p className="text-red-400 text-xs mb-1">Error</p>
                  <p className="text-red-300 text-xs">{track.pipeline_error || track.download_error}</p>
                </div>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  )
}
