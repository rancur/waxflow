'use client'

import { useState } from 'react'
import { Track } from '../types'
import StatusBadge from './StatusBadge'

function formatDuration(ms: number): string {
  const minutes = Math.floor(ms / 60_000)
  const seconds = Math.floor((ms % 60_000) / 1000)
  return `${minutes}:${seconds.toString().padStart(2, '0')}`
}

export default function TrackRow({ track }: { track: Track }) {
  const [expanded, setExpanded] = useState(false)

  return (
    <>
      <tr
        className="border-b border-slate-800/50 hover:bg-slate-800/30 cursor-pointer transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <td className="table-cell">
          <div>
            <p className="text-slate-200 font-medium">{track.title}</p>
            <p className="text-xs text-slate-500">{track.album}</p>
          </div>
        </td>
        <td className="table-cell text-slate-400">{track.artist}</td>
        <td className="table-cell">
          <StatusBadge status={track.match_status} />
        </td>
        <td className="table-cell">
          <StatusBadge status={track.download_status || 'pending'} />
        </td>
        <td className="table-cell">
          {track.verified ? (
            <span className="text-emerald-400">&#10003;</span>
          ) : (
            <span className="text-slate-600">&#8212;</span>
          )}
        </td>
        <td className="table-cell">
          {track.in_lexicon ? (
            <span className="text-emerald-400">&#10003;</span>
          ) : (
            <span className="text-slate-600">&#8212;</span>
          )}
        </td>
        <td className="table-cell text-slate-500">
          <svg
            className={`w-4 h-4 transition-transform ${expanded ? 'rotate-180' : ''}`}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
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
                <p className="text-slate-500 text-xs mb-1">Spotify ID</p>
                <p className="text-slate-300 font-mono text-xs">{track.spotify_id}</p>
              </div>
              <div>
                <p className="text-slate-500 text-xs mb-1">Duration</p>
                <p className="text-slate-300">{formatDuration(track.duration_ms)}</p>
              </div>
              <div>
                <p className="text-slate-500 text-xs mb-1">Pipeline Stage</p>
                <StatusBadge status={track.pipeline_stage} />
              </div>
              <div>
                <p className="text-slate-500 text-xs mb-1">Added</p>
                <p className="text-slate-300">{new Date(track.added_at).toLocaleDateString()}</p>
              </div>
              {track.confidence_score !== undefined && (
                <div>
                  <p className="text-slate-500 text-xs mb-1">Match Confidence</p>
                  <p className="text-slate-300">{(track.confidence_score * 100).toFixed(1)}%</p>
                </div>
              )}
              {track.file_path && (
                <div className="col-span-2">
                  <p className="text-slate-500 text-xs mb-1">File Path</p>
                  <p className="text-slate-300 font-mono text-xs truncate">{track.file_path}</p>
                </div>
              )}
              {track.error_message && (
                <div className="col-span-2">
                  <p className="text-red-400 text-xs mb-1">Error</p>
                  <p className="text-red-300 text-xs">{track.error_message}</p>
                </div>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  )
}
