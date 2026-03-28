'use client'

import { ActivityEvent } from '../types'

const typeIcons: Record<string, { icon: string; color: string }> = {
  download: { icon: '↓', color: 'text-blue-400 bg-blue-500/15' },
  match: { icon: '↔', color: 'text-purple-400 bg-purple-500/15' },
  verify: { icon: '✓', color: 'text-emerald-400 bg-emerald-500/15' },
  error: { icon: '!', color: 'text-red-400 bg-red-500/15' },
  sync: { icon: '⟳', color: 'text-amber-400 bg-amber-500/15' },
  upload: { icon: '↑', color: 'text-cyan-400 bg-cyan-500/15' },
}

function formatTime(ts: string): string {
  const d = new Date(ts)
  const now = new Date()
  const diff = now.getTime() - d.getTime()
  if (diff < 60_000) return 'just now'
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
  return d.toLocaleDateString()
}

export default function ActivityFeed({
  events,
  loading,
}: {
  events?: ActivityEvent[]
  loading?: boolean
}) {
  if (loading) {
    return (
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
    )
  }

  if (!events || events.length === 0) {
    return (
      <div className="text-center py-8 text-slate-500 text-sm">
        No recent activity
      </div>
    )
  }

  return (
    <div className="space-y-2 max-h-96 overflow-y-auto pr-1">
      {events.map((event) => {
        const config = typeIcons[event.type] || typeIcons.sync
        return (
          <div
            key={event.id}
            className="flex items-start gap-3 px-3 py-2.5 rounded-lg hover:bg-slate-800/50 transition-colors"
          >
            <div
              className={`w-8 h-8 rounded-lg flex items-center justify-center text-sm font-bold flex-shrink-0 ${config.color}`}
            >
              {config.icon}
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-sm text-slate-300 truncate">{event.message}</p>
              <p className="text-xs text-slate-600 mt-0.5">{formatTime(event.timestamp)}</p>
            </div>
          </div>
        )
      })}
    </div>
  )
}
