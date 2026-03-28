'use client'

import { ParityData } from '../types'

function getColor(pct: number): { ring: string; text: string; glow: string } {
  if (pct >= 90) return { ring: 'stroke-emerald-500', text: 'text-emerald-400', glow: 'glow-green' }
  if (pct >= 70) return { ring: 'stroke-amber-500', text: 'text-amber-400', glow: 'glow-amber' }
  return { ring: 'stroke-red-500', text: 'text-red-400', glow: 'glow-red' }
}

export default function ParityMeter({ data, loading }: { data?: ParityData; loading?: boolean }) {
  if (loading || !data) {
    return (
      <div className="flex flex-col items-center justify-center py-8">
        <div className="w-48 h-48 rounded-full skeleton" />
        <div className="mt-4 w-32 h-6 skeleton" />
      </div>
    )
  }

  const { synced, total, percentage } = data
  const pct = Math.min(percentage, 100)
  const { ring, text, glow } = getColor(pct)

  // SVG circle math
  const radius = 88
  const circumference = 2 * Math.PI * radius
  const offset = circumference - (pct / 100) * circumference

  return (
    <div className={`flex flex-col items-center justify-center py-4 ${glow} rounded-2xl`}>
      <div className="relative w-52 h-52">
        <svg className="w-full h-full -rotate-90" viewBox="0 0 200 200">
          {/* Background ring */}
          <circle
            cx="100"
            cy="100"
            r={radius}
            fill="none"
            stroke="currentColor"
            className="text-slate-800"
            strokeWidth="10"
          />
          {/* Progress ring */}
          <circle
            cx="100"
            cy="100"
            r={radius}
            fill="none"
            className={ring}
            strokeWidth="10"
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            style={{
              transition: 'stroke-dashoffset 1s ease-in-out',
            }}
          />
        </svg>
        {/* Center text */}
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span className={`text-4xl font-bold tabular-nums ${text}`}>
            {pct.toFixed(1)}%
          </span>
          <span className="text-xs text-slate-500 mt-1">PARITY</span>
        </div>
      </div>
      <p className="mt-3 text-sm text-slate-400">
        <span className="text-slate-200 font-semibold tabular-nums">{synced.toLocaleString()}</span>
        {' / '}
        <span className="tabular-nums">{total.toLocaleString()}</span>
        {' synced'}
      </p>
    </div>
  )
}
