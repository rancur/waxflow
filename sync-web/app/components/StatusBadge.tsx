'use client'

type BadgeVariant =
  | 'complete' | 'synced' | 'pass' | 'matched' | 'approved'
  | 'downloading' | 'matching' | 'verifying' | 'queued'
  | 'error' | 'failed'
  | 'pending' | 'unmatched'
  | 'mismatched' | 'rejected'

const variantStyles: Record<string, string> = {
  complete: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
  synced: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
  pass: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
  matched: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
  approved: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
  downloading: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
  matching: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
  verifying: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
  queued: 'bg-blue-500/15 text-blue-400 border-blue-500/30',
  error: 'bg-red-500/15 text-red-400 border-red-500/30',
  failed: 'bg-red-500/15 text-red-400 border-red-500/30',
  pending: 'bg-slate-500/15 text-slate-400 border-slate-500/30',
  unmatched: 'bg-slate-500/15 text-slate-400 border-slate-500/30',
  mismatched: 'bg-orange-500/15 text-orange-400 border-orange-500/30',
  rejected: 'bg-red-500/15 text-red-400 border-red-500/30',
}

export default function StatusBadge({ status }: { status: string }) {
  const style = variantStyles[status] || variantStyles.pending
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border ${style}`}>
      {status}
    </span>
  )
}
