interface StatusBadgeProps {
  status?: string | boolean | null;
}

const STATUS_STYLES: Record<string, string> = {
  live: 'border-red-400/35 bg-red-500/15 text-red-100',
  active: 'border-emerald-400/35 bg-emerald-500/15 text-emerald-100',
  verified: 'border-sky-400/35 bg-sky-500/15 text-sky-100',
  archived: 'border-slate-400/35 bg-slate-500/15 text-slate-100',
  offline: 'border-slate-500/40 bg-slate-700/30 text-slate-200',
  warning: 'border-amber-400/35 bg-amber-500/15 text-amber-100',
  error: 'border-red-400/35 bg-red-500/15 text-red-100',
  trialing: 'border-violet-400/35 bg-violet-500/15 text-violet-100',
  past_due: 'border-amber-400/35 bg-amber-500/15 text-amber-100',
};

export function StatusBadge({ status }: StatusBadgeProps) {
  const normalized =
    typeof status === 'boolean'
      ? status
        ? 'verified'
        : 'offline'
      : String(status || 'offline').trim().toLowerCase();

  return (
    <span
      className={[
        'inline-flex items-center rounded-full border px-2.5 py-1 text-[0.7rem] font-semibold uppercase tracking-[0.18em]',
        STATUS_STYLES[normalized] || 'border-white/10 bg-white/5 text-white/70',
      ].join(' ')}
    >
      {normalized.replace('_', ' ')}
    </span>
  );
}
