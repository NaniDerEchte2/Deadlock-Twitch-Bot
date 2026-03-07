import type { LucideIcon } from 'lucide-react';

interface KpiCardProps {
  title: string;
  value: string;
  hint?: string;
  tone?: 'primary' | 'accent' | 'neutral';
  icon?: LucideIcon;
}

export function KpiCard({ title, value, hint, tone = 'neutral', icon: Icon }: KpiCardProps) {
  const toneClass =
    tone === 'primary'
      ? 'from-primary/30 to-primary/5'
      : tone === 'accent'
        ? 'from-accent/30 to-accent/5'
        : 'from-white/8 to-white/0';

  return (
    <article className={`panel-card soft-elevate rounded-[1.6rem] p-5`}>
      <div className={`absolute inset-x-0 top-0 h-20 bg-gradient-to-br ${toneClass}`} />
      <div className="relative flex items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-text-secondary">{title}</p>
          <p className="mt-3 text-3xl font-semibold text-white">{value}</p>
          {hint ? <p className="mt-2 text-sm text-text-secondary">{hint}</p> : null}
        </div>
        {Icon ? (
          <div className="rounded-2xl border border-white/10 bg-white/5 p-3 text-white/90">
            <Icon className="h-5 w-5" />
          </div>
        ) : null}
      </div>
    </article>
  );
}
