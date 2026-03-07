import { Link, useParams } from 'react-router-dom';
import { ArrowLeft, BarChart3, Clock3, Radio } from 'lucide-react';
import { KpiCard } from '@/components/shared/KpiCard';
import { DataTable, type TableColumn } from '@/components/shared/DataTable';
import { StatusBadge } from '@/components/shared/StatusBadge';
import { useStreamerDetail } from '@/hooks/useAdmin';
import type { SessionSummary } from '@/api/types';
import { coerceRecord, formatDateTime, formatNumber, formatRelativeTime } from '@/utils/formatters';

function metricFromStats(stats: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    if (typeof stats[key] === 'number' || typeof stats[key] === 'string') {
      return String(stats[key]);
    }
  }
  return '—';
}

export function StreamerDetailPage() {
  const params = useParams();
  const detailQuery = useStreamerDetail(params.login);
  const detail = detailQuery.data;
  const stats = coerceRecord(detail?.stats);
  const settings = coerceRecord(detail?.settings);
  const lastSeenAt =
    (typeof stats.lastSeenAt === 'string' && stats.lastSeenAt) ||
    (typeof stats.last_seen_at === 'string' && stats.last_seen_at) ||
    undefined;

  const sessionColumns: TableColumn<SessionSummary>[] = [
    {
      key: 'session',
      title: 'Session',
      sortable: true,
      sortValue: (row) => row.sessionId ?? 0,
      render: (row) => (
        <div>
          <p className="font-medium text-white">{row.title || `Session #${row.sessionId ?? '—'}`}</p>
          <p className="text-xs uppercase tracking-[0.16em] text-text-secondary">{row.category || 'Kategorie unbekannt'}</p>
        </div>
      ),
    },
    {
      key: 'started',
      title: 'Start',
      sortable: true,
      sortValue: (row) => row.startedAt || '',
      render: (row) => formatDateTime(row.startedAt),
    },
    {
      key: 'avg',
      title: 'Avg Viewer',
      sortable: true,
      sortValue: (row) => row.averageViewers ?? 0,
      render: (row) => formatNumber(row.averageViewers ?? 0),
    },
    {
      key: 'peak',
      title: 'Peak',
      sortable: true,
      sortValue: (row) => row.peakViewers ?? 0,
      render: (row) => formatNumber(row.peakViewers ?? 0),
    },
  ];

  if (detailQuery.isLoading) {
    return <div className="panel-card rounded-[1.8rem] p-8 text-white">Streamer wird geladen …</div>;
  }

  if (!detail) {
    return <div className="panel-card rounded-[1.8rem] p-8 text-white">Kein Datensatz für diesen Streamer.</div>;
  }

  return (
    <section className="space-y-5">
      <header className="panel-card rounded-[1.8rem] p-6">
        <Link to="/streamers" className="inline-flex items-center gap-2 text-sm text-text-secondary hover:text-white">
          <ArrowLeft className="h-4 w-4" />
          Zurück zur Liste
        </Link>
        <div className="mt-5 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.28em] text-text-secondary">Streamer Detail</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">{detail.displayName || detail.login}</h1>
            <p className="mt-2 text-sm text-text-secondary">Zuletzt gesehen {formatRelativeTime(lastSeenAt)}</p>
          </div>
          <div className="flex flex-wrap gap-2">
            <StatusBadge status={detail.isLive ? 'live' : detail.archived ? 'archived' : detail.verified ? 'verified' : 'offline'} />
            {detail.planId ? <StatusBadge status={detail.planId} /> : null}
          </div>
        </div>
      </header>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <KpiCard title="Plan" value={detail.planId || 'Unbekannt'} hint="aus Admin-Detailendpoint" icon={BarChart3} />
        <KpiCard title="Letzter Peak" value={metricFromStats(stats, 'peak_viewers', 'peakViewers')} hint="letzte bekannte Auswertung" icon={Radio} tone="primary" />
        <KpiCard title="Sessions" value={metricFromStats(stats, 'totalSessions', 'session_count', 'sessions')} hint="aggregiert im Detailpayload" icon={Clock3} />
        <KpiCard title="Follower Delta" value={metricFromStats(stats, 'followerDelta', 'follower_delta_total', 'followers_delta_total')} hint="wenn im Backend geliefert" tone="accent" />
      </section>

      <section className="grid gap-5 xl:grid-cols-[1.2fr_0.8fr]">
        <article className="panel-card rounded-[1.8rem] p-6">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-text-secondary">Letzte Sessions</p>
          <div className="mt-4">
            <DataTable columns={sessionColumns} rows={detail.sessions ?? []} rowKey={(row, index) => `${row.sessionId ?? index}`} />
          </div>
        </article>
        <article className="panel-card rounded-[1.8rem] p-6">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-text-secondary">Settings Snapshot</p>
          <pre className="mt-4 overflow-auto rounded-[1.4rem] border border-white/10 bg-slate-950/55 p-4 text-xs leading-6 text-emerald-100">
            {JSON.stringify(settings, null, 2)}
          </pre>
        </article>
      </section>
    </section>
  );
}
