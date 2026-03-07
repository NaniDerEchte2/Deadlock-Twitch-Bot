import { motion } from 'framer-motion';
import { Activity, CreditCard, Radio, Server, Users } from 'lucide-react';
import { Link } from 'react-router-dom';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { KpiCard } from '@/components/shared/KpiCard';
import { StatusBadge } from '@/components/shared/StatusBadge';
import { useDashboardOverview, useStreamers, useSubscriptions, useSystemHealth } from '@/hooks/useAdmin';
import { formatBytes, formatDuration, formatNumber, formatRelativeTime } from '@/utils/formatters';

const quickActions = [
  {
    title: 'Streamer verwalten',
    description: 'Partner prüfen, archivieren oder neue Logins aufnehmen.',
    to: '/streamers',
  },
  {
    title: 'System prüfen',
    description: 'EventSub, Datenbank und Fehlerlog zentral überwachen.',
    to: '/monitoring',
  },
  {
    title: 'Bot konfigurieren',
    description: 'Promo-Mode und Polling ohne Legacy-HTML anfassen.',
    to: '/config',
  },
];

export function Dashboard() {
  const overviewQuery = useDashboardOverview();
  const streamersQuery = useStreamers();
  const healthQuery = useSystemHealth();
  const subscriptionsQuery = useSubscriptions();

  const streamers = streamersQuery.data ?? [];
  const subscriptions = subscriptionsQuery.data ?? [];
  const liveCount = streamers.filter((row) => row.isLive).length;
  const verifiedCount = streamers.filter((row) => row.verified).length;
  const chartRows = [...streamers]
    .filter((row) => row.viewerCount)
    .sort((left, right) => (right.viewerCount ?? 0) - (left.viewerCount ?? 0))
    .slice(0, 6)
    .map((row) => ({ login: row.login, viewers: row.viewerCount ?? 0 }));
  const activity = overviewQuery.data?.recentActivity ?? [];

  return (
    <motion.div initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} className="space-y-5">
      <section className="panel-card rounded-[2rem] p-6 md:p-8">
        <div className="relative flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div className="max-w-3xl">
            <p className="text-xs font-semibold uppercase tracking-[0.28em] text-text-secondary">Admin Dashboard</p>
            <h1 className="mt-3 text-4xl font-semibold text-white">Kontrollraum für das Twitch-Bot-System</h1>
            <p className="mt-4 max-w-2xl text-sm leading-7 text-text-secondary">
              Neue React-Oberfläche für Streamer-Verwaltung, Monitoring, Konfiguration und Billing.
              Der Fokus liegt auf schneller Eingriffsfähigkeit ohne in Legacy-Seiten springen zu müssen.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <span className="stat-pill">
              <Activity className="h-4 w-4" />
              Last Tick {formatRelativeTime(healthQuery.data?.lastTickAt)}
            </span>
            <span className="stat-pill">
              <Radio className="h-4 w-4" />
              {liveCount} live
            </span>
          </div>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <KpiCard title="Monitored Streamer" value={formatNumber(streamers.length)} hint={`${verifiedCount} verifiziert`} icon={Users} tone="primary" />
        <KpiCard title="Live jetzt" value={formatNumber(liveCount)} hint="aus twitch_live_state" icon={Radio} tone="accent" />
        <KpiCard title="Speicher" value={formatBytes(healthQuery.data?.memoryBytes ?? healthQuery.data?.memoryRssBytes)} hint={`Uptime ${formatDuration(healthQuery.data?.uptimeSeconds)}`} icon={Server} />
        <KpiCard title="Subscriptions" value={formatNumber(subscriptions.length)} hint="aktive, Trial- und Past-Due-Datensätze" icon={CreditCard} />
      </section>

      <section className="grid gap-5 xl:grid-cols-[1.3fr_0.9fr]">
        <article className="panel-card rounded-[1.8rem] p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-text-secondary">Live Capacity</p>
              <h2 className="mt-2 text-2xl font-semibold text-white">Top Live-Streamer</h2>
            </div>
            <Link to="/monitoring" className="admin-button admin-button-secondary">
              Monitoring
            </Link>
          </div>
          <div className="mt-6 h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartRows}>
                <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
                <XAxis dataKey="login" stroke="#9bb3c5" tickLine={false} axisLine={false} />
                <YAxis stroke="#9bb3c5" tickLine={false} axisLine={false} />
                <Tooltip
                  cursor={{ fill: 'rgba(255,255,255,0.04)' }}
                  contentStyle={{
                    background: '#0f2431',
                    border: '1px solid rgba(255,255,255,0.1)',
                    borderRadius: '16px',
                  }}
                />
                <Bar dataKey="viewers" fill="#ff7a18" radius={[10, 10, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="panel-card rounded-[1.8rem] p-6">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-text-secondary">Quick Actions</p>
          <div className="mt-4 space-y-3">
            {quickActions.map((action) => (
              <Link key={action.to} to={action.to} className="soft-elevate block rounded-[1.4rem] border border-white/10 bg-white/[0.04] p-4">
                <h3 className="font-semibold text-white">{action.title}</h3>
                <p className="mt-2 text-sm leading-6 text-text-secondary">{action.description}</p>
              </Link>
            ))}
          </div>
        </article>
      </section>

      <section className="grid gap-5 lg:grid-cols-[1.2fr_0.8fr]">
        <article className="panel-card rounded-[1.8rem] p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-text-secondary">Recent Activity</p>
              <h2 className="mt-2 text-2xl font-semibold text-white">Interne Admin-Signale</h2>
            </div>
            <StatusBadge status={healthQuery.isSuccess ? 'active' : 'warning'} />
          </div>
          <div className="mt-5 space-y-3">
            {activity.length ? (
              activity.slice(0, 6).map((entry, index) => (
                <div key={`${index}-${String(entry.title ?? entry.message ?? 'activity')}`} className="rounded-[1.3rem] border border-white/10 bg-white/[0.03] p-4">
                  <p className="font-medium text-white">
                    {String(entry.title ?? entry.message ?? entry.label ?? 'Aktivität')}
                  </p>
                  <p className="mt-1 text-sm text-text-secondary">
                    {String(entry.description ?? entry.detail ?? '') || 'Kein zusätzlicher Kontext vorhanden.'}
                  </p>
                </div>
              ))
            ) : (
              <div className="empty-state">`/twitch/api/v2/internal-home` liefert aktuell keine recentActivity.</div>
            )}
          </div>
        </article>

        <article className="panel-card rounded-[1.8rem] p-6">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-text-secondary">System Snapshot</p>
          <div className="mt-4 space-y-3 text-sm">
            <div className="rounded-[1.2rem] border border-white/10 bg-white/[0.04] p-4">
              <span className="text-text-secondary">Python</span>
              <p className="mt-1 font-semibold text-white">{healthQuery.data?.pythonVersion || '—'}</p>
            </div>
            <div className="rounded-[1.2rem] border border-white/10 bg-white/[0.04] p-4">
              <span className="text-text-secondary">Uptime</span>
              <p className="mt-1 font-semibold text-white">{formatDuration(healthQuery.data?.uptimeSeconds)}</p>
            </div>
            <div className="rounded-[1.2rem] border border-white/10 bg-white/[0.04] p-4">
              <span className="text-text-secondary">Last Tick</span>
              <p className="mt-1 font-semibold text-white">{formatRelativeTime(healthQuery.data?.lastTickAt)}</p>
            </div>
          </div>
        </article>
      </section>
    </motion.div>
  );
}
