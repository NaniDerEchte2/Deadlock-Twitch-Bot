import { useState, useEffect, useRef, Component, type ReactNode, type ErrorInfo } from 'react';
import { useQuery, QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Header } from '@/components/layout/Header';
import { TabNavigation, type TabId } from '@/components/layout/TabNavigation';
import { Overview } from '@/pages/Overview';
import { Sessions } from '@/pages/Sessions';
import { ChatAnalytics } from '@/pages/ChatAnalytics';
import { Growth } from '@/pages/Growth';
import { Audience } from '@/pages/Audience';
import { Comparison } from '@/pages/Comparison';
import { Schedule } from '@/pages/Schedule';
import { Coaching } from '@/pages/Coaching';
import { Monetization } from '@/pages/Monetization';
import { Category } from '@/pages/Category';
import { Viewers } from '@/pages/Viewers';
import { Experimental } from '@/pages/Experimental';
import { AIAnalysis } from '@/pages/AIAnalysis';
import {
  fetchInternalHome,
  type InternalHomeImpactEntry,
  type InternalHomeSession,
} from '@/api/client';
import { useStreamerList, useAuthStatus } from '@/hooks/useAnalytics';
import type { TimeRange } from '@/types/analytics';
import {
  AlertTriangle,
  ArrowRight,
  BarChart3,
  CalendarClock,
  CheckCircle2,
  Gauge,
  Loader2,
  RadioTower,
  Shield,
  ShieldAlert,
  ShieldCheck,
  Sparkles,
  Twitch,
  Wifi,
  type LucideIcon,
} from 'lucide-react';

// Error Boundary to prevent white screen on crashes
interface ErrorBoundaryProps {
  children: ReactNode;
}
interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Dashboard Error:', error, info.componentStack);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-bg flex items-center justify-center p-8">
          <div className="panel-card rounded-2xl p-8 max-w-lg text-center">
            <AlertTriangle className="w-12 h-12 text-warning mx-auto mb-4" />
            <h2 className="text-xl font-bold text-white mb-2">Dashboard-Fehler</h2>
            <p className="text-text-secondary mb-4">
              {this.state.error?.message || 'Ein unerwarteter Fehler ist aufgetreten.'}
            </p>
            <button
              onClick={() => this.setState({ hasError: false, error: null })}
              className="px-4 py-2 bg-primary text-white rounded-lg hover:bg-primary-hover transition-colors"
            >
              Erneut versuchen
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

// Create QueryClient
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 2,
      refetchOnWindowFocus: false,
    },
  },
});

function normalizePathname(pathname: string): string {
  const normalized = pathname.replace(/\/+$/, '');
  return normalized || '/';
}

function formatNumber(value: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return new Intl.NumberFormat('de-DE', { maximumFractionDigits: digits }).format(value);
}

function formatSignedNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return new Intl.NumberFormat('de-DE', {
    signDisplay: 'always',
    maximumFractionDigits: 0,
  }).format(value);
}

function formatBanKpi(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  return new Intl.NumberFormat('de-DE', { maximumFractionDigits: 0 }).format(value);
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) {
    return 'Unbekannt';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return 'Unbekannt';
  }
  return date.toLocaleString('de-DE', {
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatDuration(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  if (value < 60) {
    return `${Math.max(0, Math.round(value))}m`;
  }
  const hours = Math.floor(value / 60);
  const minutes = Math.round(value % 60);
  return `${hours}h ${minutes}m`;
}

function streamKey(stream: InternalHomeSession, index: number): string {
  if (stream.id !== null && stream.id !== undefined) {
    return String(stream.id);
  }
  return `stream-${index}`;
}

function impactKey(entry: InternalHomeImpactEntry, index: number): string {
  if (entry.id !== null && entry.id !== undefined) {
    return String(entry.id);
  }
  return `impact-${index}`;
}

function impactTone(entry: InternalHomeImpactEntry): {
  label: string;
  badgeClass: string;
  dotClass: string;
} {
  const severity = String(entry.severity || 'info').toLowerCase();
  if (severity === 'critical' || severity === 'error') {
    return {
      label: 'Kritisch',
      badgeClass: 'border-error/35 bg-error/10 text-error',
      dotClass: 'bg-error',
    };
  }
  if (severity === 'warning') {
    return {
      label: 'Warnung',
      badgeClass: 'border-warning/35 bg-warning/10 text-warning',
      dotClass: 'bg-warning',
    };
  }
  if (severity === 'success') {
    return {
      label: 'Positiv',
      badgeClass: 'border-success/35 bg-success/10 text-success',
      dotClass: 'bg-success',
    };
  }
  return {
    label: 'Info',
    badgeClass: 'border-accent/35 bg-accent/10 text-accent',
    dotClass: 'bg-accent',
  };
}

function InternalHome() {
  const { data, isLoading, isError, error, refetch, isFetching } = useQuery({
    queryKey: ['internal-home'],
    queryFn: fetchInternalHome,
    staleTime: 60 * 1000,
  });

  const home = data ?? {};
  const twitchLogin = home.twitchLogin?.trim() || '';
  const displayName = home.displayName?.trim() || twitchLogin || 'Creator';
  const greeting = home.greeting?.trim() || `Willkommen zurück, ${displayName}`;
  const grantedScopes = home.oauth?.grantedScopes ?? [];
  const missingScopes = home.oauth?.missingScopes ?? [];
  const oauthStatus =
    home.oauth?.status ||
    (home.oauth?.connected ? 'connected' : missingScopes.length > 0 ? 'missing' : 'partial');
  const oauthStatusText =
    oauthStatus === 'connected'
      ? 'OAuth verbunden'
      : oauthStatus === 'missing'
        ? 'Scopes fehlen'
        : oauthStatus === 'error'
          ? 'OAuth Fehler'
          : 'OAuth prüfen';
  const oauthStatusClass =
    oauthStatus === 'connected'
      ? 'border-success/35 bg-success/10 text-success'
      : oauthStatus === 'missing'
        ? 'border-warning/35 bg-warning/10 text-warning'
        : oauthStatus === 'error'
          ? 'border-error/35 bg-error/10 text-error'
          : 'border-accent/35 bg-accent/10 text-accent';
  const followerDelta = home.kpis30d?.followerDelta;
  const followerDeltaClass =
    followerDelta === null || followerDelta === undefined
      ? 'text-white'
      : followerDelta < 0
        ? 'text-error'
        : 'text-success';
  const recentStreams = (home.recentStreams ?? []).slice(0, 5);
  const impactFeed = (home.impactFeed ?? []).slice(0, 8);
  const raidInfo =
    home.raid?.statusText?.trim() ||
    home.raid?.note?.trim() ||
    'Automatischer Raid-Modus aktiv und schreibgeschützt.';
  const oauthFallbackUrl = '/twitch/auth/login?next=%2Ftwitch%2Fdashboard';
  const oauthQuickHref =
    oauthStatus === 'connected'
      ? home.oauth?.profileUrl || home.oauth?.reconnectUrl || oauthFallbackUrl
      : home.oauth?.reconnectUrl || oauthFallbackUrl;

  const quickActions: Array<{
    id: string;
    title: string;
    description: string;
    href: string;
    icon: LucideIcon;
  }> = [
    {
      id: 'analysis-v2',
      title: 'Analyse v2',
      description: 'Zum vollständigen Analytics-Dashboard wechseln',
      href: '/twitch/dashboard-v2',
      icon: BarChart3,
    },
    {
      id: 'raid-history',
      title: 'Raid / History',
      description: 'Raid-Verlauf und Requirements öffnen',
      href: '/twitch/raid/history',
      icon: RadioTower,
    },
    {
      id: 'billing',
      title: 'Abbo / Billing',
      description: 'Subscription, Rechnungen und Checkout verwalten',
      href: '/twitch/abbo',
      icon: Gauge,
    },
    {
      id: 'oauth',
      title: 'OAuth reconnect / Profile',
      description:
        oauthStatus === 'connected'
          ? 'Profil und Scope-Status prüfen'
          : 'OAuth neu verbinden und Scopes aktualisieren',
      href: oauthQuickHref,
      icon: ShieldCheck,
    },
  ];

  if (isLoading) {
    return (
      <div className="min-h-screen relative px-3 py-4 md:px-7 md:py-8">
        <div className="relative max-w-[1700px] mx-auto space-y-4 md:space-y-6">
          <div className="panel-card rounded-2xl p-6 md:p-8">
            <div className="flex items-center gap-3 text-text-secondary">
              <Loader2 className="h-5 w-5 animate-spin text-primary" />
              <span>Interne Startseite wird geladen ...</span>
            </div>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            {Array.from({ length: 4 }).map((_, index) => (
              <div key={`loading-kpi-${index}`} className="panel-card rounded-xl p-4">
                <div className="h-4 w-20 rounded bg-background/80" />
                <div className="mt-3 h-8 w-24 rounded bg-background/70" />
                <div className="mt-2 h-3 w-28 rounded bg-background/60" />
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  if (isError) {
    const errorMessage = error instanceof Error ? error.message : 'Unbekannter Fehler';
    return (
      <div className="min-h-screen relative px-3 py-4 md:px-7 md:py-8">
        <div className="relative max-w-[1700px] mx-auto">
          <div className="panel-card rounded-2xl p-6 md:p-8">
            <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
              <div>
                <h2 className="text-xl font-bold text-white">Internal Home nicht verfügbar</h2>
                <p className="mt-1 text-sm text-text-secondary">{errorMessage}</p>
              </div>
              <button
                onClick={() => void refetch()}
                className="inline-flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-2 text-sm font-semibold text-white transition-colors hover:border-border-hover hover:bg-card-hover"
              >
                <ArrowRight className="h-4 w-4" />
                Erneut laden
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen relative px-3 py-4 md:px-7 md:py-8">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        <div className="absolute -top-28 right-[-7rem] h-[25rem] w-[25rem] rounded-full bg-primary/12 blur-3xl" />
        <div className="absolute top-[28%] -left-24 h-[20rem] w-[20rem] rounded-full bg-accent/14 blur-3xl" />
      </div>

      <div className="relative max-w-[1700px] mx-auto space-y-4 md:space-y-6">
        <section className="panel-card rounded-2xl p-5 md:p-7 internal-home-section" data-delay="0">
          <div className="internal-home-hero flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
            <div className="space-y-3">
              <div className="inline-flex items-center gap-2 rounded-full border border-accent/40 bg-accent/10 px-3 py-1 text-[11px] font-semibold tracking-wider text-accent uppercase">
                <Sparkles className="h-3.5 w-3.5" />
                Internal Home
              </div>

              <h1 className="display-font text-2xl font-bold leading-tight text-white md:text-3xl">
                {greeting}
              </h1>

              <p className="max-w-2xl text-sm text-text-secondary md:text-base">
                Zentrale Übersicht für deinen Twitch-Bot: Auth-Status, 30-Tage-KPIs, letzte Streams
                und operatives Impact-Feed.
              </p>

              <div className="flex flex-wrap items-center gap-2">
                <span className={`internal-home-pill ${oauthStatusClass}`}>
                  {oauthStatus === 'connected' ? (
                    <ShieldCheck className="h-3.5 w-3.5" />
                  ) : (
                    <ShieldAlert className="h-3.5 w-3.5" />
                  )}
                  {oauthStatusText}
                </span>
                <span className="internal-home-pill border-success/35 bg-success/10 text-success">
                  <RadioTower className="h-3.5 w-3.5" />
                  Auto-Raid aktiv (read-only)
                </span>
                <span className="internal-home-pill border-border/80 bg-background/70 text-text-secondary">
                  <CheckCircle2 className="h-3.5 w-3.5 text-accent" />
                  {grantedScopes.length} Scopes ok
                  {missingScopes.length > 0 ? ` · ${missingScopes.length} offen` : ''}
                </span>
              </div>

              <p className="text-xs text-text-secondary">
                Raid-Status: <span className="text-success">{raidInfo}</span>
              </p>
            </div>

            <div className="w-full max-w-md rounded-xl border border-border bg-background/70 p-4 md:p-5">
              <div className="text-xs font-semibold uppercase tracking-wider text-text-secondary">
                Twitch Login
              </div>

              <div className="mt-3 flex items-center gap-2 text-lg font-semibold text-white">
                <Twitch className="h-4 w-4 text-primary" />
                {twitchLogin ? `@${twitchLogin}` : 'Nicht verbunden'}
              </div>

              <p className="mt-1 text-xs text-text-secondary">
                {home.displayName?.trim() || 'Bitte Twitch OAuth verbinden, um den vollen Scope zu laden.'}
              </p>

              <a
                href={home.loginUrl || oauthFallbackUrl}
                className="mt-4 inline-flex items-center gap-2 rounded-lg border border-primary/40 bg-primary/15 px-3 py-2 text-sm font-semibold text-primary transition-colors hover:bg-primary/25"
              >
                <Shield className="h-4 w-4" />
                {twitchLogin ? 'OAuth / Profil öffnen' : 'Mit Twitch verbinden'}
                <ArrowRight className="h-4 w-4" />
              </a>
            </div>
          </div>
        </section>

        <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4 internal-home-section" data-delay="1">
          <article className="internal-home-kpi panel-card rounded-xl p-4">
            <div className="text-xs font-semibold uppercase tracking-wider text-text-secondary">Streams</div>
            <div className="mt-3 text-2xl font-bold text-white">{formatNumber(home.kpis30d?.streams)}</div>
            <p className="mt-1 text-xs text-text-secondary">Letzte 30 Tage</p>
          </article>

          <article className="internal-home-kpi panel-card rounded-xl p-4">
            <div className="text-xs font-semibold uppercase tracking-wider text-text-secondary">Ø Viewer</div>
            <div className="mt-3 text-2xl font-bold text-white">
              {formatNumber(home.kpis30d?.avgViewers, 1)}
            </div>
            <p className="mt-1 text-xs text-text-secondary">Durchschnitt pro Session</p>
          </article>

          <article className="internal-home-kpi panel-card rounded-xl p-4">
            <div className="text-xs font-semibold uppercase tracking-wider text-text-secondary">
              Follower-Delta
            </div>
            <div className={`mt-3 text-2xl font-bold ${followerDeltaClass}`}>
              {formatSignedNumber(followerDelta)}
            </div>
            <p className="mt-1 text-xs text-text-secondary">Nettowachstum 30 Tage</p>
          </article>

          <article className="internal-home-kpi panel-card rounded-xl p-4">
            <div className="text-xs font-semibold uppercase tracking-wider text-text-secondary">Ban-KPI</div>
            <div className="mt-3 flex items-center gap-2 text-2xl font-bold text-white">
              <Gauge className="h-5 w-5 text-warning" />
              {formatBanKpi(home.kpis30d?.banKpi)}
            </div>
            <p className="mt-1 text-xs text-text-secondary">Moderationssignal 30 Tage</p>
          </article>
        </section>

        <section className="panel-card rounded-2xl p-5 md:p-6 internal-home-section" data-delay="2">
          <div className="mb-4 flex items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-bold text-white">Letzte Streams</h2>
              <p className="text-xs text-text-secondary">Neueste 5 Sessions</p>
            </div>
            <CalendarClock className="h-4 w-4 text-text-secondary" />
          </div>

          {recentStreams.length === 0 ? (
            <div className="rounded-xl border border-border bg-background/60 p-4 text-sm text-text-secondary">
              Noch keine Sessions vorhanden.
            </div>
          ) : (
            <div className="space-y-3">
              {recentStreams.map((stream, index) => (
                <article
                  key={streamKey(stream, index)}
                  className="internal-home-stream rounded-xl border border-border bg-background/60 p-4"
                >
                  <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                    <div className="min-w-0">
                      <p className="truncate text-sm font-semibold text-white">
                        {stream.title?.trim() || `Session ${index + 1}`}
                      </p>
                      <p className="mt-1 text-xs text-text-secondary">
                        {formatDateTime(stream.startedAt)}
                        {stream.category ? ` · ${stream.category}` : ''}
                      </p>
                    </div>

                    <div className="grid grid-cols-2 gap-2 text-xs sm:min-w-[340px] sm:grid-cols-4">
                      <div className="rounded-md border border-border/70 bg-background/65 px-2 py-1.5 text-center">
                        <div className="text-[10px] uppercase tracking-wider text-text-secondary">Ø Viewer</div>
                        <div className="mt-1 font-semibold text-white">
                          {formatNumber(stream.avgViewers, 1)}
                        </div>
                      </div>
                      <div className="rounded-md border border-border/70 bg-background/65 px-2 py-1.5 text-center">
                        <div className="text-[10px] uppercase tracking-wider text-text-secondary">Peak</div>
                        <div className="mt-1 font-semibold text-white">{formatNumber(stream.peakViewers)}</div>
                      </div>
                      <div className="rounded-md border border-border/70 bg-background/65 px-2 py-1.5 text-center">
                        <div className="text-[10px] uppercase tracking-wider text-text-secondary">Dauer</div>
                        <div className="mt-1 font-semibold text-white">
                          {formatDuration(stream.durationMinutes)}
                        </div>
                      </div>
                      <div className="rounded-md border border-border/70 bg-background/65 px-2 py-1.5 text-center">
                        <div className="text-[10px] uppercase tracking-wider text-text-secondary">Follower Δ</div>
                        <div
                          className={`mt-1 font-semibold ${
                            (stream.followerDelta || 0) < 0 ? 'text-error' : 'text-success'
                          }`}
                        >
                          {formatSignedNumber(stream.followerDelta)}
                        </div>
                      </div>
                    </div>
                  </div>
                </article>
              ))}
            </div>
          )}
        </section>

        <section className="panel-card rounded-2xl p-5 md:p-6 internal-home-section" data-delay="3">
          <div className="mb-4 flex items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-bold text-white">Bot-Impact Feed</h2>
              <p className="text-xs text-text-secondary">Aktuelle Bot-Wirkung und Ereignisse</p>
            </div>
            <Sparkles className="h-4 w-4 text-accent" />
          </div>

          {impactFeed.length === 0 ? (
            <div className="rounded-xl border border-border bg-background/60 p-4 text-sm text-text-secondary">
              Kein Impact-Eintrag verfügbar.
            </div>
          ) : (
            <div className="space-y-2">
              {impactFeed.map((entry, index) => {
                const tone = impactTone(entry);
                return (
                  <article
                    key={impactKey(entry, index)}
                    className="rounded-xl border border-border bg-background/60 px-4 py-3"
                  >
                    <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className={`h-2.5 w-2.5 rounded-full ${tone.dotClass}`} />
                          <p className="truncate text-sm font-semibold text-white">
                            {entry.title?.trim() || 'Bot Update'}
                          </p>
                          <span
                            className={`rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${tone.badgeClass}`}
                          >
                            {tone.label}
                          </span>
                        </div>
                        <p className="mt-1 text-xs text-text-secondary">
                          {entry.description?.trim() || entry.metric?.trim() || 'Keine Details'}
                        </p>
                      </div>
                      <div className="text-[11px] text-text-secondary">{formatDateTime(entry.timestamp)}</div>
                    </div>
                  </article>
                );
              })}
            </div>
          )}
        </section>

        <section className="panel-card rounded-2xl p-5 md:p-6 internal-home-section" data-delay="4">
          <div className="mb-4 flex items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-bold text-white">Quick Actions</h2>
              <p className="text-xs text-text-secondary">Schneller Zugriff auf Kernbereiche</p>
            </div>
            <ArrowRight className="h-4 w-4 text-text-secondary" />
          </div>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            {quickActions.map((action) => {
              const Icon = action.icon;
              return (
                <a
                  key={action.id}
                  href={action.href}
                  className="internal-home-quick-action group rounded-xl border border-border bg-background/65 p-4 transition-colors hover:border-border-hover hover:bg-card-hover"
                >
                  <div className="flex items-start gap-3">
                    <div className="mt-0.5 flex h-9 w-9 items-center justify-center rounded-lg border border-accent/40 bg-accent/10 text-accent">
                      <Icon className="h-4 w-4" />
                    </div>
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm font-semibold text-white">{action.title}</p>
                      <p className="mt-1 text-xs text-text-secondary">{action.description}</p>
                    </div>
                    <ArrowRight className="h-4 w-4 shrink-0 text-text-secondary transition-transform duration-150 group-hover:translate-x-0.5 group-hover:text-white" />
                  </div>
                </a>
              );
            })}
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-2 text-xs text-text-secondary">
            {isFetching ? (
              <>
                <Loader2 className="h-3.5 w-3.5 animate-spin text-accent" />
                Aktualisiere Daten ...
              </>
            ) : (
              <>
                <CheckCircle2 className="h-3.5 w-3.5 text-success" />
                Zuletzt aktualisiert: {formatDateTime(home.generatedAt)}
              </>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}

function AnalyticsDashboard() {
  const [streamer, setStreamer] = useState<string | null>(null);
  const [days, setDays] = useState<TimeRange>(30);
  const [activeTab, setActiveTab] = useState<TabId>('overview');

  const { data: streamers = [], isLoading: loadingStreamers } = useStreamerList();
  const { data: authStatus, isLoading: loadingAuth, isError: authError } = useAuthStatus();

  // Tracks if we already auto-set the streamer from auth (fire-once guard)
  const hasAutoSetStreamer = useRef(false);

  // Parse URL params on mount
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const urlStreamer = params.get('streamer');
    const urlDays = params.get('days');

    if (urlStreamer) {
      setStreamer(urlStreamer);
      hasAutoSetStreamer.current = true; // URL explicitly set — skip auto-set
    }
    if (urlDays) {
      const d = parseInt(urlDays, 10);
      if (d === 7 || d === 30 || d === 90) setDays(d);
    }
  }, []);

  // Auto-set streamer to logged-in Twitch user on first auth load
  useEffect(() => {
    if (!hasAutoSetStreamer.current && authStatus?.twitchLogin) {
      setStreamer(authStatus.twitchLogin);
      hasAutoSetStreamer.current = true;
    }
  }, [authStatus]);

  // Update URL when params change
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);

    if (streamer) {
      params.set('streamer', streamer);
    } else {
      params.delete('streamer');
    }
    params.set('days', String(days));

    const newUrl = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState({}, '', newUrl);
  }, [streamer, days]);

  const handleSessionClick = (sessionId: number) => {
    // TODO: Navigate to session detail view
    console.log('Session clicked:', sessionId);
  };

  // Auth badge component
  const AuthBadge = () => {
    const badgeBase =
      'flex items-center gap-2 px-3 py-1.5 rounded-full border text-xs font-semibold tracking-wide backdrop-blur-md';

    if (loadingAuth) return null;

    if (authError || !authStatus?.authenticated) {
      return (
        <div className={`${badgeBase} bg-error/10 border-error/30 text-error`}>
          <ShieldAlert className="w-4 h-4" />
          <span>Nicht authentifiziert</span>
        </div>
      );
    }

    if (authStatus.isLocalhost) {
      return (
        <div className={`${badgeBase} bg-success/10 border-success/30 text-success`}>
          <Wifi className="w-4 h-4" />
          <span>Localhost (Admin)</span>
        </div>
      );
    }

    if (authStatus.isAdmin) {
      return (
        <div className={`${badgeBase} bg-primary/10 border-primary/30 text-primary`}>
          <ShieldCheck className="w-4 h-4" />
          <span>Admin</span>
        </div>
      );
    }

    return (
      <div className={`${badgeBase} bg-accent/10 border-accent/30 text-accent`}>
        <Shield className="w-4 h-4" />
        <span>Partner</span>
      </div>
    );
  };

  return (
    <div className="min-h-screen relative px-3 py-4 md:px-7 md:py-8">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        <div className="absolute -top-28 right-[-7rem] h-[25rem] w-[25rem] rounded-full bg-primary/12 blur-3xl" />
        <div className="absolute top-[28%] -left-24 h-[20rem] w-[20rem] rounded-full bg-accent/14 blur-3xl" />
      </div>
      <div className="relative max-w-[1700px] mx-auto">
        {/* Auth Status Badge */}
        <div className="flex justify-end mb-4">
          <AuthBadge />
        </div>

        <Header
          streamer={streamer}
          streamers={streamers}
          days={days}
          onStreamerChange={setStreamer}
          onDaysChange={setDays}
          isLoading={loadingStreamers}
          canViewAllStreamers={authStatus?.permissions?.viewAllStreamers || false}
        />

        <TabNavigation activeTab={activeTab} onTabChange={setActiveTab} />

        {/* Tab Content */}
        {activeTab === 'overview' && (
          <Overview
            streamer={streamer}
            days={days}
            onSessionClick={handleSessionClick}
          />
        )}

        {activeTab === 'streams' && (
          <Sessions streamer={streamer || ''} days={days} />
        )}

        {activeTab === 'chat' && (
          <ChatAnalytics streamer={streamer || ''} days={days} />
        )}

        {activeTab === 'growth' && (
          <Growth streamer={streamer || ''} days={days} />
        )}

        {activeTab === 'audience' && (
          <Audience streamer={streamer || ''} days={days} />
        )}

        {activeTab === 'viewers' && (
          <Viewers streamer={streamer} days={days} />
        )}

        {activeTab === 'compare' && (
          <Comparison streamer={streamer || ''} days={days} />
        )}

        {activeTab === 'schedule' && (
          <Schedule streamer={streamer || ''} days={days} />
        )}

        {activeTab === 'coaching' && (
          <Coaching streamer={streamer || ''} days={days} />
        )}

        {activeTab === 'monetization' && (
          <Monetization streamer={streamer} days={days} />
        )}

        {activeTab === 'category' && (
          <Category
            streamer={streamer}
            days={days}
            onStreamerSelect={setStreamer}
            onNavigate={setActiveTab}
          />
        )}

        {activeTab === 'experimental' && (
          <Experimental streamer={streamer} days={days} />
        )}

        {activeTab === 'ai' && (
          <AIAnalysis streamer={streamer} days={days} />
        )}

      </div>
    </div>
  );
}

export default function App() {
  const isInternalHomeRoute = normalizePathname(window.location.pathname) === '/twitch/dashboard';

  return (
    <QueryClientProvider client={queryClient}>
      <ErrorBoundary>
        {isInternalHomeRoute ? <InternalHome /> : <AnalyticsDashboard />}
      </ErrorBoundary>
    </QueryClientProvider>
  );
}
