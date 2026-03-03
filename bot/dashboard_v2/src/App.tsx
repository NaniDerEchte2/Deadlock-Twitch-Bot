import { useState, useEffect, useMemo, useRef, Component, type FormEvent, type ReactNode, type ErrorInfo } from 'react';
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
  createInternalHomeChangelogEntry,
  fetchInternalHome,
  type InternalHomeActionEntry,
  type InternalHomeChangelogEntry,
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

function todayDateInputValue(): string {
  return new Date().toISOString().slice(0, 10);
}

function initialInternalHomeStreamer(): string | null {
  const params = new URLSearchParams(window.location.search);
  const streamer = params.get('streamer')?.trim().toLowerCase() || '';
  return streamer || null;
}

function streamKey(stream: InternalHomeSession, index: number): string {
  if (stream.id !== null && stream.id !== undefined) {
    return String(stream.id);
  }
  return `stream-${index}`;
}

function formatCalendarDate(value: string | null | undefined): string {
  if (!value) {
    return 'Unbekannt';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return 'Unbekannt';
  }
  return date.toLocaleDateString('de-DE', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
  });
}

function actionLogKey(entry: InternalHomeActionEntry, index: number): string {
  if (entry.id !== null && entry.id !== undefined) {
    return String(entry.id);
  }
  return `action-${index}`;
}

function changelogKey(entry: InternalHomeChangelogEntry, index: number): string {
  if (entry.id !== null && entry.id !== undefined) {
    return String(entry.id);
  }
  return `changelog-${index}`;
}

function actionLogTone(entry: InternalHomeActionEntry): {
  label: string;
  badgeClass: string;
  dotClass: string;
  tone: 'critical' | 'warning' | 'success' | 'info';
} {
  const severity = String(entry.severity || 'info').toLowerCase();
  if (severity === 'critical' || severity === 'error') {
    return {
      label: 'Kritisch',
      badgeClass: 'border-error/35 bg-error/10 text-error',
      dotClass: 'bg-error',
      tone: 'critical',
    };
  }
  if (severity === 'warning') {
    return {
      label: 'Warnung',
      badgeClass: 'border-warning/35 bg-warning/10 text-warning',
      dotClass: 'bg-warning',
      tone: 'warning',
    };
  }
  if (severity === 'success') {
    return {
      label: 'Positiv',
      badgeClass: 'border-success/35 bg-success/10 text-success',
      dotClass: 'bg-success',
      tone: 'success',
    };
  }
  return {
    label: 'Info',
    badgeClass: 'border-accent/35 bg-accent/10 text-accent',
    dotClass: 'bg-accent',
    tone: 'info',
  };
}

function formatActionUser(entry: InternalHomeActionEntry): string {
  const targetLogin = entry.targetLogin?.trim();
  const actorLogin = entry.actorLogin?.trim();
  const targetId = entry.targetId?.trim();

  if (targetLogin) {
    return `@${targetLogin}`;
  }
  if (actorLogin) {
    return `@${actorLogin}`;
  }
  if (targetId) {
    return `ID ${targetId}`;
  }
  return 'System';
}

function formatActionMeta(entry: InternalHomeActionEntry): string {
  const meta: string[] = [];
  const targetLogin = entry.targetLogin?.trim();
  const actorLogin = entry.actorLogin?.trim();
  const targetId = entry.targetId?.trim();
  const eventType = entry.eventType?.trim();

  if (actorLogin && actorLogin !== targetLogin) {
    meta.push(`Mod @${actorLogin}`);
  }
  if (targetId) {
    meta.push(`ID ${targetId}`);
  }
  if (eventType) {
    meta.push(eventType.toUpperCase());
  }
  if (entry.viewerCount !== null && entry.viewerCount !== undefined) {
    meta.push(`${formatNumber(entry.viewerCount)} Viewer`);
  }

  return meta.join(' · ') || 'Bot-Aktivität';
}

function isBanAction(entry: InternalHomeActionEntry): boolean {
  const haystack = [
    entry.eventType,
    entry.statusLabel,
    entry.title,
    entry.summary,
    entry.reason,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  return haystack.includes('ban');
}

function InternalHome() {
  const { data: authStatus, isLoading: loadingAuth } = useAuthStatus();
  const { data: streamers = [], isLoading: loadingStreamers } = useStreamerList();
  const [selectedStreamer, setSelectedStreamer] = useState<string | null>(initialInternalHomeStreamer);
  const [changelogTitle, setChangelogTitle] = useState('');
  const [changelogContent, setChangelogContent] = useState('');
  const [changelogDate, setChangelogDate] = useState(() => todayDateInputValue());
  const [isSavingChangelog, setIsSavingChangelog] = useState(false);
  const [changelogError, setChangelogError] = useState<string | null>(null);
  const [changelogSuccess, setChangelogSuccess] = useState<string | null>(null);
  const normalizedSelectedStreamer = selectedStreamer?.trim().toLowerCase() || null;
  const partnerStreamers = useMemo(
    () =>
      streamers
        .map((candidate) => ({
          ...candidate,
          login: candidate.login?.trim().toLowerCase() || '',
        }))
        .filter((candidate) => candidate.isPartner && candidate.login),
    [streamers]
  );
  const partnerLoginSet = useMemo(
    () => new Set(partnerStreamers.map((candidate) => candidate.login)),
    [partnerStreamers]
  );
  const isAdminView = Boolean(authStatus?.isAdmin || authStatus?.isLocalhost);
  const streamerOverride = isAdminView ? normalizedSelectedStreamer : null;
  const hasValidAdminSelection =
    streamerOverride !== null && partnerLoginSet.has(streamerOverride);
  const canRequestInternalHome = !loadingAuth && (!isAdminView || hasValidAdminSelection);
  const { data, isLoading, isError, error, refetch, isFetching } = useQuery({
    queryKey: ['internal-home', streamerOverride],
    queryFn: () => fetchInternalHome(streamerOverride),
    staleTime: 60 * 1000,
    enabled: canRequestInternalHome,
  });

  useEffect(() => {
    if (loadingAuth || !isAdminView || loadingStreamers) {
      return;
    }

    if (normalizedSelectedStreamer && partnerLoginSet.has(normalizedSelectedStreamer)) {
      return;
    }

    const ownLogin = authStatus?.twitchLogin?.trim().toLowerCase() || '';
    const fallbackStreamer =
      ownLogin && partnerLoginSet.has(ownLogin)
        ? ownLogin
        : partnerStreamers[0]?.login || null;

    if (fallbackStreamer !== normalizedSelectedStreamer) {
      setSelectedStreamer(fallbackStreamer);
    }
  }, [
    authStatus?.twitchLogin,
    isAdminView,
    loadingAuth,
    loadingStreamers,
    normalizedSelectedStreamer,
    partnerLoginSet,
    partnerStreamers,
  ]);

  useEffect(() => {
    if (loadingAuth) {
      return;
    }

    const params = new URLSearchParams(window.location.search);
    const nextStreamer = isAdminView ? normalizedSelectedStreamer || '' : '';
    const currentStreamer = params.get('streamer')?.trim().toLowerCase() || '';

    if (nextStreamer) {
      params.set('streamer', nextStreamer);
    } else if (currentStreamer) {
      params.delete('streamer');
    }

    const nextSearch = params.toString();
    const nextUrl = `${window.location.pathname}${nextSearch ? `?${nextSearch}` : ''}${window.location.hash}`;
    const currentUrl = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    if (nextUrl !== currentUrl) {
      window.history.replaceState({}, '', nextUrl);
    }
  }, [isAdminView, loadingAuth, normalizedSelectedStreamer]);

  if (!canRequestInternalHome) {
    const emptyAdminState =
      !loadingAuth && isAdminView && !loadingStreamers && partnerStreamers.length === 0;

    return (
      <div className="min-h-screen relative px-3 py-4 md:px-7 md:py-8">
        <div className="relative max-w-[1700px] mx-auto space-y-4 md:space-y-6">
          <div className="panel-card rounded-2xl p-6 md:p-8">
            {emptyAdminState ? (
              <div className="space-y-3">
                <h2 className="text-xl font-bold text-white">Kein Partner auswählbar</h2>
                <p className="text-sm text-text-secondary">
                  In der Admin-Ansicht werden nur aktive Partner-Profile angezeigt. Sobald Partner
                  verfügbar sind, kannst du sie hier auswählen.
                </p>
              </div>
            ) : (
              <div className="flex items-center gap-3 text-text-secondary">
                <Loader2 className="h-5 w-5 animate-spin text-primary" />
                <span>Admin-Profil wird vorbereitet ...</span>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  const home = data ?? {};
  const twitchLogin = home.twitchLogin?.trim() || '';
  const displayName = home.displayName?.trim() || twitchLogin || 'Creator';
  const greeting = home.greeting?.trim() || `Willkommen zurück, ${displayName}`;
  const grantedScopes = home.oauth?.grantedScopes ?? [];
  const missingScopes = home.oauth?.missingScopes ?? [];
  const hasScopeIssue =
    missingScopes.length > 0 ||
    home.oauth?.status === 'partial' ||
    home.oauth?.status === 'missing';
  const oauthStatus =
    home.oauth?.status ||
    (home.oauth?.connected ? 'connected' : hasScopeIssue ? 'missing' : 'partial');
  const oauthStatusText =
    oauthStatus === 'connected'
      ? 'OAuth verbunden'
      : oauthStatus === 'error'
        ? 'OAuth Fehler'
        : hasScopeIssue
        ? 'Scopes fehlen'
        : 'OAuth prüfen';
  const oauthStatusClass =
    oauthStatus === 'connected'
      ? 'border-success/35 bg-success/10 text-success'
      : oauthStatus === 'error' || hasScopeIssue
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
  const rawActionLog = home.actionLog ?? [];
  const hasActivityFeedNote =
    rawActionLog.find((entry) => String(entry.id || '').trim() === 'impact-note')?.summary?.trim() ||
    '';
  const actionLog = rawActionLog
    .filter((entry) => String(entry.id || '').trim() !== 'impact-note')
    .slice(0, 10);
  const changelogEntries = (home.changelog?.entries ?? []).slice(0, 6);
  const canWriteChangelog = home.changelog?.canWrite === true;
  const oauthFallbackUrl = '/twitch/auth/login?next=%2Ftwitch%2Fdashboard';
  const reconnectUrl = home.oauth?.reconnectUrl || oauthFallbackUrl;
  const profileUrl = home.oauth?.profileUrl || reconnectUrl;
  const needsOauthReconnect = oauthStatus !== 'connected' || hasScopeIssue;
  const oauthQuickHref = needsOauthReconnect ? reconnectUrl : profileUrl;
  const scopeWatchCardClass = hasScopeIssue
    ? 'rounded-2xl border border-error/35 bg-gradient-to-br from-error/16 via-background/80 to-background/60 p-4'
    : 'rounded-2xl border border-accent/20 bg-gradient-to-br from-accent/12 via-background/80 to-background/60 p-4';
  const scopeWatchValueClass = hasScopeIssue ? 'text-error' : 'text-white';

  async function handleChangelogSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (isSavingChangelog || !canWriteChangelog) {
      return;
    }

    const trimmedContent = changelogContent.trim();
    if (!trimmedContent) {
      setChangelogError('Bitte zuerst einen Changelog-Text eintragen.');
      setChangelogSuccess(null);
      return;
    }

    setIsSavingChangelog(true);
    setChangelogError(null);
    setChangelogSuccess(null);

    try {
      await createInternalHomeChangelogEntry({
        title: changelogTitle.trim() || undefined,
        content: trimmedContent,
        entryDate: changelogDate || undefined,
      });
      setChangelogTitle('');
      setChangelogContent('');
      setChangelogDate(todayDateInputValue());
      setChangelogSuccess('Changelog gespeichert.');
      await refetch();
    } catch (submitError) {
      setChangelogError(
        submitError instanceof Error ? submitError.message : 'Changelog konnte nicht gespeichert werden.'
      );
    } finally {
      setIsSavingChangelog(false);
    }
  }

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
        needsOauthReconnect
          ? 'Direkter Re-Auth-Link fuer fehlende Scopes'
          : 'Profil und Scope-Status prüfen',
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
        <section className="panel-card rounded-2xl p-4 md:p-5 internal-home-section" data-delay="0">
          <div className="internal-home-shell">
            <div className="space-y-4">
              <div className="space-y-3">
                <div className="inline-flex items-center gap-2 rounded-full border border-accent/40 bg-accent/10 px-3 py-1 text-[11px] font-semibold tracking-wider text-accent uppercase">
                  <Sparkles className="h-3.5 w-3.5" />
                  Internal Home
                </div>

                <h1 className="display-font text-2xl font-bold leading-tight text-white md:text-3xl">
                  {greeting}
                </h1>

                <p className="max-w-2xl text-sm text-text-secondary md:text-base">
                  Kompakte Ops-Startseite mit mehr Fokus auf schnelle Aktionen, Re-Auth und einen
                  sichtbaren Aktivitäts Feed im Log-Look.
                </p>

              </div>

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
                  Auto-Raid aktiv
                </span>
                <span
                  className={`internal-home-pill ${
                    hasScopeIssue
                      ? 'border-error/35 bg-error/10 text-error'
                      : 'border-border/80 bg-background/70 text-text-secondary'
                  }`}
                >
                  <CheckCircle2 className={`h-3.5 w-3.5 ${hasScopeIssue ? 'text-error' : 'text-accent'}`} />
                  {grantedScopes.length} Scopes ok
                  {missingScopes.length > 0 ? ` · ${missingScopes.length} offen` : ''}
                </span>
                <span className="internal-home-pill border-border/80 bg-background/70 text-text-secondary">
                  <Sparkles className="h-3.5 w-3.5 text-primary" />
                  {actionLog.length} Log-Eintraege
                </span>
              </div>

              <div className="rounded-2xl border border-accent/20 bg-gradient-to-br from-accent/10 via-background/80 to-background/70 p-4 md:p-5">
                <div className="text-xs font-semibold uppercase tracking-wider text-text-secondary">
                  OAuth & Profil
                </div>
                <div className="mt-3 flex items-center gap-2 text-lg font-semibold text-white">
                  <Twitch className="h-4 w-4 text-primary" />
                  {twitchLogin ? `@${twitchLogin}` : 'Nicht verbunden'}
                </div>
                <p className="mt-1 text-xs text-text-secondary">
                  {home.displayName?.trim() ||
                    'Bitte Twitch OAuth verbinden, um den vollen Scope zu laden.'}
                </p>

                <a
                  href={oauthQuickHref}
                  className="mt-4 inline-flex items-center gap-2 rounded-lg border border-primary/40 bg-primary/15 px-3 py-2 text-sm font-semibold text-primary transition-colors hover:bg-primary/25"
                >
                  <Shield className="h-4 w-4" />
                  {twitchLogin
                    ? needsOauthReconnect
                      ? 'OAuth jetzt neu autorisieren'
                      : 'OAuth / Profil oeffnen'
                    : 'Mit Twitch verbinden'}
                  <ArrowRight className="h-4 w-4" />
                </a>

                {needsOauthReconnect ? (
                  <div className="mt-3 rounded-xl border border-error/25 bg-error/10 p-3 text-xs text-text-secondary">
                    <div className="flex items-start gap-2">
                      <ShieldAlert className="mt-0.5 h-4 w-4 shrink-0 text-error" />
                      <div>
                        <p className="font-semibold text-white">
                          Fehlende Scopes erkannt. Keine Weiterleitung ins Analyse-Dashboard.
                        </p>
                        <a
                          href={reconnectUrl}
                          className="mt-2 inline-flex items-center gap-1 font-semibold text-error transition-colors hover:text-white"
                        >
                          Direkt neu autorisieren
                          <ArrowRight className="h-3.5 w-3.5" />
                        </a>
                      </div>
                    </div>
                  </div>
                ) : null}
              </div>

              <div className="rounded-2xl border border-border bg-background/55 p-4">
                <div className="text-[11px] font-semibold uppercase tracking-wider text-text-secondary">
                  Twitch Login
                </div>
                <div className="mt-2 flex items-center gap-2 text-base font-semibold text-white">
                  <Twitch className="h-4 w-4 text-primary" />
                  {twitchLogin ? `@${twitchLogin}` : 'Nicht verbunden'}
                </div>
                <p className="mt-1 text-xs text-text-secondary">
                  {home.displayName?.trim() ||
                    'Bitte Twitch OAuth verbinden, um den vollen Scope zu laden.'}
                </p>
              </div>
            </div>

            <div className="space-y-3 xl:self-end">
              {isAdminView ? (
                <div className="rounded-2xl border border-primary/20 bg-gradient-to-br from-primary/10 via-background/80 to-background/60 p-4 md:p-5">
                  <div className="text-[11px] font-semibold uppercase tracking-wider text-text-secondary">
                    Admin View
                  </div>
                  <div className="mt-3 flex flex-col gap-3 lg:flex-row lg:items-center">
                    <label className="text-xs font-semibold text-white" htmlFor="internal-home-streamer-switch">
                      Partner-Profil wechseln
                    </label>
                    <select
                      id="internal-home-streamer-switch"
                      value={normalizedSelectedStreamer || ''}
                      onChange={(event) => setSelectedStreamer(event.target.value || null)}
                      disabled={loadingStreamers || partnerStreamers.length === 0}
                      className="min-w-[220px] rounded-xl border border-border bg-background/80 px-3 py-2 text-sm font-medium text-white outline-none transition-colors focus:border-border-hover disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      {partnerStreamers.length === 0 ? (
                        <option value="">Keine Partner verfügbar</option>
                      ) : (
                        partnerStreamers.map((candidate) => (
                          <option key={candidate.login} value={candidate.login}>
                            {candidate.login}
                          </option>
                        ))
                      )}
                    </select>
                  </div>
                  <p className="mt-2 text-xs text-text-secondary">
                    Aktuelles Partner-Profil:{' '}
                    <span className="font-semibold text-white">
                      @{normalizedSelectedStreamer || twitchLogin || 'unbekannt'}
                    </span>
                  </p>
                </div>
              ) : null}

              <div className={scopeWatchCardClass}>
                <div className="text-[11px] font-semibold uppercase tracking-wider text-text-secondary">
                  Scope Watch
                </div>
                <div className={`mt-2 text-xl font-bold ${scopeWatchValueClass}`}>
                  {hasScopeIssue ? `${missingScopes.length || 1} offen` : `${grantedScopes.length} ok`}
                </div>
                <p className="mt-1 text-xs text-text-secondary">
                  {hasScopeIssue
                    ? 'Scopes fehlen. Nutze direkt den Re-Auth-Link.'
                    : 'OAuth sauber verbunden und bereit.'}
                </p>
              </div>

              <div className="rounded-2xl border border-primary/20 bg-gradient-to-br from-primary/10 via-background/80 to-background/70 p-4 md:p-5">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-xs font-semibold uppercase tracking-wider text-text-secondary">
                      Quick Actions
                    </div>
                    <p className="mt-1 text-xs text-text-secondary">
                      Direktzugriff auf die wichtigsten Bereiche in einem Block.
                    </p>
                  </div>
                  <ArrowRight className="h-4 w-4 text-text-secondary" />
                </div>

                <div className="mt-4 grid gap-3 sm:grid-cols-2">
                  {quickActions.map((action) => {
                    const Icon = action.icon;
                    return (
                      <a
                        key={action.id}
                        href={action.href}
                        className="internal-home-quick-action group rounded-xl border border-border bg-card/70 p-4 transition-colors hover:border-border-hover hover:bg-card-hover"
                      >
                        <div className="flex items-start gap-3">
                          <div className="mt-0.5 flex h-9 w-9 items-center justify-center rounded-lg border border-accent/40 bg-accent/10 text-accent">
                            <Icon className="h-4 w-4" />
                          </div>
                          <div className="min-w-0 flex-1">
                            <p className="truncate text-sm font-semibold text-white">
                              {action.title}
                            </p>
                            <p className="mt-1 text-xs text-text-secondary">
                              {action.description}
                            </p>
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
              </div>
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

        <section
          className="grid gap-4 xl:grid-cols-[minmax(0,1.75fr)_minmax(320px,0.95fr)] internal-home-section"
          data-delay="2"
        >
          <article className="panel-card rounded-2xl p-5 md:p-6">
            <div className="mb-4 flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
              <div>
                <h2 className="text-lg font-bold text-white">Aktivitäts Feed</h2>
                <p className="text-xs text-text-secondary">
                  Die letzten 10 Bot-Aktionen inklusive Moderation, Bans und Raid-Events.
                </p>
              </div>
              <span className="inline-flex items-center rounded-full border border-border bg-background/70 px-3 py-1 text-[11px] font-semibold tracking-wider text-text-secondary uppercase">
                {actionLog.length} / 10 Aktionen
              </span>
            </div>

            {hasActivityFeedNote ? (
              <div className="mb-4 rounded-xl border border-accent/20 bg-accent/5 p-3 text-xs text-text-secondary">
                Read-only: Dieser Feed zeigt nur an und löst hier keine Schreibaktionen aus.
              </div>
            ) : null}

            {actionLog.length === 0 ? (
              <div className="rounded-xl border border-border bg-background/60 p-4 text-sm text-text-secondary">
                Kein Aktivitäts Feed verfuegbar.
              </div>
            ) : (
              <div className="internal-home-log-grid">
                {actionLog.map((entry, index) => {
                  const tone = actionLogTone(entry);
                  const isBanEntry = isBanAction(entry);
                  const title = entry.title?.trim() || entry.eventType?.trim() || 'Bot Aktion';
                  const reasonText = entry.reason?.trim() || '';
                  const summaryText =
                    entry.summary?.trim() ||
                    entry.description?.trim() ||
                    reasonText ||
                    entry.metric?.trim() ||
                    'Kein Detailtext';
                  const statusText = entry.statusLabel?.trim() || tone.label;
                  const metricText = entry.metric?.trim() || '';
                  const banMessage = reasonText || summaryText || 'Keine Nachricht gespeichert';

                  return (
                    <article
                      key={actionLogKey(entry, index)}
                      className="internal-home-log-card rounded-2xl border border-border bg-background/55 p-4 md:p-5"
                      data-tone={tone.tone}
                      data-kind={isBanEntry ? 'ban' : 'default'}
                    >
                      <div className="flex flex-wrap items-center gap-2 text-[11px] text-text-secondary">
                        <span className="rounded-full border border-border/70 bg-background/80 px-2.5 py-1 font-semibold text-white">
                          {formatDateTime(entry.timestamp)}
                        </span>
                        <span className="rounded-full border border-border/70 bg-background/70 px-2.5 py-1 font-semibold text-text-secondary">
                          {formatActionUser(entry)}
                        </span>
                        <span
                          className={`rounded-full border px-2.5 py-1 font-semibold uppercase tracking-wider ${tone.badgeClass}`}
                        >
                          {statusText}
                        </span>
                        {isBanEntry ? (
                          <span className="rounded-full border border-warning/35 bg-warning/10 px-2.5 py-1 font-semibold uppercase tracking-wider text-warning">
                            Ban Feed
                          </span>
                        ) : null}
                      </div>

                      {isBanEntry ? (
                        <div className="mt-4 rounded-2xl border border-warning/20 bg-black/20 p-4">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <h3 className="truncate text-sm font-semibold text-white md:text-base">
                                Ban gegen {formatActionUser(entry)}
                              </h3>
                              <p className="mt-1 text-xs text-text-secondary">
                                Gebannt am {formatDateTime(entry.timestamp)}
                              </p>
                            </div>
                            <span className={`mt-1 h-2.5 w-2.5 shrink-0 rounded-full ${tone.dotClass}`} />
                          </div>

                          <div className="mt-4 grid gap-3 md:grid-cols-2">
                            <div className="rounded-xl border border-border/60 bg-background/55 px-3 py-2.5">
                              <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-text-secondary">
                                User
                              </div>
                              <div className="mt-1 text-sm font-semibold text-white">
                                {formatActionUser(entry)}
                              </div>
                            </div>
                            <div className="rounded-xl border border-border/60 bg-background/55 px-3 py-2.5">
                              <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-text-secondary">
                                Zeitpunkt
                              </div>
                              <div className="mt-1 text-sm font-semibold text-white">
                                {formatDateTime(entry.timestamp)}
                              </div>
                            </div>
                          </div>

                          <div className="mt-3 rounded-xl border border-border/60 bg-background/55 px-3 py-3">
                            <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-text-secondary">
                              Gebannte Nachricht
                            </div>
                            <p className="mt-2 break-words text-sm leading-6 text-text-primary/90">
                              {banMessage}
                            </p>
                          </div>

                          {entry.actorLogin?.trim() ? (
                            <p className="mt-3 text-xs text-text-secondary">
                              Mod: @{entry.actorLogin.trim()}
                            </p>
                          ) : null}
                        </div>
                      ) : (
                        <div className="mt-4">
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              <h3 className="truncate text-sm font-semibold text-white md:text-base">
                                {title}
                              </h3>
                              <p className="mt-1 text-xs text-text-secondary">
                                {formatActionMeta(entry)}
                              </p>
                            </div>
                            <span className={`mt-1 h-2.5 w-2.5 shrink-0 rounded-full ${tone.dotClass}`} />
                          </div>

                          <p className="mt-3 text-sm leading-6 text-text-primary/90">{summaryText}</p>

                          {metricText && metricText !== summaryText ? (
                            <p className="mt-3 text-xs font-semibold text-accent">
                              Metrik: {metricText}
                            </p>
                          ) : null}
                        </div>
                      )}

                      {!isBanEntry && reasonText && reasonText !== summaryText ? (
                        <p className="mt-3 text-xs text-text-secondary">Grund: {reasonText}</p>
                      ) : null}
                    </article>
                  );
                })}
              </div>
            )}
          </article>

          <aside className="panel-card rounded-2xl p-5 md:p-6">
            <div className="mb-4 flex items-center justify-between gap-3">
              <div>
                <h2 className="text-lg font-bold text-white">Changelog</h2>
                <p className="text-xs text-text-secondary">
                  Letzte interne Updates mit Datum und kurzer Notiz.
                </p>
              </div>
              <CalendarClock className="h-4 w-4 text-text-secondary" />
            </div>

            {canWriteChangelog ? (
              <form
                onSubmit={(event) => void handleChangelogSubmit(event)}
                className="mb-4 rounded-xl border border-border bg-background/55 p-4"
              >
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wider text-text-secondary">
                      Neuer Eintrag
                    </p>
                    <p className="mt-1 text-[11px] text-text-secondary">
                      Max. {home.changelog?.maxEntries ?? 20} Einträge, neueste zuerst.
                    </p>
                  </div>
                  <span className="rounded-full border border-success/35 bg-success/10 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wider text-success">
                    Admin
                  </span>
                </div>

                <div className="mt-4 space-y-3">
                  <input
                    type="text"
                    value={changelogTitle}
                    onChange={(event) => setChangelogTitle(event.target.value)}
                    maxLength={160}
                    placeholder="Titel (optional)"
                    disabled={isSavingChangelog}
                    className="w-full rounded-lg border border-border bg-background/75 px-3 py-2 text-sm text-white outline-none transition-colors placeholder:text-text-secondary/70 focus:border-border-hover"
                  />

                  <input
                    type="date"
                    value={changelogDate}
                    onChange={(event) => setChangelogDate(event.target.value)}
                    disabled={isSavingChangelog}
                    className="w-full rounded-lg border border-border bg-background/75 px-3 py-2 text-sm text-white outline-none transition-colors focus:border-border-hover"
                  />

                  <textarea
                    value={changelogContent}
                    onChange={(event) => setChangelogContent(event.target.value)}
                    maxLength={4000}
                    placeholder="Was hat sich geändert?"
                    disabled={isSavingChangelog}
                    rows={4}
                    className="w-full resize-y rounded-lg border border-border bg-background/75 px-3 py-2 text-sm text-white outline-none transition-colors placeholder:text-text-secondary/70 focus:border-border-hover"
                  />
                </div>

                {changelogError ? (
                  <p className="mt-3 text-xs text-error">{changelogError}</p>
                ) : null}
                {changelogSuccess ? (
                  <p className="mt-3 text-xs text-success">{changelogSuccess}</p>
                ) : null}

                <button
                  type="submit"
                  disabled={isSavingChangelog}
                  className="mt-4 inline-flex items-center gap-2 rounded-lg border border-primary/40 bg-primary/15 px-3 py-2 text-sm font-semibold text-primary transition-colors hover:bg-primary/25 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {isSavingChangelog ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <Sparkles className="h-4 w-4" />
                  )}
                  {isSavingChangelog ? 'Speichere ...' : 'Changelog speichern'}
                </button>
              </form>
            ) : (
              <div className="mb-4 rounded-xl border border-border bg-background/55 p-4 text-xs text-text-secondary">
                Changelog ist hier sichtbar. Schreiben bleibt auf Admin/Localhost beschränkt.
              </div>
            )}

            {changelogEntries.length === 0 ? (
              <div className="rounded-xl border border-border bg-background/60 p-4 text-sm text-text-secondary">
                Kein Changelog-Eintrag verfuegbar.
              </div>
            ) : (
              <div className="space-y-3">
                {changelogEntries.map((entry, index) => {
                  const title = entry.title?.trim() || 'Update';
                  const content = entry.content?.trim() || 'Kein Beschreibungstext';
                  const primaryDate = entry.entryDate || entry.createdAt;

                  return (
                    <article
                      key={changelogKey(entry, index)}
                      className="internal-home-changelog-entry rounded-xl border border-border bg-background/55 p-4"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <span className="rounded-lg border border-border/80 bg-background/80 px-3 py-1.5 text-xs font-semibold text-white">
                          {formatCalendarDate(primaryDate)}
                        </span>
                        {entry.createdAt ? (
                          <span className="text-[11px] text-text-secondary">
                            {formatDateTime(entry.createdAt)}
                          </span>
                        ) : null}
                      </div>

                      <h3 className="mt-3 text-sm font-semibold text-white">{title}</h3>
                      <p className="mt-2 text-xs leading-6 text-text-secondary">{content}</p>
                    </article>
                  );
                })}
              </div>
            )}
          </aside>
        </section>

        <section className="panel-card rounded-2xl p-5 md:p-6 internal-home-section" data-delay="3">
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
                        <div className="text-[10px] uppercase tracking-wider text-text-secondary">
                          Ø Viewer
                        </div>
                        <div className="mt-1 font-semibold text-white">
                          {formatNumber(stream.avgViewers, 1)}
                        </div>
                      </div>
                      <div className="rounded-md border border-border/70 bg-background/65 px-2 py-1.5 text-center">
                        <div className="text-[10px] uppercase tracking-wider text-text-secondary">
                          Peak
                        </div>
                        <div className="mt-1 font-semibold text-white">
                          {formatNumber(stream.peakViewers)}
                        </div>
                      </div>
                      <div className="rounded-md border border-border/70 bg-background/65 px-2 py-1.5 text-center">
                        <div className="text-[10px] uppercase tracking-wider text-text-secondary">
                          Dauer
                        </div>
                        <div className="mt-1 font-semibold text-white">
                          {formatDuration(stream.durationMinutes)}
                        </div>
                      </div>
                      <div className="rounded-md border border-border/70 bg-background/65 px-2 py-1.5 text-center">
                        <div className="text-[10px] uppercase tracking-wider text-text-secondary">
                          Follower Δ
                        </div>
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
