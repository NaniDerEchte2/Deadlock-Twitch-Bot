import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  fetchInternalHome,
  type InternalHomeActionEntry,
  type InternalHomeChangelogEntry,
} from '@/api/client';
import { useStreamerList, useAuthStatus } from '@/hooks/useAnalytics';
import {
  ArrowRight,
  BarChart3,
  CalendarClock,
  CheckCircle2,
  Gauge,
  Loader2,
  RadioTower,
  ShieldAlert,
  ShieldCheck,
  Sparkles,
  Twitch,
  type LucideIcon,
} from 'lucide-react';

function initialInternalHomeStreamer(): string | null {
  const params = new URLSearchParams(window.location.search);
  const streamer = params.get('streamer')?.trim().toLowerCase() || '';
  return streamer || null;
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) return 'Unbekannt';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'Unbekannt';
  return date.toLocaleString('de-DE', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function formatCalendarDate(value: string | null | undefined): string {
  if (!value) return 'Unbekannt';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'Unbekannt';
  return date.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

function actionLogTone(entry: InternalHomeActionEntry): {
  label: string;
  badgeClass: string;
} {
  const severity = String(entry.severity || 'info').toLowerCase();
  if (severity === 'critical' || severity === 'error') {
    return { label: 'Kritisch', badgeClass: 'border-error/35 bg-error/10 text-error' };
  }
  if (severity === 'warning') {
    return { label: 'Warnung', badgeClass: 'border-warning/35 bg-warning/10 text-warning' };
  }
  if (severity === 'success') {
    return { label: 'Positiv', badgeClass: 'border-success/35 bg-success/10 text-success' };
  }
  return { label: 'Info', badgeClass: 'border-accent/35 bg-accent/10 text-accent' };
}

function formatActionUser(entry: InternalHomeActionEntry): string {
  const targetLogin = entry.targetLogin?.trim();
  const actorLogin = entry.actorLogin?.trim();
  if (targetLogin) return `@${targetLogin}`;
  if (actorLogin) return `@${actorLogin}`;
  return 'System';
}

function isBanAction(entry: InternalHomeActionEntry): boolean {
  const haystack = [
    entry.eventType,
    entry.statusLabel,
    entry.title,
    entry.summary,
    entry.reason,
    entry.description,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  return haystack.includes('ban') || haystack.includes('banned') || haystack.includes('gebannt');
}

function isServicePitchWarningAction(entry: InternalHomeActionEntry): boolean {
  const haystack = [
    entry.eventType,
    entry.statusLabel,
    entry.title,
    entry.summary,
    entry.reason,
    entry.description,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  return (
    haystack.includes('service_pitch_warning')
    || haystack.includes('service-pitch')
    || haystack.includes('service pitch')
    || haystack.includes('pitch warn')
  );
}

function stripActionNoise(value: string): string {
  const cleaned = value
    .replace(/auto[_\s-]*raid[_\s-]*on[_\s-]*offline/gi, '')
    .replace(/auto[_\s-]*offline[_\s-]*raid/gi, '')
    .replace(/\s{2,}/g, ' ')
    .replace(/\s+([,.;:!?])/g, '$1')
    .trim();
  return cleaned;
}

function actionKey(entry: InternalHomeActionEntry, index: number): string {
  if (entry.id !== null && entry.id !== undefined) return String(entry.id);
  return `action-${index}`;
}

function changelogKey(entry: InternalHomeChangelogEntry, index: number): string {
  if (entry.id !== null && entry.id !== undefined) return String(entry.id);
  return `changelog-${index}`;
}

function splitActionDetailSegments(value: string | null | undefined): string[] {
  if (!value) return [];
  return value
    .split('|')
    .map((segment) => stripActionNoise(segment.trim()))
    .filter(Boolean);
}

function normalizeActionEventType(entry: InternalHomeActionEntry): string {
  return String(entry.eventType || '').trim().toLowerCase();
}

function formatActionSourceLabel(source: string | null | undefined): string {
  const normalized = String(source || '').trim().toLowerCase();
  if (!normalized) return '';
  if (normalized === 'autoban_log') return 'Auto-Ban Log';
  if (normalized === 'service_warning_log') return 'Service-Warning Log';
  return normalized.replace(/[_-]+/g, ' ');
}

function isVisibleChannelAction(
  entry: InternalHomeActionEntry,
  channelLogin: string
): boolean {
  const eventType = normalizeActionEventType(entry);
  if (!eventType) return false;

  if (eventType === 'ban' || eventType === 'ban_keyword_hit' || eventType === 'unban') {
    return true;
  }
  if (eventType === 'raid' || eventType === 'raid_history') {
    return true;
  }
  if (eventType === 'service_pitch_warning') {
    if (!channelLogin) return true;
    const actorLogin = String(entry.actorLogin || '').trim().toLowerCase();
    if (!actorLogin) return true;
    return actorLogin === channelLogin;
  }
  return false;
}

function buildPriorityActionDetails(
  entry: InternalHomeActionEntry,
  isServicePitchWarning: boolean
): string[] {
  const detailLines: string[] = [];
  const summary = stripActionNoise(entry.summary?.trim() || '');
  const targetLogin = entry.targetLogin?.trim() || '';
  const targetId = entry.targetId?.trim() || '';
  const actorLogin = entry.actorLogin?.trim() || '';
  const metric = stripActionNoise(entry.metric?.trim() || '');
  const reason = stripActionNoise(entry.reason?.trim() || '');
  const statusLabel = entry.statusLabel?.trim() || '';
  const sourceLabel = formatActionSourceLabel(entry.source);

  if (summary) detailLines.push(summary);
  if (statusLabel) detailLines.push(`Status: ${statusLabel}`);
  if (targetLogin) detailLines.push(`Nutzer: @${targetLogin}`);
  if (targetId) detailLines.push(`Nutzer-ID: ${targetId}`);
  if (actorLogin) detailLines.push(`${isServicePitchWarning ? 'Kanal' : 'Moderator'}: @${actorLogin}`);
  if (metric) detailLines.push(`Metrik: ${metric}`);
  if (reason) detailLines.push(`Grund: ${reason}`);
  if (sourceLabel) detailLines.push(`Quelle: ${sourceLabel}`);
  detailLines.push(...splitActionDetailSegments(entry.description));

  const seen = new Set<string>();
  return detailLines.filter((line) => {
    const normalized = line.trim().toLowerCase();
    if (!normalized || seen.has(normalized)) return false;
    seen.add(normalized);
    return true;
  });
}

function prioritizeActionLog(entries: InternalHomeActionEntry[], limit: number): InternalHomeActionEntry[] {
  if (limit <= 0 || entries.length === 0) return [];
  const banEvents = entries.filter((entry) => isBanAction(entry));
  const serviceWarningEvents = entries.filter(
    (entry) => !isBanAction(entry) && isServicePitchWarningAction(entry)
  );
  const regularEvents = entries.filter(
    (entry) => !isBanAction(entry) && !isServicePitchWarningAction(entry)
  );
  return [...banEvents, ...serviceWarningEvents, ...regularEvents].slice(0, limit);
}

export function InternalHomeLanding() {
  const { data: authStatus, isLoading: loadingAuth } = useAuthStatus();
  const { data: streamers = [], isLoading: loadingStreamers } = useStreamerList();
  const [selectedStreamer, setSelectedStreamer] = useState<string | null>(initialInternalHomeStreamer);
  const normalizedSelectedStreamer = selectedStreamer?.trim().toLowerCase() || null;
  const partnerStreamers = useMemo(
    () =>
      streamers
        .map((candidate) => ({ ...candidate, login: candidate.login?.trim().toLowerCase() || '' }))
        .filter((candidate) => candidate.isPartner && candidate.login),
    [streamers]
  );
  const partnerLoginSet = useMemo(() => new Set(partnerStreamers.map((candidate) => candidate.login)), [partnerStreamers]);
  const isAdminView = Boolean(authStatus?.isAdmin || authStatus?.isLocalhost);
  const streamerOverride = isAdminView ? normalizedSelectedStreamer : null;
  const hasValidAdminSelection = streamerOverride !== null && partnerLoginSet.has(streamerOverride);
  const canRequestInternalHome = !loadingAuth && (!isAdminView || hasValidAdminSelection);
  const { data, isLoading, isError, error, refetch, isFetching } = useQuery({
    queryKey: ['internal-home', streamerOverride],
    queryFn: () => fetchInternalHome(streamerOverride),
    staleTime: Number.POSITIVE_INFINITY,
    enabled: canRequestInternalHome,
  });

  useEffect(() => {
    if (loadingAuth || !isAdminView || loadingStreamers) return;
    if (normalizedSelectedStreamer && partnerLoginSet.has(normalizedSelectedStreamer)) return;
    const ownLogin = authStatus?.twitchLogin?.trim().toLowerCase() || '';
    const fallbackStreamer = ownLogin && partnerLoginSet.has(ownLogin) ? ownLogin : partnerStreamers[0]?.login || null;
    if (fallbackStreamer !== normalizedSelectedStreamer) setSelectedStreamer(fallbackStreamer);
  }, [authStatus?.twitchLogin, isAdminView, loadingAuth, loadingStreamers, normalizedSelectedStreamer, partnerLoginSet, partnerStreamers]);

  useEffect(() => {
    if (loadingAuth) return;
    const params = new URLSearchParams(window.location.search);
    const nextStreamer = isAdminView ? normalizedSelectedStreamer || '' : '';
    const currentStreamer = params.get('streamer')?.trim().toLowerCase() || '';
    if (nextStreamer) params.set('streamer', nextStreamer);
    else if (currentStreamer) params.delete('streamer');
    const nextSearch = params.toString();
    const nextUrl = `${window.location.pathname}${nextSearch ? `?${nextSearch}` : ''}${window.location.hash}`;
    const currentUrl = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    if (nextUrl !== currentUrl) window.history.replaceState({}, '', nextUrl);
  }, [isAdminView, loadingAuth, normalizedSelectedStreamer]);

  if (!canRequestInternalHome) {
    const emptyAdminState = !loadingAuth && isAdminView && !loadingStreamers && partnerStreamers.length === 0;
    return (
      <div className="min-h-screen relative px-3 py-4 md:px-7 md:py-8">
        <div className="relative max-w-[1280px] mx-auto">
          <div className="panel-card rounded-2xl p-6 md:p-8">
            {emptyAdminState ? (
              <div className="space-y-2">
                <h2 className="text-xl font-bold text-white">Kein Partner auswählbar</h2>
                <p className="text-sm text-text-secondary">In der Admin-Ansicht werden nur aktive Partner-Profile angezeigt.</p>
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
  const missingScopeCount = missingScopes.length;
  const hasScopeIssue = missingScopeCount > 0 || home.oauth?.status === 'partial' || home.oauth?.status === 'missing';
  const oauthStatus = home.oauth?.status || (home.oauth?.connected ? 'connected' : hasScopeIssue ? 'missing' : 'partial');
  const oauthBannerText = oauthStatus === 'connected' ? 'OAuth verbunden' : oauthStatus === 'error' ? 'OAuth Fehler' : hasScopeIssue ? 'Scopes fehlen' : 'OAuth prüfen';
  const oauthBannerClass = oauthStatus === 'connected' ? 'border-success/35 bg-success/10 text-success' : oauthStatus === 'error' || hasScopeIssue ? 'border-error/35 bg-error/10 text-error' : 'border-accent/35 bg-accent/10 text-accent';
  const oauthFallbackUrl = '/twitch/auth/login?next=%2Ftwitch%2Fdashboard';
  const discordConnectFallbackUrl = '/twitch/auth/discord/login?next=%2Ftwitch%2Fdashboard';
  const reconnectUrl = home.oauth?.reconnectUrl || oauthFallbackUrl;
  const profileUrl = home.oauth?.profileUrl || reconnectUrl;
  const needsOauthReconnect = oauthStatus !== 'connected' || hasScopeIssue;
  const oauthQuickHref = needsOauthReconnect ? reconnectUrl : profileUrl;
  const oauthCardStatusText = !needsOauthReconnect ? 'Verbunden' : missingScopeCount > 1 ? 'Re-Auth nötig' : 'Unvollständig';
  const oauthCardStatusClass = !needsOauthReconnect ? 'text-success' : missingScopeCount === 1 ? 'text-error' : 'text-warning';
  const oauthCardHintText = !needsOauthReconnect
    ? 'Verbindung steht und ist nutzbar.'
    : missingScopeCount > 1
      ? `${missingScopeCount} Scopes fehlen. Bitte neu autorisieren.`
      : '1 Scope fehlt. Kritisch unvollständig.';
  const scopeCardStatusText = missingScopeCount === 0 ? 'Vollständig' : missingScopeCount === 1 ? 'Kritisch unvollständig' : 'Re-Auth nötig';
  const scopeCardStatusClass = missingScopeCount === 0 ? 'text-success' : missingScopeCount === 1 ? 'text-error' : 'text-warning';
  const scopeCardHintText = missingScopeCount === 0
    ? `${grantedScopes.length} erwartete Scopes aktiv.`
    : missingScopeCount === 1
      ? 'Genau 1 Scope fehlt.'
      : `${missingScopeCount} Scopes fehlen.`;
  const discordConnected = Boolean(home.discord?.connected);
  const discordConnectUrl = home.discord?.connectUrl || discordConnectFallbackUrl;
  const discordCardStatusText = discordConnected ? 'Verbunden' : 'Nicht verbunden';
  const discordCardStatusClass = discordConnected ? 'text-success' : 'text-warning';
  const discordCardHintText = discordConnected
    ? 'Discord-Verknüpfung erkannt.'
    : 'Noch kein Discord-Profil verknüpft.';
  const liveAnnouncementTarget = normalizedSelectedStreamer || twitchLogin;
  const liveAnnouncementHref = liveAnnouncementTarget ? `/twitch/live-announcement?streamer=${encodeURIComponent(liveAnnouncementTarget)}` : '/twitch/live-announcement';
  const rawActionLog = home.actionLog ?? [];
  const activityFeedNote = rawActionLog.find((entry) => String(entry.id || '').trim() === 'impact-note')?.summary?.trim() || '';
  const channelScopeLogin = (normalizedSelectedStreamer || twitchLogin || '').trim().toLowerCase();
  const baseActionLog = rawActionLog
    .filter((entry) => String(entry.id || '').trim() !== 'impact-note')
    .filter((entry) => isVisibleChannelAction(entry, channelScopeLogin));
  const actionLog = prioritizeActionLog(baseActionLog, 8);
  const changelogEntries = (home.changelog?.entries ?? []).slice(0, 3);
  const quickActions: Array<{ id: string; title: string; description: string; href: string; icon: LucideIcon }> = [
    {
      id: 'oauth',
      title: 'OAuth & Profil',
      description: needsOauthReconnect ? 'Fehlende Scopes direkt neu autorisieren' : 'Profil, Verknuepfung und Scopes pruefen',
      href: oauthQuickHref,
      icon: ShieldCheck,
    },
    {
      id: 'analysis-v2',
      title: 'Analytics v2',
      description: 'Vollstaendiges Analyse-Dashboard mit allen Tabs',
      href: '/twitch/dashboard-v2',
      icon: BarChart3,
    },
    {
      id: 'live-announcement',
      title: 'Live Message Builder',
      description: 'Go-Live Text, Embed und Buttons konfigurieren',
      href: liveAnnouncementHref,
      icon: RadioTower,
    },
    {
      id: 'billing',
      title: 'Abo / Billing',
      description: 'Subscription, Rechnungen und Checkout verwalten',
      href: '/twitch/abbo',
      icon: Gauge,
    },
    {
      id: 'oauth-reauth',
      title: 'Twitch neu autorisieren',
      description: 'Direkter Re-Auth-Link für fehlende Scopes.',
      href: reconnectUrl,
      icon: ShieldAlert,
    },
    {
      id: 'discord-connect',
      title: 'Discord verbinden',
      description: discordConnected ? 'Discord erneut verbinden oder Status pruefen.' : 'Discord-Verknüpfung jetzt starten.',
      href: discordConnectUrl,
      icon: CheckCircle2,
    },
  ];

  if (isLoading) {
    return (
      <div className="min-h-screen relative px-3 py-4 md:px-7 md:py-8">
        <div className="relative max-w-[1280px] mx-auto space-y-4 md:space-y-5">
          <div className="panel-card rounded-2xl p-6 md:p-8">
            <div className="flex items-center gap-3 text-text-secondary"><Loader2 className="h-5 w-5 animate-spin text-primary" /><span>Interne Startseite wird geladen ...</span></div>
          </div>
        </div>
      </div>
    );
  }

  if (isError) {
    const errorMessage = error instanceof Error ? error.message : 'Unbekannter Fehler';
    return (
      <div className="min-h-screen relative px-3 py-4 md:px-7 md:py-8">
        <div className="relative max-w-[1280px] mx-auto">
          <div className="panel-card rounded-2xl p-6 md:p-8">
            <h2 className="text-xl font-bold text-white">Internal Home nicht verfügbar</h2>
            <p className="mt-1 text-sm text-text-secondary">{errorMessage}</p>
            <button onClick={() => void refetch()} className="mt-4 inline-flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-2 text-sm font-semibold text-white transition-colors hover:border-border-hover hover:bg-card-hover">
              <ArrowRight className="h-4 w-4" />
              Erneut laden
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="internal-home-vibe min-h-screen relative px-3 py-4 md:px-7 md:py-8">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        <div className="absolute -top-32 right-[-8rem] h-[28rem] w-[28rem] rounded-full bg-primary/22 blur-3xl" />
        <div className="absolute top-[24%] -left-28 h-[22rem] w-[22rem] rounded-full bg-accent/24 blur-3xl" />
        <div className="absolute bottom-[-8rem] left-[34%] h-[24rem] w-[24rem] rounded-full bg-success/20 blur-3xl" />
      </div>

      <div className="relative max-w-[1280px] mx-auto space-y-4 md:space-y-5">
        <section className="panel-card rounded-2xl p-5 md:p-6 internal-home-section" data-delay="0">
          <div className="internal-home-landing-shell">
            <div className="space-y-3">
              <div className="space-y-3">
                <div className="inline-flex items-center gap-2 rounded-full border border-accent/40 bg-accent/10 px-3 py-1 text-[11px] font-semibold tracking-wider text-accent uppercase">
                  <Sparkles className="h-3.5 w-3.5" />
                  Internal Home
                </div>
                <h1 className="display-font text-2xl font-bold leading-tight text-white md:text-3xl">{greeting}</h1>
                <p className="max-w-3xl text-sm text-text-secondary md:text-base">
                  Landing-Übersicht fuer Auth, letzte Aktionen und direkte Wege in die wichtigsten Bereiche.
                </p>
              </div>

              <div className="flex flex-wrap items-center gap-2">
                <span className={`internal-home-pill ${oauthBannerClass}`}>
                  {oauthStatus === 'connected' ? <ShieldCheck className="h-3.5 w-3.5" /> : <ShieldAlert className="h-3.5 w-3.5" />}
                  {oauthBannerText}
                </span>
                <span className="internal-home-pill border-border/80 bg-background/70 text-text-secondary">
                  <Twitch className="h-3.5 w-3.5 text-primary" />
                  {twitchLogin ? `@${twitchLogin}` : 'Nicht verbunden'}
                </span>
                <span className="internal-home-pill border-border/80 bg-background/70 text-text-secondary">
                  <CheckCircle2 className={`h-3.5 w-3.5 ${hasScopeIssue ? 'text-error' : 'text-accent'}`} />
                  {grantedScopes.length} Scopes ok{missingScopes.length > 0 ? ` · ${missingScopes.length} offen` : ''}
                </span>
                <span className="internal-home-pill border-border/80 bg-background/70 text-text-secondary">
                  <CalendarClock className="h-3.5 w-3.5 text-accent" />
                  Aktualisiert: {formatDateTime(home.generatedAt)}
                </span>
              </div>
            </div>

            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-end">
              {isAdminView ? (
                <div className="rounded-xl border border-border bg-background/65 p-3 sm:min-w-[260px]">
                  <label className="mb-2 block text-[11px] font-semibold uppercase tracking-wider text-text-secondary" htmlFor="internal-home-streamer-switch">
                    Partner-Profil
                  </label>
                  <select
                    id="internal-home-streamer-switch"
                    value={normalizedSelectedStreamer || ''}
                    onChange={(event) => setSelectedStreamer(event.target.value || null)}
                    disabled={loadingStreamers || partnerStreamers.length === 0}
                    className="w-full rounded-lg border border-border bg-background/80 px-3 py-2 text-sm font-medium text-white outline-none transition-colors focus:border-border-hover disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {partnerStreamers.length === 0 ? (
                      <option value="">Keine Partner verfügbar</option>
                    ) : (
                      partnerStreamers.map((candidate) => (
                        <option key={candidate.login} value={candidate.login}>{candidate.login}</option>
                      ))
                    )}
                  </select>
                </div>
              ) : null}

              <button
                onClick={() => void refetch()}
                disabled={isFetching}
                className="inline-flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-2 text-sm font-semibold text-white transition-colors hover:border-border-hover hover:bg-card-hover disabled:cursor-not-allowed disabled:opacity-70"
              >
                {isFetching ? <Loader2 className="h-4 w-4 animate-spin" /> : <ArrowRight className="h-4 w-4" />}
                Neu laden
              </button>
            </div>
          </div>
        </section>

        <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4 internal-home-section" data-delay="1">
          <article className="internal-home-status-card rounded-xl border border-border bg-background/60 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-wider text-text-secondary">OAuth Verbindung</div>
            <p className={`mt-2 text-base font-semibold ${oauthCardStatusClass}`}>{oauthCardStatusText}</p>
            <p className="mt-1 text-xs text-text-secondary">{oauthCardHintText}</p>
          </article>

          <article className="internal-home-status-card rounded-xl border border-border bg-background/60 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-wider text-text-secondary">Scopes</div>
            <p className={`mt-2 text-base font-semibold ${scopeCardStatusClass}`}>{scopeCardStatusText}</p>
            <p className="mt-1 text-xs text-text-secondary">{scopeCardHintText}</p>
          </article>

          <article className="internal-home-status-card rounded-xl border border-border bg-background/60 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-wider text-text-secondary">Discord verbunden</div>
            <p className={`mt-2 text-base font-semibold ${discordCardStatusClass}`}>{discordCardStatusText}</p>
            <p className="mt-1 text-xs text-text-secondary">{discordCardHintText}</p>
          </article>

          <article className="internal-home-status-card rounded-xl border border-border bg-background/60 p-4">
            <div className="text-[11px] font-semibold uppercase tracking-wider text-text-secondary">Abo</div>
            <p className="mt-2 text-base font-semibold text-white">Free</p>
            <p className="mt-1 text-xs text-text-secondary">Platzhalter für spätere Abo-Details.</p>
          </article>
        </section>

        <section className="panel-card rounded-2xl p-5 md:p-6 internal-home-section" data-delay="2">
          <div className="mb-4 flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <h2 className="text-lg font-bold text-white">Schnellzugriff</h2>
              <p className="text-xs text-text-secondary">Direkte Ziele fuer Navigation, ohne in Details zu versinken.</p>
            </div>
            <span className="text-xs text-text-secondary">{quickActions.length} Aktionen</span>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            {quickActions.map((action) => {
              const Icon = action.icon;
              return (
                <a key={action.id} href={action.href} className="internal-home-link-card group rounded-xl border border-border bg-card/70 p-4">
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
        </section>

        <section className="internal-home-split-1-4 grid gap-4 internal-home-section" data-delay="3">
          <aside className="panel-card rounded-2xl p-5 md:p-6">
            <div className="mb-4 flex items-center justify-between gap-3">
              <div>
                <h2 className="text-lg font-bold text-white">Was gibt&apos;s Neues</h2>
                <p className="text-xs text-text-secondary">Letzte interne Updates (read-only).</p>
              </div>
              <CalendarClock className="h-4 w-4 text-text-secondary" />
            </div>

            {changelogEntries.length === 0 ? (
              <div className="rounded-xl border border-border bg-background/60 p-4 text-sm text-text-secondary">Keine neuen Updates verfügbar.</div>
            ) : (
              <div className="space-y-2.5">
                {changelogEntries.map((entry, index) => {
                  const title = entry.title?.trim() || 'Update';
                  const content = entry.content?.trim() || 'Kein Beschreibungstext';
                  const primaryDate = entry.entryDate || entry.createdAt;
                  return (
                    <article key={changelogKey(entry, index)} className="internal-home-news-item rounded-xl border border-border bg-background/55 p-3.5">
                      <div className="flex flex-wrap items-center justify-between gap-2 text-[11px]">
                        <span className="rounded-full border border-border/70 bg-background/80 px-2.5 py-1 font-semibold text-white">{formatCalendarDate(primaryDate)}</span>
                        {entry.createdAt ? <span className="text-text-secondary">{formatDateTime(entry.createdAt)}</span> : null}
                      </div>
                      <p className="mt-2 text-sm font-semibold text-white">{title}</p>
                      <p className="mt-1 text-xs leading-5 text-text-secondary">{content}</p>
                    </article>
                  );
                })}
              </div>
            )}
          </aside>

          <article className="panel-card rounded-2xl p-5 md:p-6">
            <div className="mb-4 flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
              <div>
                <h2 className="text-lg font-bold text-white">Letzte Aktionen</h2>
                <p className="text-xs text-text-secondary">Kanalbezogene, sichtbare Aktionen (Ban/Unban/Raid/Service-Pitch) mit Priorität auf Ban und Service-Pitch.</p>
              </div>
              <span className="inline-flex items-center rounded-full border border-border bg-background/70 px-3 py-1 text-[11px] font-semibold tracking-wider text-text-secondary uppercase">{actionLog.length} Einträge</span>
            </div>

            {activityFeedNote ? <div className="mb-4 rounded-xl border border-accent/20 bg-accent/5 p-3 text-xs text-text-secondary">{activityFeedNote}</div> : null}
            {actionLog.length === 0 ? (
              <div className="rounded-xl border border-border bg-background/60 p-4 text-sm text-text-secondary">Keine Aktionen vorhanden.</div>
            ) : (
              <ul className="space-y-2.5">
                {actionLog.map((entry, index) => {
                  const isBan = isBanAction(entry);
                  const isServicePitchWarning = isServicePitchWarningAction(entry);
                  const isPriorityWarning = isBan || isServicePitchWarning;
                  const tone = actionLogTone(entry);
                  const rawTitle = entry.title?.trim() || entry.eventType?.trim() || 'Bot Aktion';
                  const title = stripActionNoise(rawTitle) || 'Bot Aktion';
                  const rawSummary = entry.summary?.trim() || entry.description?.trim() || entry.reason?.trim() || entry.metric?.trim() || '';
                  const summaryText = stripActionNoise(rawSummary);
                  const statusText = isBan
                    ? 'BAN'
                    : isServicePitchWarning
                      ? 'SERVICE-PITCH'
                      : entry.statusLabel?.trim() || tone.label;
                  const accountText = formatActionUser(entry);
                  const statusBadgeClass = isPriorityWarning
                    ? 'border-warning/35 bg-warning/10 text-warning'
                    : tone.badgeClass;
                  const cardClass = isPriorityWarning
                    ? 'internal-home-action-item rounded-xl border border-warning/35 bg-warning/10 p-3.5'
                    : 'internal-home-action-item rounded-xl border border-border bg-background/55 p-3.5';
                  const detailLines = isPriorityWarning
                    ? buildPriorityActionDetails(entry, isServicePitchWarning)
                    : [];

                  return (
                    <li key={actionKey(entry, index)} className={cardClass}>
                      <div className="flex flex-wrap items-center gap-2 text-[11px] text-text-secondary">
                        <span className="rounded-full border border-border/70 bg-background/80 px-2.5 py-1 font-semibold text-white">{formatDateTime(entry.timestamp)}</span>
                        <span className="rounded-full border border-border/70 bg-background/70 px-2.5 py-1 font-semibold text-text-secondary">{accountText}</span>
                        <span className={`rounded-full border px-2.5 py-1 font-semibold uppercase tracking-wider ${statusBadgeClass}`}>{statusText}</span>
                      </div>
                      {isPriorityWarning ? (
                        <>
                          <p className="mt-2 text-sm font-semibold text-white">{title || (isBan ? 'Ban erkannt' : 'Service-Pitch Warnung')}</p>
                          {detailLines.length === 0 ? (
                            <p className="mt-1 text-xs leading-5 text-text-primary">Keine Details gespeichert.</p>
                          ) : (
                            <div className="mt-1.5 space-y-1 text-xs leading-5 text-text-primary">
                              {detailLines.map((line, detailIndex) => (
                                <p key={`detail-${detailIndex}`}>{line}</p>
                              ))}
                            </div>
                          )}
                        </>
                      ) : (
                        <p className="mt-2 text-sm leading-5 text-text-secondary">
                          <span className="font-semibold text-white">{title}</span>
                          {summaryText ? ` · ${summaryText}` : ''}
                        </p>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}
          </article>
        </section>

      </div>
    </div>
  );
}
