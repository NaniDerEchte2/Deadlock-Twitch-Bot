// API Client for Twitch Analytics Dashboard

import type {
  AIHistoryEntry,
  MonetizationStats,
  CategoryTimings,
  CategoryActivitySeries,
  DashboardOverview,
  MonthlyStats,
  WeekdayStats,
  HourlyHeatmapData,
  CalendarHeatmapData,
  ChatAnalytics,
  ViewerOverlap,
  TagPerformance,
  TagPerformanceExtended,
  TitlePerformance,
  RankingEntry,
  StreamSession,
  TimeRange,
  CategoryComparison,
  WatchTimeDistribution,
  FollowerFunnel,
  AudienceInsights,
  ViewerTimelinePoint,
  CategoryLeaderboard,
  CoachingData,
  LurkerAnalysis,
  RaidRetention,
  ViewerProfiles,
  AudienceSharing,
  ViewerDirectory,
  ViewerDetail,
  ViewerSegments,
  ViewerSortField,
  ViewerFilterType,
  ChatHypeTimeline,
  ChatContentAnalysis,
  ChatSocialGraph,
  ExpOverview,
  ExpGameBreakdown,
  ExpGameTransition,
  ExpGrowthCurve,
  AIAnalysisResult,
} from '@/types/analytics';

const API_BASE = '/twitch/api/v2';
const INTERNAL_REDIRECT_PREFIX = '/twitch';
const DASHBOARD_V2_LOGIN_FALLBACK = '/twitch/auth/login?next=%2Ftwitch%2Fdashboard-v2';

// Get partner token from URL or localStorage
function getPartnerToken(): string | null {
  const urlParams = new URLSearchParams(window.location.search);
  const token = urlParams.get('partner_token');
  if (token) {
    localStorage.setItem('partner_token', token);
    return token;
  }
  return localStorage.getItem('partner_token');
}

function isAllowedInternalRedirectPath(pathname: string): boolean {
  return pathname === INTERNAL_REDIRECT_PREFIX || pathname.startsWith(`${INTERNAL_REDIRECT_PREFIX}/`);
}

function sanitizeInternalRedirectUrl(rawUrl: string | null | undefined, fallback: string): string {
  const fallbackCandidate = (fallback || '').trim();
  let safeFallback = DASHBOARD_V2_LOGIN_FALLBACK;
  if (fallbackCandidate && fallbackCandidate.startsWith('/') && !fallbackCandidate.startsWith('//')) {
    try {
      const parsedFallback = new URL(fallbackCandidate, window.location.origin);
      if (
        parsedFallback.origin === window.location.origin &&
        isAllowedInternalRedirectPath(parsedFallback.pathname)
      ) {
        safeFallback = `${parsedFallback.pathname}${parsedFallback.search}${parsedFallback.hash}`;
      }
    } catch {
      safeFallback = DASHBOARD_V2_LOGIN_FALLBACK;
    }
  }

  const candidate = (rawUrl || '').trim();
  if (!candidate) {
    return safeFallback;
  }

  if (!candidate.startsWith('/') || candidate.startsWith('//') || candidate.includes('\\')) {
    return safeFallback;
  }

  try {
    const parsed = new URL(candidate, window.location.origin);
    if (parsed.origin !== window.location.origin) {
      return safeFallback;
    }
    const normalized = `${parsed.pathname}${parsed.search}${parsed.hash}`;
    if (!isAllowedInternalRedirectPath(parsed.pathname)) {
      return safeFallback;
    }
    return normalized;
  } catch {
    return safeFallback;
  }
}

// Helper to build URL with params
function buildUrl(endpoint: string, params: Record<string, string | number | boolean> = {}): string {
  const url = new URL(`${API_BASE}${endpoint}`, window.location.origin);
  const token = getPartnerToken();
  if (token) {
    url.searchParams.set('partner_token', token);
  }
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  });
  return url.toString();
}

function getBrowserTimezone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
  } catch {
    return 'UTC';
  }
}

// Generic fetch wrapper
async function fetchApi<T>(endpoint: string, params: Record<string, string | number | boolean> = {}, timeoutMs?: number): Promise<T> {
  const url = buildUrl(endpoint, params);

  const abortCtrl = timeoutMs ? new AbortController() : null;
  const timer = abortCtrl ? setTimeout(() => abortCtrl.abort(), timeoutMs!) : null;

  const response = await fetch(url, {
    headers: { 'Accept': 'application/json' },
    signal: abortCtrl?.signal,
  }).finally(() => {
    if (timer) clearTimeout(timer);
  });

  if (response.status === 401) {
    const unauthorized = await response.json().catch(() => null) as
      | { error?: string; loginUrl?: string }
      | null;
    if (unauthorized?.loginUrl) {
      window.location.href = sanitizeInternalRedirectUrl(
        unauthorized.loginUrl,
        DASHBOARD_V2_LOGIN_FALLBACK
      );
      throw new Error('Redirecting to Twitch login');
    }
    throw new Error(unauthorized?.error || 'Unauthorized');
  }

  if (!response.ok) {
    const error = await response.json().catch(() => null);
    throw new Error(error?.error || `Server-Fehler (HTTP ${response.status})`);
  }

  return response.json();
}

// API Functions

export async function fetchOverview(
  streamer: string | null,
  days: TimeRange
): Promise<DashboardOverview> {
  return fetchApi<DashboardOverview>('/overview', {
    streamer: streamer || '',
    days,
  });
}

export async function fetchMonthlyStats(
  streamer: string | null,
  months: number = 12
): Promise<MonthlyStats[]> {
  return fetchApi<MonthlyStats[]>('/monthly-stats', {
    streamer: streamer || '',
    months,
  });
}

export async function fetchWeekdayStats(
  streamer: string | null,
  days: TimeRange
): Promise<WeekdayStats[]> {
  return fetchApi<WeekdayStats[]>('/weekly-stats', {
    streamer: streamer || '',
    days,
  });
}

export async function fetchHourlyHeatmap(
  streamer: string | null,
  days: TimeRange
): Promise<HourlyHeatmapData[]> {
  return fetchApi<HourlyHeatmapData[]>('/hourly-heatmap', {
    streamer: streamer || '',
    days,
  });
}

export async function fetchCalendarHeatmap(
  streamer: string | null,
  days: number = 365
): Promise<CalendarHeatmapData[]> {
  return fetchApi<CalendarHeatmapData[]>('/calendar-heatmap', {
    streamer: streamer || '',
    days,
  });
}

export async function fetchChatAnalytics(
  streamer: string | null,
  days: TimeRange
): Promise<ChatAnalytics> {
  return fetchApi<ChatAnalytics>('/chat-analytics', {
    streamer: streamer || '',
    days,
    timezone: getBrowserTimezone(),
  });
}

export async function fetchViewerOverlap(
  streamer: string | null,
  limit: number = 20
): Promise<ViewerOverlap[]> {
  return fetchApi<ViewerOverlap[]>('/viewer-overlap', {
    streamer: streamer || '',
    limit,
  });
}

export async function fetchTagAnalysis(
  days: TimeRange,
  limit: number = 30
): Promise<TagPerformance[]> {
  return fetchApi<TagPerformance[]>('/tag-analysis', {
    days,
    limit,
  });
}

export async function fetchRankings(
  metric: 'viewers' | 'growth' | 'retention' | 'chat',
  days: TimeRange,
  limit: number = 20,
  excludeExternal = true
): Promise<RankingEntry[]> {
  return fetchApi<RankingEntry[]>('/rankings', {
    metric,
    days,
    limit,
    ...(excludeExternal && { exclude_external: '1' }),
  });
}

export async function fetchSessionDetail(
  sessionId: number
): Promise<StreamSession & { timeline: { minute: number; viewers: number }[]; chatters: { login: string; messages: number }[] }> {
  return fetchApi(`/session/${sessionId}`);
}

export async function fetchStreamerList(): Promise<{ login: string; isPartner: boolean }[]> {
  return fetchApi<{ login: string; isPartner: boolean }[]>('/streamers');
}

// Auth Status
export interface AuthStatus {
  authenticated: boolean;
  level: 'localhost' | 'admin' | 'partner' | 'none';
  isAdmin: boolean;
  isLocalhost: boolean;
  canViewAllStreamers: boolean;
  twitchLogin?: string | null;
  displayName?: string | null;
  permissions: {
    viewAllStreamers: boolean;
    viewComparison: boolean;
    viewChatAnalytics: boolean;
    viewOverlap: boolean;
  };
}

export async function fetchAuthStatus(): Promise<AuthStatus> {
  return fetchApi<AuthStatus>('/auth-status');
}

// Internal Home
export interface InternalHomeOAuthStatus {
  connected?: boolean;
  status?: 'connected' | 'partial' | 'missing' | 'error';
  grantedScopes?: string[];
  missingScopes?: string[];
  reconnectUrl?: string | null;
  profileUrl?: string | null;
  lastCheckedAt?: string | null;
}

export interface InternalHomeRaidStatus {
  active?: boolean;
  statusText?: string | null;
  note?: string | null;
  lastEventAt?: string | null;
}

export interface InternalHomeKpis30d {
  streams?: number;
  avgViewers?: number;
  followerDelta?: number;
  banKpi?: number;
}

export interface InternalHomeSession {
  id?: number | string;
  startedAt?: string | null;
  endedAt?: string | null;
  durationMinutes?: number | null;
  avgViewers?: number | null;
  peakViewers?: number | null;
  followerDelta?: number | null;
  title?: string | null;
  category?: string | null;
}

export interface InternalHomeImpactEntry {
  id?: number | string;
  timestamp?: string | null;
  title?: string | null;
  description?: string | null;
  metric?: string | null;
  severity?: 'success' | 'info' | 'warning' | 'critical' | string;
}

export interface InternalHomeData {
  greeting?: string | null;
  twitchLogin?: string | null;
  displayName?: string | null;
  loginUrl?: string | null;
  oauth?: InternalHomeOAuthStatus | null;
  raid?: InternalHomeRaidStatus | null;
  kpis30d?: InternalHomeKpis30d | null;
  recentStreams?: InternalHomeSession[] | null;
  impactFeed?: InternalHomeImpactEntry[] | null;
  generatedAt?: string | null;
}

interface InternalHomeRawOAuthStatus {
  connected?: boolean;
  status?: string;
  granted_scopes?: string[];
  missing_scopes?: string[];
  reconnect_url?: string | null;
  profile_url?: string | null;
  last_checked_at?: string | null;
}

interface InternalHomeRawRaidStatus {
  state?: string | null;
  read_only?: boolean;
}

interface InternalHomeRawProfile {
  twitch_login?: string | null;
  twitch_user_id?: string | null;
  display_name?: string | null;
}

interface InternalHomeRawKpis {
  streams_count?: number | null;
  avg_viewers?: number | null;
  follower_delta?: number | null;
  bot_bans_keyword_count?: number | null;
}

interface InternalHomeRawStream {
  started_at?: string | null;
  ended_at?: string | null;
  duration_seconds?: number | null;
  avg_viewers?: number | null;
  peak_viewers?: number | null;
  follower_delta?: number | null;
  title?: string | null;
}

interface InternalHomeRawImpactEvent {
  type?: string | null;
  timestamp?: string | null;
  target_login?: string | null;
  moderator_login?: string | null;
  reason?: string | null;
  viewer_count?: number | null;
  success?: boolean | null;
}

interface InternalHomeRawResponse {
  profile?: InternalHomeRawProfile | null;
  status?: {
    oauth?: InternalHomeRawOAuthStatus | null;
    raid_status?: InternalHomeRawRaidStatus | null;
  } | null;
  kpis?: InternalHomeRawKpis | null;
  recent_streams?: InternalHomeRawStream[] | null;
  bot_impact?: {
    events?: InternalHomeRawImpactEvent[] | null;
    note?: string | null;
  } | null;
  links?: {
    oauth_reconnect?: string | null;
    profile_status?: string | null;
  } | null;
  generated_at?: string | null;
}

function toFiniteNumber(value: unknown): number | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return undefined;
  }
  return numeric;
}

function mapImpactEntry(
  entry: InternalHomeRawImpactEvent,
  index: number
): InternalHomeImpactEntry {
  const eventType = String(entry.type || '').toLowerCase();
  const timestamp = String(entry.timestamp || '') || null;

  if (eventType === 'ban_keyword_hit') {
    const target = String(entry.target_login || '').trim();
    const moderator = String(entry.moderator_login || '').trim();
    const reason = String(entry.reason || '').trim();
    return {
      id: `ban-${index}`,
      timestamp,
      title: target ? `Ban gegen ${target}` : 'Bot-relevanter Ban',
      description: reason || (moderator ? `Mod: ${moderator}` : 'Ban-Reason nicht gesetzt'),
      metric: moderator ? `Moderator: ${moderator}` : null,
      severity: 'warning',
    };
  }

  if (eventType === 'raid_history') {
    const target = String(entry.target_login || '').trim();
    const viewers = toFiniteNumber(entry.viewer_count);
    const reason = String(entry.reason || '').trim();
    const success = entry.success !== false;
    return {
      id: `raid-${index}`,
      timestamp,
      title: target ? `Raid zu ${target}` : 'Raid-Aktivität',
      description: reason || (success ? 'Raid erfolgreich ausgeführt' : 'Raid nicht erfolgreich'),
      metric: viewers !== undefined ? `${viewers.toLocaleString('de-DE')} Viewer` : null,
      severity: success ? 'info' : 'warning',
    };
  }

  return {
    id: `event-${index}`,
    timestamp,
    title: 'Bot Update',
    description: 'Neues Bot-Ereignis',
    severity: 'info',
  };
}

export async function fetchInternalHome(): Promise<InternalHomeData> {
  const raw = await fetchApi<InternalHomeRawResponse>('/internal-home');
  const profile = raw.profile || {};
  const status = raw.status || {};
  const oauth = status.oauth || {};
  const raidStatus = status.raid_status || {};
  const kpis = raw.kpis || {};
  const links = raw.links || {};
  const loginUrl = sanitizeInternalRedirectUrl(
    links.oauth_reconnect || null,
    '/twitch/auth/login?next=%2Ftwitch%2Fdashboard'
  );
  const reconnectUrl = sanitizeInternalRedirectUrl(
    oauth.reconnect_url || links.oauth_reconnect || null,
    '/twitch/raid/auth'
  );
  const profileUrl = sanitizeInternalRedirectUrl(
    oauth.profile_url || links.profile_status || null,
    '/twitch/raid/requirements'
  );

  const impactEvents = (raw.bot_impact?.events || []).map(mapImpactEntry);
  const note = String(raw.bot_impact?.note || '').trim();
  if (note) {
    impactEvents.push({
      id: 'impact-note',
      timestamp: raw.generated_at || null,
      title: 'Hinweis',
      description: note,
      severity: 'info',
    });
  }

  const missingScopes = oauth.missing_scopes || [];
  const connected = Boolean(oauth.connected);
  const oauthStatus = String(oauth.status || '').toLowerCase();
  const normalizedOauthStatus: InternalHomeOAuthStatus['status'] =
    oauthStatus === 'connected' || oauthStatus === 'partial' || oauthStatus === 'missing'
      ? oauthStatus
      : connected
        ? 'connected'
        : missingScopes.length > 0
          ? 'missing'
          : 'partial';

  return {
    greeting: profile.display_name
      ? `Willkommen zurück, ${profile.display_name}`
      : profile.twitch_login
        ? `Willkommen zurück, ${profile.twitch_login}`
        : null,
    twitchLogin: profile.twitch_login || null,
    displayName: profile.display_name || profile.twitch_login || null,
    loginUrl,
    oauth: {
      connected,
      status: normalizedOauthStatus,
      grantedScopes: oauth.granted_scopes || [],
      missingScopes,
      reconnectUrl,
      profileUrl,
      lastCheckedAt: oauth.last_checked_at || raw.generated_at || null,
    },
    raid: {
      active: String(raidStatus.state || '').toLowerCase() === 'active',
      statusText: String(raidStatus.state || '').toLowerCase() === 'active' ? 'Auto-Raid: Aktiv' : 'Auto-Raid: Unbekannt',
      note: raidStatus.read_only ? 'Raid-Status ist schreibgeschützt (read-only).' : null,
      lastEventAt: impactEvents[0]?.timestamp || null,
    },
    kpis30d: {
      streams: toFiniteNumber(kpis.streams_count),
      avgViewers: toFiniteNumber(kpis.avg_viewers),
      followerDelta: toFiniteNumber(kpis.follower_delta),
      banKpi: toFiniteNumber(kpis.bot_bans_keyword_count),
    },
    recentStreams: (raw.recent_streams || []).map((stream, index) => ({
      id: `stream-${index}`,
      startedAt: stream.started_at || null,
      endedAt: stream.ended_at || null,
      durationMinutes:
        stream.duration_seconds === null || stream.duration_seconds === undefined
          ? null
          : Math.round(Number(stream.duration_seconds) / 60),
      avgViewers: toFiniteNumber(stream.avg_viewers),
      peakViewers: toFiniteNumber(stream.peak_viewers),
      title: stream.title || null,
      category: null,
      followerDelta: toFiniteNumber(stream.follower_delta),
    })),
    impactFeed: impactEvents,
    generatedAt: raw.generated_at || null,
  };
}

// Category Comparison
export async function fetchCategoryComparison(
  streamer: string | null,
  days: TimeRange,
  excludeExternal = true
): Promise<CategoryComparison> {
  return fetchApi('/category-comparison', {
    streamer: streamer || '',
    days,
    ...(excludeExternal && { exclude_external: '1' }),
  });
}

// Watch Time Distribution
export async function fetchWatchTimeDistribution(
  streamer: string | null,
  days: TimeRange
): Promise<WatchTimeDistribution> {
  return fetchApi<WatchTimeDistribution>('/watch-time-distribution', {
    streamer: streamer || '',
    days,
  });
}

// Follower Funnel
export async function fetchFollowerFunnel(
  streamer: string | null,
  days: TimeRange
): Promise<FollowerFunnel> {
  return fetchApi<FollowerFunnel>('/follower-funnel', {
    streamer: streamer || '',
    days,
  });
}

// Extended Tag Analysis with Trends
export async function fetchTagAnalysisExtended(
  streamer: string | null,
  days: TimeRange,
  limit: number = 20
): Promise<TagPerformanceExtended[]> {
  return fetchApi<TagPerformanceExtended[]>('/tag-analysis-extended', {
    streamer: streamer || '',
    days,
    limit,
  });
}

// Title Performance Analysis
export async function fetchTitlePerformance(
  streamer: string | null,
  days: TimeRange,
  limit: number = 20
): Promise<TitlePerformance[]> {
  return fetchApi<TitlePerformance[]>('/title-performance', {
    streamer: streamer || '',
    days,
    limit,
  });
}

// Combined Audience Insights (all in one call)
export async function fetchAudienceInsights(
  streamer: string | null,
  days: TimeRange
): Promise<AudienceInsights> {
  return fetchApi<AudienceInsights>('/audience-insights', {
    streamer: streamer || '',
    days,
  });
}

// Audience Demographics
export interface AudienceDemographicsResponse {
  viewerTypes: { label: string; percentage: number }[];
  activityPattern: 'weekend-heavy' | 'weekday-focused' | 'balanced';
  primaryLanguage: string;
  languageConfidence: number;
  peakActivityHours: number[];
  peakHoursMethod?: string;
  chatPenetrationPct?: number | null;
  chatPenetrationReliable?: boolean;
  messagesPer100ViewerMinutes?: number | null;
  viewerMinutes?: number;
  legacyInteractionActivePerAvgViewer?: number | null;
  interactiveRate: number;
  // Legacy fields (temporary compatibility)
  interactionRateActivePerViewer?: number;
  interactionRateActivePerAvgViewer?: number | null;
  interactionRateReliable?: boolean;
  loyaltyScore: number;
  timezone?: string;
  dataQuality?: {
    confidence: 'very_low' | 'low' | 'medium' | 'high';
    sessions?: number;
    method?: 'no_data' | 'low_coverage' | 'real_samples' | string;
    peakMethod?: 'no_data' | 'low_coverage' | 'real_samples' | string;
    coverage?: number;
    sampleCount?: number;
    peakSessionCount?: number;
    peakSessionsWithActivity?: number;
    interactiveSampleCount?: number;
    interactionCoverage?: number;
    chattersCoverage?: number;
    chattersApiCoverage?: number;
    passiveViewerSamples?: number;
    sessionsWithChat?: number;
    chatSessionCoverage?: number;
    viewerSampleCount?: number;
    viewerMinutesSource?: 'real_samples' | 'low_coverage' | string;
  };
}

export async function fetchAudienceDemographics(
  streamer: string | null,
  days: TimeRange
): Promise<AudienceDemographicsResponse> {
  return fetchApi<AudienceDemographicsResponse>('/audience-demographics', {
    streamer: streamer || '',
    days,
    timezone: getBrowserTimezone(),
  });
}

// Viewer Timeline (bucketed stats_tracked data)
export async function fetchViewerTimeline(
  streamer: string | null,
  days: number
): Promise<ViewerTimelinePoint[]> {
  return fetchApi<ViewerTimelinePoint[]>('/viewer-timeline', {
    streamer: streamer || '',
    days,
  });
}

// Coaching Data
export async function fetchCoaching(
  streamer: string,
  days: TimeRange
): Promise<CoachingData> {
  return fetchApi<CoachingData>('/coaching', {
    streamer,
    days,
  });
}

// Category Timings (Median-basiert, outlier-resistent)
export async function fetchCategoryTimings(
  days: TimeRange,
  source: 'category' | 'tracked' = 'category'
): Promise<CategoryTimings> {
  return fetchApi<CategoryTimings>('/category-timings', { days, source });
}

export async function fetchCategoryActivitySeries(
  days: TimeRange
): Promise<CategoryActivitySeries> {
  return fetchApi<CategoryActivitySeries>('/category-activity-series', { days });
}

// Monetization & Hype Train
export async function fetchMonetization(
  streamer: string | null,
  days: TimeRange
): Promise<MonetizationStats> {
  return fetchApi<MonetizationStats>('/monetization', {
    streamer: streamer || '',
    days,
  });
}

// Lurker Analysis
export async function fetchLurkerAnalysis(
  streamer: string | null,
  days: TimeRange
): Promise<LurkerAnalysis> {
  return fetchApi<LurkerAnalysis>('/lurker-analysis', {
    streamer: streamer || '',
    days,
  });
}

// Raid Retention
export async function fetchRaidRetention(
  streamer: string | null,
  days: TimeRange
): Promise<RaidRetention> {
  return fetchApi<RaidRetention>('/raid-retention', {
    streamer: streamer || '',
    days,
  });
}

// Viewer Profiles
export async function fetchViewerProfiles(
  streamer: string | null,
  days: TimeRange
): Promise<ViewerProfiles> {
  return fetchApi<ViewerProfiles>('/viewer-profiles', {
    streamer: streamer || '',
    days,
  });
}

// Audience Sharing
export async function fetchAudienceSharing(
  streamer: string | null,
  days: TimeRange
): Promise<AudienceSharing> {
  return fetchApi<AudienceSharing>('/audience-sharing', {
    streamer: streamer || '',
    days,
  });
}

// Viewer Directory (paginated, filtered, sorted)
export async function fetchViewerDirectory(
  streamer: string | null,
  sort: ViewerSortField = 'sessions',
  order: 'asc' | 'desc' = 'desc',
  filter: ViewerFilterType = 'all',
  search: string = '',
  page: number = 1,
  perPage: number = 50
): Promise<ViewerDirectory> {
  return fetchApi<ViewerDirectory>('/viewer-directory', {
    streamer: streamer || '',
    sort,
    order,
    filter,
    ...(search && { search }),
    page,
    per_page: perPage,
  });
}

// Viewer Detail (single viewer deep-dive)
export async function fetchViewerDetail(
  streamer: string | null,
  login: string
): Promise<ViewerDetail> {
  return fetchApi<ViewerDetail>('/viewer-detail', {
    streamer: streamer || '',
    login,
  });
}

// Viewer Segments (segmentation + churn risk)
export async function fetchViewerSegments(
  streamer: string | null
): Promise<ViewerSegments> {
  return fetchApi<ViewerSegments>('/viewer-segments', {
    streamer: streamer || '',
  });
}

// Chat Hype Timeline
export async function fetchChatHypeTimeline(
  streamer: string | null,
  sessionId?: number
): Promise<ChatHypeTimeline> {
  return fetchApi<ChatHypeTimeline>('/chat-hype-timeline', {
    streamer: streamer || '',
    ...(sessionId != null && { session_id: sessionId }),
  });
}

// Chat Content Analysis (hero mentions, topics, sentiment)
export async function fetchChatContentAnalysis(
  streamer: string | null,
  days: number
): Promise<ChatContentAnalysis> {
  return fetchApi<ChatContentAnalysis>('/chat-content-analysis', {
    streamer: streamer || '',
    days,
  });
}

// Chat Social Graph (@mentions network)
export async function fetchChatSocialGraph(
  streamer: string | null,
  days: number
): Promise<ChatSocialGraph> {
  return fetchApi<ChatSocialGraph>('/chat-social-graph', {
    streamer: streamer || '',
    days,
  });
}

// Category Leaderboard (top-N from stats_category)
export async function fetchCategoryLeaderboard(
  streamer: string | null,
  days: number,
  limit: number = 25,
  sort: 'avg' | 'peak' = 'avg',
  excludeExternal = false
): Promise<CategoryLeaderboard> {
  return fetchApi<CategoryLeaderboard>('/category-leaderboard', {
    streamer: streamer || '',
    days,
    limit,
    sort,
    ...(excludeExternal && { exclude_external: '1' }),
  });
}

// ========= Experimental (Labor) API Functions =========

export async function fetchExpOverview(
  streamer: string,
  days: number
): Promise<ExpOverview> {
  return fetchApi<ExpOverview>("/exp/overview", { streamer, days });
}

export async function fetchExpGameBreakdown(
  streamer: string,
  days: number
): Promise<ExpGameBreakdown[]> {
  return fetchApi<ExpGameBreakdown[]>("/exp/game-breakdown", { streamer, days });
}

export async function fetchExpGameTransitions(
  streamer: string,
  days: number
): Promise<ExpGameTransition[]> {
  return fetchApi<ExpGameTransition[]>("/exp/game-transitions", { streamer, days });
}

export async function fetchExpGrowthCurves(
  streamer: string,
  days: number
): Promise<ExpGrowthCurve[]> {
  return fetchApi<ExpGrowthCurve[]>("/exp/growth-curves", { streamer, days });
}

// ========= KI Analyse (AI Analysis) =========

export async function fetchAIAnalysis(
  streamer: string,
  days: number,
  gameFilter: 'deadlock' | 'all' = 'all'
): Promise<AIAnalysisResult> {
  return fetchApi<AIAnalysisResult>('/ai/analysis', { streamer, days, game_filter: gameFilter }, 240_000);
}

export async function fetchAIHistory(
  streamer: string,
  limit = 20
): Promise<AIHistoryEntry[]> {
  return fetchApi<AIHistoryEntry[]>('/ai/history', { streamer, limit });
}
