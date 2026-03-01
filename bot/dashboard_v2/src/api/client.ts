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
      window.location.href = unauthorized.loginUrl;
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
