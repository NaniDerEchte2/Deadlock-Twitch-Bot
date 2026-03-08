import type {
  AdminActionResult,
  AdminAuthStatus,
  AdminConfigScope,
  AffiliateRecord,
  ChatConfigSnapshot,
  ChatConfigUpdatePayload,
  ConfigOverview,
  DatabaseStatsResponse,
  ErrorLogsResponse,
  EventSubStatusResponse,
  InternalHomeOverview,
  RaidConfigSnapshot,
  RaidConfigUpdatePayload,
  StreamerDetail,
  StreamerRow,
  SubscriptionRecord,
  SystemHealth,
} from '@/api/types';
import { coerceArray, coerceRecord } from '@/utils/formatters';

const ADMIN_API_BASE = '/twitch/api/admin';
const AUTH_STATUS_URL = '/twitch/api/v2/auth-status';
const INTERNAL_HOME_URL = '/twitch/api/v2/internal-home';
const LEGACY_CSRF_PAGE = '/twitch/admin/announcements';
let cachedCsrfToken = '';

export class ApiError extends Error {
  status: number;
  payload?: unknown;

  constructor(message: string, status: number, payload?: unknown) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.payload = payload;
  }
}

function sanitizeNextPath(rawPath: string): string {
  if (!rawPath || !rawPath.startsWith('/') || rawPath.startsWith('//') || rawPath.includes('\\')) {
    return '/twitch/admin';
  }
  return rawPath;
}

export function buildDiscordAdminLoginUrl(nextPath?: string): string {
  const next = sanitizeNextPath(nextPath || `${window.location.pathname}${window.location.search}`);
  return `/twitch/auth/discord/login?next=${encodeURIComponent(next)}`;
}

async function parsePayload(response: Response): Promise<unknown> {
  const contentType = response.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    return response.json().catch(() => null);
  }
  return response.text().catch(() => '');
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    credentials: 'include',
    headers: {
      Accept: 'application/json',
      ...(init?.headers ?? {}),
    },
    ...init,
  });
  const payload = await parsePayload(response);
  if (!response.ok) {
    const record = coerceRecord(payload);
    const message =
      typeof record.message === 'string'
        ? record.message
        : typeof record.error === 'string'
          ? record.error
          : `HTTP ${response.status}`;
    if (response.status === 401 || response.status === 403) {
      const loginUrl =
        typeof record.loginUrl === 'string'
          ? record.loginUrl
          : typeof record.discordLoginUrl === 'string'
            ? record.discordLoginUrl
            : buildDiscordAdminLoginUrl();
      throw new ApiError(loginUrl, response.status, payload);
    }
    throw new ApiError(message, response.status, payload);
  }
  return payload as T;
}

function admin<T>(suffix: string, init?: RequestInit) {
  return request<T>(`${ADMIN_API_BASE}${suffix}`, init);
}

function cacheCsrfToken(token: string | undefined | null): string {
  const normalized = typeof token === 'string' ? token.trim() : '';
  if (normalized) {
    cachedCsrfToken = normalized;
  }
  return normalized;
}

function readString(record: Record<string, unknown>, ...keys: string[]): string {
  for (const key of keys) {
    if (typeof record[key] === 'string') {
      return String(record[key]);
    }
  }
  return '';
}

function readNumber(record: Record<string, unknown>, ...keys: string[]): number | undefined {
  for (const key of keys) {
    const rawValue = record[key];
    if (rawValue === undefined || rawValue === null || rawValue === '') {
      continue;
    }
    const candidate = Number(rawValue);
    if (Number.isFinite(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

function readBoolean(record: Record<string, unknown>, ...keys: string[]): boolean | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === 'boolean') {
      return value;
    }
    if (typeof value === 'number') {
      return value !== 0;
    }
    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (normalized) {
        return !['0', 'false', 'off', 'no'].includes(normalized);
      }
    }
  }
  return undefined;
}

function readScope(record: Record<string, unknown>, ...keys: string[]): AdminConfigScope | undefined {
  const candidate = readString(record, ...keys).trim().toLowerCase();
  if (candidate === 'active' || candidate === 'all') {
    return candidate;
  }
  return undefined;
}

function parseRaidSnapshot(record: Record<string, unknown>): RaidConfigSnapshot {
  return {
    totalManagedStreamers: readNumber(record, 'totalManagedStreamers', 'total_managed_streamers'),
    raidBotEnabledCount: readNumber(record, 'raidBotEnabledCount', 'raid_bot_enabled_count'),
    livePingEnabledCount: readNumber(record, 'livePingEnabledCount', 'live_ping_enabled_count'),
    allRaidBotEnabled: readBoolean(record, 'allRaidBotEnabled', 'all_raid_bot_enabled'),
    allLivePingEnabled: readBoolean(record, 'allLivePingEnabled', 'all_live_ping_enabled'),
    scope: readScope(record, 'scope', 'defaultScope', 'default_scope'),
    raw: record,
  };
}

function parseChatSnapshot(record: Record<string, unknown>): ChatConfigSnapshot {
  return {
    totalManagedStreamers: readNumber(record, 'totalManagedStreamers', 'total_managed_streamers'),
    silentBanCount: readNumber(record, 'silentBanCount', 'silent_ban_count'),
    silentRaidCount: readNumber(record, 'silentRaidCount', 'silent_raid_count'),
    allSilentBan: readBoolean(record, 'allSilentBan', 'all_silent_ban'),
    allSilentRaid: readBoolean(record, 'allSilentRaid', 'all_silent_raid'),
    scope: readScope(record, 'scope', 'defaultScope', 'default_scope'),
    raw: record,
  };
}

export async function fetchAuthStatus(): Promise<AdminAuthStatus> {
  try {
    const payload = await request<Record<string, unknown>>(AUTH_STATUS_URL);
    const csrfToken = cacheCsrfToken(readString(payload, 'csrfToken', 'csrf_token')) || undefined;
    const authLevel = readString(payload, 'authLevel', 'auth_level').toLowerCase();
    const isAdmin = Boolean(payload.isAdmin ?? payload.is_admin) || authLevel === 'admin';
    const isLocalhost =
      Boolean(payload.isLocalhost ?? payload.is_localhost) || authLevel === 'localhost';
    return {
      authenticated: Boolean(payload.authenticated) || isAdmin || isLocalhost || !!authLevel,
      authLevel,
      isAdmin,
      isLocalhost,
      loginUrl: readString(payload, 'loginUrl', 'login_url') || buildDiscordAdminLoginUrl(),
      discordLoginUrl:
        readString(payload, 'discordLoginUrl', 'discord_login_url', 'loginUrl', 'login_url') ||
        buildDiscordAdminLoginUrl(),
      csrfToken,
      user: {
        displayName: readString(payload, 'displayName', 'display_name', 'twitchLogin', 'login') || undefined,
        login: readString(payload, 'twitchLogin', 'login') || undefined,
        authType: readString(payload, 'authType', 'auth_type') || undefined,
      },
      permissions: coerceRecord(payload.permissions),
    };
  } catch (error) {
    if (error instanceof ApiError) {
      return {
        authenticated: false,
        loginUrl: error.message || buildDiscordAdminLoginUrl(),
        discordLoginUrl: error.message || buildDiscordAdminLoginUrl(),
      };
    }
    throw error;
  }
}

export async function fetchDashboardOverview(): Promise<InternalHomeOverview> {
  const payload = await request<Record<string, unknown>>(INTERNAL_HOME_URL);
  return {
    metrics: coerceArray(payload.metrics) as InternalHomeOverview['metrics'],
    actions: coerceArray(payload.actions),
    recentActivity: coerceArray(payload.recentActivity ?? payload.recent_activity ?? payload.activity),
    raw: payload,
  };
}

export async function fetchAdminStreamers(): Promise<StreamerRow[]> {
  const payload = await admin<unknown>('/streamers');
  const rows = Array.isArray(payload)
    ? payload
    : coerceArray<Record<string, unknown>>(coerceRecord(payload).items ?? coerceRecord(payload).streamers);
  return rows.map((row) => {
    const record = coerceRecord(row);
    return {
      login: readString(record, 'login', 'twitch_login', 'streamer_login'),
      displayName: readString(record, 'displayName', 'display_name', 'login') || undefined,
      twitchUserId: readString(record, 'twitchUserId', 'twitch_user_id') || undefined,
      verified: readBoolean(record, 'verified', 'is_verified'),
      archived: readBoolean(record, 'archived', 'is_archived'),
      isLive: readBoolean(record, 'isLive', 'is_live'),
      viewerCount: readNumber(record, 'viewerCount', 'viewer_count'),
      activeSessionId: readNumber(record, 'activeSessionId', 'active_session_id') ?? null,
      lastSeenAt: readString(record, 'lastSeenAt', 'last_seen_at') || null,
      planId: readString(record, 'planId', 'plan_id') || undefined,
      promoDisabled: readBoolean(record, 'promoDisabled', 'promo_disabled'),
      notes: readString(record, 'notes', 'manual_plan_notes') || undefined,
      status: readString(record, 'status') || undefined,
      raw: record,
    };
  });
}

export async function fetchAdminStreamerDetail(login: string): Promise<StreamerDetail> {
  const payload = await admin<Record<string, unknown>>(`/streamers/${encodeURIComponent(login)}`);
  const sessions = coerceArray<Record<string, unknown>>(payload.sessions).map((session) => ({
    sessionId: readNumber(session, 'sessionId', 'session_id'),
    startedAt: readString(session, 'startedAt', 'started_at') || undefined,
    endedAt: readString(session, 'endedAt', 'ended_at') || undefined,
    title: readString(session, 'title') || undefined,
    category: readString(session, 'category', 'game_name') || undefined,
    averageViewers: readNumber(session, 'averageViewers', 'avg_viewers'),
    peakViewers: readNumber(session, 'peakViewers', 'peak_viewers'),
    watchTimeHours: readNumber(session, 'watchTimeHours', 'watch_time_hours', 'watch_hours'),
    followerDelta: readNumber(session, 'followerDelta', 'follower_delta'),
  }));
  return {
    login: readString(payload, 'login', 'twitch_login') || login,
    displayName: readString(payload, 'displayName', 'display_name', 'login') || undefined,
    twitchUserId: readString(payload, 'twitchUserId', 'twitch_user_id') || undefined,
    verified: readBoolean(payload, 'verified', 'is_verified'),
    archived: readBoolean(payload, 'archived', 'is_archived'),
    isLive: readBoolean(payload, 'isLive', 'is_live'),
    planId: readString(payload, 'planId', 'plan_id') || undefined,
    stats: coerceRecord(payload.stats),
    settings: coerceRecord(payload.settings),
    sessions,
    recentActivity: coerceArray(payload.recentActivity ?? payload.recent_activity),
    raw: payload,
  };
}

export async function fetchSystemHealth(): Promise<SystemHealth> {
  const payload = await admin<Record<string, unknown>>('/system/health');
  return {
    uptimeSeconds: readNumber(payload, 'uptimeSeconds', 'uptime_seconds', 'uptime'),
    memoryBytes: readNumber(payload, 'memoryBytes', 'memory_bytes'),
    memoryRssBytes: readNumber(payload, 'memoryRssBytes', 'memory_rss_bytes'),
    pythonVersion: readString(payload, 'pythonVersion', 'python_version') || undefined,
    processId: readNumber(payload, 'processId', 'pid'),
    lastTickAt: readString(payload, 'lastTickAt', 'last_tick_at') || undefined,
    lastTickAgeSeconds: readNumber(payload, 'lastTickAgeSeconds', 'last_tick_age_seconds'),
    serviceWarnings: coerceArray(payload.serviceWarnings ?? payload.service_warnings),
    raw: payload,
  };
}

export async function fetchEventSubStatus(): Promise<EventSubStatusResponse> {
  return admin<EventSubStatusResponse>('/system/eventsub');
}

export async function fetchDatabaseStats(): Promise<DatabaseStatsResponse> {
  return admin<DatabaseStatsResponse>('/system/database');
}

export async function fetchErrorLogs(page = 1, pageSize = 25): Promise<ErrorLogsResponse> {
  return admin<ErrorLogsResponse>(`/system/errors?page=${page}&page_size=${pageSize}`);
}

export async function fetchConfigOverview(scope?: AdminConfigScope): Promise<ConfigOverview> {
  const query = scope ? `?scope=${encodeURIComponent(scope)}` : '';
  const payload = await admin<Record<string, unknown>>(`/config/overview${query}`);
  const csrfToken = cacheCsrfToken(readString(payload, 'csrfToken', 'csrf_token')) || undefined;
  return {
    promo: coerceRecord(payload.promo),
    polling: coerceRecord(payload.polling),
    raids: parseRaidSnapshot(coerceRecord(payload.raids)),
    chat: parseChatSnapshot(coerceRecord(payload.chat)),
    announcements: coerceRecord(payload.announcements),
    csrfToken,
    raw: payload,
  };
}

function readBodyCsrfToken(body: Record<string, unknown>): string {
  const rawToken = body.csrf_token ?? body.csrfToken;
  return typeof rawToken === 'string' ? rawToken.trim() : '';
}

async function resolveJsonCsrfToken(body: Record<string, unknown>): Promise<string> {
  const explicitToken = cacheCsrfToken(readBodyCsrfToken(body));
  if (explicitToken) {
    return explicitToken;
  }
  if (cachedCsrfToken) {
    return cachedCsrfToken;
  }

  try {
    const auth = await fetchAuthStatus();
    const authToken = cacheCsrfToken(auth.csrfToken);
    if (authToken) {
      return authToken;
    }
  } catch {
    // Fall back to the legacy admin page token read below.
  }

  return cacheCsrfToken(await fetchLegacyCsrfToken());
}

async function postAdminJson<T, TBody extends object = Record<string, unknown>>(path: string, body: TBody) {
  const normalizedBody = coerceRecord(body);
  const csrfToken = await resolveJsonCsrfToken(normalizedBody);
  const payload = { ...normalizedBody, csrf_token: csrfToken };
  return admin<T>(path, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-CSRF-Token': csrfToken,
    },
    body: JSON.stringify(payload),
  });
}

export async function updatePromoConfig(body: Record<string, unknown>) {
  return postAdminJson<Record<string, unknown>>('/config/promo', body);
}

export async function updatePollingConfig(body: Record<string, unknown>) {
  return postAdminJson<Record<string, unknown>>('/config/polling', body);
}

export async function updateRaidConfig(body: RaidConfigUpdatePayload) {
  return postAdminJson<Record<string, unknown>, RaidConfigUpdatePayload>('/config/raids', body);
}

export async function updateChatConfig(body: ChatConfigUpdatePayload) {
  return postAdminJson<Record<string, unknown>, ChatConfigUpdatePayload>('/config/chat', body);
}

export async function fetchSubscriptions(): Promise<SubscriptionRecord[]> {
  const payload = await admin<unknown>('/billing/subscriptions');
  return Array.isArray(payload)
    ? (payload as SubscriptionRecord[])
    : (coerceArray(coerceRecord(payload).items ?? coerceRecord(payload).subscriptions) as SubscriptionRecord[]);
}

export async function fetchAffiliates(): Promise<AffiliateRecord[]> {
  const payload = await admin<unknown>('/billing/affiliates');
  return Array.isArray(payload)
    ? (payload as AffiliateRecord[])
    : (coerceArray(coerceRecord(payload).items ?? coerceRecord(payload).affiliates) as AffiliateRecord[]);
}

export async function fetchLegacyCsrfToken(): Promise<string> {
  if (cachedCsrfToken) {
    return cachedCsrfToken;
  }
  const response = await fetch(LEGACY_CSRF_PAGE, {
    credentials: 'include',
    headers: { Accept: 'text/html' },
  });
  const html = await response.text();
  const match =
    html.match(/name=["']csrf_token["'][^>]*value=["']([^"']+)["']/i) ??
    html.match(/value=["']([^"']+)["'][^>]*name=["']csrf_token["']/i);
  if (!match?.[1]) {
    throw new ApiError('CSRF-Token konnte nicht gelesen werden.', 500);
  }
  return cacheCsrfToken(match[1]);
}

async function submitLegacyAction(path: string, fields: Record<string, string>): Promise<AdminActionResult> {
  const csrfToken = fields.csrf_token || cachedCsrfToken || (await fetchLegacyCsrfToken());
  const body = new URLSearchParams({ ...fields, csrf_token: csrfToken });
  const response = await fetch(path, {
    method: 'POST',
    credentials: 'include',
    headers: {
      Accept: 'text/html',
      'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
    },
    body,
    redirect: 'follow',
  });
  if (!response.ok) {
    throw new ApiError(`Aktion fehlgeschlagen (${response.status})`, response.status);
  }
  const finalUrl = new URL(response.url, window.location.origin);
  return {
    ok: !finalUrl.searchParams.get('err'),
    message: finalUrl.searchParams.get('ok') || finalUrl.searchParams.get('err') || 'Aktion ausgeführt.',
    redirectUrl: `${finalUrl.pathname}${finalUrl.search}`,
  };
}

export function addStreamer(login: string) {
  return submitLegacyAction('/twitch/add_any', { login });
}

export function removeStreamer(login: string) {
  return submitLegacyAction('/twitch/remove', { login });
}

export function verifyStreamer(login: string, mode: 'verified' | 'unverified' = 'verified') {
  return submitLegacyAction('/twitch/verify', { login, mode });
}

export function archiveStreamer(login: string, mode: 'archive' | 'unarchive' | 'toggle' = 'toggle') {
  return submitLegacyAction('/twitch/archive', { login, mode });
}
