export interface AdminUserInfo {
  displayName?: string;
  username?: string;
  login?: string;
  userId?: string;
  authType?: string;
}

export interface AdminAuthStatus {
  authenticated: boolean;
  authLevel?: string;
  isAdmin?: boolean;
  isLocalhost?: boolean;
  loginUrl?: string;
  discordLoginUrl?: string;
  csrfToken?: string;
  user?: AdminUserInfo;
  permissions?: Record<string, unknown>;
}

export interface StreamerRow {
  login: string;
  displayName?: string;
  twitchUserId?: string;
  verified?: boolean;
  archived?: boolean;
  isLive?: boolean;
  viewerCount?: number;
  activeSessionId?: number | null;
  lastSeenAt?: string | null;
  planId?: string;
  promoDisabled?: boolean;
  notes?: string;
  status?: string;
  raw?: Record<string, unknown>;
}

export interface SessionSummary {
  sessionId?: number;
  startedAt?: string;
  endedAt?: string;
  title?: string;
  category?: string;
  averageViewers?: number;
  peakViewers?: number;
  watchTimeHours?: number;
  followerDelta?: number;
}

export interface StreamerDetail {
  login: string;
  displayName?: string;
  twitchUserId?: string;
  verified?: boolean;
  archived?: boolean;
  isLive?: boolean;
  planId?: string;
  stats?: Record<string, unknown>;
  settings?: Record<string, unknown>;
  sessions?: SessionSummary[];
  recentActivity?: Record<string, unknown>[];
  raw?: Record<string, unknown>;
}

export interface InternalHomeMetric {
  label: string;
  value: string | number;
  hint?: string;
}

export interface InternalHomeOverview {
  metrics?: InternalHomeMetric[];
  actions?: Record<string, unknown>[];
  recentActivity?: Record<string, unknown>[];
  raw?: Record<string, unknown>;
}

export interface SystemHealth {
  uptimeSeconds?: number;
  memoryBytes?: number;
  memoryRssBytes?: number;
  pythonVersion?: string;
  processId?: number;
  lastTickAt?: string;
  lastTickAgeSeconds?: number;
  serviceWarnings?: Record<string, unknown>[];
  raw?: Record<string, unknown>;
}

export interface EventSubSubscription {
  id?: string;
  type?: string;
  status?: string;
  transport?: string;
  createdAt?: string;
  cost?: number;
  condition?: Record<string, unknown>;
}

export interface EventSubStatusResponse {
  websocketStatus?: string;
  websocketSessionId?: string;
  websocketConnectedAt?: string;
  websocketReconnectedAt?: string;
  activeSubscriptionCount?: number;
  capacity?: {
    used?: number;
    max?: number;
    remaining?: number;
    lastSnapshotAt?: string;
  };
  subscriptions?: EventSubSubscription[];
  raw?: Record<string, unknown>;
}

export interface DatabaseTableStat {
  table: string;
  rowCount?: number;
  sizeBytes?: number;
  updatedAt?: string;
}

export interface DatabaseStatsResponse {
  databaseSizeBytes?: number;
  tables?: DatabaseTableStat[];
  raw?: Record<string, unknown>;
}

export interface ErrorLogEntry {
  id: string;
  timestamp?: string;
  level?: string;
  source?: string;
  message: string;
  context?: string;
}

export interface ErrorLogsResponse {
  page: number;
  pageSize: number;
  total?: number;
  hasMore?: boolean;
  entries: ErrorLogEntry[];
}

export interface ConfigOverview {
  promo?: Record<string, unknown>;
  polling?: Record<string, unknown>;
  raids?: Record<string, unknown>;
  chat?: Record<string, unknown>;
  announcements?: Record<string, unknown>;
  csrfToken?: string;
  raw?: Record<string, unknown>;
}

export interface SubscriptionRecord {
  login?: string;
  customerReference?: string;
  planId?: string;
  status?: string;
  trialEndsAt?: string;
  currentPeriodEnd?: string;
  updatedAt?: string;
  priceLabel?: string;
  raw?: Record<string, unknown>;
}

export interface AffiliateRecord {
  twitchLogin?: string;
  stripeAccountId?: string;
  status?: string;
  payoutEmail?: string;
  commissionRate?: number;
  updatedAt?: string;
  raw?: Record<string, unknown>;
}

export interface AdminActionResult {
  ok: boolean;
  message: string;
  redirectUrl?: string;
}
