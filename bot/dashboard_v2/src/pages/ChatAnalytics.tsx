import { motion } from 'framer-motion';
import { MessageCircle, Users, Heart, TrendingUp, AlertCircle, Loader2, Award, Info } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { useId } from 'react';
import { fetchChatAnalytics } from '@/api/client';
import { useViewerProfiles, useCoaching } from '@/hooks/useAnalytics';
import { ViewerProfiles } from '@/components/charts/ViewerProfiles';
import type { ChatAnalytics as ChatAnalyticsType, CoachingData } from '@/types/analytics';
import {
  CHAT_AUDIENCE_TOOLTIP,
  normalizeHourlyActivity,
  resolveChatPenetration,
  resolveMessagesPer100ViewerMinutes,
  resolveQualityMethod,
} from '@/utils/engagementKpi';

import type { TimeRange } from '@/types/analytics';

interface ChatAnalyticsProps {
  streamer: string;
  days: TimeRange;
}

export function ChatAnalytics({ streamer, days }: ChatAnalyticsProps) {
  const { data, isLoading } = useQuery<ChatAnalyticsType>({
    queryKey: ['chatAnalytics', streamer, days],
    queryFn: () => fetchChatAnalytics(streamer, days),
    enabled: !!streamer,
  });

  const { data: viewerProfilesData } = useViewerProfiles(streamer, days);
  const { data: coachingData } = useCoaching(streamer, days);

  if (!streamer) {
    return (
      <div className="flex flex-col items-center justify-center h-64">
        <AlertCircle className="w-12 h-12 text-text-secondary mb-4" />
        <p className="text-text-secondary text-lg">Wähle einen Streamer aus</p>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    );
  }

  if (!data) {
    return (
      <div className="flex flex-col items-center justify-center h-64">
        <MessageCircle className="w-12 h-12 text-text-secondary mb-4" />
        <p className="text-text-secondary text-lg">Keine Chat-Daten verfügbar</p>
      </div>
    );
  }

  const totalChatters = data.totalChatterSessions ?? data.uniqueChatters ?? 0;
  const totalTrackedViewers = data.totalTrackedViewers ?? totalChatters;
  const firstTimeChatters = data.firstTimeChatters ?? 0;
  const returningChatters = data.returningChatters ?? Math.max(0, totalChatters - firstTimeChatters);
  const messagesPer100ViewerMinutes = resolveMessagesPer100ViewerMinutes(data);
  const chatterReturnRate =
    data.chatterReturnRate ?? (totalChatters ? (returningChatters / totalChatters) * 100 : 0);
  const penetration = resolveChatPenetration(data);
  const interactionRate = penetration.value;
  const interactionRateReliable = penetration.reliable;
  const interactionCoverage = penetration.coverage;
  const hourlyActivity = normalizeHourlyActivity(data.hourlyActivity);
  const hasHourlySamples = hourlyActivity.some((h) => h.count > 0);
  const maxHourlyCount = Math.max(1, ...hourlyActivity.map((h) => h.count));
  const dataMethod = resolveQualityMethod(data.dataQuality?.method, data.totalMessages > 0);

  // Community loyalty state detection
  const noReturnHistory = chatterReturnRate === 0 && firstTimeChatters >= totalChatters && totalChatters > 0;
  const chattersApiInactive = interactionCoverage === 0 && totalTrackedViewers > 0;

  return (
    <div className="space-y-6">
      {dataMethod !== 'real_samples' && (
        <div className="panel-card rounded-2xl p-4 text-sm text-text-secondary">
          Datenqualität eingeschränkt: mindestens eine KPI basiert auf Low-Coverage/Fallback-Samples.
        </div>
      )}
      {chattersApiInactive ? (
        <div className="panel-card rounded-2xl p-4 text-sm flex items-start gap-3 text-text-secondary">
          <Info className="w-4 h-4 text-primary shrink-0 mt-0.5" />
          <div>
            <span className="text-white font-medium">Chatters-API nicht aktiv</span>
            <span className="ml-2">— Chat Penetration kann nicht berechnet werden. Daten stammen nur aus Chat-Nachrichten (kein passive Viewer Tracking).</span>
          </div>
        </div>
      ) : !interactionRateReliable && totalTrackedViewers > 0 && (
        <div className="panel-card rounded-2xl p-4 text-sm text-text-secondary">
          Chat Penetration ist derzeit nicht belastbar: passive Samples oder Chatters-Coverage sind zu gering ({(interactionCoverage * 100).toFixed(1)}% Coverage).
        </div>
      )}

      {/* Overview Stats - 4-column grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          icon={<Users className="w-5 h-5" />}
          label="Aktive Chatters"
          value={totalChatters.toLocaleString('de-DE')}
          subtext={`Getrackte Chat-Accounts: ${totalTrackedViewers.toLocaleString('de-DE')}`}
          color="primary"
        />
        <StatCard
          icon={<TrendingUp className="w-5 h-5" />}
          label="Chat Penetration"
          value={
            chattersApiInactive
              ? data.legacyInteractionActivePerAvgViewer != null
                ? `${(data.legacyInteractionActivePerAvgViewer * 100).toFixed(1)}%`
                : 'N/A'
              : interactionRateReliable && interactionRate !== null
                ? `${interactionRate.toFixed(1)}%`
                : 'Nicht belastbar'
          }
          subtext={
            chattersApiInactive
              ? data.legacyInteractionActivePerAvgViewer != null
                ? 'Fallback: aktive Chatter / Ø Viewer (eingeschränkt)'
                : 'Chatters-API nicht aktiv'
              : `Coverage ${(interactionCoverage * 100).toFixed(1)}% · ${CHAT_AUDIENCE_TOOLTIP}`
          }
          color="accent"
        />
        <StatCard
          icon={<MessageCircle className="w-5 h-5" />}
          label="Messages pro 100 Viewer-Minuten"
          value={
            messagesPer100ViewerMinutes !== null
              ? messagesPer100ViewerMinutes.toFixed(1)
              : '-'
          }
          subtext={
            data.viewerMinutes && data.viewerMinutes > 0
              ? `Basis: ${data.viewerMinutes.toFixed(0)} Viewer-Minuten`
              : 'Keine Viewer-Minuten im Zeitraum'
          }
          color="success"
        />
        <StatCard
          icon={<Heart className="w-5 h-5" />}
          label="Wiederkehrende Chatters"
          value={returningChatters.toLocaleString('de-DE')}
          subtext={`${chatterReturnRate.toFixed(1)}% Return Rate · Erstmalig: ${firstTimeChatters.toLocaleString('de-DE')}`}
          color="warning"
        />
      </div>

      {/* Chatter Loyalty Distribution */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="panel-card rounded-2xl p-6"
      >
        <div className="flex items-center gap-3 mb-6">
          <Heart className="w-6 h-6 text-primary" />
          <h2 className="text-xl font-bold text-white">Community-Treue</h2>
        </div>

        {noReturnHistory ? (
          <div className="flex items-center gap-3 p-4 rounded-xl bg-primary/10 border border-primary/20">
            <Info className="w-5 h-5 text-primary shrink-0" />
            <div>
              <p className="text-white font-medium text-sm">Noch zu wenig Historie</p>
              <p className="text-text-secondary text-sm mt-0.5">
                Alle {totalChatters} Chatter wurden erstmalig gesehen. Sobald sie wiederkehren, werden Return-Rate und Stammzuschauer berechnet.
              </p>
            </div>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <LoyaltyGauge
              label="Neue Zuschauer"
              percentage={(firstTimeChatters / Math.max(1, totalChatters)) * 100}
              description="Chatten zum ersten Mal"
              startColor="var(--color-accent)"
              endColor="var(--color-primary)"
            />
            {returningChatters > 0 ? (
              <LoyaltyGauge
                label="Stammzuschauer"
                percentage={chatterReturnRate}
                description="Kommen regelmäßig zurück"
                startColor="var(--color-success)"
                endColor="var(--color-accent)"
              />
            ) : (
              <div className="text-center">
                <div className="relative w-32 h-32 mx-auto mb-3 flex items-center justify-center rounded-full border-4 border-border">
                  <span className="text-text-secondary text-sm text-center px-2">Noch keine<br/>Stammzuschauer</span>
                </div>
                <div className="font-medium text-white">Stammzuschauer</div>
                <div className="text-sm text-text-secondary">Kommen regelmäßig zurück</div>
              </div>
            )}
            <LoyaltyGauge
              label="Chat Penetration"
              percentage={
                chattersApiInactive
                  ? data.legacyInteractionActivePerAvgViewer != null
                    ? data.legacyInteractionActivePerAvgViewer * 100
                    : 0
                  : interactionRateReliable && interactionRate !== null
                    ? interactionRate
                    : 0
              }
              description={
                chattersApiInactive
                  ? 'Fallback-Metrik (eingeschränkt)'
                  : interactionRateReliable
                    ? 'Aktive Chatters / getrackte Chat-Accounts'
                    : `Nicht belastbar (${(interactionCoverage * 100).toFixed(1)}% Coverage)`
              }
              startColor="var(--color-primary)"
              endColor="var(--color-success)"
            />
          </div>
        )}
      </motion.div>

      {/* Message Analysis */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Message Types */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
          className="panel-card rounded-2xl p-6"
        >
          <div className="flex items-center gap-3 mb-6">
            <MessageCircle className="w-6 h-6 text-accent" />
            <h2 className="text-xl font-bold text-white">Nachrichtentypen</h2>
          </div>
          <div className="space-y-4">
            {data.messageTypes?.map((type) => (
              <div key={type.type}>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-text-secondary">{type.type}</span>
                  <span className="text-white font-medium">{type.percentage}% ({type.count})</span>
                </div>
                <div className="h-2 bg-background/80 rounded-full overflow-hidden">
                  <motion.div
                    initial={{ width: 0 }}
                    animate={{ width: `${type.percentage}%` }}
                    transition={{ duration: 1, ease: "easeOut" }}
                    className="h-full bg-gradient-to-r from-primary to-accent"
                  />
                </div>
              </div>
            ))}
            {(!data.messageTypes || data.messageTypes.length === 0) && (
               <p className="text-text-secondary text-center py-4">Keine Daten verfügbar</p>
            )}
          </div>
        </motion.div>

        {/* Hourly Activity */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.15 }}
          className="panel-card rounded-2xl p-6"
        >
          <div className="flex items-center gap-3 mb-6">
            <TrendingUp className="w-6 h-6 text-success" />
            <h2 className="text-xl font-bold text-white">Aktivität nach Tageszeit</h2>
            <span className="text-[11px] text-text-secondary border border-border/60 rounded-full px-2 py-0.5">
              {data.timezone || 'UTC'}
            </span>
          </div>
          {hasHourlySamples ? (
            <div className="h-64 flex items-end gap-1">
              {Array.from({ length: 24 }).map((_, hour) => {
                const stat = hourlyActivity.find(h => h.hour === hour);
                const count = stat?.count || 0;
                const height = (count / maxHourlyCount) * 100;

                return (
                  <div key={hour} className="flex-1 flex flex-col items-center group relative">
                    <motion.div
                      initial={{ height: 0 }}
                      animate={{ height: `${height}%` }}
                      transition={{ duration: 0.5, delay: hour * 0.02 }}
                      className={`w-full bg-success/60 rounded-t-sm group-hover:bg-success transition-colors ${height === 0 ? 'min-h-[2px] bg-border' : ''}`}
                    />
                    {/* Tooltip */}
                    <div className="absolute bottom-full mb-2 hidden group-hover:block bg-popover text-white text-xs p-2 rounded z-10 whitespace-nowrap border border-border">
                      {hour}:00 Uhr: {count} Nachrichten
                    </div>
                    {hour % 4 === 0 && (
                      <div className="text-[10px] text-text-secondary mt-1">{hour}h</div>
                    )}
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="h-64 rounded-lg border border-border bg-background/50 p-4 text-sm text-text-secondary flex items-center justify-center text-center">
              Keine belastbaren Stundenmuster: zu wenig valide Chat-Timestamps.
            </div>
          )}
        </motion.div>
      </div>

      {/* Top Chatters Leaderboard */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2 }}
        className="panel-card rounded-2xl p-6"
      >
        <div className="flex items-center gap-3 mb-6">
          <Award className="w-6 h-6 text-warning" />
          <h2 className="text-xl font-bold text-white">Top Chatter</h2>
        </div>

        {data.topChatters && data.topChatters.length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {data.topChatters.slice(0, 12).map((chatter, i) => (
              <ChatterCard
                key={chatter.login}
                rank={i + 1}
                login={chatter.login}
                messages={chatter.totalMessages}
                sessions={chatter.totalSessions}
                loyaltyScore={chatter.loyaltyScore}
              />
            ))}
          </div>
        ) : (
          <div className="text-center py-8 text-text-secondary">
            <Users className="w-12 h-12 mx-auto mb-3 opacity-50" />
            <p>Keine Chatter-Daten vorhanden</p>
          </div>
        )}
      </motion.div>

      {/* Zuschauer-Profile */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.25 }}
        className="panel-card rounded-2xl p-6"
      >
        <div className="flex items-center gap-3 mb-6">
          <Users className="w-6 h-6 text-accent" />
          <h2 className="text-xl font-bold text-white">Zuschauer-Profile</h2>
        </div>
        <ViewerProfiles data={viewerProfilesData} />
      </motion.div>

      {/* Chat-Konzentration */}
      {coachingData && !coachingData.empty && (
        <ChatConcentrationSection data={coachingData} />
      )}

      {/* Insights */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.3 }}
        className="rounded-2xl border border-primary/25 bg-gradient-to-r from-primary/16 via-card to-accent/16 p-6"
      >
        <h3 className="font-bold text-white mb-4">Chat-Insights</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {totalChatters > 0 ? (
            <>
              <InsightItem
                type={chatterReturnRate > 30 ? 'positive' : 'warning'}
                text={chatterReturnRate > 30
                  ? `Starke Community! ${chatterReturnRate.toFixed(0)}% deiner Chatter kommen wieder.`
                  : `${chatterReturnRate.toFixed(0)}% Return Rate - versuche mehr Interaktion!`}
              />
              <InsightItem
                type={firstTimeChatters > returningChatters ? 'info' : 'positive'}
                text={firstTimeChatters > returningChatters
                  ? 'Viele neue Gesichter! Binde sie durch direkte Ansprache.'
                  : 'Deine Stammzuschauer sind loyal - belohne sie!'
                }
              />
            </>
          ) : (
            <InsightItem
              type="info"
              text="Keine aktiven Chatter im Zeitraum: Erst bei echten Chat-Samples werden Treue-Insights angezeigt."
            />
          )}
        </div>
      </motion.div>
    </div>
  );
}

interface StatCardProps {
  icon: React.ReactNode;
  label: string;
  value: string;
  subtext?: string;
  color: 'primary' | 'accent' | 'success' | 'warning';
}

function StatCard({ icon, label, value, subtext, color }: StatCardProps) {
  const colorClasses = {
    primary: 'bg-primary/10 text-primary',
    accent: 'bg-accent/10 text-accent',
    success: 'bg-success/10 text-success',
    warning: 'bg-warning/10 text-warning',
  };

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      className="panel-card soft-elevate rounded-2xl p-5"
    >
      <div className={`w-10 h-10 rounded-xl ${colorClasses[color]} flex items-center justify-center mb-3`}>
        {icon}
      </div>
      <div className="text-sm text-text-secondary mb-1">{label}</div>
      <div className="display-font text-2xl font-bold text-white">{value}</div>
      {subtext && <div className="text-sm text-text-secondary mt-1">{subtext}</div>}
    </motion.div>
  );
}

interface LoyaltyGaugeProps {
  label: string;
  percentage: number;
  description: string;
  startColor: string;
  endColor: string;
}

function LoyaltyGauge({ label, percentage, description, startColor, endColor }: LoyaltyGaugeProps) {
  const clampedPercentage = Math.min(100, Math.max(0, percentage));
  const gradientId = `gauge-${useId().replace(/:/g, '')}`;

  return (
    <div className="text-center">
      <div className="relative w-32 h-32 mx-auto mb-3">
        <svg className="w-full h-full -rotate-90">
          <circle
            cx="64"
            cy="64"
            r="56"
            fill="none"
            stroke="currentColor"
            strokeWidth="12"
            className="text-border"
          />
          <motion.circle
            cx="64"
            cy="64"
            r="56"
            fill="none"
            stroke={`url(#${gradientId})`}
            strokeWidth="12"
            strokeLinecap="round"
            strokeDasharray={`${(clampedPercentage / 100) * 352} 352`}
            initial={{ strokeDasharray: '0 352' }}
            animate={{ strokeDasharray: `${(clampedPercentage / 100) * 352} 352` }}
            transition={{ duration: 1, delay: 0.3 }}
          />
          <defs>
            <linearGradient id={gradientId} x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor={startColor} />
              <stop offset="100%" stopColor={endColor} />
            </linearGradient>
          </defs>
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-2xl font-bold text-white">{clampedPercentage.toFixed(0)}%</span>
        </div>
      </div>
      <div className="font-medium text-white">{label}</div>
      <div className="text-sm text-text-secondary">{description}</div>
    </div>
  );
}

interface ChatterCardProps {
  rank: number;
  login: string;
  messages: number;
  sessions: number;
  loyaltyScore: number;
}

function ChatterCard({ rank, login, messages, sessions, loyaltyScore }: ChatterCardProps) {
  const getRankStyle = () => {
    if (rank === 1) return 'bg-gradient-to-br from-yellow-400 to-yellow-600 text-black';
    if (rank === 2) return 'bg-gradient-to-br from-gray-300 to-gray-500 text-black';
    if (rank === 3) return 'bg-gradient-to-br from-amber-600 to-amber-800 text-white';
    return 'bg-border text-text-secondary';
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: rank * 0.03 }}
      className="flex items-center gap-3 p-3 rounded-xl bg-background/75 border border-border/65"
    >
      <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${getRankStyle()}`}>
        {rank}
      </div>
      <div className="flex-1 min-w-0">
        <div className="font-medium text-white truncate">{login}</div>
        <div className="text-xs text-text-secondary">
          {messages.toLocaleString('de-DE')} Nachrichten • {sessions} Sessions
        </div>
      </div>
      <div className="text-right">
        <div className="text-sm font-medium text-primary">{loyaltyScore}</div>
        <div className="text-xs text-text-secondary">Loyalität</div>
      </div>
    </motion.div>
  );
}

interface InsightItemProps {
  type: 'positive' | 'warning' | 'info';
  text: string;
}

function InsightItem({ type, text }: InsightItemProps) {
  const styles = {
    positive: 'bg-success/10 border-success/20 text-success',
    warning: 'bg-warning/10 border-warning/20 text-warning',
    info: 'bg-primary/10 border-primary/20 text-primary',
  };

  return (
    <div className={`p-3 rounded-lg border ${styles[type]}`}>
      <p className="text-sm text-white">{text}</p>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Chat-Konzentration (moved from Coaching tab)
// ---------------------------------------------------------------------------

const LOYALTY_LABELS: Record<string, { label: string; color: string }> = {
  oneTimer: { label: 'Einmalig', color: 'bg-error/60' },
  casual: { label: 'Gelegentlich', color: 'bg-warning/60' },
  regular: { label: 'Regulär', color: 'bg-primary/60' },
  loyal: { label: 'Loyal', color: 'bg-success/60' },
};

function ChatConcentrationSection({ data }: { data: CoachingData }) {
  const chat = data.chatConcentration;
  if (!chat || chat.totalChatters === 0) return null;

  const bucketOrder = ['oneTimer', 'casual', 'regular', 'loyal'];
  const buckets = bucketOrder
    .filter(k => chat.loyaltyBuckets[k])
    .map(k => ({ key: k, ...chat.loyaltyBuckets[k], ...LOYALTY_LABELS[k] }));

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: 0.28 }}
      className="panel-card rounded-2xl p-6"
    >
      <div className="flex items-center gap-3 mb-6">
        <MessageCircle className="w-6 h-6 text-primary" />
        <h2 className="text-xl font-bold text-white">Chat-Konzentration</h2>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <div className="text-center p-3 rounded-lg bg-background/50">
          <div className={`text-2xl font-bold ${chat.top1Pct > 50 ? 'text-error' : chat.top1Pct > 30 ? 'text-warning' : 'text-success'}`}>
            {chat.top1Pct}%
          </div>
          <div className="text-xs text-text-secondary">Top-1 Chatter Anteil</div>
        </div>
        <div className="text-center p-3 rounded-lg bg-background/50">
          <div className={`text-2xl font-bold ${chat.top3Pct > 70 ? 'text-warning' : 'text-white'}`}>
            {chat.top3Pct}%
          </div>
          <div className="text-xs text-text-secondary">Top-3 kumulativ</div>
        </div>
        <div className="text-center p-3 rounded-lg bg-background/50">
          <div className="text-2xl font-bold text-white">{chat.msgsPerChatter}</div>
          <div className="text-xs text-text-secondary">Msgs / Chatter</div>
        </div>
        <div className="text-center p-3 rounded-lg bg-background/50">
          <div className={`text-2xl font-bold ${chat.concentrationIndex > 2500 ? 'text-error' : 'text-white'}`}>
            {chat.concentrationIndex.toLocaleString()}
          </div>
          <div className="text-xs text-text-secondary">HHI-Index</div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Loyalty Buckets */}
        <div>
          <h3 className="text-sm font-medium text-text-secondary mb-3">Chatter-Loyalität</h3>
          <div className="flex rounded-lg overflow-hidden h-8 mb-3">
            {buckets.map(b => (
              <div
                key={b.key}
                className={`${b.color} flex items-center justify-center text-xs text-white font-medium`}
                style={{ width: `${b.pct}%`, minWidth: b.pct > 5 ? undefined : '2px' }}
                title={`${b.label}: ${b.count} (${b.pct}%)`}
              >
                {b.pct >= 10 && `${b.pct}%`}
              </div>
            ))}
          </div>
          <div className="flex flex-wrap gap-3 text-xs">
            {buckets.map(b => (
              <span key={b.key} className="flex items-center gap-1.5 text-text-secondary">
                <span className={`w-2.5 h-2.5 rounded-sm ${b.color}`} />
                {b.label}: {b.count} ({b.pct}%)
              </span>
            ))}
          </div>
          <div className="mt-3 text-xs text-text-secondary">
            Einmaliger Chatter-Anteil: <span className={chat.ownOneTimerPct > chat.avgPeerOneTimerPct ? 'text-warning' : 'text-success'}>
              {chat.ownOneTimerPct}%
            </span>
            {' '}vs Kategorie-Schnitt {chat.avgPeerOneTimerPct}%
          </div>
        </div>

        {/* Top Chatters by share */}
        <div>
          <h3 className="text-sm font-medium text-text-secondary mb-3">Nachrichten-Anteil Top-Chatter</h3>
          <div className="space-y-2">
            {chat.topChatters.slice(0, 5).map((c, i) => (
              <div key={c.login} className="flex items-center gap-3">
                <span className="text-xs text-text-secondary w-5 text-right">{i + 1}.</span>
                <span className="text-sm text-white flex-1 truncate">{c.login}</span>
                <div className="w-24 bg-background rounded-full h-2 overflow-hidden">
                  <div
                    className="h-full bg-primary/70 rounded-full"
                    style={{ width: `${Math.min(100, c.sharePct)}%` }}
                  />
                </div>
                <span className="text-xs text-text-secondary w-12 text-right">{c.sharePct.toFixed(1)}%</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </motion.div>
  );
}

export default ChatAnalytics;
