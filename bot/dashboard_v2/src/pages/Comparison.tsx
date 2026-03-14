import { useState } from 'react';
import { motion } from 'framer-motion';
import { Scale, Users, TrendingUp, Target, AlertCircle, Loader2, Filter } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { fetchCategoryComparison, fetchViewerOverlap } from '@/api/client';
import { useAudienceSharing } from '@/hooks/useAnalytics';
import { AudienceSharing } from '@/components/charts/AudienceSharing';
import { PlanGateCard } from '@/components/cards/PlanGateCard';
import type { CategoryComparison, ViewerOverlap } from '@/types/analytics';

import type { TimeRange } from '@/types/analytics';

interface ComparisonProps {
  streamer: string;
  days: TimeRange;
}

export function Comparison({ streamer, days }: ComparisonProps) {
  // Exclude streamers with external reach (YouTube/Social-Media Publikum) from category stats
  const [excludeExternal, setExcludeExternal] = useState(true);

  const { data: comparison, isLoading: loadingComparison } = useQuery<CategoryComparison>({
    queryKey: ['categoryComparison', streamer, days, excludeExternal],
    queryFn: () => fetchCategoryComparison(streamer, days, excludeExternal),
    enabled: !!streamer,
  });

  const { data: overlap, isLoading: loadingOverlap } = useQuery<ViewerOverlap[]>({
    queryKey: ['viewerOverlap', streamer],
    queryFn: () => fetchViewerOverlap(streamer, 20),
    enabled: !!streamer,
  });

  const { data: audienceSharingData } = useAudienceSharing(streamer, days);

  if (!streamer) {
    return (
      <div className="flex flex-col items-center justify-center h-64">
        <AlertCircle className="w-12 h-12 text-text-secondary mb-4" />
        <p className="text-text-secondary text-lg">Wähle einen Streamer aus</p>
      </div>
    );
  }

  if (loadingComparison || loadingOverlap) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 animate-spin text-primary" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Category Comparison Section */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="bg-card rounded-xl border border-border p-6"
      >
        <div className="flex items-center justify-between mb-6 flex-wrap gap-3">
          <div className="flex items-center gap-3">
            <Scale className="w-6 h-6 text-primary" />
            <h2 className="text-xl font-bold text-white">Deadlock Kategorie-Vergleich</h2>
          </div>
          <button
            onClick={() => setExcludeExternal(e => !e)}
            title="Streamer mit externer Reichweite (YouTube, Social Media) aus dem Kategorie-Schnitt ausschließen"
            className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-colors border ${
              excludeExternal
                ? 'bg-accent/15 text-accent border-accent/30'
                : 'bg-background text-text-secondary border-border hover:text-white'
            }`}
          >
            <Filter className="w-3.5 h-3.5" />
            Nur organische Streamer
          </button>
        </div>

        {comparison && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <ComparisonMetric
              label="Ø Viewer"
              yourValue={comparison.yourStats.avgViewers}
              categoryValue={comparison.categoryAvg.avgViewers}
              percentile={comparison.percentiles.avgViewers}
              format="number"
            />
            <ComparisonMetric
              label="Peak Viewer"
              yourValue={comparison.yourStats.peakViewers}
              categoryValue={comparison.categoryAvg.peakViewers}
              percentile={50}
              format="number"
            />
            <ComparisonMetric
              label="10-Min Retention"
              yourValue={comparison.yourStats.retention10m}
              categoryValue={comparison.categoryAvg.retention10m}
              percentile={comparison.percentiles.retention10m}
              format="percent"
            />
            <ComparisonMetric
              label="Chat Health"
              yourValue={comparison.yourStats.chatHealth}
              categoryValue={comparison.categoryAvg.chatHealth}
              percentile={comparison.percentiles.chatHealth}
              format="decimal"
            />
          </div>
        )}

        {/* Percentile Ranking */}
        <PlanGateCard featureId="rankings_extended" title="Erweiterte Rankings">
          {comparison && comparison.percentiles.avgViewers > 0 && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.3 }}
              className="mt-6 p-4 bg-background rounded-lg"
            >
              <div className="flex items-center gap-2 mb-3">
                <Target className="w-5 h-5 text-primary" />
                <span className="font-medium text-white">Dein Ranking</span>
              </div>
              <div className="text-3xl font-bold text-transparent bg-gradient-to-r from-primary to-accent bg-clip-text">
                Top {100 - comparison.percentiles.avgViewers}%
              </div>
              <p className="text-sm text-text-secondary mt-1">
                aller Deadlock-Streamer nach Ø Viewern
              </p>
            </motion.div>
          )}
        </PlanGateCard>
      </motion.div>

      {/* Viewer Overlap Section */}
      <PlanGateCard featureId="viewer_overlap" title="Viewer-Overlap">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
          className="bg-card rounded-xl border border-border p-6"
        >
          <div className="flex items-center gap-3 mb-6">
            <Users className="w-6 h-6 text-accent" />
            <h2 className="text-xl font-bold text-white">Viewer-Überschneidung</h2>
          </div>

          {overlap && overlap.length > 0 ? (
            <div className="space-y-3">
              {overlap.slice(0, 10).map((item, i) => (
                <OverlapBar
                  key={item.streamerB}
                  rank={i + 1}
                  streamer={item.streamerB}
                  sharedChatters={item.sharedChatters}
                  percentage={
                    item.overlapPercentage
                    ?? item.jaccard
                    ?? item.overlapAtoB
                    ?? item.overlapBtoA
                    ?? 0
                  }
                />
              ))}
            </div>
          ) : (
            <div className="text-center py-8 text-text-secondary">
              <Users className="w-12 h-12 mx-auto mb-3 opacity-50" />
              <p>Keine Überschneidungsdaten vorhanden</p>
              <p className="text-sm mt-1">Sammle mehr Chat-Daten</p>
            </div>
          )}

          {overlap && overlap.length > 0 && (
            <div className="mt-6 p-4 bg-gradient-to-r from-accent/10 to-primary/10 rounded-lg border border-accent/20">
              <div className="flex items-center gap-2 mb-2">
                <TrendingUp className="w-5 h-5 text-accent" />
                <span className="font-medium text-white">Raid-Empfehlung</span>
              </div>
              {(() => {
                const top = overlap[0];
                const topPct = top
                  ? top.overlapPercentage
                    ?? top.jaccard
                    ?? top.overlapAtoB
                    ?? top.overlapBtoA
                    ?? 0
                  : 0;

                return (
                  <p className="text-text-secondary text-sm">
                    <span className="text-white font-medium">{top?.streamerB}</span> hat die höchste
                    Viewer-Überschneidung ({topPct.toFixed(1)}%).
                    Ein Raid könnte für beide Communities wertvoll sein!
                  </p>
                );
              })()}
            </div>
          )}
        </motion.div>
      </PlanGateCard>

      {/* Zuschauer-Netzwerk */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.3 }}
        className="bg-card rounded-xl border border-border p-6"
      >
        <div className="flex items-center gap-3 mb-6">
          <Users className="w-6 h-6 text-primary" />
          <h2 className="text-xl font-bold text-white">Zuschauer-Netzwerk</h2>
        </div>
        <AudienceSharing data={audienceSharingData} />
      </motion.div>
    </div>
  );
}

interface ComparisonMetricProps {
  label: string;
  yourValue: number;
  categoryValue: number;
  percentile: number;
  format: 'number' | 'percent' | 'decimal';
}

function ComparisonMetric({ label, yourValue, categoryValue, percentile, format }: ComparisonMetricProps) {
  const formatValue = (val: number) => {
    if (format === 'percent') return `${val.toFixed(1)}%`;
    if (format === 'decimal') return val.toFixed(1);
    return val.toLocaleString('de-DE', { maximumFractionDigits: 0 });
  };

  const diff = categoryValue > 0 ? ((yourValue - categoryValue) / categoryValue) * 100 : 0;
  const isPositive = diff >= 0;

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      className="p-4 bg-background rounded-lg"
    >
      <div className="text-sm text-text-secondary mb-2">{label}</div>

      <div className="flex items-end justify-between mb-3">
        <div>
          <div className="text-2xl font-bold text-white">{formatValue(yourValue)}</div>
          <div className="text-xs text-text-secondary">Dein Wert</div>
        </div>
        <div className="text-right">
          <div className="text-lg text-text-secondary">{formatValue(categoryValue)}</div>
          <div className="text-xs text-text-secondary">Kategorie Ø</div>
        </div>
      </div>

      {/* Progress bar showing position relative to category */}
      <div className="h-2 bg-border rounded-full overflow-hidden mb-2">
        <motion.div
          initial={{ width: 0 }}
          animate={{ width: `${Math.min(100, Math.max(0, percentile))}%` }}
          transition={{ delay: 0.3, duration: 0.5 }}
          className="h-full bg-gradient-to-r from-primary to-accent"
        />
      </div>

      <div className={`text-sm font-medium ${isPositive ? 'text-success' : 'text-error'}`}>
        {isPositive ? '+' : ''}{diff.toFixed(1)}% vs Kategorie
      </div>
    </motion.div>
  );
}

interface OverlapBarProps {
  rank: number;
  streamer: string;
  sharedChatters: number;
  percentage: number;
}

function OverlapBar({ rank, streamer, sharedChatters, percentage }: OverlapBarProps) {
  const pct = Number.isFinite(percentage) ? percentage : 0;
  return (
    <motion.div
      initial={{ opacity: 0, x: -20 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ delay: rank * 0.05 }}
      className="flex items-center gap-4"
    >
      <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${
        rank <= 3 ? 'bg-gradient-to-br from-primary to-accent text-white' : 'bg-border text-text-secondary'
      }`}>
        {rank}
      </div>

      <div className="flex-1">
        <div className="flex items-center justify-between mb-1">
          <span className="font-medium text-white">{streamer}</span>
          <span className="text-sm text-text-secondary">
            {sharedChatters.toLocaleString('de-DE')} Chatter
          </span>
        </div>
        <div className="h-2 bg-border rounded-full overflow-hidden">
          <motion.div
            initial={{ width: 0 }}
            animate={{ width: `${Math.max(0, Math.min(pct, 100))}%` }}
            transition={{ delay: 0.3 + rank * 0.05, duration: 0.5 }}
            className={`h-full ${rank <= 3 ? 'bg-gradient-to-r from-accent to-primary' : 'bg-primary/60'}`}
          />
        </div>
      </div>

      <div className="w-16 text-right">
        <span className="text-sm font-medium text-white">{pct.toFixed(1)}%</span>
      </div>
    </motion.div>
  );
}

export default Comparison;
