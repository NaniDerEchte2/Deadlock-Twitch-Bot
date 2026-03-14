import { Sparkles, X } from 'lucide-react';
import { usePlan } from '../../context/PlanContext';
import { getTierDisplayName } from '../../types/billing';
import { GlowButton } from '@/components/ui/GlowButton';

export function UpgradeBanner() {
  const { isPreviewMode, setView, tier } = usePlan();

  if (!isPreviewMode) return null;

  const nextTier = tier === 'free' ? 'basic' : 'extended';

  return (
    <div className="mx-4 mb-4 rounded-xl bg-gradient-to-r from-purple-500/10 to-pink-500/10 border border-purple-400/20 p-4">
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-start gap-3">
          <div className="rounded-lg bg-purple-500/20 p-2">
            <Sparkles className="w-5 h-5 text-purple-400" />
          </div>
          <div>
            <h3 className="font-semibold text-white">
              Erweiterte Analyse — Vorschau-Modus
            </h3>
            <p className="text-sm text-white/60 mt-1">
              Schalte tiefere Einblicke frei mit dem {getTierDisplayName(nextTier)}-Plan.
            </p>
            <ul className="mt-2 space-y-1 text-sm text-white/50">
              {nextTier === 'basic' ? (
                <>
                  <li>• Chat-Analytics & Aktivitäts-Tracking</li>
                  <li>• Growth-Metriken & Tag-Analyse</li>
                  <li>• Audience-Insights & Follower-Funnel</li>
                  <li>• Kategorie-Vergleich & Rankings</li>
                </>
              ) : (
                <>
                  <li>• AI-gestützte Analyse & Coaching</li>
                  <li>• Viewer-Profile & Segmente</li>
                  <li>• Monetization-Insights</li>
                  <li>• Erweiterte Karten in allen Tabs</li>
                </>
              )}
            </ul>
            <GlowButton
              href="/twitch/pricing"
              variant="primary"
              size="sm"
              className="mt-3 gap-2"
            >
              <Sparkles className="w-4 h-4" />
              Jetzt freischalten
            </GlowButton>
          </div>
        </div>
        <button
          onClick={() => setView('basic')}
          className="text-white/40 hover:text-white/70 transition-colors"
          title="Zurück zur Basic-Ansicht"
        >
          <X className="w-5 h-5" />
        </button>
      </div>
    </div>
  );
}
