import { createContext, useContext, useState, useEffect, useMemo, type ReactNode } from 'react';
import type { PlanTier, DashboardView, PlanStatus, TabId, FeatureId } from '../types/billing';
import { TAB_TIERS, FEATURE_TIERS, tierMeetsRequirement } from '../types/billing';

interface PlanContextType {
  tier: PlanTier;
  plan: PlanStatus | null;
  view: DashboardView;
  setView: (view: DashboardView) => void;
  isPreviewMode: boolean;
  canAccessTab: (tabId: TabId) => boolean;
  isTabLocked: (tabId: TabId) => boolean;
  isFeatureLocked: (featureId: FeatureId) => boolean;
  hasFullAccess: boolean;
}

const PlanContext = createContext<PlanContextType | null>(null);

interface PlanProviderProps {
  children: ReactNode;
  plan: PlanStatus | null;
  isAdmin: boolean;
  isLocalhost: boolean;
}

export function PlanProvider({ children, plan, isAdmin, isLocalhost }: PlanProviderProps) {
  const hasFullAccess = isAdmin || isLocalhost;
  const tier: PlanTier = hasFullAccess ? 'extended' : (plan?.tier ?? 'free');
  const [view, setView] = useState<DashboardView>(
    tier === 'extended' ? 'extended' : 'basic'
  );
  // Sync view when tier changes after mount (e.g. auth loads async)
  useEffect(() => {
    if (tier === 'extended' || hasFullAccess) {
      setView('extended');
    }
  }, [tier, hasFullAccess]);

  const isPreviewMode = view === 'extended' && !tierMeetsRequirement(tier, 'extended') && !hasFullAccess;

  const value = useMemo<PlanContextType>(() => ({
    tier,
    plan,
    view,
    setView,
    isPreviewMode,
    canAccessTab: (tabId: TabId) => {
      if (hasFullAccess) return true;
      return tierMeetsRequirement(tier, TAB_TIERS[tabId]);
    },
    isTabLocked: (tabId: TabId) => {
      if (hasFullAccess) return false;
      return !tierMeetsRequirement(tier, TAB_TIERS[tabId]);
    },
    isFeatureLocked: (featureId: FeatureId) => {
      if (hasFullAccess) return false;
      const requiredTier = FEATURE_TIERS[featureId];
      return !tierMeetsRequirement(tier, requiredTier);
    },
    hasFullAccess,
  }), [tier, plan, view, isPreviewMode, hasFullAccess]);

  return <PlanContext.Provider value={value}>{children}</PlanContext.Provider>;
}

export function usePlan(): PlanContextType {
  const ctx = useContext(PlanContext);
  if (!ctx) throw new Error('usePlan must be used within a PlanProvider');
  return ctx;
}
