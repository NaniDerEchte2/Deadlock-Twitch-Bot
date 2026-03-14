// Plan tier levels
export type PlanTier = 'free' | 'basic' | 'extended';

// Dashboard view mode (what the user is currently viewing)
export type DashboardView = 'basic' | 'extended';

// Plan status from auth-status API
export interface PlanStatus {
  planId: string | null;
  planName: string | null;
  tier: PlanTier;
  isExtended: boolean;
  expiresAt: string | null;
  source: string | null;
}

// Tab IDs matching the analytics dashboard tabs
// NOTE: These must match TabId from components/layout/TabNavigation.tsx
export type TabId =
  | 'overview'
  | 'streams'
  | 'schedule'
  | 'category'
  | 'chat'
  | 'growth'
  | 'audience'
  | 'compare'
  | 'viewers'
  | 'coaching'
  | 'monetization'
  | 'experimental'
  | 'ai';

// Tab visibility configuration per tier
export const TAB_TIERS: Record<TabId, PlanTier> = {
  // Free (4 tabs)
  'overview': 'free',
  'streams': 'free',
  'schedule': 'free',
  'category': 'free',
  // Basic (+ 4 tabs = 8 total)
  'chat': 'basic',
  'growth': 'basic',
  'audience': 'basic',
  'compare': 'basic',
  // Extended (+ 5 tabs = 13 total)
  'viewers': 'extended',
  'coaching': 'extended',
  'monetization': 'extended',
  'experimental': 'extended',
  'ai': 'extended',
};

// Feature IDs for card-level gating within tabs
export type FeatureId =
  | 'health_scores'
  | 'calendar_heatmap'
  | 'insights_panel'
  | 'stream_timeline_detail'
  | 'chatter_list'
  | 'hype_timeline'
  | 'chat_content_analysis'
  | 'chat_social_graph'
  | 'title_performance'
  | 'raid_retention'
  | 'lurker_analysis'
  | 'audience_sharing'
  | 'viewer_overlap'
  | 'category_timings'
  | 'category_activity_series'
  | 'rankings_extended';

// Feature tier requirements (cards within tabs that need higher tier)
export const FEATURE_TIERS: Record<FeatureId, PlanTier> = {
  // Extended-only features within Basic tabs
  'health_scores': 'extended',
  'calendar_heatmap': 'extended',
  'insights_panel': 'extended',
  'stream_timeline_detail': 'extended',
  'chatter_list': 'extended',
  'hype_timeline': 'extended',
  'chat_content_analysis': 'extended',
  'chat_social_graph': 'extended',
  'title_performance': 'extended',
  'raid_retention': 'extended',
  'lurker_analysis': 'extended',
  'audience_sharing': 'extended',
  'viewer_overlap': 'extended',
  'category_timings': 'extended',
  'category_activity_series': 'extended',
  'rankings_extended': 'extended',
};

// Tier hierarchy for comparison
const TIER_ORDER: Record<PlanTier, number> = {
  'free': 0,
  'basic': 1,
  'extended': 2,
};

// Check if a tier meets or exceeds a required tier
export function tierMeetsRequirement(userTier: PlanTier, requiredTier: PlanTier): boolean {
  return TIER_ORDER[userTier] >= TIER_ORDER[requiredTier];
}

// Get display name for tier
export function getTierDisplayName(tier: PlanTier): string {
  switch (tier) {
    case 'free': return 'Free';
    case 'basic': return 'Basic';
    case 'extended': return 'Erweitert';
  }
}

// Billing catalog plan
export interface CatalogPlan {
  id: string;
  name: string;
  tier: PlanTier;
  price_monthly: number;
  features: string[];
  is_current: boolean;
}
