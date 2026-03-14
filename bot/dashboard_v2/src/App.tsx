import { useState, useEffect, useRef, Component, type ReactNode, type ErrorInfo } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Header } from '@/components/layout/Header';
import { TabNavigation, type TabId } from '@/components/layout/TabNavigation';
import { Overview } from '@/pages/Overview';
import { Sessions } from '@/pages/Sessions';
import { ChatAnalytics } from '@/pages/ChatAnalytics';
import { Growth } from '@/pages/Growth';
import { Audience } from '@/pages/Audience';
import { Comparison } from '@/pages/Comparison';
import { Schedule } from '@/pages/Schedule';
import { Coaching } from '@/pages/Coaching';
import { Monetization } from '@/pages/Monetization';
import { Category } from '@/pages/Category';
import { Viewers } from '@/pages/Viewers';
import { Experimental } from '@/pages/Experimental';
import { AIAnalysis } from '@/pages/AIAnalysis';
import { InternalHomeLanding } from '@/pages/InternalHomeLanding';
import { VerwaltungPage } from '@/pages/Verwaltung';
import Pricing from '@/pages/Pricing';
import AffiliatePortal from '@/pages/AffiliatePortal';
import { PlanProvider } from '@/context/PlanContext';
import { UpgradeBanner } from '@/components/cards/UpgradeBanner';
import { useStreamerList, useAuthStatus } from '@/hooks/useAnalytics';
import type { TimeRange } from '@/types/analytics';
import {
  AlertTriangle,
  Shield,
  ShieldAlert,
  ShieldCheck,
  Wifi,
} from 'lucide-react';
import { GlowOrb } from '@/components/effects/GlowOrb';
import { GlowButton } from '@/components/ui/GlowButton';

// Error Boundary to prevent white screen on crashes
interface ErrorBoundaryProps {
  children: ReactNode;
}
interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Dashboard Error:', error, info.componentStack);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-bg flex items-center justify-center p-8">
          <div className="panel-card rounded-2xl p-8 max-w-lg text-center">
            <AlertTriangle className="w-12 h-12 text-warning mx-auto mb-4" />
            <h2 className="text-xl font-bold text-white mb-2">Dashboard-Fehler</h2>
            <p className="text-text-secondary mb-4">
              {this.state.error?.message || 'Ein unerwarteter Fehler ist aufgetreten.'}
            </p>
            <GlowButton
              as="button"
              variant="primary"
              size="sm"
              onClick={() => this.setState({ hasError: false, error: null })}
            >
              Erneut versuchen
            </GlowButton>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

// Create QueryClient
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 2,
      refetchOnWindowFocus: false,
    },
  },
});

function normalizePathname(pathname: string): string {
  const normalized = pathname.replace(/\/+$/, '');
  return normalized || '/';
}

function InternalHome() {
  return <InternalHomeLanding />;
}

function AnalyticsDashboard() {
  const [streamer, setStreamer] = useState<string | null>(null);
  const [days, setDays] = useState<TimeRange>(30);
  const [activeTab, setActiveTab] = useState<TabId>('overview');

  const { data: streamers = [], isLoading: loadingStreamers } = useStreamerList();
  const { data: authStatus, isLoading: loadingAuth, isError: authError } = useAuthStatus();

  // Tracks if we already auto-set the streamer from auth (fire-once guard)
  const hasAutoSetStreamer = useRef(false);

  // Parse URL params on mount
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const urlStreamer = params.get('streamer');
    const urlDays = params.get('days');

    if (urlStreamer) {
      setStreamer(urlStreamer);
      hasAutoSetStreamer.current = true; // URL explicitly set — skip auto-set
    }
    if (urlDays) {
      const d = parseInt(urlDays, 10);
      if (d === 7 || d === 30 || d === 90) setDays(d);
    }
  }, []);

  // Auto-set streamer to logged-in Twitch user on first auth load
  useEffect(() => {
    if (!hasAutoSetStreamer.current && authStatus?.twitchLogin) {
      setStreamer(authStatus.twitchLogin);
      hasAutoSetStreamer.current = true;
    }
  }, [authStatus]);

  // Update URL when params change
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);

    if (streamer) {
      params.set('streamer', streamer);
    } else {
      params.delete('streamer');
    }
    params.set('days', String(days));

    const newUrl = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState({}, '', newUrl);
  }, [streamer, days]);

  const handleSessionClick = (sessionId: number) => {
    // TODO: Navigate to session detail view
    console.log('Session clicked:', sessionId);
  };

  // Auth badge component
  const AuthBadge = () => {
    const badgeBase =
      'flex items-center gap-2 px-3 py-1.5 rounded-full border text-xs font-semibold tracking-wide backdrop-blur-md';

    if (loadingAuth) return null;

    if (authError || !authStatus?.authenticated) {
      return (
        <div className={`${badgeBase} bg-error/10 border-error/30 text-error`}>
          <ShieldAlert className="w-4 h-4" />
          <span>Nicht authentifiziert</span>
        </div>
      );
    }

    if (authStatus.isLocalhost) {
      return (
        <div className={`${badgeBase} bg-success/10 border-success/30 text-success`}>
          <Wifi className="w-4 h-4" />
          <span>Localhost (Admin)</span>
        </div>
      );
    }

    if (authStatus.isAdmin) {
      return (
        <div className={`${badgeBase} bg-primary/10 border-primary/30 text-primary`}>
          <ShieldCheck className="w-4 h-4" />
          <span>Admin</span>
        </div>
      );
    }

    return (
      <div className={`${badgeBase} bg-accent/10 border-accent/30 text-accent`}>
        <Shield className="w-4 h-4" />
        <span>Partner</span>
      </div>
    );
  };

  // Map auth plan to PlanStatus shape (or null for backward compat)
  const planStatus = authStatus?.plan ?? null;

  return (
    <PlanProvider
      plan={planStatus}
      isAdmin={authStatus?.isAdmin ?? false}
      isLocalhost={authStatus?.isLocalhost ?? false}
    >
      <GlowOrb />
      <div className="min-h-screen relative px-3 py-4 md:px-7 md:py-8">
        <div className="relative max-w-[1700px] mx-auto">
          {/* Auth Status Badge */}
          <div className="flex justify-end mb-4">
            <AuthBadge />
          </div>

          <Header
            streamer={streamer}
            streamers={streamers}
            days={days}
            onStreamerChange={setStreamer}
            onDaysChange={setDays}
            isLoading={loadingStreamers}
            canViewAllStreamers={authStatus?.permissions?.viewAllStreamers || false}
          />

          <UpgradeBanner />

          <TabNavigation activeTab={activeTab} onTabChange={setActiveTab} />

          {/* Tab Content */}
          {activeTab === 'overview' && (
            <Overview
              streamer={streamer}
              days={days}
              onSessionClick={handleSessionClick}
            />
          )}

          {activeTab === 'streams' && (
            <Sessions streamer={streamer || ''} days={days} />
          )}

          {activeTab === 'chat' && (
            <ChatAnalytics streamer={streamer || ''} days={days} />
          )}

          {activeTab === 'growth' && (
            <Growth streamer={streamer || ''} days={days} />
          )}

          {activeTab === 'audience' && (
            <Audience streamer={streamer || ''} days={days} />
          )}

          {activeTab === 'viewers' && (
            <Viewers streamer={streamer} days={days} />
          )}

          {activeTab === 'compare' && (
            <Comparison streamer={streamer || ''} days={days} />
          )}

          {activeTab === 'schedule' && (
            <Schedule streamer={streamer || ''} days={days} />
          )}

          {activeTab === 'coaching' && (
            <Coaching streamer={streamer || ''} days={days} />
          )}

          {activeTab === 'monetization' && (
            <Monetization streamer={streamer} days={days} />
          )}

          {activeTab === 'category' && (
            <Category
              streamer={streamer}
              days={days}
              onStreamerSelect={setStreamer}
              onNavigate={setActiveTab}
            />
          )}

          {activeTab === 'experimental' && (
            <Experimental streamer={streamer} days={days} />
          )}

          {activeTab === 'ai' && (
            <AIAnalysis streamer={streamer} days={days} />
          )}

        </div>
      </div>
    </PlanProvider>
  );
}

export default function App() {
  const pathname = normalizePathname(window.location.pathname);
  const isInternalHomeRoute = pathname === '/twitch/dashboard';
  const isVerwaltungRoute = pathname === '/twitch/verwaltung';
  const isPricingRoute = pathname === '/twitch/pricing';
  const isAffiliatePortalRoute = pathname === '/twitch/affiliate/portal';

  return (
    <QueryClientProvider client={queryClient}>
      <ErrorBoundary>
        {isVerwaltungRoute ? (
          <VerwaltungPage />
        ) : isInternalHomeRoute ? (
          <InternalHome />
        ) : isPricingRoute ? (
          <Pricing />
        ) : isAffiliatePortalRoute ? (
          <AffiliatePortal />
        ) : (
          <AnalyticsDashboard />
        )}
      </ErrorBoundary>
    </QueryClientProvider>
  );
}

