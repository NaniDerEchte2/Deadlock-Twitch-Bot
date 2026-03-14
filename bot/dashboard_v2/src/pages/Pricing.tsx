import { Check, Sparkles, Star } from 'lucide-react';
import { useBillingCatalog } from '../hooks/useAnalytics';

// Fetch from /twitch/api/v2/billing/catalog
// Response: { plans: CatalogPlan[] }

export default function Pricing() {
  const { data } = useBillingCatalog();

  const plans = data?.plans ?? [];

  return (
    <div className="max-w-5xl mx-auto px-4 py-8">
      {/* Hero */}
      <div className="text-center mb-10">
        <h1 className="text-3xl font-bold text-white mb-2">
          Dein Wachstums-Coach für Twitch
        </h1>
        <p className="text-white/50 text-lg">
          So holst du noch mehr aus deinem Kanal raus.
        </p>
      </div>

      {/* Plan Cards */}
      <div className="grid md:grid-cols-3 gap-6 mb-12">
        {plans.map((plan) => {
          const isCurrent = plan.is_current;
          const isPopular = plan.tier === 'basic';
          const isExtended = plan.tier === 'extended';

          return (
            <div
              key={plan.id}
              className={`relative bg-card rounded-2xl border p-6 flex flex-col ${
                isPopular
                  ? 'border-purple-400/40 shadow-lg shadow-purple-500/10'
                  : 'border-border'
              }`}
            >
              {isPopular && (
                <div className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 rounded-full bg-purple-500 text-white text-xs font-medium flex items-center gap-1">
                  <Star className="w-3 h-3" /> Beliebt
                </div>
              )}

              <div className="mb-4">
                <div className="flex items-center gap-2 mb-1">
                  {isExtended && <Sparkles className="w-5 h-5 text-purple-400" />}
                  <h2 className="text-xl font-bold text-white">{plan.name}</h2>
                </div>
                <div className="flex items-baseline gap-1">
                  <span className="text-3xl font-bold text-white">
                    {plan.price_monthly === 0 ? 'Kostenlos' : `${plan.price_monthly.toFixed(2).replace('.', ',')}€`}
                  </span>
                  {plan.price_monthly > 0 && (
                    <span className="text-white/40 text-sm">/Monat</span>
                  )}
                </div>
              </div>

              <ul className="space-y-3 flex-1 mb-6">
                {plan.features.map((feature) => (
                  <li key={feature} className="flex items-start gap-2 text-sm">
                    <Check className={`w-4 h-4 mt-0.5 flex-shrink-0 ${
                      isExtended ? 'text-purple-400' : isPopular ? 'text-emerald-400' : 'text-white/30'
                    }`} />
                    <span className="text-white/70">{feature}</span>
                  </li>
                ))}
              </ul>

              {isCurrent ? (
                <div className="text-center py-2.5 rounded-xl bg-white/5 text-white/50 text-sm font-medium">
                  Aktueller Plan
                </div>
              ) : (
                <a
                  href="/twitch/abbo"
                  className={`text-center py-2.5 rounded-xl text-sm font-medium transition-colors ${
                    isPopular
                      ? 'bg-purple-500 hover:bg-purple-400 text-white'
                      : isExtended
                      ? 'bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-400 hover:to-pink-400 text-white'
                      : 'bg-white/10 hover:bg-white/15 text-white'
                  }`}
                >
                  {plan.price_monthly === 0 ? 'Loslegen' : 'Upgrade'}
                </a>
              )}
            </div>
          );
        })}
      </div>

      {/* Feature Comparison Matrix */}
      <div className="bg-card rounded-2xl border border-border p-6">
        <h2 className="text-lg font-bold text-white mb-4">Feature-Vergleich</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/10">
                <th className="text-left py-3 text-white/40 font-normal">Feature</th>
                <th className="text-center py-3 text-white/60 font-medium">Free</th>
                <th className="text-center py-3 text-purple-400 font-medium">Basic</th>
                <th className="text-center py-3 text-pink-400 font-medium">Erweitert</th>
              </tr>
            </thead>
            <tbody className="text-white/60">
              {[
                ['Analytics Tabs', '4', '8', '13'],
                ['Viewer-Trend', '✓', '✓', '✓'],
                ['Stream-Übersicht', '✓', '✓', '✓'],
                ['Schedule Heatmap', '✓', '✓', '✓'],
                ['Chat-Analytics', '–', '✓', '✓'],
                ['Growth-Tracking', '–', '✓', '✓'],
                ['Audience-Insights', '–', '✓', '✓'],
                ['Kategorie-Vergleich', '–', '✓', '✓'],
                ['AI-Analyse', '–', '–', '✓'],
                ['Viewer-Profile', '–', '–', '✓'],
                ['Coaching', '–', '–', '✓'],
                ['Monetization', '–', '–', '✓'],
              ].map(([feature, free, basic, ext]) => (
                <tr key={feature} className="border-b border-white/5">
                  <td className="py-2.5">{feature}</td>
                  <td className="text-center">{free === '✓' ? <Check className="w-4 h-4 text-white/30 mx-auto" /> : free === '–' ? <span className="text-white/20">–</span> : free}</td>
                  <td className="text-center">{basic === '✓' ? <Check className="w-4 h-4 text-emerald-400 mx-auto" /> : basic === '–' ? <span className="text-white/20">–</span> : basic}</td>
                  <td className="text-center">{ext === '✓' ? <Check className="w-4 h-4 text-purple-400 mx-auto" /> : ext === '–' ? <span className="text-white/20">–</span> : ext}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Back link */}
      <div className="text-center mt-8">
        <a href="/twitch/dashboard" className="text-sm text-white/40 hover:text-white/60 transition-colors">
          ← Zurück zum Dashboard
        </a>
      </div>
    </div>
  );
}
