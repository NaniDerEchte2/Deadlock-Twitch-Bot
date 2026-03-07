import { useEffect, useState, useCallback } from "react";
import {
  LayoutDashboard,
  Users,
  Receipt,
  Settings,
  LogIn,
  CheckCircle,
  AlertCircle,
  Clock,
  XCircle,
  ArrowRightLeft,
} from "lucide-react";

type View = "login" | "dashboard";
type Tab = "overview" | "streamers" | "commissions" | "settings";

interface AffiliateProfile {
  twitch_login: string;
  display_name: string;
  email: string;
  full_name: string;
  stripe_connect_status: string;
  stripe_account_id: string;
}

interface Claim {
  claimed_streamer_login: string;
  claimed_at: string;
  total_commission_cents: number;
  commission_count: number;
}

interface Commission {
  id: number;
  streamer_login: string;
  brutto_cents: number;
  commission_cents: number;
  currency: string;
  status: string;
  created_at: string;
}

function formatCents(cents: number): string {
  return (cents / 100).toFixed(2) + " \u20AC";
}

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    transferred: "bg-emerald-500/20 text-emerald-400",
    skipped: "bg-yellow-500/20 text-yellow-400",
    pending: "bg-gray-500/20 text-gray-400",
    failed: "bg-red-500/20 text-red-400",
  };
  return (
    <span
      className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium ${styles[status] ?? styles.pending}`}
    >
      {status}
    </span>
  );
}

const TABS: { id: Tab; label: string; icon: typeof LayoutDashboard }[] = [
  { id: "overview", label: "Übersicht", icon: LayoutDashboard },
  { id: "streamers", label: "Streamer", icon: Users },
  { id: "commissions", label: "Provisionen", icon: Receipt },
  { id: "settings", label: "Einstellungen", icon: Settings },
];

export default function AffiliatePortal() {
  const [view, setView] = useState<View>("login");
  const [tab, setTab] = useState<Tab>("overview");
  const [profile, setProfile] = useState<AffiliateProfile | null>(null);
  const [claims, setClaims] = useState<Claim[]>([]);
  const [commissions, setCommissions] = useState<Commission[]>([]);
  const [claimInput, setClaimInput] = useState("");
  const [claimMsg, setClaimMsg] = useState<{ ok: boolean; text: string } | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    try {
      const res = await fetch("/twitch/api/affiliate/me");
      if (res.status === 401) {
        setView("login");
        setLoading(false);
        return;
      }
      const data = await res.json();
      setProfile(data);
      setView("dashboard");

      const [claimsRes, commissionsRes] = await Promise.all([
        fetch("/twitch/api/affiliate/claims"),
        fetch("/twitch/api/affiliate/commissions"),
      ]);
      setClaims(await claimsRes.json());
      const commData = await commissionsRes.json();
      setCommissions(commData.items ?? []);
    } catch {
      setView("login");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  async function handleClaim() {
    const login = claimInput.trim();
    if (!login) return;
    setClaimMsg(null);
    try {
      const res = await fetch("/twitch/affiliate/claim", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ streamer_login: login }),
      });
      const data = await res.json();
      if (data.ok) {
        setClaimMsg({ ok: true, text: `${data.claimed} erfolgreich beansprucht!` });
        setClaimInput("");
        fetchData();
      } else {
        setClaimMsg({ ok: false, text: data.error ?? "Fehler beim Beanspruchen." });
      }
    } catch {
      setClaimMsg({ ok: false, text: "Netzwerkfehler." });
    }
  }

  if (loading) {
    return (
      <div className="min-h-screen bg-[#07151d] flex items-center justify-center">
        <div className="w-8 h-8 border-2 border-[#ff7a18] border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  if (view === "login") {
    return (
      <div className="min-h-screen bg-[#07151d] flex items-center justify-center p-6">
        <div className="panel-card rounded-2xl p-10 max-w-md w-full text-center">
          <div className="w-16 h-16 rounded-xl gradient-accent flex items-center justify-center mx-auto mb-6">
            <LogIn size={28} className="text-white" />
          </div>
          <h1 className="text-2xl font-bold text-[#e9f1f7] font-[Sora] mb-3">
            Affiliate-Portal
          </h1>
          <p className="text-[#9bb3c5] text-sm mb-8 leading-relaxed">
            Werde Vertriebler und verdiene 30% Provision auf jede Zahlung deiner
            geworbenen Streamer — dauerhaft und ohne Limit. Melde dich mit
            deinem Twitch-Account an, um loszulegen.
          </p>
          <button
            onClick={() => {
              window.location.href = "/twitch/auth/affiliate/login";
            }}
            className="gradient-accent rounded-xl px-7 py-3.5 font-semibold text-white w-full transition-all duration-200 hover:brightness-110 hover:shadow-[0_0_24px_4px_rgba(255,122,24,0.3)]"
          >
            Mit Twitch anmelden
          </button>
        </div>
      </div>
    );
  }

  const totalCents = commissions.reduce((s, c) => s + c.commission_cents, 0);
  const transferredCents = commissions
    .filter((c) => c.status === "transferred")
    .reduce((s, c) => s + c.commission_cents, 0);
  const pendingCents = commissions
    .filter((c) => c.status === "pending")
    .reduce((s, c) => s + c.commission_cents, 0);

  return (
    <div className="min-h-screen bg-[#07151d] text-[#e9f1f7] font-[Manrope]">
      {/* Top nav tabs */}
      <nav className="glass sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-6 flex items-center gap-1 h-14 overflow-x-auto">
          <span className="text-sm font-bold text-[#ff7a18] mr-4 shrink-0 font-[Sora]">
            Affiliate
          </span>
          {TABS.map((t) => {
            const Icon = t.icon;
            const active = tab === t.id;
            return (
              <button
                key={t.id}
                onClick={() => setTab(t.id)}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors shrink-0 ${
                  active
                    ? "bg-[#102635] text-[#ff7a18]"
                    : "text-[#9bb3c5] hover:text-[#e9f1f7] hover:bg-[#102635]/50"
                }`}
              >
                <Icon size={16} />
                {t.label}
              </button>
            );
          })}
        </div>
      </nav>

      <main className="max-w-7xl mx-auto px-6 py-10">
        {/* Overview */}
        {tab === "overview" && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold font-[Sora]">Übersicht</h2>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <StatCard
                label="Gesamtverdienst"
                value={formatCents(totalCents)}
                icon={<Receipt size={20} />}
              />
              <StatCard
                label="Ausstehend"
                value={formatCents(pendingCents)}
                icon={<Clock size={20} />}
              />
              <StatCard
                label="Übertragen"
                value={formatCents(transferredCents)}
                icon={<ArrowRightLeft size={20} />}
              />
            </div>

            <StripeCard profile={profile} />
          </div>
        )}

        {/* Streamers */}
        {tab === "streamers" && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold font-[Sora]">Streamer</h2>

            <div className="panel-card rounded-xl p-6">
              <h3 className="text-sm font-semibold text-[#9bb3c5] uppercase tracking-wider mb-4">
                Streamer beanspruchen
              </h3>
              <div className="flex gap-3">
                <input
                  type="text"
                  placeholder="Twitch-Login eingeben..."
                  value={claimInput}
                  onChange={(e) => setClaimInput(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleClaim()}
                  className="flex-1 bg-[#07151d] border border-[rgba(194,221,240,0.14)] rounded-lg px-4 py-2.5 text-sm text-[#e9f1f7] placeholder-[#9bb3c5]/50 focus:outline-none focus:border-[#ff7a18] transition-colors"
                />
                <button
                  onClick={handleClaim}
                  className="gradient-accent rounded-lg px-5 py-2.5 text-sm font-semibold text-white transition-all hover:brightness-110"
                >
                  Beanspruchen
                </button>
              </div>
              {claimMsg && (
                <div
                  className={`mt-3 flex items-center gap-2 text-sm ${claimMsg.ok ? "text-emerald-400" : "text-red-400"}`}
                >
                  {claimMsg.ok ? <CheckCircle size={16} /> : <AlertCircle size={16} />}
                  {claimMsg.text}
                </div>
              )}
            </div>

            <div className="panel-card rounded-xl overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[rgba(194,221,240,0.14)] text-[#9bb3c5] text-left">
                    <th className="px-6 py-3 font-medium">Login</th>
                    <th className="px-6 py-3 font-medium">Anzahl</th>
                    <th className="px-6 py-3 font-medium">Gesamtverdienst</th>
                    <th className="px-6 py-3 font-medium">Beansprucht am</th>
                  </tr>
                </thead>
                <tbody>
                  {claims.map((c) => (
                    <tr
                      key={c.claimed_streamer_login}
                      className="border-b border-[rgba(194,221,240,0.14)] last:border-0"
                    >
                      <td className="px-6 py-3 font-medium">{c.claimed_streamer_login}</td>
                      <td className="px-6 py-3">{c.commission_count}</td>
                      <td className="px-6 py-3">{formatCents(c.total_commission_cents)}</td>
                      <td className="px-6 py-3 text-[#9bb3c5]">
                        {new Date(c.claimed_at).toLocaleDateString("de-DE")}
                      </td>
                    </tr>
                  ))}
                  {claims.length === 0 && (
                    <tr>
                      <td colSpan={4} className="px-6 py-8 text-center text-[#9bb3c5]">
                        Noch keine Streamer beansprucht.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Commissions */}
        {tab === "commissions" && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold font-[Sora]">Provisionen</h2>

            <div className="panel-card rounded-xl overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[rgba(194,221,240,0.14)] text-[#9bb3c5] text-left">
                    <th className="px-6 py-3 font-medium">Datum</th>
                    <th className="px-6 py-3 font-medium">Streamer</th>
                    <th className="px-6 py-3 font-medium">Brutto</th>
                    <th className="px-6 py-3 font-medium">Provision (30%)</th>
                    <th className="px-6 py-3 font-medium">Status</th>
                    <th className="px-6 py-3 font-medium">Transfer-ID</th>
                  </tr>
                </thead>
                <tbody>
                  {commissions.map((c) => (
                    <tr
                      key={c.id}
                      className="border-b border-[rgba(194,221,240,0.14)] last:border-0"
                    >
                      <td className="px-6 py-3 text-[#9bb3c5]">
                        {new Date(c.created_at).toLocaleDateString("de-DE")}
                      </td>
                      <td className="px-6 py-3 font-medium">{c.streamer_login}</td>
                      <td className="px-6 py-3">{formatCents(c.brutto_cents)}</td>
                      <td className="px-6 py-3 text-[#10b7ad] font-medium">
                        {formatCents(c.commission_cents)}
                      </td>
                      <td className="px-6 py-3">
                        <StatusBadge status={c.status} />
                      </td>
                      <td className="px-6 py-3 text-[#9bb3c5] font-mono text-xs">
                        {c.id}
                      </td>
                    </tr>
                  ))}
                  {commissions.length === 0 && (
                    <tr>
                      <td colSpan={6} className="px-6 py-8 text-center text-[#9bb3c5]">
                        Noch keine Provisionen vorhanden.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Settings */}
        {tab === "settings" && profile && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold font-[Sora]">Einstellungen</h2>

            <div className="panel-card rounded-xl p-6 space-y-4">
              <h3 className="text-sm font-semibold text-[#9bb3c5] uppercase tracking-wider">
                Profil
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <InfoField label="Twitch Login" value={profile.twitch_login} />
                <InfoField label="Anzeigename" value={profile.display_name} />
                <InfoField label="E-Mail" value={profile.email} />
                <InfoField label="Name" value={profile.full_name} />
              </div>
            </div>

            <StripeCard profile={profile} />
          </div>
        )}
      </main>
    </div>
  );
}

function StatCard({
  label,
  value,
  icon,
}: {
  label: string;
  value: string;
  icon: React.ReactNode;
}) {
  return (
    <div className="panel-card rounded-xl p-6">
      <div className="flex items-center justify-between mb-2">
        <span className="text-sm text-[#9bb3c5]">{label}</span>
        <span className="text-[#ff7a18]">{icon}</span>
      </div>
      <p className="text-2xl font-bold font-[Sora]">{value}</p>
    </div>
  );
}

function StripeCard({ profile }: { profile: AffiliateProfile | null }) {
  if (!profile) return null;
  const connected = profile.stripe_connect_status === "connected";
  return (
    <div className="panel-card rounded-xl p-6">
      <h3 className="text-sm font-semibold text-[#9bb3c5] uppercase tracking-wider mb-4">
        Stripe Connect
      </h3>
      {connected ? (
        <div className="flex items-center gap-2 text-emerald-400">
          <CheckCircle size={18} />
          <span className="font-medium">Verbunden</span>
        </div>
      ) : (
        <div className="space-y-3">
          <div className="flex items-center gap-2 text-yellow-400">
            <XCircle size={18} />
            <span className="text-sm">Nicht verbunden</span>
          </div>
          <button
            onClick={() => {
              window.location.href = "/twitch/affiliate/connect/stripe";
            }}
            className="gradient-accent rounded-lg px-5 py-2.5 text-sm font-semibold text-white transition-all hover:brightness-110"
          >
            Stripe-Konto verbinden
          </button>
        </div>
      )}
    </div>
  );
}

function InfoField({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span className="text-xs text-[#9bb3c5] uppercase tracking-wider">{label}</span>
      <p className="text-sm font-medium mt-1">{value || "—"}</p>
    </div>
  );
}
