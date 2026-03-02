import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Sparkles, Flame, Heart } from "lucide-react";
import { SectionHeading } from "@/components/ui/SectionHeading";
import { BrowserMockup } from "@/components/ui/BrowserMockup";
import { ScrollReveal } from "@/components/ui/ScrollReveal";

// ---------------------------------------------------------------------------
// Tab definitions
// ---------------------------------------------------------------------------

const TABS = ["Uebersicht", "Performance", "Heatmap", "Audience", "Health Score"] as const;
type Tab = (typeof TABS)[number];

// ---------------------------------------------------------------------------
// Tab mock content
// ---------------------------------------------------------------------------

function TabUebersicht() {
  return (
    <div className="h-full flex flex-col gap-4">
      {/* KPI row */}
      <div className="grid grid-cols-4 gap-3">
        {[
          { label: "Avg. Viewer", color: "#ff7a18" },
          { label: "Peak Viewer", color: "#10b7ad" },
          { label: "Stunden", color: "#2ecc71" },
          { label: "Follower", color: "#f5b642" },
        ].map(({ label, color }) => (
          <div
            key={label}
            className="rounded-lg p-3 flex flex-col gap-2"
            style={{ background: "rgba(255,255,255,0.04)", border: "1px solid var(--color-border)" }}
          >
            <div className="h-2 rounded-full w-3/4" style={{ background: color, opacity: 0.7 }} />
            <div className="h-5 rounded w-1/2" style={{ background: color }} />
            <p className="text-xs text-[var(--color-text-secondary)]">{label}</p>
          </div>
        ))}
      </div>
      {/* Chart placeholder */}
      <div
        className="flex-1 rounded-xl p-4 flex flex-col justify-end gap-1"
        style={{ background: "rgba(255,255,255,0.03)", border: "1px solid var(--color-border)" }}
      >
        <p className="text-xs text-[var(--color-text-secondary)] mb-2">Viewer-Verlauf (7 Tage)</p>
        <div className="flex items-end gap-1.5 h-20">
          {[45, 62, 55, 80, 70, 90, 76].map((h, i) => (
            <div
              key={i}
              className="flex-1 rounded-t"
              style={{
                height: `${h}%`,
                background: `linear-gradient(180deg, #ff7a18 0%, rgba(255,122,24,0.3) 100%)`,
                opacity: 0.75,
              }}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function TabPerformance() {
  const bars = [
    { label: "Engagement Rate", value: 82, color: "#ff7a18" },
    { label: "Chat Activity", value: 67, color: "#10b7ad" },
    { label: "Viewer Retention", value: 74, color: "#2ecc71" },
    { label: "Follower Growth", value: 55, color: "#f5b642" },
    { label: "Clip Views", value: 91, color: "#a78bfa" },
  ];
  return (
    <div className="h-full flex flex-col justify-center gap-4 py-4">
      <p className="text-xs text-[var(--color-text-secondary)] mb-1">Stream-Performance Metriken</p>
      {bars.map(({ label, value, color }) => (
        <div key={label} className="flex items-center gap-3">
          <p className="text-xs text-[var(--color-text-secondary)] w-36 shrink-0">{label}</p>
          <div className="flex-1 h-3 rounded-full" style={{ background: "rgba(255,255,255,0.06)" }}>
            <div
              className="h-full rounded-full"
              style={{ width: `${value}%`, background: color, opacity: 0.85 }}
            />
          </div>
          <p className="text-xs font-mono text-[var(--color-text-primary)] w-8 text-right">{value}%</p>
        </div>
      ))}
    </div>
  );
}

function TabHeatmap() {
  const cols = 12;
  const rows = 6;
  return (
    <div className="h-full flex flex-col gap-3">
      <p className="text-xs text-[var(--color-text-secondary)]">Viewer-Aktivitaet nach Stunde / Wochentag</p>
      <div className="flex-1 flex flex-col gap-1">
        {Array.from({ length: rows }).map((_, r) => (
          <div key={r} className="flex gap-1 flex-1">
            {Array.from({ length: cols }).map((_, c) => {
              const intensity = Math.random();
              const isOrange = intensity > 0.6;
              const isTeal = intensity > 0.35 && !isOrange;
              const bg = isOrange
                ? `rgba(255,122,24,${0.3 + intensity * 0.7})`
                : isTeal
                ? `rgba(16,183,173,${0.25 + intensity * 0.6})`
                : `rgba(255,255,255,0.05)`;
              return (
                <div
                  key={c}
                  className="flex-1 rounded-sm"
                  style={{ background: bg }}
                />
              );
            })}
          </div>
        ))}
      </div>
      <div className="flex items-center gap-3 mt-1">
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded-sm" style={{ background: "rgba(255,122,24,0.8)" }} />
          <span className="text-xs text-[var(--color-text-secondary)]">Hoch</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded-sm" style={{ background: "rgba(16,183,173,0.7)" }} />
          <span className="text-xs text-[var(--color-text-secondary)]">Mittel</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded-sm" style={{ background: "rgba(255,255,255,0.08)" }} />
          <span className="text-xs text-[var(--color-text-secondary)]">Niedrig</span>
        </div>
      </div>
    </div>
  );
}

function TabAudience() {
  const segments = [
    { label: "Stammzuschauer", pct: 42, color: "#ff7a18" },
    { label: "Gelegentlich", pct: 31, color: "#10b7ad" },
    { label: "Neu", pct: 27, color: "#2ecc71" },
  ];
  // Simple donut ring using conic-gradient
  const conicStr = `conic-gradient(#ff7a18 0% 42%, #10b7ad 42% 73%, #2ecc71 73% 100%)`;
  return (
    <div className="h-full flex items-center gap-8">
      {/* Donut */}
      <div className="shrink-0 flex items-center justify-center">
        <div
          className="w-32 h-32 rounded-full flex items-center justify-center"
          style={{ background: conicStr }}
        >
          <div
            className="w-20 h-20 rounded-full flex flex-col items-center justify-center"
            style={{ background: "var(--color-card)" }}
          >
            <p className="text-lg font-bold text-[var(--color-text-primary)]">42%</p>
            <p className="text-[10px] text-[var(--color-text-secondary)]">Stamm</p>
          </div>
        </div>
      </div>
      {/* Legend + stats */}
      <div className="flex-1 space-y-4">
        {segments.map(({ label, pct, color }) => (
          <div key={label} className="flex items-center gap-3">
            <div className="w-3 h-3 rounded-full shrink-0" style={{ background: color }} />
            <p className="text-sm text-[var(--color-text-secondary)] flex-1">{label}</p>
            <p className="text-sm font-semibold text-[var(--color-text-primary)]">{pct}%</p>
          </div>
        ))}
        <div
          className="mt-2 rounded-lg p-3"
          style={{ background: "rgba(255,255,255,0.04)", border: "1px solid var(--color-border)" }}
        >
          <p className="text-xs text-[var(--color-text-secondary)]">Avg. Beobachtungszeit</p>
          <p className="text-lg font-bold text-[var(--color-text-primary)]">34 Min.</p>
        </div>
      </div>
    </div>
  );
}

function TabHealthScore() {
  const score = 87;
  const circumference = 2 * Math.PI * 44; // r=44
  const strokeDashoffset = circumference - (score / 100) * circumference;
  return (
    <div className="h-full flex flex-col items-center justify-center gap-6">
      <p className="text-xs text-[var(--color-text-secondary)]">Kanal-Gesundheitsscore</p>
      {/* Circular progress */}
      <div className="relative">
        <svg width="140" height="140" viewBox="0 0 100 100">
          {/* Track */}
          <circle cx="50" cy="50" r="44" fill="none" stroke="rgba(255,255,255,0.07)" strokeWidth="8" />
          {/* Progress arc */}
          <circle
            cx="50"
            cy="50"
            r="44"
            fill="none"
            stroke="url(#hsGrad)"
            strokeWidth="8"
            strokeLinecap="round"
            strokeDasharray={circumference}
            strokeDashoffset={strokeDashoffset}
            transform="rotate(-90 50 50)"
          />
          <defs>
            <linearGradient id="hsGrad" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor="#ff7a18" />
              <stop offset="100%" stopColor="#10b7ad" />
            </linearGradient>
          </defs>
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <p className="text-3xl font-bold text-[var(--color-text-primary)]">{score}</p>
          <p className="text-xs text-[var(--color-text-secondary)]">/ 100</p>
        </div>
      </div>
      {/* Sub-scores */}
      <div className="grid grid-cols-3 gap-3 w-full max-w-xs">
        {[
          { label: "Wachstum", v: 91, c: "#2ecc71" },
          { label: "Aktivitaet", v: 84, c: "#ff7a18" },
          { label: "Bindung", v: 86, c: "#10b7ad" },
        ].map(({ label, v, c }) => (
          <div
            key={label}
            className="rounded-lg p-2 text-center"
            style={{ background: "rgba(255,255,255,0.04)", border: "1px solid var(--color-border)" }}
          >
            <p className="text-base font-bold" style={{ color: c }}>{v}</p>
            <p className="text-[10px] text-[var(--color-text-secondary)]">{label}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab content router
// ---------------------------------------------------------------------------

function TabContent({ tab }: { tab: Tab }) {
  return (
    <div className="aspect-video bg-gradient-to-br from-[var(--color-card)] to-[var(--color-bg)] rounded-lg p-8">
      {tab === "Uebersicht" && <TabUebersicht />}
      {tab === "Performance" && <TabPerformance />}
      {tab === "Heatmap" && <TabHeatmap />}
      {tab === "Audience" && <TabAudience />}
      {tab === "Health Score" && <TabHealthScore />}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Callout cards
// ---------------------------------------------------------------------------

const callouts = [
  {
    Icon: Sparkles,
    title: "KI-Coaching",
    description:
      "Personalisierte Verbesserungsvorschlaege basierend auf deinen Stream-Daten",
  },
  {
    Icon: Flame,
    title: "Echtzeit-Heatmaps",
    description:
      "Visualisiere Viewer-Aktivitaet ueber den gesamten Stream",
  },
  {
    Icon: Heart,
    title: "Health Score",
    description:
      "Ganzheitliche Bewertung deiner Kanal-Gesundheit",
  },
];

// ---------------------------------------------------------------------------
// Main export
// ---------------------------------------------------------------------------

export function Dashboard() {
  const [activeTab, setActiveTab] = useState<Tab>("Uebersicht");

  return (
    <section id="dashboard" className="py-24">
      <div className="max-w-7xl mx-auto px-6">

        {/* Heading */}
        <SectionHeading
          badge="Analytics"
          title="Analytics auf einem neuen Level"
          subtitle="13 spezialisierte Tabs fuer jeden Aspekt deines Streams."
        />

        {/* Tab selector */}
        <ScrollReveal delay={0.1}>
          <div className="mt-12 flex flex-wrap gap-2 justify-center">
            {TABS.map((tab) => {
              const isActive = tab === activeTab;
              return (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={[
                    "rounded-lg px-4 py-2 text-sm transition",
                    isActive
                      ? "gradient-accent text-white"
                      : "bg-[var(--color-card)] border border-[var(--color-border)] text-[var(--color-text-secondary)] hover:text-[var(--color-text-primary)]",
                  ].join(" ")}
                >
                  {tab}
                </button>
              );
            })}
          </div>
        </ScrollReveal>

        {/* Browser mockup + animated tab content */}
        <ScrollReveal delay={0.15}>
          <div className="mt-8">
            <BrowserMockup url="demo.earlysalty.com">
              <AnimatePresence mode="wait">
                <motion.div
                  key={activeTab}
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -8 }}
                  transition={{ duration: 0.28, ease: "easeOut" }}
                  className="p-4"
                >
                  <TabContent tab={activeTab} />
                </motion.div>
              </AnimatePresence>
            </BrowserMockup>
          </div>
        </ScrollReveal>

        {/* Callout cards */}
        <ScrollReveal delay={0.2}>
          <div className="mt-12 grid grid-cols-1 md:grid-cols-3 gap-6">
            {callouts.map(({ Icon, title, description }, i) => (
              <motion.div
                key={title}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true, margin: "-60px" }}
                transition={{ duration: 0.5, delay: i * 0.1, ease: "easeOut" }}
                className="panel-card rounded-xl p-6 soft-elevate"
              >
                <div className="w-10 h-10 rounded-lg gradient-accent flex items-center justify-center mb-4">
                  <Icon size={18} className="text-white" />
                </div>
                <h3 className="text-base font-semibold text-[var(--color-text-primary)] mb-2">
                  {title}
                </h3>
                <p className="text-sm text-[var(--color-text-secondary)] leading-relaxed">
                  {description}
                </p>
              </motion.div>
            ))}
          </div>
        </ScrollReveal>

        {/* CTA link */}
        <div className="mt-10 text-center">
          <a
            href="https://demo.earlysalty.com"
            target="_blank"
            rel="noopener noreferrer"
            className="text-[var(--color-primary)] hover:text-[var(--color-primary-hover)] font-semibold transition"
          >
            Demo Dashboard ansehen →
          </a>
        </div>

      </div>
    </section>
  );
}
