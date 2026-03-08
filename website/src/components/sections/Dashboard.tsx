import { useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import {
  BarChart3,
  Brain,
  CalendarRange,
  GraduationCap,
  LayoutDashboard,
  MessageSquare,
  Sparkles,
  Target,
  UserSearch,
  Users,
} from "lucide-react";
import { SectionHeading } from "@/components/ui/SectionHeading";
import { BrowserMockup } from "@/components/ui/BrowserMockup";
import { ScrollReveal } from "@/components/ui/ScrollReveal";

const DEMO_DASHBOARD_URL = "https://demo.earlysalty.com/twitch/demo/";

const PRODUCT_TABS: Array<{ id: string; label: string; beta?: boolean }> = [
  { id: "overview", label: "Übersicht" },
  { id: "streams", label: "Streams" },
  { id: "chat", label: "Chat" },
  { id: "growth", label: "Wachstum" },
  { id: "audience", label: "Audience" },
  { id: "viewers", label: "Viewer" },
  { id: "compare", label: "Vergleich" },
  { id: "schedule", label: "Zeitplan" },
  { id: "coaching", label: "Coaching" },
  { id: "monetization", label: "Monetization" },
  { id: "category", label: "Kategorie" },
  { id: "experimental", label: "Labor", beta: true },
  { id: "ai", label: "KI Analyse", beta: true },
] as const;

const TABS = [
  {
    id: "overview",
    label: "Übersicht",
    Icon: LayoutDashboard,
    teaser: "Viewer, Peak, Chat und Momentum in einem schnellen Einstieg.",
    title: "Alles Wichtige in einem Board",
    description:
      "Die Übersicht verdichtet die wichtigsten Signale, bevor du tiefer in Chat, Audience, Viewer, Wachstum oder Coaching gehst.",
    stats: [
      ["Ø Viewer", "142", "+12%"],
      ["Peak", "218", "20:15"],
      ["Chatters", "63", "starke Basis"],
    ],
    rows: [
      ["Momentum", "19-22 Uhr"],
      ["Retention", "68%"],
      ["Fokus", "Chat + Viewer"],
    ],
    bars: [46, 54, 51, 67, 74, 88, 79],
    signals: [
      "Der schnelle Daily Check-in fuer Performance und Richtung.",
      "Macht auffaellige Veraenderungen sichtbar, bevor man Details liest.",
      "Perfekter Startpunkt fuer alle anderen Analytics-Tabs.",
    ],
  },
  {
    id: "chat",
    label: "Chat",
    Icon: MessageSquare,
    teaser: "Chat-Tiefe, Aktivitaet und Wiederkehrer statt nur Message Count.",
    title: "Chat als echte Community lesen",
    description:
      "Hier wird sichtbar, wann der Chat wirklich lebt, wie tief die Gespräche gehen und ob aus Aktivität echte Bindung entsteht.",
    stats: [
      ["Aktive Chatters", "91", "38 wiederkehrend"],
      ["Penetration", "31%", "stark im Kernslot"],
      ["Msg / 100 VM", "46.2", "gute Tiefe"],
    ],
    rows: [
      ["Peak", "20:15"],
      ["Return Rate", "42%"],
      ["Hype", "3 Spikes"],
    ],
    bars: [18, 26, 22, 28, 37, 52, 64, 78, 61, 46, 33, 24],
    signals: [
      "Peak-Momente und Tageszeit-Signale werden sofort lesbar.",
      "Neue und wiederkehrende Chatter lassen sich klar trennen.",
      "Hilft, Unterhaltung statt nur Aktivitaet zu bewerten.",
    ],
  },
  {
    id: "audience",
    label: "Audience",
    Icon: Target,
    teaser: "Core Audience, Discovery und Cross-Community sauber getrennt.",
    title: "Audience mit echtem Kontext",
    description:
      "Audience zeigt nicht nur, wie viele Leute da waren, sondern welche Gruppen bleiben, entdecken oder über Partner-Netzwerke ankommen.",
    stats: [
      ["Core", "46%", "wiederkehrend"],
      ["Neu", "28%", "Discovery"],
      ["Shared", "17%", "Partner-Fit"],
    ],
    rows: [
      ["Watchtime", "34 Min."],
      ["Discovery", "Do + Fr"],
      ["Raid Fit", "hoch"],
    ],
    bars: [46, 28, 17, 9],
    signals: [
      "Zeigt, welche Reichweite wirklich Bindung aufbaut.",
      "Hilft bei Raid-Entscheidungen und Partner-Matching.",
      "Macht Cross-Community innerhalb des Netzwerks sichtbar.",
    ],
  },
  {
    id: "viewers",
    label: "Viewer",
    Icon: UserSearch,
    teaser: "Viewer werden als Profile und Segmente lesbar, nicht nur als Zahl.",
    title: "Viewer-Daten endlich nutzbar",
    description:
      "Der Viewer-Tab macht Wiederkehrer, Dormant Viewer und besonders wertvolle Community-Profile sichtbar.",
    stats: [
      ["Wiederkehrer", "58%", "mehrfach aktiv"],
      ["Dormant", "24", "rueckholbar"],
      ["High Value", "19", "Chat + Watchtime"],
    ],
    rows: [
      ["Pool", "312"],
      ["Core Layer", "74"],
      ["Reactivation", "12"],
    ],
    bars: [58, 23, 19],
    signals: [
      "Hilft bei Rewards, Reaktivierung und Community-Pflege.",
      "Macht Viewer-Verhalten über einzelne Streams hinaus sichtbar.",
      "Verbindet Chat-, Audience- und Growth-Signale sinnvoll.",
    ],
  },
  {
    id: "growth",
    label: "Wachstum",
    Icon: BarChart3,
    teaser: "Wachstum liest Titel, Timing, Trends und Raid-Retention zusammen.",
    title: "Wachstum als Muster statt Zufall",
    description:
      "Hier laufen Monatsentwicklung, Tags, Titel, Wochentage und Raid-Retention zusammen, damit du den Grund hinter dem Wachstum erkennst.",
    stats: [
      ["Hours Watched", "18.4k", "+16%"],
      ["Follower", "+412", "stabil"],
      ["Raid Retention", "63%", "nach 10 Min."],
    ],
    rows: [
      ["Bester Tag", "Donnerstag"],
      ["Titel-Muster", "Road to"],
      ["Sweet Spot", "19-22 Uhr"],
    ],
    bars: [42, 47, 56, 61, 74, 82],
    signals: [
      "Macht echte Wachstumshebel statt nur Endwerte sichtbar.",
      "Verbindet Schedule, Titel und Reichweite in einer Sicht.",
      "Zeigt, ob Partner-Raids nachhaltig weitertragen.",
    ],
  },
  {
    id: "coaching",
    label: "Coaching",
    Icon: GraduationCap,
    teaser: "Analytics wird in konkrete nächste Schritte übersetzt.",
    title: "Coaching macht Analytics handlungsfaehig",
    description:
      "Der Coaching-Tab verdichtet Daten in priorisierte Empfehlungen für Timing, Titel, Retention, Community und Netzwerk.",
    stats: [
      ["Top Hebel", "3", "priorisiert"],
      ["Gap", "6%", "zum Peer-Cluster"],
      ["Effizienz", "1.8x", "Viewer-Hours"],
    ],
    rows: [
      ["Prioritaet 1", "Startzeit"],
      ["Titel-Coach", "Ranked + Ziel"],
      ["Netzwerk", "aktiv"],
    ],
    bars: [86, 74, 69],
    signals: [
      "Bringt Analyse und konkrete Maßnahmen zusammen.",
      "Gibt Daten durch Peer-Vergleiche einen Maßstab.",
      "Hilft direkt bei Content-, Titel- und Timing-Entscheidungen.",
    ],
  },
] as const;

type DemoTabId = (typeof TABS)[number]["id"];

const callouts = [
  {
    Icon: LayoutDashboard,
    title: "13 echte Perspektiven",
    description:
      "Neben der Vorschau gehören auch Streams, Vergleich, Zeitplan, Kategorie, Monetization, Labor und KI Analyse zum Produkt.",
  },
  {
    Icon: Users,
    title: "Viewer mit Struktur",
    description:
      "Nicht nur Durchschnittswerte: Wiederkehrer, Discovery, Core Audience und Cross-Community lassen sich getrennt lesen.",
  },
  {
    Icon: Sparkles,
    title: "Coaching mit Kontext",
    description:
      "Empfehlungen entstehen aus Timing, Titeln, Retention, Konkurrenz und Netzwerk statt aus isolierten Metriken.",
  },
];

export function Dashboard() {
  const [activeTab, setActiveTab] = useState<DemoTabId>("chat");
  const activeDemo = TABS.find((tab) => tab.id === activeTab) ?? TABS[0];
  const secondaryTabs = PRODUCT_TABS.filter(
    (tab) => !TABS.some((previewTab) => previewTab.id === tab.id),
  );

  return (
    <section id="dashboard" className="py-24">
      <div className="max-w-7xl mx-auto px-6">
        <SectionHeading
          badge="Analytics"
          title="Analytics auf einem neuen Level"
          subtitle="13 spezialisierte Tabs fuer jeden Aspekt deines Streams. Chat, Audience, Viewer, Wachstum und Coaching greifen direkt ineinander statt isoliert nebeneinander zu stehen."
        />

        <ScrollReveal delay={0.1}>
          <div className="mt-12 flex flex-wrap gap-2 justify-center">
            {TABS.map((tab) => {
              const isActive = tab.id === activeTab;
              const Icon = tab.Icon;

              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={[
                    "rounded-lg px-4 py-2 text-sm transition inline-flex items-center gap-2",
                    isActive
                      ? "gradient-accent text-white"
                      : "bg-[var(--color-card)] border border-[var(--color-border)] text-[var(--color-text-secondary)] hover:text-[var(--color-text-primary)]",
                  ].join(" ")}
                >
                  <Icon size={16} />
                  {tab.label}
                </button>
              );
            })}
          </div>

          <p className="mt-4 text-center text-sm text-[var(--color-text-secondary)] max-w-3xl mx-auto">
            {activeDemo.teaser}
          </p>
        </ScrollReveal>

        <ScrollReveal delay={0.15}>
          <div className="mt-8">
            <BrowserMockup url="demo.earlysalty.com/twitch/demo">
              <AnimatePresence mode="wait">
                <motion.div
                  key={activeTab}
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -8 }}
                  transition={{ duration: 0.28, ease: "easeOut" }}
                  className="bg-[radial-gradient(circle_at_top_right,rgba(255,122,24,0.16),transparent_32%),radial-gradient(circle_at_bottom_left,rgba(16,183,173,0.18),transparent_30%),linear-gradient(180deg,rgba(7,21,29,0.96),rgba(10,29,41,0.96))] p-4 md:p-5"
                >
                  <div className="rounded-2xl border border-[var(--color-border)] bg-[rgba(7,21,29,0.62)] p-3 md:p-4">
                    <div className="flex flex-col gap-4">
                      <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
                        <div className="max-w-2xl">
                          <span className="inline-flex items-center gap-2 rounded-full border border-[rgba(16,183,173,0.24)] bg-[rgba(16,183,173,0.12)] px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-[var(--color-accent)]">
                            Produktvorschau
                          </span>
                          <h3 className="mt-3 text-lg font-semibold text-[var(--color-text-primary)] md:text-xl">
                            {activeDemo.title}
                          </h3>
                          <p className="mt-2 max-w-xl text-sm leading-relaxed text-[var(--color-text-secondary)]">
                            {activeDemo.description}
                          </p>
                        </div>

                        <div className="grid gap-2 rounded-2xl border border-[var(--color-border)] bg-[rgba(255,255,255,0.03)] p-4 text-sm text-[var(--color-text-secondary)] sm:grid-cols-3 xl:min-w-[340px] xl:grid-cols-1">
                          <div className="flex items-center gap-2">
                            <LayoutDashboard className="h-4 w-4 text-[var(--color-primary)]" />
                            13 Tabs im Produkt
                          </div>
                          <div className="flex items-center gap-2">
                            <CalendarRange className="h-4 w-4 text-[var(--color-accent)]" />
                            Fokus auf 30 Tage
                          </div>
                          <div className="flex items-center gap-2">
                            <Brain className="h-4 w-4 text-[var(--color-success)]" />
                            Direkt auf der Website
                          </div>
                        </div>
                      </div>

                      <div className="flex flex-wrap gap-2">
                        {PRODUCT_TABS.map((tab) => {
                          const isActive = tab.id === activeTab;
                          const isPreviewed = TABS.some((previewTab) => previewTab.id === tab.id);

                          return (
                            <span
                              key={tab.id}
                              className={[
                                "inline-flex items-center gap-2 rounded-full border px-3 py-1.5 text-xs font-medium",
                                isActive
                                  ? "border-transparent gradient-accent text-white"
                                  : "border-[var(--color-border)] bg-[rgba(255,255,255,0.03)] text-[var(--color-text-secondary)]",
                              ].join(" ")}
                            >
                              {tab.label}
                              {tab.beta ? (
                                <span className="rounded-full border border-[rgba(255,255,255,0.18)] px-1.5 py-0.5 text-[9px] uppercase tracking-[0.12em]">
                                  Beta
                                </span>
                              ) : null}
                              {!isPreviewed && !tab.beta ? (
                                <span className="text-[10px] uppercase tracking-[0.12em] text-[var(--color-accent)]">
                                  live
                                </span>
                              ) : null}
                            </span>
                          );
                        })}
                      </div>

                      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_320px]">
                        <div className="space-y-4">
                          <div className="grid gap-3 sm:grid-cols-3">
                            {activeDemo.stats.map(([label, value, detail], index) => (
                              <div
                                key={label}
                                className="rounded-2xl border border-[var(--color-border)] bg-[rgba(8,24,33,0.84)] p-4"
                              >
                                <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-[var(--color-text-secondary)]">
                                  {label}
                                </p>
                                <div className="mt-3 display-font text-3xl font-bold text-[var(--color-text-primary)]">
                                  {value}
                                </div>
                                <p className="mt-2 text-sm text-[var(--color-text-secondary)]">
                                  {detail}
                                </p>
                                <div
                                  className={`mt-4 h-1.5 rounded-full ${
                                    index === 0
                                      ? "bg-[rgba(255,122,24,0.78)]"
                                      : index === 1
                                      ? "bg-[rgba(16,183,173,0.78)]"
                                      : "bg-[rgba(46,204,113,0.78)]"
                                  }`}
                                />
                              </div>
                            ))}
                          </div>

                          <div className="rounded-2xl border border-[var(--color-border)] bg-[rgba(6,18,25,0.65)] p-4 md:p-5">
                            <div className="flex flex-wrap items-center justify-between gap-3">
                              <div>
                                <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-[var(--color-accent)]">
                                  Analytics Preview
                                </p>
                                <h4 className="mt-1 text-base font-semibold text-[var(--color-text-primary)]">
                                  {activeDemo.label} im Fokus
                                </h4>
                              </div>
                              <span className="rounded-full border border-[var(--color-border)] bg-[rgba(255,255,255,0.03)] px-3 py-1 text-xs text-[var(--color-text-secondary)]">
                                30 Tage
                              </span>
                            </div>

                            <div className="mt-6 flex h-44 items-end gap-2">
                              {activeDemo.bars.map((bar, index) => (
                                <div key={`${activeDemo.id}-${index}`} className="flex flex-1 flex-col items-center gap-3">
                                  <div className="relative flex h-full w-full items-end">
                                    <div
                                      className={`w-full rounded-t-xl ${
                                        index === activeDemo.bars.length - 1
                                          ? "bg-gradient-to-t from-[rgba(16,183,173,0.28)] to-[rgba(16,183,173,0.96)]"
                                          : "bg-gradient-to-t from-[rgba(255,122,24,0.28)] to-[rgba(255,122,24,0.9)]"
                                      }`}
                                      style={{ height: `${bar}%` }}
                                    />
                                  </div>
                                  <span className="text-[10px] text-[var(--color-text-secondary)]">
                                    {index + 1}
                                  </span>
                                </div>
                              ))}
                            </div>

                            <div className="mt-5 grid gap-3 sm:grid-cols-3">
                              {activeDemo.rows.map(([label, value]) => (
                                <div
                                  key={label}
                                  className="rounded-xl border border-[var(--color-border)] bg-[rgba(255,255,255,0.03)] p-3"
                                >
                                  <p className="text-xs uppercase tracking-[0.14em] text-[var(--color-text-secondary)]">
                                    {label}
                                  </p>
                                  <p className="mt-2 text-sm font-semibold text-[var(--color-text-primary)]">
                                    {value}
                                  </p>
                                </div>
                              ))}
                            </div>
                          </div>
                        </div>

                        <aside className="panel-card rounded-2xl p-5">
                          <div className="w-fit rounded-full border border-[rgba(255,122,24,0.24)] bg-[rgba(255,122,24,0.12)] px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-[var(--color-primary)]">
                            {activeDemo.label} Tab
                          </div>

                          <p className="mt-4 text-sm leading-relaxed text-[var(--color-text-secondary)]">
                            {activeDemo.description}
                          </p>

                          <div className="mt-6 space-y-3">
                            {activeDemo.signals.map((signal) => (
                              <div
                                key={signal}
                                className="flex items-start gap-3 rounded-xl border border-[var(--color-border)] bg-[rgba(255,255,255,0.03)] px-3 py-3 text-sm text-[var(--color-text-secondary)]"
                              >
                                <span className="mt-1 h-2 w-2 rounded-full bg-[var(--color-accent)]" />
                                <span>{signal}</span>
                              </div>
                            ))}
                          </div>

                          <div className="mt-6 rounded-2xl border border-[var(--color-border)] bg-[rgba(255,255,255,0.03)] p-4">
                            <p className="text-xs font-semibold uppercase tracking-[0.14em] text-[var(--color-text-secondary)]">
                              Weitere Tabs live im Produkt
                            </p>
                            <div className="mt-3 flex flex-wrap gap-2">
                              {secondaryTabs.map((tab) => (
                                <span
                                  key={tab.id}
                                  className="rounded-full border border-[var(--color-border)] bg-[rgba(7,21,29,0.75)] px-3 py-1.5 text-xs text-[var(--color-text-secondary)]"
                                >
                                  {tab.label}
                                </span>
                              ))}
                            </div>
                          </div>
                        </aside>
                      </div>
                    </div>
                  </div>
                </motion.div>
              </AnimatePresence>
            </BrowserMockup>
          </div>
        </ScrollReveal>

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

        <div className="mt-10 text-center">
          <a
            href={DEMO_DASHBOARD_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="text-[var(--color-primary)] hover:text-[var(--color-primary-hover)] font-semibold transition"
          >
            Komplettes Demo-Dashboard ansehen →
          </a>
        </div>
      </div>
    </section>
  );
}
