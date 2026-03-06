import { motion } from "framer-motion";
import { CheckCircle2 } from "lucide-react";
import { ScrollReveal } from "@/components/ui/ScrollReveal";
import { GradientText } from "@/components/ui/GradientText";

const features = [
  {
    title: "Fairer Algorithmus",
    description:
      "Gewichtetes System basierend auf letztem Raid-Zeitpunkt und Viewer-Overlap",
  },
  {
    title: "Viewer-Tracking",
    description:
      "Echtzeit-Erfassung der Viewer-Zahlen aller Partner-Kanäle",
  },
  {
    title: "Erfolgsstatistiken",
    description:
      "Detaillierte Auswertung jedes Raids mit Viewer-Retention",
  },
  {
    title: "Silent Mode",
    description:
      "Automatische Raids ohne Chat-Ankündigung möglich",
  },
];

const streamers = [
  { name: "StreamerA", viewers: "142 Viewer", score: "Score: 0.94", highlight: true },
  { name: "StreamerB", viewers: "87 Viewer", score: "Score: 0.71", highlight: false },
  { name: "StreamerC", viewers: "203 Viewer", score: "Score: 0.58", highlight: false },
];

const containerVariants = {
  hidden: {},
  visible: {
    transition: {
      staggerChildren: 0.12,
    },
  },
};

const rowVariants = {
  hidden: { opacity: 0, x: 20 },
  visible: { opacity: 1, x: 0, transition: { duration: 0.45, ease: "easeOut" as const } },
};

export function RaidSystem() {
  return (
    <section id="raid" className="py-24">
      <div className="max-w-7xl mx-auto px-6">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-16 items-center">

          {/* LEFT — text */}
          <ScrollReveal>
            <p className="text-sm text-[var(--color-accent)] font-medium uppercase tracking-wider mb-4">
              Auto-Raid System
            </p>

            <h2 className="text-3xl md:text-4xl font-bold text-[var(--color-text-primary)] mb-6">
              <GradientText>Intelligentes</GradientText> Raid-System
            </h2>

            <p className="text-[var(--color-text-secondary)] text-lg mb-8 leading-relaxed">
              Unser Raid-System nutzt einen fairen Algorithmus, der sicherstellt,
              dass alle Partner gleichmäßig Raids erhalten.
            </p>

            <ul className="space-y-4">
              {features.map((feature) => (
                <li key={feature.title} className="flex items-start gap-3">
                  <CheckCircle2
                    size={20}
                    className="text-[var(--color-accent)] shrink-0 mt-1"
                  />
                  <span className="text-[var(--color-text-secondary)] leading-relaxed">
                    <strong className="text-[var(--color-text-primary)] font-semibold">
                      {feature.title}
                    </strong>{" "}
                    — {feature.description}
                  </span>
                </li>
              ))}
            </ul>
          </ScrollReveal>

          {/* RIGHT — visual mockup */}
          <ScrollReveal delay={0.2}>
            <div className="panel-card rounded-2xl p-8">
              <p className="text-lg font-semibold text-[var(--color-text-primary)] mb-6">
                Raid-Auswahl
              </p>

              <motion.div
                variants={containerVariants}
                initial="hidden"
                whileInView="visible"
                viewport={{ once: true, margin: "-60px" }}
              >
                {streamers.map((streamer) => (
                  <motion.div
                    key={streamer.name}
                    variants={rowVariants}
                    className="bg-[var(--color-bg)]/50 rounded-lg p-4 mb-3 flex justify-between items-center"
                    style={{
                      border: streamer.highlight
                        ? "1px solid rgba(16,183,173,0.35)"
                        : "1px solid var(--color-border)",
                    }}
                  >
                    <div className="flex items-center gap-3">
                      {/* Avatar placeholder */}
                      <div
                        className="w-8 h-8 rounded-full shrink-0"
                        style={{
                          background: streamer.highlight
                            ? "linear-gradient(135deg, #ff7a18, #10b7ad)"
                            : "linear-gradient(135deg, #1a3f57, #204e6b)",
                        }}
                      />
                      <div>
                        <p className="text-sm font-semibold text-[var(--color-text-primary)]">
                          {streamer.name}
                        </p>
                        <p className="text-xs text-[var(--color-text-secondary)]">
                          {streamer.viewers}
                        </p>
                      </div>
                    </div>
                    <span
                      className="text-xs font-mono font-semibold px-2 py-1 rounded"
                      style={{
                        color: streamer.highlight
                          ? "var(--color-accent)"
                          : "var(--color-text-secondary)",
                        background: streamer.highlight
                          ? "rgba(16,183,173,0.12)"
                          : "rgba(155,179,197,0.08)",
                      }}
                    >
                      {streamer.score}
                    </span>
                  </motion.div>
                ))}
              </motion.div>

              {/* CTA mock button */}
              <div className="gradient-accent rounded-lg px-4 py-2 text-sm text-white w-full text-center mt-4 font-semibold select-none">
                Raid starten → StreamerA
              </div>
            </div>
          </ScrollReveal>

        </div>
      </div>
    </section>
  );
}
