import { motion } from "framer-motion";
import { ExternalLink } from "lucide-react";
import { BrowserMockup } from "@/components/ui/BrowserMockup";
import {
  TWITCH_BOT_AUTH_START_URL,
  buildTwitchBotAuthUrl,
} from "@/data/externalLinks";

const HERO_STATS = [
  {
    title: "1900+ Mitglieder — in der deutschen Deadlock-Community"
  },
  {
    title: "Automatisierung für Raids, Analytics, Clips und Moderation",
  },
  {
    title: "Größtes Deadlock-Raid-Netzwerk — auf Twitch im deutschsprachigen Raum",
  },
];

const HERO_STAT_STAIR_LAYOUT = [
  "md:w-[80%] md:mr-[20%]",
  "md:w-[80%] md:mx-auto",
  "md:w-[80%] md:ml-[20%]",
] as const;

export function Hero() {
  return (
    <section
      id="hero"
      className="relative min-h-screen flex flex-col justify-center overflow-hidden"
    >
      <div className="max-w-[96rem] mx-auto px-6 pt-32 pb-20 w-full">
        {/* Centered content */}
        <div className="text-center">
          {/* Badge */}
          <motion.div
            initial={{ opacity: 0, y: -12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5 }}
            className="inline-flex items-center rounded-full px-4 py-1.5 bg-[var(--color-card)] border border-[var(--color-border)] text-sm text-[var(--color-accent)]"
          >
            Deadlock Community Bot
          </motion.div>

          {/* Headline */}
          <motion.h1
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.1 }}
            className="mt-6 text-5xl md:text-6xl lg:text-7xl font-bold leading-tight text-[var(--color-text-primary)]"
          >
            Dein Twitch-Kanal.
            <br />
            <span className="bg-gradient-to-r from-[var(--color-primary)] to-[var(--color-accent)] bg-clip-text text-transparent">
              Intelligenter
            </span>{" "}
            verwaltet.
          </motion.h1>

          {/* Subheadline */}
          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.2 }}
            className="mt-6 text-xl text-[var(--color-text-secondary)] max-w-2xl mx-auto"
          >
            Auto-Raid, Echtzeit-Analytics, Clip Manager und Community Tools —
            Auto-Raid, Echtzeit-Analytics, Clip Manager und Community Tools — alles, was du brauchst, um in der Deadlock-Community zu wachsen.
          </motion.p>

          {/* CTA Buttons */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.3 }}
            className="mt-10 flex gap-4 justify-center flex-wrap"
          >
            <a
              href={TWITCH_BOT_AUTH_START_URL}
              target="_blank"
              rel="noopener noreferrer"
              onClick={(event) => {
                event.preventDefault();
                window.open(
                  buildTwitchBotAuthUrl(),
                  "_blank",
                  "noopener,noreferrer",
                );
              }}
              className="gradient-accent rounded-xl px-7 py-3.5 font-semibold text-white inline-flex items-center gap-2 transition-all duration-200 hover:brightness-110 hover:shadow-[0_0_24px_4px_rgba(255,122,24,0.3)]"
            >
              <ExternalLink size={18} />
              Partner werden
            </a>
          </motion.div>

          <motion.p
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.4 }}
            className="mt-6 text-xl md:text-2xl font-semibold display-font"
          >
            <span className="bg-gradient-to-r from-[var(--color-primary)] to-[var(--color-accent)] bg-clip-text text-transparent">
              Na? Sneak Peek gefällig?
            </span>
          </motion.p>
        </div>

        {/* Browser Mockup */}
        <div className="relative mt-16 mx-auto w-full max-w-[1375px]">
          <motion.div
            initial={{ opacity: 0, y: 40 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8, delay: 0.3 }}
          >
            <BrowserMockup>
              <div className="hero-demo-frame relative aspect-video overflow-hidden rounded bg-gradient-to-br from-[var(--color-card)] to-[var(--color-bg)]">
                <iframe
                  src="https://demo.earlysalty.com/twitch/demo/"
                  title="Twitch Analyse Demo Live View"
                  className="hero-demo-frame__iframe border-0"
                  loading="lazy"
                  referrerPolicy="no-referrer"
                />
                <span className="pointer-events-none absolute left-3 top-3 rounded-full border border-[var(--color-border)] bg-[rgba(7,21,29,0.78)] px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-[var(--color-accent)]">
                  Live View
                </span>
                <a
                  href="https://demo.earlysalty.com/twitch/demo/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="absolute bottom-3 right-3 rounded-md border border-[var(--color-border)] bg-[rgba(7,21,29,0.8)] px-2.5 py-1 text-[11px] font-semibold text-[var(--color-text-primary)] transition hover:border-[var(--color-border-hover)]"
                >
                  Vollansicht
                </a>
              </div>
            </BrowserMockup>
          </motion.div>

          <div className="mt-6 flex w-full flex-col gap-3">
            {HERO_STATS.map((stat, index) => (
              <motion.div
                key={stat.title}
                initial={{ opacity: 0, y: 16 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true, amount: 0.55 }}
                transition={{
                  duration: 0.45,
                  delay: index * 0.08,
                  ease: "easeOut",
                }}
                className={`panel-card w-full rounded-xl px-4 py-3 text-center ${HERO_STAT_STAIR_LAYOUT[index] ?? "md:w-[80%] md:mx-auto"}`}
              >
                <p className="text-base font-semibold text-[var(--color-text-primary)]">
                  {stat.title}
                </p>
              </motion.div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
