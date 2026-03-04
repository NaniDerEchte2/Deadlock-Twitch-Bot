import { motion } from "framer-motion";
import { ExternalLink } from "lucide-react";
import { BrowserMockup } from "@/components/ui/BrowserMockup";
import {
  TWITCH_BOT_AUTH_START_URL,
  buildTwitchBotAuthUrl,
} from "@/data/externalLinks";

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
            alles in einem Bot fuer die Deadlock-Community.
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
              Bot autorisieren
            </a>
          </motion.div>
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

          {/* Floating badges — hidden on mobile */}
          <motion.div
            className="hidden md:block absolute -left-6 top-1/4 panel-card rounded-full px-4 py-2 text-sm font-medium text-[var(--color-text-primary)]"
            animate={{ y: [0, -10, 0] }}
            transition={{ duration: 3, repeat: Infinity, ease: "easeInOut" }}
          >
            280+ Streamer
          </motion.div>

          <motion.div
            className="hidden md:block absolute -right-4 top-1/3 panel-card rounded-full px-4 py-2 text-sm font-medium text-[var(--color-text-primary)]"
            animate={{ y: [0, -10, 0] }}
            transition={{
              duration: 4,
              repeat: Infinity,
              ease: "easeInOut",
              delay: 0.6,
            }}
          >
            24/7 Online
          </motion.div>

          <motion.div
            className="hidden md:block absolute left-1/4 -bottom-4 panel-card rounded-full px-4 py-2 text-sm font-medium text-[var(--color-text-primary)]"
            animate={{ y: [0, -10, 0] }}
            transition={{
              duration: 3.5,
              repeat: Infinity,
              ease: "easeInOut",
              delay: 1.1,
            }}
          >
            15s Echtzeit
          </motion.div>
        </div>
      </div>
    </section>
  );
}
