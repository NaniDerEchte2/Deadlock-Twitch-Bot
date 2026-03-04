import { ExternalLink } from "lucide-react";
import { ScrollReveal } from "@/components/ui/ScrollReveal";
import {
  TWITCH_BOT_AUTH_START_URL,
  buildTwitchBotAuthUrl,
} from "@/data/externalLinks";

export function CTA() {
  return (
    <section id="cta" className="py-24 relative overflow-hidden">
      {/* Background gradient */}
      <div className="absolute inset-0 bg-gradient-to-br from-[var(--color-primary)]/5 via-transparent to-[var(--color-accent)]/5" />

      <div className="max-w-3xl mx-auto px-6 text-center relative z-10">
        <ScrollReveal>
          <h2 className="text-4xl md:text-5xl font-bold text-[var(--color-text-primary)] mb-6 font-display">
            Bereit für{" "}
            <span className="bg-gradient-to-r from-[var(--color-primary)] to-[var(--color-accent)] bg-clip-text text-transparent inline">
              intelligenteres
            </span>{" "}
            Streaming?
          </h2>

          <p className="text-xl text-[var(--color-text-secondary)] mb-10">
            Werde Partner und nutze alle Features kostenlos.
          </p>

          <div className="flex gap-4 justify-center flex-wrap">
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
              className="gradient-accent rounded-xl px-8 py-4 font-semibold text-white text-lg inline-flex items-center gap-2"
            >
              <ExternalLink size={20} />
              Partner werden
            </a>
          </div>
        </ScrollReveal>
      </div>
    </section>
  );
}
