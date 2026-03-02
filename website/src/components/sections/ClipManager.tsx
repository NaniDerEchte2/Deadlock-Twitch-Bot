import { Play, Youtube, Film, Camera, CheckCircle2 } from "lucide-react";
import { ScrollReveal } from "@/components/ui/ScrollReveal";

export function ClipManager() {
  return (
    <section id="clips" className="py-24">
      <div className="max-w-7xl mx-auto px-6">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-16 items-center">

          {/* LEFT: Visual mock UI */}
          <ScrollReveal direction="left">
            <div className="panel-card rounded-2xl p-8">
              <p className="text-lg font-semibold text-[var(--color-text-primary)] mb-6">
                Clip verteilen
              </p>

              {/* Mock clip preview */}
              <div className="aspect-video bg-[var(--color-bg)]/50 rounded-lg mb-6 flex flex-col items-center justify-center gap-3">
                <div className="w-12 h-12 rounded-full gradient-accent flex items-center justify-center">
                  <Play size={22} className="text-white ml-1" />
                </div>
                <span className="text-sm text-[var(--color-text-secondary)]">
                  Krasser 4K Play
                </span>
              </div>

              {/* Platform toggles */}
              <div className="space-y-2">
                {/* YouTube Shorts — on */}
                <div className="flex items-center justify-between bg-[var(--color-bg)]/50 rounded-lg p-3">
                  <div className="flex items-center gap-3">
                    <Youtube size={18} className="text-[var(--color-text-secondary)]" />
                    <span className="text-sm text-[var(--color-text-primary)]">YouTube Shorts</span>
                  </div>
                  <div className="w-10 h-5 bg-[var(--color-success)] rounded-full relative flex items-center px-0.5">
                    <div className="w-4 h-4 bg-white rounded-full ml-auto shadow-sm" />
                  </div>
                </div>

                {/* TikTok — on */}
                <div className="flex items-center justify-between bg-[var(--color-bg)]/50 rounded-lg p-3">
                  <div className="flex items-center gap-3">
                    <Film size={18} className="text-[var(--color-text-secondary)]" />
                    <span className="text-sm text-[var(--color-text-primary)]">TikTok</span>
                  </div>
                  <div className="w-10 h-5 bg-[var(--color-success)] rounded-full relative flex items-center px-0.5">
                    <div className="w-4 h-4 bg-white rounded-full ml-auto shadow-sm" />
                  </div>
                </div>

                {/* Instagram Reels — off */}
                <div className="flex items-center justify-between bg-[var(--color-bg)]/50 rounded-lg p-3">
                  <div className="flex items-center gap-3">
                    <Camera size={18} className="text-[var(--color-text-secondary)]" />
                    <span className="text-sm text-[var(--color-text-primary)]">Instagram Reels</span>
                  </div>
                  <div className="w-10 h-5 bg-[var(--color-border)] rounded-full relative flex items-center px-0.5">
                    <div className="w-4 h-4 bg-white rounded-full shadow-sm" />
                  </div>
                </div>
              </div>

              {/* Publish button */}
              <div className="gradient-accent rounded-lg px-4 py-2 text-sm text-white w-full text-center mt-4 cursor-pointer">
                Clip veröffentlichen
              </div>
            </div>
          </ScrollReveal>

          {/* RIGHT: Text content */}
          <ScrollReveal delay={0.2}>
            <p className="text-sm text-[var(--color-accent)] font-medium uppercase tracking-wider mb-4">
              Clip Manager
            </p>
            <h2 className="text-3xl md:text-4xl font-bold text-[var(--color-text-primary)] mb-6 font-display">
              Clips{" "}
              <span className="bg-gradient-to-r from-[var(--color-primary)] to-[var(--color-accent)] bg-clip-text text-transparent inline">
                automatisch
              </span>{" "}
              überall teilen
            </h2>
            <p className="text-[var(--color-text-secondary)] text-lg mb-8 leading-relaxed">
              Erstelle Clips direkt aus dem Chat und verteile sie automatisch auf allen Plattformen.
            </p>

            {/* Feature list */}
            <ul className="space-y-4">
              {[
                {
                  title: "Multi-Plattform",
                  desc: "YouTube, TikTok und Instagram mit einem Befehl",
                },
                {
                  title: "Smart Templates",
                  desc: "Automatische Titel, Beschreibungen und Hashtags",
                },
                {
                  title: "Auto-Crop",
                  desc: "Intelligentes Zuschneiden für vertikale Formate",
                },
                {
                  title: "Clip-Archiv",
                  desc: "Alle Clips organisiert und durchsuchbar",
                },
              ].map((item) => (
                <li key={item.title} className="flex items-start gap-3">
                  <CheckCircle2
                    size={20}
                    className="text-[var(--color-accent)] shrink-0 mt-0.5"
                  />
                  <div>
                    <span className="text-[var(--color-text-primary)] font-semibold">
                      {item.title}
                    </span>{" "}
                    <span className="text-[var(--color-text-secondary)]">
                      — {item.desc}
                    </span>
                  </div>
                </li>
              ))}
            </ul>
          </ScrollReveal>

        </div>
      </div>
    </section>
  );
}
