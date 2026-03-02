import { SectionHeading } from "@/components/ui/SectionHeading";
import { TerminalMockup } from "@/components/ui/TerminalMockup";
import { ScrollReveal } from "@/components/ui/ScrollReveal";
import { commands } from "@/data/commands";

export function Commands() {
  return (
    <section id="commands" className="py-24">
      <div className="max-w-6xl mx-auto px-6">
        <SectionHeading
          badge="Chat-Befehle"
          title="Einfache Chat-Befehle"
          subtitle="Steuere alles direkt aus dem Twitch-Chat."
        />

        <div className="mt-12 flex justify-center">
          <div className="w-full max-w-2xl">
            <ScrollReveal>
              <TerminalMockup commands={commands} />
            </ScrollReveal>
          </div>
        </div>
      </div>
    </section>
  );
}
