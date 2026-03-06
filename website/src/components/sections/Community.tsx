import { Trophy, Link, Award, Bell } from "lucide-react";
import type { ReactNode } from "react";
import { ScrollReveal } from "@/components/ui/ScrollReveal";
import { SectionHeading } from "@/components/ui/SectionHeading";

interface CommunityCardProps {
  icon: ReactNode;
  title: string;
  description: string;
  delay?: number;
}

function CommunityCard({ icon, title, description, delay = 0 }: CommunityCardProps) {
  return (
    <ScrollReveal delay={delay}>
      <div className="panel-card rounded-xl p-6 flex items-start gap-4 h-full">
        <div className="w-10 h-10 rounded-lg gradient-accent flex items-center justify-center shrink-0">
          {icon}
        </div>
        <div>
          <h3 className="text-lg font-semibold text-[var(--color-text-primary)] mb-1">
            {title}
          </h3>
          <p className="text-sm text-[var(--color-text-secondary)]">
            {description}
          </p>
        </div>
      </div>
    </ScrollReveal>
  );
}

export function Community() {
  return (
    <section id="community" className="py-24">
      <div className="max-w-7xl mx-auto px-6">
        <SectionHeading
          badge="Community"
          title="Deine Community, organisiert"
          subtitle="Mach aus neuen Viewern eine aktive Stamm-Community - mit automatischen Belohnungen, Rollen und Live-Signalen."
        />

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-16">
          <CommunityCard
            icon={<Trophy size={20} className="text-white" />}
            title="Leaderboard"
            description="Automatisches Ranking basierend auf Watch-Time, Aktivität und Treue"
            delay={0}
          />
          <CommunityCard
            icon={<Link size={20} className="text-white" />}
            title="Discord-Integration"
            description="Nahtlose Verbindung zwischen Twitch-Chat und Discord-Server"
            delay={0.1}
          />
          <CommunityCard
            icon={<Award size={20} className="text-white" />}
            title="Rollen-System"
            description="Automatische Rollenvergabe basierend auf Abonnement und Aktivität"
            delay={0.2}
          />
          <CommunityCard
            icon={<Bell size={20} className="text-white" />}
            title="Live-Benachrichtigungen"
            description="Automatische Benachrichtigungen in Discord, wenn du live gehst"
            delay={0.3}
          />
        </div>
      </div>
    </section>
  );
}
