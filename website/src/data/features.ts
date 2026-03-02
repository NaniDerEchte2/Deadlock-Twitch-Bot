export interface Feature {
  id: string;
  icon: string;
  title: string;
  description: string;
}

export const features: Feature[] = [
  {
    id: "auto-raid",
    icon: "Swords",
    title: "Auto-Raid",
    description:
      "Intelligentes Raid-System mit fairem Algorithmus – verteilt Raids automatisch an aktive Community-Mitglieder und berücksichtigt Aktivität, Größe und gegenseitige Unterstützung.",
  },
  {
    id: "analytics",
    icon: "BarChart2",
    title: "Analytics",
    description:
      "Echtzeit-Dashboard mit 13 Tabs und KI-Coaching – verfolge Viewer-Zahlen, Peak-Stunden, Follower-Wachstum und erhalte personalisierte Optimierungsvorschläge.",
  },
  {
    id: "clip-manager",
    icon: "Clapperboard",
    title: "Clip Manager",
    description:
      "Multi-Plattform Upload für YouTube, TikTok und Instagram – schneide, betexte und veröffentliche Clips direkt aus dem Chat heraus mit einem einzigen Befehl.",
  },
  {
    id: "community",
    icon: "Users",
    title: "Community",
    description:
      "Leaderboard, Discord-Link und Rollen-Management – belohne treue Zuschauer automatisch, verwalte Subscriber-Rollen und verknüpfe deinen Discord nahtlos.",
  },
  {
    id: "monitoring",
    icon: "Activity",
    title: "Monitoring",
    description:
      "24/7 Stream-Überwachung mit 15s Polling – erkennt Ausfälle, verfolgt Latenz und benachrichtigt dich sofort bei ungewöhnlichen Ereignissen oder Verbindungsproblemen.",
  },
  {
    id: "moderation",
    icon: "ShieldCheck",
    title: "Moderation",
    description:
      "Auto-Mod, Chat-Filter und Timeout-Management – halte deinen Chat sauber mit anpassbaren Regeln, Wortfiltern und automatischen Timeouts für Regelverstöße.",
  },
];
