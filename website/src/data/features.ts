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
      "Intelligentes Raid-System mit fairem Algorithmus – verteilt Raids automatisch an aktive Streamer für gegenseitige Unterstützung.",
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
    title: "Clip Manager (Coming Soon)",
    description:
      "Multi-Plattform Upload für YouTube, TikTok und Instagram – Clips aus dem Chat erstellen und künftig automatisch auf YouTube, TikTok und Instagram verteilen.",
  },
  {
    id: "community",
    icon: "Users",
    title: "Community",
    description:
      "Belohne treue Zuschauer automatisch, und sprich deine Lurker direkt im Chat automatisier an.",
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
