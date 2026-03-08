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
      "Auto-Raid hält eure Community in Bewegung: Endet ein Deadlock-Stream, wird sie automatisch an einen passenden Live-Partner weitergegeben. Manuelle Raids bleiben jederzeit möglich.",
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
      "Belohne treue Zuschauer automatisch und aktiviere Lurker gezielt im Chat — für eine Community, die wächst.",
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
