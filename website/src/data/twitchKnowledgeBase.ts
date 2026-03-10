import {
  DISCORD_INVITE_URL,
  EARLYSALTY_WEBSITE_URL,
  TWITCH_ABBO_URL,
  TWITCH_ADMIN_URL,
  TWITCH_AFFILIATE_URL,
  TWITCH_AGB_URL,
  TWITCH_DASHBOARD_URL,
  TWITCH_DASHBOARD_V2_URL,
  TWITCH_DATENSCHUTZ_URL,
  TWITCH_DEMO_DASHBOARD_URL,
  TWITCH_FAQ_URL,
  TWITCH_IMPRESSUM_URL,
  TWITCH_LIVE_ANNOUNCEMENT_URL,
  TWITCH_ONBOARDING_URL,
  TWITCH_RAID_ANALYTICS_URL,
  TWITCH_RAID_HISTORY_URL,
  TWITCH_SOCIAL_MEDIA_URL,
  buildTwitchDashboardLoginUrl,
} from "@/data/externalLinks";

export interface LinkRef {
  label: string;
  href: string;
}

export interface OnboardingHighlight {
  label: string;
  value: string;
}

export interface OnboardingStep {
  eyebrow: string;
  title: string;
  description: string;
  bullets: string[];
  routeLabel?: string;
  routeHref?: string;
}

export interface CapabilityCard {
  title: string;
  description: string;
  bullets: string[];
}

export interface ChecklistItem {
  title: string;
  description: string;
  href?: string;
  label?: string;
}

export interface FaqItem {
  question: string;
  answer: string;
  details: string[];
  access: string;
  tags: string[];
  routes?: LinkRef[];
}

export interface FaqSection {
  id: string;
  badge: string;
  title: string;
  description: string;
  items: FaqItem[];
}

export const ONBOARDING_HIGHLIGHTS: OnboardingHighlight[] = [
  { label: "Domain", value: "twitch.earlysalty.com" },
  { label: "Zugang", value: "Twitch Login plus Partner-Freigabe" },
  { label: "Scope", value: "Analytics, Raids, Discord, Billing, Clips" },
];

export const ONBOARDING_STEPS: OnboardingStep[] = [
  {
    eyebrow: "1. Einstieg",
    title: "Partner werden und freigeschaltet werden",
    description:
      "Neue Streamer kommen meist über die Website, Discord oder das Netzwerk zum Bot. Bevor das Dashboard sinnvoll nutzbar ist, muss der Channel als Partner im System freigeschaltet sein.",
    bullets: [
      "Die Website erklärt den Produktumfang, dieses Onboarding erklärt den echten Produkt-Flow.",
      "Partner werden im Netzwerk verifiziert, damit Dashboard, Raids und Discord-Automation auf den richtigen Channel laufen.",
      "Nach der Freigabe führen alle streamerbezogenen Links auf dieselbe Twitch-Domain.",
    ],
    routeLabel: "Zur Hauptseite",
    routeHref: EARLYSALTY_WEBSITE_URL,
  },
  {
    eyebrow: "2. Login",
    title: "Mit Twitch einloggen und die Session starten",
    description:
      "Der Standard-Einstieg läuft über Twitch OAuth. Danach landet der Streamer wieder auf einer internen Dashboard-Route und bekommt Zugriff auf die für ihn freigeschalteten Bereiche.",
    bullets: [
      "Login nutzt Twitch OAuth mit Session-Cookie.",
      "Der Login kann auf klassische Dashboard-Flows oder auf Analytics V2 verweisen.",
      "Discord-Auth existiert ebenfalls im System, die Standard-Route für Streamer bleibt aber Twitch.",
    ],
    routeLabel: "Mit Twitch einloggen",
    routeHref: buildTwitchDashboardLoginUrl(),
  },
  {
    eyebrow: "3. Orientierung",
    title: "Dashboard, Analytics und Demo verstehen",
    description:
      "Im Produkt gibt es ein klassisches Dashboard für Kernflüsse und ein modernes Dashboard V2 für tiefe Analytics. Für öffentliche Vorschauen gibt es außerdem ein Demo-Dashboard.",
    bullets: [
      "Dashboard V1 konzentriert sich auf Einstieg, Status und server-renderte Flows.",
      "Dashboard V2 zeigt tiefe Analytics in interaktiven Charts und Auswertungen.",
      "Die Demo ist öffentlich und zeigt das Look-and-feel ohne Partner-Login.",
    ],
    routeLabel: "Demo ansehen",
    routeHref: TWITCH_DEMO_DASHBOARD_URL,
  },
  {
    eyebrow: "4. Discord und Go-Live",
    title: "Announcements, Live-Pings und Community-Flows aktivieren",
    description:
      "Der Bot erkennt Stream-Starts, kann Discord-Announcements bauen und pro Streamer konfigurieren, wann und wie Go-Live-Posts erscheinen.",
    bullets: [
      "Es gibt eine eigene Live-Announcement-Seite mit Config, Preview und Test-Send.",
      "Live-Pings können über Discord-Rollen gespielt werden.",
      "Das System kann streamerindividuelle und globale Announcement-Modi unterscheiden.",
    ],
    routeLabel: "Announcements öffnen",
    routeHref: TWITCH_LIVE_ANNOUNCEMENT_URL,
  },
  {
    eyebrow: "5. Raid-Netzwerk",
    title: "Raid-Bot autorisieren und Netzwerk-Funktionen nutzen",
    description:
      "Sobald der Channel freigeschaltet ist, kann der Streamer den Raid-Bot für seinen Kanal autorisieren. Danach stehen Auth, Anforderungen, Verlauf und Analytics rund um das Raid-Netzwerk bereit.",
    bullets: [
      "OAuth für Raids nutzt einen separaten Flow mit Callback und sicherer Token-Speicherung.",
      "Im Dashboard gibt es Anforderungen, History und Analyse zu eigenen Raids.",
      "Das Netzwerk kann automatisiert Partner-Raids ausführen, statt nur manuell Links zu sammeln.",
    ],
    routeLabel: "Raid Analytics",
    routeHref: TWITCH_RAID_ANALYTICS_URL,
  },
  {
    eyebrow: "6. Ausbau",
    title: "Billing, Clips und spätere Erweiterungen nachziehen",
    description:
      "Nicht jeder Streamer braucht sofort jeden Bereich. Billing, Social Media und weitere Module können nach der Kern-Einrichtung schrittweise dazugeschaltet und verstanden werden.",
    bullets: [
      "Billing deckt Pläne, Checkout, Rechnungen und Stripe-Settings ab.",
      "Social Media sammelt Twitch-Clips und verteilt sie Richtung TikTok, YouTube und Instagram.",
      "Die FAQ unten erklärt auch Features, die nicht jeder Partner am ersten Tag braucht.",
    ],
    routeLabel: "Bot FAQ öffnen",
    routeHref: TWITCH_FAQ_URL,
  },
];

export const ONBOARDING_CAPABILITIES: CapabilityCard[] = [
  {
    title: "Analytics Dashboard",
    description:
      "Von Overview-Metriken bis zu KI-Analysen zieht das Dashboard Sessions, Viewer, Tags, Titel, Retention und Audience-Signale zusammen.",
    bullets: [
      "Dashboard V1 plus Dashboard V2",
      "Overview, Heatmaps, Session-Details, Rankings",
      "Viewer-Profile, Segmente, Demographics, AI",
    ],
  },
  {
    title: "Discord Automation",
    description:
      "Der Bot verbindet Twitch-Live-Status mit Discord-Embeds, Pings und wiederverwendbaren Announcement-Konfigurationen.",
    bullets: [
      "Go-Live-Erkennung",
      "Preview und Test-Send für Announcements",
      "Live-Ping-Rollen und globale Modi",
    ],
  },
  {
    title: "Raid Netzwerk",
    description:
      "Das Produkt ist nicht nur ein Dashboard, sondern auch ein aktives Raid-System mit OAuth, History, Analytics und Netzwerkregeln.",
    bullets: [
      "Raid OAuth für den eigenen Channel",
      "Raid History und Raid Retention",
      "Partner-Auswahl, Enable/Disable und Netzwerklogik",
    ],
  },
  {
    title: "Billing und Pläne",
    description:
      "Pläne, Rechnungsdaten, Checkout, Rechnungen, Kündigungen und Stripe-Settings leben direkt im Streamerbereich.",
    bullets: [
      "Catalog, Checkout Preview, Invoice Preview",
      "Stripe Checkout und Stripe Settings",
      "Promo-Messages und planabhängige Erweiterungen",
    ],
  },
  {
    title: "Social Media und Clips",
    description:
      "Clips können geholt, gefiltert, mit Templates versehen und für Uploads auf mehrere Plattformen vorbereitet werden.",
    bullets: [
      "TikTok, YouTube und Instagram",
      "Clip Queue, Batch Upload und Mark-as-uploaded",
      "OAuth und Platform Status",
    ],
  },
  {
    title: "Community und Zusatztools",
    description:
      "Neben Streamer-Flows gibt es Community-Funktionen, Chat-Commands, Leaderboards und interne Admin-Tools für das Netzwerk.",
    bullets: [
      "!twl für Live-Partner",
      "Viewer Leaderboard",
      "Partner-Recruiting, Moderation und Admin-Steuerung",
    ],
  },
];

export const START_CHECKLIST: ChecklistItem[] = [
  {
    title: "Twitch Login testen",
    description:
      "Starte mit der normalen Login-Route, damit Session, Rollen und Partner-Freigabe korrekt greifen.",
    href: buildTwitchDashboardLoginUrl(),
    label: "Login starten",
  },
  {
    title: "Dashboard aufrufen",
    description:
      "Nach dem Login zuerst das klassische Dashboard prüfen, danach für tiefe Auswertungen auf Dashboard V2 wechseln.",
    href: TWITCH_DASHBOARD_URL,
    label: "Dashboard öffnen",
  },
  {
    title: "Announcements konfigurieren",
    description:
      "Lege fest, wann Discord-Go-Live-Posts erscheinen, wie die Texte aussehen und ob Test-Sends funktionieren.",
    href: TWITCH_LIVE_ANNOUNCEMENT_URL,
    label: "Announcements",
  },
  {
    title: "Raid-Bereich verstehen",
    description:
      "Sobald du freigeschaltet bist, sind History und Analytics die beste Kontrollstelle für das Netzwerk-Verhalten.",
    href: TWITCH_RAID_HISTORY_URL,
    label: "Raid History",
  },
  {
    title: "Billing nur bei Bedarf aktivieren",
    description:
      "Abo, Checkout, Rechnungen und Stripe-Settings sind im Produkt vorbereitet, aber nicht jeder Streamer braucht sie direkt am ersten Tag.",
    href: TWITCH_ABBO_URL,
    label: "Billing ansehen",
  },
  {
    title: "Komplette Funktionsliste nachlesen",
    description:
      "Die Bot-FAQ erklärt auch seltenere und fortgeschrittene Features wie Affiliate, Social Media, AI und Admin-Flows.",
    href: TWITCH_FAQ_URL,
    label: "Zur FAQ",
  },
];

export const FAQ_SECTIONS: FaqSection[] = [];

FAQ_SECTIONS.push(
  {
    id: "zugang",
    badge: "Setup",
    title: "Zugang, Onboarding und Einstiegsflächen",
    description:
      "Hier steht, wie neue Streamer reinkommen, wo sie starten und welche Oberflächen öffentlich oder erst nach Login verfügbar sind.",
    items: [
      {
        question: "Was ist das Streamer-Onboarding auf der Twitch-Domain?",
        answer:
          "Das Onboarding ist die öffentliche Erklärungsseite für neue Streamer. Sie lebt absichtlich auf derselben Domain wie das Partner-Dashboard, damit Website, Login und Produkt-Flow nicht auseinanderfallen.",
        details: [
          "Die Seite ist öffentlich und braucht keinen Login.",
          "Sie erklärt den Partner-Flow, bevor Streamer in geschützte Dashboard-Routen springen.",
          "Sie ersetzt den alten öffentlichen Redirect auf die allgemeine Website-Landingpage.",
        ],
        access: "Öffentlich",
        tags: ["onboarding", "setup", "partner", "website", "faq"],
        routes: [
          { label: "Onboarding", href: TWITCH_ONBOARDING_URL },
          { label: "FAQ", href: TWITCH_FAQ_URL },
        ],
      },
      {
        question: "Wie kommen neue Streamer ins Netzwerk?",
        answer:
          "Neue Streamer kommen typischerweise über Website, Discord oder Partner-Recruiting rein und müssen anschließend im System als Partner freigeschaltet werden.",
        details: [
          "Ohne Freigabe sind viele Streamer-Routen zwar erreichbar, liefern aber keine nutzbare Partner-Funktion.",
          "Partner-Recruiting und Streamer-Management liegen intern beim Admin-Bereich.",
          "Die Freigabe koppelt Channel, Session, Discord und spätere Feature-Rechte zusammen.",
        ],
        access: "Gemischt",
        tags: ["recruiting", "partner", "verification", "admin"],
        routes: [
          { label: "Website", href: EARLYSALTY_WEBSITE_URL },
          { label: "Discord", href: DISCORD_INVITE_URL },
          { label: "Admin-Panel", href: TWITCH_ADMIN_URL },
        ],
      },
      {
        question: "Wie funktioniert der Login für Streamer?",
        answer:
          "Streamer loggen sich standardmäßig über Twitch OAuth ein. Nach erfolgreichem Login landet die Session wieder auf einer internen Dashboard-Route.",
        details: [
          "Der Haupt-Flow verwendet `/twitch/auth/login` und `/twitch/auth/callback`.",
          "Es gibt Session-Handling und Redirects über den `next`-Parameter.",
          "Discord-Login existiert ebenfalls, wird aber vor allem für alternative oder Admin-nahe Flows genutzt.",
        ],
        access: "Öffentlich plus Streamer",
        tags: ["login", "oauth", "twitch", "session", "discord"],
        routes: [
          { label: "Twitch Login", href: buildTwitchDashboardLoginUrl() },
          { label: "Dashboard", href: TWITCH_DASHBOARD_URL },
        ],
      },
      {
        question: "Gibt es eine öffentliche Demo, ohne dass ich Partner bin?",
        answer:
          "Ja. Das Produkt hat ein öffentliches Demo-Dashboard, damit der Look und ein Teil der Analytics vorab sichtbar sind.",
        details: [
          "Die Demo nutzt Demo-Daten statt der eigenen Partnerdaten.",
          "Sie zeigt die Oberfläche von Dashboard V2 ohne geschützten Account-Zugriff.",
          "Sie ist ideal für Preview, Pitch und Erstverständnis.",
        ],
        access: "Öffentlich",
        tags: ["demo", "preview", "dashboard-v2", "public"],
        routes: [{ label: "Demo Dashboard", href: TWITCH_DEMO_DASHBOARD_URL }],
      },
    ],
  },
  {
    id: "dashboards",
    badge: "Analytics",
    title: "Dashboards und Kern-Analytics",
    description:
      "Diese Sektion erklärt, welche Dashboard-Flächen es gibt und welche Metriken sie für Partner sichtbar machen.",
    items: [
      {
        question: "Wofür gibt es Dashboard V1 und Dashboard V2?",
        answer:
          "Dashboard V1 ist die klassische, server-renderte Einstiegsebene. Dashboard V2 ist die moderne Analytics-Oberfläche mit interaktiven Charts und deutlich mehr Detailtiefe.",
        details: [
          "V1 eignet sich für Status, klassische Übersichten und bestehende HTML-Flows.",
          "V2 konzentriert sich auf Analytics und Recharts/Framer-Motion-basierte Visualisierung.",
          "Beide Oberflächen leben auf derselben Twitch-Domain.",
        ],
        access: "Streamer",
        tags: ["dashboard", "dashboard-v1", "dashboard-v2", "analytics"],
        routes: [
          { label: "Dashboard", href: TWITCH_DASHBOARD_URL },
          { label: "Dashboard V2", href: TWITCH_DASHBOARD_V2_URL },
        ],
      },
      {
        question: "Welche Kernmetriken zeigt das Analytics-System?",
        answer:
          "Die Basis umfasst Overview, monatliche und wöchentliche Stats, Viewer-Timelines, Rankings, Streamer-Vergleiche und Session-Details.",
        details: [
          "Dazu kommen stunden- und kalenderbasierte Heatmaps.",
          "Einzelne Sessions können im Detail abgefragt werden.",
          "Kategorien, Spiele und Timing-Muster lassen sich über mehrere Endpunkte vergleichen.",
        ],
        access: "Streamer",
        tags: ["overview", "monthly", "weekly", "heatmap", "session", "ranking"],
        routes: [
          { label: "Dashboard V2", href: TWITCH_DASHBOARD_V2_URL },
          { label: "Demo", href: TWITCH_DEMO_DASHBOARD_URL },
        ],
      },
      {
        question: "Welche tiefen Audience- und Viewer-Funktionen gibt es?",
        answer:
          "Das System schaut nicht nur auf Gesamtviewer, sondern auf Verzeichnisse, Profile, Segmente, Viewer-Overlap, Audience-Sharing und Follower-Funnels.",
        details: [
          "Viewer-Directory und Viewer-Detail helfen bei konkreten Profilfragen.",
          "Audience-Demographics und Audience-Insights machen Zielgruppen veränderbar sichtbar.",
          "Lurker-Analyse und Audience-Sharing helfen bei Community-Qualität und Kollaboration.",
        ],
        access: "Streamer",
        tags: ["viewer", "audience", "directory", "overlap", "segments", "demographics"],
        routes: [{ label: "Dashboard V2", href: TWITCH_DASHBOARD_V2_URL }],
      },
      {
        question: "Gibt es experimentelle oder erweiterte Analytics-Endpunkte?",
        answer:
          "Ja. Das System hat neben den Kernmetriken auch Experimental-Endpunkte und Zusatz-Views für Growth Curves, Game Breakdown und weitere Spezialauswertungen.",
        details: [
          "Retention, Loyalty und Monetization sind eigene Analysebereiche.",
          "Experimental-Endpunkte ergänzen die stabile API, ohne das Haupt-Dashboard zu ersetzen.",
          "Roadmap-Endpunkte existieren ebenfalls und werden für Planung und interne Produktsteuerung genutzt.",
        ],
        access: "Streamer plus Admin",
        tags: ["experimental", "growth", "retention", "loyalty", "roadmap", "monetization"],
        routes: [
          { label: "Dashboard V2", href: TWITCH_DASHBOARD_V2_URL },
          { label: "Admin-Panel", href: TWITCH_ADMIN_URL },
        ],
      },
    ],
  },
);

FAQ_SECTIONS.push(
  {
    id: "affiliate-social",
    badge: "Growth",
    title: "Affiliate, Social Media und Clip-Workflows",
    description:
      "Neben den Kernfunktionen gibt es Wachstums- und Vermarktungsfunktionen für Links, Clips und Plattform-Uploads.",
    items: [
      {
        question: "Was kann das Affiliate-System?",
        answer:
          "Streamer können Affiliate-Links erzeugen, Klicks tracken, Stats ansehen, Settings pflegen und Auswertungen oder Exporte abrufen.",
        details: [
          "Es gibt Links, Stats, Summary, Export und Settings.",
          "Klicks und Redirects werden über eigene Tracking-Routen verarbeitet.",
          "Affiliate ist ein eigenständiger Streamer-Bereich und nicht nur ein einzelner Linkgenerator.",
        ],
        access: "Streamer",
        tags: ["affiliate", "links", "stats", "click tracking", "export"],
        routes: [{ label: "Affiliate", href: TWITCH_AFFILIATE_URL }],
      },
      {
        question: "Wie funktioniert der Social-Media-Clip-Manager?",
        answer:
          "Das System kann Twitch-Clips holen, im Dashboard listen, Uploads in eine Queue legen, Batch-Uploads anstoßen und Upload-Status manuell markieren.",
        details: [
          "Clips können einzeln oder gesammelt verarbeitet werden.",
          "Es gibt Statistik-, Analytics- und Template-Endpunkte.",
          "Der Social-Media-Bereich ist eine eigene Web-Oberfläche außerhalb der `/twitch/*`-Routen.",
        ],
        access: "Streamer",
        tags: ["social media", "clips", "queue", "batch upload", "templates"],
        routes: [{ label: "Social Media", href: TWITCH_SOCIAL_MEDIA_URL }],
      },
      {
        question: "Welche Plattformen werden für Clip-Uploads unterstützt?",
        answer:
          "Im Code sind eigene Uploader für TikTok, YouTube und Instagram vorhanden. Dazu kommen OAuth-Start, Callback, Disconnect und Platform-Status-Flows.",
        details: [
          "OAuth wird pro Plattform verwaltet.",
          "Token-Refresh läuft im Hintergrund.",
          "Video-Verarbeitung und Upload-Worker sind als interne Module getrennt organisiert.",
        ],
        access: "Streamer plus Intern",
        tags: ["tiktok", "youtube", "instagram", "oauth", "upload worker"],
        routes: [{ label: "Social Media", href: TWITCH_SOCIAL_MEDIA_URL }],
      },
      {
        question: "Gibt es Template- und Hashtag-Hilfen für Clips?",
        answer:
          "Ja. Das Social-Media-System kennt globale Templates, streamerindividuelle Templates, Template-Anwendung auf Clips und gespeicherte letzte Hashtags.",
        details: [
          "Das beschleunigt wiederkehrende Video-Beschreibungen und Upload-Texte.",
          "Templates können pro Streamer gepflegt werden.",
          "Der Bereich ist für Content-Produktionsroutinen gedacht, nicht nur für reinen Upload.",
        ],
        access: "Streamer",
        tags: ["templates", "hashtags", "clips", "social"],
        routes: [{ label: "Social Media", href: TWITCH_SOCIAL_MEDIA_URL }],
      },
    ],
  },
  {
    id: "community-admin",
    badge: "Community",
    title: "Community, Commands und Admin-Werkzeuge",
    description:
      "Nicht jede Funktion ist direkt für neue Streamer sichtbar, aber der Bot deckt auch Community- und Betreiber-Werkzeuge ab.",
    items: [
      {
        question: "Welche Community-Funktionen gibt es für Zuschauer und Partner?",
        answer:
          "Das System führt ein Viewer-Leaderboard, kennt Invite-Tracking und bietet Community-Features rund um Live-Partner und Sichtbarkeit.",
        details: [
          "Das Leaderboard lebt im Community-Modul.",
          "Invite- und Partnerbeziehungen werden in der Datenbank verfolgt.",
          "Diese Funktionen stärken das Netzwerk über den einzelnen Stream hinaus.",
        ],
        access: "Gemischt",
        tags: ["community", "leaderboard", "invites", "network"],
        routes: [
          { label: "Discord", href: DISCORD_INVITE_URL },
          { label: "Website", href: EARLYSALTY_WEBSITE_URL },
        ],
      },
      {
        question: "Welche Chat-Commands existieren heute schon?",
        answer:
          "Dokumentiert ist vor allem `!twl` für aktuelle Live-Partner. Zusätzlich gibt es Admin-Commands für Reloads und Raid-Steuerung.",
        details: [
          "`!twl` reagiert in den dafür vorgesehenen Stats-Channels.",
          "`!raid_enable` und `!raid_disable` steuern das Raid-Netzwerk.",
          "`!reload` beziehungsweise die Reload-Logik hilft beim Admin-Betrieb.",
        ],
        access: "Gemischt",
        tags: ["commands", "twl", "reload", "raid_enable", "raid_disable"],
        routes: [{ label: "Discord", href: DISCORD_INVITE_URL }],
      },
      {
        question: "Welche Admin-Funktionen hat das System außerhalb des Streamer-Dashboards?",
        answer:
          "Admins können Streamer hinzufügen, entfernen, verifizieren, archivieren, Discord-Flags setzen, Pläne manuell überschreiben und Announcement-Modi steuern.",
        details: [
          "Das Admin-Panel liegt absichtlich auf einer separaten Domain.",
          "Manuelle Plan-Overrides und Markt-/Roadmap-Funktionen gehören ebenfalls dazu.",
          "Dadurch bleiben Streamer- und Admin-Flächen sauber getrennt.",
        ],
        access: "Admin",
        tags: ["admin", "verify", "archive", "manual plan", "market", "roadmap"],
        routes: [{ label: "Admin-Panel", href: TWITCH_ADMIN_URL }],
      },
      {
        question: "Was passiert intern im Hintergrund, während ich das UI sehe?",
        answer:
          "Monitoring, EventSub, Session-Verwaltung, Analytics-Loops, OAuth-State-Verwaltung, Token-Refresh und Storage-Layer laufen getrennt unter der UI.",
        details: [
          "Der Bot ist nicht nur ein Frontend, sondern ein laufendes Service-System.",
          "Discord-Cog, Dashboard-Service und Datenbank teilen sich dieselbe Produktwelt.",
          "Viele sichtbare Features beruhen auf diesen Hintergrunddiensten.",
        ],
        access: "Intern",
        tags: ["eventsub", "monitoring", "sessions", "storage", "dashboard service"],
      },
    ],
  },
  {
    id: "legal",
    badge: "Support",
    title: "Rechtliches, Support und Hilfslinks",
    description:
      "Zum Produkt gehören auch die öffentlichen Basis-Seiten und Hilfslinks, damit Streamer nicht auf lose Chat-Nachrichten angewiesen sind.",
    items: [
      {
        question: "Wo finde ich Impressum, Datenschutz und AGB?",
        answer:
          "Die rechtlichen Seiten werden direkt über die Twitch-Domain ausgeliefert und sind ohne Login verfügbar.",
        details: [
          "Impressum, Datenschutz und AGB gehören bewusst zum öffentlichen Surface.",
          "So können Billing, OAuth und Produktinformationen sauber verlinkt werden.",
          "Diese Seiten bleiben von Streamer-Logins entkoppelt.",
        ],
        access: "Öffentlich",
        tags: ["impressum", "datenschutz", "agb", "legal"],
        routes: [
          { label: "Impressum", href: TWITCH_IMPRESSUM_URL },
          { label: "Datenschutz", href: TWITCH_DATENSCHUTZ_URL },
          { label: "AGB", href: TWITCH_AGB_URL },
        ],
      },
      {
        question: "Was ist die ausführliche Bot-FAQ?",
        answer:
          "Diese FAQ ist die Sammelstelle für sämtliche dokumentierten Produktfunktionen: von Setup über Analytics bis zu Admin- und Community-Bausteinen.",
        details: [
          "Sie ist absichtlich viel breiter als ein klassisches Support-FAQ.",
          "Sie soll Website, Discord und Produktdoku zusammenfassen.",
          "Wenn neue Module hinzukommen, wird diese Fläche erweitert.",
        ],
        access: "Öffentlich",
        tags: ["faq", "knowledge base", "help", "features"],
        routes: [{ label: "FAQ", href: TWITCH_FAQ_URL }],
      },
      {
        question: "Wo startet man, wenn man möglichst schnell live gehen will?",
        answer:
          "Am schnellsten funktioniert die Reihenfolge Onboarding, Twitch Login, Dashboard, Announcement-Setup und danach erst Raid/Billing/Social Media nach Bedarf.",
        details: [
          "So bleibt der Erststart simpel.",
          "Die produktive Tiefe ist da, muss aber nicht in Minute eins komplett aktiviert werden.",
          "Für alles Weitere gibt es das Onboarding und diese FAQ als Nachschlagewerk.",
        ],
        access: "Öffentlich",
        tags: ["quick start", "support", "onboarding", "login"],
        routes: [
          { label: "Onboarding", href: TWITCH_ONBOARDING_URL },
          { label: "Login", href: buildTwitchDashboardLoginUrl() },
        ],
      },
    ],
  },
);

FAQ_SECTIONS.push(
  {
    id: "raids",
    badge: "Raid Netzwerk",
    title: "Raid-Bot, History und Netzwerklogik",
    description:
      "Das Raid-System ist ein eigener Produktbereich mit OAuth, History, Analysen und Netzwerkregeln für Partner-Raids.",
    items: [
      {
        question: "Wie autorisiere ich den Raid-Bot für meinen Kanal?",
        answer:
          "Der Streamer startet einen speziellen Raid-OAuth-Flow. Danach kann der Bot im Namen des Streamers Raids ausführen, wenn die Netzwerklogik das vorsieht.",
        details: [
          "Es gibt Start-, Kurz-Redirect- und Callback-Routen.",
          "Die Tokens werden verschlüsselt gespeichert.",
          "Ohne gültige Autorisierung bleibt der Raid-Bereich unvollständig.",
        ],
        access: "Streamer",
        tags: ["raid", "oauth", "callback", "authorization"],
        routes: [
          { label: "Raid History", href: TWITCH_RAID_HISTORY_URL },
          { label: "Raid Analytics", href: TWITCH_RAID_ANALYTICS_URL },
        ],
      },
      {
        question: "Was sehe ich im Raid-Dashboard?",
        answer:
          "Der Raid-Bereich bietet Anforderungen, History, Analytics und Callback-bezogene Flows für den eigenen Channel.",
        details: [
          "History zeigt vergangene Raids und deren Ergebnis.",
          "Raid Analytics und Raid Retention messen Wirkung und Zuschauerverhalten.",
          "Requirement-Flows helfen bei der Aktivierung der nötigen Berechtigungen.",
        ],
        access: "Streamer",
        tags: ["raid history", "raid analytics", "retention", "requirements"],
        routes: [
          { label: "Raid History", href: TWITCH_RAID_HISTORY_URL },
          { label: "Raid Analytics", href: TWITCH_RAID_ANALYTICS_URL },
        ],
      },
      {
        question: "Wie entscheidet das Netzwerk, wen es raidet?",
        answer:
          "Unter der Haube kombiniert das System Partnerstatus, Berechtigung, Live-Status, Blacklists, Cooldowns und weitere Kriterien. Ziel ist ein verwaltetes Netzwerk statt zufälliger Linklisten.",
        details: [
          "Raid Enable/Disable ist administrativ steuerbar.",
          "Partner-Auswahl und Ausführung laufen über eigene Raid-Module.",
          "Analytics helfen später zu bewerten, ob das System wirklich Wirkung erzielt.",
        ],
        access: "Intern plus Admin",
        tags: ["selection", "cooldown", "blacklist", "partner state", "auto raid"],
        routes: [{ label: "Admin-Panel", href: TWITCH_ADMIN_URL }],
      },
      {
        question: "Welche Admin-Tools gibt es rund um Raids?",
        answer:
          "Admins können Streamer für das Raid-Netzwerk aktivieren oder deaktivieren und die Netzwerkregeln steuern.",
        details: [
          "Es existieren Discord-Commands wie `!raid_enable` und `!raid_disable`.",
          "Admin-Flows leben getrennt von öffentlichen Streamer-Routen.",
          "So bleibt das Partner-Dashboard schlank, während das Netzwerk zentral verwaltet wird.",
        ],
        access: "Admin",
        tags: ["raid_enable", "raid_disable", "commands", "admin"],
        routes: [{ label: "Admin-Panel", href: TWITCH_ADMIN_URL }],
      },
    ],
  },
  {
    id: "billing",
    badge: "Billing",
    title: "Pläne, Rechnungen und Stripe",
    description:
      "Billing ist ein vollwertiger Streamer-Bereich mit Planverwaltung, Stripe-Checkout und Rechnungsfunktionen.",
    items: [
      {
        question: "Welche Billing-Flächen sind für Streamer vorhanden?",
        answer:
          "Es gibt eine Abo-Übersicht, Checkout-Flows, Rechnungsdaten, Kündigung, Rechnungs-History und Stripe-Settings.",
        details: [
          "Die Billing-Routen leben gesammelt unter `/twitch/abbo`.",
          "Zusatzschreibweisen wie `/twitch/abo` und `/twitch/abos` werden umgeleitet.",
          "Damit können Pläne sauber im Produkt verwaltet werden, ohne externe Admin-Handarbeit.",
        ],
        access: "Streamer",
        tags: ["billing", "plans", "checkout", "invoices", "stripe"],
        routes: [{ label: "Billing", href: TWITCH_ABBO_URL }],
      },
      {
        question: "Wie weit geht die Stripe-Integration?",
        answer:
          "Stripe deckt Catalog, Readiness, Checkout Preview, Checkout Session, Invoice Preview und Webhook-Verarbeitung ab.",
        details: [
          "Checkout und Rechnungen sind in die Dashboard-Flows integriert.",
          "Stripe Settings helfen beim späteren Account- oder Portal-Zugriff.",
          "Readiness und Produktsync existieren zusätzlich für die Admin-Ebene.",
        ],
        access: "Streamer plus Admin",
        tags: ["stripe", "checkout preview", "invoice preview", "webhook", "catalog"],
        routes: [
          { label: "Billing", href: TWITCH_ABBO_URL },
          { label: "Admin-Panel", href: TWITCH_ADMIN_URL },
        ],
      },
      {
        question: "Kann ich Rechnungsdaten, Rechnungen und Kündigungen selbst verwalten?",
        answer:
          "Ja. Rechnungsdaten, Rechnungshistorie, Einzelrechnungen und Kündigung sind als eigene Flows im Streamerbereich angelegt.",
        details: [
          "Der Streamer muss für diese Vorgange nicht in ein externes Backoffice wechseln.",
          "Billing bleibt damit Produktteil und nicht nur ein Payment-Anhang.",
          "Die Rechnungs- und Vorschau-Flows sind bereits im Service vorhanden.",
        ],
        access: "Streamer",
        tags: ["invoice", "billing data", "cancel", "self service"],
        routes: [{ label: "Billing", href: TWITCH_ABBO_URL }],
      },
      {
        question: "Was ist Promo-Mode im Billing-Kontext?",
        answer:
          "Es gibt Promo-Settings und Promo-Messages, die plan- und regelbezogene Kommunikation beeinflussen können.",
        details: [
          "Promo-Settings und Promo-Message sind getrennte Endpunkte.",
          "Die Inhalte werden validiert, bevor sie akzeptiert werden.",
          "Zusätzlich existiert ein globaler Promo-Mode für die Admin-Ebene.",
        ],
        access: "Streamer plus Admin",
        tags: ["promo", "message", "validation", "billing"],
        routes: [
          { label: "Billing", href: TWITCH_ABBO_URL },
          { label: "Admin-Panel", href: TWITCH_ADMIN_URL },
        ],
      },
    ],
  },
);

FAQ_SECTIONS.push(
  {
    id: "chat-ai",
    badge: "Insights",
    title: "Chat, Performance und KI-Auswertung",
    description:
      "Der Bot misst nicht nur Zahlen, sondern auch Chat-Dynamik, Content-Signale und KI-gestützte Empfehlungen.",
    items: [
      {
        question: "Welche Chat-Analysen sind enthalten?",
        answer:
          "Neben einer klassischen Chat-Übersicht gibt es Hype-Timelines, Content-Analysen und Social-Graph-Auswertungen für Zuschauerinteraktion.",
        details: [
          "So lässt sich erkennen, wann Chat wirklich eskaliert oder abfällt.",
          "Inhalte und Beziehungen im Chat werden nicht nur als reine Nachrichtenmenge behandelt.",
          "Diese Tiefe ist besonders relevant für Community-Format-Optimierung.",
        ],
        access: "Streamer",
        tags: ["chat", "hype", "content", "social graph", "engagement"],
        routes: [{ label: "Dashboard V2", href: TWITCH_DASHBOARD_V2_URL }],
      },
      {
        question: "Wie hilft das System bei Tags, Titeln und Watch Time?",
        answer:
          "Das Analytics-System bewertet Tags, Titel-Performance, Watch-Time-Verteilung und weitere Signale, damit Streamer ihren Content datengetriebener planen können.",
        details: [
          "Tag-Analysis und Title-Performance erklären, welche Verpackung besser zieht.",
          "Watch-Time, Retention und Loyalty zeigen, ob Zuschauer nur klicken oder wirklich bleiben.",
          "Die Metriken greifen auf Session- und Audience-Daten zurück.",
        ],
        access: "Streamer",
        tags: ["tags", "titles", "watch time", "retention", "loyalty"],
        routes: [{ label: "Dashboard V2", href: TWITCH_DASHBOARD_V2_URL }],
      },
      {
        question: "Was macht die Coaching- und KI-Schicht?",
        answer:
          "Es gibt einen Coaching-Endpunkt und zusätzliche KI-Endpunkte für Analyse und Verlauf. Damit werden Muster aus den Rohdaten in konkretere Hinweise übersetzt.",
        details: [
          "Coaching ist kein separater Bot, sondern Teil des Analytics-Stacks.",
          "AI Analysis und AI History ergänzen die klassischen Dashboards.",
          "Die Hinweise bauen auf vorhandenen Leistungsdaten auf, statt losgelöst davon zu arbeiten.",
        ],
        access: "Streamer",
        tags: ["coaching", "ai", "analysis", "history", "recommendations"],
        routes: [{ label: "Dashboard V2", href: TWITCH_DASHBOARD_V2_URL }],
      },
      {
        question: "Welche Zuschauer- und Community-Muster erkennt der Bot intern?",
        answer:
          "Unter der Haube laufen Engagement-Metriken, Lurker-Erkennung und weitere Hintergrundjobs, damit die sichtbaren Dashboards nicht nur auf simplen Counterdaten beruhen.",
        details: [
          "Lurker-Tracking, Engagement-Metriken und Chat-Bot-Erkennung sorgen für sauberere Daten.",
          "Monitoring und Analytics-Loops aktualisieren die Datengrundlage laufend.",
          "Diese internen Bausteine sind nicht immer direkt im UI sichtbar, beeinflussen aber die Produktqualitaet.",
        ],
        access: "Intern",
        tags: ["lurker", "engagement", "monitoring", "chat bots", "background jobs"],
      },
    ],
  },
  {
    id: "live-discord",
    badge: "Automation",
    title: "Go-Live, Discord und Announcement-Automation",
    description:
      "Hier geht es um alles, was der Bot zwischen Twitch-Live-Status und Discord-Community automatisch erledigt.",
    items: [
      {
        question: "Wie funktionieren Go-Live-Announcements?",
        answer:
          "Der Bot erkennt Stream-Starts und baut daraus Discord-Embeds. Pro Streamer kann konfiguriert werden, ob, wann und wie diese Posts erscheinen.",
        details: [
          "Monitoring erkennt den Live-Start.",
          "Die Template-Engine setzt Variablen wie Streamer, Titel, Game, Viewer und URL ein.",
          "Optional können Live-Ping-Rollen mitgesendet werden.",
        ],
        access: "Streamer",
        tags: ["go live", "discord", "announcement", "embeds", "template"],
        routes: [{ label: "Announcements", href: TWITCH_LIVE_ANNOUNCEMENT_URL }],
      },
      {
        question: "Welche Tools gibt es für Announcement-Konfiguration?",
        answer:
          "Es gibt eine eigene Config-Oberfläche plus Preview- und Test-Endpunkte. Streamer müssen also nicht blind live schalten, um ihre Texte zu prüfen.",
        details: [
          "Config kann geladen und gespeichert werden.",
          "Preview erzeugt eine Vorschau des Resultats.",
          "Test-Send prüft die effektive Discord-Auslieferung.",
        ],
        access: "Streamer",
        tags: ["config", "preview", "test", "discord", "live"],
        routes: [{ label: "Live Announcement", href: TWITCH_LIVE_ANNOUNCEMENT_URL }],
      },
      {
        question: "Gibt es globale oder adminseitige Announcement-Modi?",
        answer:
          "Ja. Neben streamerindividueller Config existiert ein Admin-Bereich für globale Announcement- und Broadcast-Modi.",
        details: [
          "Der Admin kann netzwerkweite Modi und Ankündigungsregeln steuern.",
          "Diese globale Schicht ergänzt die Streamer-Config statt sie komplett zu ersetzen.",
          "Damit lassen sich Kampagnen oder Community-weite Broadcast-Phasen koordinieren.",
        ],
        access: "Admin",
        tags: ["admin", "announcement mode", "broadcast", "global config"],
        routes: [{ label: "Admin-Panel", href: TWITCH_ADMIN_URL }],
      },
      {
        question: "Welche weiteren Discord-Verbindungen hat das System?",
        answer:
          "Discord ist nicht nur Ziel für Live-Posts. Das System kennt Discord-Linking, Rollen-Sync, Invite-Flows und Community-Kommandos.",
        details: [
          "Discord-Linking ist Teil der Partner- und Rechteverwaltung.",
          "Rollen können mit Streamer-Status synchronisiert werden.",
          "Invite- und Community-Funktionen leben ebenfalls in angrenzenden Modulen.",
        ],
        access: "Gemischt",
        tags: ["discord link", "role sync", "community", "invites"],
        routes: [
          { label: "Discord", href: DISCORD_INVITE_URL },
          { label: "Admin-Panel", href: TWITCH_ADMIN_URL },
        ],
      },
    ],
  },
);


