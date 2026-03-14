import {
  DISCORD_INVITE_URL,
  TWITCH_AFFILIATE_URL,
  TWITCH_AGB_URL,
  TWITCH_DASHBOARD_V2_URL,
  TWITCH_DATENSCHUTZ_URL,
  TWITCH_DEMO_DASHBOARD_URL,
  TWITCH_FAQ_URL,
  TWITCH_IMPRESSUM_URL,
  TWITCH_ONBOARDING_URL,
  TWITCH_RAID_ANALYTICS_URL,
  TWITCH_RAID_HISTORY_URL,
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
  { label: "Netzwerk", value: "30+ Deadlock-Streamer" },
  { label: "Analytics", value: "13 Dashboard-Tabs, Echtzeit" },
  { label: "Kostenlos starten", value: "Kein Abo nötig zum Loslegen" },
];

export const ONBOARDING_STEPS: OnboardingStep[] = [
  {
    eyebrow: "1. Autorisieren",
    title: "Bot autorisieren und loslegen",
    description:
      "Ein Klick auf \"Mit Twitch einloggen\", Bot autorisieren — damit hast du 90% geschafft. Kein extra Passwort, keine Bewerbung. Sofort startklar.",
    bullets: [
      "Kein Registrierungsformular, kein Freischaltungs-Prozess — du loggst dich ein und bist dabei.",
      "Dein Twitch-Account reicht aus, um alle Features sofort nutzen zu können.",
      "Innerhalb von Sekunden hast du Zugriff auf dein persönliches Dashboard.",
    ],
    routeLabel: "Mit Twitch einloggen",
    routeHref: buildTwitchDashboardLoginUrl(),
  },
  {
    eyebrow: "2. Dashboard",
    title: "Entdecke dein Dashboard",
    description:
      "Dein persönliches Analytics-Cockpit: Viewer-Trends, Heatmaps, Rankings, Session-Details und KI-gestützte Empfehlungen. Eine Demo zum Vorher-Testen ist ebenfalls verfügbar.",
    bullets: [
      "13 Tabs mit Echtzeit-Daten zu deinem Stream — von Viewer-Zahlen bis Zuschauer-Segmenten.",
      "Interaktive Charts und Heatmaps zeigen dir auf einen Blick, wann dein Stream am stärksten performt.",
      "KI-Coaching gibt dir personalisierte Empfehlungen basierend auf deinen echten Daten.",
    ],
    routeLabel: "Demo ansehen",
    routeHref: TWITCH_DEMO_DASHBOARD_URL,
  },
  {
    eyebrow: "3. Auto-Raid",
    title: "Auto-Raid — dein Netzwerk arbeitet für dich",
    description:
      "Kein Schalter, kein Setup: Das Raid-Netzwerk ist automatisch aktiv. Wenn dein Stream endet, leitet der Bot deine Zuschauer an einen passenden Live-Partner im Deadlock-Netzwerk weiter. Deine Community bleibt in Bewegung, und du profitierst genauso von den Raids anderer.",
    bullets: [
      "Vollautomatisch — du musst nichts aktivieren oder konfigurieren.",
      "Der Bot wählt intelligent den passenden Raid-Partner aus dem Netzwerk.",
      "Du bekommst auch Raids von anderen Streamern, wenn diese offline gehen.",
    ],
    routeLabel: "Raid Analytics",
    routeHref: TWITCH_RAID_ANALYTICS_URL,
  },
  {
    eyebrow: "4. Discord",
    title: "Discord beitreten — kostenlose Reichweite mitnehmen",
    description:
      "Nicht Pflicht, aber extrem empfohlen: Im Discord wirst du als Streamer hervorgehoben und bekommst Sichtbarkeit, die du sonst nirgends bekommst.",
    bullets: [
      "Go-Live-Posts: Sobald du live gehst, erscheint automatisch ein Post im Discord — alle sehen, dass du streamst.",
      "Es gibt einen eigenen Streamer-Bereich mit exklusiven Infos und direktem Austausch.",
      "Pro-Tipp: Zock mit der Community! Wer mit anderen Deadlock-Spielern unterwegs ist, baut sich organisch eine treue Zuschauerschaft auf.",
    ],
    routeLabel: "Discord beitreten",
    routeHref: DISCORD_INVITE_URL,
  },
  {
    eyebrow: "5. Entdecken",
    title: "Weitere Features entdecken",
    description:
      "Affiliate-Links, Chat-Commands, Leaderboards und mehr warten darauf, von dir erkundet zu werden. Schau dich im Dashboard um — es gibt immer etwas Neues zu entdecken.",
    bullets: [
      "Erstelle Affiliate-Links und tracke Klicks direkt im Dashboard.",
      "Nutze Chat-Commands wie !twl um deinen Zuschauern Live-Partner zu zeigen.",
      "Das Viewer-Leaderboard zeigt, wer deine treuesten Zuschauer sind.",
    ],
    routeLabel: "Alle Features in der FAQ",
    routeHref: TWITCH_FAQ_URL,
  },
];

export const ONBOARDING_CAPABILITIES: CapabilityCard[] = [
  {
    title: "Echtzeit-Analytics",
    description:
      "Dein Cockpit für alles rund um deinen Stream: Viewer-Trends, Heatmaps, Rankings und Streamer-Vergleich.",
    bullets: [
      "Live-Viewer-Tracking und Session-Details",
      "Stunden- und Kalender-Heatmaps",
      "Streamer-Ranking und direkter Vergleich",
    ],
  },
  {
    title: "Auto-Raid-Netzwerk",
    description:
      "Deine Zuschauer finden immer den nächsten Stream. Automatische Weiterleitung an Live-Partner wenn du offline gehst.",
    bullets: [
      "Vollautomatische Raid-Auswahl",
      "Raid-History und Analytics",
      "Das Netzwerk arbeitet in beide Richtungen",
    ],
  },
  {
    title: "Discord-Community",
    description:
      "Go-Live-Posts zeigen automatisch wenn du streamst, eigener Streamer-Bereich, und eine Community die zusammen spielt und wächst.",
    bullets: [
      "Automatische Go-Live-Benachrichtigungen",
      "Exklusiver Streamer-Bereich",
      "Community-Events und gemeinsames Spielen",
    ],
  },
  {
    title: "KI-Coaching",
    description:
      "Personalisierte Tipps basierend auf deinen Daten: Beste Streaming-Zeiten, Titel-Performance, Zuschauer-Retention.",
    bullets: [
      "Datenbasierte Empfehlungen für dein Wachstum",
      "Tag- und Titel-Analyse",
      "Watch-Time und Retention-Auswertung",
    ],
  },
  {
    title: "Community & Netzwerk",
    description:
      "Viewer-Leaderboard, Affiliate-Links, Chat-Commands und mehr — alles was dein Netzwerk stärkt.",
    bullets: [
      "Viewer-Leaderboard für treue Zuschauer",
      "Affiliate-Links mit Klick-Tracking",
      "Chat-Commands wie !twl für Live-Partner",
    ],
  },
];

export const START_CHECKLIST: ChecklistItem[] = [
  {
    title: "Bot autorisieren",
    description:
      "Mit Twitch einloggen — das war's. Kein Formular, kein Warten. 90% geschafft!",
    href: buildTwitchDashboardLoginUrl(),
    label: "Mit Twitch einloggen",
  },
  {
    title: "Dashboard erkunden",
    description:
      "Deine Zahlen auf einen Blick: Viewer-Trends, Heatmaps, Rankings und Session-Details.",
    href: TWITCH_DASHBOARD_V2_URL,
    label: "Dashboard öffnen",
  },
  {
    title: "Discord beitreten",
    description:
      "Sichtbarkeit, automatische Go-Live-Posts, Streamer-Bereich und eine aktive Community.",
    href: DISCORD_INVITE_URL,
    label: "Discord beitreten",
  },
  {
    title: "Mit der Community zocken",
    description:
      "Spiel mit anderen Deadlock-Spielern — wer zusammen spielt, baut organisch eine treue Zuschauerschaft auf.",
  },
  {
    title: "Fragen? Ab in die FAQ",
    description:
      "Antworten zu Analytics, Raids, Discord, Affiliate und allem rund um EarlySalty.",
    href: TWITCH_FAQ_URL,
    label: "FAQ öffnen",
  },
];

export const FAQ_SECTIONS: FaqSection[] = [];

FAQ_SECTIONS.push(
  {
    id: "einstieg",
    badge: "Einstieg",
    title: "Erste Schritte & Zugang",
    description:
      "Alles was du wissen musst, um loszulegen — von der Anmeldung bis zum ersten Blick ins Dashboard.",
    items: [
      {
        question: "Was ist EarlySalty?",
        answer:
          "EarlySalty ist eine kostenlose Plattform für Deadlock-Streamer auf Twitch. Du bekommst ein Analytics-Dashboard mit Echtzeit-Daten, ein automatisches Raid-Netzwerk, Discord-Automation und KI-gestütztes Coaching — alles an einem Ort.",
        details: [
          "Über 30 Deadlock-Streamer nutzen das Netzwerk bereits.",
          "Das Dashboard hat 13 Tabs mit detaillierten Auswertungen zu deinem Stream.",
          "Du brauchst nur deinen Twitch-Account — keine zusätzliche Registrierung.",
        ],
        access: "Alle",
        tags: ["earlysalty", "plattform", "deadlock", "streamer", "überblick"],
        routes: [
          { label: "Onboarding", href: TWITCH_ONBOARDING_URL },
          { label: "Demo Dashboard", href: TWITCH_DEMO_DASHBOARD_URL },
        ],
      },
      {
        question: "Wie starte ich?",
        answer:
          "Klick auf \"Mit Twitch einloggen\", autorisiere den Bot — fertig. Kein Bewerbungsformular, keine Wartezeit. Du bist sofort dabei und hast Zugriff auf alle Features.",
        details: [
          "Der gesamte Anmeldeprozess dauert weniger als 30 Sekunden.",
          "Du brauchst nur deinen bestehenden Twitch-Account.",
          "Nach dem Login landest du direkt in deinem persönlichen Dashboard.",
        ],
        access: "Alle",
        tags: ["start", "anmeldung", "login", "registrierung", "einloggen"],
        routes: [
          { label: "Mit Twitch einloggen", href: buildTwitchDashboardLoginUrl() },
        ],
      },
      {
        question: "Kostet EarlySalty etwas?",
        answer:
          "Nein, du kannst kostenlos loslegen. Alle Kern-Features wie Analytics, Auto-Raid und Discord-Automation sind ohne Abo nutzbar.",
        details: [
          "Es gibt kein Pflicht-Abo, um die Plattform zu nutzen.",
          "Premium-Features kannst du später im Dashboard entdecken, falls du möchtest.",
          "Du wirst nie ungefragt in ein kostenpflichtiges Abo gesteckt.",
        ],
        access: "Alle",
        tags: ["kosten", "kostenlos", "gratis", "preis", "abo", "bezahlen"],
      },
      {
        question: "Warum sollte ich dem Discord beitreten?",
        answer:
          "Der Discord ist deine beste Quelle für kostenlose Reichweite. Sobald du live gehst, erscheint automatisch ein Go-Live-Post — alle Community-Mitglieder sehen, dass du streamst.",
        details: [
          "Go-Live-Posts werden automatisch gepostet, wenn du live gehst — ohne dass du etwas tun musst.",
          "Es gibt einen eigenen Streamer-Bereich mit exklusiven Infos und direktem Austausch.",
          "Pro-Tipp: Zock mit der Community! Wer mit anderen Deadlock-Spielern unterwegs ist, baut sich organisch eine treue Zuschauerschaft auf.",
        ],
        access: "Alle",
        tags: ["discord", "community", "go-live", "reichweite", "netzwerk"],
        routes: [
          { label: "Discord beitreten", href: DISCORD_INVITE_URL },
        ],
      },
      {
        question: "Gibt es eine Demo?",
        answer:
          "Ja! Es gibt ein öffentliches Demo-Dashboard, in dem du dir alle Features anschauen kannst, bevor du dich anmeldest. Kein Login nötig.",
        details: [
          "Die Demo zeigt echte Dashboard-Funktionen mit Beispieldaten.",
          "Du siehst alle 13 Tabs und kannst die Oberfläche frei erkunden.",
          "Ideal, um dir ein Bild zu machen, bevor du deinen eigenen Account verbindest.",
        ],
        access: "Alle",
        tags: ["demo", "vorschau", "testen", "preview", "ausprobieren"],
        routes: [
          { label: "Demo Dashboard", href: TWITCH_DEMO_DASHBOARD_URL },
        ],
      },
    ],
  },
  {
    id: "analytics",
    badge: "Analytics",
    title: "Dashboard & Analytics",
    description:
      "Alles über dein persönliches Analytics-Dashboard — von Viewer-Trends bis KI-Empfehlungen.",
    items: [
      {
        question: "Was zeigt mir das Dashboard?",
        answer:
          "Dein Dashboard ist dein persönliches Cockpit mit 13 Tabs: Viewer-Trends, Heatmaps, Session-Details, Streamer-Rankings, Zuschauer-Segmente, Chat-Analysen und vieles mehr.",
        details: [
          "Die Overview zeigt dir die wichtigsten Kennzahlen auf einen Blick.",
          "Heatmaps verraten dir, an welchen Tagen und zu welchen Uhrzeiten dein Stream am besten performt.",
          "Session-Details schlüsseln jeden einzelnen Stream nach Viewern, Dauer und Verlauf auf.",
          "Rankings zeigen dir, wo du im Vergleich zu anderen Deadlock-Streamern stehst.",
        ],
        access: "Alle",
        tags: ["dashboard", "analytics", "viewer", "heatmap", "ranking", "session"],
        routes: [
          { label: "Dashboard", href: TWITCH_DASHBOARD_V2_URL },
          { label: "Demo", href: TWITCH_DEMO_DASHBOARD_URL },
        ],
      },
      {
        question: "Wie aktuell sind die Daten?",
        answer:
          "Die Viewer-Daten werden alle 15 Sekunden aktualisiert. Dein Dashboard zeigt dir also nahezu in Echtzeit, was in deinem Stream passiert.",
        details: [
          "Viewer-Zahlen werden im 15-Sekunden-Takt erfasst und gespeichert.",
          "Session-Daten werden automatisch nach Stream-Ende zusammengefasst.",
          "Historische Daten stehen dir dauerhaft zur Verfügung — nichts wird gelöscht.",
        ],
        access: "Alle",
        tags: ["echtzeit", "aktuell", "daten", "tracking", "live"],
        routes: [
          { label: "Dashboard", href: TWITCH_DASHBOARD_V2_URL },
        ],
      },
      {
        question: "Kann ich mich mit anderen Streamern vergleichen?",
        answer:
          "Ja! Das Ranking-System zeigt dir, wo du im Deadlock-Streamer-Netzwerk stehst. Du kannst deine Zahlen direkt mit anderen vergleichen.",
        details: [
          "Vergleiche Viewer-Zahlen, Stream-Dauer und Wachstum mit anderen Deadlock-Streamern.",
          "Das Ranking wird regelmäßig aktualisiert und basiert auf echten Daten.",
          "Du siehst auch Trends — ob du im Vergleich zum Netzwerk wächst oder stagnierst.",
        ],
        access: "Alle",
        tags: ["ranking", "vergleich", "streamer", "wettbewerb", "netzwerk"],
        routes: [
          { label: "Dashboard", href: TWITCH_DASHBOARD_V2_URL },
        ],
      },
      {
        question: "Was ist das KI-Coaching?",
        answer:
          "Das KI-Coaching analysiert deine Stream-Daten und gibt dir personalisierte Empfehlungen: Beste Streaming-Zeiten, welche Titel gut funktionieren, wie du Zuschauer länger hältst.",
        details: [
          "Die KI wertet deine historischen Daten aus und erkennt Muster.",
          "Du bekommst konkrete Tipps, z. B. wann die besten Uhrzeiten für deinen Stream sind.",
          "Titel-Performance zeigt dir, welche Stream-Titel mehr Zuschauer anziehen.",
          "Retention-Analyse verrät, wie lange Zuschauer im Schnitt bleiben.",
        ],
        access: "Alle",
        tags: ["ki", "coaching", "ai", "empfehlung", "tipps", "optimierung"],
        routes: [
          { label: "Dashboard", href: TWITCH_DASHBOARD_V2_URL },
        ],
      },
    ],
  },
);

FAQ_SECTIONS.push(
  {
    id: "raids",
    badge: "Raids",
    title: "Auto-Raid-Netzwerk",
    description:
      "Wie das automatische Raid-System funktioniert und warum es dein wichtigstes Wachstums-Tool ist.",
    items: [
      {
        question: "Was ist der Auto-Raid?",
        answer:
          "Der Auto-Raid ist ein Kern-Feature von EarlySalty: Wenn dein Stream endet, leitet der Bot deine Zuschauer automatisch an einen passenden Live-Partner im Deadlock-Netzwerk weiter. Das passiert ohne dein Zutun — der Raid ist immer aktiv.",
        details: [
          "Du musst den Auto-Raid nicht aktivieren oder konfigurieren — er läuft automatisch.",
          "Der Bot wählt intelligent den besten Raid-Partner basierend auf mehreren Kriterien.",
          "So bleiben deine Zuschauer im Deadlock-Ökosystem und du hilfst gleichzeitig anderen Streamern.",
        ],
        access: "Alle",
        tags: ["auto-raid", "raid", "automatisch", "netzwerk", "weiterleitung"],
        routes: [
          { label: "Raid Analytics", href: TWITCH_RAID_ANALYTICS_URL },
          { label: "Raid History", href: TWITCH_RAID_HISTORY_URL },
        ],
      },
      {
        question: "Wie funktioniert die Raid-Auswahl?",
        answer:
          "Der Bot berücksichtigt mehrere Faktoren: Wer ist gerade live, wie viele Viewer hat der Partner, wann wurde zuletzt dorthin geraidet, und weitere Netzwerk-Kriterien. Ziel ist immer eine sinnvolle Weiterleitung, kein Zufall.",
        details: [
          "Live-Status der Partner wird in Echtzeit geprüft.",
          "Cooldowns verhindern, dass immer derselbe Streamer geraidet wird.",
          "Das System sorgt für faire Verteilung im gesamten Netzwerk.",
        ],
        access: "Alle",
        tags: ["raid-auswahl", "algorithmus", "partner", "kriterien"],
        routes: [
          { label: "Raid Analytics", href: TWITCH_RAID_ANALYTICS_URL },
        ],
      },
      {
        question: "Kann ich sehen, wen ich geraidet habe?",
        answer:
          "Ja! In der Raid History siehst du alle deine vergangenen Raids mit Details. Die Raid Analytics zeigen dir zusätzlich, wie effektiv deine Raids waren.",
        details: [
          "Die Raid History listet jeden einzelnen Raid mit Datum, Partner und Viewer-Anzahl.",
          "Raid Analytics zeigen dir Trends und Muster über Zeit.",
          "Du siehst auch, wie viele Viewer nach dem Raid beim Partner geblieben sind.",
        ],
        access: "Alle",
        tags: ["raid history", "raid analytics", "verlauf", "statistik"],
        routes: [
          { label: "Raid History", href: TWITCH_RAID_HISTORY_URL },
          { label: "Raid Analytics", href: TWITCH_RAID_ANALYTICS_URL },
        ],
      },
      {
        question: "Profitiere ich auch von Raids anderer?",
        answer:
          "Ja, das Netzwerk arbeitet in beide Richtungen! Wenn andere Streamer offline gehen, können deren Zuschauer automatisch zu dir weitergeleitet werden, solange du live bist.",
        details: [
          "Je aktiver du im Netzwerk bist, desto mehr profitierst du von eingehenden Raids.",
          "Eingehende Raids siehst du ebenfalls in deiner Raid History.",
          "Das System sorgt dafür, dass alle Partner fair berücksichtigt werden.",
        ],
        access: "Alle",
        tags: ["eingehende raids", "netzwerk", "gegenseitig", "wachstum"],
        routes: [
          { label: "Raid History", href: TWITCH_RAID_HISTORY_URL },
        ],
      },
    ],
  },
  {
    id: "community",
    badge: "Community",
    title: "Community, Discord & Netzwerk",
    description:
      "Discord-Automation, Chat-Commands, Affiliate-Links und alles was die Community zusammenhält.",
    items: [
      {
        question: "Wie funktionieren die Go-Live-Posts?",
        answer:
          "Sobald du auf Twitch live gehst, postet der Bot automatisch eine Benachrichtigung im Discord. Alle Community-Mitglieder sehen sofort, dass du streamst — ohne dass du selbst etwas tun musst.",
        details: [
          "Die Posts enthalten deinen Stream-Titel, das Spiel und einen direkten Link zu deinem Stream.",
          "Du musst nichts konfigurieren — die Posts erscheinen automatisch.",
          "Das gibt dir kostenlose Reichweite bei jedem einzelnen Stream.",
        ],
        access: "Alle",
        tags: ["go-live", "discord", "benachrichtigung", "automatisch", "post"],
        routes: [
          { label: "Discord beitreten", href: DISCORD_INVITE_URL },
        ],
      },
      {
        question: "Welche Chat-Commands gibt es?",
        answer:
          "Der wichtigste Command ist !twl — er zeigt deinen Zuschauern, welche anderen Deadlock-Streamer gerade live sind. So können Viewer zwischen Streams wechseln und das Netzwerk wächst.",
        details: [
          "!twl listet alle aktuell live streamenden Partner im Deadlock-Netzwerk.",
          "Der Command funktioniert in jedem Twitch-Chat, in dem der Bot aktiv ist.",
          "Weitere Commands werden laufend ergänzt.",
        ],
        access: "Alle",
        tags: ["commands", "twl", "chat", "twitch", "bot"],
      },
      {
        question: "Was ist das Affiliate-System?",
        answer:
          "Du kannst im Dashboard eigene Affiliate-Links erstellen und deren Klicks in Echtzeit tracken. Ideal, um Produkte oder Services zu bewerben und den Erfolg direkt zu messen.",
        details: [
          "Erstelle Links direkt im Dashboard — kein externes Tool nötig.",
          "Klick-Statistiken zeigen dir, welche Links am besten performen.",
          "Du kannst Links jederzeit bearbeiten, pausieren oder löschen.",
        ],
        access: "Alle",
        tags: ["affiliate", "links", "klicks", "tracking", "werbung"],
        routes: [
          { label: "Affiliate", href: TWITCH_AFFILIATE_URL },
        ],
      },
      {
        question: "Was ist das Viewer-Leaderboard?",
        answer:
          "Das Leaderboard zeigt dir, wer deine treuesten Zuschauer sind. Es trackt, wie oft und wie lange Viewer in deinem Stream sind, und erstellt daraus ein Ranking.",
        details: [
          "Erkenne deine loyalsten Community-Mitglieder auf einen Blick.",
          "Das Leaderboard basiert auf echten Viewing-Daten, nicht auf Chat-Aktivität allein.",
          "Ein tolles Tool, um deine Community besser kennenzulernen und wertzuschätzen.",
        ],
        access: "Alle",
        tags: ["leaderboard", "viewer", "community", "ranking", "treue"],
        routes: [
          { label: "Dashboard", href: TWITCH_DASHBOARD_V2_URL },
        ],
      },
    ],
  },
);

FAQ_SECTIONS.push({
  id: "affiliate",
  badge: "Affiliate",
  title: "Vertriebler-Programm",
  description:
    "Alles über das Affiliate-Programm: Anmeldung, Stripe, Provisionen und wie du Streamer wirbst.",
  items: [
    {
      question: "Was ist das Vertriebler-Programm?",
      answer:
        "Du wirbst Deadlock-Streamer für EarlySalty und bekommst dauerhaft automatisch 30% Provision auf jede Zahlung deiner geworbenen Streamer.",
      details: [
        "Die Provision läuft dauerhaft weiter, solange dein geworbener Streamer zahlt.",
        "Die Zuordnung erfolgt über das Affiliate-Portal und ist eindeutig pro Streamer.",
        "Das Modell ist auf laufende Provisionen statt einmalige Boni ausgelegt.",
      ],
      access: "Alle",
      tags: ["affiliate", "vertriebler", "provision", "programm", "streamer"],
      routes: [
        { label: "Affiliate", href: TWITCH_AFFILIATE_URL },
      ],
    },
    {
      question: "Wie melde ich mich an?",
      answer:
        "Du klickst auf \"Mit Twitch einloggen\" und bist damit startklar. Kein Formular, kein Papierkram, in etwa 2 Minuten erledigt.",
      details: [
        "Die Anmeldung läuft direkt über deinen Twitch-Account.",
        "Es werden keine separaten Registrierungsformulare verlangt.",
        "Nach dem Login kannst du direkt ins Affiliate-Portal wechseln.",
      ],
      access: "Alle",
      tags: ["anmeldung", "login", "twitch", "affiliate", "start"],
      routes: [
        { label: "Affiliate", href: TWITCH_AFFILIATE_URL },
      ],
    },
    {
      question: "Wie funktioniert Stripe Connect?",
      answer:
        "Du verbindest dein Stripe-Konto einmalig. Das dauert ungefähr 5 Minuten. Danach werden deine Provisionen bei jeder Streamer-Zahlung automatisch ausgezahlt.",
      details: [
        "Stripe Connect ist die Auszahlungsschiene für deine Provisionen.",
        "Nach der Einrichtung landen Auszahlungen automatisch auf deinem Bankkonto.",
        "Die Verknüpfung ist nur einmalig notwendig.",
      ],
      access: "Alle",
      tags: ["stripe", "stripe connect", "auszahlung", "bankkonto", "affiliate"],
      routes: [
        { label: "Affiliate", href: TWITCH_AFFILIATE_URL },
      ],
    },
    {
      question: "Wie wirbt man Streamer?",
      answer:
        "Du findest Deadlock-Streamer auf Twitch, in Discord-Servern oder in Communities, empfiehlst EarlySalty und beanspruchst den Streamer danach im Portal. Sobald er zahlt, bekommst du 30%.",
      details: [
        "Geeignet sind Streamer, die noch nicht bei EarlySalty registriert sind.",
        "Die Zuordnung erfolgt über den Twitch-Login des Streamers im Portal.",
        "Jeder beanspruchte Streamer kann nur einem Vertriebler zugeordnet sein.",
      ],
      access: "Alle",
      tags: ["streamer", "werben", "claim", "portal", "provision"],
      routes: [
        { label: "Affiliate", href: TWITCH_AFFILIATE_URL },
        { label: "Discord beitreten", href: DISCORD_INVITE_URL },
      ],
    },
    {
      question: "Was passiert ohne Stripe-Konto?",
      answer:
        "Provisionen werden bis 50€ gespeichert. Darüber hinaus verfallen sie. Deshalb lohnt es sich, Stripe möglichst früh zu verbinden.",
      details: [
        "Ohne Stripe gehen kleine Provisionen nicht sofort verloren.",
        "Die Speichergrenze liegt bei insgesamt 50€ offenen Provisionen.",
        "Sobald Stripe verbunden ist, lohnt sich die automatische Auszahlung sofort.",
      ],
      access: "Alle",
      tags: ["ohne stripe", "50 euro", "limit", "pending", "affiliate"],
      routes: [
        { label: "Affiliate", href: TWITCH_AFFILIATE_URL },
      ],
    },
  ],
});

FAQ_SECTIONS.push({
  id: "support",
  badge: "Support",
  title: "Hilfe, Konto & Rechtliches",
  description:
    "Antworten zu Support, Account-Verwaltung und rechtlichen Informationen.",
  items: [
    {
      question: "Wo bekomme ich Hilfe?",
      answer:
        "Der schnellste Weg ist der Discord — dort gibt es einen Support-Bereich, in dem dir direkt geholfen wird. Alternativ findest du Antworten in dieser FAQ oder im Onboarding.",
      details: [
        "Im Discord antworten erfahrene Community-Mitglieder und das Team.",
        "Die FAQ deckt die häufigsten Fragen ab — nutze die Suche oben.",
        "Das Onboarding erklärt den Einstieg Schritt für Schritt.",
      ],
      access: "Alle",
      tags: ["hilfe", "support", "fragen", "kontakt", "discord"],
      routes: [
        { label: "Discord beitreten", href: DISCORD_INVITE_URL },
        { label: "Onboarding", href: TWITCH_ONBOARDING_URL },
      ],
    },
    {
      question: "Wie lösche ich meinen Account oder widerrufe den Zugang?",
      answer:
        "Du kannst den Bot-Zugang jederzeit über deine Twitch-Einstellungen widerrufen. Geh dazu in deine Twitch-Verbindungen und entferne die EarlySalty-Autorisierung. Für eine komplette Datenlöschung kontaktiere uns im Discord.",
      details: [
        "Twitch-Autorisierung widerrufen: Twitch → Einstellungen → Verbindungen → EarlySalty entfernen.",
        "Nach dem Widerruf hat der Bot keinen Zugriff mehr auf deinen Account.",
        "Für eine vollständige Löschung deiner gespeicherten Daten melde dich im Discord.",
      ],
      access: "Alle",
      tags: ["löschen", "account", "widerrufen", "kündigung", "datenlöschung", "abmelden"],
      routes: [
        { label: "Discord (Support)", href: DISCORD_INVITE_URL },
      ],
    },
    {
      question: "Wo finde ich Impressum, Datenschutz und AGB?",
      answer:
        "Impressum, Datenschutzerklärung und AGB sind öffentlich auf der Website verfügbar — kein Login nötig.",
      details: [
        "Alle rechtlichen Dokumente sind jederzeit ohne Anmeldung einsehbar.",
        "Die Links findest du auch im Footer jeder Seite.",
        "Bei Fragen zum Datenschutz kannst du dich im Discord oder per E-Mail melden.",
      ],
      access: "Alle",
      tags: ["impressum", "datenschutz", "agb", "rechtliches", "legal", "dsgvo"],
      routes: [
        { label: "Impressum", href: TWITCH_IMPRESSUM_URL },
        { label: "Datenschutz", href: TWITCH_DATENSCHUTZ_URL },
        { label: "AGB", href: TWITCH_AGB_URL },
      ],
    },
  ],
});
