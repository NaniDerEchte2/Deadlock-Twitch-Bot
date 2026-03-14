export interface AffiliateFeature {
  icon: string;
  title: string;
  description: string;
}

export const affiliateFeatures: AffiliateFeature[] = [
  {
    icon: "Percent",
    title: "30% Provision",
    description:
      "Auf jede Zahlung deiner geworbenen Streamer — dauerhaft, ohne Obergrenze. Solange der Streamer zahlt, verdienst du mit.",
  },
  {
    icon: "UserPlus",
    title: "Streamer beanspruchen",
    description:
      "Finde Deadlock-Streamer und beanspruche sie im Portal, bevor jemand anders es tut. First come, first served.",
  },
  {
    icon: "CreditCard",
    title: "Stripe Connect",
    description:
      "Verbinde dein Stripe-Konto einmalig und bekomme jede Provision automatisch auf dein Bankkonto — kein manuelles Anfordern.",
  },
  {
    icon: "Infinity",
    title: "Kein Ablaufdatum",
    description:
      "Einmal geworben, dauerhaft provisionsberechtigt. Keine zeitliche Begrenzung, kein Verfall.",
  },
  {
    icon: "LayoutDashboard",
    title: "Echtzeit-Dashboard",
    description:
      "Sieh in Echtzeit was du verdienst: Beanspruchte Streamer, Provisionen, Auszahlungen und History — alles im Portal.",
  },
  {
    icon: "LogIn",
    title: "Einfache Registrierung",
    description:
      "Melde dich mit deinem Twitch-Account an, verbinde Stripe — in unter 5 Minuten startklar. Keine Adressdaten nötig.",
  },
];
