import { DISCORD_INVITE_URL } from "@/data/externalLinks";

function scrollToId(id: string) {
  document.getElementById(id)?.scrollIntoView({ behavior: 'smooth' });
}

interface FooterLink {
  label: string;
  href?: string;
  sectionId?: string;
  external?: boolean;
}

const NAV_SECTION_LINKS: FooterLink[] = [
  { label: 'Features', sectionId: 'features' },
  { label: 'Dashboard', sectionId: 'dashboard' },
  { label: 'Community', sectionId: 'community' },
  { label: 'Befehle', sectionId: 'commands' },
];

const MORE_LINKS: FooterLink[] = [
  { label: 'Demo Dashboard', href: 'https://demo.earlysalty.com', external: true },
  { label: 'Discord beitreten', href: DISCORD_INVITE_URL, external: true },
  { label: 'Impressum', href: 'https://twitch.earlysalty.com/twitch/impressum', external: true },
  { label: 'Datenschutz', href: 'https://twitch.earlysalty.com/twitch/datenschutz', external: true },
];

function FooterLinkItem({ link }: { link: FooterLink }) {
  const baseClass =
    'text-sm text-text-secondary hover:text-text-primary transition-colors duration-200';

  if (link.sectionId) {
    return (
      <button
        onClick={() => scrollToId(link.sectionId!)}
        className={`${baseClass} bg-transparent border-none p-0 cursor-pointer text-left`}
      >
        {link.label}
      </button>
    );
  }

  return (
    <a
      href={link.href ?? '#'}
      target={link.external ? '_blank' : undefined}
      rel={link.external ? 'noopener noreferrer' : undefined}
      className={baseClass}
    >
      {link.label}
    </a>
  );
}

export function Footer() {
  return (
    <footer className="w-full border-t border-border" style={{ background: 'rgba(7,21,29,0.5)' }}>
      <div className="max-w-7xl mx-auto px-6 py-12">
        {/* Three-column grid */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {/* Column 1 – Brand */}
          <div className="flex flex-col gap-3">
            <span className="font-display font-bold text-xl bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
              EarlySalty
            </span>
            <p className="text-text-secondary text-sm leading-relaxed">
              Erstellt für die Deadlock-Community
            </p>
          </div>

          {/* Column 2 – Navigation */}
          <div className="flex flex-col gap-3">
            <h4 className="text-sm font-semibold text-text-primary tracking-wide uppercase">
              Navigation
            </h4>
            <nav className="flex flex-col gap-2">
              {NAV_SECTION_LINKS.map((link) => (
                <FooterLinkItem key={link.label} link={link} />
              ))}
            </nav>
          </div>

          {/* Column 3 – More */}
          <div className="flex flex-col gap-3">
            <h4 className="text-sm font-semibold text-text-primary tracking-wide uppercase">
              Mehr
            </h4>
            <nav className="flex flex-col gap-2">
              {MORE_LINKS.map((link) => (
                <FooterLinkItem key={link.label} link={link} />
              ))}
            </nav>
          </div>
        </div>

        {/* Bottom bar */}
        <div className="border-t border-border mt-8 pt-8 flex flex-col sm:flex-row justify-between items-center gap-3">
          <p className="text-text-secondary text-sm">
            &copy; 2026 EarlySalty. Alle Rechte vorbehalten.
          </p>
        </div>
      </div>
    </footer>
  );
}
