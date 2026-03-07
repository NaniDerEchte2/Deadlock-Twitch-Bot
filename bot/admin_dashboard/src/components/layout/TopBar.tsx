import { ChevronRight, LogOut, ShieldCheck, Wifi } from 'lucide-react';
import { Link, useLocation } from 'react-router-dom';
import type { AdminAuthStatus } from '@/api/types';

interface TopBarProps {
  auth?: AdminAuthStatus;
}

function buildBreadcrumbs(pathname: string) {
  const trimmed = pathname.replace(/^\/+|\/+$/g, '');
  if (!trimmed) {
    return [{ label: 'Dashboard', to: '/' }];
  }
  const parts = trimmed.split('/');
  return parts.map((part, index) => ({
    label:
      part === 'streamers'
        ? 'Streamer'
        : part === 'monitoring'
          ? 'Monitoring'
          : part === 'config'
            ? 'Konfiguration'
            : part === 'billing'
              ? 'Billing'
              : decodeURIComponent(part),
    to: `/${parts.slice(0, index + 1).join('/')}`,
  }));
}

export function TopBar({ auth }: TopBarProps) {
  const location = useLocation();
  const breadcrumbs = buildBreadcrumbs(location.pathname);
  const logoutHref = auth?.user?.authType === 'discord_admin' ? '/twitch/auth/discord/logout' : '/twitch/auth/logout';

  return (
    <header className="glass sticky top-4 z-20 rounded-[1.8rem] px-5 py-4">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex flex-wrap items-center gap-2 text-sm text-text-secondary">
          {breadcrumbs.map((crumb, index) => (
            <div key={crumb.to} className="flex items-center gap-2">
              {index > 0 ? <ChevronRight className="h-4 w-4" /> : null}
              <Link to={crumb.to} className={index === breadcrumbs.length - 1 ? 'text-white' : 'hover:text-white'}>
                {crumb.label}
              </Link>
            </div>
          ))}
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <span className="stat-pill">
            {auth?.isLocalhost ? <Wifi className="h-4 w-4" /> : <ShieldCheck className="h-4 w-4" />}
            {auth?.isLocalhost ? 'Localhost Admin' : 'Discord Admin'}
          </span>
          <div className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm">
            <span className="text-text-secondary">Angemeldet als </span>
            <span className="font-semibold text-white">
              {auth?.user?.displayName || auth?.user?.login || 'Admin'}
            </span>
          </div>
          <a className="admin-button admin-button-secondary" href={logoutHref}>
            <LogOut className="h-4 w-4" />
            Logout
          </a>
        </div>
      </div>
    </header>
  );
}
