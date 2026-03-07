import { useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ApiError, buildDiscordAdminLoginUrl, fetchAuthStatus } from '@/api/client';

export function useAuth() {
  return useQuery({
    queryKey: ['admin-auth-status'],
    queryFn: fetchAuthStatus,
    staleTime: 60_000,
    retry: false,
  });
}

export function useRequireAdminAuth() {
  const authQuery = useAuth();

  useEffect(() => {
    if (authQuery.isLoading) {
      return;
    }
    const auth = authQuery.data;
    if (auth?.isAdmin || auth?.isLocalhost) {
      return;
    }
    const target = auth?.discordLoginUrl || auth?.loginUrl || buildDiscordAdminLoginUrl();
    if (window.location.pathname.startsWith('/twitch/admin')) {
      window.location.href = target;
    }
  }, [authQuery.data, authQuery.isLoading]);

  return authQuery;
}

export function toAuthErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return 'Unbekannter Auth-Fehler';
}
