import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  addStreamer,
  archiveStreamer,
  fetchAdminStreamerDetail,
  fetchAdminStreamers,
  fetchAffiliates,
  fetchConfigOverview,
  fetchDashboardOverview,
  fetchDatabaseStats,
  fetchErrorLogs,
  fetchEventSubStatus,
  fetchSubscriptions,
  fetchSystemHealth,
  removeStreamer,
  updatePollingConfig,
  updatePromoConfig,
  verifyStreamer,
} from '@/api/client';

export function useDashboardOverview() {
  return useQuery({
    queryKey: ['admin-dashboard-overview'],
    queryFn: fetchDashboardOverview,
    staleTime: 60_000,
  });
}

export function useStreamers() {
  return useQuery({
    queryKey: ['admin-streamers'],
    queryFn: fetchAdminStreamers,
    staleTime: 30_000,
  });
}

export function useStreamerDetail(login: string | undefined) {
  return useQuery({
    queryKey: ['admin-streamer-detail', login],
    queryFn: () => fetchAdminStreamerDetail(login!),
    enabled: Boolean(login),
    staleTime: 30_000,
  });
}

export function useSystemHealth() {
  return useQuery({
    queryKey: ['admin-system-health'],
    queryFn: fetchSystemHealth,
    staleTime: 20_000,
    refetchInterval: 30_000,
  });
}

export function useEventSubStatus() {
  return useQuery({
    queryKey: ['admin-eventsub-status'],
    queryFn: fetchEventSubStatus,
    staleTime: 20_000,
    refetchInterval: 45_000,
  });
}

export function useDatabaseStats() {
  return useQuery({
    queryKey: ['admin-database-stats'],
    queryFn: fetchDatabaseStats,
    staleTime: 120_000,
  });
}

export function useErrorLogs(page: number, pageSize: number) {
  return useQuery({
    queryKey: ['admin-error-logs', page, pageSize],
    queryFn: () => fetchErrorLogs(page, pageSize),
    staleTime: 15_000,
  });
}

export function useConfigOverview() {
  return useQuery({
    queryKey: ['admin-config-overview'],
    queryFn: fetchConfigOverview,
    staleTime: 30_000,
  });
}

export function useSubscriptions() {
  return useQuery({
    queryKey: ['admin-subscriptions'],
    queryFn: fetchSubscriptions,
    staleTime: 120_000,
  });
}

export function useAffiliates() {
  return useQuery({
    queryKey: ['admin-affiliates'],
    queryFn: fetchAffiliates,
    staleTime: 120_000,
  });
}

export function useAddStreamer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: addStreamer,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['admin-streamers'] });
      void queryClient.invalidateQueries({ queryKey: ['admin-dashboard-overview'] });
    },
  });
}

export function useRemoveStreamer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: removeStreamer,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['admin-streamers'] });
    },
  });
}

export function useVerifyStreamer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ login, mode }: { login: string; mode?: 'verified' | 'unverified' }) =>
      verifyStreamer(login, mode),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['admin-streamers'] });
    },
  });
}

export function useArchiveStreamer() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      login,
      mode,
    }: {
      login: string;
      mode?: 'archive' | 'unarchive' | 'toggle';
    }) => archiveStreamer(login, mode),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['admin-streamers'] });
    },
  });
}

export function usePromoConfigMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: updatePromoConfig,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['admin-config-overview'] });
      void queryClient.invalidateQueries({ queryKey: ['admin-dashboard-overview'] });
    },
  });
}

export function usePollingConfigMutation() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: updatePollingConfig,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['admin-config-overview'] });
      void queryClient.invalidateQueries({ queryKey: ['admin-system-health'] });
    },
  });
}
