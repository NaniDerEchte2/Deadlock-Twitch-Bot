import { createBrowserRouter, Navigate } from 'react-router-dom';
import { AdminShell } from '@/components/layout/AdminShell';
import { Dashboard } from '@/pages/Dashboard';
import { Affiliates } from '@/pages/billing/Affiliates';
import { Gutschriften } from '@/pages/billing/Gutschriften';
import { Subscriptions } from '@/pages/billing/Subscriptions';
import { BotConfig } from '@/pages/config/BotConfig';
import { ChatConfig } from '@/pages/config/ChatConfig';
import { RaidConfig } from '@/pages/config/RaidConfig';
import { DatabaseStats } from '@/pages/monitoring/DatabaseStats';
import { ErrorLogs } from '@/pages/monitoring/ErrorLogs';
import { EventSubStatusPage } from '@/pages/monitoring/EventSubStatus';
import { SystemOverview } from '@/pages/monitoring/SystemOverview';
import { StreamerDetailPage } from '@/pages/streamers/StreamerDetail';
import { StreamerList } from '@/pages/streamers/StreamerList';

const router = createBrowserRouter(
  [
    {
      path: '/',
      element: <AdminShell />,
      children: [
        { index: true, element: <Dashboard /> },
        { path: 'streamers', element: <StreamerList /> },
        { path: 'streamers/:login', element: <StreamerDetailPage /> },
        { path: 'monitoring', element: <SystemOverview /> },
        { path: 'monitoring/eventsub', element: <EventSubStatusPage /> },
        { path: 'monitoring/database', element: <DatabaseStats /> },
        { path: 'monitoring/errors', element: <ErrorLogs /> },
        { path: 'config', element: <BotConfig /> },
        { path: 'config/raids', element: <RaidConfig /> },
        { path: 'config/chat', element: <ChatConfig /> },
        { path: 'billing', element: <Subscriptions /> },
        { path: 'billing/affiliates', element: <Affiliates /> },
        { path: 'billing/gutschriften', element: <Gutschriften /> },
        { path: '*', element: <Navigate to="/" replace /> },
      ],
    },
  ],
  { basename: '/twitch/admin' },
);

export default router;
