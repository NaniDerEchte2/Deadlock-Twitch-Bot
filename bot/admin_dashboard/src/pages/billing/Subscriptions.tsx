import { DataTable, type TableColumn } from '@/components/shared/DataTable';
import type { SubscriptionRecord } from '@/api/types';
import { useSubscriptions } from '@/hooks/useAdmin';
import { formatDateTime } from '@/utils/formatters';
import { StatusBadge } from '@/components/shared/StatusBadge';

export function Subscriptions() {
  const subscriptionsQuery = useSubscriptions();
  const rows = subscriptionsQuery.data ?? [];

  const columns: TableColumn<SubscriptionRecord>[] = [
    {
      key: 'login',
      title: 'Login',
      sortable: true,
      sortValue: (row) => row.login ?? row.customerReference ?? '',
      render: (row) => <span className="font-medium text-white">{row.login || row.customerReference || '—'}</span>,
    },
    {
      key: 'plan',
      title: 'Plan',
      sortable: true,
      sortValue: (row) => row.planId ?? '',
      render: (row) => row.planId || '—',
    },
    {
      key: 'status',
      title: 'Status',
      sortable: true,
      sortValue: (row) => row.status ?? '',
      render: (row) => <StatusBadge status={row.status} />,
    },
    {
      key: 'periodEnd',
      title: 'Period End',
      sortable: true,
      sortValue: (row) => row.currentPeriodEnd ?? row.trialEndsAt ?? '',
      render: (row) => formatDateTime(row.currentPeriodEnd || row.trialEndsAt),
    },
  ];

  return (
    <section className="space-y-5">
      <header className="panel-card rounded-[1.8rem] p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.28em] text-text-secondary">Billing</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">Subscription Übersicht</h1>
      </header>
      <article className="panel-card rounded-[1.8rem] p-6">
        <DataTable columns={columns} rows={rows} rowKey={(row, index) => `${row.customerReference ?? row.login ?? index}`} />
      </article>
    </section>
  );
}
