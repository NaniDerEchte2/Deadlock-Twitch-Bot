import { DataTable, type TableColumn } from '@/components/shared/DataTable';
import type { AffiliateRecord } from '@/api/types';
import { useAffiliates } from '@/hooks/useAdmin';
import { formatDateTime, formatPercent } from '@/utils/formatters';
import { StatusBadge } from '@/components/shared/StatusBadge';

export function Affiliates() {
  const affiliatesQuery = useAffiliates();
  const rows = affiliatesQuery.data ?? [];

  const columns: TableColumn<AffiliateRecord>[] = [
    {
      key: 'login',
      title: 'Affiliate Login',
      sortable: true,
      sortValue: (row) => row.twitchLogin ?? '',
      render: (row) => <span className="font-medium text-white">{row.twitchLogin || '—'}</span>,
    },
    {
      key: 'status',
      title: 'Status',
      sortable: true,
      sortValue: (row) => row.status ?? '',
      render: (row) => <StatusBadge status={row.status} />,
    },
    {
      key: 'commission',
      title: 'Commission',
      sortable: true,
      sortValue: (row) => row.commissionRate ?? 0,
      render: (row) => formatPercent((row.commissionRate ?? 0) * 100, 0),
    },
    {
      key: 'updated',
      title: 'Updated',
      sortable: true,
      sortValue: (row) => row.updatedAt ?? '',
      render: (row) => formatDateTime(row.updatedAt),
    },
  ];

  return (
    <section className="space-y-5">
      <header className="panel-card rounded-[1.8rem] p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.28em] text-text-secondary">Affiliates</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">Affiliate Accounts und Commission Snapshot</h1>
      </header>
      <article className="panel-card rounded-[1.8rem] p-6">
        <DataTable columns={columns} rows={rows} rowKey={(row, index) => `${row.twitchLogin ?? index}`} />
      </article>
    </section>
  );
}
