import { DataTable, type TableColumn } from '@/components/shared/DataTable';
import { KpiCard } from '@/components/shared/KpiCard';
import type { DatabaseTableStat } from '@/api/types';
import { useDatabaseStats } from '@/hooks/useAdmin';
import { formatBytes, formatNumber } from '@/utils/formatters';

export function DatabaseStats() {
  const databaseQuery = useDatabaseStats();
  const rows = databaseQuery.data?.tables ?? [];

  const columns: TableColumn<DatabaseTableStat>[] = [
    {
      key: 'table',
      title: 'Tabelle',
      sortable: true,
      sortValue: (row) => row.table,
      render: (row) => <span className="font-medium text-white">{row.table}</span>,
    },
    {
      key: 'rows',
      title: 'Rows',
      sortable: true,
      sortValue: (row) => row.rowCount ?? 0,
      render: (row) => formatNumber(row.rowCount ?? 0),
    },
    {
      key: 'size',
      title: 'Größe',
      sortable: true,
      sortValue: (row) => row.sizeBytes ?? 0,
      render: (row) => formatBytes(row.sizeBytes ?? 0),
    },
  ];

  return (
    <section className="space-y-5">
      <header className="panel-card rounded-[1.8rem] p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.28em] text-text-secondary">Database</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">Row Counts und Größen</h1>
      </header>

      <div className="grid gap-4 md:grid-cols-2">
        <KpiCard title="DB Gesamtgröße" value={formatBytes(databaseQuery.data?.databaseSizeBytes)} hint="vom Admin-Endpoint geliefert" tone="primary" />
        <KpiCard title="Tabellen im Snapshot" value={formatNumber(rows.length)} hint="nur gelieferte Tabellen" />
      </div>

      <article className="panel-card rounded-[1.8rem] p-6">
        <DataTable columns={columns} rows={rows} rowKey={(row) => row.table} />
      </article>
    </section>
  );
}
