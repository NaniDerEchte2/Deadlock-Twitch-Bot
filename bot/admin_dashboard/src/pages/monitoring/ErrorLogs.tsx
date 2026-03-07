import { useState } from 'react';
import { DataTable, type TableColumn } from '@/components/shared/DataTable';
import type { ErrorLogEntry } from '@/api/types';
import { useErrorLogs } from '@/hooks/useAdmin';
import { formatDateTime } from '@/utils/formatters';

export function ErrorLogs() {
  const [page, setPage] = useState(1);
  const pageSize = 25;
  const logsQuery = useErrorLogs(page, pageSize);
  const rows = logsQuery.data?.entries ?? [];

  const columns: TableColumn<ErrorLogEntry>[] = [
    {
      key: 'timestamp',
      title: 'Zeit',
      sortable: true,
      sortValue: (row) => row.timestamp ?? '',
      render: (row) => formatDateTime(row.timestamp),
    },
    {
      key: 'level',
      title: 'Level',
      render: (row) => row.level || '—',
    },
    {
      key: 'source',
      title: 'Quelle',
      render: (row) => row.source || '—',
    },
    {
      key: 'message',
      title: 'Message',
      className: 'min-w-[420px]',
      render: (row) => (
        <div>
          <p className="font-medium text-white">{row.message}</p>
          {row.context ? <p className="mt-1 text-xs text-text-secondary">{row.context}</p> : null}
        </div>
      ),
    },
  ];

  return (
    <section className="space-y-5">
      <header className="panel-card rounded-[1.8rem] p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.28em] text-text-secondary">Fehler-Logs</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">Paginiertes Error Log Viewer</h1>
      </header>

      <article className="panel-card rounded-[1.8rem] p-6">
        <DataTable columns={columns} rows={rows} rowKey={(row) => row.id} />
        <div className="mt-4 flex items-center justify-between">
          <p className="text-sm text-text-secondary">
            Seite {logsQuery.data?.page ?? page} · {rows.length} Einträge
          </p>
          <div className="flex gap-2">
            <button className="admin-button admin-button-secondary" disabled={page <= 1} onClick={() => setPage((current) => Math.max(1, current - 1))}>
              Zurück
            </button>
            <button
              className="admin-button admin-button-secondary"
              disabled={!logsQuery.data?.hasMore && rows.length < pageSize}
              onClick={() => setPage((current) => current + 1)}
            >
              Weiter
            </button>
          </div>
        </div>
      </article>
    </section>
  );
}
