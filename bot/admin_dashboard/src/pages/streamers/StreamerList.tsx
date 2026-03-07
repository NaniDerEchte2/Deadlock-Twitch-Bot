import { useState } from 'react';
import { CheckCircle2, Eye, FolderArchive, Plus, Trash2 } from 'lucide-react';
import { Link } from 'react-router-dom';
import { ConfirmDialog } from '@/components/shared/ConfirmDialog';
import { DataTable, type TableColumn } from '@/components/shared/DataTable';
import { SearchInput } from '@/components/shared/SearchInput';
import { StatusBadge } from '@/components/shared/StatusBadge';
import { Toast } from '@/components/shared/Toast';
import type { StreamerRow } from '@/api/types';
import {
  useAddStreamer,
  useArchiveStreamer,
  useRemoveStreamer,
  useStreamers,
  useVerifyStreamer,
} from '@/hooks/useAdmin';
import { formatNumber, formatRelativeTime } from '@/utils/formatters';

type PendingAction =
  | { type: 'remove'; row: StreamerRow }
  | { type: 'archive'; row: StreamerRow }
  | null;

export function StreamerList() {
  const [search, setSearch] = useState('');
  const [newLogin, setNewLogin] = useState('');
  const [pendingAction, setPendingAction] = useState<PendingAction>(null);
  const [toast, setToast] = useState<{ open: boolean; tone: 'success' | 'error'; message: string }>({
    open: false,
    tone: 'success',
    message: '',
  });

  const streamersQuery = useStreamers();
  const addMutation = useAddStreamer();
  const removeMutation = useRemoveStreamer();
  const verifyMutation = useVerifyStreamer();
  const archiveMutation = useArchiveStreamer();

  const rows = (streamersQuery.data ?? []).filter((row) =>
    [row.login, row.displayName, row.planId, row.status].some((value) =>
      String(value || '')
        .toLowerCase()
        .includes(search.toLowerCase()),
    ),
  );

  const columns: TableColumn<StreamerRow>[] = [
    {
      key: 'login',
      title: 'Streamer',
      sortable: true,
      sortValue: (row) => row.login,
      render: (row) => (
        <div>
          <Link to={`/streamers/${encodeURIComponent(row.login)}`} className="font-semibold text-white hover:text-primary">
            {row.displayName || row.login}
          </Link>
          <p className="text-xs uppercase tracking-[0.16em] text-text-secondary">{row.login}</p>
        </div>
      ),
    },
    {
      key: 'status',
      title: 'Status',
      sortable: true,
      sortValue: (row) => Number(row.isLive) + Number(row.verified),
      render: (row) => (
        <div className="flex flex-wrap gap-2">
          <StatusBadge status={row.isLive ? 'live' : row.archived ? 'archived' : row.verified ? 'verified' : 'offline'} />
          {row.planId ? <StatusBadge status={row.planId} /> : null}
        </div>
      ),
    },
    {
      key: 'viewers',
      title: 'Viewer',
      sortable: true,
      sortValue: (row) => row.viewerCount ?? 0,
      render: (row) => formatNumber(row.viewerCount ?? 0),
    },
    {
      key: 'lastSeen',
      title: 'Last Seen',
      sortable: true,
      sortValue: (row) => row.lastSeenAt || '',
      render: (row) => formatRelativeTime(row.lastSeenAt),
    },
    {
      key: 'actions',
      title: 'Aktionen',
      className: 'min-w-[240px]',
      render: (row) => (
        <div className="flex flex-wrap gap-2">
          <Link to={`/streamers/${encodeURIComponent(row.login)}`} className="admin-button admin-button-secondary !px-3 !py-2">
            <Eye className="h-4 w-4" />
            Detail
          </Link>
          <button
            onClick={async () => {
              try {
                const result = await verifyMutation.mutateAsync({ login: row.login, mode: row.verified ? 'unverified' : 'verified' });
                setToast({ open: true, tone: result.ok ? 'success' : 'error', message: result.message });
              } catch (error) {
                setToast({ open: true, tone: 'error', message: error instanceof Error ? error.message : 'Verifizierung fehlgeschlagen' });
              }
            }}
            className="admin-button admin-button-secondary !px-3 !py-2"
          >
            <CheckCircle2 className="h-4 w-4" />
            {row.verified ? 'Unverify' : 'Verify'}
          </button>
          <button onClick={() => setPendingAction({ type: 'archive', row })} className="admin-button admin-button-secondary !px-3 !py-2">
            <FolderArchive className="h-4 w-4" />
            {row.archived ? 'Restore' : 'Archive'}
          </button>
          <button onClick={() => setPendingAction({ type: 'remove', row })} className="admin-button admin-button-danger !px-3 !py-2">
            <Trash2 className="h-4 w-4" />
            Remove
          </button>
        </div>
      ),
    },
  ];

  return (
    <section className="space-y-5">
      <header className="panel-card rounded-[1.8rem] p-6">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.28em] text-text-secondary">Streamer Verwaltung</p>
            <h1 className="mt-3 text-3xl font-semibold text-white">Monitored Streamer zentral steuern</h1>
            <p className="mt-3 max-w-2xl text-sm leading-7 text-text-secondary">
              Nutzt die bestehenden Admin-Routen für Add/Remove/Verify/Archive, inklusive CSRF-Fallback über die Legacy-Admin-Seite.
            </p>
          </div>
          <div className="grid gap-3 md:grid-cols-[minmax(220px,1fr)_auto] md:items-center">
            <input
              value={newLogin}
              onChange={(event) => setNewLogin(event.target.value)}
              placeholder="Twitch Login hinzufügen"
              className="admin-input"
            />
            <button
              className="admin-button admin-button-primary"
              disabled={!newLogin.trim() || addMutation.isPending}
              onClick={async () => {
                try {
                  const result = await addMutation.mutateAsync(newLogin.trim());
                  setNewLogin('');
                  setToast({ open: true, tone: result.ok ? 'success' : 'error', message: result.message });
                } catch (error) {
                  setToast({ open: true, tone: 'error', message: error instanceof Error ? error.message : 'Streamer konnte nicht hinzugefügt werden' });
                }
              }}
            >
              <Plus className="h-4 w-4" />
              Streamer hinzufügen
            </button>
          </div>
        </div>
      </header>

      <div className="grid gap-4 md:grid-cols-[minmax(260px,420px)_1fr] md:items-center">
        <SearchInput placeholder="Nach Login, Plan oder Status suchen" onDebouncedChange={setSearch} />
        <div className="flex flex-wrap gap-2 text-sm text-text-secondary">
          <span className="stat-pill">{rows.length} Treffer</span>
          <span className="stat-pill">{(streamersQuery.data ?? []).filter((row) => row.isLive).length} live</span>
          <span className="stat-pill">{(streamersQuery.data ?? []).filter((row) => row.archived).length} archiviert</span>
        </div>
      </div>

      <DataTable columns={columns} rows={rows} rowKey={(row) => row.login} />

      <ConfirmDialog
        open={Boolean(pendingAction)}
        title={pendingAction?.type === 'remove' ? 'Streamer entfernen?' : 'Archivstatus ändern?'}
        description={
          pendingAction?.type === 'remove'
            ? `Der Streamer ${pendingAction?.row.login} wird aus dem Monitoring entfernt.`
            : `Für ${pendingAction?.row.login} wird der Archivstatus umgestellt.`
        }
        tone={pendingAction?.type === 'remove' ? 'danger' : 'default'}
        busy={removeMutation.isPending || archiveMutation.isPending}
        onCancel={() => setPendingAction(null)}
        onConfirm={async () => {
          if (!pendingAction) {
            return;
          }
          try {
            const result =
              pendingAction.type === 'remove'
                ? await removeMutation.mutateAsync(pendingAction.row.login)
                : await archiveMutation.mutateAsync({
                    login: pendingAction.row.login,
                    mode: pendingAction.row.archived ? 'unarchive' : 'archive',
                  });
            setToast({ open: true, tone: result.ok ? 'success' : 'error', message: result.message });
          } catch (error) {
            setToast({ open: true, tone: 'error', message: error instanceof Error ? error.message : 'Aktion fehlgeschlagen' });
          } finally {
            setPendingAction(null);
          }
        }}
      />

      <Toast open={toast.open} tone={toast.tone} message={toast.message} onClose={() => setToast((current) => ({ ...current, open: false }))} />
    </section>
  );
}
