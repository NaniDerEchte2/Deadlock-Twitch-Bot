import { useConfigOverview } from '@/hooks/useAdmin';

export function ChatConfig() {
  const configQuery = useConfigOverview();
  const payload = configQuery.data?.chat ?? configQuery.data?.raw ?? {};

  return (
    <section className="space-y-5">
      <header className="panel-card rounded-[1.8rem] p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.28em] text-text-secondary">Chat Bot</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">Chat-Bot Commands und Moderation</h1>
      </header>
      <article className="panel-card rounded-[1.8rem] p-6">
        <p className="text-sm leading-7 text-text-secondary">
          Der Chat-Bereich zeigt die aktuell gelieferten Konfigurationswerte. Für finale Write-Actions
          sollen später dedizierte Admin-Endpunkte hinzukommen.
        </p>
        <pre className="mt-4 overflow-auto rounded-[1.4rem] border border-white/10 bg-slate-950/55 p-4 text-xs leading-6 text-emerald-100">
          {JSON.stringify(payload, null, 2)}
        </pre>
      </article>
    </section>
  );
}
