import { useEffect, useState } from 'react';
import { Save } from 'lucide-react';
import { Toast } from '@/components/shared/Toast';
import { useConfigOverview, usePollingConfigMutation, usePromoConfigMutation } from '@/hooks/useAdmin';
import { coerceRecord } from '@/utils/formatters';

function readConfigString(record: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === 'string') {
      return value;
    }
  }
  return '';
}

function readConfigNullableString(record: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === 'string') {
      return value;
    }
    if (value === null) {
      return null;
    }
  }
  return undefined;
}

function readConfigBoolean(record: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === 'boolean') {
      return value;
    }
    if (typeof value === 'number') {
      return value !== 0;
    }
    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (normalized) {
        return !['0', 'false', 'off', 'no'].includes(normalized);
      }
    }
  }
  return undefined;
}

function readConfigNumber(record: Record<string, unknown>, ...keys: string[]) {
  for (const key of keys) {
    const value = record[key];
    if (value === undefined || value === null || value === '') {
      continue;
    }
    const candidate = Number(value);
    if (Number.isFinite(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

function buildPromoSavePayload(
  promo: Record<string, unknown>,
  promoConfig: Record<string, unknown>,
  promoEnabled: boolean,
  promoMessage: string,
) {
  const existingMode = readConfigString(promoConfig, 'mode') || readConfigString(promo, 'mode');
  const startsAt = readConfigNullableString(promoConfig, 'starts_at', 'startsAt');
  const endsAt = readConfigNullableString(promoConfig, 'ends_at', 'endsAt');

  return {
    mode: existingMode === 'custom_event' ? existingMode : 'custom_event',
    custom_message: promoMessage,
    starts_at: startsAt ?? null,
    ends_at: endsAt ?? null,
    is_enabled: promoEnabled,
  };
}

function buildPollingSavePayload(rawValue: string) {
  const trimmed = rawValue.trim();
  if (/^-?\d+$/.test(trimmed)) {
    return { interval_seconds: Number(trimmed) };
  }
  return { interval_seconds: trimmed };
}

export function BotConfig() {
  const configQuery = useConfigOverview();
  const promoMutation = usePromoConfigMutation();
  const pollingMutation = usePollingConfigMutation();
  const [promoEnabled, setPromoEnabled] = useState(true);
  const [promoMessage, setPromoMessage] = useState('');
  const [pollingSeconds, setPollingSeconds] = useState('60');
  const [promoDirty, setPromoDirty] = useState(false);
  const [pollingDirty, setPollingDirty] = useState(false);
  const [toast, setToast] = useState<{ open: boolean; tone: 'success' | 'error'; message: string }>({
    open: false,
    tone: 'success',
    message: '',
  });

  const promo = coerceRecord(configQuery.data?.promo);
  const promoConfig = coerceRecord(promo.config);
  const polling = coerceRecord(configQuery.data?.polling);

  useEffect(() => {
    if (!configQuery.data || promoDirty) {
      return;
    }
    setPromoEnabled(
      readConfigBoolean(promoConfig, 'is_enabled', 'enabled') ??
        readConfigBoolean(promo, 'is_enabled', 'enabled', 'is_active') ??
        false,
    );
    setPromoMessage(
      readConfigString(promoConfig, 'custom_message', 'message', 'promo_message') ||
        readConfigString(promo, 'active_message', 'message', 'promo_message'),
    );
  }, [configQuery.data, promoDirty, promo, promoConfig]);

  useEffect(() => {
    if (!configQuery.data || pollingDirty) {
      return;
    }
    const nextPollingSeconds =
      readConfigNumber(polling, 'intervalSeconds', 'interval_seconds', 'interval') ?? 60;
    setPollingSeconds(String(nextPollingSeconds));
  }, [configQuery.data, polling, pollingDirty]);

  return (
    <section className="space-y-5">
      <header className="panel-card rounded-[1.8rem] p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.28em] text-text-secondary">Bot Konfiguration</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">Promo und Polling administrieren</h1>
      </header>

      <div className="grid gap-5 xl:grid-cols-2">
        <article className="panel-card rounded-[1.8rem] p-6">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-text-secondary">Global Promo</p>
          <div className="mt-4 space-y-4">
            <label className="flex items-center justify-between rounded-[1.2rem] border border-white/10 bg-white/[0.03] px-4 py-3">
              <span className="text-white">Promo aktivieren</span>
              <input
                type="checkbox"
                checked={promoEnabled}
                onChange={(event) => {
                  setPromoDirty(true);
                  setPromoEnabled(event.target.checked);
                }}
              />
            </label>
            <textarea
              value={promoMessage}
              onChange={(event) => {
                setPromoDirty(true);
                setPromoMessage(event.target.value);
              }}
              rows={5}
              className="admin-input"
              placeholder={readConfigString(promoConfig, 'custom_message', 'message', 'promo_message') || 'Globalen Promo-Text eingeben'}
            />
            <button
              className="admin-button admin-button-primary"
              disabled={promoMutation.isPending}
              onClick={async () => {
                try {
                  await promoMutation.mutateAsync(
                    buildPromoSavePayload(promo, promoConfig, promoEnabled, promoMessage),
                  );
                  setPromoDirty(false);
                  setToast({ open: true, tone: 'success', message: 'Promo-Konfiguration gespeichert.' });
                } catch (error) {
                  setToast({ open: true, tone: 'error', message: error instanceof Error ? error.message : 'Promo-Speichern fehlgeschlagen' });
                }
              }}
            >
              <Save className="h-4 w-4" />
              Promo speichern
            </button>
          </div>
        </article>

        <article className="panel-card rounded-[1.8rem] p-6">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-text-secondary">Polling</p>
          <div className="mt-4 space-y-4">
            <input
              value={pollingSeconds}
              onChange={(event) => {
                setPollingDirty(true);
                setPollingSeconds(event.target.value);
              }}
              className="admin-input"
              placeholder={String(readConfigNumber(polling, 'intervalSeconds', 'interval_seconds', 'interval') ?? 60)}
            />
            <button
              className="admin-button admin-button-primary"
              disabled={pollingMutation.isPending}
              onClick={async () => {
                try {
                  await pollingMutation.mutateAsync(buildPollingSavePayload(pollingSeconds));
                  setPollingDirty(false);
                  setToast({ open: true, tone: 'success', message: 'Polling-Konfiguration gespeichert.' });
                } catch (error) {
                  setToast({ open: true, tone: 'error', message: error instanceof Error ? error.message : 'Polling-Speichern fehlgeschlagen' });
                }
              }}
            >
              <Save className="h-4 w-4" />
              Polling speichern
            </button>

            <pre className="overflow-auto rounded-[1.4rem] border border-white/10 bg-slate-950/55 p-4 text-xs leading-6 text-emerald-100">
              {JSON.stringify({ promo, polling }, null, 2)}
            </pre>
          </div>
        </article>
      </div>

      <Toast open={toast.open} tone={toast.tone} message={toast.message} onClose={() => setToast((current) => ({ ...current, open: false }))} />
    </section>
  );
}
