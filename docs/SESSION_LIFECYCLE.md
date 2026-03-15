# Session-Lifecycle & Daten-Pipeline

Dokumentiert die zwei getrennten Subsysteme, die Streamer-Daten sammeln,
und wie Sessions als Bindeglied funktionieren.

---

## Ueberblick: Zwei Welten

Das System hat zwei unabhaengige Subsysteme, die Streamer entdecken und verarbeiten:

```
┌─────────────────────────────────────────────────────────────────────┐
│                      WELT 1: Monitoring-Loop                        │
│                  (autoritaere Session-Verwaltung)                    │
│                                                                     │
│  Quelle:  twitch_streamers_partner_state (View)                     │
│  Datei:   bot/monitoring/monitoring.py  →  _process_postings()      │
│  Mixin:   bot/monitoring/sessions_mixin.py                          │
│  Takt:    alle 15s (POLL_INTERVAL_SECONDS)                          │
│                                                                     │
│  Verantwortung:                                                     │
│    - Session erstellen    (_ensure_stream_session)                   │
│    - Session finalisieren (_finalize_stream_session)                 │
│    - Session Samples      (_record_session_sample)                   │
│    - Live-State pflegen   (twitch_live_state)                        │
│    - Stats schreiben      (twitch_stats_tracked)                     │
│    - Go-Live-Posts, Raid-Erkennung, EventSub-Management              │
│                                                                     │
│  Streamer-Pool: Nur verifizierte Partner aus                         │
│                 twitch_streamers_partner_state                       │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                       WELT 2: Scout + ChatBot                       │
│                    (Kategorie-Entdeckung & Chat)                     │
│                                                                     │
│  Scout:   bot/base.py  →  _scout_loop()                             │
│  ChatBot: bot/chat/bot.py + bot/chat/moderation.py                  │
│  Takt:    Scout periodisch, ChatBot event-getrieben                  │
│                                                                     │
│  Scout-Verantwortung:                                                │
│    - Neue Deadlock-DE-Streamer entdecken (Twitch API)                │
│    - In twitch_streamers eintragen (is_monitored_only = 1)           │
│    - ChatBot in Channel joinen lassen                                │
│    - Offline-Channels wieder entfernen                               │
│                                                                     │
│  ChatBot-Verantwortung:                                              │
│    - Chat-Nachrichten empfangen (event_message)                      │
│    - Chat-Health tracken (_track_chat_health)                        │
│    - Chat-Nachrichten persistieren (twitch_chat_messages)             │
│    - Chat-Moderation und Analyse                                     │
│                                                                     │
│  Streamer-Pool: Partner UND monitored-only Channels                  │
│                 (ueber _is_partner_channel_for_chat_tracking Override)│
└─────────────────────────────────────────────────────────────────────┘
```

---

## Session-Erstellung (autoritaer)

**Einzige Stelle**: `_SessionsMixin._ensure_stream_session()` in `sessions_mixin.py`

Wird ausschliesslich von `_process_postings()` in der Monitoring-Loop aufgerufen.
Keine andere Komponente darf Sessions erstellen.

### Ablauf

```
Monitoring-Loop Tick (15s)
  │
  ├─ tracked = twitch_streamers_partner_state (alle Partner)
  ├─ streams_by_login = Twitch API get_streams_by_logins(tracked)
  ├─ category_streams = Twitch API get_streams_by_category(Deadlock)
  │
  └─ _process_postings(tracked, streams_by_login):
       │
       for entry in tracked:           ◄── NUR Partner, nicht Kategorie!
       │
       ├─ is_live AND stream vorhanden?
       │   └─ JA → _ensure_stream_session()
       │          ├─ Session existiert? → _adopt_incomplete_session() → return
       │          ├─ Stream-ID geaendert? → _finalize (restarted) + neue Session
       │          └─ Keine Session? → _start_stream_session() → INSERT twitch_stream_sessions
       │
       ├─ was_live AND NOT is_live?
       │   └─ _finalize_stream_session(reason="offline")
       │      └─ SET ended_at, duration_seconds, Aggregationen
       │
       └─ NOT is_live AND alte active_session_id?
           └─ _finalize_stream_session(reason="stale")
```

### Session-Tabelle

```sql
twitch_stream_sessions
  id              SERIAL PK
  streamer_login  TEXT         -- Twitch-Login
  stream_id       TEXT         -- Twitch Stream-ID
  started_at      TIMESTAMPTZ  -- Session-Start
  ended_at        TIMESTAMPTZ  -- NULL = aktiv/offen
  peak_viewers    INTEGER
  avg_viewers     REAL
  samples         INTEGER      -- Anzahl Viewer-Samples
  duration_seconds INTEGER
  game_name       TEXT
  ...
```

**Offen** = `ended_at IS NULL` → Streamer ist live
**Geschlossen** = `ended_at IS NOT NULL` → Stream beendet

### In-Memory Cache

```
_SessionsMixin._active_sessions: dict[str, int]
  Key:   streamer_login (lowercase)
  Value: session_id

  Rehydriert bei Start: _rehydrate_active_sessions()
  Updated bei:          _start_stream_session(), _finalize_stream_session()
  Gelesen von:          _get_active_session_id()
```

---

## Chat-Health Gate-Kette

Wenn eine Chat-Nachricht ankommt, durchlaeuft sie in `_track_chat_health()`
(moderation.py) eine Gate-Kette:

```
Chat-Nachricht empfangen (event_message)
  │
  ├─ Gate 1: Channel vorhanden?
  │   └─ NEIN → skip_missing_channel (WARNING)
  │
  ├─ Gate 2: Partner-Gate
  │   │  _is_partner_channel_for_chat_tracking(login)
  │   │  [Override in bot.py: erlaubt auch monitored-only]
  │   └─ NEIN → skip_partner_gate (silent, normal)
  │
  ├─ Gate 3: Chatter-Login vorhanden?
  │   └─ NEIN → skip_missing_chatter_login (WARNING)
  │
  ├─ Gate 4: Bekannter Bot?
  │   └─ JA → skip_known_chat_bot (silent, normal)
  │
  ├─ Gate 5: Session vorhanden?                    ◄── HIER ist die Luecke
  │   │  _resolve_session_id(login)
  │   │  → SELECT FROM twitch_stream_sessions WHERE ended_at IS NULL
  │   └─ NEIN → skip_missing_session (WARNING)     ◄── betrifft monitored-only
  │
  ├─ Gate 6: Target-Game live?
  │   │  _is_target_game_live_for_chat(login, session_id)
  │   └─ NEIN → skip_target_game_gate (silent, normal)
  │
  └─ ✓ Alle Gates bestanden → Chat-Nachricht persistieren
```

### Log-Level-Steuerung (moderation.py:84-87)

```python
# Diese Gruende sind "normal" und werden NICHT geloggt:
{"skip_partner_gate", "skip_target_game_gate", "skip_known_chat_bot"}

# Alles andere (inkl. skip_missing_session) → log.warning()
```

---

## Die Luecke: Monitored-Only ohne Session

### Problem

```
Scout entdeckt Streamer "shokztv" (Deadlock, DE, live)
  │
  ├─ twitch_streamers: INSERT (is_monitored_only = 1)
  ├─ ChatBot: join_channels(["shokztv"])
  ├─ ChatBot: _is_partner_channel_for_chat_tracking("shokztv")
  │   └─ _is_monitored_only("shokztv") → True → partner_gate = allowed
  │
  └─ Chat-Nachricht kommt rein:
      ├─ partner_gate = allowed  ✓
      ├─ _resolve_session_id("shokztv")
      │   └─ SELECT FROM twitch_stream_sessions WHERE streamer_login = 'shokztv'
      │      AND ended_at IS NULL
      │   └─ → KEINE ZEILE (Monitoring-Loop erstellt keine Sessions fuer
      │        monitored-only, nur fuer twitch_streamers_partner_state)
      └─ → skip_missing_session (WARNING)  ✗
          → Chat-Nachricht wird VERWORFEN
```

### Betroffene Streamer

Alle Channels, die:
1. Vom Scout als `monitored_only` eingetragen wurden
2. NICHT in `twitch_streamers_partner_state` sind (kein Partner-Status)
3. Aktiv chatten (der Bot empfaengt Nachrichten)

### Konsequenz

- Chat-Daten dieser Channels gehen komplett verloren
- Session-Samples (`twitch_session_viewers`) werden nicht erfasst
  (da `_record_session_sample` ebenfalls `_get_active_session_id` prueft)
- WARNING-Spam im Log (eine Warnung pro Nachricht, rate-limited auf 60s/Channel)
- `twitch_stats_category` wird weiterhin befuellt (unabhaengig von Sessions)

---

## Kategorie-Daten ohne Session (funktioniert trotzdem)

Diese Daten werden unabhaengig von Sessions geschrieben:

| Daten | Tabelle | Quelle | Session noetig? |
|-------|---------|--------|-----------------|
| Viewer-Snapshots (Kategorie) | twitch_stats_category | _log_stats() | Nein |
| Viewer-Snapshots (Partner) | twitch_stats_tracked | _log_stats() | Nein |
| Chat-Nachrichten | twitch_chat_messages | _track_chat_health() | **Ja** |
| Session-Viewer-Timeline | twitch_session_viewers | _record_session_sample() | **Ja** |
| Chatter-Rollup | twitch_chatter_rollup | _track_chat_health() | **Ja** |
| Live-State | twitch_live_state | _process_postings() | Nur Partner |

---

## Session-Cleanup (Sicherheitsnetz)

`_cleanup_orphaned_sessions()` in sessions_mixin.py (alle 100 Ticks ≈ 25 Min):

| Fall | Bedingung | Aktion |
|------|-----------|--------|
| Zero-Sample Orphans | `ended_at IS NULL AND samples = 0 AND open > 24h` | Schliessen mit `duration = 0` |
| Stale Sessions | `ended_at IS NULL AND letzter Viewer-Eintrag > 1h` | Schliessen mit letztem Timestamp |

Kommentar im Code (Zeile 613):
> "category streamers not in the partner view"

→ Das Problem war bereits bekannt, wurde aber nur als Cleanup behandelt statt praeventiv geloest.

---

## Umgesetzter Fix

### Ziel
Die Monitoring-Loop bleibt die **einzige autoritaere Stelle** fuer Session-Verwaltung,
wird aber erweitert um auch monitored-only Streams abzudecken.

### Ansatz: Eingangsmenge erweitern, nicht zweiten Writer einfuehren

Statt einen zweiten Session-Write-Pfad zu bauen, wurde die Eingangsmenge
fuer die Monitoring-Loop erweitert. Der bestehende Code in `_process_postings()`
behandelt monitored-only Channels identisch zu Partnern — nur Partner-spezifische
Flows werden durch `is_verified`/`is_partner`-Guards ausgeschlossen.

### Aenderungen

**1. monitoring.py — `_load_tracked_streamers()` (neuer Helper)**

Laedt Partner UND monitored-only Channels per UNION ALL:
```sql
SELECT ... FROM twitch_streamers_partner_state
UNION ALL
SELECT ..., 0 AS is_partner, ...
  FROM twitch_streamers s
 WHERE s.is_monitored_only = 1
   AND NOT EXISTS (SELECT 1 FROM twitch_streamers_partner_state ps
                    WHERE LOWER(ps.twitch_login) = LOWER(s.twitch_login))
```

Monitored-only Channels bekommen `is_partner = 0` → `is_verified = False`.

**2. monitoring.py — Partner-spezifische Guards**

Drei Stellen in `_process_postings()` feuerten unnoetig fuer monitored-only:

| Stelle | Guard hinzugefuegt | Verhindert |
|--------|-------------------|------------|
| Go-Live Raid-Detection | `and is_verified` | Unnoetige `load_active_partner()` DB-Query |
| Partner-Score stream_restarted | `and is_verified and not is_archived` | Unnoetige Background-Tasks |
| Partner-Score online/offline | `and is_verified and not is_archived` | Unnoetige Background-Tasks |

`is_verified` und `need_link` wurden vor den Go-Live Block verschoben, damit sie
in allen Guards verfuegbar sind.

**3. base.py — Scout finalisiert Sessions vor Loeschen**

Wenn der Scout monitored-only Channels entfernt (offline/Game-Switch), werden
vor `delete_streamer()` zwei Cleanups ausgefuehrt:
- `UPDATE twitch_stream_sessions SET ended_at = ... WHERE ended_at IS NULL` (Session schliessen)
- `DELETE FROM twitch_live_state WHERE streamer_login = ?` (Stale State bereinigen)

Damit wird Session-Orphaning verhindert (vorher bis zu 1h Delay durch `_cleanup_orphaned_sessions`).

### Was monitored-only Channels jetzt bekommen

| Feature | Partner | Monitored-Only |
|---------|---------|----------------|
| Session erstellen/finalisieren | ✓ | ✓ |
| twitch_live_state | ✓ | ✓ |
| twitch_stats_tracked | ✓ | ✓ (is_partner=0) |
| twitch_session_viewers | ✓ | ✓ |
| Chat-Nachrichten persistieren | ✓ | ✓ |
| Go-Live Discord-Post | ✓ | ✗ (is_verified=False) |
| Raid-Detection | ✓ | ✗ (is_verified=False) |
| Partner-Score-Refresh | ✓ | ✗ (is_verified=False) |
| Auto-Archive | ✓ | ✗ (Query filtert is_partner=1) |

### Invariante

> Nur `_ensure_stream_session()` darf Sessions erstellen.
> Nur `_finalize_stream_session()`, `_cleanup_orphaned_sessions()` und der Scout-Cleanup duerfen Sessions schliessen.
> Der ChatBot und alle anderen Komponenten sind reine Konsumenten.
> Partner-spezifische Flows (Postings, Raids, Scores) werden durch `is_verified`-Guards geschuetzt.
