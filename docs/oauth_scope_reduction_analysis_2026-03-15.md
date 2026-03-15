# OAuth Scope Reduction Analysis

## Stand
- Datum: 15.03.2026
- Ziel: Streamer-OAuth kleiner und zugänglicher machen, ohne den Bot fachlich zu schwächen.
- Leitfrage: Welche Scopes können vom Streamer-OAuth weg auf den zentralen Bot-Account verlagert werden, welche bleiben Broadcaster-Pflicht, und welche sind aktuell nur Altlasten?

## Kurzfazit
- `analytics:read:games` kann sofort aus dem Streamer-OAuth entfernt werden, weil der Scope im Code aktuell bewusst ungenutzt ist.
- `moderator:read:chatters` ist ab jetzt fachlich nicht mehr harte Streamer-Pflicht, weil ein Bot-Fallback existiert.
- `chat:read` und `chat:edit` auf Streamer-Tokens sind sehr wahrscheinlich Legacy und sollten mittelfristig komplett aus dem Streamer-OAuth verschwinden.
- Die meisten `channel:*`-Scopes bleiben Broadcaster-Pflicht oder werden zumindest weiterhin als Broadcaster-Freigabe benötigt.
- Viele `moderator:*`-Scopes sind grundsätzlich botfähig, aber in eurem Code teils noch nicht konsequent auf Bot-Kontext umgestellt.

## Aktueller Ist-Zustand

### Streamer-Scope-Abdeckung in `twitch_raid_auth`
- `31/31`: `channel:manage:raids`
- `31/31`: `moderator:read:followers`
- `31/31`: `moderator:manage:banned_users`
- `31/31`: `moderator:manage:chat_messages`
- `31/31`: `channel:read:subscriptions`
- `31/31`: `analytics:read:games`
- `31/31`: `channel:manage:moderators`
- `31/31`: `channel:bot`
- `31/31`: `chat:read`
- `31/31`: `chat:edit`
- `30/31`: `clips:edit`
- `30/31`: `channel:read:ads`
- `30/31`: `bits:read`
- `30/31`: `channel:read:hype_train`
- `21/31`: `moderator:read:chatters`
- `21/31`: `moderator:manage:shoutouts`
- `21/31`: `channel:read:redemptions`

### Bot-Token-Scopes
- Zentral vorhanden auf dem Bot:
  - `user:read:chat`
  - `user:write:chat`
  - `channel:bot`
  - `channel:manage:raids`
  - `channel:manage:moderators`
  - `channel:read:subscriptions`
  - `channel:read:ads`
  - `channel:read:redemptions`
  - `bits:read`
  - `channel:read:hype_train`
  - `moderator:read:chatters`
  - `moderator:read:followers`
  - `moderator:manage:banned_users`
  - `moderator:manage:chat_messages`
  - `moderator:manage:shoutouts`
  - `clips:edit`
  - zusätzlich weitere Bot-Scopes

## Entscheidungsregel

### 1. Sicher botfähig
- Scope ist an den ausführenden Moderator/Bot gebunden.
- Der Bot kann mit eigenem User-Token arbeiten.
- Der Bot ist im Zielkanal Moderator oder kann Mod werden.
- Dann kann der Streamer-Scope oft optional oder ganz entfernbar werden.

### 2. Broadcaster-Pflicht
- Scope autorisiert kanalbezogene Rechte oder Broadcaster-spezifische Leserechte.
- Dann bleibt ein Broadcaster-Grant nötig, auch wenn der API- oder EventSub-Call technisch später mit App-Token läuft.

### 3. Aktuell nur Legacy im Code
- Scope wird nicht mehr fachlich benötigt, ist aber noch in Scope-Listen, UI oder Gatekeeper-Checks enthalten.
- Diese Scopes sollten zuerst aus Gatekeepern und UI entfernt werden, dann aus dem Streamer-OAuth.

## Matrix pro Scope

### `channel:manage:raids`
- Kategorie: Broadcaster-Pflicht
- Aktuelle Nutzung: tatsächlicher Raid-Start über Streamer-Token in [executor.py](../bot/raid/executor.py#L123)
- Bot-Ersatz möglich: Nein
- Empfehlung: Muss im Streamer-Core-OAuth bleiben

### `channel:bot`
- Kategorie: Broadcaster-Freigabe
- Aktuelle Nutzung:
  - als Pflicht in Scope-Listen in [auth.py](../bot/raid/auth.py#L43)
  - zentral relevant für Chat-/Bot-Zugriffskonzept
- Bot-Ersatz möglich: Nein, weil es gerade die Broadcaster-Freigabe für Bot-Kontext ist
- Empfehlung: Im Streamer-Core-OAuth behalten

### `channel:manage:moderators`
- Kategorie: Broadcaster-Pflicht, aber optionalisierbar
- Aktuelle Nutzung: automatisches Modden des Bots via Streamer-Token in [connection.py](../bot/chat/connection.py#L100)
- Bot-Ersatz möglich: Nein, das Recht den Bot zum Mod zu machen liegt beim Broadcaster
- Empfehlung:
  - behalten, wenn "Bot setzt sich selbst als Mod" UX wichtig ist
  - optional machen, wenn ihr stattdessen manuelles `/mod` akzeptiert

### `chat:read`
- Kategorie: Legacy im Streamer-OAuth
- Aktuelle Nutzung: nur noch als Gatekeeper-Check in
  - [bot.py](../bot/chat/bot.py#L1217)
  - [connection.py](../bot/chat/connection.py#L728)
  - [eventsub_mixin.py](../bot/monitoring/eventsub_mixin.py#L506)
- Bot-Ersatz möglich: Ja, faktisch bereits zentral über Bot-Scopes
- Twitch-Modell heute: Chat lesen läuft botseitig über `user:read:chat`, nicht über Streamer-`chat:read`
- Empfehlung: Nach kleiner Codebereinigung aus Streamer-OAuth entfernen

### `chat:edit`
- Kategorie: Legacy im Streamer-OAuth
- Aktuelle Nutzung: ebenfalls nur noch als Gatekeeper-Check / Scope-Liste
- Bot-Ersatz möglich: Ja, Bot sendet Chat-Nachrichten zentral mit eigenem Token und `user:write:chat`
- Empfehlung: Nach kleiner Codebereinigung aus Streamer-OAuth entfernen

### `moderator:read:chatters`
- Kategorie: botfähig
- Aktuelle Nutzung:
  - bisher streamerseitig in [mixin.py](../bot/analytics/mixin.py#L245)
  - jetzt mit Bot-Fallback in [mixin.py](../bot/analytics/mixin.py#L272)
- Bot-Ersatz möglich: Ja, wenn Bot Mod im Kanal ist
- Status: bereits technisch als Fallback umgesetzt
- Empfehlung:
  - aus "harte User-Pflicht" auf "optional / Bot-Fallback vorhanden" herabstufen
  - später ganz aus Streamer-Core-OAuth entfernen, wenn Bot-Mod-Abdeckung stabil ist

### `moderator:manage:chat_messages`
- Kategorie: botfähig
- Aktuelle Nutzung: Moderationsaktionen laufen bereits mit Bot-Token in [moderation.py](../bot/chat/moderation.py#L1048)
- Bot-Ersatz möglich: Ja, für Chat-Löschung und Moderation
- Einschränkung: Streamer-Scope könnte noch indirekt für Broadcaster-zentrierte EventSub-/Bestandslogik mitgeschleppt werden
- Empfehlung:
  - fachlich nicht mehr Core-Streamer-Pflicht für Moderationsaktionen
  - erst aus Streamer-OAuth entfernen, wenn ihr sicher seid, dass keine Broadcaster-abhängigen Nebenpfade diesen Scope voraussetzen

### `moderator:manage:banned_users`
- Kategorie: botfähig
- Aktuelle Nutzung: Bot führt Bans/Unbans selbst aus, ebenfalls in [moderation.py](../bot/chat/moderation.py#L1048)
- Bot-Ersatz möglich: Ja
- Einschränkung: ihr registriert derzeit Broadcaster-EventSub für `channel.ban` / `channel.unban` in [eventsub_mixin.py](../bot/monitoring/eventsub_mixin.py#L1117)
- Empfehlung:
  - für aktive Moderationsaktionen nicht mehr Core-Streamer-Pflicht
  - für bestehende Broadcaster-EventSub-Logik aktuell noch nicht einfach entfernbar

### `moderator:manage:shoutouts`
- Kategorie: wahrscheinlich botfähig
- Aktuelle Nutzung:
  - Scope in Listen und Broadcaster-EventSub-Registrierung in [eventsub_mixin.py](../bot/monitoring/eventsub_mixin.py#L1119)
  - Bot hat den Scope bereits
- Bot-Ersatz möglich: wahrscheinlich ja, wenn Shoutout-Aktionen rein botseitig gefahren werden
- Einschränkung: aktuelle Subscription-/Telemetry-Logik hängt noch am Broadcaster-Kontext
- Empfehlung: Kandidat für Phase 2, noch nicht sofort aus Streamer-OAuth streichen

### `moderator:read:followers`
- Kategorie: teilweise botfähig
- Aktuelle Nutzung: Follower-Abfrage läuft heute über Streamer-Token in [sessions_mixin.py](../bot/monitoring/sessions_mixin.py#L672)
- Bot-Ersatz möglich: vermutlich ja, wenn Bot Mod ist und der API-Call auf Bot-Kontext umgestellt wird
- Einschränkung: aktuelle Implementierung ist noch streamerzentriert
- Empfehlung: Kandidat für späteren Refactor, noch nicht sofort aus Streamer-Core entfernen

### `channel:read:subscriptions`
- Kategorie: Broadcaster-Pflicht
- Aktuelle Nutzung:
  - Helix Analytics in [mixin.py](../bot/analytics/mixin.py#L96)
  - EventSub `channel.subscribe` / `channel.subscription.*` in [eventsub_mixin.py](../bot/monitoring/eventsub_mixin.py#L1104)
- Bot-Ersatz möglich: Nein, Broadcaster-Grant bleibt nötig
- Empfehlung: Nicht aus Streamer-OAuth entfernen, höchstens in "Advanced Analytics" auslagern

### `channel:read:ads`
- Kategorie: Broadcaster-Pflicht
- Aktuelle Nutzung:
  - Helix `get_ad_schedule` in [mixin.py](../bot/analytics/mixin.py#L101)
  - EventSub `channel.ad_break.begin` in [eventsub_mixin.py](../bot/monitoring/eventsub_mixin.py#L1116)
- Bot-Ersatz möglich: Nein
- Empfehlung: Nicht aus Streamer-OAuth entfernen, höchstens optionalisieren

### `channel:read:redemptions`
- Kategorie: Broadcaster-Pflicht
- Aktuelle Nutzung: EventSub Channel Points in [eventsub_mixin.py](../bot/monitoring/eventsub_mixin.py#L1121)
- Bot-Ersatz möglich: Nein
- Empfehlung: Nicht botseitig ersetzbar

### `bits:read`
- Kategorie: Broadcaster-Pflicht
- Aktuelle Nutzung: EventSub `channel.cheer` / `channel.bits.use` in [eventsub_mixin.py](../bot/monitoring/eventsub_mixin.py#L1099)
- Bot-Ersatz möglich: Nein
- Empfehlung: Nicht botseitig ersetzbar

### `channel:read:hype_train`
- Kategorie: Broadcaster-Pflicht
- Aktuelle Nutzung: EventSub Hype-Train in [eventsub_mixin.py](../bot/monitoring/eventsub_mixin.py#L1101)
- Bot-Ersatz möglich: Nein
- Empfehlung: Nicht botseitig ersetzbar

### `clips:edit`
- Kategorie: Broadcaster-Pflicht
- Aktuelle Nutzung: Clip-Erstellung mit Streamer-Token in [commands.py](../bot/chat/commands.py#L407)
- Bot-Ersatz möglich: Nein, Clip wird für den Broadcaster erstellt
- Empfehlung: Nur behalten, wenn Clip-Feature Teil des Core-Angebots bleiben soll

### `analytics:read:games`
- Kategorie: ungenutzt
- Aktuelle Nutzung: laut Kommentar bewusst ungenutzt in [mixin.py](../bot/analytics/mixin.py#L13)
- Bot-Ersatz möglich: nicht relevant
- Empfehlung: Sofort aus Streamer-OAuth entfernen

## Sofort umsetzbare Reduktionen

### 1. Sofort aus dem Streamer-OAuth entfernen
- `analytics:read:games`

### 2. Sofort aus "Pflicht" auf "optional" herabstufen
- `moderator:read:chatters`

### 3. Nach kleinem Code-Refactor aus Streamer-OAuth entfernen
- `chat:read`
- `chat:edit`

Dafür müssen die alten Gatekeeper-Checks entfernt oder auf Bot-Kontext umgestellt werden in:
- [bot.py](../bot/chat/bot.py#L1217)
- [connection.py](../bot/chat/connection.py#L728)
- [eventsub_mixin.py](../bot/monitoring/eventsub_mixin.py#L506)

## Kandidaten für Phase 2
- `moderator:manage:chat_messages`
- `moderator:manage:banned_users`
- `moderator:manage:shoutouts`
- `moderator:read:followers`

Diese Scopes sind fachlich gute Bot-Kandidaten, aber aktuell noch nicht sauber genug aus Broadcaster-abhängigen Nebenpfaden entkoppelt.

## Scopes, die im Streamer-OAuth bleiben sollten
- `channel:manage:raids`
- `channel:bot`
- `channel:manage:moderators` falls Auto-Mod-Setup gewünscht bleibt
- `channel:read:subscriptions` wenn Sub-Analytics/EventSub erhalten bleiben sollen
- `channel:read:ads`
- `channel:read:redemptions`
- `bits:read`
- `channel:read:hype_train`
- `clips:edit` wenn Clip-Feature Teil des Angebots ist

## Produkt-Empfehlung für "zugänglicheres OAuth"

### Variante A: Lean Core OAuth
- Ziel: erster Consent so klein wie möglich
- Enthält nur:
  - `channel:manage:raids`
  - `channel:bot`
  - optional `channel:manage:moderators`

### Variante B: Core + Chat Automation
- zusätzlich botzentrierte Chat-Funktionen
- Streamer-seitig weiterhin nur Broadcaster-Freigaben
- Bot-Scopes liegen zentral auf dem Bot-Account und werden nicht auf jedem Streamer-OAuth-Screen wiederholt

### Variante C: Erweiterte Analytics / Monetization
- separater optionaler Reauth-Block für:
  - `channel:read:subscriptions`
  - `channel:read:ads`
  - `channel:read:redemptions`
  - `bits:read`
  - `channel:read:hype_train`
  - `clips:edit`

## Empfohlene Reihenfolge
1. `analytics:read:games` aus allen Streamer-Scope-Listen und UI entfernen.
2. `moderator:read:chatters` in UI und Readiness-Checks auf "Bot-Fallback vorhanden" umstellen.
3. `chat:read` / `chat:edit` aus den alten Partner-/Join-Gatekeepern entfernen.
4. Erst danach den sichtbaren Streamer-OAuth auf einen kleineren Core-Scope-Satz reduzieren.
5. Optional zweite OAuth-Stufe für Analytics-/Monetization-Scopes einführen.

## Wichtigster Produktpunkt
- Wenn ihr den Scope-Screen nur optisch kleiner macht, ohne die Runtime sauber umzubauen, produziert ihr später versteckte Capability-Lücken.
- Wenn ihr dagegen sauber zwischen
  - globalen Bot-Scopes,
  - Broadcaster-Core-Scopes,
  - optionalen Analytics-Scopes
  trennt, wird der OAuth nicht nur kleiner, sondern ehrlich kleiner.
