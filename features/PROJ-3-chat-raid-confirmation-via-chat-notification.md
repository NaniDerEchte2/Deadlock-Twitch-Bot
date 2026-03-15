# PROJ-3: Zielseitige Raid-Bestätigung via channel.chat.notification

## Status: 🔵 Planned

## Summary
- Der Bot ergänzt die bestehende Raid-Bestätigung über `channel.raid` um ein zweites zielseitiges Signal über `channel.chat.notification`.
- Ziel ist nicht, `channel.raid` zu ersetzen, sondern partnerseitige Raid-Ankünfte robuster und besser klassifizierbar zu machen.
- Das Feature soll vor allem drei Probleme lösen:
  - zusätzliche Bestätigung, wenn `channel.raid` verspätet oder gar nicht eintrifft
  - bessere Einordnung, ob ein Raid von einem eigenen Streamer oder von extern kam
  - bessere Nachvollziehbarkeit, warum ein Pending-Raid bestätigt oder nicht bestätigt wurde

## Ziele
- Für Partner-Zielkanäle ein zweites, chatnahes Bestätigungssignal für Raid-Ankünfte bereitstellen.
- `channel.chat.notification` mit `notice_type = raid` für die Raid-Korrelation nutzbar machen.
- Bestehende Pending-Raids durch ein alternatives Bestätigungssignal auflösen können.
- Eingehende Partner-Raids fachlich klassifizieren:
  - `ours_to_partner`
  - `external_to_partner`
- Logging, Telemetrie und Admin-Sicht so erweitern, dass Scope- und Auth-Probleme schnell sichtbar werden.
- Bot-Scopes und Streamer-Scopes im Produkt sauber voneinander trennen.

## Nicht-Ziele
- Kein Ersatz des bestehenden `channel.raid`-Signals als Primärsignal.
- Keine universelle Bestätigung für beliebige Non-Partner-Ziele ohne Chat-Zugriff.
- Keine Änderung am bestehenden Requirement, dass Auto-Raids weiterhin per Streamer-OAuth mit `channel:manage:raids` ausgeführt werden.
- Kein Umbau des gesamten Chat-Bots auf App-Access-Token-only als Voraussetzung für MVP.
- Keine Verpflichtung, `user:bot` im MVP einzuführen, solange der aktuelle User-Token-basierte Chat-EventSub-Flow stabil funktioniert.

## Aktueller Stand am 15.03.2026
- Der Chat-Bot abonniert aktuell `channel.chat.message`, aber noch nicht `channel.chat.notification`.
- Der aktuelle Bot-Token enthält bereits:
  - `user:read:chat`
  - `user:write:chat`
  - `channel:bot`
  - `chat:read`
  - `chat:edit`
- Der aktuelle Bot-Token enthält aktuell nicht `user:bot`.
- In `twitch_raid_auth` haben aktuell `31/31` autorisierte Streamer `channel:bot`.
- In `twitch_raid_auth` haben aktuell `0/31` autorisierte Streamer `user:read:chat` und `0/31` `user:bot`.
- Daraus folgt für die aktuelle Architektur:
  - Bot-seitige Chat-Scopes liegen zentral auf dem Bot-Account.
  - Streamer-seitige Channel-Freigaben liegen pro Broadcaster in `twitch_raid_auth`.
  - Eine neue Bot-Reauth ist nur nötig, wenn ein zusätzlich benötigter Bot-User-Scope fehlt.
  - Eine Streamer-Reauth ist nur nötig, wenn ein zusätzlich benötigter Broadcaster-Scope fehlt.

## Scope-Modell

### 1. Bot-User-Scopes
- Bot-User-Scopes gehören dem Bot-Account selbst.
- Beispiele:
  - `user:read:chat`
  - `user:write:chat`
  - optional `user:bot`
- Wenn ein solcher Scope fehlt, reicht eine zentrale Reauth des Bot-Accounts.
- Dieser Schritt ist global und nicht pro Streamer zu wiederholen.

### 2. Broadcaster-/Streamer-Scopes
- Broadcaster-Scopes gehören dem jeweiligen Streamer-Konto.
- Beispiele:
  - `channel:manage:raids`
  - `channel:bot`
  - `channel:read:subscriptions`
- Wenn ein solcher Scope fehlt, muss der jeweilige Streamer neu autorisieren.
- Dieser Schritt ist pro Streamer nötig und kann nicht durch einen Bot-Token ersetzt werden.

### 3. Unterschied fachlich
- Der Bot-Token sagt: "Was darf der Bot als eigener User tun oder lesen?"
- Der Streamer-Token sagt: "Was erlaubt dieser Broadcaster unserer App in seinem Kanal oder in seinem Namen?"
- Ein Bot mit `user:read:chat` darf nicht automatisch im Namen eines Streamers raiden.
- Ein Streamer mit `channel:manage:raids` ersetzt nicht die Chat-Leseberechtigung des Bot-Accounts.

### 4. Nachziehen von Scopes
- Fehlende Scopes können nicht einfach per Refresh "hinzugefügt" werden.
- Ein Refresh erneuert nur Tokens innerhalb derselben bestehenden Autorisierung.
- Neue Scopes erfordern immer einen neuen OAuth-Consent für genau den betroffenen Account:
  - Bot-Reauth für fehlende Bot-User-Scopes
  - Streamer-Reauth für fehlende Broadcaster-Scopes

## Fachliche Entscheidung für dieses Feature
- `channel.raid` bleibt das Primärsignal, weil es keine Autorisierung braucht und auf Zielseite breit verfügbar ist.
- `channel.chat.notification` wird als Sekundärsignal für Zielkanäle genutzt, in denen der Bot bereits Chat-EventSub empfangen darf.
- Für MVP wird `channel.chat.notification` nur dort ausgewertet, wo der Bot ohnehin als Chat-Bot aktiv ist oder aktiv sein darf.
- Der MVP soll nicht behaupten, dass damit alle `ours_to_non_partner`-Fälle universell bestätigt werden können.
- Die zusätzliche Logik ist ein Härtungs- und Klassifizierungs-Feature, kein fachlicher Ersatz für den bisherigen Raid-Flow.

## User Stories
- Als Betreiber möchte ich bei Partner-Zielkanälen ein zweites Raid-Bestätigungssignal haben, damit fehlende `channel.raid`-Events nicht sofort zu blinden Timeouts führen.
- Als Betreiber möchte ich eingehende Partner-Raids als `eigener Streamer` oder `extern` klassifizieren, damit Netzwerk-Raids sauber von Fremd-Raids getrennt werden.
- Als Entwickler möchte ich Bot-Scopes und Streamer-Scopes getrennt sehen, damit Reauth-Entscheidungen nicht auf falschen Annahmen beruhen.
- Als Admin möchte ich im Dashboard sehen, ob ein Problem am globalen Bot-Token oder an einzelnen Streamer-Freigaben liegt, damit ich nicht unnötig alle Streamer neu autorisieren lasse.
- Als Analyst möchte ich im Raid-Tracking den Bestätigungspfad sehen, damit ich nachvollziehen kann, ob ein Raid durch `channel.raid`, durch `channel.chat.notification` oder durch beide Signale bestätigt wurde.

## Acceptance Criteria

### 1. Zusätzliche Chat-Subscription
- Der Chat-Bot registriert für geeignete Kanäle zusätzlich zu `channel.chat.message` auch `channel.chat.notification`.
- Die Subscription nutzt denselben Kanal-Kontext wie der bestehende Chat-Join-Flow.
- Eine bestehende `channel.chat.message`-Subscription darf nicht implizit als Ersatz für `channel.chat.notification` behandelt werden.
- Subscription-Erstellung, Dedupe und Recovery müssen denselben Robustheitsgrad wie beim bestehenden Chat-Join haben.

### 2. Raid-relevante Notification-Auswertung
- Das System wertet `channel.chat.notification` mindestens für `notice_type = raid` aus.
- `notice_type = unraid` wird erkannt und separat geloggt, darf aber keine bestätigte Raid-Ankunft erzeugen.
- Nicht raid-relevante Notice-Typen werden für den Raid-Flow ignoriert, ohne Fehler oder Spam-Logs zu erzeugen.
- Der Parser extrahiert bei `notice_type = raid` mindestens:
  - Zielkanal
  - Quellkanal
  - Quell- und Ziel-User-ID, falls vorhanden
  - Viewer-Zahl, falls vorhanden
  - Zeitstempel

### 3. Korrelation mit Pending-Raids
- Wenn ein passender Pending-Raid existiert und ein kompatibles `channel.chat.notification`-Raid-Event eintrifft, darf dieses Event den Pending-Raid bestätigen.
- `channel.chat.notification` darf Pending-Raids nur dann bestätigen, wenn Source und Target eindeutig zum Pending-Raid passen.
- Wenn zuerst `channel.chat.notification` und später `channel.raid` eintrifft, wird die Ankunft nur einmal bestätigt.
- Wenn zuerst `channel.raid` und später `channel.chat.notification` eintrifft, wird das zweite Signal als Zusatz-Telemetrie gespeichert, aber darf keine doppelte Verarbeitung auslösen.

### 4. Klassifizierung
- Für bestätigte Raids auf Partner-Zielkanäle wird mindestens unterschieden:
  - `ours_to_partner`
  - `external_to_partner`
- Die Klassifizierung basiert auf der bekannten Streamer-Identity des Quellkanals.
- Wenn die Quelle nicht sicher aufgelöst werden kann, wird der Fall als `unknown_source_to_partner` geloggt und nicht stillschweigend als `external` behandelt.
- Für Non-Partner-Ziele darf die Spezifikation keinen universellen Bestätigungsanspruch machen.

### 5. Logging und Telemetrie
- Für jede bestätigte Partner-Raid-Ankunft wird geloggt:
  - Quelle
  - Ziel
  - verwendete Bestätigungssignale
  - erkannte Klassifizierung
- Für jeden Pending-Raid-Timeout wird geloggt, ob:
  - weder `channel.raid` noch `channel.chat.notification` ankamen
  - nur ein Signal fehlte
  - ein Signal wegen Korrelation oder Scope nicht nutzbar war
- Scope- oder Subscription-Fehler bei `channel.chat.notification` werden getrennt von fachlichen Raid-Timeouts geloggt.

### 6. Admin- und Scope-Transparenz
- Dashboard und Scope-Logik unterscheiden explizit zwischen:
  - globalen Bot-Scopes
  - pro Streamer erforderlichen Scopes
- `user:read:chat` darf nicht als Streamer-Pflichtscope angezeigt werden, wenn er ausschließlich botseitig benötigt wird.
- `channel:bot` bleibt als Broadcaster-Freigabe pro Streamer sichtbar.
- Das System zeigt klar an, ob ein fehlender Scope:
  - nur den Bot zentral betrifft
  - einzelne Streamer betrifft
  - nur für einen zukünftigen App-Token-Flow relevant wäre

### 7. Umgang mit user:bot
- Der MVP dokumentiert klar, dass `user:bot` nur dann Pflicht ist, wenn Chat-EventSub mit App-Access-Token statt mit User-Access-Token des Bot-Accounts betrieben wird.
- Solange der aktuelle User-Token-basierte Chat-Flow produktiv funktioniert, ist `user:bot` kein Blocker für das Feature.
- Wenn die Implementierung an irgendeiner Stelle auf App-Access-Token-basierte Chat-Subscriptions umgestellt wird, muss `user:bot` vor Produktivschaltung verpflichtend nachgezogen werden.

### 8. Testabdeckung
- Für einen Kanal mit bestehendem Chat-Join werden sowohl `channel.chat.message` als auch `channel.chat.notification` registriert.
- Ein `notice_type = raid`-Event bestätigt einen passenden Pending-Raid.
- Ein `notice_type = unraid`-Event bestätigt keinen Raid.
- Doppelte Bestätigung durch `channel.raid` plus `channel.chat.notification` erzeugt keinen doppelten History-Eintrag.
- Ein externer Raid auf einen Partner-Zielkanal wird als `external_to_partner` klassifiziert.
- Ein eigener bekannter Streamer-Raid auf einen Partner-Zielkanal wird als `ours_to_partner` klassifiziert.
- Fehlende Bot- oder Broadcaster-Scope-Situationen werden mit verständlichen Fehlermeldungen geloggt.
- Die Scope-Ansicht ordnet `user:read:chat` botseitig und `channel:bot` streamer-seitig korrekt ein.

## Edge Cases
- `channel.chat.notification` liefert ein Raid-Event, aber `channel.raid` kommt nie: Der Raid muss trotzdem als chat-bestätigt markierbar sein, sofern die Korrelation eindeutig ist.
- `channel.raid` kommt, aber `channel.chat.notification` nicht: Der bestehende Primärpfad bleibt gültig.
- Ein Raid-Event kommt vor Anlage des Pending-Raids an: Das System darf keinen falschen positiven Match erzeugen und muss den Fall zumindest diagnostisch loggen.
- Ein Zielkanal ist Partner, aber der Chat-Bot konnte den Kanal mangels Berechtigung nicht joinen: Das Feature degradiert sauber auf `channel.raid` only.
- Ein Zielkanal ist Non-Partner und wird nicht per Chat überwacht: Der Fall bleibt außerhalb des garantierten Bestätigungsbereichs dieses Features.
- Ein Streamer verliert `channel:bot`, während der Bot-Token unverändert gültig ist: Das Problem muss als kanalbezogene Broadcaster-Freigabe erkennbar sein, nicht als globales Bot-Problem.
- Der Bot verliert `user:read:chat`, während alle Streamer weiter `channel:bot` haben: Das Problem muss als globales Bot-Scope-Problem erkennbar sein.
- Twitch sendet `unraid` nach einem vorherigen `raid`: Das darf keine bestätigte Ankunft rückwirkend löschen, kann aber separat als Zustandsänderung oder Diagnose erfasst werden.

## Annahmen
- Die aktuelle Chat-Architektur verwendet für Chat-EventSub faktisch den Bot-User-Token und keinen reinen App-Access-Token-Flow.
- Deshalb ist `user:read:chat` im aktuellen Setup der relevante Bot-Scope, während `user:bot` für MVP nicht zwingend ist.
- `channel.chat.notification` ist für Raid-Bestätigung nur dort sinnvoll, wo der Zielkanal bereits chatseitig beobachtet werden darf.
- Eine globale Einordnung `ours_to_non_partner` lässt sich weiterhin nicht allein über partnerseitige Chat-Subscriptions garantieren.

## Empfehlung
- MVP als Sekundärsignal nur für Partner-Zielkanäle umsetzen.
- Scope-UI parallel bereinigen, damit Bot-Scopes und Streamer-Scopes nicht mehr vermischt werden.
- `user:bot` als vorbereitete, aber optionale Härtung dokumentieren, nicht als sofortige Pflicht für den aktuellen Flow.
