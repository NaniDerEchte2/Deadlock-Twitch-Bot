# PROJ-4: OAuth Scope Migration Plan fuer Streamer- und Bot-Scopes

## Status: 🔵 Planned

## Summary
- Das Produkt trennt den sichtbaren Streamer-OAuth kuenftig sauber in Broadcaster-Core-Scopes, optionale Advanced-Scopes und zentral verwaltete Bot-Scopes.
- Ziel ist nicht, Scopes nur optisch zu verstecken, sondern fachlich korrekt vom Streamer-Token auf den Bot-Token zu verlagern, wo Twitch und die aktuelle Bot-Architektur das wirklich erlauben.
- Die Scope-Reduktion soll den ersten OAuth zugänglicher machen, ohne spaeter stille Capability-Luecken zu erzeugen.
- Parallel dazu wird der Bot-Token auf einen kleineren, real genutzten Scope-Satz ausgerichtet.

## Ziele
- Den sichtbaren Streamer-OAuth kleiner und verstaendlicher machen.
- Bot-Scopes und Broadcaster-Scopes im Produkt explizit trennen.
- Bereits botfaehige Features aus dem Streamer-OAuth herausloesen.
- Broadcaster-pflichtige Features im Streamer-OAuth belassen und nicht falsch als botseitig darstellen.
- Die Scope-UI, Reauth-Hinweise und Missing-Scope-Diagnostik auf das neue Modell umstellen.
- Einen klaren Zielzustand fuer den Bot-Token definieren, inklusive spaeterer Reauth falls noetig.

## Nicht-Ziele
- Keine Aenderung daran, dass Auto-Raids weiter per Broadcaster-Token ausgefuehrt werden.
- Kein bloeszes Ausblenden von Scopes im UI ohne passende Runtime-Aenderung.
- Kein Zwang, alle Advanced-Analytics-Features im selben Schritt umzubauen.
- Kein sofortiger Wechsel des Chat-EventSub-Flows auf App-Access-Token-only.
- Keine automatische Abschaltung bestehender Features nur um den OAuth-Screen kuenstlich kleiner zu machen.

## Aktueller Stand am 15.03.2026
- `analytics:read:games` wurde bereits aus den aktiven Streamer-Scope-Listen und der Scope-UI entfernt.
- `moderator:read:chatters` hat bereits einen Bot-Fallback.
- `chat:read` und `chat:edit` sind im Streamer-OAuth aktuell noch Legacy-Reste aus alten Gatekeepern.
- Mehrere Moderationsaktionen laufen heute bereits technisch ueber den Bot-Token.
- Broadcaster-gebundene Analytics- und EventSub-Scopes laufen weiterhin ueber Streamer-Tokens und duerfen nicht stillschweigend als botseitig dargestellt werden.

## Problem Statement
- Der aktuelle Streamer-OAuth wirkt groesser als noetig, weil botseitige und broadcaster-seitige Rechte vermischt angezeigt werden.
- Alte User haben teils weniger oder veraltete Scopes, wodurch Reauth-Hinweise unklar oder abschreckend wirken.
- Der Bot-Token traegt heute mehr Scopes als fuer die zentrale Bot-Rolle fachlich noetig sein duerfte.
- Ohne klares Zielbild ist unklar, welche Scopes wirklich aus dem Streamer-OAuth verschwinden koennen und welche trotz Bot bestehen bleiben muessen.

## Scope-Modell

### 1. Broadcaster-Core-Scopes
- Diese Scopes bleiben im Streamer-OAuth, weil Twitch die Aktion oder das Lesen direkt an den Broadcaster bindet.
- Dazu gehoeren mindestens:
  - `channel:manage:raids`
  - `channel:bot`
- Optional im Core, falls Self-Service-Auto-Mod weiter Teil des Onboardings bleibt:
  - `channel:manage:moderators`

### 2. Broadcaster-Advanced-Scopes
- Diese Scopes bleiben streamer-seitig, koennen aber in einen zweiten optionalen OAuth-Schritt verschoben werden.
- Dazu gehoeren aktuell:
  - `channel:read:subscriptions`
  - `channel:read:ads`
  - `channel:read:redemptions`
  - `bits:read`
  - `channel:read:hype_train`
  - `clips:edit` solange Clip-Erstellung nicht sauber botseitig umgebaut ist

### 3. Bot-zentrierte Scopes
- Diese Scopes gehoeren fachlich dem Bot-Account oder dem Bot als Moderator und sollen mittelfristig nicht mehr als harte Streamer-Pflicht erscheinen.
- Dazu gehoeren:
  - `user:read:chat`
  - `user:write:chat`
  - `moderator:read:chatters`
  - `moderator:manage:chat_messages`
  - `moderator:manage:banned_users`
  - `moderator:read:followers`
  - `moderator:manage:shoutouts`
- Optional spaeter:
  - `user:bot` fuer einen moeglichen App-Access-Token-Chat-EventSub-Flow
  - `clips:edit`, falls Clip-Erstellung bewusst auf den Bot-Token migriert wird

### 4. Legacy-Scopes im Streamer-OAuth
- Diese Scopes wirken aktuell noch im Streamer-OAuth, obwohl die Fachlogik bereits botzentriert ist oder fast botzentriert ist.
- Dazu gehoeren:
  - `chat:read`
  - `chat:edit`
- Diese Scopes sollen erst nach Gatekeeper-Bereinigung aus dem Streamer-OAuth verschwinden.

## Zielbild

### Streamer OAuth Stufe 1: Lean Core
- Sichtbar im Erst-Onboarding:
  - `channel:manage:raids`
  - `channel:bot`
  - optional `channel:manage:moderators`
- Zweck:
  - Auto-Raids
  - Bot-Zugriff im Kanal
  - optional Bot-Self-Service-Mod-Setup

### Streamer OAuth Stufe 2: Advanced Analytics und Monetization
- Nur fuer Features, die wirklich broadcaster-seitige Leserechte brauchen:
  - `channel:read:subscriptions`
  - `channel:read:ads`
  - `channel:read:redemptions`
  - `bits:read`
  - `channel:read:hype_train`
  - optional `clips:edit`
- Zweck:
  - Revenue-, Funnel- und Engagement-Analytics
  - EventSub fuer monetarisierungsnahe Kanalereignisse
  - optionale Clip-Funktionen

### Zentraler Bot-OAuth
- Nicht pro Streamer erneut sichtbar.
- Ziel-Scopes:
  - `user:read:chat`
  - `user:write:chat`
  - `moderator:read:chatters`
  - `moderator:manage:chat_messages`
  - `moderator:manage:banned_users`
  - `moderator:read:followers`
  - `moderator:manage:shoutouts`
- Optional:
  - `clips:edit` falls Clip-Migration umgesetzt wird
  - `user:bot` falls Chat-EventSub spaeter mit App-Token gefahren wird

## Scope-Matrix

| Scope | Zielzustand | Streamer sichtbar? | Bot-Reauth moeglich? | Hinweis |
| --- | --- | --- | --- | --- |
| `channel:manage:raids` | Broadcaster-Core | Ja | Nein | Muss beim Streamer bleiben |
| `channel:bot` | Broadcaster-Core | Ja | Nein | Broadcaster-Freigabe, kein echter Bot-Only-Scope |
| `channel:manage:moderators` | Optionaler Broadcaster-Core | Optional | Nein | Nur noetig fuer Auto-Mod-Self-Service |
| `chat:read` | Legacy, entfernen | Nein | Nicht relevant | Durch Bot-Chatmodell ersetzen |
| `chat:edit` | Legacy, entfernen | Nein | Nicht relevant | Durch Bot-Chatmodell ersetzen |
| `moderator:read:chatters` | Bot-zentriert | Spaeter nein | Ja | Fallback existiert bereits |
| `moderator:manage:chat_messages` | Bot-zentriert | Spaeter nein | Ja | Delete/Moderation laeuft schon botseitig |
| `moderator:manage:banned_users` | Bot-zentriert | Spaeter nein | Ja | Ban/Unban weitgehend botseitig |
| `moderator:read:followers` | Bot-zentriert nach Refactor | Spaeter nein | Ja | Aktuell noch gemischt |
| `moderator:manage:shoutouts` | Bot-zentriert nach Refactor | Spaeter nein | Ja | Kandidat fuer Phase 2 |
| `channel:read:subscriptions` | Broadcaster-Advanced | Optional | Nein | Broadcaster-gebunden |
| `channel:read:ads` | Broadcaster-Advanced | Optional | Nein | Broadcaster-gebunden |
| `channel:read:redemptions` | Broadcaster-Advanced | Optional | Nein | Broadcaster-gebunden |
| `bits:read` | Broadcaster-Advanced | Optional | Nein | Broadcaster-gebunden |
| `channel:read:hype_train` | Broadcaster-Advanced | Optional | Nein | Broadcaster-gebunden |
| `clips:edit` | Offen, Phase-Entscheidung | Optional | Ja, falls migriert | Vorlaeufig nicht aus Streamer-OAuth entfernen |

## User Stories
- Als Streamer moechte ich beim ersten OAuth nur die Rechte sehen, die fuer den Kernnutzen wirklich noetig sind, damit der Consent nicht abschreckend wirkt.
- Als Betreiber moechte ich botseitige Rechte zentral verwalten, damit ich nicht fuer jede Bot-Funktion alle Streamer neu autorisieren lassen muss.
- Als Admin moechte ich klar sehen, ob ein fehlender Scope ein globales Bot-Problem oder ein kanalbezogenes Broadcaster-Problem ist.
- Als Entwickler moechte ich fuer jeden Scope einen definierten Zielzustand haben, damit Refactors nicht zu halb migrierten Zwischenzustaenden fuehren.
- Als Produktverantwortlicher moechte ich Advanced-Analytics-Scopes separat anbieten, damit der Core-OAuth ehrlich kleiner wird.

## Umsetzungsphasen

### Phase 1: Bereits umgesetzt oder unmittelbar verfuegbar
- `analytics:read:games` bleibt dauerhaft entfernt.
- `moderator:read:chatters` wird nicht mehr als harte Streamer-Pflicht behandelt.
- Die Doku und Scope-UI benennen `analytics:read:games` nicht mehr als Produktvoraussetzung.

### Phase 2: Legacy-Chat-Sauberkeit
- Alte Gatekeeper fuer `chat:read` und `chat:edit` werden entfernt oder auf Bot-Kontext umgestellt.
- Missing-Scope-Checks duerfen diese beiden Streamer-Scopes danach nicht mehr als Pflicht anzeigen.
- Reauth-Hinweise fuer alte User muessen nach der Umstellung ohne diese Legacy-Scopes funktionieren.

### Phase 3: Bot als Source of Truth fuer Moderator-Scopes
- `moderator:manage:chat_messages` wird produktseitig explizit als botzentriert modelliert.
- `moderator:manage:banned_users`, `moderator:read:followers` und `moderator:manage:shoutouts` werden auf echte Bot-Pfade geprueft und schrittweise umgestellt.
- Streamer-UI kennzeichnet diese Scopes waehrend der Migration als `Bot-Fallback vorhanden` oder `wird zentralisiert`.

### Phase 4: Streamer-OAuth splitten
- Streamer-Consent wird in `Lean Core` und `Advanced Analytics / Monetization` getrennt.
- Features, die Advanced-Scopes brauchen, muessen in UI und Runtime sichtbar degradieren, wenn nur Core autorisiert wurde.
- Reauth-Aufforderungen duerfen nicht mehr pauschal den Vollsatz verlangen, wenn fuer den User nur Core noetig ist.

### Phase 5: Bot-OAuth neu zuschneiden
- Der aktuelle Bot-Scope-Bestand wird gegen echte Bot-Nutzung auditiert.
- Scopes, die nur broadcaster-seitige Rechte fuer den Bot-eigenen Kanal abbilden und fuer das Produkt nicht gebraucht werden, werden aus dem Ziel-Bot-OAuth entfernt.
- Kandidaten fuer spaetere Entfernung vom Bot-Token:
  - `channel:manage:raids`
  - `channel:manage:moderators`
  - `channel:read:subscriptions`
  - `channel:read:ads`
  - `channel:read:redemptions`
  - `bits:read`
  - `channel:read:hype_train`
- `clips:edit` bleibt auf dem Bot-Token nur dann erhalten, wenn die Clip-Erstellung wirklich botseitig migriert wird.
- `user:bot` wird nur nachgezogen, wenn der Chat-EventSub-Flow es technisch wirklich verlangt.

## Acceptance Criteria

### 1. Scope-Zielzustand dokumentiert
- Es existiert fuer jeden heute angefragten Scope ein dokumentierter Zielzustand:
  - Broadcaster-Core
  - Broadcaster-Advanced
  - Bot-zentriert
  - Legacy/entfernen
- Produkt, Runtime und Doku verwenden dieselbe Zuordnung.

### 2. Lean Core ist ehrlich kleiner
- Ein neuer Streamer sieht im Erst-OAuth keinen Scope, der ausschliesslich botseitig gebraucht wird.
- `analytics:read:games` taucht in aktiven Scope-Listen, Reauth-Hinweisen und Scope-UI nicht mehr auf.
- `chat:read` und `chat:edit` tauchen nach Abschluss von Phase 2 nicht mehr im Streamer-Core auf.

### 3. Advanced-Scopes sind separat behandelbar
- Broadcaster-gebundene Analytics- und Monetization-Scopes koennen separat von Core betrachtet und spaeter separat autorisiert werden.
- Fehlende Advanced-Scopes blockieren keine Core-Funktion, solange die betroffenen Features sauber degradiert werden.
- Die UI zeigt dem User klar, welche Features ohne Advanced-Scopes fehlen.

### 4. Bot-Probleme und Streamer-Probleme sind getrennt sichtbar
- Fehlende botseitige Scopes werden als globales Bot-Problem dargestellt.
- Fehlende broadcaster-seitige Scopes werden kanalbezogen dargestellt.
- `channel:bot` wird nicht als globaler Bot-User-Scope missverstaendlich angezeigt.

### 5. Bot-OAuth ist auditierbar
- Vor einer Bot-Reauth gibt es eine explizite Liste:
  - welche Bot-Scopes bleiben muessen
  - welche optional sind
  - welche entfernt werden koennen
- Die Bot-Reauth ergaenzt nur Scopes, die fuer den Zielzustand wirklich gebraucht werden.
- Die Bot-Reauth schleppt keine ueberholten Broadcaster-Scopes mehr mit, nur weil sie historisch vorhanden waren.

### 6. Bestehende User werden sauber migriert
- Alte User mit historischem Scope-Satz bleiben funktional, bis die jeweilige Migration abgeschlossen ist.
- Reauth-Aufforderungen verlangen nur den Scope-Satz, der fuer den jeweiligen Zielpfad noch wirklich notwendig ist.
- Die Migration produziert keine stillen Regressionen in Auto-Raids, Moderation oder Analytics.

## Edge Cases
- Ein Streamer hat nur Lean Core autorisiert: Analytics- und Monetization-Features muessen sauber als nicht verfuegbar dargestellt werden.
- Der Bot hat den noetigen Moderator-Scope, ist im Kanal aber nicht Mod: Die Funktion darf nicht als `fehlender Scope` fehlklassifiziert werden.
- Ein Streamer hat `channel:manage:moderators` nicht autorisiert, moddet den Bot aber manuell: Der Kanal muss trotzdem botzentrierte Moderator-Features nutzen koennen.
- Der Bot verliert global einen zentralen Scope wie `user:write:chat`: Das Problem muss als globaler Bot-Fehler erscheinen, nicht als fehlender Streamer-Scope.
- Ein alter Streamer hat noch `chat:read` und `chat:edit`, ein neuer nicht: Beide muessen nach Phase 2 gleich funktionieren.
- `clips:edit` bleibt im Streamer-OAuth, waehrend andere Bot-Scopes bereits migriert wurden: Die UI muss klar machen, dass Clips ein eigener Sonderfall sind.
- Ein Streamer hat Advanced-Analytics nicht freigeschaltet, aber Core-Funktionen inklusive Auto-Raid aktiviert: Das darf keine pauschale Reauth-Pflicht ausloesen.

## Offene Produktentscheidungen
- Soll `channel:manage:moderators` Teil des Lean-Core bleiben oder als optionale Komfort-Freigabe behandelt werden?
- Soll `clips:edit` bewusst auf den Bot-Token migriert werden oder als streamer-seitige Spezialfunktion verbleiben?
- Sollen `channel:read:subscriptions`, `channel:read:ads`, `bits:read`, `channel:read:hype_train` und `channel:read:redemptions` als gemeinsames `Advanced Analytics`-Paket vermarktet werden?
- Soll `user:bot` nur vorbereitet dokumentiert oder bereits im naechsten Bot-Reauth mitgenommen werden?

## Empfehlung
- Erst den Streamer-OAuth fachlich sauber verkleinern, dann den Bot-OAuth entruempeln.
- Zuerst Legacy-Scopes und Bot-Fallbacks bereinigen, danach den Consent-Screen reduzieren.
- Broadcaster-Advanced-Scopes nicht verstecken, sondern als optionales zweites Autorisierungsniveau produktseitig sauber erklaeren.
