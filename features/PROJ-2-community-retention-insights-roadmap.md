# PROJ-2: Community Retention Insights Expansion

## Status: 🔵 Planned

## Summary
- Das Dashboard bekommt drei neue Retention- und Community-Insights mit echtem Entscheidungswert.
- Der Fokus liegt auf Loyalität, Abwanderung und Wiederkehr statt auf zusätzlichen Deko-KPIs.
- Bestehende Datenquellen und teils vorhandene Backend-Endpunkte werden bevorzugt wiederverwendet.

## Ziele
- Sichtbar machen, ob die Community aus One-Timern oder aus wiederkehrenden Stammzuschauern besteht.
- Drop-Off-Momente im Stream identifizieren und mit Chat- und Ad-Signalen erklären.
- Messbar machen, wie gut neue Chatter in wiederkehrende Community-Mitglieder übergehen.
- Features priorisieren, die sofort produktiv nutzbar sind und nicht nur analytisch interessant wirken.

## Nicht-Ziele
- Keine neue KI-Zusammenfassung ohne harte Datenbasis.
- Kein zusätzlicher Social-Graph oder Netzwerk-Chart als Primärfeature.
- Keine globale Benchmarking-Logik gegen alle Twitch-Streamer als Voraussetzung für MVP.
- Keine allumfassende Viewer-Journey über Video-Viewer ohne belastbare Datenbasis außerhalb der Chat-Audience.

## Priorisierung

### Phase 1: Loyalitätskurve und One-Timer-Quote
- Höchster Produktwert bei geringem Implementierungsrisiko.
- Backend ist bereits vorhanden über `loyalty_curve`.
- Ziel: Auf einen Blick zeigen, wie viele Accounts nur 1x auftauchen und wie stark der loyale Kern ist.

### Phase 2: Retention-Drop-Diagnose
- Höchster analytischer Mehrwert für konkrete Verbesserungen im Stream-Ablauf.
- Bestehende Retention-Kurve soll mit Drop-Events, Ad-Breaks und Chat-Spikes verbunden werden.
- Ziel: Nicht nur sehen, dass Viewer gehen, sondern wann und mit welchem Kontext.

### Phase 3: New-Chatter-Return-Funnel
- Strategisch sehr stark, weil er Community-Aufbau messbar macht.
- Braucht neue, saubere Funnel-Definitionen, ist aber mit vorhandenen Rollup-Daten machbar.
- Ziel: Aus Erstkontakten eine belastbare Return-Metrik machen.

## User Stories
- Als Streamer möchte ich sehen, wie hoch mein Anteil an One-Timern und Mehrfach-Besuchern ist, damit ich erkenne, ob ich Community aufbaue oder nur kurzfristige Reichweite einsammle.
- Als Streamer möchte ich kritische Drop-Off-Momente im Stream mit Kontext sehen, damit ich konkrete Format-, Timing- oder Moderationsentscheidungen treffen kann.
- Als Streamer möchte ich wissen, wie viele neue Chatter innerhalb eines definierten Zeitfensters zurückkommen, damit ich Onboarding- und Community-Maßnahmen bewerten kann.
- Als Analyst möchte ich, dass neue Features vorhandene Datenquellen wiederverwenden, damit Datenlogik konsistent bleibt und nicht doppelt gepflegt werden muss.
- Als Nutzer möchte ich klare, erklärbare Metriken sehen, damit Prozentwerte, Rohwerte und Vergleichsbasis nicht verwechselt werden.

## Acceptance Criteria

### 1. Loyalitätskurve
- Es gibt ein neues Dashboard-Modul oder eine neue Sektion für `Loyalitätskurve`.
- Die Kurve zeigt mindestens die Verteilung nach `1x`, `2x`, `3x`, `4-5x`, `6-9x`, `10x+`.
- Die `One-Timer-Quote` wird als prominente Kennzahl angezeigt.
- Die Kurve basiert auf realen Rollup-Daten und nicht auf Schätzungen.
- Die UI erklärt klar, dass es sich um All-Time- oder klar benannte Lifetime-Logik handelt.
- Wenn keine Daten vorliegen, wird kein irreführender Leerchart angezeigt, sondern ein sauberer Fallback-Status.

### 2. Retention-Drop-Diagnose
- Es gibt eine neue Retention-Ansicht mit aggregierter Kurve, P25/P75-Band und erkannten Drop-Events.
- Drop-Events werden mindestens nach `ad_break` und `unknown` klassifiziert.
- Relevante Chat-Kontextsignale werden pro Drop-Moment angezeigt:
  - Chat-Spike ja/nein
  - Chat-Volumen rund um das Event
  - optional Sentiment- oder Themenhinweis, wenn belastbar
- Die Ansicht zeigt die Datengrundlage an, mindestens `sessions_used`.
- Die Darstellung trennt klar zwischen:
  - aggregierter Verlauf
  - markierten Problemstellen
  - möglicher Erklärung
- Ohne ausreichende Viewer-Samples wird das Feature nicht als belastbare Diagnose dargestellt.

### 3. New-Chatter-Return-Funnel
- Es gibt eine neue Funnel-KPI für neue Chatter im gewählten Startfenster.
- Die Definition ist klar und in der UI dokumentiert:
  - `neu` = erster Chat-Kontakt im Startfenster
  - `return` = erneuter Chat-Kontakt innerhalb eines definierten Return-Fensters
- Das Return-Fenster ist für MVP fest definiert und sichtbar benannt.
- Das Feature zeigt mindestens:
  - Anzahl neuer Chatter
  - Anzahl zurückgekehrter Chatter
  - Return-Rate in Prozent
- Die KPI ist vom bestehenden `Wiederkehrende Chatters` klar abgegrenzt.
- Wenn die Historie für eine saubere Return-Bewertung nicht ausreicht, wird das deutlich markiert.

## Delivery-Plan

### Sprint 1
- Loyalitätskurve im Frontend auf vorhandenen Endpoint aufsetzen.
- One-Timer-Quote prominent in Chat- oder Viewer-Kontext platzieren.
- Copy und Tooltip-Text sauber definieren.

### Sprint 2
- Retention-Curve-Endpoint im Frontend anbinden.
- Drop-Events visualisieren.
- Ad-Break- und Chat-Hype-Kontext daneben anzeigen.

### Sprint 3
- New-Chatter-Return-Funnel fachlich definieren.
- Backend-Query für Kohorten-Return bauen.
- KPI und kurze Interpretation ins Dashboard bringen.

## Datenlage und Wiederverwendung

### Bereits vorhanden
- Loyalitätsverteilung auf Basis `twitch_chatter_rollup.total_sessions`.
- Aggregierte Retention-Kurve mit `drop_events`.
- Chat-Hype-Timeline mit Spike-Erkennung.
- Chat-Content-Analyse mit Themen- und Sentiment-Signalen.
- Rollup- und Session-Daten für `first_seen`, `last_seen`, `total_sessions`, `total_messages`.

### Neu zu bauen
- Saubere Kohortenlogik für den New-Chatter-Return-Funnel.
- UI-Komposition für die Retention-Drop-Diagnose.
- Einheitliche Copy für Lifetime vs Windowed Metrics.

## Edge Cases
- Rollup-Historie wurde erst kürzlich aufgebaut: Loyalitätskurve und Return-Funnel dürfen dann nicht zu optimistisch wirken.
- Wenige Sessions im Zeitraum: Retention-Drops und Funnel-Rates dürfen nicht wie hoch belastbare Trends dargestellt werden.
- Ad-Break ohne saubere Viewer-Samples: Event darf angezeigt werden, aber nicht als harte Ursache verkauft werden.
- Große Streams mit starkem Hype-Spam: Chat-Spikes dürfen nicht automatisch als positives Qualitätsmerkmal interpretiert werden.
- Neue Chatter am Ende des gewählten Zeitfensters haben naturgemäß weniger Zeit zur Rückkehr: Das Return-Fenster muss fachlich sauber fixiert oder explizit erklärt werden.
- Viewer ohne Chat-Nachricht dürfen nicht versehentlich in den New-Chatter-Return-Funnel einfließen.

## Annahmen
- `Loyalitätskurve` ist als Lifetime-Metrik sinnvoller als als `7/30/90`-Metrik.
- `Retention-Drop-Diagnose` ist vor allem für Session-Postmortems und Content-Optimierung gedacht.
- `New-Chatter-Return-Funnel` soll zunächst chatbasiert sein und nicht alle Video-Viewer abbilden.
- Für MVP ist ein streamer-interner Vergleich wertvoller als ein globales Twitch-Benchmarking.

## Empfehlung
- Zuerst `Loyalitätskurve`.
- Danach `Retention-Drop-Diagnose`.
- Danach `New-Chatter-Return-Funnel`.

Diese Reihenfolge liefert am schnellsten sichtbaren Mehrwert, nutzt vorhandene Daten am besten aus und minimiert fachliches Risiko.
