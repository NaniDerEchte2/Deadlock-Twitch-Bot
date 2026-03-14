# Chat Penetration

Stand: `2026-03-14`

## Status

Die Metrik `Chat Penetration` ist im Code weiterhin vorhanden, wird im Streamer-Dashboard aktuell aber bewusst nicht angezeigt.

## Hintergrund

Die Kennzahl beschreibt im aktuellen Modell vereinfacht:

- aktive Chatters
- geteilt durch getrackte Chat-Accounts

Sie basiert also nicht auf allen Video-Viewern, sondern auf der erfassten Chat-Audience.

Technisch bleiben Berechnung und API-Felder erhalten. Deaktiviert ist aktuell nur die sichtbare Darstellung im Frontend.

## Warum deaktiviert

Der praktische Mehrwert ist im aktuellen Produkt zu gering und die Metrik ist fuer viele Streamer schwer intuitiv zu lesen.

Konkret:

- Die Kennzahl wird leicht als "Anteil aller Viewer, die schreiben" missverstanden, obwohl sie das nicht misst.
- Fuer den Stream-Alltag liefert sie selten klarere Entscheidungen als `Aktive Chatters`, `Messages pro 100 Viewer-Minuten` und `Wiederkehrende Chatters`.
- Der Erklaerungsaufwand ist hoch, waehrend der operative Nutzen im Dashboard eher niedrig bleibt.

## Reaktivierung

Die sichtbare Deaktivierung erfolgt aktuell ueber ein lokales Frontend-Flag in:

- `bot/dashboard_v2/src/pages/ChatAnalytics.tsx`

Bei einer spaeteren Reaktivierung sollte erneut geprueft werden:

- Ist die Metrik im UI ohne lange Erklaerung sofort verstaendlich?
- Liefert sie eine konkrete, von anderen Chat-KPIs klar abgegrenzte Entscheidungshilfe?
- Soll sie als Haupt-KPI erscheinen oder eher als optionale Sekundaer-Metrik?
