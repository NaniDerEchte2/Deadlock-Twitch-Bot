# Chat-Netzwerk

Stand: `2026-03-14`

## Status

Das Feature `Chat-Netzwerk` beziehungsweise `chat_social_graph` ist im Code weiterhin vorhanden, aber im Streamer-Dashboard aktuell bewusst deaktiviert.

## Hintergrund

Die bisherige Surface visualisiert vor allem:

- `@mentions` insgesamt
- Conversation-Hubs
- Top-Gespraeche zwischen Usern
- Mention-Verteilung

Technisch bleibt die Implementierung in Frontend und API erhalten. Sie wird derzeit nur nicht mehr im Tab `chat` ausgespielt.

## Warum deaktiviert

Der praktische Nutzen fuer Streamer ist im aktuellen Produkt zu gering im Verhaeltnis zu Platz, Komplexitaet und Erklaerungsaufwand im Dashboard.

Konkret:

- Der Block liefert selten direkt umsetzbare Entscheidungen fuer den Stream-Alltag.
- Andere Chat-Module wie Loyalitaet, Hype-Momente, Themen und Tageszeiten sind fuer Streamer deutlich relevanter.
- Die Netzwerkansicht erzeugt Erklaerungsbedarf, ohne in der Regel eine klare Produktentscheidung oder Optimierung auszuloesen.

## Reaktivierung

Die React-Implementierung bleibt absichtlich bestehen, damit das Feature bei spaeterem Bedarf schnell wieder aktiviert werden kann.

Aktuell erfolgt die Deaktivierung nur ueber ein lokales Frontend-Flag in:

- `bot/dashboard_v2/src/pages/ChatAnalytics.tsx`

Bei einer spaeteren Reaktivierung sollte vor dem Re-Enable erneut geprueft werden:

- Gibt es einen klaren Streamer-Use-Case?
- Lassen sich daraus konkrete Handlungsempfehlungen ableiten?
- Ist die Surface gegenueber den anderen Chat-Modulen prioritaetswuerdig?
