# Model-Proxy Spezifikation — Priorisierte Request-Queue vor Strata

Stand: 2026-10-01 · Ziel: Umsetzungsfertiges Dokument für den OpenAI-kompatiblen Prioritäts-Proxy vor Strata (Port 1238)

---

## 1. Ziel & Kontext

**Problem:** Strata hat genau **1 Slot** (`total_slots: 1`, FIFO, `threading.Lock()` in server.py Z.624). Recherche-Sub-Agents (OpenWebUI-Task-Pfad) senden Requests mit 200k–263k Tokens Kontext; ein Prefill dauert bei ~530 tok/s **7–12 min**, die Runde selbst 400–600 s. Mehrere Nutzer über OpenWebUI warten dadurch bei parallelen Recherchen **Stunden** auf Chat-Antworten.

**Lösung:** Ein OpenAI-kompatibler Proxy zwischen OpenWebUI und Strata, der:
1. Requests **taggt** (Chat = sofort, Recherche = warten)
2. Recherche-Requests erst sendet, wenn **X Minuten keine Chat-Nachricht** einging
3. Einen **laufenden Recherche-Prefill abbricht**, wenn er noch am Anfang steht (Prefill < 85 %) — der Client merkt nichts, bekommt die Antwort einfach später
4. Laufende Requests **nicht** unterbricht, wenn der Prefill fast fertig ist oder die Generation bereits läuft

**Nicht-Ziele:** Kein Multi-Slot, kein echtes Preempting mitten in der Generation, keine Änderung an Strata selbst (Server bleibt unberührt), kein Modell-Switching.

---

## 2. Architektur

```
OpenWebUI (Port 3004)
   │  Base-URL auf Proxy: http://<proxy>:1240/v1
   ▼
┌──────────────────────────────┐
│   Model-Proxy (Port 1240)    │  Python 3.11+ / asyncio
│  ┌────────────────────────┐  │
│  │ Tagging (model-Feld)   │  │  <MODEL>-CHAT      → Prio HIGH (sofort)
│  │                        │  │  <MODEL>-RESEARCH  → Prio LOW  (halten)
│  └────────────────────────┘  │
│  ┌────────────────────────┐  │
│  │ Prioritäts-Queue (Heap)│  │  (priority, arrival_time, seq, request)
│  └────────────────────────┘  │
│  ┌────────────────────────┐  │
│  │ Scheduler (1 aktiv)    │  │  immer genau 1 aktiver Upstream-Request an Strata
│  └────────────────────────┘  │
│  ┌────────────────────────┐  │
│  │ Strata-Client          │  │  -> http://<strata-host>:1238/v1/chat/completions
│  │ + Prefill-Monitor      │  │     Poll GET /metrics alle 2 s (prompt_read/prompt_total)
│  │ + Cancel-Mechanik      │  │     Upstream-Verbindung schließen = Abbrechen
│  └────────────────────────┘  │
└──────────────────────────────┘
   │
   ▼
Strata (<strata-host>:1238, unverändert)
```

**Betriebsort des Proxys:** Lokal auf demselben Host wie OpenWebUI (Docker-Container `open-webui`, Port 3004). Strata läuft auf einem separaten Rechner im LAN, Konfiguration über `PROXY_UPSTREAM`.

---

## 3. Strata-Fakten (belegt, für die Implementierung verbindlich)

### 3.1 Endpoints
| Endpoint | Zweck |
|---|---|
| `GET /health` | `{"status":"ok","max_context":524288,"model":"<MODEL>","loaded":true}` |
| `GET /status` | `{"busy":bool,"queued":int,"phase":"reading the prompt"/"answering","prompt_tokens":N,"generated":N,"max_tokens":N}` |
| `GET /metrics` | **live-Prefill-Fortschritt**: `live.prompt_read`, `live.prompt_total`, `live.phase`, Engine-Daten, `requests[]`, `totals` |
| `GET /props` | `default_generation_settings.n_ctx: <max_context>`, `total_slots: 1` |
| `POST /v1/chat/completions` | OpenAI-kompatibel |
| `POST /v1/messages` | Anthropic-kompatibel |

### 3.2 Prefill-Fortschritt (Kernstück der Abruch-Entscheidung)
`GET /metrics` liefert unter `live`:
```json
{"state":"reading","phase":"reading the prompt","prompt_tokens":217695,
 "prompt_read":71983,"prompt_total":217695,"generated":0,
 "max_tokens":46297,"elapsed_s":143.2,"prefill_tok_s_mean":530.4}
```
- **Fortschritt = `prompt_read / prompt_total`** (0.0–1.0)
- Wenn `phase` nicht mehr `"reading the prompt"` ist → Prefill fertig, Generation läuft → **nicht mehr abbrechen**
- `prompt_total` = Kontext des aktuellen Request; bei Chat-Requests klein (58–1150), bei Recherche groß (200k–263k)

### 3.3 Cancel-Mechanik (belegt server.py)
- Es gibt **keinen** HTTP-/cancel-Endpoint
- **Abbruch = Upstream-Verbindung schließen:** Strata prüft `cancel.is_set()` (Client-Disconnect) → `finish="cancel"`, laufende Tools werden gestoppt (Z.1200: „the client is gone: stop the tool too")
- Abbrechen eines laufenden Prefills: **gesamte Prefill-Arbeit verfällt** (`reused: 0` bestätigt: neue Prompts haben keinen KV-Reuse; kein Persistenz-Cache, conversation_cache_mib: 0)
- Nach Abbruch: Server bleibt stabil, nächster Request startet normal

### 3.4 Streaming-Verhalten
- `POST /v1/chat/completions` mit `"stream": true` → Server-Sent-Events (SSE), Fortschritt als keep-alives (Z.362–378: PP-Zeilen senden Heartbeats, sonst lange Prompts ohne Keep-Alive)
- **Implikation für Queue-Halten:** Der Proxy kann gehaltene Requests **nicht** ans Modell schicken und dann einfach pausieren — er muss sie **ganz zurückhalten** (nichts an Strata senden), bis zur Sendung. Während des Haltens: Client-Verbindung offen halten.

### 3.5 Client-Timeouts (die harte Grenze)
- **OpenAI-Python-SDK (OWM-Backend):** `DEFAULT_TIMEOUT = httpx.Timeout(timeout=600, connect=5.0)` → **10 min ohne Response-Bytes = Timeout beim Client**
- **Streaming:** Read-Timeout wird durch jede ankommende SSE-Zeile zurückgesetzt → Proxy kann mit `: ping`-Heartbeats **beliebig lange** halten **aber nur**, wenn die Antwort streamt
- **Nicht-Streaming (OWM-Task/Sub-Agent-Pfad, generate_follow_ups):** keine Heartbeats möglich → gehaltene Wartezeit + Prefill muss **unter 600 s** bleiben, sonst Fehler beim Client. **Lösung:** Timeout im OWM-Recherche-Aufruf erhöhen (Config/Env am Client) — dokumentieren, Proxys „client-hold"-Zeit begrenzen.

---

## 4. Tagging-Konzept (OpenWebUI-seitig)

**Zwei Modell-Aliase in OWM anlegen, beide zeigen auf den Proxy (Port 1240):**

| OWM-Modell-Alias | Prio | Verwendung |
|---|---|---|
| `<MODEL>-CHAT` | HIGH (0) | normale Chat-Nachrichten |
| `<MODEL>-RESEARCH` | LOW (1) | Recherche-/Task-/Sub-Agent-Verwaltung |

**Proxy-Tagging:** Liest `model` aus Request-Body. Endet es auf `-RESEARCH` → LOW, sonst HIGH. Vor Weiterleitung an Strata: Model-Feld auf den echten Strata-Namen `<MODEL>` mappen (Strata lehnt unbekannte Modellnamen ab, 404).

**OWM-Config (config-Tabelle der OpenWebUI-DB):**
- `task.model.default` → `<MODEL>-RESEARCH` (damit Sub-Agents/Tasks automatisch getaggt)
- `ui.default_models` / `ui.default_pinned_models` → `<MODEL>-CHAT`
- WICHTIG: Werte immer **JSON-encodiert** schreiben (json.dumps)! Rohe Strings brechen OWM (JSONDecodeError beim Start, 500er).
- Parallel betriebene Zusatz-Modelle unangetastet lassen.

**Ohne OWM-Eingriff möglich (Fallback):** Proxy taggt nach `user`-Feld oder HTTP-Header — aber 2 Modell-Einträge sind der sauberste Weg.

---

## 5. Queue-Logik (Scheduler)

### 5.1 Zustände im Proxy
```
HOLDING     – Request eingegangen, in Queue, noch nicht an Strata gesendet
RUNNING     – genau 1 Request wird an Strata gestreamt
ABORTED     – RUNNING war LOW, Prefill < 85 %, Chat kam rein → Upstream geschlossen,
              Request wandert zurück an Queue-Anfang (HOLDING) oder komplett ans Ende (konfigurierbar)
DONE        – Antwort komplett an Client geliefert
```

### 5.2 Scheduler-Regeln (Priorität 1 = höchste)
1. **Prioritäts-Heap:** `(priority, arrival_seq, request)` — HIGH immer vor LOW, innerhalb gleicher Prio FIFO (arrival_seq)
2. **Genau 1 aktiver Upstream-Request** an Strata (1 Slot!)
3. **Startregel für LOW (Recherche):** nur senden, wenn (a) kein HIGH in Queue und (b) **kein Chat** in den letzten `idle_hold_s` Sekunden eingegangen (Timer läuft ab letzter HIGH-Ankunft)
4. **Unterbrechung (nur Streaming; reuse-fest über Prefill-Dauer):** Kommt HIGH, während LOW **im Prefill** ist (`phase == "reading the prompt"`), und der LOW ist ein **Streaming-Request**:
   - `elapsed < 0.4 × erwarteter Prefill-Zeit` (erwartet = `prompt_total / prefill_tok_s_mean`) → **Abbruch** (Upstream schließen), LOW in Queue zurück (vorne)
   - sonst (`≥ 40 %` oder `phase != reading`) → **laufen lassen**, HIGH wartet (Queue)
   - **Non-streaming LOW wird nie abgebrochen** — er kann nicht per SSE-Heartbeat gehalten werden; ein Requeue würde das Client-Timeout reißen (`TransferEncodingError`). Er läuft immer fertig, HIGH wartet dahinter.
5. **HIGH + Strata frei** → sofort senden
6. **Starvation-Schutz (Aging):** LOW wartet > `starvation_s` (z. B. 1200 s) → temporär wie HIGH behandeln (einmalig)

### 5.3 Parameter (Konfig, Defaults)
| Param | Default | Bedeutung |
|---|---|---|
| `upstream_url` | `http://<strata-host>:1238/v1` | Strata |
| `listen_port` | `1240` | Proxy-Port |
| `real_model` | `<MODEL>` | Modellname für Upstream |
| `idle_hold_s` | 120 | Wartezeit PURO nach letztem Chat, bevor LOW startet |
| `prefill_abort_threshold` | 0.4 | Anteil der erwarteten Prefill-Zeit als Abort-Schwelle (reuse-fest, nur Streaming) |
| `starvation_s` | 1200 | Aging: LOW wird nach dieser Wartezeit bevorzugt |
| `metrics_poll_s` | 2 | Poll-Intervall /metrics |
| `max_client_hold_s` | 540 | harte Obergrenze für HOLDING (s. 3.5, muss < Client-Timeout 600) |
| `client_heartbeat_s` | 15 | SSE-Keepalive-Intervall an den Client (nur Streaming) |

---

## 6. Ablauf-Szenarien

### Szenario A: Chat kommt, keine Recherche läuft
Chat → tag HIGH → Strata frei → sofort senden, streamen, fertig. Keine Verzögerung.

### Szenario B: Recherche wartet (HOLDING), Chat kommt dazwischen
Recherche eingetroffen (LOW) → Timer startet → Chat (HIGH) kommt → Timer **reset** → 120 s nach letztem Chat ohne weitere Chats: Recherche wird gesendet.

### Szenario C: Recherche läuft im Prefill (20 % der erwarteten Dauer), Streaming, Chat kommt
Chat (HIGH) → Scheduler prüft laufenden LOW (streaming): `elapsed = 0.2 × erwartete Prefill-Zeit < 0.4` → Upstream schließen → LOW zurück an Queue-Start → Chat senden → danach (120 s Ruhe) Recherche **komplett neu** (Prefill-Verlust akzeptiert).

### Szenario D: Recherche fast fertig mit Prefill (95 % der erwarteten Dauer), Chat kommt
`0.95 ≥ 0.4` → **nicht abbrechen** → Chat wartet ~Rest-Prefill + Generation (kurz) → Chat bekommt Slot.

### Szenario E: Recherche generiert bereits (phase = answering)
Nicht abbrechen (Generation unterbrechen = Teilantwort verlieren) → Chat wartet bis `DONE`.

---

## 7. Client-Seite: „Client merkt nichts"

### Streaming (Chat-Pfad)
- Proxy hält die Client-SSE-Verbindung offen, sendet alle `heartbeat_s` ein SSE-Kommentar `: ping` (kein Daten-Chunk) → Read-Timeout beim Client resettet → **Client wartet unbegrenzt, keine Fehlermeldung**
- Sobald der Upstream-Request startet: Bytes 1:1 durchreichen

### Nicht-Streaming (OWM-Task/Recherche-Pfad)
- **Keine** Heartbeats möglich (HTTP wartet auf die komplette JSON-Antwort)
- Gesamtwartezeit des Clients = (HOLDING-Zeit) + (Strata-Bearbeitung) muss < Client-Timeout (OWM: 600 s Default)
- **Konsequenz (belegt):** Recherche mit 200k+ Kontext sprengt das allein schon (Prefill 7–12 min = 420–720 s). Daher **zwingend:** OWM-Recherche-Timeout erhöhen (z. B. `timeout=3600` im OpenAI-Client des Task-Pfads) ODER Recherche-Requests **streaming erzwingen** (Proxy kann `stream=true` zum Upstream verwenden und den Client trotzdem json liefern) — Empfehlung: **beides dokumentieren, Timeout-Erhöhung umsetzen**, da OWM-Task-Pfad non-streaming ist.
- Folge: Bei sehr langen Recherchen + Unterbrechung kann der Client trotzdem einen Timeout sehen — das ist die dokumentierte Grenze („höchstens irgendwann ein Timeout").

---

## 8. Tavily-Kosten (belegt — kein doppeltes Bezahlen)

- `search_web`/`fetch_url` sind **OWM-Builtin-Tools** (tools/builtin.py) → OWM ruft Tavily auf, **bevor** der Request zu Strata geht; Ergebnisse liegen als Message/Sub-Agent-Ergebnis **im Request-Kontext**
- Strata führt keine Web-Tools aus (`tool: null` im Status, kein MCP-Hub konfiguriert)
- **Abbruch + Neustart desselben Request-Bodys → keine neuen Tavily-Credits** (die Suche wurde schon bezahlt, Ergebnisse stecken im Body, den der Proxy wieder sendet)
- Ein Tavily-Ergebnis-Cache wäre nur nötig, wenn **dieselbe Suche in verschiedenen Recherchen** mehrfach liefe — dann OWM-seitig (Redis um `search_web`), **nicht** Proxy-Aufgabe

---

## 9. Technische Umsetzung

### 9.1 Stack
- Python 3.11+ / **asyncio** (keine Threads nötig — Strata selbst serialisiert ohnehin)
- http.server-Stil vermeiden → **aiohttp** (Client: `aiohttp.ClientSession` mit `read_timeout=None` gegen Strata!) 
  oder FastAPI/uvicorn, wenn Logging/Middleware gewünscht. Empfehlung: **aiohttp pur** (kein Framework-Overhead, ~150 Zeilen)
- Abhängigkeiten: `aiohttp` nur. Kein ORM, keine DB.

### 9.2 Module
```
proxy.py            – Entry Point: aiohttp-Server :1240, Routes /v1/chat/completions, /v1/models, /health
queue.py            – Prioritäts-Heap + Zustandsverwaltung (HOLDING/RUNNING/ABORTED/DONE)
scheduler.py        – asyncio-Task: entscheidet Starts/Abbrüche, pollt /metrics (2 s)
strata.py           – Upstream-Client: streamende Weiterleitung, SSE-Parsing, Heartbeat-Erzeugung
config.py           – Parameter-Sektion 5.3, CLI/Env übersteuerbar (PROXY_*)
```

### 9.3 Wichtige Implementierungsdetails
- **/v1/models**: an den Client den Strata-Inhalt liefern, aber Model-Namen durch die zwei Aliase ersetzen (sonst zeigt OWM den echten Namen als nicht-auswählbar an)
- **SSE-Durchreichung:** Strata-Chunks (Chunk-Parsing `data: …`) 1:1 weiterreichen; bei STRATA-Abbruch: Ergebnis verwerfen, Request in Queue zurück
- **Cancellation Upstream:** aiohttp-Request-Task `cancel()` bzw. Session-Close → Strata erkennt Disconnect
- **Clients, die disconnecten** (Browser zu, Nutzer weg): Upstream genauso schließen (wie Strata es selbst tut) — Request aus Queue entfernen
- **Fehlerpfade:** Strata `503 EngineDied` → an Client durchreichen (einmal), nächster Request startet Engine neu (Strata-Verhalten); Strata-down (connection refused): Client 503 + keine Queue-Poisonierung
- **Idempotenz:** Recherche-Requests nach Abbruch unverändert neu senden (gleicher Body inkl. gleicher `messages`), KEINE Token/Streams an Client weiterreichen, die vor dem Abbruch ankamen

### 9.4 Betrieb
- systemd-User-Unit `model-proxy.service` (`ExecStart=/usr/bin/python3 …/proxy.py`, Restart=on-failure) auf dem Betriebshost
- Log: stdout/Journal; jeder Request mit Tag/Prio/Zeiten loggen (für spätere Analyse: „wie lange wartete ein Chat?")

---

## 10. Offene Punkte / Entscheidungen vor Umsetzung

1. **OWM-Recherche-Timeout erhöhen** — wo genau (middleware.py OpenAI-Client, Env `OPENAI_TIMEOUT` o. ä.)? Muss vor dem Live-Betrieb geklärt werden, sonst sehen Nutzer bei >10-min-Recherchen Timeouts. (Konfig suchen beim Implementieren.)
2. **ABORTED-Reihenfolge:** abgebrochene Recherche wieder an Queue-**Anfang** (fair: sie war zuerst da) oder **Ende**? Default: Anfang (Antwort), konfigurierbar.
3. **Mehrere LOW-Hintereinander rauschen durch:** Nach einem LOW-Start, der von HIGH unterbrochen wurde, erneut 120-s-Ruhe abwarten oder sofort? Default: erneut warten (konsistent).
4. **Conversation-Cache (#189)**: könnte Abbrüche fast gratis machen (Prefill-Reuse über Requests) — `conversation_cache_slots: 4` existiert, ist aber 0 MiB. Prüfen, ob Aktivierung sinnvoll ist (separates Ticket; nicht Blockade für Proxy).
5. **Metrics-Dashboard:** optionaler `/metrics`-Endpoint am Proxy (Anzahl HOLDING/ABORTED, Wartezeiten) für Observability.

---

## 11. Testplan (vor Produktiv)

1. **Tagging:** Request mit `model=X-CHAT` und `X-RESEARCH` → korrekte Prio-Zuordnung, /v1/models zeigt beide Aliase
2. **Chat sofort:** HIGH bei freiem Strata → < 1 s Verzögerung, identische Antwort wie ohne Proxy
3. **Hold-Logik:** LOW senden, 5 s später HIGH senden → Strata-Engine bekommt LOW erst 120 s nach HIGH (Log: „deferred")
4. **Abbruch im Prefill:** LOW mit 200k-Prompt starten, 30 s später HIGH → /metrics zeigt prompt_read ~15 % → Upstream wird geschlossen → HIGH antwortet sofort → LOW läuft danach neu (Prefill von vorn)
5. **Kein Abbruch bei 90 %:** LOW mit großem Prompt, HIGH erst bei prompt_read > 90 % → kein Cancel, HIGH wartet
6. **Client-Verhalten:** während HOLDING sendet Proxy `: ping` → Client (OWM) zeigt „Antwort wird generiert", kein Fehler
7. **Strata-down:** Proxy liefert 503 an Client, Queue bleibt konsistent, nach Strata-Start läuft alles normal
8. **Remapping:** Upstream sieht nur `<MODEL>` (nie die Aliase)

---

## 12. Nachweisquellen (für Implementierung referenziert)

- Strata server.py: FIFO Z.624, Cancel Z.1026–1027/1200, Heartbeats Z.362–378, Routen Z.1427–1552
- Strata /metrics live: `prompt_read`, `prompt_total`, `phase`, `prefill_tok_s_mean`
- OWM openai-SDK Default-Timeout 600 s (`/usr/local/lib/python3.11/site-packages/openai/_constants.py:9`)
- OWM DB-Config: Werte JSON-encodiert (`json.dumps`) — siehe config-Tabelle
- Strata-Doku DETAILS.md:517–535 (YaRN) — Kontext-Decke `finish:"length"` bei 263,6k beobachtet