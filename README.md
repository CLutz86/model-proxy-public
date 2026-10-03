# Model-Proxy — Priorisierte Request-Queue vor Strata

OpenAI-kompatibler Proxy (Port **1240**), der parallele Anfragen von
OpenWebUI/LibreChat auf einen einzelnen Strata-Slot (Port 1238,
`total_slots: 1`, FIFO) in **drei Prioritätsstufen** führt:
**Chat sofort, Research danach, Coding nur wenn nichts anderes ansteht**.

## Kernverhalten

| Regel | Verhalten |
|---|---|
| Tagging | `model`-Suffix → Prio: `-CHAT`=P0 (sofort), `-RESEARCH`=P1, `-CODING`=P2 (nur Rest); ohne Suffix = CHAT |
| Priorität | P0 CHAT startet immer sofort und wird nie abgebrochen; P1 RESEARCH vor P2 CODING |
| Abort-Hierarchie | CHAT bricht laufenden RESEARCH/CODING im Prefill, RESEARCH bricht CODING (nur **streaming**) |
| Remapping | Upstream sieht immer den echten Strata-Modellnamen |
| Hold | P1/P2 starten erst nach `idle_hold_s` (Default 120 s) Ruhe seit letztem CHAT |
| Abort | höhere Prio während Prefill (nur **streaming**): abbrechbar, solange Prefill-Zeit < 40 % der erwarteten (erwartet = `prompt_total / prefill_tok_s_mean`). Upstream-Disconnect = Strata-Cancel, verdrängter Request zurück an Queue-Anfang |
| Kein Abort | Prefill-Zeit ≥ 40 % der erwarteten oder Generation läuft (`phase != "reading the prompt"`); non-streaming wird nie abgebrochen |
| Aging | P1/P2 wartet > `starvation_s` (1200 s) → wird wie CHAT behandelt |
| Client-Hold | SSE `: ping` alle 15 s; hartes Limit `max_client_hold_s=540` |
| Scheduler | genau 1 aktiver Upstream-Request (Strata hat 1 Slot) |

## Module

```
proxy.py     – aiohttp-Server :1240, Routes /v1/chat/completions, /v1/models, /health, /metrics
queue_.py    – Prioritäts-Heap + Zustandsverwaltung (HOLDING/RUNNING/ABORTED/DONE)
scheduler.py – asyncio-Task: Starts/Abbrüche, pollt /metrics (2 s)
strata.py    – Upstream-Client: streamende Weiterleitung, SSE, Cancel via close
config.py    – alle Parameter, über PROXY_* Env übersteuerbar
```

## Parameter (Env)

| Env | Default | Bedeutung |
|---|---|---|
| `PROXY_UPSTREAM` | `http://127.0.0.1:1238` | Strata-Base-URL |
| `PROXY_PORT` | `1240` | Listen-Port |
| `PROXY_REAL_MODEL` | `REPLACE_WITH_UPSTREAM_MODEL_ID` | Upstream-Modellname |
| `PROXY_IDLE_HOLD_S` | `120` | Ruhe nach letztem Chat, bevor LOW startet |
| `PROXY_PREFILL_ABORT` | `0.4` | Abort-Schwelle: Anteil der erwarteten Prefill-Zeit (reuse-fest) |
| `PROXY_STARVATION_S` | `1200` | Aging |
| `PROXY_METRICS_POLL_S` | `2` | /metrics-Pollintervall |
| `PROXY_MAX_CLIENT_HOLD_S` | `540` | hartes Hold-Limit (< Client-Timeout) |
| `PROXY_CLIENT_HEARTBEAT_S` | `15` | SSE-Ping-Intervall |
| `PROXY_REQUEUE_AT_FRONT` | `1` | abgebrochene LOW wieder an Queue-Anfang |

## Betrieb (systemd-user)

```ini
# ~/.config/systemd/user/model-proxy.service
[Service]
ExecStart=/usr/bin/python3 /pfad/zu/proxy.py
Environment=PROXY_UPSTREAM=http://127.0.0.1:1238
Restart=on-failure
```

`systemctl --user enable --now model-proxy.service` (Linger aktivieren,
damit der Service ohne offene Session läuft).

## OpenWebUI-Anbindung

- Drei Modell-Aliase registrieren (alle Base-URL → Proxy):
  `<Modell>-CHAT` (P0), `<Modell>-RESEARCH` (P1) und `<Modell>-CODING` (P2)
- OWM-Registrierung läuft über die config-Tabelle `openai.api_base_urls`
  (JSON-Array, Werte immer `json.dumps`-encodiert), nicht nur über das Env
- Task- (Research-) Pfad automatisch P1: `task.model.default` → RESEARCH-Alias;
  Coding-/Agent-Pfad → CODING-Alias (P2)
- Non-streaming-Timeouts erhöhen: `AIOHTTP_CLIENT_TIMEOUT=3600`,
  `AIOHTTP_CLIENT_STREAM_IDLE_TIMEOUT=600` (Default 300 s sprengt lange
  Recherche-Prefills)

## Tests

```bash
# Suite (Mock-Strata :19999 + Proxy :1241, reinigt Prozesse selbst):
#   wartet auf Ports, startet Mock+Proxy, läuft test_scenarios.py, räumt auf.
PROXY_REAL_MODEL=test-model python3 tests/run_suite.py
```
Oder manuell: `tests/mock_strata.py` (19999) + Proxy gegen `PROXY_UPSTREAM=
http://127.0.0.1:19999 PROXY_PORT=1241 PROXY_IDLE_HOLD_S=5
PROXY_STARVATION_S=15`, dann `python3 tests/test_scenarios.py`
**20 Checks** (Tagging 3-Prio, Hold, Abort-Hierarchie CHAT>RESEARCH>CODING,
kein Abort >=40 %/Generation, Heartbeats, Disconnect, Requeue-Stream, Aging,
Stats).

Live gegen echtes Strata: `tests/test_live_strata.py` (Szenario C),
`tests/test_live_D.py` (Szenario D). Hinweis: Test-Prompts brauchen
durchgehenden Zufallsinhalt, sonst frisst der KV-Cache der Engine den
Prefill (`reused ≈ 100 %`).

## Grenzen (bewusst)

- Kein Multi-Slot, kein Preempting mitten in der Generation
- Prefill-Abort = gesamte Prefill-Arbeit verfällt (kein Persistenz-Cache)
- Non-streaming-Clients mit Wartezeit > Timeout sehen 504 — dokumentierte
  Grenze; Hold-Limit bleibt unter Client-Timeout
- Proxy lauscht ohne Auth — nur im vertrauenswürdigen Netz betreiben
