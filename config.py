"""Parameter-Sektion 5.3 — alle Werte über PROXY_* Env übersteuerbar."""
import os


def _f(name: str, default: str) -> float:
    return float(os.getenv(name, default))


UPSTREAM_URL = os.getenv("PROXY_UPSTREAM", "http://127.0.0.1:1238")  # Base-URL ohne Pfad; /v1/... und /metrics werden angehängt
LISTEN_PORT = int(os.getenv("PROXY_PORT", "1240"))
REAL_MODEL = os.getenv("PROXY_REAL_MODEL", "REPLACE_WITH_UPSTREAM_MODEL_ID")
IDLE_HOLD_S = _f("PROXY_IDLE_HOLD_S", "120")          # Ruhe nach letztem Chat, bevor LOW startet
PREFILL_ABORT_THRESHOLD = _f("PROXY_PREFILL_ABORT", "0.85")
STARVATION_S = _f("PROXY_STARVATION_S", "1200")       # Aging: LOW wird bevorzugt
METRICS_POLL_S = _f("PROXY_METRICS_POLL_S", "2")
MAX_CLIENT_HOLD_S = _f("PROXY_MAX_CLIENT_HOLD_S", "540")  # harte Obergrenze HOLDING
CLIENT_HEARTBEAT_S = _f("PROXY_CLIENT_HEARTBEAT_S", "15")
REQUEUE_AT_FRONT = os.getenv("PROXY_REQUEUE_AT_FRONT", "1") == "1"
CHAT_SUFFIX = "-CHAT"
RESEARCH_SUFFIX = "-RESEARCH"
CODING_SUFFIX = "-CODING"

# Drei Prioritätsstufen: CHAT(0) erste Reihe, RESEARCH(1) darunter, CODING(2) nur Rest.
CHAT_PRIO, RESEARCH_PRIO, CODING_PRIO = 0, 1, 2
