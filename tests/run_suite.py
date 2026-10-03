#!/usr/bin/env python3
"""Startet Mock-Strata (19999) + Proxy (1241) als Subprozesse,
fuehrt test_scenarios.py aus und beendet beide wieder.
Reine Logik -> KEIN echtes Modell.
"""
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def wait_port(port: int, timeout: float = 15) -> bool:
    import socket
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def main() -> int:
    env = dict(os.environ, MOCK_MODEL="test-model",
               PROXY_REAL_MODEL="test-model",
               PROXY_UPSTREAM="http://127.0.0.1:19999",
               PROXY_PORT="1241", PROXY_IDLE_HOLD_S="5",
               PROXY_STARVATION_S="15", PROXY_PREFILL_ABORT="0.4")
    procs = []
    try:
        mock = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "mock_strata.py")],
            env=env, cwd=ROOT, stdout=open("/tmp/mock_strata.log", "w"),
            stderr=subprocess.STDOUT)
        procs.append(mock)
        if not wait_port(19999):
            print("FAIL Mock-Strata nicht erreichbar")
            return 1
        proxy = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "..", "proxy.py")],
            env=env, cwd=ROOT, stdout=open("/tmp/proxy_1241.log", "w"),
            stderr=subprocess.STDOUT)
        procs.append(proxy)
        if not wait_port(1241):
            print("FAIL Proxy nicht erreichbar")
            return 1
        print("Mock(19999)+Proxy(1241) bereit, starte Test-Suite ...\n")
        return subprocess.call(
            [sys.executable, os.path.join(HERE, "test_scenarios.py")],
            env=env, cwd=ROOT)
    finally:
        for p in procs:
            p.terminate()
        time.sleep(1)
        for p in procs:
            if p.poll() is None:
                p.kill()


if __name__ == "__main__":
    sys.exit(main())