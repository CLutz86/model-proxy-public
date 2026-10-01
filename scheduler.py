"""Scheduler: entscheidet Starts/Abbrüche, pollt /metrics (2 s)."""
from __future__ import annotations

import asyncio
import logging
import time

import config
from queue_ import ABORTED_SENTINEL, PriorityQueue, QueuedRequest
from strata import StrataClient

log = logging.getLogger("proxy.scheduler")


class Scheduler:
    def __init__(self, strata: StrataClient) -> None:
        self.q = PriorityQueue()
        self.strata = strata
        self.running: QueuedRequest | None = None
        self.run_task: asyncio.Task | None = None
        self.last_high: float = 0.0          # monotonic; 0 = nie -> LOW darf sofort
        self.stats = {"high": 0, "low": 0, "done": 0, "aborted": 0,
                      "errors": 0, "dropped": 0, "started": 0,
                      "waits": []}
        self._loop: asyncio.Task | None = None

    # ---- Eingang -------------------------------------------------------
    def submit(self, req: QueuedRequest) -> None:
        if req.priority == config.HIGH:
            self.last_high = time.monotonic()
            self.stats["high"] += 1
        else:
            self.stats["low"] += 1
        self.q.push(req)
        log.info("ENQUEUE prio=%s seq=%s stream=%s queue=%d",
                 "HIGH" if req.priority == config.HIGH else "LOW",
                 req.seq, req.stream, len(self.q))

    # ---- Lifecycle -----------------------------------------------------
    def start(self) -> None:
        self._loop = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if self._loop:
            self._loop.cancel()

    # ---- Hauptschleife -------------------------------------------------
    async def _run_loop(self) -> None:
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # Schleife darf nie sterben
                log.exception("tick error: %s", exc)
            await asyncio.sleep(config.METRICS_POLL_S)

    async def _tick(self) -> None:
        now = time.monotonic()

        # 1) Unterbrechungs-Check für laufenden LOW im Prefill (Regel 5.2)
        if self.running is not None and self.running.priority == config.LOW \
                and self.running.stream \
                and not self.running.gate.is_set():
            # ABORT nur für streaming Requests. Non-streaming (OWM Research/Task,
            # stream=False) kann NICHT per SSE-Heartbeat gehalten werden und
            # würde bei Requeue das aiohttp-CLIENT_TIMEOUT (300s) reißen ->
            # TransferEncodingError. Solche Requests laufen immer fertig, der
            # Chat wartet dahinter.
            live = await self._live()
            if live:
                phase = live.get("phase")
                read, total = live.get("prompt_read"), live.get("prompt_total")
                if phase and phase != "reading the prompt":
                    self.running.gate.set()   # Generation läuft -> durchreichen
                elif phase == "reading the prompt" and read is not None and total:
                    # Option 2 (reuse-fest): Entscheidung über PREFILL-DAUER statt
                    # prompt_read/total. Prefix-Reuse (bis 99%) lässt progress
                    # sofort auf hohe Werte springen -> 85%-Schwelle wurde nie
                    # unterschritten -> Abort griff nie. Stattdessen: abbrechbar,
                    # solange die Prefill-Zeit unter 40% der erwarteten liegt.
                    rate = live.get("prefill_tok_s_mean") or 530.0
                    elapsed = live.get("elapsed_s") or 0
                    expected = (total / rate) if rate else 0
                    under_effort = expected <= 0 or elapsed < expected * 0.4
                    if self.q.has_priority(config.HIGH) and under_effort:
                        log.info("ABORT-TRIGGER seq=%s elapsed=%.0fs "
                                 "erwartet=%.0fs (%.0f%%) -> HIGH wartet",
                                 self.running.seq, elapsed, expected,
                                 100 * elapsed / expected if expected else 0)
                        self.run_task.cancel()
                        return
                    elif self.q.has_priority(config.HIGH):
                        log.info("NO-ABORT seq=%s elapsed=%.0fs "
                                 "erwartet=%.0fs -> HIGH wartet",
                                 self.running.seq, elapsed, expected)

        # 2) Start-Entscheidung (genau 1 aktiver Upstream-Request)
        if self.running is None and len(self.q):
            head = self.q.peek()
            # Client bereits weg -> nie senden, aus Queue entfernen
            if head.dropped:
                self.q.pop()
                self.stats["dropped"] += 1
                log.info("DROPPED (pre-start) seq=%s -> nie gesendet",
                         head.seq)
                return
            # Starvation-Schutz: LOW wartet > starvation_s -> einmalig wie HIGH
            if head.priority == config.LOW and head.waited() > config.STARVATION_S:
                log.info("AGING seq=%s waited=%.0fs -> HIGH-Prio", head.seq,
                         head.waited())
                head.priority = config.HIGH
                self.q.promote(head)
                head.gate.set()  # nicht mehr abortbar -> direkt streamen
            if head.priority == config.HIGH:
                self._start(self.q.pop())
            elif not self.q.has_priority(config.HIGH) \
                    and (now - self.last_high) >= config.IDLE_HOLD_S:
                log.info("deferred LOW seq=%s gestartet nach %s",
                         head.seq,
                         f"{now - self.last_high:.0f}s Ruhe"
                         if self.last_high else "kein Chat je")
                self._start(self.q.pop())
            # else: weiter halten (Szenario B)

    async def _live(self):
        m = await self.strata.get_json("/metrics")
        return (m or {}).get("live") if isinstance(m, dict) else None

    def _start(self, req: QueuedRequest) -> None:
        req.state = "RUNNING"
        self.running = req
        self.stats["started"] += 1
        if req.priority == config.HIGH:
            req.gate.set()  # HIGH wird nie abgebrochen -> sofort durchreichen
        self.run_task = asyncio.create_task(self._run(req))
        log.info("START prio=%s seq=%s waited=%.1fs",
                 "HIGH" if req.priority == config.HIGH else "LOW",
                 req.seq, req.waited())

    async def _run(self, req: QueuedRequest) -> None:
        outcome = await self.strata.run(req)
        self.running = None
        self.run_task = None
        if outcome == "aborted":
            if req.dropped:            # Client weg -> nicht neu einreihen
                req.state = "DONE"
                self.stats["dropped"] += 1
                return
            req.state = "HOLDING"
            self.stats["aborted"] += 1
            req.requeued.set()
            # arrival bleibt -> waited zählt weiter (Aging greift später)
            self.q.push(req, front=config.REQUEUE_AT_FRONT)
            if req.stream:
                req.chunks.put_nowait(ABORTED_SENTINEL)
        elif outcome == "done":
            req.state = "DONE"
            self.stats["done"] += 1
            if not req.stream and req.completed and not req.completed.done():
                req.completed.set_result(req.result)
        else:  # error
            req.state = "DONE"
            self.stats["errors"] += 1
            if not req.stream and req.completed and not req.completed.done():
                req.completed.set_exception(
                    req.error or RuntimeError("upstream error"))

    # ---- Client ist weg -------------------------------------------------
    def client_gone(self, req: QueuedRequest) -> None:
        req.dropped = True
        if self.q.remove(req):
            self.stats["dropped"] += 1
            log.info("DROPPED (queue) seq=%s", req.seq)
        elif self.running is req:
            log.info("DROPPED (running) seq=%s -> Upstream schließen", req.seq)
            if self.run_task:
                self.run_task.cancel()

    def snapshot(self) -> dict:
        return {
            "running": ({"seq": self.running.seq,
                         "prio": "HIGH" if self.running.priority == config.HIGH
                                 else "LOW",
                         "state": self.running.state,
                         "gate_open": self.running.gate.is_set()}
                        if self.running else None),
            "queued": len(self.q),
            "last_high_age_s": round(time.monotonic() - self.last_high, 1)
                              if self.last_high else None,
            "stats": self.stats,
        }
