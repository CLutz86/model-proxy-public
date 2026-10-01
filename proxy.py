"""Model-Proxy Entry Point: aiohttp-Server :1240.

Routes: /v1/chat/completions, /v1/models, /health, /metrics
Tagging: model endet auf -RESEARCH -> LOW, sonst HIGH.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

from aiohttp import web

import config
from queue_ import (ABORTED_SENTINEL, ERROR_SENTINEL, SENTINEL,
                    QueuedRequest)
from scheduler import Scheduler
from strata import StrataClient, UpstreamError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("proxy")

strata = StrataClient()
sched = Scheduler(strata)


def tag_priority(model: str) -> int:
    return config.LOW if (model or "").endswith(config.RESEARCH_SUFFIX) \
        else config.HIGH


# ---- /v1/chat/completions ------------------------------------------------
async def chat_completions(request: web.Request) -> web.StreamResponse:
    try:
        body = await request.json()
    except Exception as exc:
        ct = request.headers.get("Content-Type", "?")
        log.warning("BAD-JSON request from %s ct=%s len=%s: %s",
                    request.remote, ct, request.headers.get("Content-Length"),
                    exc)
        return web.json_response(
            {"error": {"message": "invalid JSON body"}}, status=400)

    model = body.get("model", "")
    prio = tag_priority(model)
    stream = bool(body.get("stream", False))
    req = QueuedRequest(priority=prio, body=body, stream=stream)
    if not stream:
        req.completed = asyncio.get_running_loop().create_future()
    sched.submit(req)

    if stream:
        return await _stream_response(request, req)
    return await _json_response(request, req)


async def _stream_response(request: web.Request,
                           req: QueuedRequest) -> web.StreamResponse:
    resp = web.StreamResponse(status=200, headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })
    await resp.prepare(request)
    hold_start = time.monotonic()
    gate_open = req.gate.is_set()
    try:
        while True:
            if not gate_open:
                # HOLDING: warte auf Gate, halte Client mit SSE-Kommentar offen
                try:
                    await asyncio.wait_for(req.gate.wait(),
                                           timeout=config.CLIENT_HEARTBEAT_S)
                except asyncio.TimeoutError:
                    if time.monotonic() - hold_start > config.MAX_CLIENT_HOLD_S:
                        payload = {"error": {
                            "message": "proxy hold limit reached; request "
                                       "still queued, retry later",
                            "type": "proxy_hold_timeout"}}
                        await resp.write(
                            b"data: " + json.dumps(payload).encode() + b"\n\n"
                            b"data: [DONE]\n\n")
                        sched.client_gone(req)
                        return resp
                    await resp.write(b": ping\n\n")
                    continue
                gate_open = True
                log.info("GATE-OPEN seq=%s nach %.1fs Hold",
                         req.seq, time.monotonic() - hold_start)
            # RUNNING/durchreichen: Chunks 1:1 weiterleiten
            item = await req.chunks.get()
            if item is SENTINEL:
                break
            if item is ABORTED_SENTINEL:
                # Abbruch: Gate war zu -> es wurden keine Tokens gesendet.
                # Ergebnis verwerfen, Request erneut halten (9.3 Idempotenz).
                req.requeued.clear()
                gate_open = False
                hold_start = time.monotonic()
                continue
            if item is ERROR_SENTINEL:
                err = req.error
                payload = {"error": {"message": str(err),
                                     "type": "upstream_error"}}
                await resp.write(b"data: " + json.dumps(payload).encode()
                                 + b"\n\ndata: [DONE]\n\n")
                return resp
            await resp.write(item)
        return resp
    except (ConnectionResetError, asyncio.CancelledError):
        log.info("client disconnected seq=%s", req.seq)
        sched.client_gone(req)
        raise
    finally:
        if req.state != "DONE":
            pass  # Scheduler behandelt Rest


async def _json_response(request: web.Request,
                         req: QueuedRequest) -> web.Response:
    assert req.completed is not None
    loop = asyncio.get_running_loop()
    deadline = loop.time() + config.MAX_CLIENT_HOLD_S + 4 * 3600
    status = None
    data = None
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            sched.client_gone(req)
            return web.json_response(
                {"error": {"message": "proxy wait limit reached",
                           "type": "proxy_wait_timeout"}}, status=504)
        try:
            status, data = await asyncio.wait_for(
                asyncio.shield(req.completed),
                timeout=min(remaining, 5.0))
            break
        except asyncio.TimeoutError:
            # aiohttp bricht Nicht-Streaming-Handler NICHT bei Client-
            # Disconnect ab (kein Write) -> Transport aktiv prüfen, sonst
            # bleibt der Request in der Queue und wird unnötig gesendet
            tr = request.transport
            if tr is None or tr.is_closing():
                log.info("client gone (poll) seq=%s -> aus Queue/Running",
                         req.seq)
                sched.client_gone(req)
                return web.json_response(
                    {"error": {"message": "client disconnected"}},
                    status=499)
        except asyncio.CancelledError:
            sched.client_gone(req)
            raise
        except UpstreamError as exc:
            return web.Response(status=exc.status, body=exc.body,
                                content_type="application/json")
        except Exception as exc:
            return web.json_response(
                {"error": {"message": f"upstream unreachable: {exc}",
                           "type": "upstream_unavailable"}}, status=503)
    if req.dropped:
        return web.json_response(
            {"error": {"message": "client gone"}}, status=499)
    return web.Response(status=status, body=data,
                        content_type="application/json")


# ---- /v1/models ----------------------------------------------------------
async def models(_request: web.Request) -> web.Response:
    log.info("MODELS-ABFRAGE von %s", _request.remote)
    m = await strata.get_json("/v1/models")
    names = []
    if isinstance(m, dict) and isinstance(m.get("data"), list):
        names = [d.get("id") for d in m["data"]]
    alias_ids = [config.REAL_MODEL,
                 config.REAL_MODEL + "-CHAT",
                 config.REAL_MODEL + "-RESEARCH"]
    data = [{"id": i, "object": "model", "owned_by": "strata"}
            for i in alias_ids]
    return web.json_response({"object": "list", "data": data})


# ---- /health, /metrics ---------------------------------------------------
async def health(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "running":
                              sched.running is not None,
                              "queued": len(sched.q)})


async def proxy_metrics(_request: web.Request) -> web.Response:
    return web.json_response(sched.snapshot())


async def _startup(_app: web.Application) -> None:
    sched.start()


async def _cleanup(_app: web.Application) -> None:
    await sched.stop()
    await strata.close()


def main() -> None:
    app = web.Application(client_max_size=64 * 1024 * 1024)
    # aiohttp>=3.10: Handler-Cancellation ist Standard
    app.router.add_post("/v1/chat/completions", chat_completions)
    app.router.add_get("/v1/models", models)
    app.router.add_get("/health", health)
    app.router.add_get("/metrics", proxy_metrics)
    app.on_startup.append(_startup)
    app.on_cleanup.append(_cleanup)
    log.info("Model-Proxy startet auf :%s -> %s (idle_hold=%ss, "
             "abort<%.2f, starvation=%ss, max_hold=%ss)",
             config.LISTEN_PORT, config.UPSTREAM_URL, config.IDLE_HOLD_S,
             config.PREFILL_ABORT_THRESHOLD, config.STARVATION_S,
             config.MAX_CLIENT_HOLD_S)
    web.run_app(app, host="0.0.0.0", port=config.LISTEN_PORT,
                access_log=log, print=None,
                access_log_format='%r %s %b "%{User-Agent}i" from %a')


if __name__ == "__main__":
    main()
