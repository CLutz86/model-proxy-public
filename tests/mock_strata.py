"""Mock-Strata für Tests: simuliert Prefill-Fortschritt, Streaming, Cancel bei Disconnect."""
import asyncio
import json
import os
import time

from aiohttp import web

MODEL = os.getenv("MOCK_MODEL", "test-model")

STATE = {"phase": None, "prompt_read": None, "prompt_total": None,
         "state": "idle", "queued": 0, "generated": 0,
         "prefill_tok_s_mean": 530.0, "elapsed_s": None}
CURRENT = asyncio.Event()
CURRENT.set()  # frei
CANCEL_LOG = []


async def metrics(_r):
    return web.json_response({"engine": {"model": MODEL},
                              "live": dict(STATE)})


async def health(_r):
    return web.json_response({"status": "ok", "max_context": 524288,
                              "model": MODEL,
                              "loaded": True})


async def models(_r):
    return web.json_response({"object": "list", "data": [
        {"id": MODEL, "object": "model"}]})


async def completions(request: web.Request):
    body = await request.json()
    model = body.get("model")
    if model != MODEL:
        return web.json_response({"error": "unknown model " + str(model)},
                                 status=404)
    # Promptgröße aus messages ableiten (Test-Konvention)
    msgs = json.dumps(body.get("messages", []))
    n_prompt = int(len(msgs))  # 1 Zeichen = 1 "Token" für Tests
    stream = body.get("stream", False)
    prefill_s = float(body.get("mock_prefill_s", 0))
    gen_s = float(body.get("mock_gen_s", 0.5))

    await CURRENT.wait()
    CURRENT.clear()
    STATE.update(state="processing", phase="reading the prompt",
                 prompt_total=n_prompt, prompt_read=0, generated=0,
                 elapsed_s=0)
    t0 = time.monotonic()

    def gone():
        tr = request.transport
        return tr is None or tr.is_closing()

    cancelled = False
    try:
        resp = web.StreamResponse() if stream else None
        if stream:
            await resp.prepare(request)
        # Prefill-Simulation
        steps = 20
        for i in range(1, steps + 1):
            await asyncio.sleep(prefill_s / steps)
            if gone():  # Client-Disconnect -> Arbeit stoppen (wie Strata)
                CANCEL_LOG.append({"model": model, "prompt_total": n_prompt,
                                   "prompt_read": STATE["prompt_read"],
                                   "phase": STATE["phase"]})
                return web.Response(status=499)
            STATE["prompt_read"] = int(n_prompt * i / steps)
            STATE["elapsed_s"] = round(time.monotonic() - t0, 1)
            if stream:
                await resp.write(b": pp keepalive\n\n")
        STATE.update(phase="answering", prompt_read=n_prompt)
        # Generierung
        n_chunks = 5
        for i in range(1, n_chunks + 1):
            await asyncio.sleep(gen_s / n_chunks)
            if gone():
                CANCEL_LOG.append({"model": model, "prompt_total": n_prompt,
                                   "prompt_read": STATE["prompt_read"],
                                   "phase": STATE["phase"]})
                return web.Response(status=499)
            STATE["generated"] = i
            chunk = {"id": "mock", "object": "chat.completion.chunk",
                     "choices": [{"index": 0,
                                  "delta": {"content": f"w{i}"}}]}
            if stream:
                await resp.write(b"data: " + json.dumps(chunk).encode()
                                 + b"\n\n")
        if stream:
            await resp.write(b"data: [DONE]\n\n")
            return resp
        return web.json_response({
            "id": "mock", "object": "chat.completion", "model": model,
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant",
                                     "content": "mock-answer"}}],
            "usage": {"prompt_tokens": n_prompt,
                      "completion_tokens": n_chunks}})
    except (ConnectionResetError, asyncio.CancelledError):
        cancelled = True
        CANCEL_LOG.append({"model": model, "prompt_total": n_prompt,
                           "prompt_read": STATE["prompt_read"],
                           "phase": STATE["phase"]})
        raise
    finally:
        STATE.update(state="idle", phase=None, prompt_read=None,
                     prompt_total=None, generated=None, elapsed_s=None)
        CURRENT.set()


def cancel_log(_r):
    return web.json_response(CANCEL_LOG)


app = web.Application()
app.router.add_get("/metrics", metrics)
app.router.add_get("/health", health)
app.router.add_get("/v1/models", models)
app.router.add_post("/v1/chat/completions", completions)
app.router.add_get("/cancel_log", cancel_log)

if __name__ == "__main__":
    web.run_app(app, host="127.0.0.1", port=19999, print=None,
                access_log=None)
