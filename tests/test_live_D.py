#!/usr/bin/env python3
"""Szenario D live: Chat bei Prefill >=85% -> KEIN Abort, Chat wartet."""
import asyncio
import json
import os
import random
import time

import aiohttp

BASE = "http://127.0.0.1:1240"
STRATA = os.getenv("STRATA_BASE", "http://127.0.0.1:1238")
CHAT = os.getenv("PROXY_REAL_MODEL", "<MODEL>") + "-CHAT"
RES = os.getenv("PROXY_REAL_MODEL", "<MODEL>") + "-RESEARCH"


def big_prompt(n_chars):
    rnd = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=64))
    filler = (f"Protokoll {rnd}. Der Systemarchivar protokolliert jede "
              "Änderung an der Queue und notiert Zeitstempel, Priorität "
              "und Begründung. ")
    return (filler * (n_chars // len(filler) + 1))[:n_chars]


async def live(sess):
    async with sess.get(STRATA + "/metrics") as r:
        return (await r.json())["live"]


async def post(sess, model, content, timeout=600):
    body = {"model": model,
            "messages": [{"role": "user", "content": content}],
            "max_tokens": 60}
    t0 = time.monotonic()
    async with sess.post(BASE + "/v1/chat/completions", json=body,
                         timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        data = await r.read()
    return r.status, data, time.monotonic() - t0


async def main():
    async with aiohttp.ClientSession() as sess:
        print("=== Szenario D: Chat bei Prefill >=85% -> KEIN Abort ===",
              flush=True)
        res_task = asyncio.create_task(post(sess, RES, big_prompt(40_000)))
        prog = 0.0
        t0 = time.monotonic()
        while prog < 0.90 and time.monotonic() - t0 < 120:
            lv = await live(sess)
            ph = lv.get("phase")
            if ph == "reading the prompt":
                prog = (lv.get("prompt_read") or 0) / (lv.get("prompt_total") or 1)
                print(f"  reading {prog:.2%} t+{time.monotonic()-t0:.0f}s",
                      flush=True)
            elif ph == "answering":
                prog = 1.0
            await asyncio.sleep(1.5)
        print(f"  Chat kommt bei Prefill {prog:.2%}", flush=True)
        st, data, dt = await post(sess, CHAT,
                                  "Antworte nur mit: NOABORT-CHAT-OK")
        msg = json.loads(data)["choices"][0]["message"]
        got = (msg.get("content") or "") + (msg.get("reasoning_content") or "")
        ok = st == 200 and "NOABORT-CHAT-OK" in got
        print(f"  CHAT: status={st} dt={dt:.1f}s ok={ok}", flush=True)
        st2, data2, dt2 = await res_task
        print(f"  RESEARCH: status={st2} dt={dt2:.1f}s", flush=True)
        print(f"  -> Szenario D: {'PASS' if ok else 'FAIL'}", flush=True)
        async with sess.get(BASE + "/metrics") as r:
            print("Proxy-Stats:", json.dumps((await r.json())["stats"]),
                  flush=True)
        return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
