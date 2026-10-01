#!/usr/bin/env python3
"""Realtest gegen Strata via Proxy: Szenario C (Abort im Prefill) + D (kein Abort)."""
import asyncio
import json
import os
import time

import aiohttp

BASE = "http://127.0.0.1:1240"
STRATA = os.getenv("STRATA_BASE", "http://127.0.0.1:1238")
CHAT = os.getenv("PROXY_REAL_MODEL", "<MODEL>") + "-CHAT"
RES = os.getenv("PROXY_REAL_MODEL", "<MODEL>") + "-RESEARCH"


def big_prompt(n_chars):
    # Einmaliger Zufalls-Präfix verhindert KV-Reuse -> echter Prefill
    import random
    rnd = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=64))
    filler = (f"Protokoll {rnd}. Der Systemarchivar protokolliert jede "
              "Änderung an der Queue und notiert Zeitstempel, Priorität "
              "und Begründung. ")
    return (filler * (n_chars // len(filler) + 1))[:n_chars]


async def live(sess):
    async with sess.get(STRATA + "/metrics") as r:
        m = await r.json()
    return m["live"]


async def post(sess, model, content, extra=None, timeout=1800):
    body = {"model": model,
            "messages": [{"role": "user", "content": content}],
            "max_tokens": 60}
    if extra:
        body.update(extra)
    t0 = time.monotonic()
    async with sess.post(BASE + "/v1/chat/completions", json=body,
                         timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        data = await r.read()
    return r.status, data, time.monotonic() - t0


async def main():
    async with aiohttp.ClientSession() as sess:
        print("=== Szenario C: RESEARCH-Prefill, Chat bricht bei <85% ab ===")
        t_start = time.monotonic()
        res_task = asyncio.create_task(
            post(sess, RES, big_prompt(60_000)))
        # auf Prefill-Fortschritt warten
        prog = 0.0
        while prog < 0.10:
            await asyncio.sleep(3)
            lv = await live(sess)
            if lv.get("phase") == "reading the prompt":
                prog = (lv["prompt_read"] or 0) / (lv["prompt_total"] or 1)
                print(f"  Prefill-Fortschritt: {prog:.2%} "
                      f"({lv['prompt_read']}/{lv['prompt_total']}) "
                      f"t+{time.monotonic()-t_start:.0f}s", flush=True)
        # Chat rein -> muss Abort auslösen
        st, data, dt = await post(sess, CHAT, "Antworte nur: ABORT-TEST-CHAT-OK")
        content = json.loads(data)["choices"][0]["message"]
        print(f"  CHAT: status={st} dt={dt:.1f}s "
              f"content={(content.get('content') or '')[:40]!r}")
        ok_chat = st == 200 and "ABORT-TEST-CHAT-OK" in (content.get("content") or "")
        # RESEARCH danach zu Ende
        st2, data2, dt2 = await res_task
        j2 = json.loads(data2)
        ok_res = st2 == 200 and "choices" in j2
        print(f"  RESEARCH: status={st2} dt={dt2:.1f}s "
              f"finish={j2['choices'][0].get('finish_reason')}")
        print(f"  -> Szenario C: {'PASS' if ok_chat and ok_res else 'FAIL'}")

        print("\n=== Szenario D: Chat während RESEARCH nahe Prefill-Ende ===")
        t_start = time.monotonic()
        res_task = asyncio.create_task(
            post(sess, RES, big_prompt(60_000)))
        prog = 0.0
        while prog < 0.90:
            await asyncio.sleep(2)
            lv = await live(sess)
            if lv.get("phase") == "reading the prompt":
                prog = (lv["prompt_read"] or 0) / (lv["prompt_total"] or 1)
        print(f"  Chat kommt bei Prefill {prog:.2%}")
        st, data, dt = await post(sess, CHAT, "Antworte nur: NOABORT-CHAT-OK")
        content = json.loads(data)["choices"][0]["message"]
        ok = st == 200 and "NOABORT-CHAT-OK" in (content.get("content") or "")
        print(f"  CHAT: status={st} dt={dt:.1f}s "
              f"content={(content.get('content') or '')[:40]!r}")
        print(f"  -> Szenario D: {'PASS' if ok else 'FAIL'}")
        await res_task

        async with sess.get(BASE + "/metrics") as r:
            print("\nProxy-Stats:", json.dumps((await r.json())["stats"]))


if __name__ == "__main__":
    asyncio.run(main())
