#!/usr/bin/env python3
"""Testszenarien 11.1-11.8 gegen Mock-Strata (Port 19999) + Proxy (Port 1241)."""
import asyncio
import json
import os
import subprocess
import sys
import time

import aiohttp

BASE = "http://127.0.0.1:1241"
MOCK = "http://127.0.0.1:19999"
MODEL = os.getenv("PROXY_REAL_MODEL", "test-model")
CHAT = MODEL + "-CHAT"
RES = MODEL + "-RESEARCH"
COD = MODEL + "-CODING"
PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name} {detail}", flush=True)


async def post_chat(sess, model, content, extra=None, timeout=300):
    body = {"model": model, "messages": [{"role": "user", "content": content}]}
    if extra:
        body.update(extra)
    t0 = time.monotonic()
    async with sess.post(BASE + "/v1/chat/completions", json=body,
                         timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        data = await r.read()
    return r.status, data, time.monotonic() - t0


async def stream_chat(sess, model, content, extra=None, collect_pings=False):
    body = {"model": model, "stream": True,
            "messages": [{"role": "user", "content": content}]}
    if extra:
        body.update(extra)
    t0 = time.monotonic()
    pings, chunks, first_byte = 0, [], None
    async with sess.post(
            BASE + "/v1/chat/completions", json=body,
            timeout=aiohttp.ClientTimeout(total=300)) as r:
        async for line in r.content:
            if first_byte is None:
                first_byte = time.monotonic() - t0
            s = line.decode(errors="replace").strip()
            if s == ": ping":
                pings += 1
            elif s.startswith("data: ") and s != "data: [DONE]":
                chunks.append(s)
    return r.status, pings, chunks, first_byte, time.monotonic() - t0


async def cancel_log(sess):
    async with sess.get(MOCK + "/cancel_log") as r:
        return await r.json()


async def main():
    async with aiohttp.ClientSession() as sess:
        # --- T1: Tagging / Modelle ---
        print("T1: Tagging & /v1/models")
        async with sess.get(BASE + "/v1/models") as r:
            m = await r.json()
        ids = [d["id"] for d in m["data"]]
        check("T1a aliases sichtbar",
              CHAT in ids
              and RES in ids, str(ids))

        # --- T2: Chat sofort (HIGH, frei) ---
        print("T2: Chat bei freiem Strata")
        st, data, dt = await post_chat(
            sess, CHAT, "hi")
        j = json.loads(data)
        check("T2a Antwort korrekt", st == 200
              and j.get("choices", [{}])[0].get("message", {}).get("content")
              == "mock-answer", f"status={st} dt={dt:.1f}s")
        check("T2b Remapping (kein 404)", st != 404)

        # --- T3: Hold-Logik (LOW wartet idle_hold nach letztem Chat) ---
        print("T3: Hold-Logik")
        low_task = asyncio.create_task(post_chat(
            sess, RES, "x" * 100,
            extra={"mock_prefill_s": 1}))
        await asyncio.sleep(1)
        st2, d2, dt2 = await post_chat(
            sess, CHAT, "hi")
        stl, dl, dtl = await low_task
        # idle_hold=5s: LOW muss >=5s nach Chat-Ankunft fertig sein
        check("T3a Chat nicht verzögert", dt2 < 5, f"chat dt={dt2:.1f}s")
        check("T3b LOW gehalten (>=5s nach Chat)", dtl >= 5,
              f"low dt={dtl:.1f}s")

        # --- T4: Abbruch im Prefill (streaming, da Abort nur für streaming) ---
        print("T4: Abort im Prefill (streaming)")
        before = len(await cancel_log(sess))
        low_task = asyncio.create_task(stream_chat(
            sess, RES, "y" * 100,
            extra={"mock_prefill_s": 20}))
        await asyncio.sleep(4)  # LOW im Prefill bei ~20%
        st4, d4, dt4 = await post_chat(
            sess, CHAT, "hi")
        logs = await cancel_log(sess)
        check("T4a Prefill abgebrochen", len(logs) > before,
              f"cancel_log={logs[-1] if logs else None}")
        check("T4b Chat antwortet schnell", st4 == 200 and dt4 < 10,
              f"dt={dt4:.1f}s")
        stl, pl, cl, fbl, dtl = await low_task
        check("T4c LOW später komplett (transparent)",
              stl == 200 and len(cl) >= 5, f"low dt={dtl:.1f}s chunks={len(cl)}")

        # --- T5: Kein Abbruch bei >=85% ---
        print("T5: Kein Abort bei hohem Fortschritt")
        before = len(await cancel_log(sess))
        low_task = asyncio.create_task(post_chat(
            sess, RES, "z" * 100,
            extra={"mock_prefill_s": 10}))
        await asyncio.sleep(9.5)  # Prefill ~95%
        st5, d5, dt5 = await post_chat(
            sess, CHAT, "hi")
        logs = await cancel_log(sess)
        check("T5a KEIN Cancel bei >=85%", len(logs) == before,
              f"cancels={len(logs)-before}")
        check("T5b Chat wartet und bekommt Slot", st5 == 200,
              f"chat dt={dt5:.1f}s")
        await low_task

        # --- T6: Generation läuft -> kein Abort ---
        print("T6: Kein Abort in Generation")
        before = len(await cancel_log(sess))
        low_task = asyncio.create_task(post_chat(
            sess, RES, "g" * 100,
            extra={"mock_prefill_s": 2, "mock_gen_s": 8}))
        await asyncio.sleep(4)  # Prefill fertig, Generation läuft
        st6, d6, dt6 = await post_chat(
            sess, CHAT, "hi")
        logs = await cancel_log(sess)
        check("T6a Generation nicht unterbrochen", len(logs) == before)
        check("T6b Chat danach bedient", st6 == 200, f"dt={dt6:.1f}s")
        await low_task

        # --- T7: Streaming-Heartbeat während HOLDING ---
        print("T7: SSE-Heartbeats im Hold")
        # Blockiere Strata mit langem HIGH (HIGH bricht HIGH nicht ab),
        # dann streamender CHAT -> HOLDING mit Pings
        blocker = asyncio.create_task(post_chat(
            sess, CHAT, "b" * 100,
            extra={"mock_prefill_s": 12, "mock_gen_s": 2}))
        await asyncio.sleep(0.5)
        st7, pings, chunks7, fb7, dt7 = await stream_chat(
            sess, CHAT, "stream-me")
        check("T7a Pings im Hold", pings >= 1, f"pings={pings}")
        check("T7b Stream-Inhalt ok", len(chunks7) >= 5,
              f"chunks={len(chunks7)} first_byte={fb7:.1f}s")
        await blocker

        # --- T8: Client-Disconnect -> Queue-Bereinigung ---
        print("T8: Client-Disconnect")
        t8 = asyncio.create_task(post_chat(
            sess, RES, "d" * 100,
            extra={"mock_prefill_s": 3}))
        await asyncio.sleep(1)  # LOW in Queue (HOLD), noch nicht gesendet
        t8.cancel()             # Client geht weg
        try:
            await t8
        except (asyncio.CancelledError, Exception):
            pass
        await asyncio.sleep(2)
        async with sess.get(BASE + "/metrics") as r:
            snap = await r.json()
        check("T8a Queue nach Disconnect leer", snap["queued"] == 0,
              f"queued={snap['queued']}")

        # --- T9: Starvation/Aging ---
        print("T9: Aging (starvation_s=15)")
        t0 = time.monotonic()
        st9, d9, dt9 = await post_chat(
            sess, RES, "a" * 100,
            extra={"mock_prefill_s": 1})
        # Nach T8 idle; letzter Chat ist lange her -> LOW startet sofort
        check("T9a LOW nach Ruhezeit bedient", st9 == 200 and dt9 < 8,
              f"dt={dt9:.1f}s")

        # --- T11: 3-Prio-Routing (CODING < RESEARCH < CHAT) ---
        print("T11: CODING-Priorität (unter RESEARCH & CHAT)")
        before = len(await cancel_log(sess))
        cod_task = asyncio.create_task(stream_chat(
            sess, COD, "c" * 100,
            extra={"mock_prefill_s": 20}))
        await asyncio.sleep(4)  # CODING im Prefill (~20%)
        stc, dc, dtc = await post_chat(
            sess, RES, "r" * 100,   # RESEARCH soll CODING verdrängen
            extra={"mock_prefill_s": 20})
        logs = await cancel_log(sess)
        check("T11a RESEARCH verdrängt CODING (Abort)",
              len(logs) > before, f"cancels={len(logs)-before}")
        # RESEARCH wurde gestartet (läuft); CODING requeued hinter
        stcod, pco, cco, fbco, dtcod = await cod_task
        check("T11b CODING später trotzdem komplett",
              stcod == 200 and len(cco) >= 5, f"dt={dtcod:.1f}s chunks={len(cco)}")
        stc2, dc2, dtc2 = await post_chat(
            sess, CHAT, "hi")  # CHAT -> läuft sofort durch (RESEARCH/CODING hinten)
        check("T11c CHAT geht durch wenn fertig", stc2 == 200,
              f"dt={dtc2:.1f}s")

        # --- T10: Proxy-Metriken ---
        print("T10: /metrics Snapshot")
        async with sess.get(BASE + "/metrics") as r:
            snap = await r.json()
        check("T10a Stats vorhanden",
              "stats" in snap and snap["stats"]["aborted"] >= 1,
              json.dumps(snap["stats"]))

    print(f"\nERGEBNIS: {len(PASS)} PASS / {len(FAIL)} FAIL")
    if FAIL:
        print("FEHLER:", FAIL)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))