"""Upstream-Client: streamende Weiterleitung, SSE-Parsing, Heartbeat-Erzeugung."""
from __future__ import annotations

import asyncio
import json
import logging

import aiohttp

import config
from queue_ import ERROR_SENTINEL, SENTINEL, QueuedRequest

log = logging.getLogger("proxy.strata")


class UpstreamError(Exception):
    def __init__(self, status: int, body: bytes) -> None:
        super().__init__(f"upstream {status}")
        self.status = status
        self.body = body

# read_timeout=None: Strata-Prefills dauern 7-12 min ohne Bytes (3.4)
_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=10, sock_read=None)


class StrataClient:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=_TIMEOUT)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_json(self, path: str):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as s:
                async with s.get(config.UPSTREAM_URL.rstrip("/") + path) as r:
                    if r.status != 200:
                        return None
                    return await r.json(content_type=None)
        except Exception:
            return None

    async def run(self, req: QueuedRequest) -> str:
        """Request an Strata senden und Ergebnis liefern.

        Returns: 'done' | 'aborted' | 'error'
        ('aborted' = Task wurde per cancel() beendet -> Upstream-Disconnect)
        """
        body = dict(req.body)
        body["model"] = config.REAL_MODEL          # Remapping (Abschnitt 4)
        body["stream"] = req.stream
        url = config.UPSTREAM_URL.rstrip("/") + "/v1/chat/completions"
        try:
            async with (await self.session()).post(url, json=body) as resp:
                if resp.status >= 400:
                    data = await resp.read()
                    log.warning("upstream status %s: %s",
                                resp.status, data[:300])
                    req.error = UpstreamError(resp.status, data)
                    req.result = (resp.status, data)
                    if req.stream:
                        req.chunks.put_nowait(ERROR_SENTINEL)
                    return "error"
                if req.stream:
                    async for chunk in resp.content.iter_any():
                        req.chunks.put_nowait(chunk)
                    req.chunks.put_nowait(SENTINEL)
                else:
                    data = await resp.read()
                    req.result = (resp.status, data)
                return "done"
        except asyncio.CancelledError:
            # Upstream-Verbindung geschlossen = Strata bricht ab (3.3).
            # Prioritäts-Abort (Client noch da): KEIN SENTINEL legen, sonst
            # bricht proxy._stream_response sofort ab (Z.99), bevor das
            # ABORTED_SENTINEL aus scheduler._run (requeue) gelesen wird ->
            # Requeue-Stream would stay dead (T4c/T11b). scheduler._run legt
            # das ABORTED_SENTINEL. Nur bei echtem Client-Disconnect (dropped)
            # Stream beenden.
            if req.stream and req.dropped:
                req.chunks.put_nowait(SENTINEL)
            log.info("ABORTED seq=%s nach %.1fs (Prefill-Arbeit verfällt)",
                     req.seq, req.waited())
            return "aborted"
        except Exception as exc:
            log.error("upstream error seq=%s: %s", req.seq, exc)
            req.error = exc
            if req.stream:
                req.chunks.put_nowait(SENTINEL)
            return "error"
