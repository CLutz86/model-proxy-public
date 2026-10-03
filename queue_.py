"""Prioritäts-Heap + Zustandsverwaltung (HOLDING/RUNNING/ABORTED/DONE)."""
from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from dataclasses import dataclass, field

import config

SENTINEL = object()          # Stream-Ende (normal)
ABORTED_SENTINEL = object()  # Stream-Ende wegen Abbruch -> Client weiterhalten
ERROR_SENTINEL = object()    # Stream-Ende wegen Upstream-Fehler


@dataclass
class QueuedRequest:
    priority: int
    body: dict
    stream: bool
    seq: int = 0
    arrival: float = field(default_factory=time.monotonic)
    state: str = "HOLDING"
    chunks: asyncio.Queue = field(default_factory=asyncio.Queue)
    gate: asyncio.Event = field(default_factory=asyncio.Event)  # offen = an Client senden
    requeued: asyncio.Event = field(default_factory=asyncio.Event)  # nach ABORT -> neu halten
    dropped: bool = False
    # non-streaming Ergebnis:
    result: tuple | None = None          # (status, bytes) bei DONE
    error: Exception | None = None
    completed: asyncio.Future | None = None  # non-streaming completion

    def waited(self) -> float:
        return time.monotonic() - self.arrival


class PriorityQueue:
    def __init__(self) -> None:
        self._heap: list[tuple[int, int, QueuedRequest]] = []
        self._counter = itertools.count()

    def push(self, req: QueuedRequest, front: bool = False) -> None:
        if front:
            # abgebrochene Recherche zurück an Queue-Anfang: Seq unter alles Bestehende
            existing = [h[1] for h in self._heap]
            req.seq = (min(existing) - 1) if existing else next(self._counter)
        else:
            req.seq = next(self._counter)
        heapq.heappush(self._heap, (req.priority, req.seq, req))

    def peek(self) -> QueuedRequest | None:
        return self._heap[0][2] if self._heap else None

    def pending(self) -> list[QueuedRequest]:
        return [h[2] for h in self._heap]

    def pop(self) -> QueuedRequest | None:
        return heapq.heappop(self._heap)[2] if self._heap else None

    def has_priority(self, prio: int) -> bool:
        return any(h[0] == prio for h in self._heap)

    def promote(self, req: QueuedRequest) -> None:
        """Aging: Priority im Heap-Eintrag aktualisieren."""
        for i, h in enumerate(self._heap):
            if h[2] is req:
                self._heap[i] = (req.priority, h[1], req)
                heapq.heapify(self._heap)
                return

    def remove(self, req: QueuedRequest) -> bool:
        for i, h in enumerate(self._heap):
            if h[2] is req:
                self._heap[i] = self._heap[-1]
                self._heap.pop()
                heapq.heapify(self._heap)
                return True
        return False

    def __len__(self) -> int:
        return len(self._heap)

