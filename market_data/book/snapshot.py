"""
Exchange-agnostic L2 snapshot/diff merge state machine.

Implements the 6-step reconnect protocol that all supported exchanges share:
  1. Open WS stream, buffer NormalizedDepthEvents in a queue.
  2. Fetch REST snapshot; record last_update_id.
  3. Drop buffered events where last_update_id < snapshot.last_update_id.
  4. First kept event must satisfy: first_update_id <= snapshot.last_update_id+1
     <= last_update_id.  No such event within the WS gap window → restart.
  5. Apply that event. For every subsequent event verify:
       prev_update_id == prev applied last_update_id  (if exchange provides it)
     Continuity break → restart from step 1.
  6. Feed events to L2Book.apply_diff() from here on.

Exchanges that don't provide a prev_update_id (prev_update_id is None) skip
the step-5 continuity check — they rely purely on the monotonic ID check in
L2Book.apply_diff().
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Callable

from loguru import logger

from market_data.book.l2 import BookInvariantError, BookSnapshot, L2Book
from market_data.exchanges.base import ExchangeRestAdapter, NormalizedDepthEvent


def _on_handler_done(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.exception("on_book_update handler raised: {}", task.exception())


_SYNC_TIMEOUT_S: float = 5.0


class _State(Enum):
    BUFFERING = auto()
    SYNCING   = auto()
    LIVE      = auto()


@dataclass
class MachineStats:
    restarts:       int = 0
    events_dropped: int = 0
    events_applied: int = 0


class DepthStateMachine:
    """
    Manages the L2 book lifecycle for one symbol on any exchange.

    Pass NormalizedDepthEvent objects from the WS adapter via on_depth_event().
    The machine buffers internally until the REST snapshot is fetched and the
    first valid event is found, then fires on_book_update with each snapshot.

    Usage::

        rest    = BinanceRestAdapter()
        machine = DepthStateMachine("BTCUSDT", rest, on_book_update=handler)
        await machine.start()
        await machine.on_depth_event(normalized_event)
    """

    def __init__(
        self,
        symbol:         str,
        rest_client:    ExchangeRestAdapter,
        on_book_update: Callable | None = None,
        snapshot_depth: int = 20,
    ) -> None:
        self.symbol      = symbol.upper()
        self._rest       = rest_client
        self._on_update  = on_book_update
        self._snap_depth = snapshot_depth
        self._book       = L2Book(self.symbol)
        self._buffer:    list[NormalizedDepthEvent] = []
        self._state      = _State.BUFFERING
        self._prev_u:    int | None = None
        self.stats       = MachineStats()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Fetch REST snapshot and advance to SYNCING.

        Call once after WS is open and events are flowing into the buffer.
        Retries indefinitely on network errors.
        """
        while True:
            try:
                snap = await self._rest.fetch_snapshot(self.symbol, limit=1000)
                break
            except Exception as exc:
                logger.warning("{} snapshot fetch failed: {} — retrying in 1s",
                               self.symbol, exc)
                await asyncio.sleep(1.0)

        if self._state == _State.LIVE:
            logger.debug("{} start() aborting: already LIVE", self.symbol)
            return

        self._book.load_snapshot(
            snap.last_update_id, snap.bids, snap.asks, snap.event_time_ms
        )
        self._state  = _State.SYNCING
        self._prev_u = None

        logger.info("{} snapshot loaded: lastUpdateId={} bids={} asks={}",
                    self.symbol, snap.last_update_id, len(snap.bids), len(snap.asks))

        await self._drain_buffer()

    async def on_depth_event(self, event: NormalizedDepthEvent) -> None:
        """Receive one normalized depth event from the WS adapter."""
        if self._state in (_State.BUFFERING, _State.SYNCING):
            self._buffer.append(event)
            return
        self._apply_event(event)

    def restart(self, reason: str) -> None:
        self.stats.restarts += 1
        logger.warning("{} L2 restart #{} reason={}", self.symbol, self.stats.restarts, reason)
        self._buffer.clear()
        self._state  = _State.BUFFERING
        self._prev_u = None
        self._book   = L2Book(self.symbol)

    @property
    def is_live(self) -> bool:
        return self._state == _State.LIVE

    @property
    def book(self) -> L2Book:
        return self._book

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _drain_buffer(self) -> None:
        last_uid = self._book.last_update_id

        pre = len(self._buffer)
        self._buffer = [e for e in self._buffer if e.last_update_id >= last_uid]
        dropped = pre - len(self._buffer)
        if dropped:
            self.stats.events_dropped += dropped
            logger.debug("{} dropped {} buffered events (last_update_id < {})",
                         self.symbol, dropped, last_uid)

        if self._state == _State.SYNCING and self._prev_u is None:
            anchor_found = False
            while self._buffer:
                event = self._buffer[0]
                if event.first_update_id <= last_uid + 1 <= event.last_update_id:
                    anchor_found = True
                    break
                self._buffer.pop(0)
                self.stats.events_dropped += 1
                logger.debug("{} dropped bridging event first={} last={} (lastUpdateId={})",
                             self.symbol, event.first_update_id, event.last_update_id, last_uid)

            if not anchor_found:
                return

            anchor = self._buffer.pop(0)
            self._apply_event(anchor, first_event=True)
            self._state = _State.LIVE
            logger.info("{} L2 book LIVE: first event first={} last={}",
                        self.symbol, anchor.first_update_id, anchor.last_update_id)

        while self._buffer:
            self._apply_event(self._buffer.pop(0))

    def _apply_event(self, event: NormalizedDepthEvent, first_event: bool = False) -> None:
        # Step 5: prev_update_id continuity (skip on first event, skip if exchange omits it)
        if (
            not first_event
            and self._prev_u is not None
            and event.prev_update_id is not None
            and event.prev_update_id != self._prev_u
        ):
            self.restart(
                f"continuity_break: prev_update_id={event.prev_update_id} "
                f"!= prev_u={self._prev_u} at last={event.last_update_id}"
            )
            return

        try:
            self._book.apply_diff(
                U=event.first_update_id,
                u=event.last_update_id,
                event_time_ms=event.event_time_ms,
                bids=event.bids,
                asks=event.asks,
            )
        except BookInvariantError as exc:
            self.restart(f"invariant_violation: {exc}")
            return

        self._prev_u = event.last_update_id
        self.stats.events_applied += 1

        if self._on_update is not None:
            snap: BookSnapshot = self._book.snapshot(self._snap_depth)
            try:
                result = self._on_update(snap)
                if asyncio.iscoroutine(result):
                    task = asyncio.ensure_future(result)
                    task.add_done_callback(_on_handler_done)
            except Exception as exc:
                logger.exception("{} on_book_update handler raised: {}", self.symbol, exc)
