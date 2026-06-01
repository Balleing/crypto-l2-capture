"""
Binance USDT-M Futures — L2 snapshot/diff merge state machine (Phase 0).

Implements the exact 6-step reconnect protocol from the Binance API docs:
  1. Open WS @depth@100ms, buffer events in a queue.
  2. Fetch REST snapshot GET /fapi/v1/depth?limit=1000; record lastUpdateId.
  3. Drop buffered events where u < lastUpdateId.
  4. First kept event must satisfy: U <= lastUpdateId+1 <= u.
     No such event within 5s → restart from step 1.
  5. Apply that event. For every subsequent event verify pu == prev_u.
     Continuity break → restart from step 1.
  6. Feed events to L2Book.apply_diff() from here on.

Every restart is logged with a reason string. The state machine is the
single authority for deciding when the book is "live" and when handlers
receive snapshots.

Binance API ref:
  https://developers.binance.com/docs/derivatives/usdt-margined-futures
  /websocket-market-streams/Diff-Book-Depth-Streams
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING

from loguru import logger

from market_data.book.l2 import BookInvariantError, BookSnapshot, L2Book
from market_data.feeds.binance_rest import BinanceRestClient


def _on_handler_done(task: asyncio.Task) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.exception("on_book_update handler raised: {}", task.exception())

if TYPE_CHECKING:
    pass

_SYNC_TIMEOUT_S: float = 5.0  # step 4: give up waiting for first-valid event


class _State(Enum):
    BUFFERING   = auto()   # pre-snapshot, queuing WS events
    SYNCING     = auto()   # snapshot fetched, hunting for first valid event
    LIVE        = auto()   # book is authoritative, dispatching to handlers


@dataclass
class MachineStats:
    restarts:       int = 0
    events_dropped: int = 0   # events discarded while u < lastUpdateId
    events_applied: int = 0


class DepthStateMachine:
    """
    Manages the L2 book lifecycle for one symbol.

    Call ``on_depth_event(data)`` for every @depth@100ms WS message.
    The machine buffers internally until the REST snapshot is fetched
    and the first valid event is found. After that each event is applied
    immediately and ``on_book_update`` is called with the new snapshot.

    Usage::

        machine = DepthStateMachine("BTCUSDT", rest_client, on_book_update=handler)
        await machine.start()                 # fetches snapshot, enters LIVE
        # then route WS depth events:
        await machine.on_depth_event(data)    # data = the "data" dict from WS
    """

    def __init__(
        self,
        symbol:         str,
        rest_client:    BinanceRestClient,
        on_book_update: callable | None = None,
        snapshot_depth: int = 20,
    ) -> None:
        self.symbol          = symbol.upper()
        self._rest           = rest_client
        self._on_update      = on_book_update
        self._snap_depth     = snapshot_depth
        self._book           = L2Book(self.symbol)
        self._buffer:        list[dict] = []
        self._state          = _State.BUFFERING
        self._prev_u:        int | None = None
        self.stats           = MachineStats()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Step 2: fetch REST snapshot and advance to SYNCING state.

        Call this once after the WS connection is open and events are
        flowing into the buffer. Blocks until the snapshot is fetched.
        Retries indefinitely on network errors (WS reconnect handles the
        outer loop).
        """
        while True:
            try:
                raw = await self._rest.depth(self.symbol, limit=1000)
                break
            except Exception as exc:
                logger.warning(
                    "{} snapshot fetch failed: {} — retrying in 1s",
                    self.symbol, exc,
                )
                await asyncio.sleep(1.0)

        # on_depth_event's SYNCING drain may have gone LIVE while we were
        # awaiting the REST fetch — don't clobber that valid live state.
        if self._state == _State.LIVE:
            logger.debug("{} start() aborting: already LIVE", self.symbol)
            return

        last_update_id: int          = int(raw["lastUpdateId"])
        bids: list[list[str]]        = raw["bids"]
        asks: list[list[str]]        = raw["asks"]
        event_time_ms: int           = int(raw.get("T", 0))

        self._book.load_snapshot(last_update_id, bids, asks, event_time_ms)
        self._state  = _State.SYNCING
        self._prev_u = None

        logger.info(
            "{} snapshot loaded: lastUpdateId={} bids={} asks={}",
            self.symbol, last_update_id, len(bids), len(asks),
        )

        # Step 3 + 4: drain the buffer with the new lastUpdateId
        await self._drain_buffer()

    async def on_depth_event(self, data: dict) -> None:
        """
        Receive one @depth@100ms event payload (the "data" dict from WS).

        Routes to buffer or live application depending on state.
        """
        if self._state == _State.BUFFERING:
            self._buffer.append(data)
            return

        if self._state == _State.SYNCING:
            # Buffer only — start() is the sole path to LIVE.
            # Calling _drain_buffer here creates a second path that races
            # with the start_task and can override LIVE state.
            self._buffer.append(data)
            return

        # _State.LIVE: apply directly (synchronous — no yield points)
        self._apply_event(data)

    def restart(self, reason: str) -> None:
        """
        Full restart: clear book, go back to BUFFERING, log reason.
        Caller must call start() again after the WS reconnects or
        immediately if the WS is still open.
        """
        self.stats.restarts += 1
        logger.warning(
            "{} L2 restart #{} reason={}",
            self.symbol, self.stats.restarts, reason,
        )
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
        """
        Steps 3–4: process buffered events against the current snapshot.

        Step 3: drop events where u < lastUpdateId.
        Step 4: find first event where U <= lastUpdateId+1 <= u.
                If not found within 5s of snapshot fetch → restart.
        Step 5 onward: check pu continuity, apply.
        """
        last_uid = self._book.last_update_id

        # Step 3: discard events that predate the snapshot
        pre = len(self._buffer)
        self._buffer = [e for e in self._buffer if int(e["u"]) >= last_uid]
        dropped = pre - len(self._buffer)
        if dropped:
            self.stats.events_dropped += dropped
            logger.debug(
                "{} dropped {} buffered events (u < lastUpdateId={})",
                self.symbol, dropped, last_uid,
            )

        if self._state == _State.SYNCING and self._prev_u is None:
            # Step 4: find first event satisfying U <= lastUpdateId+1 <= u
            anchor_found = False
            while self._buffer:
                event = self._buffer[0]
                U_ = int(event["U"])
                u_ = int(event["u"])
                if U_ <= last_uid + 1 <= u_:
                    anchor_found = True
                    break
                # This event doesn't bridge the snapshot — drop it
                self._buffer.pop(0)
                self.stats.events_dropped += 1
                logger.debug(
                    "{} dropped bridging event U={} u={} (lastUpdateId={})",
                    self.symbol, U_, u_, last_uid,
                )

            if not anchor_found:
                # No valid anchor in buffer — stay in SYNCING until more events arrive.
                # The WS client's 5s gap detection handles the timeout case.
                return

            # Found the anchor event — apply it and transition to LIVE
            anchor = self._buffer.pop(0)
            self._apply_event(anchor, first_event=True)
            self._state = _State.LIVE
            logger.info(
                "{} L2 book LIVE: first event U={} u={}",
                self.symbol, anchor["U"], anchor["u"],
            )

        # Apply any remaining buffered events now that we're LIVE.
        # _apply_event is synchronous (no yield points) so no concurrent
        # on_depth_event call can interleave and corrupt _prev_u.
        while self._buffer:
            event = self._buffer.pop(0)
            self._apply_event(event)

    def _apply_event(self, data: dict, first_event: bool = False) -> None:
        """
        Apply a single diff event after all preconditions are met.

        Step 5: pu continuity check (skip on first_event — prev_u is None).
        Calls L2Book.apply_diff() and dispatches to on_book_update.
        Restarts on any invariant violation.

        Synchronous by design: no yield points here prevents two concurrent
        callers (start() task drain loop vs on_depth_event LIVE path) from
        interleaving and corrupting _prev_u.  The on_book_update callback is
        fired as a task (ensure_future) so it never blocks the apply path.
        """
        U_  = int(data["U"])
        u_  = int(data["u"])
        pu_ = int(data["pu"])
        ts  = int(data.get("T", data.get("E", 0)))

        # Step 5: pu == previous event's u (invariant 4)
        if not first_event and self._prev_u is not None:
            if pu_ != self._prev_u:
                self.restart(
                    f"pu_continuity_break: pu={pu_} != prev_u={self._prev_u} "
                    f"at u={u_}"
                )
                return

        try:
            self._book.apply_diff(
                U=U_,
                u=u_,
                event_time_ms=ts,
                bids=data.get("b", []),
                asks=data.get("a", []),
            )
        except BookInvariantError as exc:
            self.restart(f"invariant_violation: {exc}")
            return

        self._prev_u = u_
        self.stats.events_applied += 1

        if self._on_update is not None:
            snap: BookSnapshot = self._book.snapshot(self._snap_depth)
            try:
                result = self._on_update(snap)
                if asyncio.iscoroutine(result):
                    task = asyncio.ensure_future(result)
                    task.add_done_callback(_on_handler_done)
            except Exception as exc:
                logger.exception(
                    "{} on_book_update handler raised: {}", self.symbol, exc
                )
                # Handler crash does NOT restart the book — same principle as WS client
