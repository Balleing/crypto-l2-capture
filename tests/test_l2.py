"""
Tests for the L2 book (l2.py) and snapshot/diff state machine (snapshot.py).

L2 tests use synthetic price/qty strings — no network calls.
State machine tests mock BinanceRestClient to avoid real HTTP.

Coverage:
  - BookSide: apply, best, levels, zero-qty removal
  - L2Book: load_snapshot, apply_diff, all 4 invariants, microprice, imbalance
  - DepthStateMachine: buffering, step-3 drop, step-4 anchor, step-5 pu
    continuity, restart counting, handler dispatch
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from market_data.book.l2 import (
    BookInvariantError,
    BookSide,
    BookSnapshot,
    L2Book,
)
from market_data.book.snapshot import DepthStateMachine


# ---------------------------------------------------------------------------
# BookSide
# ---------------------------------------------------------------------------

class TestBookSide:
    def test_apply_adds_level(self) -> None:
        side = BookSide(_is_bid=True)
        side.apply("50000.00", "1.5")
        assert len(side) == 1

    def test_apply_zero_removes_level(self) -> None:
        side = BookSide(_is_bid=True)
        side.apply("50000.00", "1.5")
        side.apply("50000.00", "0.0")
        assert len(side) == 0

    def test_negative_qty_raises(self) -> None:
        side = BookSide(_is_bid=True)
        with pytest.raises(BookInvariantError, match="negative"):
            side.apply("50000.00", "-1.0")

    def test_bid_best_is_highest_price(self) -> None:
        side = BookSide(_is_bid=True)
        side.apply("49000.00", "1.0")
        side.apply("50000.00", "2.0")
        side.apply("48000.00", "3.0")
        assert side.best().price == Decimal("50000.00")

    def test_ask_best_is_lowest_price(self) -> None:
        side = BookSide(_is_bid=False)
        side.apply("50001.00", "1.0")
        side.apply("50000.00", "0.5")
        assert side.best().price == Decimal("50000.00")

    def test_best_on_empty_side_is_none(self) -> None:
        assert BookSide(_is_bid=True).best() is None

    def test_levels_sorted_bids_descending(self) -> None:
        side = BookSide(_is_bid=True)
        for p in ["49000", "50000", "48000"]:
            side.apply(p, "1.0")
        prices = [lv.price for lv in side.levels()]
        assert prices == sorted(prices, reverse=True)

    def test_levels_sorted_asks_ascending(self) -> None:
        side = BookSide(_is_bid=False)
        for p in ["50002", "50000", "50001"]:
            side.apply(p, "1.0")
        prices = [lv.price for lv in side.levels()]
        assert prices == sorted(prices)

    def test_levels_depth_clamp(self) -> None:
        side = BookSide(_is_bid=True)
        for i in range(10):
            side.apply(str(50000 + i), "1.0")
        assert len(side.levels(depth=3)) == 3


# ---------------------------------------------------------------------------
# L2Book — load_snapshot
# ---------------------------------------------------------------------------

class TestL2BookSnapshot:
    def _book(self) -> L2Book:
        book = L2Book("BTCUSDT")
        book.load_snapshot(
            last_update_id=100,
            bids=[["50000.00", "1.0"], ["49999.00", "2.0"]],
            asks=[["50001.00", "0.5"], ["50002.00", "1.5"]],
        )
        return book

    def test_is_ready_after_load(self) -> None:
        assert self._book().is_ready

    def test_not_ready_before_load(self) -> None:
        assert not L2Book("BTCUSDT").is_ready

    def test_last_update_id_set(self) -> None:
        assert self._book().last_update_id == 100

    def test_snapshot_has_correct_levels(self) -> None:
        snap = self._book().snapshot()
        assert snap.best_bid.price == Decimal("50000.00")
        assert snap.best_ask.price == Decimal("50001.00")

    def test_crossed_book_raises_on_load(self) -> None:
        book = L2Book("BTCUSDT")
        with pytest.raises(BookInvariantError, match="crossed"):
            book.load_snapshot(
                last_update_id=1,
                bids=[["50002.00", "1.0"]],   # bid > ask
                asks=[["50001.00", "0.5"]],
            )


# ---------------------------------------------------------------------------
# L2Book — apply_diff
# ---------------------------------------------------------------------------

class TestL2BookApplyDiff:
    def _seeded(self) -> L2Book:
        book = L2Book("BTCUSDT")
        book.load_snapshot(
            last_update_id=100,
            bids=[["50000.00", "1.0"]],
            asks=[["50001.00", "1.0"]],
        )
        return book

    def test_apply_diff_updates_level(self) -> None:
        book = self._seeded()
        book.apply_diff(U=101, u=102, event_time_ms=0,
                        bids=[["50000.00", "3.0"]], asks=[])
        assert book.snapshot().best_bid.qty == Decimal("3.0")

    def test_apply_diff_removes_zero_qty_level(self) -> None:
        book = self._seeded()
        book.apply_diff(U=101, u=102, event_time_ms=0,
                        bids=[["50000.00", "0.0"]], asks=[])
        assert book.snapshot().best_bid is None

    def test_apply_diff_increments_last_u(self) -> None:
        book = self._seeded()
        book.apply_diff(U=101, u=105, event_time_ms=0, bids=[], asks=[])
        assert book.last_update_id == 105

    def test_non_monotonic_u_raises(self) -> None:
        book = self._seeded()
        with pytest.raises(BookInvariantError, match="non-monotonic"):
            book.apply_diff(U=98, u=99, event_time_ms=0, bids=[], asks=[])

    def test_crossed_book_raises_on_diff(self) -> None:
        book = self._seeded()
        with pytest.raises(BookInvariantError, match="crossed"):
            book.apply_diff(
                U=101, u=102, event_time_ms=0,
                bids=[["50002.00", "1.0"]],   # bid > existing ask 50001
                asks=[],
            )

    def test_negative_qty_in_diff_raises(self) -> None:
        book = self._seeded()
        with pytest.raises(BookInvariantError, match="negative"):
            book.apply_diff(U=101, u=102, event_time_ms=0,
                            bids=[["49998.00", "-1.0"]], asks=[])


# ---------------------------------------------------------------------------
# BookSnapshot computed properties
# ---------------------------------------------------------------------------

class TestBookSnapshotProperties:
    def _snap(
        self,
        bid_price: str = "50000",
        bid_qty:   str = "2.0",
        ask_price: str = "50001",
        ask_qty:   str = "1.0",
    ) -> BookSnapshot:
        from market_data.book.l2 import BookLevel
        return BookSnapshot(
            symbol="BTCUSDT",
            last_update_id=1,
            event_time_ms=0,
            bids=[BookLevel(Decimal(bid_price), Decimal(bid_qty))],
            asks=[BookLevel(Decimal(ask_price), Decimal(ask_qty))],
        )

    def test_mid_price(self) -> None:
        snap = self._snap("50000", "1.0", "50002", "1.0")
        assert snap.mid == Decimal("50001")

    def test_spread(self) -> None:
        snap = self._snap("50000", "1.0", "50002", "1.0")
        assert snap.spread == Decimal("2")

    def test_microprice_bid_heavy(self) -> None:
        # bid_qty=2, ask_qty=1 → microprice closer to ask (ask × bid_qty dominates)
        # mp = (50001×2 + 50000×1) / 3 = 100002+50000/3 = 150002/3 = 50000.667
        snap = self._snap("50000", "2.0", "50001", "1.0")
        mp = snap.microprice
        assert mp is not None
        assert Decimal("50000") < mp < Decimal("50001")

    def test_microprice_symmetric(self) -> None:
        # Equal quantities → microprice == mid
        snap = self._snap("50000", "1.0", "50002", "1.0")
        assert snap.microprice == Decimal("50001")

    def test_imbalance_bid_heavy(self) -> None:
        snap = self._snap("50000", "3.0", "50001", "1.0")
        imb = snap.imbalance
        assert imb is not None
        assert imb > 0

    def test_imbalance_ask_heavy(self) -> None:
        snap = self._snap("50000", "1.0", "50001", "3.0")
        imb = snap.imbalance
        assert imb is not None
        assert imb < 0

    def test_imbalance_symmetric(self) -> None:
        snap = self._snap("50000", "1.0", "50001", "1.0")
        assert snap.imbalance == Decimal("0")

    def test_empty_book_properties_are_none(self) -> None:
        snap = BookSnapshot(
            symbol="X", last_update_id=0, event_time_ms=0, bids=[], asks=[]
        )
        assert snap.mid        is None
        assert snap.spread     is None
        assert snap.microprice is None
        assert snap.imbalance  is None


# ---------------------------------------------------------------------------
# DepthStateMachine
# ---------------------------------------------------------------------------

def _make_event(
    U: int, u: int, pu: int,
    bids: list | None = None,
    asks: list | None = None,
    ts: int = 0,
) -> dict:
    return {"U": U, "u": u, "pu": pu, "b": bids or [], "a": asks or [], "T": ts}


def _make_rest_client(last_update_id: int = 100) -> MagicMock:
    """Mock REST client returning a snapshot with the given lastUpdateId."""
    client = MagicMock()
    client.depth = AsyncMock(return_value={
        "lastUpdateId": last_update_id,
        "T": 0,
        "bids": [["50000.00", "1.0"]],
        "asks": [["50001.00", "1.0"]],
    })
    return client


class TestDepthStateMachineBuffering:
    async def test_events_buffered_before_start(self) -> None:
        machine = DepthStateMachine("BTCUSDT", _make_rest_client())
        event = _make_event(U=101, u=101, pu=100)
        await machine.on_depth_event(event)
        assert not machine.is_live
        assert len(machine._buffer) == 1

    async def test_is_live_after_start_with_valid_event(self) -> None:
        rest = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest)
        await machine.on_depth_event(_make_event(U=100, u=101, pu=99))
        await machine.start()
        assert machine.is_live


class TestDepthStateMachineStep3:
    async def test_stale_events_dropped(self) -> None:
        """Events where u < lastUpdateId must be silently dropped (step 3)."""
        rest = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest)
        await machine.on_depth_event(_make_event(U=90, u=95, pu=89))
        await machine.on_depth_event(_make_event(U=96, u=99, pu=95))
        await machine.on_depth_event(_make_event(U=100, u=101, pu=99))
        await machine.start()
        assert machine.is_live
        assert machine.stats.events_dropped == 2


class TestDepthStateMachineStep4:
    async def test_no_anchor_stays_syncing(self) -> None:
        """If no event bridges lastUpdateId, machine stays in SYNCING."""
        rest = _make_rest_client(last_update_id=200)
        machine = DepthStateMachine("BTCUSDT", rest)
        await machine.on_depth_event(_make_event(U=210, u=215, pu=209))
        await machine.start()
        assert not machine.is_live

    async def test_anchor_event_satisfies_U_le_last_plus1_le_u(self) -> None:
        rest = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest)
        # U=99, u=101 → 99 <= 101 <= 101 ✓
        await machine.on_depth_event(_make_event(U=99, u=101, pu=98))
        await machine.start()
        assert machine.is_live


class TestDepthStateMachineStep5:
    async def test_pu_continuity_break_triggers_restart(self) -> None:
        """pu of second event != u of first event → restart."""
        rest = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest)
        await machine.on_depth_event(_make_event(U=100, u=101, pu=99))
        await machine.start()
        assert machine.is_live

        # Second event: pu=999 but prev_u=101
        await machine.on_depth_event(_make_event(U=102, u=103, pu=999))
        assert machine.stats.restarts == 1
        assert not machine.is_live

    async def test_valid_continuation_increments_applied(self) -> None:
        rest = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest)
        await machine.on_depth_event(_make_event(U=100, u=101, pu=99))
        await machine.start()
        await machine.on_depth_event(_make_event(U=102, u=103, pu=101))
        assert machine.stats.events_applied == 2

    async def test_invariant_violation_triggers_restart(self) -> None:
        """A crossed-book update from a diff restarts the machine."""
        rest = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest)
        await machine.on_depth_event(_make_event(U=100, u=101, pu=99))
        await machine.start()
        bad_event = _make_event(
            U=102, u=103, pu=101,
            bids=[["50005.00", "1.0"]],   # bid > ask → crossed
        )
        await machine.on_depth_event(bad_event)
        assert machine.stats.restarts == 1


class TestDepthStateMachineHandlerDispatch:
    async def test_handler_called_when_live(self) -> None:
        handler = AsyncMock()
        rest    = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest, on_book_update=handler)
        await machine.on_depth_event(_make_event(U=100, u=101, pu=99))
        await machine.start()
        await machine.on_depth_event(_make_event(U=102, u=103, pu=101))
        await asyncio.sleep(0)
        assert handler.await_count >= 1

    async def test_handler_crash_does_not_restart(self) -> None:
        async def bad_handler(snap: object) -> None:
            raise RuntimeError("boom")

        rest    = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest, on_book_update=bad_handler)
        await machine.on_depth_event(_make_event(U=100, u=101, pu=99))
        await machine.start()
        await machine.on_depth_event(_make_event(U=102, u=103, pu=101))
        await asyncio.sleep(0)
        assert machine.stats.restarts == 0
        assert machine.is_live

    async def test_no_handler_no_error(self) -> None:
        rest    = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest)
        await machine.on_depth_event(_make_event(U=100, u=101, pu=99))
        await machine.start()
        await machine.on_depth_event(_make_event(U=102, u=103, pu=101))
        assert machine.is_live


class TestDepthStateMachineManualRestart:
    async def test_restart_clears_state(self) -> None:
        rest    = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest)
        await machine.on_depth_event(_make_event(U=100, u=101, pu=99))
        await machine.start()
        assert machine.is_live
        machine.restart("test_restart")
        assert not machine.is_live
        assert machine.stats.restarts == 1

    async def test_restart_clears_buffer(self) -> None:
        rest    = _make_rest_client(last_update_id=100)
        machine = DepthStateMachine("BTCUSDT", rest)
        await machine.on_depth_event(_make_event(U=50, u=55, pu=49))
        machine.restart("test")
        assert machine._buffer == []
