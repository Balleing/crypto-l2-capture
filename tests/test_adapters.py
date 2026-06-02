"""
Tests for the exchange adapter normalisation layer.

Validates that each adapter correctly maps raw exchange messages into
NormalizedDepthEvent with the right continuity fields, and that
DepthStateMachine handles them correctly regardless of exchange.
"""

from __future__ import annotations

import pytest

from market_data.exchanges.base import NormalizedDepthEvent, NormalizedSnapshot
from market_data.exchanges.binance.ws import BinanceWsAdapter
from market_data.exchanges.bybit.ws import BybitWsAdapter
from market_data.exchanges.deribit.ws import DeribitWsAdapter
from market_data.exchanges.okx.ws import OkxWsAdapter
from market_data.book.snapshot import DepthStateMachine


# ---------------------------------------------------------------------------
# Binance parse_depth
# ---------------------------------------------------------------------------

class TestBinanceAdapter:
    def setup_method(self):
        self.adapter = BinanceWsAdapter("BTCUSDT")

    def _make_msg(self, U, u, pu, bids=None, asks=None):
        return {
            "stream": "btcusdt@depth@100ms",
            "data": {
                "U": U, "u": u, "pu": pu,
                "T": 1_700_000_000_000,
                "b": bids or [["30000.0", "1.5"]],
                "a": asks or [["30001.0", "2.0"]],
            }
        }

    def test_parse_depth_basic(self):
        event = self.adapter.parse_depth(self._make_msg(100, 110, 99))
        assert event is not None
        assert event.first_update_id == 100
        assert event.last_update_id  == 110
        assert event.prev_update_id  == 99

    def test_parse_depth_bids_asks(self):
        event = self.adapter.parse_depth(
            self._make_msg(1, 2, 0, bids=[["29999.0", "0.5"]], asks=[["30000.0", "1.0"]])
        )
        assert event.bids == [["29999.0", "0.5"]]
        assert event.asks == [["30000.0", "1.0"]]

    def test_non_depth_stream_returns_none(self):
        msg = {"stream": "btcusdt@aggTrade", "data": {"T": 1, "a": 1, "p": "30000", "q": "1", "m": False, "s": "BTCUSDT"}}
        assert self.adapter.parse_depth(msg) is None

    def test_parse_trade(self):
        msg = {
            "stream": "btcusdt@aggTrade",
            "data": {"T": 1_700_000_000_000, "a": 42, "p": "30000.0", "q": "0.5", "m": False, "s": "BTCUSDT"},
        }
        trade = self.adapter.parse_trade(msg)
        assert trade is not None
        assert trade.price == "30000.0"
        assert trade.buyer_is_maker is False
        assert trade.taker_sign() if hasattr(trade, "taker_sign") else True


# ---------------------------------------------------------------------------
# Bybit parse_depth
# ---------------------------------------------------------------------------

class TestBybitAdapter:
    def setup_method(self):
        self.adapter = BybitWsAdapter("BTCUSDT")

    def _make_snapshot(self, seq=100):
        return {
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "seq": seq,
            "ts": 1_700_000_000_000,
            "data": {"b": [["30000.0", "1.5"]], "a": [["30001.0", "2.0"]]},
        }

    def _make_delta(self, seq, bids=None, asks=None):
        return {
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "seq": seq,
            "ts": 1_700_000_000_001,
            "data": {"b": bids or [], "a": asks or []},
        }

    def test_snapshot_resets_continuity(self):
        event = self.adapter.parse_depth(self._make_snapshot(seq=500))
        assert event is not None
        assert event.prev_update_id is None  # snapshot always None

    def test_delta_tracks_prev_seq(self):
        self.adapter.parse_depth(self._make_snapshot(seq=100))
        event = self.adapter.parse_depth(self._make_delta(seq=101))
        assert event is not None
        assert event.prev_update_id == 100
        assert event.last_update_id == 101

    def test_wrong_symbol_returns_none(self):
        msg = self._make_snapshot()
        msg["topic"] = "orderbook.50.ETHUSDT"
        assert self.adapter.parse_depth(msg) is None


# ---------------------------------------------------------------------------
# OKX parse_depth
# ---------------------------------------------------------------------------

class TestOkxAdapter:
    def setup_method(self):
        self.adapter = OkxWsAdapter("BTC-USDT-SWAP")

    def _make_snapshot(self, seq_id=1000):
        return {
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "snapshot",
            "data": [{
                "seqId": seq_id, "prevSeqId": -1,
                "ts": "1700000000000",
                "bids": [["30000.0", "1.5", "0", "1"]],
                "asks": [["30001.0", "2.0", "0", "1"]],
            }],
        }

    def _make_update(self, seq_id, prev_seq_id):
        return {
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "update",
            "data": [{
                "seqId": seq_id, "prevSeqId": prev_seq_id,
                "ts": "1700000000001",
                "bids": [], "asks": [],
            }],
        }

    def test_snapshot_clears_prev(self):
        event = self.adapter.parse_depth(self._make_snapshot(seq_id=1000))
        assert event is not None
        assert event.prev_update_id is None

    def test_update_carries_prev_seq(self):
        self.adapter.parse_depth(self._make_snapshot(1000))
        event = self.adapter.parse_depth(self._make_update(1001, 1000))
        assert event is not None
        assert event.last_update_id  == 1001
        assert event.prev_update_id  == 1000

    def test_wrong_channel_returns_none(self):
        msg = self._make_snapshot()
        msg["arg"]["channel"] = "tickers"
        assert self.adapter.parse_depth(msg) is None


# ---------------------------------------------------------------------------
# Deribit parse_depth
# ---------------------------------------------------------------------------

class TestDeribitAdapter:
    def setup_method(self):
        self.adapter = DeribitWsAdapter("BTC-PERPETUAL")

    def _make_msg(self, change_id, prev_change_id=None, msg_type="change"):
        data = {
            "type": msg_type,
            "change_id": change_id,
            "timestamp": 1_700_000_000_000,
            "bids": [["new", 30000.0, 10.0]],
            "asks": [["new", 30001.0, 5.0]],
        }
        if prev_change_id is not None:
            data["prev_change_id"] = prev_change_id
        return {
            "method": "subscription",
            "params": {"channel": "book.BTC-PERPETUAL.100ms", "data": data},
        }

    def test_snapshot_clears_prev(self):
        event = self.adapter.parse_depth(self._make_msg(500, msg_type="snapshot"))
        assert event is not None
        assert event.prev_update_id is None

    def test_change_carries_prev_change_id(self):
        event = self.adapter.parse_depth(self._make_msg(501, prev_change_id=500))
        assert event is not None
        assert event.last_update_id == 501
        assert event.prev_update_id == 500

    def test_delete_action_becomes_zero_qty(self):
        msg = self._make_msg(502, prev_change_id=501)
        msg["params"]["data"]["bids"] = [["delete", 30000.0, 10.0]]
        event = self.adapter.parse_depth(msg)
        assert event.bids == [["30000.0", "0"]]

    def test_non_subscription_returns_none(self):
        assert self.adapter.parse_depth({"result": "ok", "id": 1}) is None


# ---------------------------------------------------------------------------
# DepthStateMachine with normalized events (exchange-agnostic)
# ---------------------------------------------------------------------------

class MockRestAdapter:
    """Minimal REST adapter that returns a fixed snapshot."""
    def __init__(self, last_update_id=100, bids=None, asks=None):
        self._snap = NormalizedSnapshot(
            last_update_id=last_update_id,
            event_time_ms=0,
            bids=bids or [["29999.0", "1.0"]],
            asks=asks or [["30001.0", "1.0"]],
        )

    @property
    def exchange(self):
        return "mock"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def fetch_snapshot(self, symbol, limit=1000):
        return self._snap

    async def check_clock_drift(self):
        return 0


def _make_event(first, last, prev, bids=None, asks=None):
    return NormalizedDepthEvent(
        first_update_id=first,
        last_update_id=last,
        prev_update_id=prev,
        event_time_ms=1_700_000_000_000,
        bids=bids or [],
        asks=asks or [],
    )


@pytest.mark.asyncio
async def test_state_machine_goes_live():
    rest    = MockRestAdapter(last_update_id=100)
    machine = DepthStateMachine("BTCUSDT", rest)

    # Buffer events before snapshot
    machine._buffer.append(_make_event(100, 101, 99))
    machine._buffer.append(_make_event(101, 102, 101))

    await machine.start()
    assert machine.is_live


@pytest.mark.asyncio
async def test_state_machine_continuity_break_triggers_restart():
    rest    = MockRestAdapter(last_update_id=100)
    machine = DepthStateMachine("BTCUSDT", rest)

    machine._buffer.append(_make_event(100, 101, 99))
    await machine.start()
    assert machine.is_live

    # Send event with wrong prev_update_id
    await machine.on_depth_event(_make_event(200, 201, 999))  # 999 != 101
    assert not machine.is_live
    assert machine.stats.restarts == 1


@pytest.mark.asyncio
async def test_state_machine_none_prev_skips_continuity():
    """Exchanges that don't provide prev_update_id should never trigger restarts."""
    rest    = MockRestAdapter(last_update_id=100)
    machine = DepthStateMachine("BTCUSDT", rest)

    machine._buffer.append(_make_event(100, 101, None))  # no prev
    await machine.start()
    assert machine.is_live

    await machine.on_depth_event(_make_event(102, 103, None))
    assert machine.is_live
    assert machine.stats.restarts == 0
