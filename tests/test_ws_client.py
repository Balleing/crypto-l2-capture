"""
Unit tests for BinanceWsClient — message dispatch and URL construction.

These tests exercise the client's internal logic without a real WebSocket
connection. The _process() method is called directly with synthetic message
payloads so dispatch routing, error handling, and stat counters can all be
verified deterministically.
"""

from __future__ import annotations

import json

import pytest
from unittest.mock import AsyncMock, MagicMock

from market_data.feeds.binance_ws import BinanceWsClient, _GapDetected


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------

class TestBuildUrl:
    def test_uses_fstream_base(self) -> None:
        client = BinanceWsClient("BTCUSDT")
        assert client.build_url().startswith("wss://fstream.binance.com")

    def test_contains_all_four_streams(self) -> None:
        client = BinanceWsClient("BTCUSDT")
        url = client.build_url()
        assert "btcusdt@depth@100ms"  in url
        assert "btcusdt@aggTrade"     in url
        assert "btcusdt@markPrice@1s" in url
        assert "btcusdt@bookTicker"   in url

    def test_symbol_lowercased(self) -> None:
        url = BinanceWsClient("ETHUSDT").build_url()
        assert "ethusdt@" in url
        assert "ETHUSDT@" not in url

    def test_streams_joined_with_slash(self) -> None:
        url = BinanceWsClient("BTCUSDT").build_url()
        path = url.split("?streams=")[1]
        parts = path.split("/")
        assert len(parts) == 4

    def test_different_symbols_produce_different_urls(self) -> None:
        assert BinanceWsClient("BTCUSDT").build_url() != BinanceWsClient("ETHUSDT").build_url()


# ---------------------------------------------------------------------------
# Dispatch routing
# ---------------------------------------------------------------------------

class TestDispatch:
    def _msg(self, stream: str, data: dict) -> str:
        return json.dumps({"stream": stream, "data": data})

    async def test_depth_handler_called_with_data(self) -> None:
        handler = AsyncMock()
        client  = BinanceWsClient("BTCUSDT", on_depth=handler)
        await client._process(self._msg("btcusdt@depth@100ms", {"U": 1, "u": 2, "b": [], "a": []}))
        handler.assert_awaited_once()
        assert handler.call_args[0][0]["U"] == 1

    async def test_agg_trade_handler_called(self) -> None:
        handler = AsyncMock()
        client  = BinanceWsClient("BTCUSDT", on_agg_trade=handler)
        await client._process(self._msg("btcusdt@aggTrade", {"p": "50000.0", "q": "0.01", "m": False}))
        handler.assert_awaited_once()
        assert handler.call_args[0][0]["p"] == "50000.0"

    async def test_mark_price_handler_called(self) -> None:
        handler = AsyncMock()
        client  = BinanceWsClient("BTCUSDT", on_mark_price=handler)
        await client._process(self._msg("btcusdt@markPrice@1s", {"p": "50001.0"}))
        handler.assert_awaited_once()

    async def test_book_ticker_handler_called(self) -> None:
        handler = AsyncMock()
        client  = BinanceWsClient("BTCUSDT", on_book_ticker=handler)
        await client._process(self._msg("btcusdt@bookTicker", {"b": "49999.9", "a": "50000.1"}))
        handler.assert_awaited_once()

    async def test_wrong_symbol_handler_not_called(self) -> None:
        """Handler registered for BTCUSDT must not fire on an ETHUSDT stream."""
        handler = AsyncMock()
        client  = BinanceWsClient("BTCUSDT", on_depth=handler)
        await client._process(self._msg("ethusdt@depth@100ms", {"U": 1}))
        handler.assert_not_awaited()

    async def test_unregistered_stream_ignored(self) -> None:
        handler = AsyncMock()
        client  = BinanceWsClient("BTCUSDT", on_depth=handler)
        await client._process(self._msg("btcusdt@kline_1m", {"k": {}}))
        handler.assert_not_awaited()

    async def test_no_handlers_registered_no_error(self) -> None:
        client = BinanceWsClient("BTCUSDT")
        await client._process(self._msg("btcusdt@depth@100ms", {"U": 1}))

    async def test_only_registered_handler_fires(self) -> None:
        """When four handlers exist, only the matching one is called."""
        depth_h  = AsyncMock()
        trade_h  = AsyncMock()
        mark_h   = AsyncMock()
        ticker_h = AsyncMock()
        client = BinanceWsClient(
            "BTCUSDT",
            on_depth=depth_h, on_agg_trade=trade_h,
            on_mark_price=mark_h, on_book_ticker=ticker_h,
        )
        await client._process(self._msg("btcusdt@aggTrade", {"p": "1.0"}))
        trade_h.assert_awaited_once()
        depth_h.assert_not_awaited()
        mark_h.assert_not_awaited()
        ticker_h.assert_not_awaited()


# ---------------------------------------------------------------------------
# Error handling and stat counters
# ---------------------------------------------------------------------------

class TestErrorHandling:
    async def test_invalid_json_increments_parse_errors(self) -> None:
        client = BinanceWsClient("BTCUSDT")
        await client._process("not json {{{{")
        assert client.stats.parse_errors == 1

    async def test_invalid_json_does_not_raise(self) -> None:
        client = BinanceWsClient("BTCUSDT")
        await client._process("{broken")

    async def test_handler_exception_increments_dispatch_errors(self) -> None:
        async def bad_handler(data: dict) -> None:
            raise RuntimeError("handler blew up")

        client = BinanceWsClient("BTCUSDT", on_depth=bad_handler)
        await client._process(json.dumps({"stream": "btcusdt@depth@100ms", "data": {}}))
        assert client.stats.dispatch_errors == 1

    async def test_handler_exception_does_not_propagate(self) -> None:
        """A crashing handler must never tear down the WS connection."""
        async def bad_handler(data: dict) -> None:
            raise ValueError("boom")

        client = BinanceWsClient("BTCUSDT", on_depth=bad_handler)
        await client._process(json.dumps({"stream": "btcusdt@depth@100ms", "data": {}}))

    async def test_multiple_parse_errors_accumulate(self) -> None:
        client = BinanceWsClient("BTCUSDT")
        for _ in range(3):
            await client._process("bad")
        assert client.stats.parse_errors == 3

    async def test_successful_dispatch_does_not_increment_errors(self) -> None:
        handler = AsyncMock()
        client  = BinanceWsClient("BTCUSDT", on_depth=handler)
        await client._process(json.dumps({"stream": "btcusdt@depth@100ms", "data": {"U": 1}}))
        assert client.stats.parse_errors    == 0
        assert client.stats.dispatch_errors == 0


# ---------------------------------------------------------------------------
# Stats counters
# ---------------------------------------------------------------------------

class TestStats:
    def test_initial_stats_are_zero(self) -> None:
        s = BinanceWsClient("BTCUSDT").stats
        assert s.connects        == 0
        assert s.gaps            == 0
        assert s.messages_recv   == 0
        assert s.parse_errors    == 0
        assert s.dispatch_errors == 0

    async def test_process_does_not_increment_messages_recv(self) -> None:
        # messages_recv is incremented in _recv_loop (after ws.recv()), not in _process
        client = BinanceWsClient("BTCUSDT")
        await client._process(json.dumps({"stream": "btcusdt@depth@100ms", "data": {}}))
        assert client.stats.messages_recv == 0


# ---------------------------------------------------------------------------
# Bytes payload (websockets can deliver bytes)
# ---------------------------------------------------------------------------

class TestBytesPayload:
    async def test_bytes_payload_dispatched_correctly(self) -> None:
        handler = AsyncMock()
        client  = BinanceWsClient("BTCUSDT", on_depth=handler)
        raw     = json.dumps({"stream": "btcusdt@depth@100ms", "data": {"U": 99}}).encode()
        await client._process(raw)
        handler.assert_awaited_once()
        assert handler.call_args[0][0]["U"] == 99

    async def test_invalid_bytes_increments_parse_errors(self) -> None:
        client = BinanceWsClient("BTCUSDT")
        await client._process(b"\xff\xfe invalid utf8 bytes")
        assert client.stats.parse_errors == 1
