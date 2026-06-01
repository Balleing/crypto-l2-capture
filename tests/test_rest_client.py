"""
Unit tests for BinanceRestClient.

All tests mock aiohttp.ClientSession so no real network calls are made.
Tests cover: rate-limit tracking, clock drift detection, signing infrastructure,
and error propagation.
"""

from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from market_data.feeds.binance_rest import (
    BinanceRestClient,
    ClockDriftError,
    RateLimitError,
    RestStats,
    _DRIFT_HALT_MS,
    _WEIGHT_HALT_PCT,
    _WEIGHT_LIMIT,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(body: dict, weight: int = 10, status: int = 200) -> MagicMock:
    """Build a mock aiohttp response."""
    resp = MagicMock()
    resp.status = status
    resp.headers = {"X-MBX-USED-WEIGHT-1M": str(weight)}
    resp.json = AsyncMock(return_value=body)
    resp.raise_for_status = MagicMock()
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__  = AsyncMock(return_value=False)
    return resp


def _make_session(response: MagicMock) -> MagicMock:
    session = MagicMock()
    session.request = MagicMock(return_value=response)
    session.close   = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__  = AsyncMock(return_value=False)
    return session


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    async def test_requires_context_manager(self) -> None:
        client = BinanceRestClient()
        with pytest.raises(RuntimeError, match="context manager"):
            await client.server_time()

    async def test_opens_and_closes_session(self) -> None:
        resp    = _make_response({"serverTime": 1_700_000_000_000})
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                await client.server_time()
        session.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# Weight tracking
# ---------------------------------------------------------------------------

class TestWeightTracking:
    async def test_used_weight_updated_from_header(self) -> None:
        resp    = _make_response({"serverTime": 1_000_000}, weight=500)
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                await client.server_time()
        assert client.stats.used_weight == 500

    async def test_above_halt_threshold_raises(self) -> None:
        halt_weight = int(_WEIGHT_LIMIT * _WEIGHT_HALT_PCT) + 1
        resp    = _make_response({"serverTime": 1_000_000}, weight=halt_weight)
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                with pytest.raises(RateLimitError):
                    await client.server_time()

    async def test_below_halt_threshold_no_error(self) -> None:
        safe_weight = int(_WEIGHT_LIMIT * _WEIGHT_HALT_PCT) - 1
        resp    = _make_response({"serverTime": 1_000_000}, weight=safe_weight)
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                await client.server_time()

    async def test_missing_weight_header_no_error(self) -> None:
        resp = _make_response({"serverTime": 1_000_000})
        resp.headers = {}
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                await client.server_time()
        assert client.stats.used_weight == 0


# ---------------------------------------------------------------------------
# Clock drift
# ---------------------------------------------------------------------------

class TestClockDrift:
    async def test_small_drift_ok(self) -> None:
        now_ms = int(time.time() * 1000)
        resp    = _make_response({"serverTime": now_ms})
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                drift = await client.check_clock_drift()
        assert abs(drift) < 200

    async def test_large_drift_raises(self) -> None:
        server_time_ms = int(time.time() * 1000) - 2000   # 2 seconds behind
        resp    = _make_response({"serverTime": server_time_ms})
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                with pytest.raises(ClockDriftError):
                    await client.check_clock_drift()

    async def test_drift_stored_in_stats(self) -> None:
        now_ms = int(time.time() * 1000)
        resp    = _make_response({"serverTime": now_ms})
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                await client.check_clock_drift()
        assert client.stats.clock_drift_ms >= 0


# ---------------------------------------------------------------------------
# Stats counters
# ---------------------------------------------------------------------------

class TestStats:
    def test_initial_stats_zero(self) -> None:
        s = BinanceRestClient().stats
        assert s.requests       == 0
        assert s.errors         == 0
        assert s.used_weight    == 0
        assert s.clock_drift_ms == 0

    async def test_request_counter_increments(self) -> None:
        resp    = _make_response({"serverTime": 1_000_000})
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                await client.server_time()
                await client.server_time()
        assert client.stats.requests == 2


# ---------------------------------------------------------------------------
# Depth endpoint shape
# ---------------------------------------------------------------------------

class TestDepthEndpoint:
    async def test_depth_returns_dict_with_expected_keys(self) -> None:
        body = {
            "lastUpdateId": 123456,
            "T": 1_700_000_000_000,
            "E": 1_700_000_000_001,
            "bids": [["50000.00", "1.5"]],
            "asks": [["50001.00", "0.8"]],
        }
        resp    = _make_response(body)
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                result = await client.depth("BTCUSDT")
        assert result["lastUpdateId"] == 123456
        assert len(result["bids"])    == 1
        assert len(result["asks"])    == 1

    async def test_depth_uppercases_symbol(self) -> None:
        body    = {"lastUpdateId": 1, "bids": [], "asks": [], "T": 0, "E": 0}
        resp    = _make_response(body)
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                await client.depth("ethusdt")
        call_kwargs = session.request.call_args
        assert "ETHUSDT" in str(call_kwargs)


# ---------------------------------------------------------------------------
# Signing infrastructure
# ---------------------------------------------------------------------------

class TestSigning:
    async def test_signed_request_requires_credentials(self) -> None:
        """_signed_get must raise if credentials were not provided."""
        resp    = _make_response({})
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient() as client:
                with pytest.raises(RuntimeError, match="Signed request"):
                    await client._signed_get("/fapi/v2/account")

    async def test_signed_request_includes_signature(self) -> None:
        """HMAC signature and timestamp must appear in request params."""
        body    = {}
        resp    = _make_response(body)
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient(api_key="key", api_secret="secret") as client:
                await client._signed_get("/fapi/v2/account")
        call_kwargs = session.request.call_args
        params = call_kwargs[1].get("params") or call_kwargs[0][2]
        assert "signature" in params
        assert "timestamp" in params

    async def test_api_key_in_header(self) -> None:
        """X-MBX-APIKEY header must be set when api_key is provided."""
        body    = {}
        resp    = _make_response(body)
        session = _make_session(resp)
        with patch("aiohttp.ClientSession", return_value=session):
            async with BinanceRestClient(api_key="mykey", api_secret="secret") as client:
                await client._signed_get("/fapi/v2/account")
        headers = session.request.call_args[1].get("headers", {})
        assert headers.get("X-MBX-APIKEY") == "mykey"
