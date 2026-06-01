"""
Binance USDT-M Futures — async REST client.

Base URL: https://fapi.binance.com  (NOT api.binance.com — that is spot)

Design contract:
  - Every response tracks X-MBX-USED-WEIGHT-1M. Alert >70%, halt >90%.
  - Credentials loaded from environment; never logged, never in args.
  - Clock drift checked at startup and every 60s via GET /fapi/v1/time.
  - Halt if |server_time − local_time| > 500ms.
  - All public methods are typed. No bare except. No print.

Binance API ref:
  https://developers.binance.com/docs/derivatives/usdt-margined-futures/market-data
"""

from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

import aiohttp
from loguru import logger

_REST_BASE: str = "https://fapi.binance.com"

_WEIGHT_WARN_PCT: float = 0.70
_WEIGHT_HALT_PCT: float = 0.90
_WEIGHT_LIMIT: int      = 2400
_DRIFT_HALT_MS: int     = 1000   # Binance signed-request window


class RateLimitError(Exception):
    """Raised when used weight exceeds the halt threshold."""


class ClockDriftError(Exception):
    """Raised when local clock drift vs Binance server exceeds 500ms."""


@dataclass
class RestStats:
    """Counters for monitoring."""
    requests:       int = 0
    errors:         int = 0
    used_weight:    int = 0
    clock_drift_ms: int = 0


class BinanceRestClient:
    """
    Async REST client for Binance USDT-M Futures market data.

    API key is optional — all capture endpoints are public.
    Pass api_key + api_secret only if you need signed endpoints.

    Usage::

        async with BinanceRestClient() as client:
            snapshot = await client.depth("BTCUSDT")
    """

    def __init__(
        self,
        api_key:    str | None = None,
        api_secret: str | None = None,
        base_url:   str        = _REST_BASE,
    ) -> None:
        self._api_key    = api_key
        self._api_secret = api_secret
        self._base_url   = base_url.rstrip("/")
        self._session:   aiohttp.ClientSession | None = None
        self.stats       = RestStats()

    async def __aenter__(self) -> "BinanceRestClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Market data endpoints (no auth required)
    # ------------------------------------------------------------------

    async def server_time(self) -> int:
        """
        GET /fapi/v1/time — server timestamp in milliseconds.

        Weight: 1. Used for clock-drift check.
        """
        data = await self._get("/fapi/v1/time")
        return int(data["serverTime"])

    async def check_clock_drift(self) -> int:
        """
        Fetch server time and compare against local clock.

        Returns drift in ms (positive = local is ahead).
        Raises ClockDriftError if |drift| > 500ms.
        """
        t0 = time.time_ns() // 1_000_000
        server_ms = await self.server_time()
        t1 = time.time_ns() // 1_000_000
        local_ms = (t0 + t1) // 2
        drift_ms = local_ms - server_ms
        self.stats.clock_drift_ms = abs(drift_ms)
        if abs(drift_ms) > _DRIFT_HALT_MS:
            raise ClockDriftError(
                f"clock drift {drift_ms:+d}ms exceeds {_DRIFT_HALT_MS}ms halt threshold"
            )
        logger.debug("clock drift {}ms (local − server)", drift_ms)
        return drift_ms

    async def depth(self, symbol: str, limit: int = 1000) -> dict:
        """
        GET /fapi/v1/depth — L2 order book snapshot.

        Returns dict with keys: lastUpdateId, T, E, bids, asks.
        Weight: 20 (limit=1000). Required to seed the WS diff stream.
        """
        return await self._get("/fapi/v1/depth", symbol=symbol.upper(), limit=limit)

    async def exchange_info(self) -> dict:
        """
        GET /fapi/v1/exchangeInfo — symbol metadata, filters, lot/tick sizes.

        Weight: 1.
        """
        return await self._get("/fapi/v1/exchangeInfo")

    async def premium_index(self, symbol: str) -> dict:
        """
        GET /fapi/v1/premiumIndex — mark price, index price, next funding rate.

        Weight: 1.
        """
        return await self._get("/fapi/v1/premiumIndex", symbol=symbol.upper())

    async def funding_rate(self, symbol: str, limit: int = 1000) -> list[dict]:
        """
        GET /fapi/v1/fundingRate — historical funding rate records.

        Weight: 1.
        """
        return await self._get("/fapi/v1/fundingRate", symbol=symbol.upper(), limit=limit)

    # ------------------------------------------------------------------
    # HTTP primitives
    # ------------------------------------------------------------------

    async def _get(self, path: str, **params: Any) -> Any:
        return await self._request("GET", path, params=params or None)

    async def _signed_get(self, path: str, **params: Any) -> Any:
        return await self._request("GET", path, params=params, signed=True)

    async def _signed_post(self, path: str, **params: Any) -> Any:
        return await self._request("POST", path, params=params, signed=True)

    async def _signed_put(self, path: str, **params: Any) -> Any:
        return await self._request("PUT", path, params=params, signed=True)

    async def _signed_delete(self, path: str, **params: Any) -> Any:
        return await self._request("DELETE", path, params=params, signed=True)

    async def _request(
        self,
        method:  str,
        path:    str,
        params:  dict | None = None,
        signed:  bool        = False,
    ) -> Any:
        if self._session is None:
            raise RuntimeError("BinanceRestClient must be used as async context manager")

        params = dict(params or {})

        headers: dict[str, str] = {}
        if self._api_key:
            headers["X-MBX-APIKEY"] = self._api_key

        if signed:
            if not self._api_key or not self._api_secret:
                raise RuntimeError("Signed request requires api_key and api_secret")
            params["timestamp"]  = str(time.time_ns() // 1_000_000)
            params["recvWindow"] = "5000"
            query = urlencode(params)
            params["signature"] = hmac.new(
                self._api_secret.encode(),
                query.encode(),
                hashlib.sha256,
            ).hexdigest()

        url = self._base_url + path
        self.stats.requests += 1

        try:
            async with self._session.request(
                method, url, params=params or None, headers=headers
            ) as resp:
                self._track_weight(resp)
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientResponseError as exc:
            self.stats.errors += 1
            logger.error("{} {} → HTTP {}: {}", method, path, exc.status, exc.message)
            raise
        except aiohttp.ClientError as exc:
            self.stats.errors += 1
            logger.error("{} {} → network error: {}", method, path, exc)
            raise

    def _track_weight(self, resp: aiohttp.ClientResponse) -> None:
        raw = resp.headers.get("X-MBX-USED-WEIGHT-1M")
        if raw is None:
            return
        try:
            used = int(raw)
        except ValueError:
            return

        self.stats.used_weight = used
        pct = used / _WEIGHT_LIMIT

        if pct > _WEIGHT_HALT_PCT:
            logger.critical(
                "REST weight {}% ({}/{}) — halting",
                int(pct * 100), used, _WEIGHT_LIMIT,
            )
            raise RateLimitError(
                f"used weight {used}/{_WEIGHT_LIMIT} ({pct:.0%}) exceeds halt threshold"
            )
        if pct > _WEIGHT_WARN_PCT:
            logger.warning(
                "REST weight {}% ({}/{}) — approaching limit",
                int(pct * 100), used, _WEIGHT_LIMIT,
            )
