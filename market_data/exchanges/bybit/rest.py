"""
Bybit Linear REST adapter.

Endpoints:
  GET /v5/market/orderbook?category=linear&symbol=BTCUSDT&limit=200
  GET /v5/market/time   → server time for clock drift
"""

from __future__ import annotations

import time
from typing import Any

import aiohttp
from loguru import logger

from market_data.exchanges.base import (
    ClockDriftError,
    ExchangeRestAdapter,
    NormalizedSnapshot,
)

_REST_BASE     = "https://api.bybit.com"
_DRIFT_HALT_MS = 1000


class BybitRestAdapter(ExchangeRestAdapter):

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    @property
    def exchange(self) -> str:
        return "bybit"

    async def __aenter__(self) -> "BybitRestAdapter":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def fetch_snapshot(self, symbol: str, limit: int = 200) -> NormalizedSnapshot:
        # Bybit max depth is 200 for linear
        limit = min(limit, 200)
        raw   = await self._get(
            "/v5/market/orderbook",
            category="linear", symbol=symbol.upper(), limit=limit,
        )
        data = raw["result"]
        return NormalizedSnapshot(
            last_update_id=int(data.get("u", data.get("seq", 0))),
            event_time_ms=int(data.get("ts", int(time.time() * 1000))),
            bids=[[str(p), str(q)] for p, q in data.get("b", [])],
            asks=[[str(p), str(q)] for p, q in data.get("a", [])],
        )

    async def check_clock_drift(self) -> int:
        t0      = time.time_ns() // 1_000_000
        raw     = await self._get("/v5/market/time")
        t1      = time.time_ns() // 1_000_000
        local   = (t0 + t1) // 2
        server  = int(raw["result"]["timeMillisecond"])
        drift   = local - server
        if abs(drift) > _DRIFT_HALT_MS:
            raise ClockDriftError(f"Bybit clock drift {drift:+d}ms exceeds {_DRIFT_HALT_MS}ms")
        logger.debug("bybit clock drift {}ms", drift)
        return drift

    async def _get(self, path: str, **params: Any) -> Any:
        if self._session is None:
            raise RuntimeError("Must be used as async context manager")
        async with self._session.get(_REST_BASE + path, params=params or None) as resp:
            resp.raise_for_status()
            return await resp.json()
