"""
Deribit REST adapter.

Endpoints:
  GET /api/v2/public/get_order_book?instrument_name=BTC-PERPETUAL&depth=20
  GET /api/v2/public/get_time → server time ms
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

_REST_BASE     = "https://www.deribit.com"
_DRIFT_HALT_MS = 1000


class DeribitRestAdapter(ExchangeRestAdapter):

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    @property
    def exchange(self) -> str:
        return "deribit"

    async def __aenter__(self) -> "DeribitRestAdapter":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def fetch_snapshot(self, symbol: str, limit: int = 20) -> NormalizedSnapshot:
        # Deribit max depth is 10000 but realistic values are 20-100
        depth = min(limit, 100)
        raw   = await self._get(
            "/api/v2/public/get_order_book",
            instrument_name=symbol.upper(), depth=depth,
        )
        result = raw["result"]

        # Deribit format: [[price, amount], ...]
        bids = [[str(p), str(q)] for p, q in result.get("bids", [])]
        asks = [[str(p), str(q)] for p, q in result.get("asks", [])]

        return NormalizedSnapshot(
            last_update_id=int(result.get("change_id", 0)),
            event_time_ms=int(result.get("timestamp", int(time.time() * 1000))),
            bids=bids,
            asks=asks,
        )

    async def check_clock_drift(self) -> int:
        t0     = time.time_ns() // 1_000_000
        raw    = await self._get("/api/v2/public/get_time")
        t1     = time.time_ns() // 1_000_000
        local  = (t0 + t1) // 2
        server = int(raw["result"])
        drift  = local - server
        if abs(drift) > _DRIFT_HALT_MS:
            raise ClockDriftError(f"Deribit clock drift {drift:+d}ms exceeds {_DRIFT_HALT_MS}ms")
        logger.debug("deribit clock drift {}ms", drift)
        return drift

    async def _get(self, path: str, **params: Any) -> Any:
        if self._session is None:
            raise RuntimeError("Must be used as async context manager")
        async with self._session.get(_REST_BASE + path, params=params or None) as resp:
            resp.raise_for_status()
            return await resp.json()
