"""
OKX REST adapter.

Endpoints:
  GET /api/v5/market/books?instId=BTC-USDT-SWAP&sz=400  → snapshot
  GET /api/v5/public/time                                → server time
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

_REST_BASE     = "https://www.okx.com"
_DRIFT_HALT_MS = 1000


class OkxRestAdapter(ExchangeRestAdapter):

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    @property
    def exchange(self) -> str:
        return "okx"

    async def __aenter__(self) -> "OkxRestAdapter":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def fetch_snapshot(self, symbol: str, limit: int = 400) -> NormalizedSnapshot:
        limit = min(limit, 400)
        raw   = await self._get("/api/v5/market/books", instId=symbol, sz=str(limit))
        data  = raw["data"][0]
        return NormalizedSnapshot(
            last_update_id=int(data.get("seqId", 0)),
            event_time_ms=int(data.get("ts", int(time.time() * 1000))),
            bids=[[str(p), str(q)] for p, q, *_ in data.get("bids", [])],
            asks=[[str(p), str(q)] for p, q, *_ in data.get("asks", [])],
        )

    async def check_clock_drift(self) -> int:
        t0     = time.time_ns() // 1_000_000
        raw    = await self._get("/api/v5/public/time")
        t1     = time.time_ns() // 1_000_000
        local  = (t0 + t1) // 2
        server = int(raw["data"][0]["ts"])
        drift  = local - server
        if abs(drift) > _DRIFT_HALT_MS:
            raise ClockDriftError(f"OKX clock drift {drift:+d}ms exceeds {_DRIFT_HALT_MS}ms")
        logger.debug("okx clock drift {}ms", drift)
        return drift

    async def _get(self, path: str, **params: Any) -> Any:
        if self._session is None:
            raise RuntimeError("Must be used as async context manager")
        async with self._session.get(_REST_BASE + path, params=params or None) as resp:
            resp.raise_for_status()
            return await resp.json()
