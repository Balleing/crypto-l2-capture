"""
Binance USDT-M Futures REST adapter.

Implements ExchangeRestAdapter for:
  - GET /fapi/v1/depth   → NormalizedSnapshot
  - GET /fapi/v1/time    → clock drift check
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

_REST_BASE      = "https://fapi.binance.com"
_DRIFT_HALT_MS  = 1000
_WEIGHT_LIMIT   = 2400
_WEIGHT_WARN    = 0.70
_WEIGHT_HALT    = 0.90


class BinanceRestAdapter(ExchangeRestAdapter):
    """Binance USDT-M Futures REST adapter."""

    def __init__(self, api_key: str | None = None, api_secret: str | None = None) -> None:
        self._api_key    = api_key
        self._api_secret = api_secret
        self._session:   aiohttp.ClientSession | None = None
        self._used_weight = 0

    @property
    def exchange(self) -> str:
        return "binance"

    async def __aenter__(self) -> "BinanceRestAdapter":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def fetch_snapshot(self, symbol: str, limit: int = 1000) -> NormalizedSnapshot:
        raw = await self._get("/fapi/v1/depth", symbol=symbol.upper(), limit=limit)
        return NormalizedSnapshot(
            last_update_id=int(raw["lastUpdateId"]),
            event_time_ms=int(raw.get("T", 0)),
            bids=raw["bids"],
            asks=raw["asks"],
        )

    async def check_clock_drift(self) -> int:
        t0        = time.time_ns() // 1_000_000
        data      = await self._get("/fapi/v1/time")
        t1        = time.time_ns() // 1_000_000
        local_ms  = (t0 + t1) // 2
        drift_ms  = local_ms - int(data["serverTime"])
        if abs(drift_ms) > _DRIFT_HALT_MS:
            raise ClockDriftError(
                f"clock drift {drift_ms:+d}ms exceeds {_DRIFT_HALT_MS}ms halt threshold"
            )
        logger.debug("binance clock drift {}ms", drift_ms)
        return drift_ms

    # ------------------------------------------------------------------
    # Pass-through helpers kept for capture.py compatibility
    # ------------------------------------------------------------------

    async def premium_index(self, symbol: str) -> dict:
        return await self._get("/fapi/v1/premiumIndex", symbol=symbol.upper())

    # ------------------------------------------------------------------
    # HTTP primitives
    # ------------------------------------------------------------------

    async def _get(self, path: str, **params: Any) -> Any:
        if self._session is None:
            raise RuntimeError("Must be used as async context manager")
        headers: dict[str, str] = {}
        if self._api_key:
            headers["X-MBX-APIKEY"] = self._api_key
        url = _REST_BASE + path
        async with self._session.get(url, params=params or None, headers=headers) as resp:
            self._track_weight(resp)
            resp.raise_for_status()
            return await resp.json()

    def _track_weight(self, resp: aiohttp.ClientResponse) -> None:
        raw = resp.headers.get("X-MBX-USED-WEIGHT-1M")
        if not raw:
            return
        try:
            used = int(raw)
        except ValueError:
            return
        self._used_weight = used
        pct = used / _WEIGHT_LIMIT
        if pct > _WEIGHT_HALT:
            raise RuntimeError(f"Binance weight {used}/{_WEIGHT_LIMIT} ({pct:.0%}) — halting")
        if pct > _WEIGHT_WARN:
            logger.warning("Binance weight {}% ({}/{})", int(pct * 100), used, _WEIGHT_LIMIT)
