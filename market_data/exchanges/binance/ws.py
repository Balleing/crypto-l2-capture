"""
Binance USDT-M Futures WebSocket adapter.

Implements ExchangeWsAdapter for Binance futures combined streams:
  @depth@100ms    — L2 incremental diff
  @aggTrade       — aggregated taker trade

Binance-specific normalisation:
  NormalizedDepthEvent.prev_update_id ← data["pu"]   (futures continuity field)
  NormalizedDepthEvent.first_update_id ← data["U"]
  NormalizedDepthEvent.last_update_id  ← data["u"]
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field

import websockets
import websockets.exceptions
from loguru import logger

from market_data.exchanges.base import (
    ExchangeWsAdapter,
    NormalizedDepthEvent,
    NormalizedTrade,
)

_WS_BASE         = "wss://fstream.binance.com"
_GAP_TIMEOUT_S   = 5.0
_RECONNECT_CAP_S = 60.0
_OPEN_TIMEOUT_S  = 10.0


class _GapDetected(Exception):
    pass


@dataclass
class WsStats:
    connects:        int = 0
    gaps:            int = 0
    messages_recv:   int = 0
    parse_errors:    int = 0
    dispatch_errors: int = 0


class BinanceWsAdapter(ExchangeWsAdapter):
    """
    Binance USDT-M Futures WebSocket adapter.

    Fires on_depth with NormalizedDepthEvent for every @depth@100ms message.
    Fires on_trade with NormalizedTrade for every @aggTrade message.
    Reconnects with exponential backoff on any failure.
    """

    def __init__(self, symbol: str, on_depth=None, on_trade=None) -> None:
        super().__init__(symbol, on_depth=on_depth, on_trade=on_trade)
        self.stats = WsStats()
        sym = self.symbol.lower()
        self._stream_depth = f"{sym}@depth@100ms"
        self._stream_trade = f"{sym}@aggTrade"

    @property
    def exchange(self) -> str:
        return "binance"

    def build_url(self) -> str:
        sym     = self.symbol.lower()
        streams = f"{sym}@depth@100ms/{sym}@aggTrade/{sym}@markPrice@1s/{sym}@bookTicker"
        return f"{_WS_BASE}/stream?streams={streams}"

    def parse_depth(self, raw: dict) -> NormalizedDepthEvent | None:
        stream = raw.get("stream", "")
        if not stream.endswith("@depth@100ms"):
            return None
        d = raw.get("data", raw)
        return NormalizedDepthEvent(
            first_update_id=int(d["U"]),
            last_update_id=int(d["u"]),
            prev_update_id=int(d["pu"]),   # futures continuity field
            event_time_ms=int(d.get("T", d.get("E", 0))),
            bids=d.get("b", []),
            asks=d.get("a", []),
            raw=d,
        )

    def parse_trade(self, raw: dict) -> NormalizedTrade | None:
        stream = raw.get("stream", "")
        if not stream.endswith("@aggTrade"):
            return None
        d = raw.get("data", raw)
        return NormalizedTrade(
            symbol=self.symbol,
            timestamp_ms=int(d["T"]),
            trade_id=int(d["a"]),
            price=str(d["p"]),
            qty=str(d["q"]),
            buyer_is_maker=bool(d["m"]),
        )

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._connect()
                backoff = 1.0
            except asyncio.CancelledError:
                logger.info("{} Binance WS shut down", self.symbol)
                return
            except _GapDetected:
                logger.info("{} gap: reconnecting immediately", self.symbol)
                backoff = 1.0
            except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as exc:
                logger.warning("{} WS error [{}]: {} — retry in {:.0f}s",
                               self.symbol, type(exc).__name__, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)
            except Exception as exc:
                logger.exception("{} unexpected WS error — retry in {:.0f}s: {}",
                                 self.symbol, backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)

    async def _connect(self) -> None:
        url = self.build_url()
        self.stats.connects += 1
        logger.info("{} connecting (#{}) → {}", self.symbol, self.stats.connects, url)
        async with websockets.connect(
            url, open_timeout=_OPEN_TIMEOUT_S, ping_interval=20, ping_timeout=30
        ) as ws:
            logger.info("{} connected", self.symbol)
            await self._recv_loop(ws)

    async def _recv_loop(self, ws) -> None:
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=_GAP_TIMEOUT_S)
            except asyncio.TimeoutError:
                self.stats.gaps += 1
                logger.warning("{} gap #{}: no event for {:.0f}s",
                               self.symbol, self.stats.gaps, _GAP_TIMEOUT_S)
                raise _GapDetected

            self.stats.messages_recv += 1
            await self._process(raw)

    async def _process(self, raw: str | bytes) -> None:
        try:
            msg: dict = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            self.stats.parse_errors += 1
            logger.error("{} JSON parse error: {}", self.symbol, exc)
            return

        depth = self.parse_depth(msg)
        if depth is not None and self._on_depth is not None:
            try:
                await self._on_depth(depth)
            except Exception as exc:
                self.stats.dispatch_errors += 1
                logger.exception("{} depth handler raised: {}", self.symbol, exc)
            return

        trade = self.parse_trade(msg)
        if trade is not None and self._on_trade is not None:
            try:
                await self._on_trade(trade)
            except Exception as exc:
                self.stats.dispatch_errors += 1
                logger.exception("{} trade handler raised: {}", self.symbol, exc)
