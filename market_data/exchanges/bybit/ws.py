"""
Bybit Linear (USDT Perpetual) WebSocket adapter.

Protocol:
  WS endpoint:  wss://stream.bybit.com/v5/public/linear
  Subscribe:    {"op": "subscribe", "args": ["orderbook.50.BTCUSDT", "publicTrade.BTCUSDT"]}
  Snapshot:     first message per topic has type="snapshot"
  Delta:        subsequent messages have type="delta"

Continuity field:
  Each orderbook message carries "seq" (uint64). The delta messages are
  contiguous when seq_n == seq_{n-1} + 1. Unlike Binance there is no
  separate "prev_seq" field — we track it ourselves and map it to
  NormalizedDepthEvent.prev_update_id.

  On sequence gap → restart (same protocol as Binance pu break).

Ref: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field

import websockets
import websockets.exceptions
from loguru import logger

from market_data.exchanges.base import (
    ExchangeWsAdapter,
    NormalizedDepthEvent,
    NormalizedTrade,
)

_WS_URL          = "wss://stream.bybit.com/v5/public/linear"
_GAP_TIMEOUT_S   = 5.0
_RECONNECT_CAP_S = 60.0
_OPEN_TIMEOUT_S  = 10.0
_HEARTBEAT_S     = 20.0   # Bybit requires ping every 20s


class _GapDetected(Exception):
    pass


@dataclass
class WsStats:
    connects:      int = 0
    gaps:          int = 0
    messages_recv: int = 0
    parse_errors:  int = 0


class BybitWsAdapter(ExchangeWsAdapter):
    """
    Bybit Linear WebSocket adapter.

    Fires on_depth for each orderbook.50 snapshot/delta.
    Fires on_trade for each publicTrade event.

    Sequence continuity: tracks last seen seq per symbol; a gap triggers
    the DepthStateMachine restart via the prev_update_id field.
    """

    def __init__(self, symbol: str, on_depth=None, on_trade=None) -> None:
        super().__init__(symbol, on_depth=on_depth, on_trade=on_trade)
        self.stats      = WsStats()
        self._last_seq: int | None = None

    @property
    def exchange(self) -> str:
        return "bybit"

    def build_url(self) -> str:
        return _WS_URL

    def parse_depth(self, raw: dict) -> NormalizedDepthEvent | None:
        topic = raw.get("topic", "")
        if not topic.startswith("orderbook."):
            return None
        if topic.split(".")[-1].upper() != self.symbol:
            return None

        data     = raw.get("data", {})
        seq      = int(raw.get("seq", 0))
        ts_ms    = int(raw.get("ts", int(time.time() * 1000)))
        msg_type = raw.get("type", "delta")  # "snapshot" or "delta"

        bids = [[str(p), str(q)] for p, q in (data.get("b") or [])]
        asks = [[str(p), str(q)] for p, q in (data.get("a") or [])]

        if msg_type == "snapshot":
            # Snapshot resets continuity — treat as first event
            prev_seq       = None
            self._last_seq = seq
        else:
            prev_seq       = self._last_seq
            self._last_seq = seq

        return NormalizedDepthEvent(
            first_update_id=seq,
            last_update_id=seq,
            prev_update_id=prev_seq,
            event_time_ms=ts_ms,
            bids=bids,
            asks=asks,
            raw=raw,
        )

    def parse_trade(self, raw: dict) -> NormalizedTrade | None:
        topic = raw.get("topic", "")
        if not topic.startswith("publicTrade."):
            return None
        trades = raw.get("data", [])
        if not trades:
            return None
        t = trades[0]
        return NormalizedTrade(
            symbol=self.symbol,
            timestamp_ms=int(t.get("T", 0)),
            trade_id=int(t.get("i", 0)),
            price=str(t.get("p", "0")),
            qty=str(t.get("v", "0")),
            buyer_is_maker=(t.get("S", "Buy") == "Sell"),
        )

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._connect()
                backoff = 1.0
            except asyncio.CancelledError:
                logger.info("{} Bybit WS shut down", self.symbol)
                return
            except _GapDetected:
                logger.info("{} gap: reconnecting immediately", self.symbol)
                backoff = 1.0
            except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as exc:
                logger.warning("{} Bybit WS error [{}]: {} — retry in {:.0f}s",
                               self.symbol, type(exc).__name__, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)
            except Exception as exc:
                logger.exception("{} unexpected Bybit WS error — retry in {:.0f}s: {}",
                                 self.symbol, backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)

    async def _connect(self) -> None:
        self.stats.connects += 1
        self._last_seq = None
        logger.info("{} Bybit connecting (#{}) → {}", self.symbol, self.stats.connects, _WS_URL)

        async with websockets.connect(
            _WS_URL, open_timeout=_OPEN_TIMEOUT_S, ping_interval=None
        ) as ws:
            sym = self.symbol.upper()
            sub = json.dumps({
                "op": "subscribe",
                "args": [f"orderbook.50.{sym}", f"publicTrade.{sym}"],
            })
            await ws.send(sub)
            logger.info("{} Bybit subscribed", self.symbol)
            await self._recv_loop(ws)

    async def _recv_loop(self, ws) -> None:
        last_ping = asyncio.get_event_loop().time()
        while True:
            remaining = _HEARTBEAT_S - (asyncio.get_event_loop().time() - last_ping)
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, remaining))
            except asyncio.TimeoutError:
                if asyncio.get_event_loop().time() - last_ping >= _HEARTBEAT_S:
                    await ws.send(json.dumps({"op": "ping"}))
                    last_ping = asyncio.get_event_loop().time()
                continue

            self.stats.messages_recv += 1
            await self._process(raw)

    async def _process(self, raw: str | bytes) -> None:
        try:
            msg: dict = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            self.stats.parse_errors += 1
            logger.error("{} Bybit JSON parse error: {}", self.symbol, exc)
            return

        # Subscription ack / pong
        if "op" in msg:
            return

        depth = self.parse_depth(msg)
        if depth is not None and self._on_depth is not None:
            try:
                await self._on_depth(depth)
            except Exception as exc:
                logger.exception("{} Bybit depth handler raised: {}", self.symbol, exc)
            return

        trade = self.parse_trade(msg)
        if trade is not None and self._on_trade is not None:
            try:
                await self._on_trade(trade)
            except Exception as exc:
                logger.exception("{} Bybit trade handler raised: {}", self.symbol, exc)
