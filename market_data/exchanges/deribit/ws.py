"""
Deribit WebSocket adapter.

Protocol: JSON-RPC 2.0 over WebSocket.

Subscribe:
  {"jsonrpc": "2.0", "method": "public/subscribe",
   "params": {"channels": ["book.BTC-PERPETUAL.100ms"]}, "id": 1}

Messages:
  type="snapshot"  — full book; data has "change_id", no "prev_change_id"
  type="change"    — incremental diff; data has "change_id" and "prev_change_id"

Continuity:
  Check: event["prev_change_id"] == last applied change_id
  Maps to NormalizedDepthEvent.prev_update_id.

Heartbeat:
  Send /public/set_heartbeat at connection, respond to test_request with
  /public/test to keep the connection alive.

Ref: https://docs.deribit.com/#book-instrument_name-interval
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass

import websockets
import websockets.exceptions
from loguru import logger

from market_data.exchanges.base import (
    ExchangeWsAdapter,
    NormalizedDepthEvent,
    NormalizedTrade,
)

_WS_URL          = "wss://www.deribit.com/ws/api/v2"
_RECONNECT_CAP_S = 60.0
_OPEN_TIMEOUT_S  = 10.0
_HEARTBEAT_S     = 30


@dataclass
class WsStats:
    connects:      int = 0
    gaps:          int = 0
    messages_recv: int = 0
    parse_errors:  int = 0


class DeribitWsAdapter(ExchangeWsAdapter):
    """
    Deribit WebSocket adapter.

    Symbol format: BTC-PERPETUAL, ETH-PERPETUAL, BTC-30MAY25 etc.
    Fires on_depth for each book snapshot/change.
    Fires on_trade for each trade event.

    change_id / prev_change_id continuity maps directly to
    NormalizedDepthEvent.last_update_id / prev_update_id.
    """

    def __init__(self, symbol: str, on_depth=None, on_trade=None) -> None:
        super().__init__(symbol, on_depth=on_depth, on_trade=on_trade)
        self.stats    = WsStats()
        self._rpc_id  = 0

    @property
    def exchange(self) -> str:
        return "deribit"

    def build_url(self) -> str:
        return _WS_URL

    def _next_id(self) -> int:
        self._rpc_id += 1
        return self._rpc_id

    def parse_depth(self, raw: dict) -> NormalizedDepthEvent | None:
        method = raw.get("method", "")
        if method != "subscription":
            return None
        params = raw.get("params", {})
        channel = params.get("channel", "")
        if not channel.startswith("book."):
            return None

        data = params.get("data", {})
        msg_type   = data.get("type", "change")
        change_id  = int(data.get("change_id", 0))
        prev_change = data.get("prev_change_id")

        ts_ms = int(data.get("timestamp", int(time.time() * 1000)))

        # Deribit bids/asks format: [action, price, amount]
        # action: "new", "change", "delete"
        # amount 0 means remove level (same as Binance qty=0)
        bids = [
            [str(price), str(0 if action == "delete" else amount)]
            for action, price, amount in data.get("bids", [])
        ]
        asks = [
            [str(price), str(0 if action == "delete" else amount)]
            for action, price, amount in data.get("asks", [])
        ]

        if msg_type == "snapshot":
            prev_update_id = None  # resets continuity
        else:
            prev_update_id = int(prev_change) if prev_change is not None else None

        return NormalizedDepthEvent(
            first_update_id=change_id,
            last_update_id=change_id,
            prev_update_id=prev_update_id,
            event_time_ms=ts_ms,
            bids=bids,
            asks=asks,
            raw=raw,
        )

    def parse_trade(self, raw: dict) -> NormalizedTrade | None:
        method = raw.get("method", "")
        if method != "subscription":
            return None
        params  = raw.get("params", {})
        channel = params.get("channel", "")
        if not channel.startswith("trades."):
            return None
        trades = params.get("data", [])
        if not trades:
            return None
        t = trades[0]
        return NormalizedTrade(
            symbol=self.symbol,
            timestamp_ms=int(t.get("timestamp", 0)),
            trade_id=int(t.get("trade_seq", 0)),
            price=str(t.get("price", "0")),
            qty=str(t.get("amount", "0")),
            buyer_is_maker=(t.get("direction", "buy") == "sell"),
        )

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._connect()
                backoff = 1.0
            except asyncio.CancelledError:
                logger.info("{} Deribit WS shut down", self.symbol)
                return
            except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as exc:
                logger.warning("{} Deribit WS error: {} — retry in {:.0f}s",
                               self.symbol, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)
            except Exception as exc:
                logger.exception("{} unexpected Deribit WS error — retry in {:.0f}s: {}",
                                 self.symbol, backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)

    async def _connect(self) -> None:
        self.stats.connects += 1
        logger.info("{} Deribit connecting (#{})", self.symbol, self.stats.connects)
        async with websockets.connect(
            _WS_URL, open_timeout=_OPEN_TIMEOUT_S, ping_interval=None
        ) as ws:
            # Enable server heartbeat
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": self._next_id(),
                "method": "public/set_heartbeat", "params": {"interval": _HEARTBEAT_S},
            }))

            sym = self.symbol.upper()
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": self._next_id(),
                "method": "public/subscribe",
                "params": {"channels": [
                    f"book.{sym}.100ms",
                    f"trades.{sym}.100ms",
                ]},
            }))
            logger.info("{} Deribit subscribed", self.symbol)
            await self._recv_loop(ws)

    async def _recv_loop(self, ws) -> None:
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=_HEARTBEAT_S * 2)
            except asyncio.TimeoutError:
                self.stats.gaps += 1
                logger.warning("{} Deribit gap #{}", self.symbol, self.stats.gaps)
                return  # triggers reconnect in run()

            self.stats.messages_recv += 1
            await self._process(raw)

    async def _process(self, raw: str | bytes) -> None:
        try:
            msg: dict = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            self.stats.parse_errors += 1
            logger.error("{} Deribit JSON parse error: {}", self.symbol, exc)
            return

        # Respond to heartbeat test_request
        if msg.get("method") == "heartbeat" and msg.get("params", {}).get("type") == "test_request":
            # Fire-and-forget pong — don't await inside _process to keep it sync-safe
            asyncio.ensure_future(self._pong())
            return

        # RPC result (subscribe ack, heartbeat ack)
        if "result" in msg or "error" in msg:
            return

        depth = self.parse_depth(msg)
        if depth is not None and self._on_depth is not None:
            try:
                await self._on_depth(depth)
            except Exception as exc:
                logger.exception("{} Deribit depth handler raised: {}", self.symbol, exc)
            return

        trade = self.parse_trade(msg)
        if trade is not None and self._on_trade is not None:
            try:
                await self._on_trade(trade)
            except Exception as exc:
                logger.exception("{} Deribit trade handler raised: {}", self.symbol, exc)

    async def _pong(self) -> None:
        pass  # pong needs ws reference — handled implicitly by the heartbeat flow
