"""
OKX WebSocket adapter (Swap / Perpetual).

Protocol:
  WS endpoint:  wss://ws.okx.com:8443/ws/v5/public
  Subscribe:    {"op": "subscribe", "args": [{"channel": "books", "instId": "BTC-USDT-SWAP"}]}
  Snapshot:     action="snapshot" — full book, resets continuity
  Update:       action="update"   — incremental diff

Continuity fields (closest equivalent to Binance pu):
  data[0]["seqId"]     — sequence ID of this event
  data[0]["prevSeqId"] — sequence ID of the previous event

  Check: event["prevSeqId"] == last applied seqId
  -1 prevSeqId on first event after snapshot — skip check.

Ref: https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book
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

_WS_URL          = "wss://ws.okx.com:8443/ws/v5/public"
_GAP_TIMEOUT_S   = 30.0   # OKX sends heartbeat every 30s
_RECONNECT_CAP_S = 60.0
_OPEN_TIMEOUT_S  = 10.0


class _GapDetected(Exception):
    pass


@dataclass
class WsStats:
    connects:      int = 0
    gaps:          int = 0
    messages_recv: int = 0
    parse_errors:  int = 0


class OkxWsAdapter(ExchangeWsAdapter):
    """
    OKX Swap WebSocket adapter.

    Symbol format: BTC-USDT-SWAP, ETH-USDT-SWAP (OKX instId format).
    Fires on_depth for each books snapshot/update.
    Fires on_trade for each trades event.

    seqId / prevSeqId continuity: maps directly to
    NormalizedDepthEvent.last_update_id / prev_update_id.
    """

    def __init__(self, symbol: str, on_depth=None, on_trade=None) -> None:
        super().__init__(symbol, on_depth=on_depth, on_trade=on_trade)
        self.stats = WsStats()

    @property
    def exchange(self) -> str:
        return "okx"

    def build_url(self) -> str:
        return _WS_URL

    def parse_depth(self, raw: dict) -> NormalizedDepthEvent | None:
        arg = raw.get("arg", {})
        if arg.get("channel") != "books":
            return None
        if arg.get("instId", "").upper() != self.symbol.upper():
            return None

        action = raw.get("action", "update")
        data   = (raw.get("data") or [{}])[0]

        seq_id      = int(data.get("seqId", 0))
        prev_seq_id = int(data.get("prevSeqId", -1))
        ts_ms       = int(data.get("ts", int(time.time() * 1000)))

        bids = [[str(p), str(q)] for p, q, *_ in (data.get("bids") or [])]
        asks = [[str(p), str(q)] for p, q, *_ in (data.get("asks") or [])]

        if action == "snapshot":
            # Snapshot resets sequence — prevSeqId=-1 means skip continuity check
            prev_seq_id = None

        return NormalizedDepthEvent(
            first_update_id=seq_id,
            last_update_id=seq_id,
            prev_update_id=prev_seq_id if prev_seq_id != -1 else None,
            event_time_ms=ts_ms,
            bids=bids,
            asks=asks,
            raw=raw,
        )

    def parse_trade(self, raw: dict) -> NormalizedTrade | None:
        arg = raw.get("arg", {})
        if arg.get("channel") != "trades":
            return None
        trades = raw.get("data", [])
        if not trades:
            return None
        t = trades[0]
        return NormalizedTrade(
            symbol=self.symbol,
            timestamp_ms=int(t.get("ts", 0)),
            trade_id=int(t.get("tradeId", 0)),
            price=str(t.get("px", "0")),
            qty=str(t.get("sz", "0")),
            buyer_is_maker=(t.get("side", "buy") == "sell"),
        )

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._connect()
                backoff = 1.0
            except asyncio.CancelledError:
                logger.info("{} OKX WS shut down", self.symbol)
                return
            except _GapDetected:
                logger.info("{} OKX gap: reconnecting immediately", self.symbol)
                backoff = 1.0
            except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as exc:
                logger.warning("{} OKX WS error: {} — retry in {:.0f}s", self.symbol, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)
            except Exception as exc:
                logger.exception("{} unexpected OKX WS error — retry in {:.0f}s: {}",
                                 self.symbol, backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)

    async def _connect(self) -> None:
        self.stats.connects += 1
        logger.info("{} OKX connecting (#{})", self.symbol, self.stats.connects)
        async with websockets.connect(
            _WS_URL, open_timeout=_OPEN_TIMEOUT_S, ping_interval=None
        ) as ws:
            sub = json.dumps({"op": "subscribe", "args": [
                {"channel": "books",  "instId": self.symbol},
                {"channel": "trades", "instId": self.symbol},
            ]})
            await ws.send(sub)
            logger.info("{} OKX subscribed", self.symbol)
            await self._recv_loop(ws)

    async def _recv_loop(self, ws) -> None:
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=_GAP_TIMEOUT_S)
            except asyncio.TimeoutError:
                self.stats.gaps += 1
                logger.warning("{} OKX gap #{}", self.symbol, self.stats.gaps)
                raise _GapDetected

            # OKX heartbeat pong
            if raw == "pong":
                continue

            self.stats.messages_recv += 1
            await self._process(raw)

    async def _process(self, raw: str | bytes) -> None:
        try:
            msg: dict = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            self.stats.parse_errors += 1
            logger.error("{} OKX JSON parse error: {}", self.symbol, exc)
            return

        if "event" in msg:
            return  # subscribe ack / error

        depth = self.parse_depth(msg)
        if depth is not None and self._on_depth is not None:
            try:
                await self._on_depth(depth)
            except Exception as exc:
                logger.exception("{} OKX depth handler raised: {}", self.symbol, exc)
            return

        trade = self.parse_trade(msg)
        if trade is not None and self._on_trade is not None:
            try:
                await self._on_trade(trade)
            except Exception as exc:
                logger.exception("{} OKX trade handler raised: {}", self.symbol, exc)
