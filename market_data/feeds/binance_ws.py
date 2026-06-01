"""
Binance USDT-M Futures — WebSocket client (Phase 0, Session 1).

Connects to the combined stream endpoint:
  wss://fstream.binance.com/stream?streams=<s1>/<s2>/...

Each message arrives as: {"stream": "<name>", "data": {...}}

Subscribed streams per symbol:
  @depth@100ms    — L2 incremental diff (100ms cadence)
  @aggTrade       — aggregated taker trade (one event per taker order)
  @markPrice@1s   — mark price, index price, next funding rate
  @bookTicker     — best bid/ask on every change

Binance API ref: wss://fstream.binance.com (USDT-M Futures WS Streams)

Design contract:
  - This class only ingests and dispatches. No book state. No features.
  - On gap (≥5 s silence) or any WS error: reconnect, backoff 1→2→4→…→60 s.
  - Handlers receive the raw "data" dict. They are responsible for parsing.
  - All public methods are typed. No bare except. No print.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import websockets
import websockets.exceptions
from loguru import logger

_WS_BASE: str = "wss://fstream.binance.com"

_GAP_TIMEOUT_S: float   = 5.0    # reconnect if no message in this window
_RECONNECT_CAP_S: float = 60.0   # backoff ceiling
_OPEN_TIMEOUT_S: float  = 10.0   # max wait for initial WS handshake

# Async callback type — receives the "data" dict from a stream event
Handler = Callable[[dict], Awaitable[None]]


class _GapDetected(Exception):
    """Raised internally when the gap timer fires; triggers clean reconnect."""


@dataclass
class WsStats:
    """Live counters for monitoring and alerting."""
    connects:        int = 0
    gaps:            int = 0
    messages_recv:   int = 0
    parse_errors:    int = 0
    dispatch_errors: int = 0


class BinanceWsClient:
    """
    Single-symbol combined-stream WebSocket client for Binance USDT-M Futures.

    Usage::

        async def on_depth(data: dict) -> None:
            print(data["u"], data["b"])

        client = BinanceWsClient("BTCUSDT", on_depth=on_depth)
        await client.run()   # runs until cancelled

    ``run()`` never returns under normal operation. Cancel the task to stop.
    """

    def __init__(
        self,
        symbol:         str,
        on_depth:       Handler | None = None,
        on_agg_trade:   Handler | None = None,
        on_mark_price:  Handler | None = None,
        on_book_ticker: Handler | None = None,
    ) -> None:
        self.symbol = symbol.upper()
        self.stats  = WsStats()

        sym = self.symbol.lower()
        # Route stream names → handlers at construction time so dispatch is O(1)
        self._handlers: dict[str, Handler] = {}
        if on_depth       is not None: self._handlers[f"{sym}@depth@100ms"] = on_depth
        if on_agg_trade   is not None: self._handlers[f"{sym}@aggTrade"]    = on_agg_trade
        if on_mark_price  is not None: self._handlers[f"{sym}@markPrice@1s"] = on_mark_price
        if on_book_ticker is not None: self._handlers[f"{sym}@bookTicker"] = on_book_ticker

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Connect, dispatch messages, reconnect on any failure. Never returns."""
        backoff = 1.0
        while True:
            try:
                await self._connect()
                # _connect only exits via _GapDetected (raised inside → re-raises here)
                # so we should never reach this line cleanly. Reset backoff defensively.
                backoff = 1.0
            except asyncio.CancelledError:
                logger.info("{} WS client shut down (cancelled)", self.symbol)
                return
            except _GapDetected:
                # Gap is not a network error — reconnect immediately, no backoff
                logger.info("{} gap: reconnecting immediately", self.symbol)
                backoff = 1.0
            except (
                websockets.exceptions.WebSocketException,
                OSError,
                asyncio.TimeoutError,
            ) as exc:
                logger.warning(
                    "{} WS error [{}]: {} — reconnecting in {:.0f}s",
                    self.symbol, type(exc).__name__, exc, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)
            except Exception as exc:
                # Unexpected — log fully, still reconnect
                logger.exception(
                    "{} unexpected WS error — reconnecting in {:.0f}s: {}",
                    self.symbol, backoff, exc,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _RECONNECT_CAP_S)

    def build_url(self) -> str:
        """Return the combined-stream URL for this symbol's four streams."""
        sym     = self.symbol.lower()
        streams = "/".join([
            f"{sym}@depth@100ms",
            f"{sym}@aggTrade",
            f"{sym}@markPrice@1s",
            f"{sym}@bookTicker",
        ])
        return f"{_WS_BASE}/stream?streams={streams}"

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _connect(self) -> None:
        url = self.build_url()
        self.stats.connects += 1
        logger.info("{} connecting (#{}) → {}", self.symbol, self.stats.connects, url)

        async with websockets.connect(
            url,
            open_timeout=_OPEN_TIMEOUT_S,
            # ping_interval/ping_timeout: websockets handles Binance server pings automatically
            ping_interval=20,
            ping_timeout=30,
        ) as ws:
            logger.info("{} connected", self.symbol)
            await self._recv_loop(ws)

    async def _recv_loop(self, ws: websockets.ClientConnection) -> None:
        """Read messages until gap timeout or WS close."""
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=_GAP_TIMEOUT_S)
            except asyncio.TimeoutError:
                self.stats.gaps += 1
                logger.warning(
                    "{} gap #{}: no event for {:.0f}s",
                    self.symbol, self.stats.gaps, _GAP_TIMEOUT_S,
                )
                raise _GapDetected

            self.stats.messages_recv += 1
            await self._process(raw)

    async def _process(self, raw: str | bytes) -> None:
        """Parse one raw message and dispatch to the registered handler."""
        try:
            msg: dict = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            self.stats.parse_errors += 1
            logger.error(
                "{} JSON parse error: {} | raw={!r}",
                self.symbol, exc, raw[:200] if isinstance(raw, (str, bytes)) else raw,
            )
            return

        stream: str = msg.get("stream", "")
        data: dict  = msg.get("data", msg)

        handler = self._handlers.get(stream)
        if handler is None:
            # Unsubscribed stream (e.g. Binance internal pings) — ignore silently
            logger.debug("{} no handler for stream={!r}", self.symbol, stream)
            return

        try:
            await handler(data)
        except Exception as exc:
            self.stats.dispatch_errors += 1
            logger.exception(
                "{} handler raised on stream={!r}: {}", self.symbol, stream, exc
            )
            # Do NOT propagate — a bad handler must not tear down the WS connection
