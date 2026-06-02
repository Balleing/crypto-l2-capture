"""
Data Capture Daemon — exchange-agnostic.

Wires together:
  ExchangeWsAdapter  →  DepthStateMachine  →  TickWriter  →  parquet files
  ExchangeRestAdapter (clock drift watchdog + mark price poll)

One SymbolCapture task per configured symbol. A shared flush loop writes
buffered rows to disk every FLUSH_INTERVAL_S seconds.

Output layout:
  data/
    binance/BTCUSDT/books/YYYY-MM-DD/HH-MM-SS.parquet
    bybit/BTCUSDT/books/…
    okx/BTC-USDT-SWAP/books/…

Run:
    l2cap capture --exchange binance --symbols BTCUSDT ETHUSDT
"""

from __future__ import annotations

import asyncio
import signal
import sys
from pathlib import Path

from loguru import logger

from config import settings
from market_data.book.snapshot import DepthStateMachine
from market_data.exchanges.base import (
    ClockDriftError,
    ExchangeRestAdapter,
    ExchangeWsAdapter,
    NormalizedDepthEvent,
    NormalizedTrade,
)
from market_data.storage.parquet import FLUSH_INTERVAL_S, TickWriter

_CLOCK_CHECK_INTERVAL_S = 60


def _get_adapters(exchange: str, symbol: str, on_depth, on_trade):
    """Instantiate the WS adapter for the given exchange."""
    if exchange == "binance":
        from market_data.exchanges.binance import BinanceWsAdapter
        return BinanceWsAdapter(symbol, on_depth=on_depth, on_trade=on_trade)
    if exchange == "bybit":
        from market_data.exchanges.bybit import BybitWsAdapter
        return BybitWsAdapter(symbol, on_depth=on_depth, on_trade=on_trade)
    if exchange == "okx":
        from market_data.exchanges.okx import OkxWsAdapter
        return OkxWsAdapter(symbol, on_depth=on_depth, on_trade=on_trade)
    if exchange == "deribit":
        from market_data.exchanges.deribit import DeribitWsAdapter
        return DeribitWsAdapter(symbol, on_depth=on_depth, on_trade=on_trade)
    raise ValueError(f"Unknown exchange: {exchange!r}")


def _get_rest(exchange: str) -> ExchangeRestAdapter:
    """Instantiate the REST adapter for the given exchange."""
    if exchange == "binance":
        from market_data.exchanges.binance import BinanceRestAdapter
        return BinanceRestAdapter()
    if exchange == "bybit":
        from market_data.exchanges.bybit import BybitRestAdapter
        return BybitRestAdapter()
    if exchange == "okx":
        from market_data.exchanges.okx import OkxRestAdapter
        return OkxRestAdapter()
    if exchange == "deribit":
        from market_data.exchanges.deribit import DeribitRestAdapter
        return DeribitRestAdapter()
    raise ValueError(f"Unknown exchange: {exchange!r}")


# ---------------------------------------------------------------------------
# Per-symbol coordinator
# ---------------------------------------------------------------------------

class SymbolCapture:
    """Manages one symbol: WS stream + L2 state machine + tick writing."""

    def __init__(
        self,
        symbol:   str,
        exchange: str,
        rest:     ExchangeRestAdapter,
        writer:   TickWriter,
        depth:    int = 20,
    ) -> None:
        self.symbol   = symbol.upper()
        self.exchange = exchange
        self._rest    = rest
        self._writer  = writer
        self._depth   = depth

        self._machine = DepthStateMachine(
            symbol=self.symbol,
            rest_client=rest,
            on_book_update=self._on_book,
            snapshot_depth=depth,
        )
        self._ws = _get_adapters(
            exchange, self.symbol,
            on_depth=self._on_depth,
            on_trade=self._on_trade,
        )
        self._start_task: asyncio.Task | None = None

    async def run(self) -> None:
        self._start_task = asyncio.create_task(
            self._machine.start(), name=f"l2_start_{self.symbol}"
        )
        await self._ws.run()

    async def _on_depth(self, event: NormalizedDepthEvent) -> None:
        await self._machine.on_depth_event(event)
        if not self._machine.is_live and (
            self._start_task is None or self._start_task.done()
        ):
            logger.info("{} triggering L2 re-sync after restart", self.symbol)
            self._start_task = asyncio.create_task(
                self._machine.start(), name=f"l2_resync_{self.symbol}"
            )

    async def _on_trade(self, trade: NormalizedTrade) -> None:
        self._writer.add_trade_normalized(trade)

    async def _on_book(self, snap: object) -> None:
        self._writer.add_book(snap)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Clock drift watchdog
# ---------------------------------------------------------------------------

async def clock_watchdog(rest: ExchangeRestAdapter) -> None:
    while True:
        await asyncio.sleep(_CLOCK_CHECK_INTERVAL_S)
        try:
            await rest.check_clock_drift()
        except ClockDriftError as exc:
            logger.critical("Clock drift too large — halting: {}", exc)
            sys.exit(1)
        except Exception as exc:
            logger.warning("Clock drift check failed (non-fatal): {}", exc)


# ---------------------------------------------------------------------------
# Mark price poll (Binance-specific, skipped for other exchanges)
# ---------------------------------------------------------------------------

async def mark_price_poll_loop(symbols, rest, writer) -> None:
    if not hasattr(rest, "premium_index"):
        return
    while True:
        for symbol in symbols:
            try:
                raw = await rest.premium_index(symbol)
                writer.add_mark_price({
                    "s": raw["symbol"],
                    "E": int(raw["time"]),
                    "p": raw["markPrice"],
                    "i": raw["indexPrice"],
                    "r": raw.get("lastFundingRate", "0"),
                    "T": int(raw.get("nextFundingTime", 0)),
                })
            except Exception as exc:
                logger.warning("mark price poll {} failed: {}", symbol, exc)
        await asyncio.sleep(1.0)


# ---------------------------------------------------------------------------
# Flush loop
# ---------------------------------------------------------------------------

async def flush_loop(writer: TickWriter) -> None:
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_S)
        try:
            writer.flush()
        except Exception as exc:
            logger.error("Flush error: {}", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main(exchange: str | None = None, symbols: list[str] | None = None) -> None:
    logger.remove()
    logger.add(sys.stderr, format="{time:HH:mm:ss} | {level:<7} | {message}", level="INFO")
    logger.add(
        "logs/capture_{time:YYYY-MM-DD}.log",
        rotation="00:00", retention="14 days", compression="gz",
        level="DEBUG", enqueue=True,
    )

    exchange = exchange or getattr(settings, "exchange", "binance")
    symbols  = symbols  or settings.symbols

    data_root = Path(settings.capture.data_dir) / exchange

    logger.info("Capture starting: exchange={} symbols={}", exchange, symbols)
    logger.info("Writing to: {}", data_root.resolve())

    writer = TickWriter(data_root=data_root, book_depth=settings.capture.book_depth)

    stop_event = asyncio.Event()
    loop       = asyncio.get_running_loop()

    def _handle_signal() -> None:
        logger.info("Shutdown signal — draining buffers…")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    rest = _get_rest(exchange)
    async with rest:
        try:
            drift = await rest.check_clock_drift()
            logger.info("Clock drift: {}ms", drift)
        except ClockDriftError as exc:
            logger.critical("Aborting: {}", exc)
            return
        except Exception as exc:
            logger.warning("Initial clock check failed (non-fatal): {}", exc)

        tasks: list[asyncio.Task] = []

        for symbol in symbols:
            cap = SymbolCapture(
                symbol=symbol, exchange=exchange,
                rest=rest, writer=writer,
                depth=settings.capture.book_depth,
            )
            tasks.append(asyncio.create_task(cap.run(), name=f"capture_{symbol}"))

        tasks.append(asyncio.create_task(flush_loop(writer), name="flush_loop"))
        tasks.append(asyncio.create_task(clock_watchdog(rest), name="clock_watchdog"))
        tasks.append(asyncio.create_task(
            mark_price_poll_loop(symbols, rest, writer), name="mark_price_poll"
        ))

        await stop_event.wait()

        logger.info("Cancelling {} tasks…", len(tasks))
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("Final flush…")
        try:
            writer.flush()
        except Exception as exc:
            logger.error("Final flush error: {}", exc)

    logger.info("Capture stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
