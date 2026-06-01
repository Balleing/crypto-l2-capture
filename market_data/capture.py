"""
Data Capture Daemon.

Wires together:
  BinanceWsClient  →  DepthStateMachine  →  TickWriter  →  parquet files
                   →  trade rows
  REST poll loop   →  mark price rows

One SymbolCapture task per configured symbol. A shared flush loop writes
buffered rows to disk every FLUSH_INTERVAL_S seconds. Clock drift is
checked every 60s and halts the process if skew exceeds 500ms.

Output layout:
  data/
    ETHUSDT/
      books/       YYYY-MM-DD/HH-MM-SS.parquet
      trades/      YYYY-MM-DD/HH-MM-SS.parquet
      mark_price/  YYYY-MM-DD/HH-MM-SS.parquet
    BTCUSDT/
      …

Run:
    python3 -m market_data.capture

Stop with Ctrl+C — buffers are flushed before exit.
"""

from __future__ import annotations

import asyncio
import signal
import sys
from pathlib import Path

from loguru import logger

from config import settings
from market_data.book.snapshot import DepthStateMachine
from market_data.feeds.binance_rest import BinanceRestClient, ClockDriftError
from market_data.feeds.binance_ws import BinanceWsClient
from market_data.storage.parquet import FLUSH_INTERVAL_S, TickWriter

_CLOCK_CHECK_INTERVAL_S: int = 60
_DATA_ROOT = Path(settings.capture.data_dir)


# ---------------------------------------------------------------------------
# Per-symbol coordinator
# ---------------------------------------------------------------------------

class SymbolCapture:
    """
    Manages one symbol: WS stream + L2 state machine + tick writing.
    """

    def __init__(
        self,
        symbol:  str,
        rest:    BinanceRestClient,
        writer:  TickWriter,
        depth:   int = 20,
    ) -> None:
        self.symbol  = symbol.upper()
        self._rest   = rest
        self._writer = writer
        self._depth  = depth

        self._machine = DepthStateMachine(
            symbol=self.symbol,
            rest_client=rest,
            on_book_update=self._on_book,
            snapshot_depth=depth,
        )
        self._ws          = BinanceWsClient(
            symbol=self.symbol,
            on_depth=self._on_depth,
            on_agg_trade=self._on_trade,
            on_mark_price=self._on_mark_price,
            on_book_ticker=self._on_book_ticker,
        )
        self._start_task: asyncio.Task | None = None

    async def run(self) -> None:
        """Start the WS feed and initial L2 sync. Runs until cancelled."""
        self._start_task = asyncio.create_task(
            self._machine.start(), name=f"l2_start_{self.symbol}"
        )
        await self._ws.run()

    async def _on_depth(self, data: dict) -> None:
        await self._machine.on_depth_event(data)

        if not self._machine.is_live and (
            self._start_task is None or self._start_task.done()
        ):
            logger.info("{} triggering L2 re-sync after restart", self.symbol)
            self._start_task = asyncio.create_task(
                self._machine.start(), name=f"l2_resync_{self.symbol}"
            )

    async def _on_trade(self, data: dict) -> None:
        data["s"] = self.symbol
        self._writer.add_trade(data)

    async def _on_mark_price(self, data: dict) -> None:
        data["symbol"] = self.symbol
        self._writer.add_mark_price(data)

    async def _on_book(self, snap: object) -> None:
        self._writer.add_book(snap)  # type: ignore[arg-type]

    async def _on_book_ticker(self, data: dict) -> None:
        pass


# ---------------------------------------------------------------------------
# Clock drift watchdog
# ---------------------------------------------------------------------------

async def clock_watchdog(rest: BinanceRestClient) -> None:
    """Check clock drift every 60s. Halt process if skew > 500ms."""
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
# Mark price poll loop
# ---------------------------------------------------------------------------

async def mark_price_poll_loop(
    symbols: list[str],
    rest:    BinanceRestClient,
    writer:  TickWriter,
) -> None:
    """
    Poll GET /fapi/v1/premiumIndex every 1s for each symbol.

    Weight: 1 per call × N symbols × 60 = 60*N/min (well within 2400 limit).
    REST fields are remapped to the WS-format expected by TickWriter.add_mark_price().
    """
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
                logger.warning("mark price poll {} failed (non-fatal): {}", symbol, exc)
        await asyncio.sleep(1.0)


# ---------------------------------------------------------------------------
# Flush loop
# ---------------------------------------------------------------------------

async def flush_loop(writer: TickWriter) -> None:
    """Flush all buffered rows to parquet every FLUSH_INTERVAL_S seconds."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_S)
        try:
            writer.flush()
        except Exception as exc:
            logger.error("Flush error (rows still buffered): {}", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        format="{time:HH:mm:ss} | {level:<7} | {message}",
        level="INFO",
    )
    logger.add(
        "logs/capture_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="14 days",
        compression="gz",
        level="DEBUG",
        enqueue=True,
    )

    logger.info("Data capture starting")
    logger.info("Symbols: {}", settings.symbols)
    logger.info("Writing to: {}", _DATA_ROOT.resolve())

    writer = TickWriter(
        data_root=_DATA_ROOT,
        book_depth=settings.capture.book_depth,
    )

    stop_event = asyncio.Event()
    loop       = asyncio.get_running_loop()

    def _handle_signal() -> None:
        logger.info("Shutdown signal — draining buffers…")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    async with BinanceRestClient() as rest:
        try:
            drift = await rest.check_clock_drift()
            logger.info("Clock drift: {}ms", drift)
        except ClockDriftError as exc:
            logger.critical("Aborting: {}", exc)
            return
        except Exception as exc:
            logger.warning("Initial clock check failed (non-fatal): {}", exc)

        tasks: list[asyncio.Task] = []

        for symbol in settings.symbols:
            cap = SymbolCapture(
                symbol=symbol,
                rest=rest,
                writer=writer,
                depth=settings.capture.book_depth,
            )
            tasks.append(
                asyncio.create_task(cap.run(), name=f"capture_{symbol}")
            )

        tasks.append(asyncio.create_task(flush_loop(writer),   name="flush_loop"))
        tasks.append(asyncio.create_task(clock_watchdog(rest), name="clock_watchdog"))
        tasks.append(
            asyncio.create_task(
                mark_price_poll_loop(settings.symbols, rest, writer),
                name="mark_price_poll",
            )
        )

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
