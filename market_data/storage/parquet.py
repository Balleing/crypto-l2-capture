"""
Tick writer — appends book snapshots, trades, and mark-price events to
parquet files partitioned by symbol/date.

Layout on disk:
  data/
    ETHUSDT/
      books/       2025-06-01/          ← one file per flush window
                     09-00-00.parquet
                     09-05-00.parquet
                     …
      trades/      2025-06-01/
      mark_price/  2025-06-01/

Each flush writes a new file named by UTC wall-clock time. No read-back
of existing files — memory usage is O(buffer size), not O(day size).
Analysis reads the whole day with pl.scan_parquet("…/date/*.parquet").

Design choices:
  - Prices/quantities stored as float64 (converted from Decimal on ingress).
    float64 gives ~15 significant digits — sufficient for BTC at 8 decimal
    places and quantities at any precision Binance returns.
  - Timestamps stored as Int64 milliseconds (not datetime) so downstream
    code chooses its own timezone handling.
  - No schema validation here — the writer trusts its callers (capture.py).
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from market_data.book.l2 import BookSnapshot
from market_data.exchanges.base import NormalizedTrade

FLUSH_INTERVAL_S: int = 300   # flush every 5 minutes


def _ms_to_date(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def _flush_filename() -> str:
    """HH-MM-SS-ffffff timestamp for the current flush file (microseconds prevent collisions)."""
    return datetime.now(tz=timezone.utc).strftime("%H-%M-%S-%f")


def _to_float(v: Decimal | None) -> float | None:
    return float(v) if v is not None else None


class TickWriter:
    """
    Buffers tick events in memory and flushes to partitioned parquet files.

    All public methods are synchronous (building rows is CPU-only).
    ``flush()`` is synchronous too — it does blocking file I/O on the
    event loop thread. At 100ms cadence and 2 symbols, flush writes
    ~3 000 book rows plus a few hundred trades — polars concat + write
    completes in well under 1s on any SSD, so blocking the loop briefly
    is acceptable. If this ever becomes a bottleneck, wrap in
    ``asyncio.to_thread``.
    """

    def __init__(self, data_root: str | Path, book_depth: int = 20) -> None:
        self._root       = Path(data_root)
        self._depth      = book_depth
        # Buffers: symbol → list of row dicts
        self._books:      dict[str, list[dict]] = defaultdict(list)
        self._trades:     dict[str, list[dict]] = defaultdict(list)
        self._mark_price: dict[str, list[dict]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def add_book(self, snap: BookSnapshot) -> None:
        """Append one L2 book snapshot row."""
        row: dict[str, Any] = {
            "timestamp_ms":    snap.event_time_ms,
            "received_at_ms":  int(time.time() * 1000),
            "update_id":       snap.last_update_id,
            "microprice":      _to_float(snap.microprice),
            "imbalance":    _to_float(snap.imbalance),
            "mid":          _to_float(snap.mid),
            "spread":       _to_float(snap.spread),
        }
        if snap.best_bid:
            row["best_bid_price"] = float(snap.best_bid.price)
            row["best_bid_qty"]   = float(snap.best_bid.qty)
        else:
            row["best_bid_price"] = None
            row["best_bid_qty"]   = None

        if snap.best_ask:
            row["best_ask_price"] = float(snap.best_ask.price)
            row["best_ask_qty"]   = float(snap.best_ask.qty)
        else:
            row["best_ask_price"] = None
            row["best_ask_qty"]   = None

        for i in range(self._depth):
            if i < len(snap.bids):
                row[f"bid_price_{i}"] = float(snap.bids[i].price)
                row[f"bid_qty_{i}"]   = float(snap.bids[i].qty)
            else:
                row[f"bid_price_{i}"] = None
                row[f"bid_qty_{i}"]   = None
            if i < len(snap.asks):
                row[f"ask_price_{i}"] = float(snap.asks[i].price)
                row[f"ask_qty_{i}"]   = float(snap.asks[i].qty)
            else:
                row[f"ask_price_{i}"] = None
                row[f"ask_qty_{i}"]   = None

        self._books[snap.symbol].append(row)

    def add_trade(self, data: dict) -> None:
        """
        Append one @trade event (Binance USDT-M Futures).

        data fields (Binance @aggTrade stream):
          T  — trade time ms
          a  — aggregate trade ID
          p  — price string
          q  — quantity string
          m  — buyer is maker (True → taker was seller, sign = -1)
        """
        self._trades[data.get("s", "UNKNOWN")].append({
            "timestamp_ms":  int(data["T"]),
            "trade_id":      int(data["a"]),
            "price":         float(data["p"]),
            "qty":           float(data["q"]),
            "buyer_is_maker": bool(data["m"]),
            # sign: +1 if taker bought (buyer_is_maker=False), -1 if taker sold
            "taker_sign":    -1 if data["m"] else 1,
        })

    def add_trade_normalized(self, trade: NormalizedTrade) -> None:
        """Append one NormalizedTrade from any exchange adapter."""
        self._trades[trade.symbol].append({
            "timestamp_ms":   trade.timestamp_ms,
            "trade_id":       trade.trade_id,
            "price":          float(trade.price),
            "qty":            float(trade.qty),
            "buyer_is_maker": trade.buyer_is_maker,
            "taker_sign":     -1 if trade.buyer_is_maker else 1,
        })

    def add_mark_price(self, data: dict) -> None:
        """
        Append one markPrice@1s event.

        data fields:
          E  — event time ms
          p  — mark price
          i  — index price
          r  — funding rate (next)
          T  — next funding time ms
        """
        # symbol comes from stream name, not in data body for markPrice
        # capture.py passes a symbol-tagged dict — fall back gracefully
        symbol = data.get("s", data.get("symbol", "UNKNOWN"))
        self._mark_price[symbol].append({
            "timestamp_ms":      int(data["E"]),
            "mark_price":        float(data["p"]),
            "index_price":       float(data.get("i", data["p"])),
            "funding_rate":      float(data.get("r", 0.0)),
            "next_funding_time": int(data.get("T", 0)),
        })

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    def flush(self) -> None:
        """Write all buffered rows to parquet. Called every 5 min + on shutdown."""
        for symbol in list(self._books):
            self._flush_stream(symbol, self._books, "books")
        for symbol in list(self._trades):
            self._flush_stream(symbol, self._trades, "trades")
        for symbol in list(self._mark_price):
            self._flush_stream(symbol, self._mark_price, "mark_price")

    def flush_symbol(self, symbol: str) -> None:
        self._flush_stream(symbol, self._books,      "books")
        self._flush_stream(symbol, self._trades,     "trades")
        self._flush_stream(symbol, self._mark_price, "mark_price")

    def _flush_stream(
        self,
        symbol:  str,
        buffers: dict[str, list[dict]],
        stream:  str,
    ) -> None:
        rows = buffers.pop(symbol, [])
        if not rows:
            return

        # Split rows by UTC date (handles midnight crossings)
        by_date: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            by_date[_ms_to_date(r["timestamp_ms"])].append(r)

        fname = _flush_filename()
        for date_str, date_rows in by_date.items():
            # One new file per flush — no read-back, O(buffer) memory
            path = self._root / symbol / stream / date_str / f"{fname}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pl.DataFrame(date_rows).write_parquet(path, compression="zstd")

        total = sum(len(v) for v in by_date.values())
        logger.info("{} {}: flushed {} rows", symbol, stream, total)
