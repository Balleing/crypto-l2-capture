"""
Tests for TickWriter (parquet.py) and SymbolCapture wiring (capture.py).

No network calls. Parquet tests use a tmp_path fixture.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import polars as pl
import pytest

from market_data.book.l2 import BookLevel, BookSnapshot
from market_data.storage.parquet import TickWriter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _snap(
    symbol: str = "BTCUSDT",
    ts_ms: int = 1_717_200_000_000,   # 2024-06-01 00:00:00 UTC
    last_uid: int = 100,
    bid_price: str = "50000",
    bid_qty: str = "2.0",
    ask_price: str = "50001",
    ask_qty: str = "1.0",
) -> BookSnapshot:
    return BookSnapshot(
        symbol=symbol,
        last_update_id=last_uid,
        event_time_ms=ts_ms,
        bids=[BookLevel(Decimal(bid_price), Decimal(bid_qty))],
        asks=[BookLevel(Decimal(ask_price), Decimal(ask_qty))],
    )


def _trade(symbol: str = "BTCUSDT", ts_ms: int = 1_717_200_000_000) -> dict:
    return {
        "s": symbol,
        "T": ts_ms,
        "a": 999,       # aggregate trade ID — @aggTrade uses "a", not "t"
        "p": "50000.5",
        "q": "0.01",
        "m": False,
    }


def _mark(symbol: str = "BTCUSDT", ts_ms: int = 1_717_200_000_000) -> dict:
    return {
        "symbol": symbol,
        "E": ts_ms,
        "p": "50000.1",
        "i": "49999.9",
        "r": "0.0001",
        "T": 1_717_228_800_000,
    }


def _read_stream(tmp_path: Path, symbol: str, stream: str, date: str) -> pl.DataFrame:
    """Read all parquet files for a given symbol/stream/date into one DataFrame."""
    files = sorted((tmp_path / symbol / stream / date).glob("*.parquet"))
    if not files:
        return pl.DataFrame()
    return pl.concat([pl.read_parquet(f) for f in files])


def _stream_has_files(tmp_path: Path, symbol: str, stream: str, date: str) -> bool:
    date_dir = tmp_path / symbol / stream / date
    return date_dir.is_dir() and any(date_dir.glob("*.parquet"))


# ---------------------------------------------------------------------------
# TickWriter — book snapshots
# ---------------------------------------------------------------------------

class TestTickWriterBooks:
    def test_add_book_then_flush_creates_parquet(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path, book_depth=1)
        writer.add_book(_snap())
        writer.flush()
        assert _stream_has_files(tmp_path, "BTCUSDT", "books", "2024-06-01")

    def test_flush_writes_one_row(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path, book_depth=1)
        writer.add_book(_snap())
        writer.flush()
        df = _read_stream(tmp_path, "BTCUSDT", "books", "2024-06-01")
        assert len(df) == 1

    def test_two_rows_in_one_flush(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path, book_depth=1)
        writer.add_book(_snap(last_uid=1))
        writer.add_book(_snap(last_uid=2))
        writer.flush()
        df = _read_stream(tmp_path, "BTCUSDT", "books", "2024-06-01")
        assert len(df) == 2

    def test_book_row_contains_microprice(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path, book_depth=1)
        snap = _snap(bid_qty="2.0", ask_qty="1.0")
        writer.add_book(snap)
        writer.flush()
        df = _read_stream(tmp_path, "BTCUSDT", "books", "2024-06-01")
        assert "microprice" in df.columns
        mp = df["microprice"][0]
        assert mp is not None
        # bid_qty=2, ask_qty=1 → mp = (50001×2 + 50000×1) / 3 ≈ 50000.667
        assert 50000.0 < mp < 50001.0

    def test_book_row_update_id(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path, book_depth=1)
        writer.add_book(_snap(last_uid=42))
        writer.flush()
        df = _read_stream(tmp_path, "BTCUSDT", "books", "2024-06-01")
        assert df["update_id"][0] == 42

    def test_empty_book_flush_creates_no_file(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path)
        writer.flush()
        assert not (tmp_path / "BTCUSDT").exists()

    def test_midnight_split_writes_two_date_dirs(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path, book_depth=1)
        # 2024-05-31 23:59:59.999 UTC
        writer.add_book(_snap(ts_ms=1_717_199_999_999))
        # 2024-06-01 00:00:00.000 UTC
        writer.add_book(_snap(ts_ms=1_717_200_000_000))
        writer.flush()
        assert _stream_has_files(tmp_path, "BTCUSDT", "books", "2024-05-31")
        assert _stream_has_files(tmp_path, "BTCUSDT", "books", "2024-06-01")


# ---------------------------------------------------------------------------
# TickWriter — trades
# ---------------------------------------------------------------------------

class TestTickWriterTrades:
    def test_trade_flush_creates_parquet(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path)
        writer.add_trade(_trade())
        writer.flush()
        assert _stream_has_files(tmp_path, "BTCUSDT", "trades", "2024-06-01")

    def test_trade_row_taker_sign_buy(self, tmp_path: Path) -> None:
        """buyer_is_maker=False → taker bought → sign +1."""
        writer = TickWriter(tmp_path)
        writer.add_trade(_trade())
        writer.flush()
        df = _read_stream(tmp_path, "BTCUSDT", "trades", "2024-06-01")
        assert df["taker_sign"][0] == 1

    def test_trade_row_taker_sign_sell(self, tmp_path: Path) -> None:
        """buyer_is_maker=True → taker sold → sign -1."""
        writer = TickWriter(tmp_path)
        t = _trade()
        t["m"] = True
        writer.add_trade(t)
        writer.flush()
        df = _read_stream(tmp_path, "BTCUSDT", "trades", "2024-06-01")
        assert df["taker_sign"][0] == -1

    def test_trade_price_stored_as_float(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path)
        writer.add_trade(_trade())
        writer.flush()
        df = _read_stream(tmp_path, "BTCUSDT", "trades", "2024-06-01")
        assert abs(df["price"][0] - 50000.5) < 1e-9


# ---------------------------------------------------------------------------
# TickWriter — mark price
# ---------------------------------------------------------------------------

class TestTickWriterMarkPrice:
    def test_mark_price_flush_creates_parquet(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path)
        writer.add_mark_price(_mark())
        writer.flush()
        assert _stream_has_files(tmp_path, "BTCUSDT", "mark_price", "2024-06-01")

    def test_mark_price_row_contains_funding_rate(self, tmp_path: Path) -> None:
        writer = TickWriter(tmp_path)
        writer.add_mark_price(_mark())
        writer.flush()
        df = _read_stream(tmp_path, "BTCUSDT", "mark_price", "2024-06-01")
        assert abs(df["funding_rate"][0] - 0.0001) < 1e-9


# ---------------------------------------------------------------------------
# SymbolCapture wiring
# ---------------------------------------------------------------------------

class TestSymbolCapture:
    async def test_on_trade_tags_symbol(self, tmp_path: Path) -> None:
        """_on_trade should inject symbol key before passing to writer."""
        from market_data.capture import SymbolCapture

        rest   = MagicMock()
        rest.depth = AsyncMock(return_value={
            "lastUpdateId": 100,
            "T": 1_717_200_000_000,
            "bids": [["50000.00", "1.0"]],
            "asks": [["50001.00", "1.0"]],
        })
        writer = TickWriter(tmp_path, book_depth=1)
        cap    = SymbolCapture("ETHUSDT", rest, writer)

        trade_data = {"T": 1_717_200_000_000, "a": 1, "p": "3000.0", "q": "0.1", "m": False}
        await cap._on_trade(trade_data)
        assert trade_data["s"] == "ETHUSDT"

    async def test_on_mark_price_tags_symbol(self, tmp_path: Path) -> None:
        from market_data.capture import SymbolCapture

        rest   = MagicMock()
        rest.depth = AsyncMock(return_value={
            "lastUpdateId": 100, "T": 0,
            "bids": [["50000", "1"]], "asks": [["50001", "1"]],
        })
        writer = TickWriter(tmp_path)
        cap    = SymbolCapture("BTCUSDT", rest, writer)

        mark_data = {"E": 1_717_200_000_000, "p": "50000", "i": "49999", "r": "0.0001", "T": 0}
        await cap._on_mark_price(mark_data)
        assert mark_data.get("s") == "BTCUSDT" or mark_data.get("symbol") == "BTCUSDT"

    async def test_on_book_passes_snapshot_to_writer(self, tmp_path: Path) -> None:
        import asyncio
        from market_data.capture import SymbolCapture

        rest   = MagicMock()
        rest.depth = AsyncMock(return_value={
            "lastUpdateId": 100, "T": 1_717_200_000_000,
            "bids": [["50000", "2"]], "asks": [["50001", "1"]],
        })
        writer = MagicMock(spec=TickWriter)
        cap    = SymbolCapture("BTCUSDT", rest, writer)

        depth_event = {
            "U": 101, "u": 102, "pu": 100,
            "E": 1_717_200_001_000,
            "b": [["50000", "3"]],
            "a": [],
        }
        await cap._on_depth(depth_event)
        # on_book_update fires via ensure_future — yield once to let it complete
        await asyncio.sleep(0)
        writer.add_book.assert_called_once()
