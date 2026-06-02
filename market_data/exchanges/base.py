"""
Exchange adapter protocol — shared interface for all supported exchanges.

Every exchange (Binance, Bybit, OKX, Deribit) implements:
  ExchangeWsAdapter   — WebSocket connection + event parsing
  ExchangeRestAdapter — REST snapshot fetch + clock check

Raw exchange messages are normalized into:
  NormalizedDepthEvent — common diff format fed into DepthStateMachine
  NormalizedSnapshot   — common snapshot format fed into L2Book
  NormalizedTrade      — common trade format fed into TickWriter

The core pipeline (L2Book, DepthStateMachine, TickWriter) never sees
exchange-specific field names. Only the adapters know about e.g. Binance's
"pu", Bybit's "seq", OKX's "prevSeqId", Deribit's "prev_change_id".
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Normalized event types
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class NormalizedDepthEvent:
    """
    Exchange-agnostic L2 diff event.

    Fields map to Binance concepts by name — other exchanges normalise into this:
      first_update_id  — Binance U  / Bybit first seq / OKX seqId start
      last_update_id   — Binance u  / Bybit last seq  / OKX seqId
      prev_update_id   — Binance pu / Bybit prevSeq   / OKX prevSeqId / Deribit prev_change_id
                         None if the exchange does not provide continuity fields
      event_time_ms    — exchange event timestamp in milliseconds
      bids             — list of [price_str, qty_str]
      asks             — list of [price_str, qty_str]
      raw              — original exchange message (for debugging)
    """
    first_update_id: int
    last_update_id:  int
    prev_update_id:  int | None
    event_time_ms:   int
    bids:            list[list[str]]
    asks:            list[list[str]]
    raw:             dict = field(default_factory=dict, repr=False)


@dataclass(slots=True)
class NormalizedSnapshot:
    """REST order book snapshot in a common format."""
    last_update_id: int
    event_time_ms:  int
    bids:           list[list[str]]
    asks:           list[list[str]]


@dataclass(slots=True)
class NormalizedTrade:
    """Single aggTrade / trade event."""
    symbol:         str
    timestamp_ms:   int
    trade_id:       int
    price:          str
    qty:            str
    buyer_is_maker: bool


# ---------------------------------------------------------------------------
# Adapter callback types
# ---------------------------------------------------------------------------

DepthHandler = Callable[[NormalizedDepthEvent], Awaitable[None]]
TradeHandler  = Callable[[NormalizedTrade],      Awaitable[None]]


# ---------------------------------------------------------------------------
# Abstract adapters
# ---------------------------------------------------------------------------

class ExchangeWsAdapter(ABC):
    """
    Abstract WebSocket adapter for one exchange.

    Subclasses connect, subscribe, parse raw messages, and fire the
    registered depth/trade handlers with normalized events.

    The reconnect / backoff loop lives here — the caller just awaits run().
    """

    def __init__(
        self,
        symbol:       str,
        on_depth:     DepthHandler | None = None,
        on_trade:     TradeHandler  | None = None,
    ) -> None:
        self.symbol    = symbol.upper()
        self._on_depth = on_depth
        self._on_trade = on_trade

    @property
    @abstractmethod
    def exchange(self) -> str:
        """Short exchange name e.g. 'binance', 'bybit', 'okx', 'deribit'."""

    @abstractmethod
    async def run(self) -> None:
        """Connect, dispatch events, reconnect on failure. Never returns."""

    @abstractmethod
    def build_url(self) -> str:
        """Return the WebSocket endpoint URL."""

    @abstractmethod
    def parse_depth(self, raw: dict) -> NormalizedDepthEvent | None:
        """
        Parse a raw WebSocket message into a NormalizedDepthEvent.

        Return None if the message is not a depth diff (e.g. a ping, trade,
        subscription ack) — the caller skips None silently.
        """

    @abstractmethod
    def parse_trade(self, raw: dict) -> NormalizedTrade | None:
        """Parse a raw message into a NormalizedTrade, or None if not a trade."""


class ExchangeRestAdapter(ABC):
    """
    Abstract REST adapter for one exchange.

    Provides snapshot fetch and clock drift check. Subclasses map
    exchange-specific JSON to NormalizedSnapshot.
    """

    @property
    @abstractmethod
    def exchange(self) -> str:
        """Short exchange name."""

    @abstractmethod
    async def __aenter__(self) -> "ExchangeRestAdapter": ...

    @abstractmethod
    async def __aexit__(self, *_: Any) -> None: ...

    @abstractmethod
    async def fetch_snapshot(self, symbol: str, limit: int = 1000) -> NormalizedSnapshot:
        """Fetch a REST order book snapshot."""

    @abstractmethod
    async def check_clock_drift(self) -> int:
        """
        Compare local clock against exchange server time.

        Returns drift in ms (positive = local ahead).
        Raise ClockDriftError if |drift| exceeds exchange threshold.
        """


class ClockDriftError(Exception):
    """Raised when clock drift vs exchange server exceeds the halt threshold."""
