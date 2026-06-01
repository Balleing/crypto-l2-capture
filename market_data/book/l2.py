"""
L2 order book state — in-memory bid/ask ladder with runtime invariants.

This module owns only the price-level data structure and the four
invariants that must hold at every point in time. The Binance
snapshot/diff merge state machine lives in snapshot.py.

Invariants enforced (raise BookInvariantError on violation):
  1. No crossed book: best bid < best ask (when both sides non-empty)
  2. Monotonic update ID: each applied event's u > last applied u
  3. No negative quantity at any level
  4. pu continuity: each diff event's pu == last applied u
     (checked by the caller in snapshot.py before calling apply_diff)

Microprice formula (Stoikov 2018):
    microprice = (ask × bid_qty + bid × ask_qty) / (bid_qty + ask_qty)
Where bid and ask are the best-price levels and quantities are the
respective best-level quantities.
Reference: Stoikov, S. (2018). "The micro-price: a high-frequency
estimator of future prices." Quantitative Finance 18(12), 1959–1966.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterator


class BookInvariantError(Exception):
    """Raised when a book update would violate a structural invariant."""


# Price and quantity stored as Decimal to avoid float rounding issues
# when comparing levels. Conversion happens once on ingress.
Price = Decimal
Qty   = Decimal


@dataclass(slots=True)
class BookLevel:
    price: Price
    qty:   Qty


@dataclass
class BookSide:
    """One side of the book (bids or asks) as a sorted list of levels.

    Bids are sorted descending (best = index 0).
    Asks are sorted ascending (best = index 0).
    Zero-quantity updates remove the level entirely.
    """
    _is_bid: bool
    _levels: dict[Price, Qty] = field(default_factory=dict)

    def apply(self, price_str: str, qty_str: str) -> None:
        """Apply one price-level update from a WS depth diff event.

        Raises BookInvariantError if qty < 0.
        Qty == 0 removes the level (Binance convention).
        """
        price = Decimal(price_str)
        qty   = Decimal(qty_str)
        if qty < 0:
            raise BookInvariantError(
                f"negative quantity {qty} at price {price}"
            )
        if qty == 0:
            self._levels.pop(price, None)
        else:
            self._levels[price] = qty

    def best(self) -> BookLevel | None:
        """Return the best-priced level, or None if empty."""
        if not self._levels:
            return None
        if self._is_bid:
            p = max(self._levels)
        else:
            p = min(self._levels)
        return BookLevel(price=p, qty=self._levels[p])

    def levels(self, depth: int = 20) -> list[BookLevel]:
        """Return top-N levels sorted best-first."""
        prices = sorted(self._levels, reverse=self._is_bid)[:depth]
        return [BookLevel(price=p, qty=self._levels[p]) for p in prices]

    def __len__(self) -> int:
        return len(self._levels)

    def __iter__(self) -> Iterator[BookLevel]:
        return (BookLevel(price=p, qty=q) for p, q in self._levels.items())


@dataclass
class BookSnapshot:
    """Immutable cross-sectional view of the book at a point in time.

    Returned by L2Book.snapshot(). Consumers should not hold references
    across WS events — take a fresh snapshot each time.
    """
    symbol:         str
    last_update_id: int
    event_time_ms:  int
    bids:           list[BookLevel]
    asks:           list[BookLevel]

    @property
    def best_bid(self) -> BookLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> BookLevel | None:
        return self.asks[0] if self.asks else None

    @property
    def mid(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid.price + self.best_ask.price) / 2

    @property
    def spread(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask.price - self.best_bid.price

    @property
    def microprice(self) -> Decimal | None:
        """
        Stoikov (2018) microprice using best bid/ask quantities:

            mp = (ask_price × bid_qty + bid_price × ask_qty)
                 / (bid_qty + ask_qty)

        Returns None when either side is empty.
        Reference: Stoikov (2018), "The micro-price", eq. (1).
        """
        b = self.best_bid
        a = self.best_ask
        if b is None or a is None:
            return None
        denom = b.qty + a.qty
        if denom == 0:
            return None
        return (a.price * b.qty + b.price * a.qty) / denom

    @property
    def imbalance(self) -> Decimal | None:
        """
        Best-level order imbalance: (bid_qty − ask_qty) / (bid_qty + ask_qty).

        Range [−1, +1]. Positive → buy pressure. Negative → sell pressure.
        """
        b = self.best_bid
        a = self.best_ask
        if b is None or a is None:
            return None
        denom = b.qty + a.qty
        if denom == 0:
            return None
        return (b.qty - a.qty) / denom


class L2Book:
    """
    In-memory L2 order book for one symbol.

    Invariants 1–3 enforced on every update. Invariant 4 (pu continuity)
    is the caller's responsibility: snapshot.py checks pu before calling
    apply_diff().

    All price/qty strings are converted to Decimal on ingress.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol   = symbol.upper()
        self._bids    = BookSide(_is_bid=True)
        self._asks    = BookSide(_is_bid=False)
        self._last_u  = -1    # last applied final update ID
        self._last_ts = 0     # event_time of last applied event (ms)

    # ------------------------------------------------------------------
    # Initialisation from REST snapshot
    # ------------------------------------------------------------------

    def load_snapshot(
        self,
        last_update_id: int,
        bids: list[list[str]],
        asks: list[list[str]],
        event_time_ms: int = 0,
    ) -> None:
        """
        Seed the book from a GET /fapi/v1/depth REST snapshot.

        Clears any existing state. lastUpdateId from the REST response
        becomes the new last_u.

        Raises BookInvariantError on negative qty or crossed book.
        """
        self._bids    = BookSide(_is_bid=True)
        self._asks    = BookSide(_is_bid=False)
        self._last_u  = last_update_id
        self._last_ts = event_time_ms

        for price_str, qty_str in bids:
            self._bids.apply(price_str, qty_str)
        for price_str, qty_str in asks:
            self._asks.apply(price_str, qty_str)

        self._assert_not_crossed()

    # ------------------------------------------------------------------
    # Incremental diff update from WS @depth@100ms
    # ------------------------------------------------------------------

    def apply_diff(
        self,
        U:             int,          # first update ID in this event
        u:             int,          # final update ID in this event
        event_time_ms: int,
        bids:          list[list[str]],
        asks:          list[list[str]],
    ) -> None:
        """
        Apply one incremental diff event from the @depth@100ms stream.

        Invariants checked:
          2. Monotonic u: u > self._last_u
          3. No negative qty (delegated to BookSide.apply)
          1. No crossed book (after applying all updates in this event)

        Invariant 4 (pu == last_u) must be verified by the caller
        (snapshot.py) before invoking this method.

        Raises BookInvariantError on any violation.
        """
        # Invariant 2: monotonic update ID
        if u <= self._last_u:
            raise BookInvariantError(
                f"{self.symbol}: non-monotonic update ID: "
                f"u={u} <= last_u={self._last_u}"
            )

        for price_str, qty_str in bids:
            self._bids.apply(price_str, qty_str)
        for price_str, qty_str in asks:
            self._asks.apply(price_str, qty_str)

        # Invariant 1: no crossed book
        self._assert_not_crossed()

        self._last_u  = u
        self._last_ts = event_time_ms

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def snapshot(self, depth: int = 20) -> BookSnapshot:
        """Return an immutable snapshot of current book state."""
        return BookSnapshot(
            symbol=self.symbol,
            last_update_id=self._last_u,
            event_time_ms=self._last_ts,
            bids=self._bids.levels(depth),
            asks=self._asks.levels(depth),
        )

    @property
    def last_update_id(self) -> int:
        return self._last_u

    @property
    def is_ready(self) -> bool:
        """True once load_snapshot() has been called at least once."""
        return self._last_u >= 0

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _assert_not_crossed(self) -> None:
        best_bid = self._bids.best()
        best_ask = self._asks.best()
        if best_bid is None or best_ask is None:
            return
        if best_bid.price >= best_ask.price:
            raise BookInvariantError(
                f"{self.symbol}: crossed book — "
                f"bid={best_bid.price} >= ask={best_ask.price}"
            )
