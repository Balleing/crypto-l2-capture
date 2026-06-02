"""
Risk-adjusted return metrics computed from captured L2 data.

All functions operate on Polars Series/DataFrames — the same types
produced by pl.scan_parquet() on the books stream.

Annualisation convention: crypto markets run 24/7/365, so the
annualisation factor is based on calendar minutes, not trading days.
  1-minute returns  → periods_per_year = 525_960
  1-hour  returns   → periods_per_year = 8_766
  1-day   returns   → periods_per_year = 365

Pass periods_per_year=1 to get a per-period (non-annualised) ratio.
"""

from __future__ import annotations

import math

import polars as pl

# Crypto default: calendar minutes per year
_MINUTES_PER_YEAR: float = 365.25 * 24 * 60  # 525_960


def mid_returns(
    df: pl.DataFrame,
    *,
    freq: str = "1m",
    timestamp_col: str = "timestamp_ms",
    price_col: str = "mid",
) -> pl.Series:
    """
    Resample a books DataFrame to ``freq`` and return log returns of mid-price.

    Parameters
    ----------
    df:
        Books DataFrame produced by pl.scan_parquet("…/books/…").collect().
    freq:
        Polars duration string, e.g. "1m", "5m", "1h", "1d".
    """
    resampled = (
        df.with_columns(
            pl.from_epoch(timestamp_col, time_unit="ms").alias("_ts")
        )
        .sort("_ts")
        .group_by_dynamic("_ts", every=freq)
        .agg(pl.col(price_col).last().alias("close"))
        .sort("_ts")
        .select("close")
    )

    prices = resampled["close"].drop_nulls()
    if len(prices) < 2:
        return pl.Series("returns", [], dtype=pl.Float64)

    # log returns: ln(p_t / p_{t-1})
    returns = (prices / prices.shift(1)).log().drop_nulls()
    return returns.rename("returns")


def sharpe_ratio(
    returns: pl.Series,
    *,
    risk_free_rate: float = 0.0,
    periods_per_year: float = _MINUTES_PER_YEAR,
) -> float:
    """
    Annualised Sharpe ratio.

    Sharpe = (mean(r) - Rf) / std(r)  ×  sqrt(periods_per_year)

    Parameters
    ----------
    returns:
        Per-period log returns (e.g. from ``mid_returns()``).
    risk_free_rate:
        Risk-free rate *per period* (default 0 — standard for crypto).
    periods_per_year:
        Annualisation factor. Use 525_960 for 1-minute returns (default),
        8_766 for hourly, 365 for daily.
    """
    r = returns.drop_nulls()
    if len(r) < 2:
        return float("nan")

    excess = r - risk_free_rate
    mean   = excess.mean()
    std    = excess.std(ddof=1)

    if std == 0 or std is None:
        return float("inf") if mean > 0 else float("-inf")

    return float(mean / std * math.sqrt(periods_per_year))


def sortino_ratio(
    returns: pl.Series,
    *,
    risk_free_rate: float = 0.0,
    periods_per_year: float = _MINUTES_PER_YEAR,
) -> float:
    """
    Annualised Sortino ratio — like Sharpe but penalises only downside vol.

    Sortino = (mean(r) - Rf) / downside_std(r)  ×  sqrt(periods_per_year)
    """
    r = returns.drop_nulls()
    if len(r) < 2:
        return float("nan")

    excess   = r - risk_free_rate
    mean     = excess.mean()
    downside = excess.filter(excess < 0)

    if len(downside) == 0:
        return float("inf")

    downside_std = downside.std(ddof=1)
    if downside_std == 0 or downside_std is None:
        return float("inf")

    return float(mean / downside_std * math.sqrt(periods_per_year))
