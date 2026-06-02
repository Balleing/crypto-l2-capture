# crypto-l2-capture

[![CI](https://github.com/Balleing/binance-l2-capture/actions/workflows/ci.yml/badge.svg)](https://github.com/Balleing/binance-l2-capture/actions/workflows/ci.yml)

**Gap-free L2 order book capture for Binance, Bybit, OKX, and Deribit — self-hosted, data stays on your machine.**

`crypto-l2-capture` records USDT-M perpetual futures order book data to disk across four exchanges. It implements each exchange's full snapshot/diff merge protocol with gap detection, automatic resync, and runtime invariants that **halt on a bad book state rather than silently writing garbage to disk**.

You run it on your own infrastructure. The data stays on your machine — no third-party ever sees it.

---

## Exchanges

| Exchange | Symbol format | Continuity | WS endpoint |
|---|---|---|---|
| Binance | `BTCUSDT` | `pu` field on futures diff | `wss://fstream.binance.com` |
| Bybit | `BTCUSDT` | `seq` tracked across deltas | `wss://stream.bybit.com/v5/public/linear` |
| OKX | `BTC-USDT-SWAP` | `seqId` / `prevSeqId` | `wss://ws.okx.com:8443/ws/v5/public` |
| Deribit | `BTC-PERPETUAL` | `change_id` / `prev_change_id` | `wss://www.deribit.com/ws/api/v2` |

---

## Why this exists

Most homemade order book captures are subtly wrong. They drop diff events under load, mis-order updates during a reconnect, or let the book drift out of sync with the exchange — and they do it silently, so you don't find out until your backtest produces an edge that evaporates live. This tool's job is to make those failures **loud and impossible to miss**, so the data you record is data you can trust.

---

## What it guarantees

Correctness first — these are the failure modes it refuses to commit:

- **Never writes a crossed book.** Best bid ≥ best ask is treated as corruption and halts capture.
- **Halts on a detected gap instead of logging garbage.** A sequence break triggers an automatic snapshot re-fetch and book rebuild — it does not keep appending bad state.
- **Enforces monotonic sequence ordering.** Out-of-order or stale diff events are rejected, not merged.
- **Detects clock skew.** If the local clock drifts more than 1s from the exchange server time, capture halts rather than timestamping events incorrectly.
- **Exchange-agnostic continuity.** Each exchange has a different continuity field (`pu`, `seq`, `seqId`, `change_id`). The adapter layer normalizes all of them before the state machine processes them — the same gap detection logic runs on every exchange.

Operational:

- Full snapshot/diff merge protocol per exchange, correctly seeded and maintained.
- Automatic reconnect with exponential backoff.
- Parquet output rotated every 5 minutes, partitioned by exchange, symbol, and date.
- Read-only by design — needs no trading permissions.
- Runs unattended for days on a small VPS.

---

## Quickstart

Requires Python 3.11+. No API key needed for market data capture — all endpoints are public.

```bash
git clone https://github.com/Balleing/binance-l2-capture.git
cd binance-l2-capture
pip install -e .
cp .env.example .env
# edit config/settings.yaml — set your symbols
l2cap capture --exchange binance --symbols BTCUSDT ETHUSDT
```

Data lands in `data/{exchange}/SYMBOL/books/`, `trades/`, partitioned by date.

---

## CLI

```bash
# Binance USDT-M futures
l2cap capture --exchange binance --symbols BTCUSDT ETHUSDT

# Bybit Linear perpetuals
l2cap capture --exchange bybit --symbols BTCUSDT ETHUSDT

# OKX perpetual swaps (note symbol format)
l2cap capture --exchange okx --symbols BTC-USDT-SWAP ETH-USDT-SWAP

# Deribit perpetuals (note symbol format)
l2cap capture --exchange deribit --symbols BTC-PERPETUAL ETH-PERPETUAL
```

Run multiple exchanges in parallel by launching separate processes or using a process manager like `supervisord`.

---

## Data layout

```
data/
  binance/
    BTCUSDT/
      books/2025-01-15/
        12-00-00.parquet
        12-05-00.parquet
      trades/2025-01-15/
  bybit/
    BTCUSDT/
      books/2025-01-15/
  okx/
    BTC-USDT-SWAP/
      books/2025-01-15/
  deribit/
    BTC-PERPETUAL/
      books/2025-01-15/
```

---

## Output schema

Three streams per symbol, each written as `data/{exchange}/SYMBOL/<stream>/YYYY-MM-DD/HH-MM-SS.parquet`. Files load directly into pandas, Polars, or DuckDB.

**books** — full L2 snapshot at every book update event (~100ms)

| Field | Type | Notes |
|---|---|---|
| `timestamp_ms` | int64 | Exchange event time (ms since epoch) |
| `received_at_ms` | int64 | Local receive time; `received_at_ms − timestamp_ms` = capture latency |
| `update_id` | int64 | Sequence ID (exchange-normalized) |
| `microprice` | float64 | `(bid_qty×ask + ask_qty×bid) / (bid_qty + ask_qty)` |
| `imbalance` | float64 | `bid_qty / (bid_qty + ask_qty)` at best level |
| `mid` | float64 | `(best_bid + best_ask) / 2` |
| `spread` | float64 | `best_ask − best_bid` |
| `best_bid_price`, `best_bid_qty` | float64 | Top-of-book bid |
| `best_ask_price`, `best_ask_qty` | float64 | Top-of-book ask |
| `bid_price_N`, `bid_qty_N` | float64 | N = 0..depth−1 |
| `ask_price_N`, `ask_qty_N` | float64 | N = 0..depth−1 |

**trades** — aggregated trade events

| Field | Type | Notes |
|---|---|---|
| `timestamp_ms` | int64 | Trade time |
| `trade_id` | int64 | Exchange trade ID |
| `price` | float64 | Trade price |
| `qty` | float64 | Trade quantity |
| `taker_sign` | int | `+1` taker bought, `−1` taker sold |

---

## Configuration

```yaml
# config/settings.yaml
symbols:
  - BTCUSDT

exchange: binance   # default exchange (overridden by --exchange flag)

capture:
  book_depth: 20
  data_dir: "data"
```

Credentials (if required by an exchange for private endpoints) go in `/etc/scalper/credentials.env` (chmod 600) and are loaded via `pydantic-settings`. Never commit credentials.

---

## Is this legal?

Yes — because of how the tool is designed.

Exchanges prohibit the **commercial redistribution** of their market data. This project never redistributes data. **You** run the software, on **your** machine. Every byte of market data flows directly from the exchange to your own storage. If you go on to use the data you capture, your use is governed by your own agreement with the exchange.

This section explains the project's design; it is not legal advice.

---

## License

[MIT](./LICENSE) — free to use, modify, and self-host.

The author provides no market data, no trading signals, and no financial advice. This software is provided as-is, without warranty of any kind.
