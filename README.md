# binance-l2-capture

**Gap-free Binance USDT-M perpetual futures order book recording — self-hosted, bring-your-own-key.**

`binance-l2-capture` records Binance USDT-M perpetual futures order book data to disk. It implements Binance's full snapshot/diff merge protocol with gap detection, automatic resync, and runtime invariants that **halt on a bad book state rather than silently writing garbage to disk**.

You run it against your own API key, on your own infrastructure. The data stays on your machine — the author never sees, stores, or transmits any of it.

---

## Why this exists

Most homemade order book captures are subtly wrong. They drop diff events under load, mis-order updates during a reconnect, or let the book drift out of sync with the exchange — and they do it silently, so you don't find out until your backtest produces an edge that evaporates live. This tool's job is to make those failures **loud and impossible to miss**, so the data you record is data you can trust.

---

## What it guarantees

Correctness first — these are the failure modes it refuses to commit:

- **Never writes a crossed book.** Best bid ≥ best ask is treated as corruption and halts capture.
- **Halts on a detected gap instead of logging garbage.** A sequence break triggers an automatic snapshot re-fetch and book rebuild — it does not keep appending bad state.
- **Enforces monotonic sequence ordering.** Out-of-order or stale diff events are rejected, not merged.
- **Detects clock skew.** If the local clock drifts more than 1s from Binance server time, capture halts rather than timestamping events incorrectly.

Operational:

- Full Binance snapshot/diff merge protocol, correctly seeded and maintained.
- Automatic reconnect with backoff.
- Parquet output rotated every 5 minutes, partitioned by symbol and date.
- Read-only by design — needs no trading permissions on the API key.
- Runs unattended for days on a small VPS.

---

## Quickstart

Requires Python 3.11+ and a Binance API key (read-only is sufficient — see [Is this legal?](#is-this-legal)).

```bash
git clone https://github.com/Balleing/binance-l2-capture.git
cd binance-l2-capture
pip install -e .
cp .env.example .env          # add your BINANCE_API_KEY
# edit config/settings.yaml — set your symbols
l2cap run                     # data starts landing in ./data/
```

Data lands in `data/SYMBOL/books/`, `data/SYMBOL/trades/`, `data/SYMBOL/mark_price/`, rotated every 5 minutes.

---

## Is this legal?

Yes — because of how the tool is designed.

Binance's terms prohibit the **commercial redistribution** of its market data. This project never redistributes data, because it never receives any. **You** run the software, against **your** API key, on **your** machine. Every byte of market data flows directly from Binance to your own storage. The author of this project is never in that path — there is nothing to redistribute.

That's the entire design constraint behind "bring-your-own-key": it keeps you, the operator, as the only party who ever touches the data. If you go on to use the data you capture, your use is governed by your own agreement with Binance — review their API terms for your jurisdiction and use case.

This section explains the project's design; it is not legal advice.

---

## Output schema

Three streams, each written to `data/SYMBOL/<stream>/YYYY-MM-DD/HH-MM-SS.parquet`. Files load directly into pandas, Polars, or DuckDB — no preprocessing needed.

**books** — full L2 snapshot at every book update event (~100ms)

| Field | Type | Notes |
|---|---|---|
| `timestamp_ms` | int64 | Exchange event time (ms since epoch) |
| `update_id` | int64 | Binance sequence ID, for gap verification |
| `microprice` | float64 | Volume-weighted mid: `(bid_qty×ask + ask_qty×bid) / (bid_qty + ask_qty)` |
| `imbalance` | float64 | `bid_qty / (bid_qty + ask_qty)` at best level |
| `mid` | float64 | `(best_bid + best_ask) / 2` |
| `spread` | float64 | `best_ask − best_bid` |
| `best_bid_price`, `best_bid_qty` | float64 | Top-of-book bid |
| `best_ask_price`, `best_ask_qty` | float64 | Top-of-book ask |
| `bid_price_N`, `bid_qty_N` | float64 | N = 0..book_depth−1, N=0 is best bid |
| `ask_price_N`, `ask_qty_N` | float64 | N = 0..book_depth−1, N=0 is best ask |

**trades** — aggregated trade events

| Field | Type | Notes |
|---|---|---|
| `timestamp_ms` | int64 | Trade time (ms since epoch) |
| `trade_id` | int64 | Binance trade ID |
| `price` | float64 | Trade price |
| `qty` | float64 | Trade quantity |
| `taker_sign` | int | `+1` taker bought, `−1` taker sold |

**mark_price** — Binance mark price at 1-second intervals

| Field | Type | Notes |
|---|---|---|
| `timestamp_ms` | int64 | Event time (ms since epoch) |
| `mark_price` | float64 | Mark price (used for liquidations) |
| `index_price` | float64 | Underlying index price |
| `funding_rate` | float64 | Next funding rate |
| `next_funding_time` | int64 | Next funding timestamp (ms) |

---

## Configuration

All behavior is driven by `config/settings.yaml`. The API key is read from the environment variable `BINANCE_API_KEY` — it is never stored in config or the repo.

```yaml
# Symbols to capture (USDT-M perpetuals only)
symbols:
  - BTCUSDT
  - ETHUSDT

# Binance endpoints — only change these for testnet
exchange:
  ws_base:   "wss://fstream.binance.com"
  rest_base: "https://fapi.binance.com"

capture:
  book_depth: 20     # levels per side written to parquet
  data_dir:  "data"  # output root; relative to working directory
```

---

## Pro (coming soon)

The open-source core captures clean data and will stay free and maintained. A Pro tier is in the works for people running this at scale: concurrent multi-symbol capture, automatic gap-resync across reconnects, a live monitoring dashboard, additional storage backends, and a tested AWS Tokyo (`ap-northeast-1`) deploy recipe for low-latency capture.

Star the repo to follow along — details when it's ready.

---

## License

[MIT](./LICENSE) — the open-source core is free to use, modify, and self-host.

The author provides no market data, no trading signals, and no financial advice. This software is provided as-is, without warranty of any kind.
