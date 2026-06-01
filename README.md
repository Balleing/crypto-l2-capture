# binance-l2-capture

Gap-free Binance USDT-M perpetual futures L2 order book capture pipeline.

Captures order book snapshots, aggregated trades, and mark price to local Parquet files using your own Binance API key. You own the data; it never leaves your machine.

## Features

- Correct snapshot + diff merge (6-step Binance protocol, with gap detection and auto-resync)
- Runtime invariants that halt on bad book state instead of silently writing garbage
- Clean Parquet output partitioned by symbol / stream / date
- Multi-symbol concurrent capture (one asyncio task per symbol, isolated failure)
- Clock drift watchdog — halts if local clock skews more than 1s from Binance server time
- No data redistribution — you run this against your own key, on your own infrastructure

## Quickstart

```bash
# 1. Install
pip install -e ".[dev]"

# 2. Set credentials
cp .env.example .env
# edit .env — add your Binance API key (read-only, IP-restricted recommended)

# 3. Configure symbols
# edit config/settings.yaml

# 4. Run
python3 -m market_data.capture
```

Data lands in `data/SYMBOL/books/`, `data/SYMBOL/trades/`, `data/SYMBOL/mark_price/`.

## Output schema

Each stream writes one Parquet file per UTC day.

**books** — L2 snapshot at every book update event  
**trades** — aggregated trades (`taker_sign`: +1 buy, -1 sell)  
**mark_price** — mark price, index price, funding rate (1s intervals)

## Requirements

- Python 3.11+
- Binance USDT-M Futures account (read-only API key sufficient)
- ~50 MB/day disk per symbol at 100ms book depth

## License

MIT
