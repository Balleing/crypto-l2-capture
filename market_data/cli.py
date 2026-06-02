"""
l2cap — command-line entry point.

Usage:
    l2cap capture                                        # binance, symbols from config
    l2cap capture --exchange bybit --symbols BTCUSDT
    l2cap capture --exchange okx   --symbols BTC-USDT-SWAP
    l2cap capture --exchange deribit --symbols BTC-PERPETUAL
"""

from __future__ import annotations

import argparse
import asyncio
import sys

SUPPORTED_EXCHANGES = ["binance", "bybit", "okx", "deribit"]


def main() -> None:
    parser = argparse.ArgumentParser(prog="l2cap")
    sub    = parser.add_subparsers(dest="cmd")

    cap = sub.add_parser("capture", help="Start the capture daemon")
    cap.add_argument(
        "--exchange", "-e",
        default="binance",
        choices=SUPPORTED_EXCHANGES,
        help="Exchange to capture from (default: binance)",
    )
    cap.add_argument(
        "--symbols", "-s",
        nargs="+",
        metavar="SYMBOL",
        help="Symbols to capture (default: from config)",
    )

    # Legacy alias
    run = sub.add_parser("run", help="Alias for capture (binance only)")

    args = parser.parse_args()

    if args.cmd in ("capture", "run"):
        exchange = getattr(args, "exchange", "binance")
        symbols  = getattr(args, "symbols", None)
        from market_data.capture import main as _capture
        asyncio.run(_capture(exchange=exchange, symbols=symbols))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
