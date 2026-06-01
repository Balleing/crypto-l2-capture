"""
l2cap — command-line entry point.

Usage:
    l2cap run      Start the capture daemon.
"""

from __future__ import annotations

import sys
import asyncio


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] != "run":
        print("Usage: l2cap run")
        sys.exit(1)
    from market_data.capture import main as _capture
    asyncio.run(_capture())


if __name__ == "__main__":
    main()
