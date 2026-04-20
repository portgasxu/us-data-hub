#!/usr/bin/env python3
"""
US Data Hub — Price Collector Daemon
Continuously runs, collecting prices every 5 minutes.
Fixes the issue where python3 -m collectors.longbridge --data-type price exits immediately.
"""

import sys
import os
import time
import json
import subprocess
import logging
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()

from dayup_logger import setup_root_logger
setup_root_logger(level=logging.INFO)
logger = logging.getLogger("price_collector_daemon")

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]
INTERVAL = 300  # 5 minutes


def get_quote(symbol: str) -> dict:
    """Get real-time quote for a symbol."""
    symbol_us = f"{symbol.upper()}.US"
    try:
        result = subprocess.run(
            ["longbridge", "quote", symbol_us, "--format", "json"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            logger.warning(f"CLI error for {symbol_us}: {result.stderr[:200]}")
            return {}
        data = json.loads(result.stdout)
        return data[0] if data else {}
    except Exception as e:
        logger.warning(f"Quote failed for {symbol}: {e}")
        return {}


def collect_once(symbols: list):
    """Collect prices for all symbols."""
    total = 0
    for symbol in symbols:
        quote = get_quote(symbol)
        if quote and quote.get("last"):
            price = float(quote["last"])
            logger.info(f"  {symbol}: ${price:.2f}")
            total += 1
        else:
            logger.warning(f"  {symbol}: no data")
    logger.info(f"Price collection complete: {total}/{len(symbols)} symbols")
    return total


def main():
    logger.info(f"Price Collector Daemon started (interval={INTERVAL}s)")
    logger.info(f"Symbols: {DEFAULT_SYMBOLS}")

    # Collect immediately on start
    collect_once(DEFAULT_SYMBOLS)

    while True:
        try:
            time.sleep(INTERVAL)
            collect_once(DEFAULT_SYMBOLS)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
