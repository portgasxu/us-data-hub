"""
US Data Hub — Price Collector
Source: Longbridge CLI (longbridge)
Commands: kline (historical OHLCV), quote (real-time)
"""

import json
import logging
import subprocess
import time
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class PriceCollector:
    """Stock price collector using Longbridge CLI."""

    def __init__(self, cli_path: str = "longbridge", symbol_suffix: str = ".US"):
        self.cli_path = cli_path
        self.symbol_suffix = symbol_suffix

    def _run_cli(self, args: list, timeout: int = 30) -> Optional[List[Dict]]:
        """Run Longbridge CLI command, return parsed JSON or None."""
        cmd = [self.cli_path] + args
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if result.returncode != 0:
                logger.error(f"Longbridge CLI error: {' '.join(cmd)} → {result.stderr[:200]}")
                return None
            data = json.loads(result.stdout)
            return data if isinstance(data, list) else [data]
        except json.JSONDecodeError as e:
            logger.error(f"Longbridge CLI JSON parse error: {e}")
            return None
        except subprocess.TimeoutExpired:
            logger.error(f"Longbridge CLI timeout: {' '.join(cmd)}")
            return None
        except Exception as e:
            logger.error(f"Longbridge CLI failed: {e}")
            return None

    def collect(self, symbol: str, count: int = 365, **kwargs) -> List[Dict]:
        """
        Collect historical price data for a symbol via Longbridge kline.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            count: Number of days of history (default 365)
            **kwargs: period, interval (ignored, used for compatibility)

        Returns:
            List of daily price dicts with keys: date, open, high, low, close, volume
        """
        symbol = symbol.upper()
        symbol_us = f"{symbol}{self.symbol_suffix}"

        # Rate limit: sleep between requests
        time.sleep(1.0)

        # Get daily kline data from Longbridge
        data = self._run_cli(["kline", symbol_us, "--format", "json"])
        if not data:
            logger.error(f"Longbridge kline failed for {symbol}")
            return []

        prices = []
        for candle in data:
            try:
                price = {
                    "date": candle["time"][:10],  # "2025-11-19"
                    "open": float(candle["open"]),
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                    "volume": float(candle["volume"]),
                    "turnover": float(candle.get("turnover", 0)),
                    "symbol": symbol,
                }
                prices.append(price)
            except (KeyError, ValueError) as e:
                logger.warning(f"Skipping malformed candle for {symbol}: {e}")
                continue

        # Limit to requested count (most recent)
        if len(prices) > count:
            prices = prices[-count:]

        logger.info(f"{symbol}: collected {len(prices)} daily candles from Longbridge")
        return prices

    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get real-time current price via Longbridge quote."""
        symbol_us = f"{symbol.upper()}{self.symbol_suffix}"
        data = self._run_cli(["quote", symbol_us, "--format", "json"])
        if data and len(data) > 0:
            return float(data[0]["last"])
        return None
