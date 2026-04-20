"""
US Data Hub — Longbridge Trade Executor
Wraps Longbridge CLI for order management.
Commands: quote, buy, sell, positions, orders, cancel
Docs: https://open.longportapp.com/docs/cli/intro
"""

import subprocess
import json
import re
import time
import argparse
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class LongbridgeExecutor:
    """Longbridge trade executor using CLI."""

    def __init__(self, cli_path: str = "longbridge", symbol_suffix: str = ".US"):
        self.cli_path = cli_path
        self.symbol_suffix = symbol_suffix

    def _run(self, args: list, timeout: int = 30, retries: int = 2) -> Optional[Dict]:
        """Run CLI command with retry (P1 审计修复: 指数退避重试).
        
        Args:
            args: CLI arguments
            timeout: seconds per attempt
            retries: number of retry attempts (default 2)
        """
        cmd = [self.cli_path] + args
        last_error = None

        for attempt in range(1 + retries):
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
                if result.returncode != 0:
                    err_msg = result.stderr[:200]
                    last_error = f"CLI error: {' '.join(cmd)} → {err_msg}"
                    logger.error(f"[Attempt {attempt+1}/{1+retries}] {last_error}")
                else:
                    # Try JSON first
                    try:
                        return json.loads(result.stdout)
                    except json.JSONDecodeError:
                        return None  # CLI returns table format for some commands

            except subprocess.TimeoutExpired:
                last_error = f"CLI timeout: {' '.join(cmd)}"
                logger.error(f"[Attempt {attempt+1}/{1+retries}] {last_error}")
            except Exception as e:
                last_error = f"CLI failed: {e}"
                logger.error(f"[Attempt {attempt+1}/{1+retries}] {last_error}")

            if attempt < retries:
                wait = 2 ** attempt  # 1s, 2s exponential backoff
                logger.info(f"Retrying in {wait}s...")
                time.sleep(wait)

        logger.error(f"All {1+retries} attempts failed for: {' '.join(cmd)}")
        return None

    def get_quote(self, symbol: str) -> Optional[Dict]:
        """Get real-time quote."""
        symbol_us = f"{symbol.upper()}{self.symbol_suffix}"
        return self._run(["quote", symbol_us])

    def get_positions(self) -> List[Dict]:
        """Get current positions — parses CLI table output."""
        cmd = [self.cli_path, "positions"]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                logger.error(f"Longbridge positions error: {result.stderr[:200]}")
                return []

            output = result.stdout.strip()
            if not output:
                return []

            positions = []
            lines = output.split('\n')

            # Skip header lines (first two lines are header + separator)
            for line in lines[2:]:
                line = line.strip()
                if not line or line.startswith('|--') or line.startswith('+--'):
                    continue

                # Parse pipe-separated table: | Symbol | Name | Qty | Avail | Cost | Currency | Market |
                parts = [p.strip() for p in line.split('|')]
                # Remove empty strings from leading/trailing pipes
                parts = [p for p in parts if p]

                if len(parts) >= 5:
                    symbol_raw = parts[0]
                    name = parts[1]
                    quantity = int(parts[2]) if parts[2] else 0
                    available = int(parts[3]) if parts[3] else 0
                    cost_price = float(parts[4]) if parts[4] else 0.0

                    # Strip exchange suffix
                    clean_symbol = symbol_raw.split('.')[0] if '.' in symbol_raw else symbol_raw

                    positions.append({
                        'symbol': symbol_raw,
                        'clean_symbol': clean_symbol,
                        'name': name,
                        'quantity': quantity,
                        'available': available,
                        'cost_price': cost_price,
                    })

            return positions

        except Exception as e:
            logger.error(f"Longbridge get_positions failed: {e}")
            return []

    def buy(self, symbol: str, quantity: int, price: float = None,
            order_type: str = "MO") -> Optional[Dict]:
        """Place a buy order."""
        symbol_us = f"{symbol.upper()}{self.symbol_suffix}"
        args = ["order", "buy", symbol_us, str(quantity), "--order-type", order_type, "-y"]
        if price is not None:
            args.extend(["--price", str(price)])
        return self._run(args)

    def sell(self, symbol: str, quantity: int, price: float = None,
             order_type: str = "MO") -> Optional[Dict]:
        """Place a sell order."""
        symbol_us = f"{symbol.upper()}{self.symbol_suffix}"
        args = ["order", "sell", symbol_us, str(quantity), "--order-type", order_type, "-y"]
        if price is not None:
            args.extend(["--price", str(price)])
        return self._run(args)

    def get_orders(self, symbol: str = None) -> List[Dict]:
        """Get order history."""
        args = ["order", "--format", "json"]
        if symbol:
            args.append(f"{symbol.upper()}{self.symbol_suffix}")
        data = self._run(args)
        if data is None:
            return []
        return data if isinstance(data, list) else [data]

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order by ID."""
        result = self._run(["order", "cancel", order_id])
        return result is not None


def main():
    """CLI entry point for Longbridge executor."""
    parser = argparse.ArgumentParser(description="US Data Hub — Longbridge Trade Executor")
    parser.add_argument("--action", required=True,
                       choices=["quote", "positions", "orders", "buy", "sell", "cancel"],
                       help="Action to perform")
    parser.add_argument("--symbol", default="AAPL", help="Stock symbol")
    parser.add_argument("--qty", type=int, default=0, help="Quantity (for buy/sell)")
    parser.add_argument("--price", type=float, default=None, help="Price (for limit orders)")
    parser.add_argument("--order-type", default="MO", help="Order type: MO (market) or LO (limit)")
    parser.add_argument("--order-id", default=None, help="Order ID (for cancel)")
    args = parser.parse_args()

    ex = LongbridgeExecutor()

    if args.action == "quote":
        result = ex.get_quote(args.symbol)
        print(json.dumps(result, indent=2) if result else "Failed to get quote")
    elif args.action == "positions":
        result = ex.get_positions()
        print(json.dumps(result, indent=2))
    elif args.action == "orders":
        result = ex.get_orders(args.symbol)
        print(json.dumps(result, indent=2))
    elif args.action == "buy":
        result = ex.buy(args.symbol, args.qty, args.price, args.order_type)
        print(json.dumps(result, indent=2) if result else "Failed to place buy order")
    elif args.action == "sell":
        result = ex.sell(args.symbol, args.qty, args.price, args.order_type)
        print(json.dumps(result, indent=2) if result else "Failed to place sell order")
    elif args.action == "cancel":
        result = ex.cancel_order(args.order_id)
        print(f"Order canceled: {result}")


if __name__ == "__main__":
    main()
