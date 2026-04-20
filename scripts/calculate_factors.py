#!/usr/bin/env python3
"""
US Data Hub — 因子计算入口
============================
计算技术指标因子（动量、波动率、RSI 等）。

用法:
    python scripts/calculate_factors.py              # 计算所有 watchlist 因子
    python scripts/calculate_factors.py --symbol NVDA  # 只计算指定股票
"""

import sys
import os
import argparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()

from storage import Database
from dayup_logger import setup_root_logger
import logging

setup_root_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Calculate technical factors")
    parser.add_argument("--symbol", default=None, help="Specific symbol (default: all watchlist)")
    args = parser.parse_args()

    db = Database()
    db.init_schema()

    # Get symbols
    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        try:
            rows = db.conn.execute("SELECT DISTINCT symbol FROM watchlist").fetchall()
            symbols = [r[0] for r in rows]
        except Exception:
            symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]

    logger.info(f"Calculating factors for {len(symbols)} symbols: {symbols}")

    try:
        from analysis.factor_from_prices import calculate_factors_for_symbol
        count = 0
        for symbol in symbols:
            try:
                result = calculate_factors_for_symbol(db, symbol)
                if result:
                    count += 1
                    logger.info(f"  ✅ {symbol}: {len(result)} factors calculated")
                else:
                    logger.warning(f"  ⚠️  {symbol}: no factors calculated")
            except Exception as e:
                logger.error(f"  ❌ {symbol}: {e}")

        logger.info(f"Factor calculation complete: {count}/{len(symbols)} symbols")
    except ImportError:
        logger.warning("factor_from_prices module not available, skipping")
    except Exception as e:
        logger.error(f"Factor calculation error: {e}")

    db.close()


if __name__ == "__main__":
    main()
