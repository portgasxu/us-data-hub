#!/usr/bin/env python3
"""
US Data Hub — Data Collector Script
Collects data from all configured sources and stores to database.

Usage:
    python scripts/collect.py --source all
    python scripts/collect.py --source sec --symbol AAPL
    python scripts/collect.py --source google_news --symbol TSLA
    python scripts/collect.py --source reddit --symbol NVDA
    python scripts/collect.py --source longbridge --symbol MSFT --type news
"""

import sys
import os
import time
import argparse
import logging
from datetime import datetime

# Load .env file before any other imports
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import Database
from normalizers.schemas import (
    normalize_sec_filing, normalize_news,
    normalize_reddit_post, normalize_capital_flow,
    validate_data_point,
)
from processors.sentiment import batch_score_sentiment

from dayup_logger import setup_root_logger; setup_root_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

PROXY = "http://127.0.0.1:7890"

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]


def collect_source(source: str, symbols: list, data_type: str = None) -> tuple:
    """
    Collect data from a source.
    Returns (new_count, total_count).
    """
    db = Database()
    total_new = 0
    total_fetched = 0
    start_time = time.time()

    try:
        if source == "sec":
            from collectors.sec import SECCollector
            collector = SECCollector()
            for symbol in symbols:
                raw_items = collector.collect(symbol, count=10)
                total_fetched += len(raw_items)
                for item in raw_items:
                    dp = normalize_sec_filing(item)
                    errors = validate_data_point(dp)
                    if errors:
                        logger.warning(f"Validation failed: {errors}")
                        continue
                    db.insert_data_point(dp.to_dict())
                    total_new += 1
                logger.info(f"  SEC: {symbol} → {len(raw_items)} filings")

        elif source == "google_news":
            from collectors.google_news import GoogleNewsCollector
            collector = GoogleNewsCollector(proxy=PROXY)
            for symbol in symbols:
                raw_items = collector.collect(symbol, count=20)
                total_fetched += len(raw_items)
                raw_items = batch_score_sentiment(raw_items)
                for item in raw_items:
                    dp = normalize_news(item)
                    errors = validate_data_point(dp)
                    if errors:
                        logger.warning(f"Validation failed: {errors}")
                        continue
                    db.insert_data_point(dp.to_dict())
                    total_new += 1
                logger.info(f"  Google News: {symbol} → {len(raw_items)} articles")

        elif source == "longbridge":
            from collectors.longbridge import LongbridgeCollector
            collector = LongbridgeCollector()
            dt = data_type or "news"
            for symbol in symbols:
                raw_items = collector.collect(symbol, data_type=dt, count=5)
                total_fetched += len(raw_items)
                if dt == "capital":
                    for item in raw_items:
                        dp = normalize_capital_flow(item)
                        db.insert_data_point(dp.to_dict())
                        total_new += 1
                else:
                    raw_items = batch_score_sentiment(raw_items)
                    for item in raw_items:
                        dp = normalize_news(item)
                        db.insert_data_point(dp.to_dict())
                        total_new += 1
                logger.info(f"  Longbridge ({dt}): {symbol} → {len(raw_items)} items")

        elif source == "reddit":
            from collectors.reddit import RedditCollector
            collector = RedditCollector(proxy=PROXY)
            for symbol in symbols:
                raw_items = collector.collect(symbol, count=10)
                total_fetched += len(raw_items)
                raw_items = batch_score_sentiment(raw_items)
                for item in raw_items:
                    dp = normalize_reddit_post(item)
                    errors = validate_data_point(dp)
                    if errors:
                        logger.warning(f"Validation failed: {errors}")
                        continue
                    db.insert_data_point(dp.to_dict())
                    total_new += 1
                logger.info(f"  Reddit: {symbol} → {len(raw_items)} posts")

        else:
            logger.error(f"Unknown source: {source}")

        duration_ms = int((time.time() - start_time) * 1000)
        db.log_collection(
            source=source, fetched=total_fetched, new=total_new,
            duration_ms=duration_ms, status="success"
        )

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        db.log_collection(
            source=source, fetched=0, new=0,
            duration_ms=duration_ms, status="error", error=str(e)
        )
        logger.error(f"Collection failed for {source}: {e}")

    finally:
        db.close()

    return total_new, total_fetched


def collect_longbridge_price(symbols: list):
    """
    Fix #5: Dedicated price collection function (staggered from other tasks).
    Only collects real-time quotes and historical kline data.
    """
    db = Database()
    try:
        from collectors.longbridge import LongbridgeCollector
        collector = LongbridgeCollector()
        total = 0
        for symbol in symbols:
            # Real-time quote
            try:
                quote = collector.get_realtime_quote(symbol)
                if quote and quote.get("last_done"):
                    price = quote["last_done"]
                    db.insert_price(symbol, price, datetime.now().strftime("%Y-%m-%d"))
                    total += 1
            except Exception as e:
                logger.warning(f"  Realtime quote failed for {symbol}: {e}")

            # Historical kline
            try:
                kline_items = collector.get_historical_kline(symbol, days=5)
                for item in kline_items:
                    db.insert_price(symbol, item["close"], item["date"])
                    total += 1
            except Exception as e:
                logger.warning(f"  Historical kline failed for {symbol}: {e}")

        logger.info(f"  Longbridge Price: {total} price records for {len(symbols)} symbols")
        db.log_collection(source="longbridge_price", fetched=total, new=total,
                          status="success", duration_ms=0)
    except Exception as e:
        logger.error(f"Longbridge price collection failed: {e}")
        db.log_collection(source="longbridge_price", fetched=0, new=0,
                          status="error", error=str(e), duration_ms=0)
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Collect data from sources")
    parser.add_argument("--source", choices=["sec", "google_news", "longbridge", "reddit", "longbridge_price", "all"],
                        default="all", help="Data source to collect from")
    parser.add_argument("--symbol", help="Single symbol to collect (overrides watchlist)")
    parser.add_argument("--type", help="Data type for Longbridge (news/capital/filing/topic)")
    parser.add_argument("--symbols", help="Comma-separated symbols")
    parser.add_argument("--quick", action="store_true",
                        help="Quick mode: only longbridge price (for staggered cron)")
    args = parser.parse_args()

    symbols = [args.symbol] if args.symbol else (args.symbols.split(",") if args.symbols else DEFAULT_SYMBOLS)
    symbols = [s.strip().upper() for s in symbols]

    # ─── Fix #5: Quick mode for staggered cron ───
    if args.quick:
        logger.info(f"Quick collection: longbridge_price only, symbols={symbols}")
        collect_longbridge_price(symbols)
        return

    logger.info(f"Starting collection: sources=[{args.source}], symbols={symbols}")
    start = time.time()

    sources = ["sec", "google_news", "longbridge", "reddit"] if args.source == "all" else [args.source]

    total_new = 0
    total_fetched = 0
    for source in sources:
        new, fetched = collect_source(source, symbols, data_type=getattr(args, 'type', None))
        total_new += new
        total_fetched += fetched

    elapsed = time.time() - start
    logger.info(f"✅ Collection complete: {total_new} new / {total_fetched} fetched in {elapsed:.1f}s")

    # Show stats
    db = Database()
    stats = db.get_stats()
    logger.info(f"   Database: {stats['data_points']} data points, {stats['prices']} prices, {stats['factors']} factors")
    db.close()


if __name__ == "__main__":
    main()
