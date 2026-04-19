"""
US Data Hub — Unified Data Pipeline
Uses collectors/ for data fetching, storage/ for persistence,
processors/ for preprocessing. No duplicate code.
"""

import sys
import os
import time
import json
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

from storage import Database
from collectors.sec import SECCollector
from collectors.google_news import GoogleNewsCollector
from collectors.longbridge import LongbridgeCollector
from collectors.reddit import RedditCollector
from collectors.price import PriceCollector
from normalizers.schemas import (
    normalize_sec_filing, normalize_news,
    normalize_reddit_post, normalize_capital_flow,
)
from processors.sentiment import batch_score_sentiment

logger = logging.getLogger(__name__)
PROXY = config.proxy_url


def collect_all(symbols: list = None) -> dict:
    """Collect from all sources. Returns stats dict."""
    if symbols is None:
        symbols = config.watchlist or ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]

    db = Database()
    db.init_schema()
    stats = {"total_fetched": 0, "total_new": 0}

    collectors = {
        "sec": SECCollector(),
        "google_news": GoogleNewsCollector(proxy=PROXY),
        "longbridge": LongbridgeCollector(),  # Phase 3: 国内节点无需代理
        "reddit": RedditCollector(proxy=PROXY),
        "price": PriceCollector(),
    }

    # Phase 3: 并行采集
    from collectors.parallel_collector import collect_all_parallel
    all_results = collect_all_parallel(collectors, symbols, max_workers_per_source=5)

    for source_name, source_results in all_results.items():
        for symbol, raw_items in source_results.items():
            if not raw_items:
                continue
            stats["total_fetched"] += len(raw_items)

            try:
                if source_name == "price":
                    inserted = 0
                    for item in raw_items:
                        db.insert_price(symbol, item["date"], item)
                        inserted += 1
                    stats["total_new"] += inserted
                    logger.info(f"  Price: {symbol} → {inserted} candles")

                elif source_name == "sec":
                    for item in raw_items:
                        dp = normalize_sec_filing(item)
                        db.insert_data_point(dp.to_dict())
                        stats["total_new"] += 1

                elif source_name in ("google_news", "longbridge"):
                    raw_items = batch_score_sentiment(raw_items)
                    for item in raw_items:
                        dp = normalize_news(item)
                        db.insert_data_point(dp.to_dict())
                        stats["total_new"] += 1

                elif source_name == "reddit":
                    raw_items = batch_score_sentiment(raw_items)
                    for item in raw_items:
                        dp = normalize_reddit_post(item)
                        db.insert_data_point(dp.to_dict())
                        stats["total_new"] += 1

            except Exception as e:
                logger.error(f"Collection failed for {source_name}/{symbol}: {e}")

    db.log_collection(
        source="pipeline", fetched=stats["total_fetched"],
        new=stats["total_new"], status="success"
    )
    db.close()
    return stats


if __name__ == "__main__":
    import argparse
    from dayup_logger import setup_root_logger
    setup_root_logger(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Unified Data Pipeline")
    parser.add_argument("command", nargs="?", default="collect", choices=["collect", "factors"],
                        help="collect=full data collection, factors=calculate factors from prices")
    args = parser.parse_args()

    if args.command == "collect":
        stats = collect_all()
        print(f"Fetched: {stats['total_fetched']}, New: {stats['total_new']}")
    elif args.command == "factors":
        from analysis.factor_from_prices import main as compute_factors
        compute_factors()
        print("Factor computation complete")
