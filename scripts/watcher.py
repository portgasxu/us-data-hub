#!/usr/bin/env python3
"""
US Data Hub — Watcher (event-driven)
Monitors news surges and triggers screener on significant events.

Usage:
    python3 scripts/watcher.py --once        # Run once and exit
    python3 scripts/watcher.py --daemon      # Run as background daemon
    python3 scripts/watcher.py --interval 300  # Check interval in seconds (default 300)
"""

import sys
import os
import time
import signal
import logging
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from dayup_logger import setup_root_logger
setup_root_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
_running = True

def signal_handler(signum, frame):
    global _running
    _running = False
    logger.info("Received shutdown signal, exiting gracefully...")

def run_watcher():
    """
    Run the event-driven watcher: check for news surges and trigger screener.
    v6.0: 通过 Signal Hub 发布事件，不再绕过直接调 Screener。
    """
    from storage import Database
    from config import config
    from analysis.signal_hub import SignalHub, Signal, SignalDirection, SignalSource

    db = Database()
    db.init_schema()

    try:
        symbols = config.watchlist or ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]

        # Check for news volume spikes
        alerts = []
        for symbol in symbols:
            try:
                row = db.conn.execute("""
                    SELECT COUNT(*) FROM data_points
                    WHERE symbol = ? AND source = 'google_news'
                    AND timestamp >= datetime('now', '-3 hours')
                """, (symbol,)).fetchone()

                count = row[0] if row else 0
                if count > 20:
                    alerts.append(f"{symbol}: {count} articles in 3h (baseline ~2)")
                    logger.info(f"🚨 [NewsWatcher] HIGH — News surge: {symbol} ({count} in 3h)")
            except Exception as e:
                logger.warning(f"Failed to check news for {symbol}: {e}")

        # v6.0: 通过 Signal Hub 聚合信号，不再直接调 Screener
        if alerts:
            hub = SignalHub(db, min_confidence=0.3)

            # 1. 发布 watcher 信号
            for alert in alerts:
                symbol = alert.split(":")[0]
                hub.add(Signal(
                    symbol=symbol,
                    direction="buy",
                    confidence=0.7,
                    source="watcher",
                    strength=0.8,
                    reason=f"新闻异动: {alert}",
                ))

            # 2. 通过 Signal Hub 触发 Screener 聚合
            hub._collect_screener()

            # 3. 获取聚合后的信号
            signals = hub.get_tradable_signals()
            logger.info(f"✅ Watcher via SignalHub: {len(signals)} signals from {len(alerts)} alerts")
        else:
            logger.info("No significant news events detected")

    finally:
        db.close()

def run_daemon(interval: int = 300):
    """Run watcher as a daemon with periodic checks."""
    global _running
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info(f"Watcher daemon started (interval={interval}s)")

    while _running:
        try:
            run_watcher()
        except Exception as e:
            logger.error(f"Watcher run failed: {e}")

        # Sleep in small chunks for responsive shutdown
        for _ in range(interval):
            if not _running:
                break
            time.sleep(1)

    logger.info("Watcher daemon stopped")

def main():
    parser = argparse.ArgumentParser(description="US Data Hub Watcher")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--daemon", action="store_true", help="Run as background daemon")
    parser.add_argument("--interval", type=int, default=300, help="Check interval in seconds (default 300)")
    args = parser.parse_args()

    if args.once:
        run_watcher()
    elif args.daemon:
        run_daemon(args.interval)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
