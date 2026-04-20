#!/usr/bin/env python3
"""
US Data Hub — Order Monitor Daemon
Continuously runs, checking pending orders every 30 minutes.
Fixes the issue where auto_execute.py --mode order-monitor exits immediately.
"""

import sys
import os
import time
import logging
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()

from dayup_logger import setup_root_logger
setup_root_logger(level=logging.INFO)
logger = logging.getLogger("order_monitor_daemon")

from storage import Database
from monitoring.order_monitor import OrderMonitor
from executors.longbridge import LongbridgeExecutor

INTERVAL = 1800  # 30 minutes


def check_once():
    """Run one order monitoring cycle."""
    try:
        db = Database()
        db.init_schema()
        executor = LongbridgeExecutor()
        monitor = OrderMonitor(db, executor)
        result = monitor.run_full_check()
        checked = result.get('checked', 0) if result else 0
        logger.info(f"Order monitor: checked {checked} orders")
        return True
    except Exception as e:
        logger.error(f"Order monitor failed: {e}")
        return False


def main():
    logger.info(f"Order Monitor Daemon started (interval={INTERVAL}s)")

    # Run immediately on start
    check_once()

    while True:
        try:
            time.sleep(INTERVAL)
            check_once()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
