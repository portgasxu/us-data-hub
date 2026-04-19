#!/usr/bin/env python3
"""
US Data Hub — Executor CLI
Wrapper for Longbridge trade execution.

Usage:
    python scripts/executor.py --action positions
    python scripts/executor.py --action buy --symbol AAPL.US --qty 10 --price 200
    python scripts/executor.py --action sell --symbol AAPL.US --qty 10 --price 200
    python scripts/executor.py --action quote --symbol AAPL.US
    python scripts/executor.py --action cancel --order-id 1234567890
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from executors.longbridge import main

if __name__ == "__main__":
    main()
