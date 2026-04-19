"""
US Data Hub — DayUp Archiver
每日运营数据自动归档到 dayup/ 目录。

归档时机：
  每次交易完成后 → trades/ + decisions/
  每轮 pipeline 结束后 → positions/ + performance/ + reviews/
  每日收盘后 → market/ + events/ + strategy/ + system/
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DAYUP_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dayup"
)


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _write(dir_name: str, filename: str, data: dict, append: bool = False):
    """Write data to dayup subdirectory."""
    path = os.path.join(DAYUP_ROOT, dir_name, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "a" if append else "w"
    with open(path, mode, encoding="utf-8") as f:
        if append:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
        else:
            json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"[DayUp] Archived → {dir_name}/{filename}")


# ── 交易归档 ──

def archive_trade(trade: dict):
    """单笔交易完成 → trades/"""
    symbol = trade.get("symbol", "UNKNOWN")
    ts = _today()
    _write("trades", f"{ts}.jsonl", {
        "time": _now(),
        "symbol": symbol,
        "action": trade.get("action"),
        "quantity": trade.get("quantity"),
        "price": trade.get("price"),
        "signal": trade.get("signal_source"),
        "confidence": trade.get("confidence"),
    }, append=True)


def archive_decision(symbol: str, decision: dict):
    """TradingAgents 决策 → decisions/"""
    ts = _today()
    _write("decisions", f"{ts}.jsonl", {
        "time": _now(),
        "symbol": symbol,
        "rating": decision.get("rating"),
        "reason": str(decision.get("reason", ""))[:200],
    }, append=True)


# ── Pipeline 归档 ──

def archive_pipeline(collected: dict, factors: dict):
    """Pipeline 完成 → positions/ + performance/"""
    ts = _today()

    # Positions snapshot (from holdings table)
    try:
        from storage import Database
        db = Database()
        holdings = db.conn.execute(
            "SELECT symbol, quantity, cost_price FROM holdings WHERE quantity > 0"
        ).fetchall()

        positions = []
        total_cost = 0
        for symbol, qty, cost in holdings:
            total_cost += qty * cost
            positions.append({
                "symbol": symbol,
                "quantity": qty,
                "cost_price": cost,
            })

        _write("positions", f"{ts}.jsonl", {
            "time": _now(),
            "positions": positions,
            "total_cost": total_cost,
        }, append=True)

        db.close()
    except Exception as e:
        logger.warning(f"[DayUp] Positions archive failed: {e}")

    # Performance summary
    _write("performance", f"{ts}.jsonl", {
        "time": _now(),
        "collected": collected,
        "factors_count": len(factors) if isinstance(factors, (list, dict)) else 0,
    }, append=True)


# ── 风控归档 ──

def archive_risk(alert: dict):
    """风控拦截/告警 → risk/"""
    ts = _today()
    _write("risk", f"{ts}.jsonl", {
        "time": _now(),
        "type": alert.get("type"),
        "symbol": alert.get("symbol"),
        "message": str(alert.get("message", ""))[:200],
    }, append=True)


# ── 系统归档 ──

def archive_system(event: str, detail: dict = None):
    """系统事件 → system/"""
    ts = _today()
    _write("system", f"{ts}.jsonl", {
        "time": _now(),
        "event": event,
        "detail": detail or {},
    }, append=True)


# ── 每日完整归档 ──

def archive_daily_summary():
    """
    每日收盘后完整归档。
    汇总当天所有 trades/decisions/positions/performance。
    """
    today = _today()
    summary = {
        "date": today,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "trades": _read_day_file("trades", today),
        "decisions": _read_day_file("decisions", today),
        "positions": _read_day_file("positions", today),
        "risk_alerts": _read_day_file("risk", today),
        "system_events": _read_day_file("system", today),
    }

    _write("performance", f"{today}-summary.json", summary)
    logger.info(f"[DayUp] Daily summary archived for {today}")


def _read_day_file(dir_name: str, date: str) -> list:
    """Read all entries from today's .jsonl file."""
    path = os.path.join(DAYUP_ROOT, dir_name, f"{date}.jsonl")
    if not os.path.exists(path):
        return []
    entries = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return entries
