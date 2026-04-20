#!/usr/bin/env python3
"""
US Data Hub — Circuit Breaker (熔断机制)
==========================================
System-level risk controls that can halt all trading.

Rules:
  1. Daily loss > threshold → pause trading
  2. Consecutive losses > N → pause trading
  3. VIX > critical → reduce confidence or pause
  4. Manual kill switch (env var) → stop all trading
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Tuple

from alerts.notifier import alert, AlertLevel

logger = logging.getLogger(__name__)

# Circuit breaker config
CIRCUIT_BREAKER = {
    # Daily loss threshold (absolute $ amount)
    "max_daily_loss_usd": 500,
    # Consecutive losses before pause
    "max_consecutive_losses": 3,
    # VIX critical level (above this, block new buys)
    "vix_critical": 40,
    # VIX elevated level (above this, raise all thresholds by 5%)
    "vix_elevated": 30,
    # ── P1: 滚动回撤限制 ──
    "max_5day_drawdown_usd": 1500,     # 5 日最大回撤
    "max_20day_drawdown_usd": 3000,    # 20 日最大回撤
    "max_total_drawdown_pct": 0.10,    # 总回撤超过 10% 全面停止
}


def check_circuit_breaker(db) -> Tuple[bool, str]:
    """
    Check all circuit breaker conditions.
    Returns (should_halt: bool, reason: str)
    """
    # 0. Manual kill switch
    import os
    if os.getenv("TRADING_KILL_SWITCH", "0") == "1":
        return True, "Kill switch activated (TRADING_KILL_SWITCH=1)"

    # 1. Daily loss check
    halted, reason = _check_daily_loss(db)
    if halted:
        return halted, reason

    # 2. Consecutive losses check
    halted, reason = _check_consecutive_losses(db)
    if halted:
        return halted, reason

    # 3. Rolling drawdown check (P1)
    halted, reason = _check_rolling_drawdown(db)
    if halted:
        return halted, reason

    return False, "OK"


def get_vix_regime(db) -> str:
    """
    Determine market regime based on VIX.
    Returns: 'normal' | 'elevated' | 'critical' | 'unknown'
    """
    try:
        row = db.conn.execute(
            "SELECT close FROM prices WHERE symbol = '^VIX' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if row and row[0] and row[0] > 0:
            vix = float(row[0])
            if vix >= CIRCUIT_BREAKER["vix_critical"]:
                return "critical"
            elif vix >= CIRCUIT_BREAKER["vix_elevated"]:
                return "elevated"
            return "normal"
    except Exception as e:
        logger.warning(f"VIX check failed: {e}")
    return "unknown"


def get_vix_adjustment(db) -> float:
    """
    Get confidence threshold adjustment based on VIX regime.
    Returns: adjustment factor (0.0 = no change, 0.05 = +5pp)
    """
    regime = get_vix_regime(db)
    if regime == "critical":
        return 0.10  # +10pp in panic mode
    elif regime == "elevated":
        return 0.05  # +5pp in elevated volatility
    return 0.0


def _check_daily_loss(db) -> Tuple[bool, str]:
    """Check if daily loss exceeds threshold."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")

        # Get all trades today
        rows = db.conn.execute(
            """SELECT symbol, direction, quantity, price, confidence
               FROM trades WHERE timestamp >= ? AND status = 'executed'""",
            (f"{today} 00:00:00",)
        ).fetchall()

        if not rows:
            return False, "OK"

        # Calculate realized P&L for today's sells
        total_pnl = 0
        for trade in rows:
            symbol, direction, qty, price, conf = trade
            if direction == "sell":
                # Get cost basis from holdings
                cost_row = db.conn.execute(
                    "SELECT cost_price FROM holdings WHERE symbol = ? AND active = 1",
                    (symbol,)
                ).fetchone()
                if cost_row and cost_row[0] > 0:
                    pnl = (price - cost_row[0]) * qty
                    total_pnl += pnl

        # Also check unrealized P&L for active holdings
        # (for a more aggressive circuit breaker)
        holdings_rows = db.conn.execute(
            """SELECT h.symbol, h.quantity, h.cost_price,
                      (SELECT close FROM prices p
                       WHERE p.symbol = h.symbol AND p.close > 0
                       ORDER BY p.date DESC LIMIT 1) as current_price
               FROM holdings h WHERE h.active = 1 AND h.quantity > 0"""
        ).fetchall()

        unrealized_pnl = 0
        for h in holdings_rows:
            sym, qty, cost, current = h
            if current and current > 0:
                unrealized_pnl += (current - cost) * qty

        # Check against threshold (realized + unrealized)
        total_pnl = total_pnl + unrealized_pnl * 0.5  # unrealized at 50% weight

        if total_pnl < -CIRCUIT_BREAKER["max_daily_loss_usd"]:
            reason = f"Daily loss ${total_pnl:.2f} exceeds threshold ${-CIRCUIT_BREAKER['max_daily_loss_usd']}"
            logger.warning(f"🛑 CIRCUIT BREAKER: {reason}")
            alert(AlertLevel.P0, "Circuit Breaker — Daily Loss", reason)
            return True, reason

    except Exception as e:
        logger.warning(f"Daily loss check failed: {e}")
        return False, "OK"

    return False, "OK"


def _check_consecutive_losses(db) -> Tuple[bool, str]:
    """Check if consecutive losses exceed threshold.

    Fix: 不再用 holdings.cost_price（会被减仓摊薄导致失真），
    改为从 sell 发生前的近期价格推断成本基准。
    """
    try:
        max_lookback = CIRCUIT_BREAKER["max_consecutive_losses"] + 5

        rows = db.conn.execute(
            """SELECT symbol, direction, quantity, price, timestamp
               FROM trades WHERE direction = 'sell' AND status = 'executed'
               ORDER BY timestamp DESC LIMIT ?""",
            (max_lookback,)
        ).fetchall()

        if not rows:
            return False, "OK"

        consecutive = 0
        for trade in rows:
            symbol, direction, qty, price, ts = trade

            # 修复：用卖出前 5 日平均价作为成本基准（避免 holdings 摊薄成本失真）
            avg_cost = db.conn.execute(
                """SELECT AVG(close) FROM prices
                   WHERE symbol = ? AND close IS NOT NULL AND close > 0
                   AND date < date(?)
                   ORDER BY date DESC LIMIT 5""",
                (symbol, ts)
            ).fetchone()

            if avg_cost and avg_cost[0] and avg_cost[0] > 0:
                pnl = (price - avg_cost[0]) * qty
                if pnl < 0:
                    consecutive += 1
                    if consecutive >= CIRCUIT_BREAKER["max_consecutive_losses"]:
                        reason = f"Consecutive losses ({consecutive}) exceeds threshold ({CIRCUIT_BREAKER['max_consecutive_losses']})"
                        logger.warning(f"🛑 CIRCUIT BREAKER: {reason}")
                        alert(AlertLevel.P0, "Circuit Breaker — Consecutive Losses", reason)
                        return True, reason
                else:
                    break  # Reset on a winning trade

    except Exception as e:
        logger.warning(f"Consecutive loss check failed: {e}")
        return False, "OK"

    return False, "OK"


def _check_rolling_drawdown(db) -> Tuple[bool, str]:
    """P1: 滚动回撤检查 — 防止连续多天小亏累积成大损失。

    检查 3 个维度：
    1. 5 日滚动回撤（已实现 P&L）
    2. 20 日滚动回撤
    3. 总回撤百分比（相对于持仓成本）
    """
    try:
        # ── 5 日滚动已实现 P&L ──
        five_days_ago = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        rows_5d = db.conn.execute(
            """SELECT symbol, direction, quantity, price, timestamp
               FROM trades WHERE direction = 'sell' AND status = 'executed'
               AND timestamp >= ?""",
            (f"{five_days_ago} 00:00:00",)
        ).fetchall()

        pnl_5d = 0
        for trade in rows_5d:
            symbol, direction, qty, price, ts = trade
            avg_cost = db.conn.execute(
                """SELECT AVG(close) FROM prices
                   WHERE symbol = ? AND close IS NOT NULL AND close > 0
                   AND date < date(?)
                   ORDER BY date DESC LIMIT 5""",
                (symbol, ts)
            ).fetchone()
            if avg_cost and avg_cost[0] and avg_cost[0] > 0:
                pnl_5d += (price - avg_cost[0]) * qty

        if pnl_5d < -CIRCUIT_BREAKER["max_5day_drawdown_usd"]:
            reason = f"5-day rolling loss ${pnl_5d:.2f} exceeds threshold ${-CIRCUIT_BREAKER['max_5day_drawdown_usd']}"
            logger.warning(f"🛑 CIRCUIT BREAKER (5D drawdown): {reason}")
            alert(AlertLevel.P0, "Circuit Breaker — 5D Rolling Loss", reason)
            return True, reason

        # ── 20 日滚动已实现 P&L ──
        twenty_days_ago = (datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
        rows_20d = db.conn.execute(
            """SELECT symbol, direction, quantity, price, timestamp
               FROM trades WHERE direction = 'sell' AND status = 'executed'
               AND timestamp >= ?""",
            (f"{twenty_days_ago} 00:00:00",)
        ).fetchall()

        pnl_20d = 0
        for trade in rows_20d:
            symbol, direction, qty, price, ts = trade
            avg_cost = db.conn.execute(
                """SELECT AVG(close) FROM prices
                   WHERE symbol = ? AND close IS NOT NULL AND close > 0
                   AND date < date(?)
                   ORDER BY date DESC LIMIT 5""",
                (symbol, ts)
            ).fetchone()
            if avg_cost and avg_cost[0] and avg_cost[0] > 0:
                pnl_20d += (price - avg_cost[0]) * qty

        if pnl_20d < -CIRCUIT_BREAKER["max_20day_drawdown_usd"]:
            reason = f"20-day rolling loss ${pnl_20d:.2f} exceeds threshold ${-CIRCUIT_BREAKER['max_20day_drawdown_usd']}"
            logger.warning(f"🛑 CIRCUIT BREAKER (20D drawdown): {reason}")
            alert(AlertLevel.P0, "Circuit Breaker — 20D Rolling Loss", reason)
            return True, reason

        # ── 总回撤百分比（持仓浮亏相对于成本）───
        holdings_rows = db.conn.execute(
            """SELECT h.symbol, h.quantity, h.cost_price,
                      (SELECT close FROM prices p
                       WHERE p.symbol = h.symbol AND p.close > 0
                       ORDER BY p.date DESC LIMIT 1) as current_price
               FROM holdings h WHERE h.active = 1 AND h.quantity > 0"""
        ).fetchall()

        total_cost = 0
        total_current = 0
        for h in holdings_rows:
            sym, qty, cost, current = h
            if current and current > 0:
                total_cost += cost * qty
                total_current += current * qty

        if total_cost > 0:
            total_drawdown_pct = (total_current - total_cost) / total_cost
            if total_drawdown_pct < -CIRCUIT_BREAKER["max_total_drawdown_pct"]:
                reason = (f"Total portfolio drawdown {total_drawdown_pct:+.1%} "
                          f"exceeds threshold {-CIRCUIT_BREAKER['max_total_drawdown_pct']:.0%}")
                logger.warning(f"🛑 CIRCUIT BREAKER (total drawdown): {reason}")
                alert(AlertLevel.P0, "Circuit Breaker — Total Drawdown", reason)
                return True, reason

    except Exception as e:
        logger.warning(f"Rolling drawdown check failed: {e}")
        return False, "OK"

    return False, "OK"


def get_circuit_breaker_status(db) -> dict:
    """Get current circuit breaker status for reporting."""
    halted, reason = check_circuit_breaker(db)
    vix_regime = get_vix_regime(db)
    vix_adj = get_vix_adjustment(db)

    # Count today's trades
    today = datetime.now().strftime("%Y-%m-%d")
    row = db.conn.execute(
        "SELECT COUNT(*) FROM trades WHERE timestamp >= ?",
        (f"{today} 00:00:00",)
    ).fetchone()
    today_trades = row[0] if row else 0

    return {
        "halted": halted,
        "reason": reason,
        "vix_regime": vix_regime,
        "vix_adjustment": vix_adj,
        "today_trades": today_trades,
        "kill_switch_active": __import__("os").getenv("TRADING_KILL_SWITCH", "0") == "1",
    }
