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

import logging
from datetime import datetime, timedelta
from typing import Tuple

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
            return True, reason

    except Exception as e:
        logger.warning(f"Daily loss check failed: {e}")
        return False, "OK"

    return False, "OK"


def _check_consecutive_losses(db) -> Tuple[bool, str]:
    """Check if consecutive losses exceed threshold."""
    try:
        # Get last N trades, check how many consecutive sells lost money
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
            cost_row = db.conn.execute(
                "SELECT cost_price FROM holdings WHERE symbol = ? AND (active = 1 OR active = 0)",
                (symbol,)
            ).fetchone()
            if cost_row and cost_row[0] > 0:
                pnl = (price - cost_row[0]) * qty
                if pnl < 0:
                    consecutive += 1
                    if consecutive >= CIRCUIT_BREAKER["max_consecutive_losses"]:
                        reason = f"Consecutive losses ({consecutive}) exceeds threshold ({CIRCUIT_BREAKER['max_consecutive_losses']})"
                        logger.warning(f"🛑 CIRCUIT BREAKER: {reason}")
                        return True, reason
                else:
                    break  # Reset on a winning trade

    except Exception as e:
        logger.warning(f"Consecutive loss check failed: {e}")
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
