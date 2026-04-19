#!/usr/bin/env python3
"""
US Data Hub — Auto Trade Execution Engine (v3.1)
Bridges holding_monitor alerts → trade execution via Longbridge.

Risk Rules:
  - Single stock position cap: 35% (existing positions can only reduce)
  - Daily trade limit: 10 trades
  - Max single order value: $2,000 (sell: $5,000 max per trade)
  - Stop loss: -10%
  - Take profit: +30%
  - Confidence threshold: 0.7 for sell, 0.8 for buy

Circuit Breaker (NEW v3.1):
  - Daily loss > $500 → halt all trading
  - Consecutive losses > 3 → halt
  - VIX > 40 → block new buys
  - TRADING_KILL_SWITCH=1 → emergency stop

Usage:
    python3 scripts/auto_execute.py                  # Execute all pending alerts
    python3 scripts/auto_execute.py --symbol NVDA    # Execute for specific symbol
    python3 scripts/auto_execute.py --dry-run        # Simulate without placing orders
    python3 scripts/auto_execute.py --show-cb-status # Show circuit breaker status
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from storage import Database
from executors.longbridge import LongbridgeExecutor
from executors.auto_trade import execute_trade
from analysis.circuit_breaker import check_circuit_breaker, get_vix_adjustment, get_vix_regime
from analysis.session_strategy import get_market_session, should_execute_trade, print_session_status
from dayup_logger import setup_root_logger, log_risk

setup_root_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

# Risk rules — module level (needed by _execute_trades)
RISK_RULES = {
    "max_daily_loss_pct": 0.05,
    "max_concurrent_positions": 10,
    "max_single_position_pct": 0.20,
    "max_daily_trades": 20,
}

# ═══════════════════════════════════════════════════════
# v6.0: Helper functions for Signal/TradeSignal compatibility
# ═══════════════════════════════════════════════════════

def _get_direction(signal):
    """Get signal direction as string (handles both str and enum)."""
    return signal.direction.value if hasattr(signal.direction, 'value') else signal.direction

def _get_source(signal):
    """Get signal source as string (handles both str and enum)."""
    return signal.source.value if hasattr(signal.source, 'value') else signal.source

def _update_signal_log(db, signal_id: str, action: str, reason: str = ""):
    """Update signal_log with the action taken for a signal."""
    try:
        db.conn.execute(
            "UPDATE signal_log SET action_taken = ?, rejection_reason = ? WHERE signal_id = ?",
            (action, reason, signal_id)
        )
        db.conn.commit()
    except Exception:
        pass


# ─── Risk Control Config ───
RISK_RULES = {
    "max_position_pct": 0.35,        # 35% max per stock
    "daily_trade_limit": 10,          # max 10 trades/day
    "max_order_value": 2000,          # max $2000 per order (buy)
    "max_sell_order_value": 5000,     # max $5000 per sell order (panic sell guard)
    "min_confidence_sell": 0.70,      # min 70% confidence to sell
    "min_confidence_buy": 0.80,       # min 80% confidence to buy
    "stop_loss_pct": -0.10,           # 10% stop loss
    "take_profit_pct": 0.30,          # 30% take profit
    "max_sell_pct_per_trade": 0.50,   # max 50% of position per sell (except stop_loss/take_profit)
    "min_position_pct": 0.10,         # keep at least 10% of position on partial sells
}


def count_daily_trades(db: Database) -> int:
    """Count trades executed today."""
    today = datetime.now().strftime("%Y-%m-%d")
    row = db.conn.execute(
        "SELECT COUNT(*) FROM trades WHERE timestamp >= ?",
        (f"{today} 00:00:00",)
    ).fetchone()
    return row[0] if row else 0


def get_portfolio_value(db: Database) -> float:
    """Get total portfolio value."""
    row = db.conn.execute(
        """SELECT COALESCE(SUM(quantity * (
            SELECT close FROM prices p
            WHERE p.symbol = h.symbol AND p.close IS NOT NULL AND p.close > 0
            ORDER BY p.date DESC LIMIT 1
        )), 0) AS total_value
        FROM holdings h WHERE h.active = 1 AND h.quantity > 0"""
    ).fetchone()
    return row[0] if row and row[0] > 0 else 0


def get_position_pct(db: Database, symbol: str) -> float:
    """Get current position percentage of portfolio."""
    total = get_portfolio_value(db)
    if total <= 0:
        return 0

    row = db.conn.execute(
        """SELECT h.quantity * (
            SELECT close FROM prices p
            WHERE p.symbol = h.symbol AND p.close IS NOT NULL AND p.close > 0
            ORDER BY p.date DESC LIMIT 1
        ) AS value
        FROM holdings h WHERE h.symbol = ? AND h.active = 1""",
        (symbol,)
    ).fetchone()
    return (row[0] / total) if row and row[0] > 0 else 0


def _record_blocked_signal_cooldown(db: Database, symbol: str, direction: str, source: str, reason: str, cooldown_hours: int = 4):
    """Record a blocked signal so we don't keep hitting the same blocked trade every cycle."""
    from datetime import timedelta
    try:
        now = datetime.now()
        cooldown_until = now + timedelta(hours=cooldown_hours)
        db.conn.execute(
            """INSERT INTO signal_cooldowns (symbol, direction, source, blocked_at, cooldown_until, reason)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (symbol, direction, source, now.strftime("%Y-%m-%d %H:%M:%S"),
             cooldown_until.strftime("%Y-%m-%d %H:%M:%S"), reason)
        )
        db.conn.commit()
        logger.info(f"[{symbol}] Cooldown recorded for {direction} from {source} until {cooldown_until.strftime('%H:%M')}")
    except Exception as e:
        logger.debug(f"Failed to record cooldown: {e}")


def _is_signal_in_cooldown(db: Database, symbol: str, direction: str, source: str) -> bool:
    """Check if a signal is currently in cooldown period."""
    try:
        row = db.conn.execute(
            """SELECT COUNT(*) FROM signal_cooldowns
               WHERE symbol = ? AND direction = ? AND source = ?
               AND cooldown_until > datetime('now')""",
            (symbol, direction, source)
        ).fetchone()
        return row[0] > 0 if row else False
    except Exception:
        return False


def _cleanup_expired_cooldowns(db: Database):
    """Remove expired cooldown records."""
    try:
        db.conn.execute("DELETE FROM signal_cooldowns WHERE cooldown_until <= datetime('now')")
        db.conn.commit()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════
# P0: 订单冷却机制 — 防止重复提交订单
# ═══════════════════════════════════════════════════════

def _check_order_cooldown(db: Database, symbol: str, direction: str, minutes: int = 10) -> bool:
    """检查是否在订单冷却期内（同一标的同方向 N 分钟内不允许重复下单）"""
    try:
        row = db.conn.execute(
            "SELECT cooldown_until FROM signal_cooldowns "
            "WHERE symbol = ? AND direction = ? AND source = 'order_lock' "
            "AND cooldown_until > datetime('now')",
            (symbol, direction)
        ).fetchone()
        if row:
            logger.warning(f"⏳ ORDER COOLDOWN: {symbol} {direction} until {row[0]}")
            return True
    except Exception as e:
        logger.error(f"Order cooldown check failed: {e}")
    return False


def _lock_order(db: Database, symbol: str, direction: str, minutes: int = 10):
    """下单后立即锁定，防止重复提交"""
    try:
        db.conn.execute(
            "INSERT INTO signal_cooldowns (symbol, direction, source, cooldown_until, reason) "
            "VALUES (?, ?, 'order_lock', datetime('now', ?), 'order_lock')",
            (symbol, direction, f'+{minutes} minutes')
        )
        db.conn.commit()
        logger.info(f"🔒 Order locked: {symbol} {direction} for {minutes}min")
    except Exception as e:
        logger.error(f"Failed to lock order: {e}")


def _ensure_vix_in_market_indicators(db: Database):
    """Ensure VIX data exists in market_indicators table, falling back to prices."""
    try:
        vix_row = db.conn.execute(
            "SELECT indicator_value FROM market_indicators WHERE indicator_name = 'vix' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if vix_row:
            return  # Already exists
        # Fallback: get from prices table
        price_row = db.conn.execute(
            "SELECT date, close FROM prices WHERE symbol = '^VIX' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if price_row:
            db.conn.execute(
                "INSERT OR REPLACE INTO market_indicators (date, indicator_name, indicator_value) VALUES (?, 'vix', ?)",
                (price_row[0], price_row[1])
            )
            db.conn.commit()
            logger.info(f"Auto-populated VIX={price_row[1]} from prices table")
    except Exception as e:
        logger.debug(f"VIX auto-population skipped: {e}")


def _analyze_dynamic_threshold(db: Database, signal) -> dict:
    """Compute dynamic threshold using rule-based logic + LLM Router.

    P0 改造:
    - 集成 LLM Router，自动路由到 CodingPlan 端点
    - 规则版作为 fallback，LLM 失败时自动降级
    """
    from analysis.dynamic_threshold import DEFAULT_THRESHOLDS, compute_threshold_rule_based

    # Ensure VIX data is available in market_indicators
    _ensure_vix_in_market_indicators(db)

    try:
        # Gather market context
        vix_row = db.conn.execute(
            "SELECT indicator_value FROM market_indicators WHERE indicator_name = 'vix' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        vix_level = str(vix_row[0]) if vix_row else "N/A"

        today = datetime.now().strftime("%Y-%m-%d")
        daily_count = db.conn.execute(
            "SELECT COUNT(*) FROM trades WHERE timestamp >= ?",
            (f"{today} 00:00:00",)
        ).fetchone()
        daily_trade_count = daily_count[0] if daily_count else 0

        market_ctx = {
            "vix_level": vix_level,
            "daily_trade_count": daily_trade_count,
        }

        # Gather stock context
        holding = db.conn.execute(
            "SELECT quantity, cost_price FROM holdings WHERE symbol = ? AND active = 1",
            (signal.symbol,)
        ).fetchone()
        qty = holding[0] if holding else 0
        cost = holding[1] if holding else 0
        total_value = get_portfolio_value(db)
        pos_pct = (qty * cost / total_value * 100) if total_value > 0 and cost > 0 else 0

        prices = db.conn.execute(
            "SELECT close FROM prices WHERE symbol = ? AND close IS NOT NULL ORDER BY date DESC LIMIT 6",
            (signal.symbol,)
        ).fetchall()
        closes = [r[0] for r in prices[::-1]]
        momentum_5d = "N/A"
        if len(closes) >= 6:
            mom = (closes[-1] - closes[0]) / closes[0] * 100
            momentum_5d = f"+{mom:.1f}%" if mom > 0 else f"{mom:.1f}%"

        sent_row = db.conn.execute(
            "SELECT sentiment_score FROM sentiment_scores WHERE symbol = ? AND sentiment_type = 'combined' ORDER BY date DESC LIMIT 1",
            (signal.symbol,)
        ).fetchone()
        if sent_row and sent_row[0] is not None:
            s = sent_row[0]
            recent_sentiment = f"{s:.2f} {'bullish' if s > 0.1 else 'bearish' if s < -0.1 else 'neutral'}"
        else:
            recent_sentiment = "N/A"

        stock_ctx = {
            "position_pct": f"{pos_pct:.1f}%",
            "momentum_5d": momentum_5d,
            "recent_sentiment": recent_sentiment,
        }

        signal_dict = {
            "source": _get_source(signal),
            "direction": signal.direction,
            "confidence": signal.confidence,
        }

        # P0: 先使用规则版计算
        result = compute_threshold_rule_based(signal.direction, market_ctx, stock_ctx, signal_dict)

        # P0: 通过 LLM Router 调用 CodingPlan 进行动态调整
        try:
            from analysis.llm_router import LLMRouter
            router = LLMRouter()
            prompt = (
                f"根据以下信息调整交易信号阈值：\n"
                f"信号: {signal_dict}\n"
                f"市场环境: {market_ctx}\n"
                f"股票信息: {stock_ctx}\n"
                f"当前规则阈值: {result['threshold']:.0%}\n\n"
                f"请给出调整后的阈值（0-1之间）和调整理由，格式: {{'threshold': 0.xx, 'rationale': '...'}}"
            )
            llm_result = router.invoke("dynamic_threshold", [
                {"role": "user", "content": prompt}
            ])
            if llm_result.get("success"):
                import re
                match = re.search(r'\{[^}]*"threshold"\s*:\s*([0-9.]+)[^}]*\}', llm_result["content"])
                if match:
                    llm_threshold = float(match.group(1))
                    if 0.3 <= llm_threshold <= 0.95:
                        result["threshold"] = llm_threshold
                        result["original_threshold"] = result.get("threshold", llm_threshold)
                        result["rationale"] = llm_result["content"][:200]
                        result["llm_adjusted"] = True
                        logger.info(f"[{signal.symbol}] LLM 调整阈值: {llm_threshold:.0%}")
        except Exception as e:
            logger.warning(f"[{signal.symbol}] LLM 动态阈值调用失败，使用规则版: {e}")
            # 规则版已计算，直接使用

        logger.info(f"[{signal.symbol}] Dynamic threshold: {result['threshold']:.0%} "
                    f"(default {DEFAULT_THRESHOLDS.get(signal.direction, 0.75):.0%}), "
                    f"passes={result['passes']}, llm={'yes' if result.get('llm_adjusted') else 'no'}")
        return result

    except Exception as e:
        logger.warning(f"[{signal.symbol}] Dynamic threshold failed: {e} — using fixed threshold")
        default = DEFAULT_THRESHOLDS.get(signal.direction, 0.75)
        return {
            "threshold": default,
            "adjusted_confidence": signal.confidence,
            "passes": signal.confidence >= default,
            "rationale": f"Fallback to fixed threshold {default:.0%}",
        }


def check_risk_rules(db: Database, action: str, symbol: str, quantity: int,
                     price: float, confidence: float, dynamic_threshold: float = None,
                     alert_type: str = None) -> tuple:
    """
    Check if a trade passes all risk controls.

    Fix #1: Circuit breaker + VIX regime check at the start.
    Fix #4: Sell-side risk controls (max sell %, min position retention, max sell value).
    Fix #9: Kill switch support (TRADING_KILL_SWITCH env var).
    Returns (passed: bool, reason: str)
    """
    # ─── Fix #9: Kill Switch ───
    if os.environ.get("TRADING_KILL_SWITCH") == "1":
        return False, "KILL SWITCH ACTIVE — all trading halted"

    # ─── Fix #1: Circuit Breaker ───
    cb_halted, cb_reason = check_circuit_breaker(db)
    if cb_halted:
        return False, f"CIRCUIT BREAKER: {cb_reason}"

    # ─── Fix #1: VIX regime — block new buys in panic mode ───
    vix_regime = get_vix_regime(db)
    if action == "buy" and vix_regime == "critical":
        return False, f"VIX critical regime — new buys blocked (VIX > {RISK_RULES.get('vix_critical', 40)})"

    # ─── 1. Daily trade limit ───
    daily_count = count_daily_trades(db)
    if daily_count >= RISK_RULES["daily_trade_limit"]:
        return False, f"Daily trade limit reached ({daily_count}/{RISK_RULES['daily_trade_limit']})"

    # ─── 2. Confidence threshold (with VIX adjustment) ───
    vix_adj = get_vix_adjustment(db)
    if action in ("sell", "reduce"):
        threshold = dynamic_threshold if dynamic_threshold is not None else RISK_RULES["min_confidence_sell"]
        threshold = min(0.95, threshold + vix_adj)
        if confidence < threshold:
            return False, f"Confidence {confidence:.2f} < threshold {threshold:.2f}"
    elif action == "buy":
        threshold = dynamic_threshold if dynamic_threshold is not None else RISK_RULES["min_confidence_buy"]
        threshold = min(0.95, threshold + vix_adj)
        if confidence < threshold:
            return False, f"Confidence {confidence:.2f} < threshold {threshold:.2f}"

    # ─── 3. Max order value (buy only) ───
    if action == "buy":
        order_value = quantity * price
        if order_value > RISK_RULES["max_order_value"]:
            return False, f"Order value ${order_value:.2f} > max ${RISK_RULES['max_order_value']}"

    # ─── 4. Position concentration (for buys only) ───
    if action == "buy":
        order_value = quantity * price
        current_pct = get_position_pct(db, symbol)
        total_value = get_portfolio_value(db)
        new_pct = (current_pct * total_value + order_value) / (total_value + order_value) if total_value > 0 else 0
        if new_pct > RISK_RULES["max_position_pct"]:
            return False, f"New position {new_pct:.1%} > max {RISK_RULES['max_position_pct']:.0%}"

    # ─── Fix #4: Sell-side risk controls ───
    if action == "sell" and alert_type not in ("llm_stop_loss", "llm_take_profit"):
        holding_row = db.conn.execute(
            "SELECT quantity FROM holdings WHERE symbol = ? AND active = 1", (symbol,)
        ).fetchone()
        if holding_row:
            total_qty = holding_row[0]
            if total_qty > 0:
                # Check if position exceeds the max_position_pct cap (35%) — allow aggressive reduction
                current_pct = get_position_pct(db, symbol)
                cap = RISK_RULES["max_position_pct"]  # 35%

                if current_pct > cap:
                    # Above cap: allow selling enough to get below the cap
                    total_value = get_portfolio_value(db)
                    target_value = total_value * cap
                    if price > 0:
                        target_qty = int(target_value / price)
                        min_sell_to_cap = total_qty - target_qty
                        # Ensure we don't sell the entire position
                        if quantity >= total_qty:
                            return False, (
                                f"Sell {quantity} would empty position — keep at least 1 share"
                            )
                        # If the requested sell would get us below cap, allow it
                        if total_qty - quantity <= target_qty:
                            logger.info(f"[{symbol}] Concentration ({current_pct:.0%} > {cap:.0%}): "
                                        f"allowing sell of {quantity} to reduce toward cap")
                        elif quantity < min_sell_to_cap:
                            # Requested sell isn't enough to reach cap — auto-adjust
                            logger.info(f"[{symbol}] Concentration ({current_pct:.0%} > {cap:.0%}): "
                                        f"auto-adjusted sell from {quantity} → {min_sell_to_cap} "
                                        f"to reach {cap:.0%} cap")
                    # Pass through — allow the sell
                else:
                    # Normal: max 50% of position per trade
                    max_sell_qty = int(total_qty * RISK_RULES["max_sell_pct_per_trade"])
                    if quantity > max_sell_qty:
                        return False, (
                            f"Sell {quantity} > max {max_sell_qty} "
                            f"({RISK_RULES['max_sell_pct_per_trade']:.0%} of {total_qty} shares, non-emergency)"
                        )
                    # Keep at least 10% of position
                    min_keep = max(1, int(total_qty * RISK_RULES["min_position_pct"]))
                    if total_qty - quantity < min_keep:
                        return False, (
                            f"Would leave {total_qty - quantity} shares < min {min_keep} "
                            f"({RISK_RULES['min_position_pct']:.0%} of {total_qty})"
                        )

    # ─── Fix #4: Max sell value (panic sell guard) ───
    if action == "sell":
        sell_value = quantity * price
        max_sell = RISK_RULES.get("max_sell_order_value", 5000)
        if sell_value > max_sell:
            return False, f"Sell value ${sell_value:.2f} > max ${max_sell}"

    return True, "OK"


def alert_to_trade(alert: dict, db: Database) -> dict:
    """
    Convert a monitoring alert into a trade decision.
    Returns trade dict ready for execution.
    """
    symbol = alert.get("symbol", "")
    alert_type = alert.get("type", "")
    message = alert.get("message", "")
    confidence = alert.get("confidence", 0.5)

    # Get current holding info
    row = db.conn.execute(
        "SELECT quantity, cost_price, company_name FROM holdings WHERE symbol = ? AND active = 1",
        (symbol,)
    ).fetchone()

    if not row:
        logger.warning(f"No holding found for {symbol}, skipping alert")
        return {"action": "hold", "symbol": symbol, "quantity": 0, "reason": "no_holding"}

    quantity, cost_price, company_name = row["quantity"], row["cost_price"], row["company_name"]

    # Get current price
    price_row = db.conn.execute(
        "SELECT close FROM prices WHERE symbol = ? AND close IS NOT NULL AND close > 0 ORDER BY date DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    current_price = price_row[0] if price_row else cost_price

    if alert_type in ("llm_stop_loss", "llm_take_profit"):
        # Full position exit
        return {
            "action": "sell",
            "symbol": symbol,
            "quantity": quantity,
            "confidence": confidence,
            "reason": message,
            "current_price": current_price,
            "cost_price": cost_price,
        }

    elif alert_type == "llm_reduce":
        # Partial sell — reduce to 35% of portfolio
        total_value = get_portfolio_value(db)
        target_value = total_value * RISK_RULES["max_position_pct"]
        if current_price > 0:
            target_quantity = int(target_value / current_price)
        else:
            target_quantity = 0
        sell_quantity = max(1, quantity - target_quantity)

        return {
            "action": "sell",
            "symbol": symbol,
            "quantity": sell_quantity,
            "confidence": confidence,
            "reason": f"{message} (减仓 {sell_quantity} 股，保留 {target_quantity} 股)",
            "current_price": current_price,
            "cost_price": cost_price,
        }

    elif alert_type == "llm_add":
        # Partial buy — add up to 35% position cap
        total_value = get_portfolio_value(db)
        target_value = total_value * RISK_RULES["max_position_pct"]
        current_value = quantity * current_price
        buy_value = max(0, target_value - current_value)
        if current_price > 0:
            buy_quantity = min(int(buy_value / current_price), int(RISK_RULES["max_order_value"] / current_price))
        else:
            buy_quantity = 0

        return {
            "action": "buy",
            "symbol": symbol,
            "quantity": buy_quantity,
            "confidence": confidence,
            "reason": message,
            "current_price": current_price,
            "cost_price": cost_price,
        }

    else:
        return {"action": "hold", "symbol": symbol, "quantity": 0, "reason": f"unknown alert type: {alert_type}"}


def execute_alerts(specific_symbol: str = None, dry_run: bool = False) -> list:
    """
    Main entry: run holding_monitor, convert alerts to trades, execute.
    Backward-compatible with the old holding_monitor-only approach.
    """
    from monitoring.holding_monitor import HoldingMonitor
    from management.position_manager import PositionManager
    from executors.longbridge import LongbridgeExecutor

    db = Database()
    db.init_schema()
    executor = LongbridgeExecutor()
    pm = PositionManager(db, executor)

    # Sync positions first
    logger.info("Syncing positions from broker...")
    pm.sync_from_broker()

    # Run monitoring to get fresh alerts
    monitor = HoldingMonitor(db, pm)
    alerts = monitor.run_full_check()

    if not alerts:
        logger.info("No alerts from monitoring — no trades to execute")
        db.close()
        return []

    logger.info(f"Received {len(alerts)} alerts from monitoring")

    # Execute each alert
    results = []
    for alert in alerts:
        symbol = alert.get("symbol", "")

        if specific_symbol and symbol != specific_symbol:
            continue

        logger.info(f"Processing alert: {symbol} — {alert.get('type', 'unknown')}")

        # Convert alert to trade decision
        trade = alert_to_trade(alert, db)

        if trade["action"] == "hold":
            logger.info(f"[{symbol}] Trade decision: HOLD — {trade.get('reason', '')}")
            results.append({"symbol": symbol, "status": "hold", "reason": trade.get("reason", "")})
            continue

        # Risk control check
        confidence = trade.get("confidence", 0.5)
        price = trade.get("current_price", 0)
        passed, reason = check_risk_rules(db, trade["action"], symbol, trade["quantity"], price, confidence,
                                           alert_type=alert.get("type"))

        if not passed:
            logger.warning(f"[{symbol}] Risk control BLOCKED: {reason}")
            # Try to adjust quantity for value-limit cases
            if "Order value" in reason and "max" in reason:
                max_qty = int(RISK_RULES["max_order_value"] / price) if price > 0 else 0
                if max_qty > 0:
                    trade["quantity"] = max_qty
                    logger.info(f"[{symbol}] Adjusted quantity to {max_qty} to fit order value limit")
                    order_value = max_qty * price
                    passed = order_value <= RISK_RULES["max_order_value"]
                    if not passed:
                        log_risk(
                            risk_type="风控拦截",
                            trigger=f"{symbol} {trade['action']} 调整后仍超限",
                            current=str(max_qty),
                            threshold=reason,
                            action="跳过"
                        )
                        results.append({"symbol": symbol, "status": "blocked", "reason": reason})
                        continue
                else:
                    log_risk(
                        risk_type="风控拦截",
                        trigger=f"{symbol} {trade['action']} 无法调整数量",
                        current=str(trade["quantity"]),
                        threshold=reason,
                        action="跳过"
                    )
                    results.append({"symbol": symbol, "status": "blocked", "reason": reason})
                    continue
            else:
                log_risk(
                    risk_type="风控拦截",
                    trigger=f"{symbol} {trade['action']} 被风控规则拦截",
                    current=str(trade["quantity"]),
                    threshold=reason,
                    action="跳过"
                )
                results.append({"symbol": symbol, "status": "blocked", "reason": reason})
                continue

        # ─── Fix #9: Dry Run Mode ───
        if dry_run:
            logger.info(f"[{symbol}] DRY RUN — would execute: {trade['action']} {trade['quantity']} shares @ ${price:.2f}")
            results.append({
                "symbol": symbol,
                "status": "dry_run",
                "message": f"[DRY RUN] {trade['action']} {trade['quantity']} @ ${price:.2f}",
            })
            continue

        # Execute the trade
        decision = {
            "action": trade["action"],
            "symbol": symbol,
            "quantity": trade["quantity"],
            "confidence": confidence,
            "reason": trade.get("reason", ""),
        }

        logger.info(f"[{symbol}] Executing: {trade['action']} {trade['quantity']} shares (conf={confidence:.0%})")
        result = execute_trade(decision)

        # ─── Fix #6: Record trade with full tracking fields ───
        try:
            # Get factor scores for this symbol
            factor_scores = {}
            for fname in ("momentum", "value", "quality", "volatility", "rsi"):
                frow = db.conn.execute(
                    """SELECT factor_value FROM factors
                       WHERE symbol = ? AND factor_name = ?
                       ORDER BY date DESC LIMIT 1""",
                    (symbol, fname)
                ).fetchone()
                if frow and frow[0] is not None:
                    factor_scores[fname] = round(float(frow[0]), 4)

            import json
            signal_id = f"auto_{symbol}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            db.conn.execute(
                """INSERT INTO trades (symbol, direction, quantity, price,
                   agent_signal, confidence, timestamp,
                   factor_scores, stop_loss, take_profit)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    symbol,
                    trade["action"].lower(),
                    trade["quantity"],
                    price,
                    trade.get("reason", ""),
                    confidence,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    json.dumps(factor_scores) if factor_scores else None,
                    RISK_RULES["stop_loss_pct"],
                    RISK_RULES["take_profit_pct"],
                )
            )
            db.conn.commit()
        except Exception as e:
            logger.warning(f"Failed to record trade in DB: {e}")

        results.append({
            "symbol": symbol,
            "status": result["status"],
            "message": result.get("message", ""),
            "quote": result.get("quote"),
        })

    db.close()
    return results


def _get_todays_traded_symbols(db: Database) -> set:
    """Get set of symbols already traded today — prevents duplicate same-day trades."""
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        rows = db.conn.execute(
            "SELECT DISTINCT symbol FROM trades WHERE timestamp >= ?",
            (f"{today} 00:00:00",)
        ).fetchall()
        return {row[0] for row in rows}
    except Exception:
        return set()


def execute_signals(specific_symbol: str = None, min_confidence: float = 0.5, dry_run: bool = False) -> list:
    """
    New unified entry: collect signals from ALL sources via SignalHub, execute trades.
    This is the new preferred method for full-loop trading.

    Fix #6: Writes factor_scores, stop_loss, take_profit to trades table.
    Fix #7: Volatility-adaptive position sizing for buys.
    Fix #9: Dry-run mode support.
    Fix #10: Price data freshness check.
    Fix #11: Same-symbol daily trade dedup — prevents repeated buys on same stock.
    """
    from analysis.signal_hub import SignalHub
    from analysis.trace_id import generate_execution_id

    db = Database()
    db.init_schema()

    # P0: 时段策略检查
    session = get_market_session()
    logger.info(f"📊 {print_session_status()}")

    should_trade, trade_reason = should_execute_trade(session)
    if not should_trade and not dry_run:
        logger.warning(f"⛔ 当前时段 {session.session_name} 禁止交易: {trade_reason}")
        if session.deliverable:
            logger.info(f"📋 当前时段交付物: {session.deliverable}")
        db.close()
        return []
    elif should_trade:
        logger.info(f"✅ 交易权限: {trade_reason}, max_trades={session.max_trades}")

    # v6.0: Generate execution_id for this run
    execution_id = generate_execution_id()
    logger.info(f"📋 Execution started: {execution_id}")

    symbols = [specific_symbol] if specific_symbol else None

    # Collect all signals
    hub = SignalHub(db, min_confidence=min_confidence)

    # v6.0: Record execution start
    try:
        db.conn.execute(
            "INSERT INTO execution_log (execution_id, started_at, status) VALUES (?, ?, ?)",
            (execution_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "RUNNING")
        )
        db.conn.commit()
    except Exception as e:
        logger.warning(f"Failed to record execution start: {e}")

    all_signals = hub.collect_all(symbols)
    tradable = hub.get_tradable_signals()

    # v6.0: Record signals to signal_log
    signals_recorded = 0
    for sig in tradable:
        try:
            sig_dir = sig.direction.value if hasattr(sig.direction, 'value') else sig.direction
            sig_src = sig.source.value if hasattr(sig.source, 'value') else sig.source
            db.conn.execute(
                "INSERT INTO signal_log (signal_id, symbol, direction, source, confidence, execution_id, action_taken) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (sig.signal_id, sig.symbol, sig_dir, sig_src, sig.confidence, execution_id, "PENDING")
            )
            signals_recorded += 1
        except Exception:
            pass
    db.conn.commit()

    # Fix #11: Same-day dedup — only blocks BUY signals, not sells/reduces
    # Rationale: If a symbol was already bought, we still need to allow
    # sell signals (e.g. MSFT reduce) to bypass this filter.
    # Otherwise a concentration-reduce signal will always be blocked.
    traded_today = _get_todays_traded_symbols(db)
    if traded_today:
        orig_count = len(tradable)
        tradable = [
            s for s in tradable
            if s.symbol not in traded_today or _get_direction(s) in ("sell",)
        ]
        filtered = orig_count - len(tradable)
        if filtered > 0:
            logger.info(f"Same-day dedup: filtered {filtered} BUY signals for already-traded symbols: {traded_today}")

    if not tradable:
        logger.info("No tradable signals from any source")
        try:
            db.conn.execute(
                "UPDATE execution_log SET ended_at=?, status=?, signals_collected=? WHERE execution_id=?",
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "COMPLETED", signals_recorded, execution_id)
            )
            db.conn.commit()
        except Exception:
            pass
        db.close()
        return []

    logger.info(f"SignalHub: {len(all_signals)} total, {len(tradable)} tradable (execution_id={execution_id})")

    # Execute each signal
    results = []
    for signal in tradable:
        symbol = signal.symbol

        if specific_symbol and symbol != specific_symbol:
            continue

        logger.info(f"Processing signal: {signal}")

        # Get holding info for sell signals
        holding = db.conn.execute(
            "SELECT quantity, cost_price, company_name FROM holdings WHERE symbol = ? AND active = 1",
            (symbol,)
        ).fetchone()

        # ─── Fix #10: Data freshness check ───
        price_row = db.conn.execute(
            "SELECT close, date FROM prices WHERE symbol = ? AND close IS NOT NULL AND close > 0 ORDER BY date DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        if not price_row:
            logger.warning(f"[{symbol}] No price data found — skipping")
            results.append({"symbol": symbol, "status": "skipped", "reason": "no_price_data"})
            _update_signal_log(db, signal.signal_id, "SKIPPED", "no_price_data")
            continue
        current_price = price_row[0]
        price_date = price_row[1]
        try:
            price_dt = datetime.strptime(price_date[:10], "%Y-%m-%d")
            days_old = (datetime.now() - price_dt).days
            if days_old > 3:
                logger.warning(f"[{symbol}] Price data stale ({days_old} days old, {price_date}) — skipping")
                results.append({"symbol": symbol, "status": "skipped", "reason": f"stale_price_{days_old}d"})
                continue
        except Exception:
            pass

        if _get_direction(signal) == "sell":
            if not holding or holding["quantity"] <= 0:
                logger.info(f"[{symbol}] No position to sell (qty={holding['quantity'] if holding else 0}) — skipping")
                results.append({"symbol": symbol, "status": "skipped", "reason": "no_position"})
                continue
            quantity = holding["quantity"]
            # For reduce signals, calculate partial sell
            extra = getattr(signal, 'extra', {}) or {}
            is_reduce = _get_source(signal) == "holding_monitor" and (
                "reduce" in signal.reason.lower()
                or "减仓" in signal.reason
                or extra.get("_is_partial_sell")
            )
            if is_reduce:
                suggested_weight = extra.get("suggested_weight")
                # Use LLM's suggested weight if available, otherwise fall back to max_position_pct
                target_weight = suggested_weight if suggested_weight else RISK_RULES["max_position_pct"]
                total_value = get_portfolio_value(db)
                target_value = total_value * target_weight
                target_qty = int(target_value / current_price) if current_price > 0 else 0
                quantity = max(1, holding["quantity"] - target_qty)
                if suggested_weight:
                    logger.info(f"[{symbol}] LLM reduce: target weight={target_weight:.0%}, "
                                f"current_qty={holding['quantity']}, sell_qty={quantity}")

        elif _get_direction(signal) == "buy":
            # ─── Fix #7: Volatility-adaptive position sizing ───
            base_max_value = RISK_RULES["max_order_value"]
            vol_row = db.conn.execute(
                """SELECT factor_value FROM factors
                   WHERE symbol = ? AND factor_name = 'volatility'
                   ORDER BY date DESC LIMIT 1""",
                (symbol,)
            ).fetchone()
            if vol_row and vol_row[0] is not None:
                vol = float(vol_row[0])
                if vol > 0.03:  # High volatility (>3% daily)
                    base_max_value = RISK_RULES["max_order_value"] * 0.5
                    logger.info(f"[{symbol}] High vol ({vol:.2%}) → max order ${base_max_value:.0f}")
                elif vol > 0.02:  # Medium
                    base_max_value = RISK_RULES["max_order_value"] * 0.75

            max_qty_by_value = int(base_max_value / current_price) if current_price > 0 else 0
            # Also respect position cap
            total_value = get_portfolio_value(db)
            target_value = total_value * RISK_RULES["max_position_pct"]
            existing_value = (holding["quantity"] * current_price) if holding else 0
            remaining_value = max(0, target_value - existing_value)
            max_qty_by_position = int(remaining_value / current_price) if current_price > 0 else 0

            quantity = min(max_qty_by_value, max_qty_by_position)
            if quantity <= 0:
                logger.info(f"[{symbol}] Buy quantity 0 (position cap or value limit reached) — skipping")
                results.append({"symbol": symbol, "status": "skipped", "reason": "quantity_zero"})
                continue
        else:
            results.append({"symbol": symbol, "status": "hold"})
            _update_signal_log(db, signal.signal_id, "HOLD", "")
            continue

        # Dynamic threshold analysis (LLM-based)
        dynamic = _analyze_dynamic_threshold(db, signal)

        # Risk control check (uses dynamic threshold if available)
        confidence_to_check = dynamic.get("adjusted_confidence", signal.confidence)
        threshold_used = dynamic.get("threshold", None)

        passed, reason = check_risk_rules(
            db, signal.direction, symbol, quantity, current_price,
            confidence_to_check, dynamic_threshold=threshold_used
        )

        if not passed:
            logger.warning(f"[{symbol}] Risk control BLOCKED: {reason}")
            threshold_info = ""
            if dynamic.get("threshold") is not None:
                delta = dynamic["threshold"] - dynamic.get("original_threshold", dynamic["threshold"])
                delta_str = f"+{delta:.0%}" if delta > 0 else f"{delta:.0%}"
                threshold_info = f" (动态阈值 {dynamic['threshold']:.0%} {delta_str}, 原因: {dynamic.get('rationale', '')})"
            log_risk(
                risk_type="风控拦截",
                trigger=f"{symbol} {_get_direction(signal)} 被风控拦截{threshold_info}",
                current=str(quantity),
                threshold=reason,
                action="跳过"
            )

            # Record cooldown so same signal doesn't reappear every 15min cycle
            # Bug fix #3: _record_cooldown now auto-escalates based on recent block count
            hub._record_cooldown(symbol, _get_direction(signal), _get_source(signal), reason)
            _update_signal_log(db, signal.signal_id, "REJECTED", reason)

            results.append({
                "symbol": symbol,
                "status": "blocked",
                "reason": reason,
                "dynamic_threshold": dynamic.get("threshold"),
                "threshold_rationale": dynamic.get("rationale", ""),
            })
            continue

        # Execute
        decision = {
            "action": _get_direction(signal),
            "symbol": symbol,
            "quantity": quantity,
            "confidence": signal.confidence,
            "reason": signal.reason,
        }

        logger.info(f"[{symbol}] Executing: {signal.direction} {quantity} shares (conf={signal.confidence:.0%}, source={signal.source})")

        # ─── Fix #9: Dry Run ───
        if dry_run:
            logger.info(f"[{symbol}] DRY RUN — would execute: {signal.direction} {quantity} @ ${current_price:.2f}")
            results.append({
                "symbol": symbol,
                "status": "dry_run",
                "message": f"[DRY RUN] {signal.direction} {quantity} @ ${current_price:.2f}",
                "source": _get_source(signal),
            })
            continue

        result = execute_trade(decision)

        # P0: 订单冷却锁 — 下单后立即锁定，防止重复提交
        _lock_order(db, symbol, _get_direction(signal), minutes=10)

        # ─── Fix #6: Full trade tracking ───
        try:
            threshold_note = ""
            if dynamic.get("threshold") is not None:
                delta = dynamic["threshold"] - dynamic.get("original_threshold", dynamic["threshold"])
                delta_str = f"+{delta:.0%}" if delta > 0 else f"{delta:.0%}"
                threshold_note = f" | 动态阈值 {dynamic['threshold']:.0%}({delta_str}): {dynamic.get('rationale', '')}"

            # Fix #3: Use quote price if current_price is 0 (fallback chain)
            fill_price = current_price
            if fill_price <= 0 and result.get("quote", {}).get("price", 0) > 0:
                fill_price = result["quote"]["price"]
            if fill_price <= 0:
                fill_price = price_row[0]  # last known price from DB

            import json
            factor_scores = {}
            for fname in ("momentum", "value", "quality", "volatility", "rsi"):
                frow = db.conn.execute(
                    """SELECT factor_value FROM factors
                       WHERE symbol = ? AND factor_name = ?
                       ORDER BY date DESC LIMIT 1""",
                    (symbol, fname)
                ).fetchone()
                if frow and frow[0] is not None:
                    factor_scores[fname] = round(float(frow[0]), 4)

            db.conn.execute(
                """INSERT INTO trades (symbol, direction, quantity, price, agent_signal, confidence, timestamp,
                   factor_scores, stop_loss, take_profit, signal_id, execution_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    symbol,
                    _get_direction(signal).lower(),
                    quantity,
                    fill_price,
                    f"[{_get_source(signal)}] {signal.reason}{threshold_note}",
                    signal.confidence,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    json.dumps(factor_scores) if factor_scores else None,
                    RISK_RULES["stop_loss_pct"],
                    RISK_RULES["take_profit_pct"],
                    signal.signal_id,
                    execution_id,
                )
            )
            db.conn.commit()
            _update_signal_log(db, signal.signal_id, "EXECUTED", "")
        except Exception as e:
            logger.warning(f"Failed to record trade in DB: {e}")
            _update_signal_log(db, signal.signal_id, "ERROR", str(e))

        results.append({
            "symbol": symbol,
            "status": result["status"],
            "message": result.get("message", ""),
            "quote": result.get("quote"),
            "source": _get_source(signal),
            "dynamic_threshold": dynamic.get("threshold"),
            "threshold_delta": dynamic.get("threshold_delta", 0),
            "threshold_rationale": dynamic.get("rationale", ""),
        })

    db.close()
    return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Auto Trade Execution Engine")
    parser.add_argument("--symbol", default="", help="Execute for specific symbol only")
    parser.add_argument("--rules", action="store_true", help="Show current risk rules")
    parser.add_argument("--full-loop", action="store_true",
                        help="Use full signal hub (all sources) instead of holding_monitor only")
    parser.add_argument("--min-confidence", type=float, default=0.5,
                        help="Minimum confidence threshold for full-loop mode")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate trades without actually executing (Kill Switch)")
    parser.add_argument("--show-cb-status", action="store_true",
                        help="Show circuit breaker status and exit")
    # P0: 新增时段策略相关参数
    parser.add_argument("--mode", choices=["full-loop", "screener", "review", "morning-brief", "holding-monitor"],
                        help="运行模式 (替代原有的 --full-loop)")
    parser.add_argument("--show-session", action="store_true",
                        help="显示当前交易时段状态并退出")
    args = parser.parse_args()

    # P0: 显示时段状态
    if args.show_session:
        print(print_session_status())
        return

    if args.rules:
        print("Risk Rules:")
        for k, v in RISK_RULES.items():
            print(f"  {k}: {v}")
        return

    if args.show_cb_status:
        db = Database()
        try:
            from analysis.circuit_breaker import get_circuit_breaker_status
            status = get_circuit_breaker_status(db)
            print(f"\n{'='*60}")
            print(f"Circuit Breaker Status ({datetime.now().strftime('%H:%M:%S')})")
            print(f"{'='*60}")
            print(f"  Halted:          {'YES 🛑' if status['halted'] else 'No ✅'}")
            if status['halted']:
                print(f"  Reason:          {status['reason']}")
            print(f"  VIX Regime:      {status['vix_regime']}")
            print(f"  VIX Adjustment:  +{status['vix_adjustment']:.0%}")
            print(f"  Today's Trades:  {status['today_trades']}")
            print(f"  Kill Switch:     {'ACTIVE 🛑' if status['kill_switch_active'] else 'Off ✅'}")
            print(f"{'='*60}")
        except Exception as e:
            print(f"Error: {e}")
        db.close()
        return

    # P0: 支持 --mode 参数
    use_full_loop = args.full_loop or (args.mode == "full-loop")

    if use_full_loop:
        results = execute_signals(
            specific_symbol=args.symbol if args.symbol else None,
            min_confidence=args.min_confidence,
            dry_run=args.dry_run
        )
    else:
        results = execute_alerts(
            specific_symbol=args.symbol if args.symbol else None,
            dry_run=args.dry_run
        )

    if results:
        print(f"\n{'='*80}")
        mode = "FULL-LOOP + 动态阈值" if use_full_loop else "HOLDING-MONITOR"
        print(f"Auto-Trade Execution Results [{mode}] ({datetime.now().strftime('%H:%M:%S')})")
        print(f"{'='*80}")
        for r in results:
            status_emoji = {"executed": "✅", "hold": "⏸️", "blocked": "🚫", "error": "❌", "skipped": "⏭️"}.get(r["status"], "❓")
            source = f" ({r.get('source', '')})" if r.get("source") else ""
            line = f"  {status_emoji} {r['symbol']}: {r['status']}{source} — {r.get('message', r.get('reason', ''))}"
            print(line)
            if r.get("dynamic_threshold") is not None:
                delta = r.get("threshold_delta", 0)
                delta_str = f"+{delta*100:.0f}pp" if delta > 0 else f"{delta*100:.0f}pp"
                print(f"      🎯 动态阈值 {r['dynamic_threshold']:.0%} ({delta_str}) | {r.get('threshold_rationale', '')}")
        print(f"{'='*80}")
    else:
        print("No trades executed")


if __name__ == "__main__":
    main()
