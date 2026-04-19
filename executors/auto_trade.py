"""
US Data Hub — Auto Trade Executor
Reads trading decisions and executes via Longbridge CLI.
DRY_RUN removed — all orders are placed directly.
"""

import json
import logging
import subprocess
from datetime import datetime
from typing import Dict, Optional

from dayup_logger import log_trade, log_decision, log_error

logger = logging.getLogger(__name__)


def _get_quote(symbol: str) -> Optional[Dict]:
    """Get real-time quote before placing order."""
    symbol_us = f"{symbol.upper()}.US"
    try:
        result = subprocess.run(
            ["longbridge", "quote", symbol_us, "--format", "json", "-y"],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            elif isinstance(data, dict):
                return data
    except Exception as e:
        logger.warning(f"Quote fetch failed for {symbol}: {e}")
    return None


def parse_decision(signal: str) -> Dict:
    """
    Parse the trading decision signal into actionable fields.
    Supports JSON and structured text formats.
    """
    decision = {
        "action": "hold",
        "symbol": "",
        "quantity": 0,
        "confidence": 0.5,
        "reason": "",
        "parsed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "target_price": None,
    }

    # Try to extract JSON from the signal
    try:
        start = signal.find("{")
        end = signal.rfind("}") + 1
        if start >= 0 and end > start:
            data = json.loads(signal[start:end])
            decision["action"] = data.get("action", data.get("direction", "hold")).lower()
            decision["symbol"] = data.get("symbol", data.get("company", ""))
            decision["quantity"] = int(data.get("quantity", data.get("position_size", 0)))
            decision["confidence"] = float(data.get("confidence", 0.5))
            decision["reason"] = data.get("reason", data.get("rationale", ""))
            if "suggested_weight" in data:
                decision["target_weight"] = float(data["suggested_weight"])
            return decision
    except (json.JSONDecodeError, ValueError):
        pass

    # Fallback: text parsing
    signal_lower = signal.lower()
    if "buy" in signal_lower or "overweight" in signal_lower:
        decision["action"] = "buy"
    elif "sell" in signal_lower or "underweight" in signal_lower:
        decision["action"] = "sell"
    elif "stop_loss" in signal_lower or "stop loss" in signal_lower:
        decision["action"] = "sell"
    elif "take_profit" in signal_lower or "take profit" in signal_lower:
        decision["action"] = "sell"
    elif "reduce" in signal_lower:
        decision["action"] = "sell"
    elif "hold" in signal_lower:
        decision["action"] = "hold"

    return decision


def execute_trade(decision: Dict, outside_rth: str = "ANY_TIME") -> Dict:
    """
    Execute a trade decision via Longbridge CLI.
    Always executes — no dry_run mode.
    Supports 24-hour trading via --outside-rth flag.

    Args:
        decision: Parsed trade decision with action, symbol, quantity
        outside_rth: 'RTH_ONLY' | 'ANY_TIME' | 'OVERNIGHT' (default: ANY_TIME)

    Returns:
        Execution result dict
    """
    result = {
        "status": "skipped",
        "action": decision["action"],
        "symbol": decision["symbol"],
        "quantity": decision["quantity"],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "message": "",
        "quote": None,
    }

    action = decision["action"]
    symbol = decision["symbol"]
    quantity = decision["quantity"]

    if action == "hold":
        result["status"] = "skipped"
        result["message"] = f"HOLD decision for {symbol}, no action needed"
        logger.info(f"[TRADE] HOLD {symbol} — no action")
        log_decision(
            decision_type="HOLD",
            content=f"保持持仓: {symbol}",
            basis=decision.get("reason", ""),
            result="跳过执行"
        )
        return result

    if quantity <= 0:
        result["status"] = "skipped"
        result["message"] = f"Quantity is {quantity}, skipping"
        logger.warning(f"[TRADE] SKIP {symbol} — quantity {quantity} <= 0")
        log_decision(
            decision_type=action.upper(),
            content=f"{action} {symbol} — 数量为0，跳过",
            basis="",
            result="无效数量"
        )
        return result

    # Fetch real-time quote before order
    quote = _get_quote(symbol)
    estimated_price = None
    if quote:
        result["quote"] = {
            "price": quote.get("last", 0),
            "bid": quote.get("bid", 0),
            "ask": quote.get("ask", 0),
            "high": quote.get("high", 0),
            "low": quote.get("low", 0),
        }
        estimated_price = quote.get("last", 0)
        logger.info(f"[TRADE] {symbol} quote: ${quote.get('last', 'N/A')}")

    # Execute order via Longbridge CLI
    try:
        symbol_us = f"{symbol.upper()}.US"
        if action in ("buy", "sell"):
            cmd = ["longbridge", "order", action, symbol_us, str(quantity), "--order-type", "MO", "-y"]
        else:
            result["status"] = "error"
            result["message"] = f"Unknown action: {action}"
            return result

        logger.info(f"[TRADE] Executing: {' '.join(cmd)}")
        exec_result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if exec_result.returncode == 0:
            result["status"] = "executed"
            result["message"] = f"Order placed: {action} {quantity} {symbol_us}"
            result["order_response"] = exec_result.stdout.strip()
            # Fix #3: Use estimated price from quote instead of 0
            recorded_price = estimated_price if estimated_price and estimated_price > 0 else 0
            logger.info(f"[TRADE] ✅ {result['message']} @ ~${recorded_price:.2f}" if recorded_price > 0 else f"[TRADE] ✅ {result['message']}")
            log_trade(
                symbol=symbol, action=action.upper(), price=recorded_price,
                quantity=quantity, signal=decision.get("reason", ""),
                note=f"EXECUTED | {result['order_response'][:150]}"
            )
        else:
            result["status"] = "error"
            err_msg = exec_result.stderr.strip()[:300]
            result["message"] = f"CLI error: {err_msg}"
            logger.error(f"[TRADE] ❌ {result['message']}")
            log_error(
                error_type="CLIError", module="auto_trade",
                description=result["message"],
                status="待处理"
            )

    except Exception as e:
        result["status"] = "error"
        result["message"] = f"Execution failed: {str(e)}"
        logger.error(f"[TRADE] ❌ {result['message']}")
        log_error(
            error_type=type(e).__name__, module="auto_trade",
            description=result["message"],
            status="待处理"
        )

    return result


def run_auto_trade(symbol: str, signal: str) -> Dict:
    """
    High-level entry: parse signal and execute trade.
    Always executes — no dry_run mode.
    """
    decision = parse_decision(signal)
    if not decision["symbol"]:
        decision["symbol"] = symbol

    log_decision(
        decision_type=decision["action"].upper(),
        content=f"{decision['action']} {decision['symbol']} x{decision['quantity']}",
        basis=decision.get("reason", signal[:200]),
        expected=f"置信度: {decision.get('confidence', 'N/A')}",
        result="待执行"
    )

    return execute_trade(decision)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Auto Trade Executor")
    parser.add_argument("--symbol", required=True, help="Stock symbol")
    parser.add_argument("--signal-file", default="", help="Path to signal JSON file")
    parser.add_argument("--signal", default="", help="Signal text")
    args = parser.parse_args()

    signal = args.signal
    if args.signal_file:
        with open(args.signal_file) as f:
            signal = f.read()

    if not signal:
        signal = f"HOLD {args.symbol} - no signal provided"

    result = run_auto_trade(args.symbol, signal)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
