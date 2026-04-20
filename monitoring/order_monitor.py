#!/usr/bin/env python3
"""
US Data Hub — Order Monitor (Pending Order 监控模块)
=====================================================
负责监控所有 pending 订单，处理超时未成交、价格跳空、部分成交等场景。

调度：
  - 盘前 (21:30 北京) → 开盘前检查 pending 订单
  - 盘中 (每 30 分钟) → 监控成交状态
  - 盘后 (04:00 北京) → 清理所有未成交的盘外市价单

对接模块：
  - TradingAgents → 价格跳空时重新评估
  - SignalHub → 检查原始信号有效性
  - PositionManager → 同步持仓
  - auto_execute → 风控检查
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from storage import Database
from executors.longbridge import LongbridgeExecutor
from management.position_manager import PositionManager
from analysis.signal_hub import SignalHub, Signal, SignalDirection, SignalSource
from dayup_logger import setup_root_logger, log_risk

setup_root_logger(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── 配置 ───

# 价格跳空阈值：超过此幅度则取消原单重新评估
GAP_THRESHOLD_PCT = 0.03  # 3%

# 超时阈值：开盘后多少分钟未成交则取消
OPEN_TIMEOUT_MINUTES = 15

# 部分成交容忍度：低于此比例视为部分成交
PARTIAL_FILL_THRESHOLD = 0.9

# Pending 订单允许的状态
PENDING_STATUSES = {"NotReported", "Submitted", "Queued", "PartialFilled", "Modified"}

# 已成交状态
FILLED_STATUSES = {"Filled", "PartiallyFilled"}


class OrderMonitor:
    """Pending 订单监控器"""

    def __init__(self, db: Database, executor: LongbridgeExecutor):
        self.db = db
        self.executor = executor
        self.pm = PositionManager(db, executor)

    # ═══════════════════════════════════════════
    # 核心入口
    # ═══════════════════════════════════════════

    def run_full_check(self) -> Dict:
        """
        完整检查所有 pending 订单。

        Returns:
            检查结果摘要
        """
        results = {
            "checked": 0,
            "filled": [],
            "partial_filled": [],
            "cancelled_price_gap": [],
            "cancelled_timeout": [],
            "cancelled_stale": [],
            "kept": [],
        }

        # 同步持仓
        self.pm.sync_from_broker()

        # 获取所有 pending 订单
        pending_orders = self._get_pending_orders()
        results["checked"] = len(pending_orders)

        if not pending_orders:
            logger.info("✅ No pending orders to monitor")
            return results

        logger.info(f"📋 Found {len(pending_orders)} pending orders to monitor")

        for order in pending_orders:
            symbol = order.get("symbol", "").replace(".US", "").upper()
            order_id = order.get("order_id", "")
            status = order.get("status", "")
            order_type = order.get("order_type", "")
            side = order.get("side", "")
            quantity = int(order.get("quantity", 0))
            created_at = order.get("created_at", "")

            logger.info(f"  Checking {symbol} {side} {quantity} - {order_id} ({status})")

            # 检查状态变化
            current_order = self._get_order_detail(order_id)
            if not current_order:
                continue

            current_status = current_order.get("status", "")
            executed_qty = int(current_order.get("executed_quantity", 0))

            # ─── 场景1: 已成交 ───
            if current_status in FILLED_STATUSES or (executed_qty >= quantity and executed_qty > 0):
                logger.info(f"  ✅ {symbol} {order_id} FILLED ({executed_qty}/{quantity})")
                self._sync_filled_order(current_order)
                results["filled"].append({
                    "symbol": symbol,
                    "order_id": order_id,
                    "executed_qty": executed_qty,
                    "total_qty": quantity,
                })
                continue

            # ─── 场景2: 部分成交 ───
            if current_status == "PartialFilled" or (0 < executed_qty < quantity):
                fill_ratio = executed_qty / quantity
                if fill_ratio >= PARTIAL_FILL_THRESHOLD:
                    # 大部分成交，取消剩余
                    logger.info(f"  ⚠️ {symbol} {order_id} PARTIAL FILL ({executed_qty}/{quantity}, {fill_ratio:.0%})")
                    self._sync_filled_order(current_order)
                    self.executor.cancel_order(order_id)
                    results["partial_filled"].append({
                        "symbol": symbol,
                        "order_id": order_id,
                        "executed_qty": executed_qty,
                        "total_qty": quantity,
                        "fill_ratio": fill_ratio,
                    })
                else:
                    # 少部分成交，需要重新评估
                    logger.info(f"  🔍 {symbol} {order_id} LOW PARTIAL FILL ({executed_qty}/{quantity})")
                    self._handle_partial_fill(current_order)
                    results["partial_filled"].append({
                        "symbol": symbol,
                        "order_id": order_id,
                        "executed_qty": executed_qty,
                        "total_qty": quantity,
                        "fill_ratio": fill_ratio,
                        "action": "re_evaluating",
                    })
                continue

            # ─── 场景3: 盘外市价单 → 清理 ───
            if order_type == "MO" and not self._is_market_open():
                logger.info(f"  🗑️  {symbol} {order_id} STALE market order (market closed)")
                self.executor.cancel_order(order_id)
                results["cancelled_stale"].append({
                    "symbol": symbol,
                    "order_id": order_id,
                    "reason": "market_closed_mo",
                })
                continue

            # ─── 场景4: 开盘后检查价格跳空 ───
            if self._is_market_open() and order_type == "MO":
                gap_pct = self._check_price_gap(symbol, order)
                if abs(gap_pct) > GAP_THRESHOLD_PCT:
                    logger.warning(
                        f"  🚨 {symbol} {order_id} PRICE GAP {gap_pct:+.1%} > {GAP_THRESHOLD_PCT:.0%}"
                    )
                    self.executor.cancel_order(order_id)
                    self._re_evaluate_on_gap(symbol, order, gap_pct)
                    results["cancelled_price_gap"].append({
                        "symbol": symbol,
                        "order_id": order_id,
                        "gap_pct": gap_pct,
                        "action": "re_evaluated",
                    })
                    continue

            # ─── 场景5: 开盘后超时未成交 ───
            if self._is_market_open():
                minutes_since_open = self._minutes_since_market_open()
                created_dt = self._parse_order_time(created_at)
                if created_dt:
                    minutes_pending = (datetime.now() - created_dt).total_seconds() / 60
                else:
                    minutes_pending = minutes_since_open

                if minutes_pending > OPEN_TIMEOUT_MINUTES and order_type == "MO":
                    logger.info(
                        f"  ⏰ {symbol} {order_id} TIMEOUT ({minutes_pending:.0f}min > {OPEN_TIMEOUT_MINUTES}min)"
                    )
                    self.executor.cancel_order(order_id)
                    self._re_evaluate_on_timeout(symbol, order)
                    results["cancelled_timeout"].append({
                        "symbol": symbol,
                        "order_id": order_id,
                        "minutes_pending": minutes_pending,
                        "action": "re_evaluated",
                    })
                    continue

            # ─── 场景6: 保持等待 ───
            logger.info(f"  ⏳ {symbol} {order_id} KEPT (pending, {status})")
            results["kept"].append({
                "symbol": symbol,
                "order_id": order_id,
                "status": status,
            })

        return results

    # ═══════════════════════════════════════════
    # 重新评估逻辑
    # ═══════════════════════════════════════════

    def _re_evaluate_on_gap(self, symbol: str, order: Dict, gap_pct: float):
        """价格跳空时，通过 TradingAgents 重新评估"""
        logger.info(f"  🔍 Re-evaluating {symbol} due to price gap {gap_pct:+.1%}")

        try:
            from tradingagents.main import run_trading_analysis
            today = datetime.now().strftime("%Y-%m-%d")

            result = run_trading_analysis(
                stock_symbol=symbol,
                trading_date=today,
                market="US",
            )

            decision_text = result.get("decision", "")
            text_lower = decision_text.lower()

            # 判断方向
            direction = None
            if any(w in text_lower for w in ["buy", "purchase", "enter", "accumulate"]):
                direction = SignalDirection.BUY
            elif any(w in text_lower for w in ["sell", "exit", "close", "liquidate"]):
                direction = SignalDirection.SELL

            if direction:
                # 提取置信度
                import re
                confidence = 0.5
                conf_match = re.search(r'(?:confidence|conf)[\s:：]*(\d+\.?\d*)\s*%?', text_lower)
                if conf_match:
                    val = float(conf_match.group(1))
                    confidence = val / 100.0 if val > 1 else val

                ts = TradeSignal(
                    symbol=symbol,
                    direction=direction,
                    confidence=confidence,
                    source=SignalSource.ORDER_MONITOR,
                    strength=0.8,
                    reason=f"Price gap re-evaluation: {gap_pct:+.1%}. {decision_text[:300]}",
                    extra={"gap_pct": gap_pct, "original_order": order},
                )

                # 注入 SignalHub
                hub = SignalHub(self.db, min_confidence=0.3)
                hub.add_signal(ts)
                logger.info(f"  ✅ Re-evaluation signal injected for {symbol}")
            else:
                logger.info(f"  ⏸️  {symbol} re-evaluation: HOLD (no trade signal)")

        except Exception as e:
            logger.error(f"  ❌ Re-evaluation failed for {symbol}: {e}")

    def _re_evaluate_on_timeout(self, symbol: str, order: Dict):
        """超时未成交时，检查原始信号是否仍有效"""
        logger.info(f"  🔍 Re-evaluating {symbol} due to timeout")

        try:
            # 检查原始信号
            hub = SignalHub(self.db, min_confidence=0.3)
            signals = hub.collect_all([symbol])

            if signals:
                # 信号仍存在，重新提交
                for sig in signals:
                    if sig.symbol == symbol.upper().replace(".US", ""):
                        logger.info(f"  ✅ {symbol} original signal still valid, re-injecting")
                        # SignalHub 会自动处理去重，这里只需记录
                        break
                else:
                    # 原始信号已消失，需要重新评估
                    logger.info(f"  🔍 {symbol} original signal expired, running fresh analysis")
                    self._re_evaluate_on_gap(symbol, order, 0)
            else:
                logger.info(f"  ⏸️  {symbol} no active signal, skipping")

        except Exception as e:
            logger.error(f"  ❌ Timeout re-evaluation failed for {symbol}: {e}")

    def _handle_partial_fill(self, order: Dict):
        """处理部分成交：同步已成交部分，剩余重新评估"""
        symbol = order.get("symbol", "").replace(".US", "").upper()
        executed_qty = int(order.get("executed_quantity", 0))

        logger.info(f"  📊 Partial fill sync for {symbol}: {executed_qty} shares executed")

        # 同步已成交部分
        self._sync_filled_order(order)

        # 剩余部分重新评估
        remaining_signal = {
            "symbol": symbol,
            "quantity_remaining": int(order.get("quantity", 0)) - executed_qty,
            "original_order": order,
        }
        logger.info(f"  🔍 Re-evaluating remaining {remaining_signal['quantity_remaining']} shares")
        self._re_evaluate_on_timeout(symbol, order)

    # ═══════════════════════════════════════════
    # 辅助方法
    # ═══════════════════════════════════════════

    def _get_pending_orders(self) -> List[Dict]:
        """获取所有 pending 订单"""
        try:
            orders = self.executor.get_orders()
            return [
                o for o in orders
                if o.get("status", "") in PENDING_STATUSES
            ]
        except Exception as e:
            logger.error(f"Failed to get pending orders: {e}")
            return []

    def _get_order_detail(self, order_id: str) -> Optional[Dict]:
        """获取订单详情"""
        try:
            from executors.longbridge import LongbridgeExecutor
            ex = LongbridgeExecutor()
            args = ["order", "--format", "json"]
            data = ex._run(args)
            if data and isinstance(data, list):
                for o in data:
                    if o.get("order_id") == order_id:
                        return o
        except Exception as e:
            logger.error(f"Failed to get order detail for {order_id}: {e}")
        return None

    def _sync_filled_order(self, order: Dict):
        """同步已成交订单到 trades 表"""
        symbol = order.get("symbol", "").replace(".US", "").upper()
        executed_price = order.get("executed_price", "0")
        executed_qty = int(order.get("executed_quantity", 0))
        side = order.get("side", "Buy")

        if executed_qty == 0:
            return

        try:
            direction = "buy" if side == "Buy" else "sell"
            price = float(executed_price.replace("-", "0")) if executed_price not in ("-", "") else 0

            self.db.conn.execute(
                """INSERT INTO trades
                   (timestamp, symbol, direction, quantity, price, note, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    symbol,
                    direction,
                    executed_qty,
                    price,
                    f"Synced from order {order.get('order_id', 'N/A')}",
                    "filled",
                )
            )
            self.db.conn.commit()
            logger.info(f"  💾 Synced {symbol} {direction} {executed_qty} @ ${price}")
        except Exception as e:
            logger.error(f"Failed to sync filled order: {e}")
            self.db.conn.rollback()

    def _check_price_gap(self, symbol: str, order: Dict) -> float:
        """检查价格跳空幅度"""
        try:
            quote = self.executor.get_quote(symbol)
            if not quote:
                return 0

            current_price = float(quote.get("last_done", 0))
            order_price = float(order.get("price", 0))

            if order_price <= 0 or current_price <= 0:
                return 0

            gap_pct = (current_price - order_price) / order_price
            return gap_pct
        except Exception as e:
            logger.error(f"Failed to check price gap for {symbol}: {e}")
            return 0

    def _is_market_open(self) -> bool:
        """判断当前是否开盘"""
        try:
            from analysis.session_strategy import get_market_session
            session = get_market_session()
            return session.is_trading_allowed
        except Exception:
            # 默认按北京时间粗略判断
            hour = datetime.now().hour
            return 22 <= hour or hour < 5  # 夏令时盘中

    def _minutes_since_market_open(self) -> float:
        """计算开盘后经过的分钟数"""
        try:
            # 美股开盘 9:30 ET，北京时间夏令时 21:30，冬令时 22:30
            now = datetime.now()
            # 粗略估算
            return (now.hour - 21) * 60 + now.minute
        except Exception:
            return 60

    def _parse_order_time(self, time_str: str) -> Optional[datetime]:
        """解析订单创建时间"""
        try:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def run_cleanup(self) -> Dict:
        """盘后清理：取消所有未成交的市价单"""
        results = {"cancelled": [], "kept": []}

        pending_orders = self._get_pending_orders()
        for order in pending_orders:
            order_id = order.get("order_id", "")
            symbol = order.get("symbol", "").replace(".US", "").upper()
            order_type = order.get("order_type", "")

            if order_type == "MO":
                logger.info(f"  🗑️  Cleaning up stale MO: {symbol} {order_id}")
                self.executor.cancel_order(order_id)
                results["cancelled"].append({
                    "symbol": symbol,
                    "order_id": order_id,
                })
            else:
                results["kept"].append(order)

        return results


# ─── 兼容旧接口 ───
from analysis.signal_schema import TradeSignal


def main():
    """CLI 入口"""
    import argparse

    parser = argparse.ArgumentParser(description="Order Monitor")
    parser.add_argument("--mode", choices=["check", "cleanup"], default="check",
                       help="check=监控 pending 订单, cleanup=盘后清理")
    args = parser.parse_args()

    db = Database()
    db.init_schema()
    executor = LongbridgeExecutor()
    monitor = OrderMonitor(db, executor)

    if args.mode == "check":
        results = monitor.run_full_check()
        print(f"\n{'='*60}")
        print(f"📋 Order Monitor Results")
        print(f"{'='*60}")
        print(f"  Checked:          {results['checked']}")
        print(f"  Filled:           {len(results['filled'])}")
        print(f"  Partial Filled:   {len(results['partial_filled'])}")
        print(f"  Cancelled (gap):  {len(results['cancelled_price_gap'])}")
        print(f"  Cancelled (time): {len(results['cancelled_timeout'])}")
        print(f"  Cancelled (stale):{len(results['cancelled_stale'])}")
        print(f"  Kept:             {len(results['kept'])}")
        print(f"{'='*60}")
    elif args.mode == "cleanup":
        results = monitor.run_cleanup()
        print(f"\n{'='*60}")
        print(f"🗑️  Cleanup Results")
        print(f"{'='*60}")
        print(f"  Cancelled: {len(results['cancelled'])}")
        print(f"  Kept:      {len(results['kept'])}")
        print(f"{'='*60}")

    db.close()


if __name__ == "__main__":
    main()
