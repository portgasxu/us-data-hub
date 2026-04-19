#!/usr/bin/env python3
"""
US Data Hub — 影子策略执行器 (v6.0)
===================================
与主策略并行运行，只记账不下单。

全局设计:
  1. 影子策略接收与主策略完全相同的信号
  2. 使用不同的参数/规则处理（可配置）
  3. 获取实时价格用于模拟成交
  4. 每日/每周对比主策略 vs 影子策略 P&L
  5. 影子持续优于主策略时告警

用法:
    python scripts/shadow_executor.py --run          # 执行影子交易（只记账）
    python scripts/shadow_executor.py --compare      # 对比主/影子策略
    python scripts/shadow_executor.py --update       # 更新持仓 P&L
"""

import sys
import os
import logging
import argparse
import sqlite3
from datetime import datetime
from typing import Dict, Optional, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)


class ShadowConfig:
    """影子策略配置 — 与主策略的区别点"""
    # 动态阈值参数（更激进或更保守）
    BUY_THRESHOLD = 0.30      # 主策略 0.35
    SELL_THRESHOLD = -0.25    # 主策略 -0.20
    STOP_LOSS_PCT = 0.06      # 主策略 0.08
    TAKE_PROFIT_PCT = 0.15    # 主策略 0.10
    MAX_POSITIONS = 8         # 主策略 5

    # 信号权重（更依赖某些信号源）
    SIGNAL_WEIGHTS = {
        "trading_agents": 1.2,   # 更信赖 TA
        "screener": 0.8,
        "sentiment": 0.9,
        "factors": 1.0,
    }


class ShadowExecutor:
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "data", "us_data_hub.db"
            )
        self.db_path = db_path
        self._init_table()

    def _init_table(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS shadow_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    quantity REAL,
                    entry_price REAL,
                    exit_price REAL DEFAULT 0,
                    signal_id TEXT,
                    execution_id TEXT,
                    strategy_name TEXT DEFAULT 'shadow_v1',
                    entry_at TEXT,
                    exit_at TEXT,
                    pnl REAL DEFAULT 0,
                    pnl_pct REAL DEFAULT 0,
                    status TEXT DEFAULT 'OPEN',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_shadow_status ON shadow_trades(status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_shadow_symbol ON shadow_trades(symbol)"
            )

    def execute_signal(self, signal, execution_id: str = None):
        """记录影子交易（不实际下单）"""
        try:
            from analysis.signal_schema import TradeSignal, SignalDirection

            price = self._get_current_price(signal.symbol)
            if not price:
                return

            quantity = signal.quantity_suggestion or 10

            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """INSERT INTO shadow_trades
                       (symbol, action, quantity, entry_price, signal_id, execution_id, entry_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (signal.symbol, signal.direction.value if hasattr(signal.direction, 'value') else signal.direction,
                     quantity, price,
                     getattr(signal, 'signal_id', ''), execution_id,
                     datetime.now().isoformat())
                )

            logger.info(f"👻 Shadow: {signal.symbol} {signal.direction} "
                        f"{quantity} @ ${price:.2f}")

        except Exception as e:
            logger.error(f"Shadow executor failed for {signal.symbol}: {e}")

    def update_positions(self):
        """更新持仓的当前价格和 P&L"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            open_trades = conn.execute(
                "SELECT * FROM shadow_trades WHERE status = 'OPEN'"
            ).fetchall()

            updated = 0
            for trade in open_trades:
                current_price = self._get_current_price(trade["symbol"])
                if current_price:
                    pnl = (current_price - trade["entry_price"]) * trade["quantity"]
                    pnl_pct = (current_price - trade["entry_price"]) / trade["entry_price"] if trade["entry_price"] > 0 else 0

                    conn.execute(
                        """UPDATE shadow_trades SET pnl = ?, pnl_pct = ?
                           WHERE id = ?""",
                        (pnl, pnl_pct, trade["id"])
                    )
                    updated += 1

            logger.info(f"👻 Shadow: updated {updated} open positions")

    def compare_with_main(self, days: int = 30) -> Dict:
        """对比影子策略 vs 主策略"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            main = conn.execute(
                """SELECT COUNT(*) as trades,
                          AVG(CASE WHEN actual_return > 0 THEN 1 ELSE 0 END) as win_rate,
                          AVG(actual_return) as avg_return,
                          SUM(actual_return) as total_return
                   FROM trades
                   WHERE timestamp >= date('now', ?)
                   AND actual_return IS NOT NULL""",
                (f"-{days} days",)
            ).fetchone()

            shadow = conn.execute(
                """SELECT COUNT(*) as trades,
                          AVG(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as win_rate,
                          AVG(pnl_pct) as avg_return,
                          SUM(pnl) as total_pnl
                   FROM shadow_trades
                   WHERE entry_at >= date('now', ?)""",
                (f"-{days} days",)
            ).fetchone()

            return {
                "period_days": days,
                "main": {
                    "trades": main["trades"] if main else 0,
                    "win_rate": main["win_rate"] if main and main["win_rate"] else 0,
                    "avg_return": main["avg_return"] if main and main["avg_return"] else 0,
                    "total_return": main["total_return"] if main and main["total_return"] else 0,
                },
                "shadow": {
                    "trades": shadow["trades"] if shadow else 0,
                    "win_rate": shadow["win_rate"] if shadow and shadow["win_rate"] else 0,
                    "avg_return": shadow["avg_return"] if shadow and shadow["avg_return"] else 0,
                    "total_return": shadow["total_pnl"] if shadow and shadow["total_pnl"] else 0,
                },
                "shadow_better": (shadow["total_pnl"] if shadow and shadow["total_pnl"] else 0) > (main["total_return"] if main and main["total_return"] else 0),
            }

    def _get_current_price(self, symbol: str) -> Optional[float]:
        """获取最新价格"""
        try:
            from storage import Database
            db = Database()
            row = db.conn.execute(
                "SELECT close FROM prices WHERE symbol = ? AND close IS NOT NULL AND close > 0 ORDER BY date DESC LIMIT 1",
                (symbol,)
            ).fetchone()
            db.close()
            return row[0] if row else None
        except Exception:
            return None


def print_comparison(result: Dict):
    """打印对比报告"""
    print(f"\n{'='*60}")
    print(f"📊 影子策略 vs 主策略对比 ({result['period_days']}天)")
    print(f"{'='*60}")

    main = result["main"]
    shadow = result["shadow"]

    print(f"\n{'指标':<15} {'主策略':>15} {'影子策略':>15}")
    print(f"{'-'*45}")
    print(f"{'交易次数':<15} {main['trades']:>15} {shadow['trades']:>15}")
    print(f"{'胜率':<15} {main['win_rate']:>14.1%} {shadow['win_rate']:>14.1%}")
    print(f"{'平均收益':<15} {main['avg_return']:>14.1%} {shadow['avg_return']:>14.1%}")
    print(f"{'总收益':<15} {main['total_return']:>15.0f} {shadow['total_return']:>15.0f}")

    if result["shadow_better"]:
        print(f"\n⚠️  影子策略表现更好！建议考虑切换参数。")
    else:
        print(f"\n✅ 主策略表现正常，影子策略暂未超越。")

    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="影子策略执行器")
    parser.add_argument("--run", action="store_true", help="执行影子交易（只记账）")
    parser.add_argument("--compare", action="store_true", help="对比主/影子策略")
    parser.add_argument("--update", action="store_true", help="更新持仓 P&L")
    parser.add_argument("--days", type=int, default=30, help="对比天数")
    args = parser.parse_args()

    executor = ShadowExecutor()

    if args.compare:
        result = executor.compare_with_main(args.days)
        print_comparison(result)
    elif args.update:
        executor.update_positions()
        print("✅ Shadow positions updated")
    else:
        parser.print_help()
