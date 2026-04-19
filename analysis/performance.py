"""
US Data Hub — Performance Analytics
=====================================
Compute Sharpe ratio, max drawdown, win rate, and other performance metrics.

Fix #4: Previously all stats were empty — now properly calculated from trades + prices.
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)

# ─── 参数 ───
RISK_FREE_RATE = 0.05  # 5% 年化（美债利率近似）
TRADING_DAYS_PER_YEAR = 252


def compute_performance(db) -> Dict:
    """
    计算完整性能统计。

    Returns:
        {
            'sharpe_ratio': float,
            'max_drawdown': float,
            'win_rate': float,
            'total_trades': int,
            'total_return': float,
            'avg_return_per_trade': float,
            'profit_factor': float,
            'best_trade': float,
            'worst_trade': float,
            'consecutive_wins': int,
            'consecutive_losses': int,
            'avg_holding_period_days': float,
            'updated_at': str,
        }
    """
    result = {
        'sharpe_ratio': 0.0,
        'max_drawdown': 0.0,
        'win_rate': 0.0,
        'total_trades': 0,
        'total_return': 0.0,
        'avg_return_per_trade': 0.0,
        'profit_factor': 0.0,
        'best_trade': 0.0,
        'worst_trade': 0.0,
        'consecutive_wins': 0,
        'consecutive_losses': 0,
        'avg_holding_period_days': 0.0,
        'updated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    try:
        # ─── 1. 获取所有交易 ───
        trades = db.conn.execute(
            """SELECT symbol, direction, quantity, price, timestamp, factor_scores
               FROM trades ORDER BY timestamp ASC"""
        ).fetchall()

        if not trades:
            logger.info("No trades found — performance stats empty")
            return result

        result['total_trades'] = len(trades)

        # ─── 2. 计算每笔交易收益 ───
        trade_returns = []
        buy_records = {}  # symbol -> list of (qty, price, timestamp)

        for symbol, direction, qty, price, ts, factors in trades:
            price = float(price) if price else 0
            if price <= 0:
                continue

            if direction.lower() == "buy":
                if symbol not in buy_records:
                    buy_records[symbol] = []
                buy_records[symbol].append((qty, price, ts))
            elif direction.lower() in ("sell", "reduce"):
                if symbol in buy_records and buy_records[symbol]:
                    # FIFO: match with earliest buy
                    buy_qty, buy_price, buy_ts = buy_records[symbol].pop(0)
                    matched_qty = min(qty, buy_qty)
                    ret = (price - buy_price) / buy_price

                    # Remaining quantity handling
                    if qty > matched_qty:
                        # Partial: put remaining buys back
                        buy_records[symbol].insert(0, (buy_qty - matched_qty, buy_price, buy_ts))

                    trade_returns.append(ret)

        # ─── 3. 当前持仓浮盈 ───
        holdings = db.conn.execute(
            """SELECT h.symbol, h.quantity, h.cost_price
               FROM holdings h WHERE h.active = 1 AND h.quantity > 0"""
        ).fetchall()

        for symbol, qty, cost in holdings:
            if cost and cost > 0:
                price_row = db.conn.execute(
                    "SELECT close FROM prices WHERE symbol = ? AND close > 0 ORDER BY date DESC LIMIT 1",
                    (symbol,)
                ).fetchone()
                if price_row and price_row[0] > 0:
                    current_price = price_row[0]
                    pnl = (current_price - cost) / cost
                    trade_returns.append(pnl)

        if not trade_returns:
            return result

        # ─── 4. 基础统计 ───
        wins = [r for r in trade_returns if r > 0]
        losses = [r for r in trade_returns if r <= 0]

        result['win_rate'] = len(wins) / len(trade_returns)
        result['total_return'] = sum(trade_returns)
        result['avg_return_per_trade'] = sum(trade_returns) / len(trade_returns)
        result['best_trade'] = max(trade_returns)
        result['worst_trade'] = min(trade_returns)

        # Profit factor
        gross_profit = sum(wins) if wins else 0
        gross_loss = abs(sum(losses)) if losses else 0
        result['profit_factor'] = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        # ─── 5. 连续盈亏 ───
        max_consec_wins = 0
        max_consec_losses = 0
        curr_wins = 0
        curr_losses = 0
        for r in trade_returns:
            if r > 0:
                curr_wins += 1
                curr_losses = 0
                max_consec_wins = max(max_consec_wins, curr_wins)
            else:
                curr_losses += 1
                curr_wins = 0
                max_consec_losses = max(max_consec_losses, curr_losses)

        result['consecutive_wins'] = max_consec_wins
        result['consecutive_losses'] = max_consec_losses

        # ─── 6. Sharpe Ratio ───
        if len(trade_returns) >= 2:
            mean_ret = sum(trade_returns) / len(trade_returns)
            variance = sum((r - mean_ret) ** 2 for r in trade_returns) / (len(trade_returns) - 1)
            std_ret = math.sqrt(variance) if variance > 0 else 0.001
            daily_sharpe = (mean_ret - RISK_FREE_RATE / TRADING_DAYS_PER_YEAR) / std_ret
            result['sharpe_ratio'] = round(daily_sharpe * math.sqrt(TRADING_DAYS_PER_YEAR), 3)
        else:
            result['sharpe_ratio'] = 0.0

        # ─── 7. 最大回撤 ───
        result['max_drawdown'] = _compute_max_drawdown(db, trades)

        # ─── 8. 平均持仓天数 ───
        result['avg_holding_period_days'] = _compute_avg_holding_period(db)

        logger.info(f"Performance computed: {result['total_trades']} trades, "
                     f"win_rate={result['win_rate']:.1%}, sharpe={result['sharpe_ratio']:.3f}, "
                     f"max_dd={result['max_drawdown']:.1%}")

    except Exception as e:
        logger.error(f"Performance computation failed: {e}")
        import traceback
        traceback.print_exc()

    return result


def _compute_max_drawdown(db, trades) -> float:
    """
    计算最大回撤：基于持仓净值曲线。
    """
    try:
        # 构建每日净值近似
        if not trades:
            return 0.0

        # Get date range
        first_ts = trades[0][4]  # timestamp of first trade
        try:
            first_date = first_ts[:10]
        except (IndexError, TypeError):
            return 0.0

        # Get all holdings
        holdings = db.conn.execute(
            """SELECT symbol, quantity, cost_price FROM holdings WHERE active = 1"""
        ).fetchall()

        if not holdings:
            return 0.0

        # Compute current portfolio value trajectory approximation
        # Use cost basis as starting point, then track changes
        peak_value = 0
        max_dd = 0.0

        for symbol, qty, cost in holdings:
            if not cost or cost <= 0:
                continue

            # Get price history for this symbol
            prices = db.conn.execute(
                """SELECT close FROM prices WHERE symbol = ? AND close > 0
                   ORDER BY date ASC""",
                (symbol,)
            ).fetchall()

            if not prices:
                continue

            cost_basis = qty * cost
            if peak_value == 0:
                peak_value = cost_basis

            for (price,) in prices:
                current_value = qty * price
                if current_value > peak_value:
                    peak_value = current_value
                dd = (peak_value - current_value) / peak_value if peak_value > 0 else 0
                max_dd = max(max_dd, dd)

        return round(max_dd, 4)

    except Exception as e:
        logger.error(f"Max drawdown computation failed: {e}")
        return 0.0


def _compute_avg_holding_period(db) -> float:
    """计算平均持仓天数（基于已平仓交易）。"""
    try:
        buys = db.conn.execute(
            "SELECT symbol, timestamp FROM trades WHERE direction='buy' ORDER BY timestamp ASC"
        ).fetchall()
        sells = db.conn.execute(
            "SELECT symbol, timestamp FROM trades WHERE direction IN ('sell','reduce') ORDER BY timestamp ASC"
        ).fetchall()

        if not buys or not sells:
            return 0.0

        # Simple FIFO matching
        buy_map = {}
        for symbol, ts in buys:
            if symbol not in buy_map:
                buy_map[symbol] = []
            buy_map[symbol].append(ts)

        periods = []
        for symbol, ts in sells:
            if symbol in buy_map and buy_map[symbol]:
                buy_ts = buy_map[symbol].pop(0)
                try:
                    buy_dt = datetime.strptime(buy_ts[:19], "%Y-%m-%d %H:%M:%S")
                    sell_dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
                    days = (sell_dt - buy_dt).days
                    if days >= 0:
                        periods.append(days)
                except (ValueError, TypeError):
                    pass

        return round(sum(periods) / len(periods), 1) if periods else 0.0

    except Exception as e:
        logger.error(f"Average holding period computation failed: {e}")
        return 0.0


if __name__ == "__main__":
    from storage import Database
    import json

    db = Database()
    perf = compute_performance(db)
    print(json.dumps(perf, indent=2, ensure_ascii=False))
    db.close()
