"""
Simple Backtest - 简易回测引擎 (P0 Fix #3)

利用已有历史数据做"事后验证"：
1. 从 screener_history 提取过去 N 天的筛选信号
2. 用 prices 表的历史价格模拟买入/卖出
3. 对比"按信号交易" vs "持有不动"的收益差异
4. 输出：胜率、Sharpe、最大回撤、每笔盈亏

不需要新增数据源，所有数据都已经存在。
"""

import logging
from datetime import datetime

logger = logging.getLogger("backtest")

RISK_FREE_RATE = 0.05  # 年化无风险利率


def run_backtest(db, days=90, min_score=0.65):
    """运行简易回测。"""
    signals = db.conn.execute(
        "SELECT run_time, symbol, total_score FROM screener_history "
        "WHERE run_time >= date('now', ?) AND total_score >= ? ORDER BY run_time ASC",
        (f"-{days} days", min_score),
    ).fetchall()

    if not signals:
        return {"error": "No signals found for backtest period"}

    # Get price data
    prices = {}
    symbols = set(s["symbol"] for s in signals)
    for sym in symbols:
        rows = db.conn.execute(
            "SELECT date, close FROM prices WHERE symbol = ? AND date >= date('now', ?) ORDER BY date ASC",
            (sym, f"-{days} days"),
        ).fetchall()
        prices[sym] = {r["date"]: r["close"] for r in rows if r["close"]}

    # Simulate trading
    initial_capital = 10000.0
    cash = initial_capital
    positions = {}
    trades = []
    portfolio_values = [initial_capital]

    for sig in signals:
        sym = sig["symbol"]
        sig_date = sig["run_time"][:10]

        if sym not in prices or sig_date not in prices[sym]:
            continue
        price = prices[sym][sig_date]
        if not price or price <= 0:
            continue

        if sym in positions:
            continue

        buy_amount = min(cash * 0.2, 2000)
        if buy_amount < 100:
            continue

        qty = int(buy_amount / price)
        if qty <= 0:
            continue

        cost = qty * price
        if cost > cash:
            continue

        cash -= cost
        positions[sym] = {"qty": qty, "entry_price": price, "entry_date": sig_date}

        # Sell after 5 trading days
        exit_date = _find_date_offset(prices[sym], sig_date, 5)
        if exit_date and exit_date in prices[sym]:
            exit_price = prices[sym][exit_date]
            proceeds = qty * exit_price
            pnl = proceeds - cost
            pnl_pct = pnl / cost
            cash += proceeds

            trades.append({
                "symbol": sym,
                "entry_date": sig_date,
                "exit_date": exit_date,
                "entry_price": price,
                "exit_price": exit_price,
                "qty": qty,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
            })

            del positions[sym]

        # Portfolio value
        current_value = cash
        for p_sym, p_data in positions.items():
            if p_sym in prices:
                last_price = list(prices[p_sym].values())[-1]
                current_value += p_data["qty"] * last_price
        portfolio_values.append(current_value)

    if not trades:
        return {"error": "No completed trades in backtest period"}

    pnls = [t["pnl_pct"] for t in trades]
    wins = [p for p in pnls if p > 0]

    total_return = (portfolio_values[-1] - initial_capital) / initial_capital
    bh_return = _calc_buy_hold_return(prices, signals)
    sharpe = _calc_sharpe(portfolio_values)
    max_dd = _calc_max_drawdown(portfolio_values)

    by_symbol = {}
    for t in trades:
        sym = t["symbol"]
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_symbol[sym].append(t["pnl_pct"])

    best_symbol = max(by_symbol, key=lambda s: sum(by_symbol[s]) / len(by_symbol[s])) if by_symbol else "N/A"
    worst_symbol = min(by_symbol, key=lambda s: sum(by_symbol[s]) / len(by_symbol[s])) if by_symbol else "N/A"

    return {
        "period_days": days,
        "start_date": signals[0]["run_time"][:10],
        "end_date": signals[-1]["run_time"][:10],
        "total_signals": len(signals),
        "total_trades": len(trades),
        "win_rate": len(wins) / len(trades),
        "total_return": total_return,
        "buy_hold_return": bh_return,
        "excess_return": total_return - bh_return,
        "sharpe_ratio": sharpe,
        "max_drawdown": max_dd,
        "avg_pnl": sum(pnls) / len(pnls),
        "best_trade": max(pnls),
        "worst_trade": min(pnls),
        "best_symbol": best_symbol,
        "worst_symbol": worst_symbol,
        "trades": trades,
    }


def _find_date_offset(prices_dict, start_date, offset_days):
    dates = sorted(prices_dict.keys())
    try:
        start_idx = dates.index(start_date)
    except ValueError:
        return None
    target_idx = start_idx + offset_days
    if target_idx < len(dates):
        return dates[target_idx]
    return None


def _calc_buy_hold_return(prices, signals):
    returns = []
    for sig in signals:
        sym = sig["symbol"]
        if sym not in prices:
            continue
        price_dates = sorted(prices[sym].keys())
        if len(price_dates) < 2:
            continue
        first_price = prices[sym][price_dates[0]]
        last_price = prices[sym][price_dates[-1]]
        if first_price > 0:
            returns.append((last_price - first_price) / first_price)
    return sum(returns) / len(returns) if returns else 0.0


def _calc_sharpe(portfolio_values):
    if len(portfolio_values) < 3:
        return 0.0
    daily_returns = []
    for i in range(1, len(portfolio_values)):
        prev = portfolio_values[i - 1]
        if prev > 0:
            daily_returns.append((portfolio_values[i] - prev) / prev)
    if not daily_returns:
        return 0.0
    avg = sum(daily_returns) / len(daily_returns)
    variance = sum((r - avg) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
    std = variance ** 0.5
    if std == 0:
        return 0.0
    return (avg - RISK_FREE_RATE / 252) / std * (252 ** 0.5)


def _calc_max_drawdown(portfolio_values):
    if not portfolio_values:
        return 0.0
    peak = portfolio_values[0]
    max_dd = 0.0
    for value in portfolio_values:
        if value > peak:
            peak = value
        dd = (peak - value) / peak
        if dd > max_dd:
            max_dd = dd
    return max_dd


def generate_backtest_report(db, days=90):
    """生成回测报告文本。"""
    result = run_backtest(db, days)
    if "error" in result:
        return "Backtest failed: " + result["error"]

    lines = []
    lines.append("=" * 60)
    lines.append("Backtest Report")
    lines.append("Period: {} ~ {} ({} days)".format(result["start_date"], result["end_date"], result["period_days"]))
    lines.append("=" * 60)
    lines.append("")
    lines.append("Signals:       {}".format(result["total_signals"]))
    lines.append("Trades:        {}".format(result["total_trades"]))
    lines.append("Win Rate:      {:.1%}".format(result["win_rate"]))
    lines.append("")
    lines.append("Strategy Ret:  {:+.1%}".format(result["total_return"]))
    lines.append("Buy & Hold:    {:+.1%}".format(result["buy_hold_return"]))
    excess = result["excess_return"]
    lines.append("Excess:        {:+.1%} {}".format(excess, "OK" if excess > 0 else "FAIL"))
    lines.append("Sharpe:        {:.2f}".format(result["sharpe_ratio"]))
    lines.append("Max Drawdown:  {:.1%}".format(result["max_drawdown"]))
    lines.append("")
    lines.append("Avg P&L:       {:+.1%}".format(result["avg_pnl"]))
    lines.append("Best Trade:    {:+.1%}".format(result["best_trade"]))
    lines.append("Worst Trade:   {:+.1%}".format(result["worst_trade"]))
    lines.append("")
    lines.append("Best Symbol:   {}".format(result["best_symbol"]))
    lines.append("Worst Symbol:  {}".format(result["worst_symbol"]))
    lines.append("")
    if result["trades"]:
        lines.append("--- Last 10 Trades ---")
        for t in result["trades"][-10:]:
            em = "OK" if t["pnl_pct"] > 0 else "FAIL"
            lines.append("  {} {} | {}->{} | ${:.2f}->${:.2f} | {:+.1%}".format(
                em, t["symbol"], t["entry_date"], t["exit_date"],
                t["entry_price"], t["exit_price"], t["pnl_pct"]))
    return "\n".join(lines)
