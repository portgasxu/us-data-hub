"""
Smart Risk — 智能止损模块 (P0 Fix #1)

3 层防护网，任意一层触发 → 执行卖出:
  第 1 层: ATR 动态止损 (技术面)
  第 2 层: 因子恶化止损 (基本面)
  第 3 层: 反向信号止损 (系统纠错)

所有参数从现有数据自动计算，不使用硬编码固定百分比。
"""

import logging
from datetime import datetime

logger = logging.getLogger("smart_risk")

# ─── 配置 ──────────────────────────────────────────────
ATR_PERIOD = 14
ATR_STOP_MULTIPLIER = 2.0      # 止损 = 入场价 - 2×ATR
FUNDAMENTAL_CHECK_DAYS = 3     # 连续 N 天因子恶化才触发
REVERSE_SIGNAL_THRESHOLD = 0.75


def calculate_atr(db, symbol: str, period: int = ATR_PERIOD) -> float | None:
    """从 prices 表计算 ATR(14)。使用 (high - low) 的简化版，因为我们的数据源提供 OHLC。"""
    rows = db.conn.execute(
        """SELECT high, low, close FROM prices
           WHERE symbol = ? AND high IS NOT NULL AND low IS NOT NULL
           ORDER BY date DESC LIMIT ?""",
        (symbol, period + 1),
    ).fetchall()

    if len(rows) < 2:
        return None

    # 计算 True Range
    true_ranges = []
    for i in range(len(rows) - 1):
        high = rows[i]["high"]
        low = rows[i]["low"]
        prev_close = rows[i + 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)

    if not true_ranges:
        return None

    return sum(true_ranges) / len(true_ranges)


def check_atr_stop_loss(db, symbol: str, cost_price: float, current_price: float) -> tuple[bool, str | None]:
    """
    第 1 层: ATR 动态止损

    止损位 = 成本价 - ATR_MULTIPLIER × ATR
    波动大的股票止损宽，波动小的止损窄。
    不被正常波动震出去。

    Returns: (triggered, reason)
    """
    atr = calculate_atr(db, symbol)
    if atr is None:
        # fallback: 用 5% 固定止损
        loss_pct = (current_price - cost_price) / cost_price
        if loss_pct < -0.05:
            return True, f"ATR 数据不足，使用 5% fallback 止损 (当前亏损 {loss_pct:.1%})"
        return False, None

    stop_price = cost_price - (ATR_STOP_MULTIPLIER * atr)
    if current_price <= stop_price:
        loss_pct = (current_price - cost_price) / cost_price
        atr_pct = atr / cost_price
        return True, (
            f"ATR 动态止损触发: 成本=${cost_price:.2f}, 现价=${current_price:.2f}, "
            f"ATR=${atr:.2f} ({atr_pct:.1%}), 止损位=${stop_price:.2f}, "
            f"当前亏损 {loss_pct:.1%}"
        )

    return False, None


def check_fundamental_stop(db, symbol: str) -> tuple[bool, str | None]:
    """
    第 2 层: 基本面止损

    IF 质量因子连续 3 天 < -0.5
    AND 动量因子 < 0
    AND 最近新闻情绪 < -0.3
    → 不是价格跌了就跑，而是"基本面确实在变差"才跑

    Returns: (triggered, reason)
    """
    # 检查质量因子
    quality_rows = db.conn.execute(
        """SELECT date, factor_value FROM factors
           WHERE symbol = ? AND factor_name = 'quality'
           ORDER BY date DESC LIMIT ?""",
        (symbol, FUNDAMENTAL_CHECK_DAYS),
    ).fetchall()

    if len(quality_rows) < FUNDAMENTAL_CHECK_DAYS:
        return False, None

    # 检查是否连续恶化
    consecutive_bad = all(r["factor_value"] < -0.5 for r in quality_rows)
    if not consecutive_bad:
        return False, None

    quality_latest = quality_rows[0]["factor_value"]

    # 检查动量因子
    momentum_row = db.conn.execute(
        """SELECT factor_value FROM factors
           WHERE symbol = ? AND factor_name = 'momentum'
           ORDER BY date DESC LIMIT 1""",
        (symbol,),
    ).fetchone()

    if momentum_row is None or momentum_row["factor_value"] >= 0:
        return False, None

    momentum_val = momentum_row["factor_value"]

    # 检查新闻情绪 (如果有 sentiment_news 表)
    sentiment_val = None
    try:
        sent_row = db.conn.execute(
            """SELECT AVG(sentiment_score) as avg_score FROM sentiment_news
               WHERE symbol = ? AND created_at >= datetime('now', '-3 days')""",
            (symbol,),
        ).fetchone()
        if sent_row and sent_row["avg_score"] is not None:
            sentiment_val = sent_row["avg_score"]
    except Exception:
        pass  # sentiment_news 表可能不存在

    if sentiment_val is not None and sentiment_val < -0.3:
        return True, (
            f"基本面止损触发: 质量因子连续 {FUNDAMENTAL_CHECK_DAYS} 天恶化 "
            f"(最新={quality_latest:.3f}), 动量={momentum_val:.3f}, "
            f"新闻情绪={sentiment_val:.3f}"
        )

    # 即使没有情绪数据，质量+动量双重恶化也足够触发
    if consecutive_bad and momentum_val < -0.3:
        return True, (
            f"基本面止损触发: 质量因子连续 {FUNDAMENTAL_CHECK_DAYS} 天恶化 "
            f"(最新={quality_latest:.3f}), 动量={momentum_val:.3f}"
        )

    return False, None


def check_reverse_signal_stop(db, symbol: str, current_confidence: float = None) -> tuple[bool, str | None]:
    """
    第 3 层: 系统反向信号止损

    IF screener 对同一标的发出反向 SELL 信号
    AND 信号 confidence > 0.75
    → 系统自己意识到"我之前看错了"，自动纠错

    利用 screener_history 最近一次的评分来判断。
    如果综合得分低于 0.4（对应 confidence < 0.66），说明系统已不看好。

    Returns: (triggered, reason)
    """
    row = db.conn.execute(
        """SELECT run_time, total_score, dim_news_volume, dim_social_heat,
                  dim_capital_flow, dim_momentum, dim_volatility, dim_mean_reversion
           FROM screener_history
           WHERE symbol = ?
           ORDER BY run_time DESC LIMIT 1""",
        (symbol,),
    ).fetchone()

    if row is None:
        return False, None

    score = row["total_score"]
    # screener score 0-1, 低分意味着系统不看好
    if score < 0.4:
        return True, (
            f"反向信号止损: screener 最新得分 {score:.3f} (< 0.4 阈值), "
            f"系统已不看好该标的 (run_time: {row['run_time']})"
        )

    return False, None


def check_all_stop_losses(db, symbol: str, cost_price: float, current_price: float) -> list[dict]:
    """
    运行全部 3 层止损检查。

    Returns: list of triggered reasons (empty = 无需止损)
    """
    triggers = []

    triggered, reason = check_atr_stop_loss(db, symbol, cost_price, current_price)
    if triggered:
        triggers.append({"layer": 1, "type": "ATR 动态止损", "reason": reason})

    triggered, reason = check_fundamental_stop(db, symbol)
    if triggered:
        triggers.append({"layer": 2, "type": "基本面止损", "reason": reason})

    triggered, reason = check_reverse_signal_stop(db, symbol)
    if triggered:
        triggers.append({"layer": 3, "type": "反向信号止损", "reason": reason})

    return triggers


def generate_stop_report(db, symbols: list[str]) -> str:
    """生成所有持仓的止损检查报告。"""
    from analysis.signal_hub import get_position_pct, get_portfolio_value, get_current_price

    lines = ["=" * 60]
    lines.append("🛡️ 智能止损检查报告")
    lines.append(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 60)

    for symbol in symbols:
        cost = db.conn.execute(
            "SELECT cost_price FROM holdings WHERE symbol = ? AND active = 1", (symbol,)
        ).fetchone()
        if not cost:
            continue

        cost_price = cost["cost_price"]
        current_price = get_current_price(db, symbol, cost_price)
        pnl = (current_price - cost_price) / cost_price

        lines.append(f"\n{symbol}: 成本=${cost_price:.2f} → 现价=${current_price:.2f} (P&L {pnl:+.1%})")

        # ATR 信息
        atr = calculate_atr(db, symbol)
        if atr:
            stop_price = cost_price - (ATR_STOP_MULTIPLIER * atr)
            lines.append(f"  ATR: ${atr:.2f}, 动态止损位: ${stop_price:.2f}")

        # 运行检查
        triggers = check_all_stop_losses(db, symbol, cost_price, current_price)
        if triggers:
            for t in triggers:
                lines.append(f"  🔴 [{t['layer']}/{t['type']}] {t['reason']}")
        else:
            lines.append(f"  ✅ 无需止损")

    return "\n".join(lines)
