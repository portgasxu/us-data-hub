"""
Smart Profit — 智能止盈模块 (P0 Fix #2)

状态机追踪止损 + RSI 超买预警 + 板块联动
不是"赚 15% 就卖"，而是让盈利奔跑，同时保护已赚到的钱。
"""

import logging

logger = logging.getLogger("smart_profit")

# ─── 状态机配置 ─────────────────────────────────────────
PROFIT_STATES = {
    "entry":    {"min_profit": 0.00,  "trailing_offset": "ATR",      "label": "刚买入"},
    "breakeven":{"min_profit": 0.05,  "trailing_offset": "cost",     "label": "保本"},
    "locking":  {"min_profit": 0.10,  "trailing_offset": "profit_5", "label": "锁定利润"},
    "trailing": {"min_profit": 0.20,  "trailing_offset": "dynamic",  "label": "追踪模式"},
}

RSI_OVERBOUGHT = 0.85        # 严重超买阈值
VIX_SPIKE_PCT = 0.30         # VIX 跳升 30% 触发预警
TRAILING_ATR_MULTIPLIER = 2.0


def get_highest_price_since_entry(db, symbol: str, entry_date: str) -> float | None:
    """获取从入场以来的最高价。"""
    row = db.conn.execute(
        """SELECT MAX(high) as highest FROM prices
           WHERE symbol = ? AND date >= ?""",
        (symbol, entry_date),
    ).fetchone()
    return row["highest"] if row and row["highest"] else None


def get_entry_date(db, symbol: str) -> str | None:
    """Get the entry date of the CURRENT position (not just the most recent buy).
    Fix #6: If a position was sold and re-bought, we need the date when the current
    holdings were acquired, not just any historical buy."""
    # Get current holding quantity
    holding = db.conn.execute(
        "SELECT quantity FROM holdings WHERE symbol = ? AND active = 1 AND quantity > 0",
        (symbol,),
    ).fetchone()
    if not holding:
        return None

    current_qty = holding["quantity"]

    # Walk back through trades to find when this position was entered
    trades = db.conn.execute(
        """SELECT direction, quantity, timestamp FROM trades
           WHERE symbol = ? ORDER BY timestamp DESC""",
        (symbol,),
    ).fetchall()

    accumulated = 0
    for t in trades:
        if t["direction"] == "buy":
            accumulated += t["quantity"]
            if accumulated >= current_qty:
                return t["timestamp"][:10]
        else:
            accumulated -= t["quantity"]

    # Fallback: most recent buy
    if trades:
        for t in reversed(trades):
            if t["direction"] == "buy":
                return t["timestamp"][:10]
    return None


def get_profit_state(cost_price: float, current_price: float) -> str:
    """根据当前盈利判断处于哪个止盈状态。"""
    profit_pct = (current_price - cost_price) / cost_price
    if profit_pct >= 0.20:
        return "trailing"
    elif profit_pct >= 0.10:
        return "locking"
    elif profit_pct >= 0.05:
        return "breakeven"
    else:
        return "entry"


def calculate_trailing_stop(
    cost_price: float,
    current_price: float,
    highest_price: float | None,
    atr: float | None,
    state: str,
) -> float | None:
    """
    根据当前状态计算追踪止损位。
    只上移不下移。

    Returns: trailing stop price, or None if not applicable
    """
    if state == "entry":
        # 刚买入，用 ATR 止损（由 smart_risk 处理）
        if atr:
            return cost_price - (2.0 * atr)
        return None

    if state == "breakeven":
        # 盈利 > 5%，止损上移到成本价（保本）
        return cost_price

    if state == "locking":
        # 盈利 > 10%，止损上移到盈利 5% 的位置
        return cost_price * 1.05

    if state == "trailing":
        # 盈利 > 20%，追踪模式：距最高价回撤 2×ATR
        if highest_price and atr:
            return highest_price - (TRAILING_ATR_MULTIPLIER * atr)
        elif highest_price:
            # fallback: 距最高价回撤 5%
            return highest_price * 0.95
        return None

    return None


def check_rsi_overbought(db, symbol: str) -> tuple[bool, float | None]:
    """检查 RSI 是否严重超买。"""
    row = db.conn.execute(
        """SELECT factor_value FROM factors
           WHERE symbol = ? AND factor_name = 'rsi'
           ORDER BY date DESC LIMIT 1""",
        (symbol,),
    ).fetchone()

    if row and row["factor_value"] > RSI_OVERBOUGHT:
        return True, row["factor_value"]
    return False, row["factor_value"] if row else None


def check_vix_spike(db) -> tuple[bool, float | None, float | None]:
    """检查 VIX 是否突然跳升 >30%。"""
    rows = db.conn.execute(
        """SELECT indicator_value FROM market_indicators
           WHERE indicator_name = 'vix'
           ORDER BY date DESC LIMIT 2""",
    ).fetchall()

    if len(rows) < 2:
        return False, None, None

    current_vix = rows[0]["indicator_value"]
    prev_vix = rows[1]["indicator_value"]

    if prev_vix > 0 and (current_vix - prev_vix) / prev_vix > VIX_SPIKE_PCT:
        return True, current_vix, prev_vix
    return False, current_vix, prev_vix


def check_sector_weakness(db, symbol: str) -> tuple[bool, list[str]]:
    """
    检查同板块其他龙头是否开始下跌。
    如果同板块 2 只以上股票下跌 > 3%，视为板块走弱。

    Returns: (weak, [weak_symbols])
    """
    # 获取该标的的板块
    sector_row = db.conn.execute(
        """SELECT sector FROM screener_history
           WHERE symbol = ? AND sector IS NOT NULL AND sector != ''
           ORDER BY run_time DESC LIMIT 1""",
        (symbol,),
    ).fetchone()

    if not sector_row:
        return False, []

    sector = sector_row["sector"]

    # 找同板块的其他活跃标的
    peers = db.conn.execute(
        """SELECT DISTINCT symbol FROM screener_history
           WHERE sector = ? AND symbol != ?
           ORDER BY run_time DESC LIMIT 10""",
        (sector, symbol),
    ).fetchall()

    weak_peers = []
    for peer in peers:
        peer_sym = peer["symbol"]
        # 检查该 peer 最近的价格变化
        prices = db.conn.execute(
            """SELECT close FROM prices WHERE symbol = ? ORDER BY date DESC LIMIT 2""",
            (peer_sym,),
        ).fetchall()
        if len(prices) >= 2:
            change = (prices[0]["close"] - prices[1]["close"]) / prices[1]["close"]
            if change < -0.03:  # 跌超 3%
                weak_peers.append(peer_sym)

    return len(weak_peers) >= 2, weak_peers


def check_take_profit(
    db, symbol: str, cost_price: float, current_price: float, atr: float | None = None
) -> dict:
    """
    综合止盈检查。

    Returns dict with:
      - state: 当前盈利状态
      - trailing_stop: 追踪止损位
      - profit_pct: 当前盈利
      - action: 建议动作 (hold / reduce / sell)
      - reasons: 触发原因列表
    """
    from analysis.smart_risk import calculate_atr

    result = {
        "symbol": symbol,
        "cost_price": cost_price,
        "current_price": current_price,
        "profit_pct": (current_price - cost_price) / cost_price,
        "state": "entry",
        "trailing_stop": None,
        "action": "hold",
        "reasons": [],
    }

    # 如果还在亏损，不需要止盈检查
    if result["profit_pct"] < 0:
        return result

    # 1. 状态机判断
    state = get_profit_state(cost_price, current_price)
    result["state"] = state
    state_info = PROFIT_STATES[state]

    # 2. 计算追踪止损位
    highest = get_highest_price_since_entry(db, symbol, get_entry_date(db, symbol) or "2025-01-01")
    if atr is None:
        atr = calculate_atr(db, symbol)

    trailing = calculate_trailing_stop(cost_price, current_price, highest, atr, state)
    result["trailing_stop"] = trailing

    # 3. 检查是否跌破追踪止损
    if trailing and current_price <= trailing:
        result["action"] = "sell"
        result["reasons"].append(
            f"跌破追踪止损位 ${trailing:.2f} (状态: {state_info['label']})"
        )
        return result

    # 4. RSI 超买预警 → 建议减仓 50%
    rsi_overbought, rsi_val = check_rsi_overbought(db, symbol)
    if rsi_overbought:
        result["action"] = "reduce"
        result["reasons"].append(f"RSI 严重超买 ({rsi_val:.3f} > {RSI_OVERBOUGHT}), 建议减仓 50%")

    # 5. VIX 跳升 → 建议减仓
    vix_spike, cur_vix, prev_vix = check_vix_spike(db)
    if vix_spike and state in ("locking", "trailing"):
        if result["action"] != "sell":
            result["action"] = "reduce"
        result["reasons"].append(f"VIX 跳升 ({prev_vix:.1f} → {cur_vix:.1f}), 建议减仓")

    # 6. 板块走弱 → 建议减仓
    sector_weak, weak_peers = check_sector_weakness(db, symbol)
    if sector_weak and state in ("locking", "trailing"):
        if result["action"] != "sell":
            result["action"] = "reduce"
        result["reasons"].append(f"同板块走弱 ({', '.join(weak_peers)}), 建议减仓")

    return result
