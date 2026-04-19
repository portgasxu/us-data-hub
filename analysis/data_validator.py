"""
Data Validator — 数据质量校验层 (v5.3)

数据入库前校验：
- 价格异常检测 (close < 0, close = 0, 异常涨跌)
- 缺失值填充 (停牌日自动填充前一日价格)
- 数据延迟告警 (超过 30 分钟未更新)
"""

import logging
from datetime import datetime

logger = logging.getLogger("data_validator")

# ─── 校验规则 ─────────────────────────────────────────────

MAX_DAILY_CHANGE = 0.50  # 单日最大涨跌 50%
MIN_PRICE = 0.01         # 最低有效价格
MAX_PRICE = 100000       # 最高有效价格


def validate_price(symbol: str, date: str, price: float, prev_price: float = None) -> tuple[bool, str | None]:
    """校验单个价格数据。返回 (is_valid, reason)"""
    if price is None:
        return False, "价格为空"

    if price < MIN_PRICE:
        return False, f"价格 {price} 低于最小值 {MIN_PRICE}"

    if price > MAX_PRICE:
        return False, f"价格 {price} 高于最大值 {MAX_PRICE}"

    if prev_price and prev_price > 0:
        change = abs(price - prev_price) / prev_price
        if change > MAX_DAILY_CHANGE:
            return False, f"单日涨跌幅 {change:.0%} 超过阈值 {MAX_DAILY_CHANGE:.0%}"

    return True, None


def validate_ohlc(symbol: str, date: str, open_p: float, high: float, low: float, close: float) -> tuple[bool, str | None]:
    """校验 OHLC 数据完整性。"""
    if any(v is None for v in [open_p, high, low, close]):
        return False, "OHLC 数据不完整"

    if low > high:
        return False, f"最低价 {low} > 最高价 {high}"

    if close < low or close > high:
        return False, f"收盘价 {close} 超出 [{low}, {high}] 范围"

    if open_p < low or open_p > high:
        return False, f"开盘价 {open_p} 超出 [{low}, {high}] 范围"

    return True, None


def fill_missing_prices(db, symbol: str, start_date: str = None, end_date: str = None):
    """填充停牌日的缺失价格（使用前一日收盘价）。"""
    import sqlite3

    query = "SELECT date, close FROM prices WHERE symbol = ? ORDER BY date ASC"
    params = [symbol]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    rows = db.conn.execute(query, params).fetchall()

    if not rows:
        return 0

    filled = 0
    for i in range(1, len(rows)):
        expected_date = _next_trading_day(rows[i - 1]["date"])
        if expected_date and rows[i]["date"] != expected_date:
            # 缺失交易日，填充
            db.conn.execute(
                "INSERT OR IGNORE INTO prices (symbol, date, open, high, low, close, volume) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (symbol, expected_date,
                 rows[i - 1]["close"], rows[i - 1]["close"],
                 rows[i - 1]["close"], rows[i - 1]["close"], 0),
            )
            filled += 1

    if filled > 0:
        db.conn.commit()
        logger.info(f"[{symbol}] 填充 {filled} 个缺失交易日")

    return filled


def check_data_freshness(db, symbol: str = None, max_age_hours: float = 30) -> list[dict]:
    """检查数据新鲜度。超过 max_age_hours 未更新则告警。"""
    if symbol:
        row = db.conn.execute(
            "SELECT MAX(date) as last_date FROM prices WHERE symbol = ?", (symbol,)
        ).fetchone()
        symbols_to_check = [(symbol, row["last_date"])] if row else []
    else:
        rows = db.conn.execute(
            "SELECT symbol, MAX(date) as last_date FROM prices GROUP BY symbol"
        ).fetchall()
        symbols_to_check = [(r["symbol"], r["last_date"]) for r in rows]

    alerts = []
    now = datetime.now()

    for sym, last_date in symbols_to_check:
        if last_date:
            try:
                last_dt = datetime.strptime(last_date, "%Y-%m-%d")
                age_hours = (now - last_dt).total_seconds() / 3600
                if age_hours > max_age_hours:
                    alerts.append({
                        "symbol": sym,
                        "last_date": last_date,
                        "age_hours": round(age_hours, 1),
                        "message": f"{sym} 数据已 {age_hours:.0f} 小时未更新",
                    })
            except ValueError:
                alerts.append({
                    "symbol": sym,
                    "last_date": last_date,
                    "age_hours": None,
                    "message": f"{sym} 日期格式异常: {last_date}",
                })

    return alerts


def _next_trading_day(date_str: str) -> str | None:
    """计算下一个交易日（简化版，不考虑节假日）。"""
    from datetime import timedelta, datetime

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        next_dt = dt + timedelta(days=1)

        # 跳过周末
        while next_dt.weekday() >= 5:  # 5=Saturday, 6=Sunday
            next_dt += timedelta(days=1)

        return next_dt.strftime("%Y-%m-%d")
    except ValueError:
        return None
