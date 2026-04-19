"""
Extended Factors — 因子库扩展 (v5.3)

从 5 个因子扩展到 20+ 个，分 3 批次：
Phase 1: MACD, Bollinger, OBV, ADX, Historical Vol
Phase 2: VWAP, MFI, Accumulation/Distribution
Phase 3: 分析师评级、内部人交易（预留接口）

所有因子输出归一化到 [-1, 1] 区间。
"""

import logging
import math

logger = logging.getLogger("extended_factors")


def compute_macd(prices, fast=12, slow=26, signal=9):
    """MACD 指标。Fix #8: O(n) computation instead of O(n²)."""
    if len(prices) < slow + signal:
        return None

    # Compute EMA fast and slow in O(n)
    ema_fast_vals = []
    ema_slow_vals = []

    # EMA fast
    multiplier_fast = 2 / (fast + 1)
    ema_f = sum(prices[:fast]) / fast
    ema_fast_vals.append(ema_f)
    for i in range(fast, len(prices)):
        ema_f = (prices[i] - ema_f) * multiplier_fast + ema_f
        ema_fast_vals.append(ema_f)

    # EMA slow
    multiplier_slow = 2 / (slow + 1)
    ema_s = sum(prices[:slow]) / slow
    ema_slow_vals.append(ema_s)
    for i in range(slow, len(prices)):
        ema_s = (prices[i] - ema_s) * multiplier_slow + ema_s
        ema_slow_vals.append(ema_s)

    # MACD line (align by index: fast EMA starts at index fast-1, slow at slow-1)
    offset = slow - fast
    macd_values = []
    for i in range(len(ema_slow_vals)):
        macd_values.append(ema_fast_vals[i + offset] - ema_slow_vals[i])

    if len(macd_values) < signal:
        return None

    # Signal line EMA
    multiplier_signal = 2 / (signal + 1)
    sig_line = sum(macd_values[:signal]) / signal
    for i in range(signal, len(macd_values)):
        sig_line = (macd_values[i] - sig_line) * multiplier_signal + sig_line

    histogram = macd_values[-1] - sig_line

    # Normalize
    recent = macd_values[-20:]
    max_abs = max(abs(m) for m in recent) if recent else 1
    if max_abs == 0:
        max_abs = 1

    return max(-1, min(1, histogram / max_abs))


def compute_bollinger(prices, period=20, num_std=2):
    """布林带。返回价格相对位置，归一化到 [-1, 1]。
    -1 = 在下轨下方（超卖），0 = 在中轨，+1 = 在上轨上方（超买）
    """
    if len(prices) < period:
        return None

    recent = prices[-period:]
    sma = sum(recent) / period
    variance = sum((p - sma) ** 2 for p in recent) / period
    std = math.sqrt(variance)

    if std == 0:
        return 0.0

    upper = sma + num_std * std
    lower = sma - num_std * std
    current = prices[-1]

    # Position within band: -1 (below lower) to +1 (above upper)
    position = (current - sma) / (num_std * std)
    return max(-1, min(1, position))


def compute_obv(prices, volumes, period=20):
    """On-Balance Volume 趋势。返回 OBV 的 Z-score，归一化到 [-1, 1]。"""
    if len(prices) < 2 or len(volumes) < 2:
        return None

    obv_values = [0]
    for i in range(1, len(prices)):
        if prices[i] > prices[i - 1]:
            obv_values.append(obv_values[-1] + volumes[i])
        elif prices[i] < prices[i - 1]:
            obv_values.append(obv_values[-1] - volumes[i])
        else:
            obv_values.append(obv_values[-1])

    if len(obv_values) < period:
        return None

    recent_obv = obv_values[-period:]
    mean_obv = sum(recent_obv) / period
    variance = sum((v - mean_obv) ** 2 for v in recent_obv) / period
    std = math.sqrt(variance) if variance > 0 else 1

    if std == 0:
        return 0.0

    z_score = (obv_values[-1] - mean_obv) / std
    return max(-1, min(1, z_score / 3))  # 3 std = ~1


def compute_adx(highs, lows, closes, period=14):
    """Average Directional Index。返回 ADX 归一化到 [0, 1]。
    0 = 无趋势，1 = 强趋势
    """
    if len(highs) < period + 1:
        return None

    plus_dm = []
    minus_dm = []
    true_ranges = []

    for i in range(1, len(highs)):
        high_diff = highs[i] - highs[i - 1]
        low_diff = lows[i - 1] - lows[i]

        if high_diff > low_diff and high_diff > 0:
            plus_dm.append(high_diff)
        else:
            plus_dm.append(0)

        if low_diff > high_diff and low_diff > 0:
            minus_dm.append(low_diff)
        else:
            minus_dm.append(0)

        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    # Smoothed averages
    atr = sum(true_ranges[-period:]) / period
    if atr == 0:
        return 0.0

    plus_di = (sum(plus_dm[-period:]) / period) / atr * 100
    minus_di = (sum(minus_dm[-period:]) / period) / atr * 100

    dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100 if (plus_di + minus_di) > 0 else 0

    # Normalize ADX (0-100) to [0, 1]
    return dx / 100


def compute_historical_volatility(prices, period=20):
    """历史波动率。返回年化波动率 / 100，归一化到 [0, 1]。"""
    if len(prices) < period + 1:
        return None

    returns = []
    for i in range(len(prices) - period, len(prices)):
        if prices[i - 1] > 0:
            returns.append((prices[i] - prices[i - 1]) / prices[i - 1])

    if len(returns) < 2:
        return None

    mean_ret = sum(returns) / len(returns)
    variance = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)
    daily_vol = math.sqrt(variance)
    annual_vol = daily_vol * math.sqrt(252)

    # Normalize: typical vol range 0.1-1.0, cap at 1.0
    return max(0, min(1, annual_vol))


def compute_vwap(highs, lows, closes, volumes):
    """VWAP (Volume Weighted Average Price)。
    返回 (close - vwap) / vwap，归一化到 [-1, 1]。
    正值表示价格在 VWAP 之上（看涨），负值表示在 VWAP 之下（看跌）
    """
    if len(closes) < 2 or len(volumes) < 2:
        return None

    typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
    cum_vol_price = sum(tp * v for tp, v in zip(typical_prices, volumes))
    cum_vol = sum(volumes)

    if cum_vol == 0:
        return 0.0

    vwap = cum_vol_price / cum_vol

    if vwap == 0:
        return 0.0

    diff_pct = (closes[-1] - vwap) / vwap
    return max(-1, min(1, diff_pct * 10))  # Scale up for sensitivity


def compute_mfi(highs, lows, closes, volumes, period=14):
    """Money Flow Index。类似于 RSI 但包含成交量。归一化到 [-1, 1]。"""
    if len(closes) < period + 1:
        return None

    typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
    money_flows = [tp * v for tp, v in zip(typical_prices, volumes)]

    positive_flow = 0
    negative_flow = 0

    for i in range(len(typical_prices) - period, len(typical_prices)):
        if typical_prices[i] > typical_prices[i - 1]:
            positive_flow += money_flows[i]
        else:
            negative_flow += money_flows[i]

    if negative_flow == 0:
        return 1.0

    mfi = 100 - (100 / (1 + positive_flow / negative_flow))

    # Normalize MFI (0-100) to [-1, 1]
    return (mfi - 50) / 50


def compute_accumulation_distribution(highs, lows, closes, volumes, period=20):
    """Accumulation/Distribution Line 趋势。返回 Z-score，归一化到 [-1, 1]。"""
    if len(closes) < 2 or len(volumes) < 2:
        return None

    ad_values = [0]
    for i in range(1, len(closes)):
        if highs[i] != lows[i]:
            clv = ((closes[i] - lows[i]) - (highs[i] - closes[i])) / (highs[i] - lows[i])
        else:
            clv = 0
        ad_values.append(ad_values[-1] + clv * volumes[i])

    if len(ad_values) < period:
        return None

    recent = ad_values[-period:]
    mean_ad = sum(recent) / period
    variance = sum((v - mean_ad) ** 2 for v in recent) / period
    std = math.sqrt(variance) if variance > 0 else 1

    if std == 0:
        return 0.0

    z = (ad_values[-1] - mean_ad) / std
    return max(-1, min(1, z / 3))


# ─── 辅助函数 ─────────────────────────────────────────────

def _ema(data, period):
    """计算指数移动平均。"""
    if len(data) < period:
        return sum(data) / len(data) if data else 0

    multiplier = 2 / (period + 1)
    ema = sum(data[:period]) / period

    for i in range(period, len(data)):
        ema = (data[i] - ema) * multiplier + ema

    return ema


# ─── 因子注册表 ─────────────────────────────────────────────

EXTENDED_FACTORS = {
    "macd": compute_macd,
    "bollinger": compute_bollinger,
    "obv": compute_obv,
    "adx": compute_adx,
    "historical_volatility": compute_historical_volatility,
    "vwap": compute_vwap,
    "mfi": compute_mfi,
    "accumulation_distribution": compute_accumulation_distribution,
}


def compute_extended_factors(db, symbol: str) -> dict:
    """计算所有扩展因子并存储。"""
    rows = db.conn.execute(
        "SELECT date, open, high, low, close, volume FROM prices "
        "WHERE symbol = ? ORDER BY date ASC",
        (symbol,),
    ).fetchall()

    if len(rows) < 30:
        return {}

    closes = [r["close"] for r in rows if r["close"]]
    highs = [r["high"] for r in rows if r["high"]]
    lows = [r["low"] for r in rows if r["low"]]
    volumes = [r["volume"] for r in rows if r["volume"]]
    opens = [r["open"] for r in rows if r["open"]]

    today = rows[-1]["date"]
    results = {}

    for name, func in EXTENDED_FACTORS.items():
        try:
            if name == "macd":
                value = func(closes)
            elif name == "bollinger":
                value = func(closes)
            elif name == "obv":
                value = func(closes, volumes)
            elif name == "adx":
                value = func(highs, lows, closes)
            elif name == "historical_volatility":
                value = func(closes)
            elif name == "vwap":
                value = func(highs, lows, closes, volumes)
            elif name == "mfi":
                value = func(highs, lows, closes, volumes)
            elif name == "accumulation_distribution":
                value = func(highs, lows, closes, volumes)
            else:
                value = None

            if value is not None:
                results[name] = value
                db.conn.execute(
                    "INSERT OR REPLACE INTO factors (symbol, factor_name, factor_value, date) "
                    "VALUES (?, ?, ?, ?)",
                    (symbol, name, round(value, 6), today),
                )
        except Exception as e:
            logger.debug(f"Factor {name} for {symbol} failed: {e}")

    return results
