"""
Market Regime — 市场状态检测器 (v5.3)

检测市场处于牛市/熊市/震荡市，用于动态调整信号权重。
基于 VIX、均线系统、动量和波动率综合判断。
"""

import logging

logger = logging.getLogger("market_regime")


def detect_market_regime(db, symbol: str = "SPY") -> dict:
    """
    检测市场状态。Fix #7: Fallback to QQQ or IWM if SPY not available.

    返回:
        regime: "bull" | "bear" | "ranging"
        confidence: 0-1
        indicators: 各指标详情
    """
    indicators = {}

    # 1. VIX 指标
    vix_row = db.conn.execute(
        "SELECT indicator_value FROM market_indicators "
        "WHERE indicator_name = 'vix' ORDER BY date DESC LIMIT 1"
    ).fetchone()

    if vix_row and vix_row["indicator_value"]:
        vix = vix_row["indicator_value"]
        indicators["vix"] = vix
        if vix < 15:
            indicators["vix_signal"] = "bull"
        elif vix > 30:
            indicators["vix_signal"] = "bear"
        else:
            indicators["vix_signal"] = "neutral"
    else:
        indicators["vix"] = None
        indicators["vix_signal"] = "neutral"

    # 2. 大盘均线 — Fix #7: Try SPY, QQQ, IWM in order
    benchmark = None
    for sym in [symbol, "QQQ", "IWM"]:
        row = db.conn.execute(
            "SELECT COUNT(*) as cnt FROM prices WHERE symbol = ?", (sym,)
        ).fetchone()
        if row and row["cnt"] > 50:
            benchmark = sym
            break

    if benchmark:
        prices = db.conn.execute(
            "SELECT close FROM prices WHERE symbol = ? ORDER BY date DESC LIMIT 200",
            (benchmark,),
        ).fetchall()

        if prices and len(prices) >= 50:
            closes = [p["close"] for p in reversed(prices) if p["close"]]
            current = closes[-1]

            # MA20
            ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else current
            # MA50
            ma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else current
            # MA200
            ma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else ma50

            indicators["benchmark"] = benchmark
            indicators["current_price"] = current
            indicators["ma20"] = ma20
            indicators["ma50"] = ma50
            indicators["ma200"] = ma200

            # 均线排列判断
            if current > ma20 > ma50 > ma200:
                indicators["trend_signal"] = "bull"
            elif current < ma20 < ma50 < ma200:
                indicators["trend_signal"] = "bear"
            else:
                indicators["trend_signal"] = "ranging"
        else:
            indicators["trend_signal"] = "neutral"
    else:
        indicators["trend_signal"] = "neutral"

    # 3. 动量（RSI 平均）
    rsi_rows = db.conn.execute(
        "SELECT factor_value FROM factors WHERE factor_name = 'rsi' "
        "ORDER BY date DESC LIMIT 10"
    ).fetchall()

    if rsi_rows:
        avg_rsi = sum(r["factor_value"] for r in rsi_rows) / len(rsi_rows)
        indicators["avg_rsi"] = avg_rsi
        if avg_rsi > 0.6:
            indicators["momentum_signal"] = "bull"
        elif avg_rsi < 0.4:
            indicators["momentum_signal"] = "bear"
        else:
            indicators["momentum_signal"] = "ranging"
    else:
        indicators["momentum_signal"] = "neutral"

    # 综合判断（投票制）
    signals = [
        indicators.get("vix_signal", "neutral"),
        indicators.get("trend_signal", "neutral"),
        indicators.get("momentum_signal", "neutral"),
    ]

    bull_count = signals.count("bull")
    bear_count = signals.count("bear")
    ranging_count = signals.count("ranging") + signals.count("neutral")

    if bull_count >= 2:
        regime = "bull"
        confidence = bull_count / 3
    elif bear_count >= 2:
        regime = "bear"
        confidence = bear_count / 3
    else:
        regime = "ranging"
        confidence = ranging_count / 3

    return {
        "regime": regime,
        "confidence": round(confidence, 2),
        "indicators": indicators,
    }


def get_regime_weight_adjustments(regime: str) -> dict:
    """根据市场状态返回信号权重调整。"""
    if regime == "bull":
        return {
            "momentum": 0.35,      # 牛市追涨
            "value": 0.05,          # 价值因子降权
            "quality": 0.15,
            "rsi": 0.15,
            "volatility": 0.10,
            "macd": 0.10,           # 趋势跟踪加强
            "bollinger": 0.10,
        }
    elif regime == "bear":
        return {
            "momentum": 0.10,      # 熊市降权动量
            "value": 0.20,          # 重视估值
            "quality": 0.25,        # 质量因子最重要
            "rsi": 0.15,
            "volatility": 0.15,
            "macd": 0.05,
            "bollinger": 0.10,
        }
    else:  # ranging
        return {
            "momentum": 0.10,      # 震荡市动量没用
            "value": 0.15,
            "quality": 0.15,
            "rsi": 0.20,            # RSI 均值回归
            "volatility": 0.10,
            "macd": 0.15,           # MACD 震荡策略
            "bollinger": 0.15,      # 布林带高抛低吸
        }
