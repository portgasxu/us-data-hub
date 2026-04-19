#!/usr/bin/env python3
"""
US Data Hub — Dynamic Threshold Analyzer
==========================================
LLM-based dynamic confidence threshold calculator.

Instead of fixed thresholds (sell≥70%, buy≥80%), this module asks LLM
to assess each signal's context and return a personalized threshold.

Input:
  • Market state (VIX, major index trend, sector rotation)
  • Stock characteristics (volatility, liquidity, momentum)
  • Signal source reliability (LLM / screener / sentiment / factors)
  • Current position concentration
  • Daily trade count (fatigue factor)
  • Signal strength and direction

Output:
  • Personalized threshold for this specific signal
  • Confidence adjustment coefficient
  • Risk adjustment rationale
"""

import sys
import os
import json
import logging
from datetime import datetime
from typing import Dict, Optional, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)


DEFAULT_THRESHOLDS = {
    "buy": 0.80,
    "sell": 0.70,
}


def compute_threshold_rule_based(direction: str, market_ctx: Dict, stock_ctx: Dict,
                                  signal: Dict) -> Dict:
    """
    Fix #15: Rule-based dynamic threshold — no LLM needed for most signals.
    Uses the same factors as the LLM version but with deterministic formulas.

    Factors considered:
    1. Position concentration (>50% → lower sell threshold)
    2. VIX regime (high → raise buy threshold, lower sell threshold)
    3. Signal reliability (LLM > factors > screener)
    4. Trade fatigue (daily count → raise threshold)
    5. Trend confirmation (momentum + sentiment aligned → lower threshold)
    """
    base = DEFAULT_THRESHOLDS.get(direction, 0.75)
    confidence = signal.get("confidence", 0.5)
    source = signal.get("source", "unknown")

    adjustments = []

    # 1. Position concentration
    pos_pct_str = stock_ctx.get("position_pct", "N/A")
    try:
        pos_pct = float(pos_pct_str.replace("%", "")) / 100 if pos_pct_str != "N/A" else 0.3
        if pos_pct > 0.50 and direction == "sell":
            # Heavy concentration → make it easier to sell
            adj = -0.10
            base += adj
            adjustments.append(f"集中度{pos_pct:.0%}→卖阈值{adj:+.0%}")
        elif pos_pct > 0.35 and direction == "buy":
            # Already heavy → harder to buy more
            adj = +0.05
            base += adj
            adjustments.append(f"集中度{pos_pct:.0%}→买阈值{adj:+.0%}")
    except (ValueError, AttributeError):
        pass

    # 2. VIX regime
    vix_str = market_ctx.get("vix_level", "N/A")
    try:
        vix = float(vix_str) if vix_str != "N/A" else 20
        if vix > 30:
            if direction == "buy":
                adj = +0.05
                base += adj
                adjustments.append(f"VIX高({vix:.0f})→防守{adj:+.0%}")
            else:
                adj = -0.05
                base += adj
                adjustments.append(f"VIX高({vix:.0f})→加速卖{adj:+.0%}")
        elif vix < 15:
            if direction == "buy":
                adj = -0.03
                base += adj
                adjustments.append(f"VIX低({vix:.0f})→放宽{adj:+.0%}")
    except (ValueError, AttributeError):
        pass

    # 3. Signal reliability
    source_reliability = {
        "llm_analysis": 0.00,    # Most reliable → no adjustment
        "holding_monitor": -0.03, # LLM-based → slightly easier
        "factors": +0.02,        # Factor-based → slightly harder
        "screener": +0.03,       # Screener → harder
        "sentiment": +0.05,      # Sentiment only → hardest
    }
    adj = source_reliability.get(source, +0.02)
    base += adj
    if adj != 0:
        adjustments.append(f"来源{source}{adj:+.0%}")

    # 4. Trade fatigue
    daily_count = market_ctx.get("daily_trade_count", 0)
    if daily_count >= 5:
        adj = 0.02 * min(daily_count - 4, 4)  # +2% per trade after 5, max +8%
        base += adj
        adjustments.append(f"交易疲劳({daily_count}笔){adj:+.0%}")

    # 5. Trend confirmation
    momentum_str = stock_ctx.get("momentum_5d", "N/A")
    sentiment_str = stock_ctx.get("recent_sentiment", "N/A")
    try:
        mom = float(momentum_str.replace("%", "").replace("+", "")) if momentum_str != "N/A" else 0
        sent = float(sentiment_str.split()[0]) if sentiment_str != "N/A" else 0
        # Aligned: both positive for buy, both negative for sell
        if direction == "buy" and mom > 0 and sent > 0:
            adj = -0.03
            base += adj
            adjustments.append(f"趋势共振{adj:+.0%}")
        elif direction == "sell" and mom < 0 and sent < 0:
            adj = -0.03
            base += adj
            adjustments.append(f"趋势共振{adj:+.0%}")
    except (ValueError, AttributeError):
        pass

    # Clamp to hard limits
    if direction == "buy":
        threshold = max(HARD_LIMITS["buy_min"], min(HARD_LIMITS["buy_max"], base))
    elif direction == "sell":
        threshold = max(HARD_LIMITS["sell_min"], min(HARD_LIMITS["sell_max"], base))
    else:
        threshold = base

    passes = confidence >= threshold

    return {
        "threshold": round(threshold, 3),
        "adjusted_confidence": round(confidence, 3),
        "passes": passes,
        "rationale": "规则计算: " + "; ".join(adjustments) if adjustments else "规则计算: 无特殊调整",
        "original_threshold": DEFAULT_THRESHOLDS.get(direction, 0.75),
        "threshold_delta": round(threshold - DEFAULT_THRESHOLDS.get(direction, 0.75), 3),
    }

# Fixed thresholds for extreme cases (override LLM)
HARD_LIMITS = {
    "buy_min": 0.55,   # Never go below 55% for buy
    "buy_max": 0.95,   # Never go above 95% for buy
    "sell_min": 0.50,  # Never go below 50% for sell
    "sell_max": 0.90,  # Never go above 90% for sell
}


def _get_market_context(db) -> Dict:
    """Gather current market context for LLM analysis."""
    ctx = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "vix_level": "N/A",
        "major_trend": "N/A",
        "trading_session": "N/A",
        "daily_trade_count": 0,
    }

    # VIX level
    try:
        row = db.conn.execute(
            """SELECT close FROM prices WHERE symbol = '^VIX'
               ORDER BY date DESC LIMIT 1"""
        ).fetchone()
        if row and row[0]:
            vix = float(row[0])
            ctx["vix_level"] = f"{vix:.1f}"
            if vix > 30:
                ctx["market_regime"] = "high_volatility_panic"
            elif vix > 20:
                ctx["market_regime"] = "elevated_volatility"
            elif vix > 15:
                ctx["market_regime"] = "normal"
            else:
                ctx["market_regime"] = "low_volatility_complacent"
    except Exception:
        ctx["vix_level"] = "N/A"
        ctx["market_regime"] = "unknown"

    # Trading session
    from datetime import datetime as dt
    hour = dt.now().hour
    if 9 <= hour < 15:
        ctx["trading_session"] = "asia_session"
    elif 15 <= hour < 21:
        ctx["trading_session"] = "pre_market"
    elif 21 <= hour or hour < 4:
        ctx["trading_session"] = "us_market_open"
    else:
        ctx["trading_session"] = "after_hours"

    # Daily trade count
    today = dt.now().strftime("%Y-%m-%d")
    try:
        row = db.conn.execute(
            "SELECT COUNT(*) FROM trades WHERE timestamp >= ?",
            (f"{today} 00:00:00",)
        ).fetchone()
        ctx["daily_trade_count"] = row[0] if row else 0
    except Exception:
        ctx["daily_trade_count"] = 0

    # Recent portfolio performance
    try:
        rows = db.conn.execute(
            """SELECT h.symbol, h.quantity, h.cost_price,
                      (SELECT close FROM prices p
                       WHERE p.symbol = h.symbol AND p.close > 0
                       ORDER BY p.date DESC LIMIT 1) as current_price
               FROM holdings h WHERE h.active = 1 AND h.quantity > 0"""
        ).fetchall()
        total_pnl = 0
        for r in rows:
            if r[3]:
                total_pnl += (r[3] - r[2]) * r[1]
        ctx["portfolio_pnl"] = f"+{total_pnl:.0f}" if total_pnl > 0 else f"{total_pnl:.0f}"
        ctx["portfolio_pnl_pct"] = f"+{total_pnl/abs(total_pnl)*100:.0f}%" if total_pnl != 0 else "0%"
    except Exception:
        pass

    return ctx


def _get_stock_context(db, symbol: str) -> Dict:
    """Gather stock-specific context for LLM analysis."""
    ctx = {
        "symbol": symbol,
        "volatility_20d": "N/A",
        "momentum_5d": "N/A",
        "position_pct": "N/A",
        "holding_days": "N/A",
        "recent_sentiment": "N/A",
    }

    import math
    from datetime import datetime as dt, timedelta

    # Volatility and momentum
    try:
        rows = db.conn.execute(
            """SELECT close FROM prices WHERE symbol = ?
               ORDER BY date DESC LIMIT 20""",
            (symbol,)
        ).fetchall()
        if rows and len(rows) >= 5:
            prices = [r[0] for r in rows if r[0] and r[0] > 0]
            if len(prices) >= 5:
                # 20-day volatility
                returns = [(prices[i-1] - prices[i]) / prices[i] for i in range(1, len(prices))]
                mean_r = sum(returns) / len(returns)
                vol = math.sqrt(sum((r - mean_r)**2 for r in returns) / len(returns))
                ctx["volatility_20d"] = f"{vol*100:.2f}%"

                # 5-day momentum
                if len(prices) >= 5:
                    mom = (prices[0] - prices[4]) / prices[4] * 100
                    ctx["momentum_5d"] = f"+{mom:.1f}%" if mom > 0 else f"{mom:.1f}%"

                ctx["current_price"] = f"${prices[0]:.2f}"
    except Exception:
        pass

    # Position concentration
    try:
        row = db.conn.execute(
            """SELECT h.quantity, (SELECT close FROM prices p
               WHERE p.symbol = h.symbol AND p.close > 0
               ORDER BY p.date DESC LIMIT 1) as price
               FROM holdings h WHERE h.symbol = ? AND h.active = 1""",
            (symbol,)
        ).fetchone()
        if row and row[0] and row[1]:
            pos_value = row[0] * row[1]

            total = db.conn.execute(
                """SELECT COALESCE(SUM(h2.quantity * (
                    SELECT close FROM prices p2 WHERE p2.symbol = h2.symbol
                    AND p2.close > 0 ORDER BY p2.date DESC LIMIT 1)), 0)
                    FROM holdings h2 WHERE h2.active = 1"""
            ).fetchone()[0]

            if total > 0:
                ctx["position_pct"] = f"{pos_value/total*100:.1f}%"
                ctx["position_value"] = f"${pos_value:.0f}"
            ctx["holding_quantity"] = str(row[0])
    except Exception:
        pass

    # Recent sentiment
    try:
        two_days = (dt.now() - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        row = db.conn.execute(
            """SELECT AVG(sentiment_score), COUNT(*) FROM data_points
               WHERE symbol = ? AND sentiment_score IS NOT NULL AND timestamp >= ?""",
            (symbol, two_days)
        ).fetchone()
        if row and row[0] is not None:
            ctx["recent_sentiment"] = f"{row[0]:.2f} ({row[1]} articles)"
    except Exception:
        pass

    return ctx


def analyze_threshold(llm, db, signal: Dict) -> Dict:
    """
    Ask LLM to compute dynamic threshold for a signal.

    Args:
        llm: LangChain LLM instance
        db: Database instance
        signal: Signal dict with symbol, direction, confidence, source, etc.

    Returns:
        {
            "threshold": float,        # Dynamic threshold (0.0-1.0)
            "adjusted_confidence": float,  # Confidence after adjustment
            "passes": bool,            # Whether signal passes dynamic threshold
            "rationale": str,          # LLM explanation
        }
    """
    symbol = signal["symbol"]
    direction = signal["direction"]
    confidence = signal.get("confidence", 0.5)
    source = signal.get("source", "unknown")
    strength = signal.get("strength", 0.5)
    reason = signal.get("reason", "")

    market = _get_market_context(db)
    stock = _get_stock_context(db, symbol)

    # Default threshold (fallback if LLM fails)
    default_threshold = DEFAULT_THRESHOLDS.get(direction, 0.75)

    prompt = f"""你是一个专业的量化交易风控分析师。请根据以下信息，为该交易信号计算一个动态置信度阈值。

## 市场环境
- 时间: {market['timestamp']}
- VIX: {market['vix_level']}
- 市场状态: {market.get('market_regime', 'unknown')}
- 交易时段: {market['trading_session']}
- 组合盈亏: {market.get('portfolio_pnl', 'N/A')}
- 今日已交易: {market['daily_trade_count']} 笔

## 标的信息
- 标的: {stock['symbol']}
- 当前价: {stock.get('current_price', 'N/A')}
- 20日波动率: {stock['volatility_20d']}
- 5日动量: {stock['momentum_5d']}
- 仓位占比: {stock['position_pct']}
- 持仓数量: {stock.get('holding_quantity', 'N/A')}
- 近期情绪: {stock['recent_sentiment']}

## 交易信号
- 方向: {direction.upper()}
- 原始置信度: {confidence:.0%}
- 信号来源: {source}
- 信号强度: {strength:.2f}
- 理由: {reason}

## 固定基准阈值
- 买入: 80%
- 卖出: 70%

## 你的任务

请综合考虑以下因素，给出该信号应使用的动态阈值：

1. **集中度风险**：仓位占比过高时，卖出阈值应降低（加速减仓）
2. **市场状态**：高VIX时买入阈值应提高（防守为主），卖出阈值可降低
3. **信号来源可靠性**：LLM持仓分析 > 因子信号 > 选股评分
4. **交易疲劳**：今日交易次数多时，后续信号阈值应提高
5. **趋势确认**：动量和情绪方向一致时，可适当降低阈值

请严格只输出以下 JSON 格式，不要任何其他文字：
{{
  "threshold": 0.XX,
  "adjusted_confidence": 0.XX,
  "passes": true/false,
  "rationale": "一句话解释原因"
}}

注意：
- threshold 范围：买入 55%-95%，卖出 50%-90%
- adjusted_confidence 可以在原始置信度基础上微调（±5%以内）
- passes = adjusted_confidence >= threshold"""

    try:
        response = llm.invoke(prompt)
        content = response.content.strip()

        # Extract JSON from response
        start = content.find("{")
        end = content.rfind("}") + 1
        if start >= 0 and end > start:
            result = json.loads(content[start:end])

            threshold = float(result.get("threshold", default_threshold))
            adj_conf = float(result.get("adjusted_confidence", confidence))

            # Clamp to hard limits
            if direction == "buy":
                threshold = max(HARD_LIMITS["buy_min"], min(HARD_LIMITS["buy_max"], threshold))
            elif direction == "sell":
                threshold = max(HARD_LIMITS["sell_min"], min(HARD_LIMITS["sell_max"], threshold))

            passes = adj_conf >= threshold

            return {
                "threshold": round(threshold, 3),
                "adjusted_confidence": round(adj_conf, 3),
                "passes": passes,
                "rationale": result.get("rationale", "LLM 分析完成"),
                "original_threshold": default_threshold,
                "threshold_delta": threshold - default_threshold,
            }
        else:
            logger.warning(f"LLM response parse failed for {symbol}: {content[:200]}")
            return _fallback_result(confidence, default_threshold, direction)

    except Exception as e:
        logger.warning(f"LLM threshold analysis failed for {symbol}: {e}")
        return _fallback_result(confidence, default_threshold, direction)


def _fallback_result(confidence: float, default_threshold: float, direction: str) -> Dict:
    """Fallback when LLM fails — use fixed threshold."""
    return {
        "threshold": default_threshold,
        "adjusted_confidence": confidence,
        "passes": confidence >= default_threshold,
        "rationale": f"LLM 分析失败，使用固定阈值 {default_threshold:.0%}",
        "original_threshold": default_threshold,
        "threshold_delta": 0,
    }


def batch_analyze(llm, db, signals: List[Dict]) -> List[Dict]:
    """
    Analyze thresholds for multiple signals.
    Processes signals one by one to avoid rate limits.
    """
    results = []
    for signal in signals:
        logger.info(f"Dynamic threshold analysis: {signal['symbol']} {signal['direction']}")
        result = analyze_threshold(llm, db, signal)
        result["symbol"] = signal["symbol"]
        result["direction"] = signal["direction"]
        result["source"] = signal.get("source", "")
        result["original_confidence"] = signal.get("confidence", 0)
        results.append(result)
    return results


def main():
    """Test the dynamic threshold analyzer."""
    import argparse
    from storage import Database
    from langchain_openai import ChatOpenAI

    parser = argparse.ArgumentParser(description="Dynamic Threshold Analyzer")
    parser.add_argument("--symbol", default="", help="Test specific symbol")
    args = parser.parse_args()

    db = Database()
    api_key = os.getenv("OPENAI_API_KEY", "YOUR_API_KEY_HERE")
    llm = ChatOpenAI(
        model="qwen3.6-plus",
        base_url="https://coding.dashscope.aliyuncs.com/v1",
        api_key=api_key,
        temperature=0.3,
        request_timeout=90,
    )

    # Test signals
    test_signals = [
        {
            "symbol": "NVDA",
            "direction": "sell",
            "confidence": 0.85,
            "source": "holding_monitor",
            "strength": 0.7,
            "reason": "仓位过重(63%)，集中度风险极高",
        },
        {
            "symbol": "AMZN",
            "direction": "buy",
            "confidence": 0.78,
            "source": "screener",
            "strength": 0.65,
            "reason": "综合评分 0.65，动量强劲",
        },
        {
            "symbol": "TSLA",
            "direction": "buy",
            "confidence": 0.70,
            "source": "factors",
            "strength": 0.6,
            "reason": "RSI 超卖，均值回归机会",
        },
        {
            "symbol": "MSFT",
            "direction": "sell",
            "confidence": 0.60,
            "source": "factors",
            "strength": 0.5,
            "reason": "RSI 超买，回调风险",
        },
    ]

    if args.symbol:
        test_signals = [s for s in test_signals if s["symbol"] == args.symbol]
        if not test_signals:
            # Try to get real signal from signal hub
            from analysis.signal_hub import SignalHub
            hub = SignalHub(db)
            all_signals = hub.collect_all()
            test_signals = [s.to_dict() for s in hub.get_tradable_signals() if s.symbol == args.symbol]

    if not test_signals:
        print("No signals to analyze")
        db.close()
        return

    print(f"\n{'='*80}")
    print("Dynamic Threshold Analysis")
    print(f"{'='*80}\n")

    for sig in test_signals:
        result = analyze_threshold(llm, db, sig)
        emoji = "✅" if result["passes"] else "🚫"
        delta_str = f"+{result['threshold_delta']*100:.0f}%" if result['threshold_delta'] > 0 else f"{result['threshold_delta']*100:.0f}%"

        print(f"{emoji} {sig['symbol']:8s} {sig['direction'].upper():5s}")
        print(f"   原始: 置信度={sig['confidence']:.0%} | 固定阈值={DEFAULT_THRESHOLDS[sig['direction']]:.0%}")
        print(f"   动态: 置信度={result['adjusted_confidence']:.0%} | 动态阈值={result['threshold']:.0%} ({delta_str})")
        print(f"   结果: {'通过' if result['passes'] else '拦截'}")
        print(f"   原因: {result['rationale']}")
        print()

    db.close()


if __name__ == "__main__":
    main()
