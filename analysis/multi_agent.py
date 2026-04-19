"""
Multi-Agent Decision — 多 Agent 协同决策 (v5.3)

4 个专家 Agent 分工：
- Market_Analyst: 宏观判断（VIX/板块/大盘）
- Stock_Analyst: 个股分析（基本面+技术面）
- Risk_Manager: 独立风控
- Portfolio_Manager: 最终决策，综合以上意见
"""

import logging
from datetime import datetime

logger = logging.getLogger("multi_agent")


class AgentOpinion:
    """Agent 意见。"""
    def __init__(self, agent: str, symbol: str, direction: str | None,
                 confidence: float, rationale: str, recommendation: str):
        self.agent = agent
        self.symbol = symbol
        self.direction = direction  # buy | sell | hold | None
        self.confidence = confidence
        self.rationale = rationale
        self.recommendation = recommendation  # strong_buy | buy | hold | sell | strong_sell

    def to_dict(self) -> dict:
        return {
            "agent": self.agent,
            "symbol": self.symbol,
            "direction": self.direction,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "recommendation": self.recommendation,
        }


def market_analyst(db, symbol: str) -> AgentOpinion:
    """市场分析 Agent — 宏观判断。"""
    from analysis.market_regime import detect_market_regime

    regime = detect_market_regime(db)
    regime_name = regime["regime"]
    regime_conf = regime["confidence"]

    # 检查同板块表现
    sector = db.conn.execute(
        "SELECT sector FROM screener_history WHERE symbol = ? "
        "AND sector IS NOT NULL ORDER BY run_time DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    sector_info = "未知板块"
    if sector:
        sector_info = sector["sector"]

    # 检查 VIX
    vix_row = db.conn.execute(
        "SELECT indicator_value FROM market_indicators WHERE indicator_name = 'vix' "
        "ORDER BY date DESC LIMIT 1"
    ).fetchone()
    vix = vix_row["indicator_value"] if vix_row else None

    rationale = f"市场状态: {regime_name} (置信度 {regime_conf:.0%}), 板块: {sector_info}"
    if vix is not None:
        rationale += f", VIX: {vix:.1f}"

    # 熊市降低买入信心
    if regime_name == "bear":
        return AgentOpinion(
            "Market_Analyst", symbol, "hold", 0.6,
            rationale, "hold"
        )
    elif regime_name == "bull":
        return AgentOpinion(
            "Market_Analyst", symbol, "buy", 0.7,
            rationale, "buy"
        )
    else:
        return AgentOpinion(
            "Market_Analyst", symbol, None, 0.5,
            rationale + ", 震荡市建议精选个股", "hold"
        )


def stock_analyst(db, symbol: str) -> AgentOpinion:
    """个股分析 Agent — 基本面+技术面。"""
    # 获取因子
    factors = {}
    rows = db.conn.execute(
        "SELECT factor_name, factor_value FROM factors "
        "WHERE symbol = ? ORDER BY date DESC LIMIT 15",
        (symbol,),
    ).fetchall()

    for r in rows:
        factors[r["factor_name"]] = r["factor_value"]

    # 获取最新价格
    price_row = db.conn.execute(
        "SELECT close FROM prices WHERE symbol = ? ORDER BY date DESC LIMIT 1",
        (symbol,),
    ).fetchone()
    current_price = price_row["close"] if price_row else 0

    # 评分
    score = 0
    reasons = []

    # 质量因子
    quality = factors.get("quality", 0)
    if quality > 0.3:
        score += 2
        reasons.append(f"质量优秀 ({quality:.2f})")
    elif quality < -0.3:
        score -= 2
        reasons.append(f"质量差 ({quality:.2f})")

    # 动量
    momentum = factors.get("momentum", 0)
    if momentum > 0.3:
        score += 1
        reasons.append(f"动量正向 ({momentum:.2f})")
    elif momentum < -0.3:
        score -= 1
        reasons.append(f"动量负向 ({momentum:.2f})")

    # RSI
    rsi = factors.get("rsi", 0.5)
    if rsi > 0.8:
        score -= 1
        reasons.append(f"RSI 超买 ({rsi:.2f})")
    elif rsi < 0.2:
        score += 1
        reasons.append(f"RSI 超卖 ({rsi:.2f})")

    # 扩展因子
    macd = factors.get("macd", 0)
    if macd > 0.3:
        score += 1
        reasons.append(f"MACD  bullish ({macd:.2f})")
    elif macd < -0.3:
        score -= 1
        reasons.append(f"MACD bearish ({macd:.2f})")

    bb = factors.get("bollinger", 0)
    if bb < -0.5:
        score += 1
        reasons.append(f"布林带超卖 ({bb:.2f})")
    elif bb > 0.5:
        score -= 1
        reasons.append(f"布林带超买 ({bb:.2f})")

    # 归一化分数到 [-1, 1]
    norm_score = max(-1, min(1, score / 5))
    confidence = abs(norm_score) * 0.7 + 0.3

    if norm_score > 0.2:
        direction = "buy"
        recommendation = "buy" if norm_score > 0.5 else "hold"
    elif norm_score < -0.2:
        direction = "sell"
        recommendation = "sell" if norm_score < -0.5 else "hold"
    else:
        direction = None
        recommendation = "hold"

    rationale = " | ".join(reasons) if reasons else "因子无明显信号"

    return AgentOpinion(
        "Stock_Analyst", symbol, direction, round(confidence, 2),
        rationale, recommendation
    )


def risk_manager(db, symbol: str, current_position: dict = None) -> AgentOpinion:
    """风控 Agent — 独立风控判断。"""
    reasons = []
    risk_score = 0  # 越高越危险

    # 检查持仓集中度
    if current_position:
        weight = current_position.get("weight", 0)
        if weight > 0.35:
            risk_score += 3
            reasons.append(f"集中度超标 ({weight:.0%} > 35%)")
        elif weight > 0.25:
            risk_score += 1
            reasons.append(f"集中度偏高 ({weight:.0%})")

        # 检查盈亏
        pnl = current_position.get("pnl_pct", 0)
        if pnl < -0.10:
            risk_score += 2
            reasons.append(f"亏损超 10% ({pnl:.1%})")
        elif pnl > 0.20:
            reasons.append(f"盈利良好 ({pnl:.1%})，注意止盈")

    # 检查波动率
    vol_row = db.conn.execute(
        "SELECT factor_value FROM factors WHERE symbol = ? AND factor_name = 'volatility' "
        "ORDER BY date DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if vol_row and vol_row["factor_value"] > 0.7:
        risk_score += 1
        reasons.append(f"波动率高 ({vol_row['factor_value']:.2f})")

    # 检查扩展因子
    hv_row = db.conn.execute(
        "SELECT factor_value FROM factors WHERE symbol = ? AND factor_name = 'historical_volatility' "
        "ORDER BY date DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if hv_row and hv_row["factor_value"] > 0.5:
        risk_score += 1
        reasons.append(f"历史波动率高 ({hv_row['factor_value']:.2f})")

    # 判断
    if risk_score >= 4:
        direction = "sell"
        recommendation = "strong_sell"
        confidence = 0.8
    elif risk_score >= 2:
        direction = "sell"
        recommendation = "sell"
        confidence = 0.6
    else:
        direction = None
        recommendation = "hold"
        confidence = 0.5

    rationale = " | ".join(reasons) if reasons else "无明显风险"

    return AgentOpinion(
        "Risk_Manager", symbol, direction, round(confidence, 2),
        rationale, recommendation
    )


def portfolio_manager(opinions: list[AgentOpinion], current_signal) -> dict:
    """
    组合经理 Agent — 最终决策。

    综合所有 Agent 意见，做出最终决策。
    """
    # 加权投票
    weights = {
        "Market_Analyst": 0.25,
        "Stock_Analyst": 0.35,
        "Risk_Manager": 0.40,  # 风控权重最高
    }

    buy_score = 0
    sell_score = 0
    rationales = []

    for opinion in opinions:
        w = weights.get(opinion.agent, 0.2)
        rationale_detail = f"{opinion.agent}: {opinion.recommendation} (c={opinion.confidence:.0%}) - {opinion.rationale}"
        rationales.append(rationale_detail)

        if opinion.recommendation in ("buy", "strong_buy"):
            buy_score += w * opinion.confidence
            if opinion.recommendation == "strong_buy":
                buy_score += w * 0.2
        elif opinion.recommendation in ("sell", "strong_sell"):
            sell_score += w * opinion.confidence
            if opinion.recommendation == "strong_sell":
                sell_score += w * 0.2

    # 最终决策
    net_score = buy_score - sell_score

    if net_score > 0.3:
        final_decision = "buy"
        final_confidence = min(0.95, 0.5 + net_score)
    elif net_score < -0.3:
        final_decision = "sell"
        final_confidence = min(0.95, 0.5 + abs(net_score))
    else:
        final_decision = "hold"
        final_confidence = 0.5

    return {
        "decision": final_decision,
        "confidence": round(final_confidence, 2),
        "buy_score": round(buy_score, 3),
        "sell_score": round(sell_score, 3),
        "net_score": round(net_score, 3),
        "opinions": [o.to_dict() for o in opinions],
        "rationale": " | ".join(rationales),
    }


def run_multi_agent_decision(db, symbol: str, current_signal, current_position: dict = None) -> dict:
    """运行完整的多 Agent 决策流程。"""
    opinions = [
        market_analyst(db, symbol),
        stock_analyst(db, symbol),
        risk_manager(db, symbol, current_position),
    ]

    return portfolio_manager(opinions, current_signal)
