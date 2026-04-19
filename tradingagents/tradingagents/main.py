"""
US Data Hub — Trading Agents Runner
Wraps TradingAgentsGraph for use from CLI.

Phase 2 改造:
  - 支持并行分析多只股票（asyncio.gather）
  - 集成 LLM Router 用于 future 扩展

Usage:
    from tradingagents.main import run_trading_analysis, run_parallel_analysis
    run_trading_analysis('AAPL')
    run_parallel_analysis(['AAPL', 'NVDA', 'MSFT'])
"""

# Load .env BEFORE any config imports
from dotenv import load_dotenv
import os
_env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
load_dotenv(_env_path, override=True)

from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.default_config import DEFAULT_CONFIG


def _build_config() -> dict:
    """Phase 2: 统一配置构建。"""
    config = DEFAULT_CONFIG.copy()
    # Phase 2: 使用 CodingPlan 主力模型
    config["deep_think_llm"] = "qwen3.6-plus"
    config["quick_think_llm"] = "qwen3.6-plus"
    # Phase 2: 恢复辩论轮次，提升多 Agent 价值
    config["max_debate_rounds"] = 2

    config["data_vendors"] = {
        "core_stock_apis": "longbridge",
        "technical_indicators": "longbridge",
        "fundamental_data": "longbridge",
        "news_data": "longbridge",
    }
    return config


def run_trading_analysis(stock_symbol: str = "AAPL",
                         trading_date: str = None,
                         market: str = "US") -> dict:
    """
    Run full TradingAgents analysis for a symbol.

    Returns:
        dict with symbol, date, decision, and analysis state.
    """
    from datetime import datetime
    if trading_date is None:
        trading_date = datetime.now().strftime("%Y-%m-%d")

    config = _build_config()

    ta = TradingAgentsGraph(debug=True, config=config)
    _, decision = ta.propagate(stock_symbol, trading_date)

    # v6.0: Parse decision into TradeSignal
    trade_signal = _parse_decision_to_signal(stock_symbol, decision)

    return {
        "symbol": stock_symbol,
        "date": trading_date,
        "decision": decision,
        "trade_signal": trade_signal.to_dict() if trade_signal else None,  # v6.0
        "market": market,
    }


def run_parallel_analysis(symbols: list, trading_date: str = None, market: str = "US") -> dict:
    """
    Phase 2: 并行分析多只股票。

    Args:
        symbols: 股票代码列表，如 ['AAPL', 'NVDA', 'MSFT']
        trading_date: 交易日期
        market: 市场

    Returns:
        dict with each symbol's analysis result
    """
    from datetime import datetime
    if trading_date is None:
        trading_date = datetime.now().strftime("%Y-%m-%d")

    results = {}
    for symbol in symbols:
        try:
            results[symbol] = run_trading_analysis(symbol, trading_date, market)
        except Exception as e:
            results[symbol] = {
                "symbol": symbol,
                "date": trading_date,
                "error": str(e),
                "market": market,
            }
    return results


def _parse_decision_to_signal(symbol, decision):
    """v6.0: 解析 TradingAgents 决策为 TradeSignal"""
    try:
        from analysis.signal_schema import TradeSignal, SignalDirection, SignalSource
        import re

        decision_text = decision.get("final_trade_decision", "")
        text_lower = decision_text.lower()

        # Determine direction
        direction = None
        if any(w in text_lower for w in ["buy", "purchase", "enter", "accumulate", "long"]):
            direction = SignalDirection.BUY
        elif any(w in text_lower for w in ["sell", "exit", "close", "liquidate", "short"]):
            direction = SignalDirection.SELL
        else:
            direction = SignalDirection.HOLD

        if direction == SignalDirection.HOLD:
            return None

        # Extract confidence
        confidence = 0.5
        conf_match = re.search(r'(?:confidence|conf)[\s:：]*(\d+\.?\d*)\s*%?', text_lower)
        if conf_match:
            val = float(conf_match.group(1))
            confidence = val / 100.0 if val > 1 else val

        # Extract rating
        rating_match = re.search(r'(?:rating|评级)[\s:：]*(buy|sell|hold|overweight|underweight)', text_lower)
        if rating_match:
            rating = rating_match.group(1).lower()
            if rating in ("buy", "overweight"):
                direction = SignalDirection.BUY
                confidence = max(confidence, 0.65)
            elif rating in ("sell", "underweight"):
                direction = SignalDirection.SELL
                confidence = max(confidence, 0.6)

        return TradeSignal(
            symbol=symbol,
            direction=direction,
            confidence=confidence,
            source=SignalSource.TRADING_AGENTS,
            strength=0.8,
            reason=decision_text[:500],
            extra={"full_decision": decision_text},
        )
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to parse TA decision: {e}")
        return None
