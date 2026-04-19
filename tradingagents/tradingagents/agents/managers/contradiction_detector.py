"""Contradiction detection agent.

Identifies conflicts between different analysts' outputs before
the Portfolio Manager makes the final decision.
"""

from tradingagents.agents.utils.agent_utils import get_language_instruction


def create_contradiction_detector(llm):
    def contradiction_detector_node(state) -> dict:
        trader_plan = state.get("trader_investment_plan", "")
        investment_plan = state.get("investment_plan", "")
        risk_debate_history = state.get("risk_debate_state", {}).get("history", "")
        market_report = state.get("market_report", "")
        sentiment_report = state.get("sentiment_report", "")
        news_report = state.get("news_report", "")
        fundamentals_report = state.get("fundamentals_report", "")

        prompt = f"""You are a contradiction detection agent. Your job is to review inputs from multiple analysis sources and identify any contradictions, conflicts, or inconsistencies between them.

**Inputs:**

**Trader's Plan:**
{trader_plan}

**Research Manager's Investment Plan:**
{investment_plan}

**Risk Analysts Debate Summary:**
{risk_debate_history}

**Market/Technical Report:**
{market_report}

**Sentiment Report:**
{sentiment_report}

**News Report:**
{news_report}

**Fundamentals Report:**
{fundamentals_report}

**Task:**
1. Compare the directional signals across all inputs (bullish vs bearish vs neutral).
2. Identify specific contradictions — e.g., if fundamentals say "undervalued" but technicals say "overbought", or if the Trader says BUY but the Risk debate is predominantly bearish.
3. Assess the severity: "low" if inputs mostly agree, "medium" if minor conflicts exist, "high" if core dimensions (direction, valuation, momentum) directly conflict.
4. Summarize the contradictions and their implications.

**Required Output Structure:**
1. **Contradictions Found**: Yes/No
2. **Severity**: low / medium / high
3. **Details**: List each specific contradiction with the conflicting sources
4. **Summary**: Brief overview of the overall consistency assessment
5. **Recommendation**: If severity is "high", recommend reducing conviction or adopting a watch/hold stance{get_language_instruction()}"""

        response = llm.invoke(prompt)

        return {"contradiction_report": response.content}

    return contradiction_detector_node
