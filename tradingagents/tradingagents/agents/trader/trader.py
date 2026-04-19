import functools

from tradingagents.agents.utils.agent_utils import build_instrument_context
from tradingagents.strategies.dynamic_thresholds import DynamicThresholds
from tradingagents.dataflows.config import get_config


def create_trader(llm, memory):
    def trader_node(state, name):
        company_name = state["company_of_interest"]
        instrument_context = build_instrument_context(company_name)
        investment_plan = state["investment_plan"]
        market_research_report = state["market_report"]
        sentiment_report = state["sentiment_report"]
        news_report = state["news_report"]
        fundamentals_report = state["fundamentals_report"]

        dt = DynamicThresholds(get_config())
        curr_situation = f"{market_research_report}\n\n{sentiment_report}\n\n{news_report}\n\n{fundamentals_report}"
        past_memories = memory.get_memories(curr_situation, n_matches=dt.get_memory_matches("trader"))

        past_memory_str = ""
        if past_memories:
            for i, rec in enumerate(past_memories, 1):
                past_memory_str += rec["recommendation"] + "\n\n"
        else:
            past_memory_str = "No past memories found."

        context = {
            "role": "user",
            "content": f"Based on a comprehensive analysis by a team of analysts, here is an investment plan tailored for {company_name}. {instrument_context} This plan incorporates insights from current technical market trends, macroeconomic indicators, and social media sentiment. Use this plan as a foundation for evaluating your next trading decision.\n\nProposed Investment Plan: {investment_plan}\n\nLeverage these insights to make an informed and strategic decision.",
        }

        messages = [
            {
                "role": "system",
                "content": f"""You are a trading agent analyzing market data to make investment decisions. Based on your analysis, provide a structured trading plan with specific entry parameters. Apply lessons from past decisions to strengthen your analysis. Here are reflections from similar situations you traded in and the lessons learned: {past_memory_str}

Your output should include:
1. A detailed analysis of the investment opportunity
2. A structured trading plan in JSON format at the end of your response:
{{
  "direction": "BUY" or "HOLD" or "SELL",
  "confidence": 0-100,
  "position_size_pct": percentage of total portfolio (0-100),
  "entry_price_range": "price range for entry",
  "stop_loss": specific stop-loss price or null,
  "take_profit": specific take-profit price or null,
  "time_horizon": "short" or "medium" or "long",
  "rationale": "brief summary of key reasons"
}}

Always conclude your response with 'FINAL TRANSACTION PROPOSAL: **BUY/HOLD/SELL**' to confirm your recommendation.""",
            },
            context,
        ]

        result = llm.invoke(messages)

        return {
            "messages": [result],
            "trader_investment_plan": result.content,
            "sender": name,
        }

    return functools.partial(trader_node, name="Trader")
