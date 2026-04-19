# TradingAgents/graph/signal_processing.py
# Fix #12: Rule-based signal extraction — no LLM needed for simple keyword extraction.

import re
from typing import Any


# Rule-based rating extraction keywords
_BUY_KEYWORDS = {"buy", "strong buy", "bullish", "outperform", "accumulation"}
_SELL_KEYWORDS = {"sell", "strong sell", "bearish", "underperform"}
_HOLD_KEYWORDS = {"hold", "neutral", "maintain", "wait", "wait-and-see", "unchanged"}
_OVERWEIGHT_KEYWORDS = {"overweight", "outperform"}
_UNDERWEIGHT_KEYWORDS = {"underweight", "underperform"}


def _extract_rating_rule_based(text: str) -> str:
    """Extract rating from text using keyword matching. No LLM needed."""
    text_lower = text.lower()

    # 1. Try explicit rating first
    for rating in ["strong buy", "strong sell", "buy", "sell", "overweight", "underweight", "hold", "neutral"]:
        if rating in text_lower:
            if rating == "strong buy":
                return "BUY"
            elif rating == "strong sell":
                return "SELL"
            elif rating == "neutral":
                return "HOLD"
            return rating.upper()

    # 2. Keyword matching (BUY/SELL priority)
    for word in _BUY_KEYWORDS:
        if word in text_lower:
            return "BUY"
    for word in _SELL_KEYWORDS:
        if word in text_lower:
            return "SELL"
    for word in _OVERWEIGHT_KEYWORDS:
        if word in text_lower:
            return "OVERWEIGHT"
    for word in _UNDERWEIGHT_KEYWORDS:
        if word in text_lower:
            return "UNDERWEIGHT"
    for word in _HOLD_KEYWORDS:
        if word in text_lower:
            return "HOLD"

    # 3. JSON fallback
    json_match = re.search(r'"rating"\s*:\s*"([^"]+)"', text, re.IGNORECASE)
    if json_match:
        return json_match.group(1).upper()

    return "HOLD"


class SignalProcessor:
    """Processes trading signals to extract actionable decisions.

    Fix #12: Rule-based extraction by default; LLM only as fallback.
    Saves ~1 LLM call per signal analysis.
    """

    def __init__(self, quick_thinking_llm: Any = None):
        """Initialize. LLM is optional — only used as fallback."""
        self.quick_thinking_llm = quick_thinking_llm

    def process_signal(self, full_signal: str) -> str:
        """
        Process a full trading signal to extract the core decision.

        Fix #12: Rule-based first (instant, no API). LLM fallback only if
        rule-based says HOLD on a long/detailed signal (potential false negative).
        """
        rating = _extract_rating_rule_based(full_signal)

        # Fallback: HOLD on long signal might be missed nuance
        if rating == "HOLD" and len(full_signal) > 500 and self.quick_thinking_llm:
            try:
                messages = [
                    ("system",
                     "Extract the trading decision from the report. "
                     "Output exactly one word: BUY, OVERWEIGHT, HOLD, UNDERWEIGHT, or SELL."),
                    ("human", full_signal),
                ]
                llm_rating = self.quick_thinking_llm.invoke(messages).content.strip().upper()
                if llm_rating in ("BUY", "SELL", "OVERWEIGHT", "UNDERWEIGHT", "HOLD"):
                    rating = llm_rating
            except Exception:
                pass

        return rating
