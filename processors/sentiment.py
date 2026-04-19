"""
US Data Hub — Sentiment Processor (v3, Phase 2)
================================================

Phase 2 改造:
  - 集成 LLM Router，自动路由到百炼 qwen3.6-flash
  - 保留规则打分作为 fallback
  - 支持双端点降级

Uses LLM (百炼 qwen3.6-flash) to score sentiment of news/posts.
Batch mode: sends multiple headlines per LLM call to reduce API count by ~10x.
Falls back to rule-based scoring if LLM unavailable.
"""

import json
import logging
import os
from typing import List, Dict

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)


def _rule_based_sentiment(text: str) -> float:
    """Rule-based sentiment scoring (fallback)."""
    text_lower = text.lower()

    positive_words = [
        "buy", "upgrade", "outperform", "bullish", "rise", "gain", "surge",
        "beat", "growth", "profit", "record", "rally", "soar", "breakthrough",
        "acquisition", "expansion", "innovation", "strong", "exceed",
        "raise", "increase", "higher", "positive", "optimistic",
    ]
    negative_words = [
        "sell", "downgrade", "underperform", "bearish", "fall", "drop", "crash",
        "miss", "loss", "decline", "sue", "lawsuit", "fraud", "warning",
        "risk", "cut", "reduce", "lower", "negative", "pessimistic",
        "layoff", "bankruptcy", "recession", "concern", "fear",
    ]

    pos = sum(1 for w in positive_words if w in text_lower)
    neg = sum(1 for w in negative_words if w in text_lower)
    total = pos + neg

    if total == 0:
        return 0.0
    return round((pos - neg) / total * 0.8, 3)


def _llm_sentiment_batch(items: List[Dict]) -> List[float]:
    """
    Phase 2: 通过 LLM Router 批量打分，自动路由到百炼 qwen3.6-flash。
    Returns list of scores aligned with input items.
    """
    if not items:
        return []

    # Build batched prompt: numbered list of headlines
    texts = []
    for i, item in enumerate(items, 1):
        title = item.get("title", "")
        content = item.get("content_text", "")
        if isinstance(item.get("content"), str):
            content = item["content"]
        text = f"{title}. {content}" if content else title
        texts.append(f"{i}. {text[:200]}")

    numbered_list = "\n".join(texts)

    system_prompt = (
        "You are a financial sentiment analyzer. Given a numbered list of news headlines, "
        "return ONLY a JSON object with format: {\"scores\": [float, float, ...]}. "
        "Each score between -1.0 (very bearish) and 1.0 (very bullish), 0.0 is neutral. "
        "Return exactly one score per item, in order. No explanations."
    )

    user_prompt = f"Score the sentiment of each headline:\n\n{numbered_list}"

    try:
        # Phase 2: 使用 LLM Router 自动路由到百炼 qwen3.6-flash
        from analysis.llm_router import LLMRouter
        router = LLMRouter()
        result = router.invoke("sentiment_analysis", [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ])

        if not result.get("success"):
            logger.warning(f"LLM Router 调用失败: {result.get('error')}，使用规则打分")
            return [_rule_based_sentiment(texts[i]) for i in range(len(items))]

        content = result["content"].strip()
        # Parse JSON, handle potential markdown code blocks
        if content.startswith("```"):
            content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        data = json.loads(content)

        # Handle both {"scores": [...]} and direct array
        if isinstance(data, list):
            scores = data
        elif isinstance(data, dict):
            scores = data.get("scores", [])
            # If the dict contains number values directly (not a scores key)
            if not scores and all(isinstance(v, (int, float)) for v in data.values()):
                scores = list(data.values())
        else:
            scores = []

        # Validate and clamp
        result_scores = []
        for s in scores[:len(items)]:
            if isinstance(s, (int, float)):
                result_scores.append(max(-1.0, min(1.0, float(s))))
            else:
                result_scores.append(0.0)  # fallback for non-numeric
        # Pad with rule-based scores if LLM returned fewer
        while len(result_scores) < len(items):
            result_scores.append(_rule_based_sentiment(texts[len(result_scores)]))
        return result_scores
    except Exception as e:
        logger.warning(f"Batch LLM sentiment failed: {e}, falling back to rule-based")
        return [_rule_based_sentiment(texts[i]) for i in range(len(items))]


def _rule_based_sentiment(text: str) -> float:
    """Rule-based sentiment scoring (primary method now)."""
    text_lower = text.lower()
    positive_words = [
        "buy", "upgrade", "outperform", "bullish", "rise", "gain", "surge",
        "beat", "growth", "profit", "record", "rally", "soar", "breakthrough",
        "acquisition", "expansion", "innovation", "strong", "exceed",
        "raise", "increase", "higher", "positive", "optimistic",
    ]
    negative_words = [
        "sell", "downgrade", "underperform", "bearish", "fall", "drop", "crash",
        "miss", "loss", "decline", "sue", "lawsuit", "fraud", "warning",
        "risk", "cut", "reduce", "lower", "negative", "pessimistic",
        "layoff", "bankruptcy", "recession", "concern", "fear",
    ]
    pos = sum(1 for w in positive_words if w in text_lower)
    neg = sum(1 for w in negative_words if w in text_lower)
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total * 0.8, 3)


def batch_score_sentiment(items: List[Dict]) -> List[Dict]:
    """
    Score sentiment for a batch of items.
    Adds 'sentiment_score' to each item dict.

    Fix #14: Rule-based sentiment as PRIMARY method (saves LLM calls).
    LLM only used as fallback when rule-based returns 0.0 on non-trivial text.
    """
    if not items:
        return items

    # Preprocess texts
    texts = []
    for item in items:
        title = item.get("title", "")
        content = item.get("content_text", "")
        if isinstance(item.get("content"), str):
            content = item["content"]
        text = f"{title}. {content}" if content else title
        texts.append(text)

    # Rule-based scoring for all items
    for i, item in enumerate(items):
        if not texts[i].strip():
            item["sentiment_score"] = 0.0
        else:
            score = _rule_based_sentiment(texts[i])
            item["sentiment_score"] = score
            logger.debug(f"Sentiment (rule-based): {texts[i][:50]}... → {score:.2f}")

    return items
