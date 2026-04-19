"""Dynamic threshold and parameter calculator for TradingAgents.

Uses LLM-driven analysis to generate adaptive thresholds for:
- Signal correctness evaluation (per-ticker volatility-based)
- VIX regime interpretation (market-state-aware)
- News search query generation (context-aware)
- Analysis time windows (strategy-aware)

All values fall back to config defaults when LLM is unavailable.
"""

import os
import json
import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

# LLM config for dynamic calculations (uses same provider as trading system)
_LLM_CONFIG = {
    "provider": os.environ.get("LLM_PROVIDER", "openai"),
    "model": os.environ.get("DYNAMIC_PARAM_LLM", "qwen3.6-plus"),
    "base_url": os.environ.get("BACKEND_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
    "api_key_env": "DASHSCOPE_API_KEY",
}


def _call_llm(system_prompt: str, user_prompt: str, timeout: int = 15) -> Optional[dict]:
    """Call LLM synchronously and return parsed JSON or None on failure."""
    try:
        from openai import OpenAI
        api_key = os.environ.get(_LLM_CONFIG["api_key_env"]) or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.debug("No API key for dynamic threshold LLM, using defaults")
            return None

        client = OpenAI(
            api_key=api_key,
            base_url=_LLM_CONFIG["base_url"],
            timeout=timeout,
        )

        response = client.chat.completions.create(
            model=_LLM_CONFIG["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            response_format={"type": "json_object"},
        )

        raw = response.choices[0].message.content
        return json.loads(raw)
    except Exception as e:
        logger.warning(f"LLM dynamic threshold call failed: {e}")
        return None


class DynamicThresholds:
    """Calculates dynamic trading parameters using LLM and config fallbacks."""

    def __init__(self, config: dict = None):
        self.config = config or {}
        # Cache to avoid repeated LLM calls
        self._cache: dict = {}
        self._cache_ttl: int = self.config.get("dynamic_param_cache_seconds", 3600)

    # ──────────────────────────────────────────────
    # 1. Signal correctness threshold (replaces 0.5%)
    # ──────────────────────────────────────────────

    def get_signal_threshold(self, ticker: str = "", volatility_pct: Optional[float] = None) -> float:
        """Return adaptive threshold for prediction correctness evaluation.

        Uses LLM to calculate threshold based on ticker volatility profile.
        Falls back to config default.

        Args:
            ticker: Ticker symbol for context
            volatility_pct: Historical volatility % (e.g. 2.5 for 2.5% daily vol)

        Returns:
            Threshold as decimal (e.g. 0.005 = 0.5%)
        """
        cache_key = f"signal_threshold:{ticker}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return float(cached)

        # LLM-driven calculation
        if ticker:
            vol_info = f"Daily volatility: {volatility_pct}%" if volatility_pct else "Volatility unknown"
            result = _call_llm(
                system_prompt="You are a quantitative analyst. Given a ticker and its volatility profile, return a JSON with the optimal threshold for evaluating prediction correctness. The threshold should be proportional to volatility: low vol stocks need smaller thresholds (0.001-0.003), high vol stocks need larger (0.01-0.02).",
                user_prompt=f"Ticker: {ticker}\n{vol_info}\n\nReturn JSON: {{\"threshold\": <decimal>, \"rationale\": \"<brief reason>\"}}",
            )
            if result and "threshold" in result:
                threshold = float(result["threshold"])
                # Sanity bounds: 0.1% to 3%
                threshold = max(0.001, min(0.03, threshold))
                self._set_cached(cache_key, threshold)
                logger.info(f"Dynamic signal threshold for {ticker}: {threshold:.4f} ({result.get('rationale', '')})")
                return threshold

        # Fallback to config, then default 0.5%
        threshold = self.config.get("signal_threshold", 0.005)
        return float(threshold)

    # ──────────────────────────────────────────────
    # 2. VIX interpretation levels (replaces hardcoded 15/20/30)
    # ──────────────────────────────────────────────

    def get_vix_levels(self, market_regime: str = "normal") -> list[dict]:
        """Return VIX interpretation thresholds.

        Uses LLM to adapt thresholds based on market regime.
        Falls back to config defaults.

        Args:
            market_regime: "normal", "stress", "calm", or "transition"

        Returns:
            List of {threshold, label} dicts sorted ascending
        """
        cache_key = f"vix_levels:{market_regime}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # Check if config overrides exist
        config_levels = self.config.get("vix_levels")
        if config_levels:
            return config_levels

        # LLM-driven calculation
        result = _call_llm(
            system_prompt="You are a market regime analyst. Given the current market regime, return adaptive VIX interpretation thresholds as a JSON array. VIX typically ranges 10-50. In 'calm' markets, thresholds shift down; in 'stress' markets, they shift up.",
            user_prompt=f"Market regime: {market_regime}\n\nReturn JSON array: [{{\"threshold\": <number>, \"label\": \"<string>\"}}, ...] sorted by threshold ascending. The last entry should have threshold: null meaning 'above all others'.",
        )
        if result and isinstance(result, list) and len(result) >= 3:
            self._set_cached(cache_key, result)
            logger.info(f"Dynamic VIX levels for regime '{market_regime}': {result}")
            return result

        # Hardcoded defaults (same as original, but now centralized)
        return [
            {"threshold": 15, "label": "Low volatility — complacent market conditions"},
            {"threshold": 20, "label": "Moderate-low volatility — normal market conditions"},
            {"threshold": 30, "label": "Moderate-high volatility — increased uncertainty"},
            {"threshold": None, "label": "High volatility — fear/stress in the market"},
        ]

    def interpret_vix(self, current_vix: float, market_regime: str = "normal") -> str:
        """Interpret a VIX value using dynamic thresholds.

        Args:
            current_vix: Current VIX level
            market_regime: Market regime context

        Returns:
            Human-readable interpretation string
        """
        levels = self.get_vix_levels(market_regime)
        for level in levels:
            if level["threshold"] is None:
                return level["label"]
            if current_vix < level["threshold"]:
                return level["label"]
        return levels[-1]["label"]

    # ──────────────────────────────────────────────
    # 3. News search query generation (replaces hardcoded queries)
    # ──────────────────────────────────────────────

    def generate_news_queries(self, ticker: str, sector: str = "", context: str = "") -> list[str]:
        """Generate context-aware news search queries for a ticker.

        Uses LLM to generate relevant queries based on ticker, sector,
        and current market context.

        Args:
            ticker: Ticker symbol
            sector: Industry sector (optional)
            context: Additional market context (optional)

        Returns:
            List of search query strings
        """
        cache_key = f"news_queries:{ticker}:{sector}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        # LLM-driven generation
        ctx_parts = []
        if sector:
            ctx_parts.append(f"Sector: {sector}")
        if context:
            ctx_parts.append(f"Market context: {context}")
        ctx_str = "\n".join(ctx_parts) if ctx_parts else "No additional context."

        result = _call_llm(
            system_prompt="You are a financial research assistant. Generate 4-6 targeted search queries for finding relevant news about a specific stock. Mix company-specific, sector-wide, and macro queries. Keep each query under 5 words.",
            user_prompt=f"Ticker: {ticker}\n{ctx_str}\n\nReturn JSON: {{\"queries\": [\"query1\", \"query2\", ...]}}",
        )
        if result and "queries" in result and isinstance(result["queries"], list):
            queries = [q for q in result["queries"] if q.strip()]
            if queries:
                self._set_cached(cache_key, queries)
                logger.info(f"Dynamic news queries for {ticker}: {queries}")
                return queries

        # Fallback: ticker-based defaults
        sector_query = f"{sector} industry" if sector else "stock market economy"
        return [
            f"{ticker} earnings revenue",
            f"{ticker} stock analysis",
            f"{sector_query}",
            "Federal Reserve interest rates",
        ]

    def generate_global_news_queries(self, ticker: str = "") -> list[str]:
        """Generate global/macro news search queries.

        Args:
            ticker: Optional ticker for contextual relevance

        Returns:
            List of search query strings
        """
        cache_key = f"global_news_queries:{ticker}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        ticker_ctx = f"Relevant to {ticker} investment analysis." if ticker else ""

        result = _call_llm(
            system_prompt="You are a macro research assistant. Generate 4 search queries for global/macro economic news that would be relevant for equity trading decisions. Cover monetary policy, economic outlook, market structure, and geopolitical factors.",
            user_prompt=f"{ticker_ctx}\n\nReturn JSON: {{\"queries\": [\"query1\", \"query2\", \"query3\", \"query4\"]}}",
        )
        if result and "queries" in result and isinstance(result["queries"], list):
            queries = [q for q in result["queries"] if q.strip()]
            if queries:
                self._set_cached(cache_key, queries)
                return queries

        # Default fallback
        return [
            "stock market economy",
            "Federal Reserve interest rates",
            "inflation economic outlook",
            "global markets trading",
        ]

    # ──────────────────────────────────────────────
    # 4. Analysis time windows (replaces hardcoded days)
    # ──────────────────────────────────────────────

    def get_analysis_window(self, data_type: str, ticker: str = "") -> int:
        """Return the lookback period for a given data type.

        Falls back to config, then to hardcoded defaults.

        Args:
            data_type: One of 'stock_data', 'indicators', 'news', 'global_news', 'vix'
            ticker: Optional ticker for future ticker-specific windows

        Returns:
            Number of days to look back
        """
        # Config override
        windows = self.config.get("analysis_windows", {})
        if data_type in windows:
            return int(windows[data_type])

        # Hardcoded defaults (original values)
        defaults = {
            "stock_data": 90,
            "indicators": 30,
            "news": 30,
            "global_news": 7,
            "vix": 30,
        }
        return defaults.get(data_type, 30)

    # ──────────────────────────────────────────────
    # 5. Memory match counts (replaces hardcoded n_matches=2)
    # ──────────────────────────────────────────────

    def get_memory_matches(self, agent_role: str) -> int:
        """Return the number of memory matches for a given agent role.

        Args:
            agent_role: 'trader', 'bull_researcher', 'bear_researcher',
                       'research_manager', 'portfolio_manager'

        Returns:
            Number of memories to retrieve
        """
        matches = self.config.get("memory_matches", {})
        # Config can specify per-role overrides
        if agent_role in matches:
            return int(matches[agent_role])
        # Default
        return int(matches.get("default", 2))

    # ──────────────────────────────────────────────
    # 6. Market detection from ticker
    # ──────────────────────────────────────────────

    def detect_market(self, ticker: str) -> str:
        """Detect the market/exchange from ticker suffix or config.

        Args:
            ticker: Ticker symbol (may include exchange suffix)

        Returns:
            Market code: 'US', 'HK', 'CN', 'SG', etc.
        """
        # Check config for explicit mapping
        market_map = self.config.get("ticker_market_map", {})
        if ticker.upper() in market_map:
            return market_map[ticker.upper()]

        # Auto-detect from suffix
        suffix_map = {
            ".HK": "HK", ".SH": "CN", ".SZ": "CN",
            ".SG": "SG", ".T": "JP", ".L": "UK",
            ".TO": "CA", ".AX": "AU",
        }
        upper = ticker.upper()
        for suffix, market in suffix_map.items():
            if upper.endswith(suffix):
                return market

        # Default
        return self.config.get("default_market", "US")

    def format_ticker(self, ticker: str) -> str:
        """Format ticker with correct exchange suffix.

        Args:
            ticker: Ticker symbol (may or may not have suffix)

        Returns:
            Ticker with exchange suffix
        """
        if "." in ticker:
            return ticker.upper()
        market = self.detect_market(ticker)
        suffix_map = {
            "US": ".US", "HK": ".HK", "CN": ".SH",
            "SG": ".SG", "JP": ".T", "UK": ".L",
        }
        suffix = suffix_map.get(market, ".US")
        return f"{ticker.upper()}{suffix}"

    # ──────────────────────────────────────────────
    # 7. News language/region detection
    # ──────────────────────────────────────────────

    def get_news_locale(self, ticker: str = "") -> tuple[str, str]:
        """Return (language, region) for news fetching.

        Args:
            ticker: Ticker symbol

        Returns:
            (hl, gl) tuple for news API parameters
        """
        market = self.detect_market(ticker)
        locale_map = self.config.get("news_locales", {
            "US": ("en-US", "US"),
            "HK": ("zh-HK", "HK"),
            "CN": ("zh-CN", "CN"),
            "SG": ("en-SG", "SG"),
            "JP": ("ja-JP", "JP"),
            "UK": ("en-GB", "GB"),
        })
        hl, gl = locale_map.get(market, ("en-US", "US"))
        return hl, gl

    # ──────────────────────────────────────────────
    # Cache helpers
    # ──────────────────────────────────────────────

    def _get_cached(self, key: str) -> Any:
        """Get cached value if still valid."""
        if key in self._cache:
            value, timestamp = self._cache[key]
            if time.time() - timestamp < self._cache_ttl:
                return value
            del self._cache[key]
        return None

    def _set_cached(self, key: str, value: Any):
        """Set cached value with current timestamp."""
        self._cache[key] = (value, time.time())

    def clear_cache(self):
        """Clear all cached values."""
        self._cache.clear()
