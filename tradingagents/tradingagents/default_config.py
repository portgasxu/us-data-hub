import os

DEFAULT_CONFIG = {
    "project_dir": os.path.abspath(os.path.join(os.path.dirname(__file__), ".")),
    "results_dir": os.getenv("TRADINGAGENTS_RESULTS_DIR", "./results"),
    "data_cache_dir": os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), ".")),
        "dataflows/data_cache",
    ),
    # LLM settings
    "llm_provider": "openai",
    "deep_think_llm": os.getenv("TRADINGAGENTS_LLM_MODEL", "qwen3-coder-plus"),
    "quick_think_llm": os.getenv("TRADINGAGENTS_LLM_MODEL", "qwen3-coder-plus"),
    "backend_url": os.getenv("TRADINGAGENTS_BACKEND_URL", "https://coding.dashscope.aliyuncs.com/v1"),
    # OpenAI-compatible API key (used by langchain-openai)
    "api_key": os.getenv("OPENAI_API_KEY"),  # No hardcoded fallback — must come from env
    # Provider-specific thinking configuration
    "google_thinking_level": None,      # "high", "minimal", etc.
    "openai_reasoning_effort": None,    # "medium", "high", "low"
    "anthropic_effort": None,           # "high", "medium", "low"
    # Output language for analyst reports and final decision
    # Internal agent debate stays in English for reasoning quality
    "output_language": "English",
    # Debate and discussion settings
    "max_debate_rounds": 2,  # Fix #13: Reduced from 3 to 2 (saves 3 LLM calls per stock)
    "max_risk_discuss_rounds": 3,
    "max_recur_limit": 100,
    # ─── Data vendor routing ───
    # Category-level configuration (default for all tools in category)
    "data_vendors": {
        "core_stock_apis": "longbridge",
        "technical_indicators": "longbridge",
        "fundamental_data": "longbridge",
        "news_data": "longbridge",
        "market_data": "longbridge",
    },
    # Tool-level configuration (takes precedence over category-level)
    "tool_vendors": {
        # Example: "get_stock_data": "google_news",  # Override category default
    },
    # ─── Signal evaluation threshold (default 0.5%, overridden by DynamicThresholds per-ticker) ───
    "signal_threshold": 0.005,
    # ─── VIX interpretation levels ───
    "vix_levels": [
        {"threshold": 15, "label": "Low volatility — complacent market conditions"},
        {"threshold": 20, "label": "Moderate-low volatility — normal market conditions"},
        {"threshold": 30, "label": "Moderate-high volatility — increased uncertainty"},
        {"threshold": None, "label": "High volatility — fear/stress in the market"},
    ],
    # ─── Analysis time windows (days) ───
    "analysis_windows": {
        "stock_data": 90,
        "indicators": 30,
        "news": 30,
        "global_news": 7,
        "vix": 30,
    },
    # ─── Memory retrieval per agent role ───
    "memory_matches": {
        "default": 2,
        "trader": 2,
        "bull_researcher": 2,
        "bear_researcher": 2,
        "research_manager": 3,
        "portfolio_manager": 2,
    },
    # ─── Dynamic parameter LLM cache (seconds) ───
    "dynamic_param_cache_seconds": 3600,
    # ─── Market defaults ───
    "default_market": "US",
    "ticker_market_map": {},
    # ─── News locale map ───
    "news_locales": {
        "US": ("en-US", "US"),
        "HK": ("zh-HK", "HK"),
        "CN": ("zh-CN", "CN"),
        "SG": ("en-SG", "SG"),
        "JP": ("ja-JP", "JP"),
        "UK": ("en-GB", "GB"),
    },
}
