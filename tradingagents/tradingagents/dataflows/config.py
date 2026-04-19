import os
from typing import Dict, Optional

# Default config values
DEFAULT_CONFIG_VALUES = {
    "project_dir": os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "results_dir": os.getenv("TRADINGAGENTS_RESULTS_DIR", "./results"),
    "data_cache_dir": os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dataflows/data_cache",
    ),
    "llm_provider": "openai",
    "deep_think_llm": os.getenv("TRADINGAGENTS_LLM_MODEL", "qwen3-coder-plus"),
    "quick_think_llm": os.getenv("TRADINGAGENTS_LLM_MODEL", "qwen3-coder-plus"),
    "backend_url": "https://coding.dashscope.aliyuncs.com/v1",
    "api_key": os.getenv("OPENAI_API_KEY", "YOUR_API_KEY_HERE"),
    "data_vendors": {
        "core_stock_apis": "longbridge",
        "technical_indicators": "longbridge",
        "fundamental_data": "longbridge",
        "news_data": "longbridge",
    },
    "tool_vendors": {},
}

_config: Optional[Dict] = None

def initialize_config():
    global _config
    if _config is None:
        _config = dict(DEFAULT_CONFIG_VALUES)

def set_config(config: Dict):
    global _config
    if _config is None:
        _config = dict(DEFAULT_CONFIG_VALUES)
    _config.update(config)

def get_config() -> Dict:
    if _config is None:
        initialize_config()
    return _config.copy()

initialize_config()
