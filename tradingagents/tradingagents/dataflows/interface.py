# Import from vendor-specific modules
from .longbridge_news import get_news_yfinance as get_news_lb, get_global_news_yfinance as get_global_news_lb
from .longbridge import (
    get_YFin_data_online as get_lb_stock,
    get_fundamentals as get_lb_fundamentals,
    get_balance_sheet as get_lb_balance_sheet,
    get_cashflow as get_lb_cashflow,
    get_income_statement as get_lb_income_statement,
    get_insider_transactions as get_lb_insider_transactions,
    get_vix_data as get_lb_vix,
    get_stock_stats_longbridge as get_lb_stats,
)
from .google_news import get_news_google_news, get_global_news_google_news

# Configuration and routing logic
from .config import get_config

TOOLS_CATEGORIES = {
    "core_stock_apis": {"description": "OHLCV stock price data", "tools": ["get_stock_data"]},
    "technical_indicators": {"description": "Technical analysis indicators", "tools": ["get_indicators"]},
    "fundamental_data": {"description": "Company fundamentals", "tools": ["get_fundamentals", "get_balance_sheet", "get_cashflow", "get_income_statement"]},
    "news_data": {"description": "News and insider data", "tools": ["get_news", "get_global_news", "get_insider_transactions"]},
    "market_data": {"description": "Market volatility and VIX data", "tools": ["get_vix_data"]},
}

VENDOR_LIST = ["longbridge", "google_news"]

VENDOR_METHODS = {
    "get_stock_data": {
        "longbridge": get_lb_stock,
    },
    "get_indicators": {
        "longbridge": get_lb_stats,
    },
    "get_fundamentals": {
        "longbridge": get_lb_fundamentals,
    },
    "get_balance_sheet": {
        "longbridge": get_lb_balance_sheet,
    },
    "get_cashflow": {
        "longbridge": get_lb_cashflow,
    },
    "get_income_statement": {
        "longbridge": get_lb_income_statement,
    },
    "get_news": {
        "longbridge": get_news_lb,
        "google_news": get_news_google_news,
    },
    "get_global_news": {
        "longbridge": get_global_news_lb,
        "google_news": get_global_news_google_news,
    },
    "get_insider_transactions": {
        "longbridge": get_lb_insider_transactions,
    },
    "get_vix_data": {
        "longbridge": get_lb_vix,
    },
}

def get_category_for_method(method: str) -> str:
    for category, info in TOOLS_CATEGORIES.items():
        if method in info["tools"]:
            return category
    raise ValueError(f"Method '{method}' not found in any category")

def get_vendor(category: str, method: str = None) -> str:
    config = get_config()
    if method:
        tool_vendors = config.get("tool_vendors", {})
        if method in tool_vendors:
            return tool_vendors[method]
    return config.get("data_vendors", {}).get(category, "longbridge")

def route_to_vendor(method: str, *args, **kwargs):
    category = get_category_for_method(method)
    vendor_config = get_vendor(category, method)
    primary_vendors = [v.strip() for v in vendor_config.split(',')]

    if method not in VENDOR_METHODS:
        raise ValueError(f"Method '{method}' not supported")

    all_available_vendors = list(VENDOR_METHODS[method].keys())
    fallback_vendors = primary_vendors.copy()
    for vendor in all_available_vendors:
        if vendor not in fallback_vendors:
            fallback_vendors.append(vendor)

    last_error = None
    for vendor in fallback_vendors:
        if vendor not in VENDOR_METHODS[method]:
            continue
        vendor_impl = VENDOR_METHODS[method][vendor]
        impl_func = vendor_impl[0] if isinstance(vendor_impl, list) else vendor_impl
        try:
            return impl_func(*args, **kwargs)
        except Exception as e:
            last_error = f"{vendor}: {str(e)}"
            continue

    raise RuntimeError(f"No available vendor for '{method}'. Last error: {last_error}")
