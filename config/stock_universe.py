"""
US Data Hub — Stock Universe & Industry Classification
Expands beyond the fixed 7-stock watchlist with:
  - Broad US stock universe (50+ tickers)
  - Industry / sector classification
  - Growth metrics (estimates)
  - Hot industry cycle definitions
"""

from typing import Dict, List, Optional


# ============================================================
# Broad stock universe with industry + theme classification
# ============================================================

STOCK_UNIVERSE: List[Dict] = [
    # ── AI / 人工智能 ──
    {"symbol": "AAPL",  "sector": "Technology",   "industry": "Consumer Electronics",    "themes": ["AI", "Consumer"]},
    {"symbol": "GOOGL", "sector": "Technology",   "industry": "Internet Content",         "themes": ["AI", "Cloud", "Advertising"]},
    {"symbol": "MSFT",  "sector": "Technology",   "industry": "Software",                "themes": ["AI", "Cloud", "Enterprise"]},
    {"symbol": "META",  "sector": "Technology",   "industry": "Social Media",            "themes": ["AI", "Metaverse", "Advertising"]},
    {"symbol": "NVDA",  "sector": "Technology",   "industry": "Semiconductors",          "themes": ["AI", "GPU", "Data Center"]},
    {"symbol": "AMD",   "sector": "Technology",   "industry": "Semiconductors",          "themes": ["AI", "GPU", "Data Center"]},
    {"symbol": "PLTR",  "sector": "Technology",   "industry": "Software",                "themes": ["AI", "Defense", "Big Data"]},
    {"symbol": "CRWD",  "sector": "Technology",   "industry": "Cybersecurity",           "themes": ["AI", "Cloud", "Security"]},
    {"symbol": "SNOW",  "sector": "Technology",   "industry": "Software",                "themes": ["AI", "Cloud", "Big Data"]},
    {"symbol": "ORCL",  "sector": "Technology",   "industry": "Software",                "themes": ["AI", "Cloud", "Enterprise"]},
    {"symbol": "ARM",   "sector": "Technology",   "industry": "Semiconductors",          "themes": ["AI", "Chip Design"]},

    # ── 电力科技 / Energy Tech ──
    {"symbol": "NEE",   "sector": "Utilities",    "industry": "Renewable Energy",        "themes": ["AI Power", "Clean Energy"]},
    {"symbol": "VST",   "sector": "Utilities",    "industry": "Electric Utilities",      "themes": ["AI Power", "Nuclear"]},
    {"symbol": "CEG",   "sector": "Utilities",    "industry": "Nuclear Energy",          "themes": ["AI Power", "Nuclear"]},
    {"symbol": "ENPH",  "sector": "Technology",   "industry": "Solar Energy",            "themes": ["AI Power", "Clean Energy"]},
    {"symbol": "D",     "sector": "Utilities",    "industry": "Electric Utilities",      "themes": ["AI Power"]},

    # ── 航天 / 国防 ──
    {"symbol": "LMT",   "sector": "Industrials",  "industry": "Aerospace & Defense",     "themes": ["Aerospace", "Defense"]},
    {"symbol": "RTX",   "sector": "Industrials",  "industry": "Aerospace & Defense",     "themes": ["Aerospace", "Defense"]},
    {"symbol": "BA",    "sector": "Industrials",  "industry": "Aerospace & Defense",     "themes": ["Aerospace", "Defense"]},
    {"symbol": "NOC",   "sector": "Industrials",  "industry": "Aerospace & Defense",     "themes": ["Aerospace", "Defense"]},
    {"symbol": "GD",    "sector": "Industrials",  "industry": "Aerospace & Defense",     "themes": ["Aerospace", "Defense"]},
    {"symbol": "LUNR",  "sector": "Industrials",  "industry": "Space Technology",        "themes": ["Aerospace", "Space"]},

    # ── 生物科技 / Biotech ──
    {"symbol": "LLY",   "sector": "Healthcare",   "industry": "Pharmaceuticals",         "themes": ["Biotech", "GLP-1"]},
    {"symbol": "NVO",   "sector": "Healthcare",   "industry": "Pharmaceuticals",         "themes": ["Biotech", "GLP-1"]},
    {"symbol": "MRNA",  "sector": "Healthcare",   "industry": "Biotechnology",           "themes": ["Biotech", "mRNA"]},
    {"symbol": "REGN",  "sector": "Healthcare",   "industry": "Biotechnology",           "themes": ["Biotech"]},

    # ── 电动车 / 新能源 ──
    {"symbol": "TSLA",  "sector": "Consumer Cyclical", "industry": "Auto Manufacturers", "themes": ["EV", "AI", "Robotics"]},
    {"symbol": "RIVN",  "sector": "Consumer Cyclical", "industry": "Auto Manufacturers", "themes": ["EV"]},
    {"symbol": "LCID",  "sector": "Consumer Cyclical", "industry": "Auto Manufacturers", "themes": ["EV"]},

    # ── 金融 / Fintech ──
    {"symbol": "JPM",   "sector": "Financial",    "industry": "Banks",                   "themes": ["Fintech"]},
    {"symbol": "V",     "sector": "Financial",    "industry": "Payment Processing",      "themes": ["Fintech"]},
    {"symbol": "MA",    "sector": "Financial",    "industry": "Payment Processing",      "themes": ["Fintech"]},
    {"symbol": "GS",    "sector": "Financial",    "industry": "Investment Banking",      "themes": ["Fintech"]},

    # ── 消费 / Retail ──
    {"symbol": "AMZN",  "sector": "Consumer Cyclical", "industry": "Internet Retail",    "themes": ["AI", "Cloud", "E-commerce"]},
    {"symbol": "WMT",   "sector": "Consumer Defensive", "industry": "Retail",            "themes": ["Retail"]},
    {"symbol": "COST",  "sector": "Consumer Defensive", "industry": "Retail",            "themes": ["Retail"]},

    # ── 半导体设备 / Chip Equipment ──
    {"symbol": "AVGO",  "sector": "Technology",   "industry": "Semiconductors",          "themes": ["AI", "Networking"]},
    {"symbol": "QCOM",  "sector": "Technology",   "industry": "Semiconductors",          "themes": ["5G", "Mobile Chips"]},
    {"symbol": "TSM",   "sector": "Technology",   "industry": "Semiconductor Foundry",   "themes": ["AI", "Chip Manufacturing"]},
    {"symbol": "ASML",  "sector": "Technology",   "industry": "Semiconductor Equipment", "themes": ["Chip Equipment"]},
    {"symbol": "MU",    "sector": "Technology",   "industry": "Memory Chips",            "themes": ["AI", "HBM"]},

    # ── 数据中心 / 云计算 ──
    {"symbol": "AMAT",  "sector": "Technology",   "industry": "Semiconductor Equipment", "themes": ["Data Center", "AI"]},
    {"symbol": "EQIX",  "sector": "Real Estate",  "industry": "Data Centers",            "themes": ["Data Center", "AI Power"]},
    {"symbol": "DLR",   "sector": "Real Estate",  "industry": "Data Centers",            "themes": ["Data Center", "AI Power"]},
]

# ============================================================
# 行业周期热度定义
# 每月/季度更新，反映当前市场风口
# ============================================================

HOT_INDUSTRIES: Dict[str, Dict] = {
    "AI": {
        "description": "人工智能（芯片、模型、应用）",
        "heat_score": 10,  # 1-10，越高越热
        "stage": "growth",  # growth, mature, decline
        "key_themes": ["AI", "GPU", "Data Center", "Cloud"],
    },
    "AI Power": {
        "description": "AI 驱动的电力需求（核电、可再生能源）",
        "heat_score": 9,
        "stage": "growth",
        "key_themes": ["AI Power", "Nuclear", "Clean Energy"],
    },
    "Aerospace": {
        "description": "航天与国防",
        "heat_score": 8,
        "stage": "growth",
        "key_themes": ["Aerospace", "Defense", "Space"],
    },
    "Biotech": {
        "description": "生物科技（GLP-1、mRNA 等）",
        "heat_score": 7,
        "stage": "growth",
        "key_themes": ["Biotech", "GLP-1", "mRNA"],
    },
    "EV": {
        "description": "电动车",
        "heat_score": 6,
        "stage": "mature",
        "key_themes": ["EV"],
    },
    "Fintech": {
        "description": "金融科技",
        "heat_score": 5,
        "stage": "mature",
        "key_themes": ["Fintech"],
    },
    "Retail": {
        "description": "消费零售",
        "heat_score": 4,
        "stage": "mature",
        "key_themes": ["Retail"],
    },
}


# ============================================================
# 华尔街/机构成长股指标
# ============================================================

GROWTH_METRICS: Dict[str, Dict] = {
    # 高成长 (EPS growth > 20%)
    "NVDA": {"eps_growth_est": 55, "revenue_growth_est": 80, "analyst_rating": "Strong Buy", "target_upside": 15},
    "AMD":  {"eps_growth_est": 35, "revenue_growth_est": 25, "analyst_rating": "Buy", "target_upside": 20},
    "PLTR": {"eps_growth_est": 40, "revenue_growth_est": 25, "analyst_rating": "Buy", "target_upside": 10},
    "CRWD": {"eps_growth_est": 30, "revenue_growth_est": 30, "analyst_rating": "Strong Buy", "target_upside": 15},
    "ARM":  {"eps_growth_est": 45, "revenue_growth_est": 30, "analyst_rating": "Buy", "target_upside": 25},
    "CEG":  {"eps_growth_est": 25, "revenue_growth_est": 15, "analyst_rating": "Buy", "target_upside": 12},
    "VST":  {"eps_growth_est": 30, "revenue_growth_est": 20, "analyst_rating": "Buy", "target_upside": 18},
    "LLY":  {"eps_growth_est": 40, "revenue_growth_est": 35, "analyst_rating": "Strong Buy", "target_upside": 8},
    "NVO":  {"eps_growth_est": 30, "revenue_growth_est": 25, "analyst_rating": "Strong Buy", "target_upside": 10},
    "MU":   {"eps_growth_est": 60, "revenue_growth_est": 40, "analyst_rating": "Buy", "target_upside": 20},
    "TSM":  {"eps_growth_est": 25, "revenue_growth_est": 20, "analyst_rating": "Strong Buy", "target_upside": 15},

    # 中等成长
    "AAPL":  {"eps_growth_est": 10, "revenue_growth_est": 5, "analyst_rating": "Buy", "target_upside": 10},
    "MSFT":  {"eps_growth_est": 15, "revenue_growth_est": 12, "analyst_rating": "Strong Buy", "target_upside": 8},
    "GOOGL": {"eps_growth_est": 18, "revenue_growth_est": 12, "analyst_rating": "Buy", "target_upside": 12},
    "META":  {"eps_growth_est": 20, "revenue_growth_est": 15, "analyst_rating": "Buy", "target_upside": 10},
    "AMZN":  {"eps_growth_est": 25, "revenue_growth_est": 12, "analyst_rating": "Strong Buy", "target_upside": 15},
    "AVGO":  {"eps_growth_est": 15, "revenue_growth_est": 10, "analyst_rating": "Buy", "target_upside": 10},
    "QCOM":  {"eps_growth_est": 12, "revenue_growth_est": 8, "analyst_rating": "Buy", "target_upside": 10},

    # 一般成长
    "TSLA":  {"eps_growth_est": 15, "revenue_growth_est": 18, "analyst_rating": "Hold", "target_upside": 5},
    "BA":    {"eps_growth_est": -5, "revenue_growth_est": 8, "analyst_rating": "Hold", "target_upside": 8},
    "LMT":   {"eps_growth_est": 8, "revenue_growth_est": 5, "analyst_rating": "Buy", "target_upside": 5},
    "JPM":   {"eps_growth_est": 5, "revenue_growth_est": 5, "analyst_rating": "Hold", "target_upside": 5},

    # 默认值（未列出的）
    "default": {"eps_growth_est": 5, "revenue_growth_est": 5, "analyst_rating": "Hold", "target_upside": 5},
}


def get_all_symbols() -> List[str]:
    """Return all symbol tickers in the universe."""
    return [s["symbol"] for s in STOCK_UNIVERSE]


def get_symbols_by_theme(theme: str) -> List[str]:
    """Return symbols that belong to a specific theme."""
    return [s["symbol"] for s in STOCK_UNIVERSE if theme in s.get("themes", [])]


def get_symbols_by_hot_industries(top_n: int = 3) -> List[Dict]:
    """
    Return symbols from the hottest industries.

    Returns:
        List of {symbol, sector, industry, themes, industry_heat}
    """
    # Sort industries by heat_score descending
    sorted_industries = sorted(
        HOT_INDUSTRIES.items(),
        key=lambda x: x[1]["heat_score"],
        reverse=True,
    )

    top_themes = []
    for name, info in sorted_industries[:top_n]:
        top_themes.extend(info["key_themes"])

    # Find symbols matching any top theme
    seen = set()
    results = []
    for stock in STOCK_UNIVERSE:
        matching_themes = [t for t in stock.get("themes", []) if t in top_themes]
        if matching_themes and stock["symbol"] not in seen:
            seen.add(stock["symbol"])
            max_heat = max(HOT_INDUSTRIES.get(ind, {}).get("heat_score", 0)
                          for theme in matching_themes
                          for ind, info in HOT_INDUSTRIES.items()
                          if theme in info.get("key_themes", []))
            results.append({
                **stock,
                "industry_heat": max_heat,
            })

    return results


def get_growth_info(symbol: str) -> Dict:
    """Get growth metrics for a symbol."""
    return GROWTH_METRICS.get(symbol, GROWTH_METRICS["default"])
