"""
US Data Hub — Stock Universe & Industry Classification (v3.7)
==============================================================
- Broad US stock universe (50+ tickers)
- Dynamic industry heat from real data (news + Reddit volume)
- Growth metrics with Longbridge real-time fallback
"""

import logging
import subprocess
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


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
# 行业周期热度定义 (v3.7 动态计算)
# 保留 base_score 作为基础参考值，实际热度从数据中动态计算
# ============================================================

HOT_INDUSTRIES: Dict[str, Dict] = {
    "AI": {
        "description": "人工智能（芯片、模型、应用）",
        "base_score": 10,  # v3.7: 基础分，实际由 calc_dynamic_industry_heat() 计算
        "stage": "growth",
        "key_themes": ["AI", "GPU", "Data Center", "Cloud"],
    },
    "AI Power": {
        "description": "AI 驱动的电力需求（核电、可再生能源）",
        "base_score": 9,
        "stage": "growth",
        "key_themes": ["AI Power", "Nuclear", "Clean Energy"],
    },
    "Aerospace": {
        "description": "航天与国防",
        "base_score": 8,
        "stage": "growth",
        "key_themes": ["Aerospace", "Defense", "Space"],
    },
    "Biotech": {
        "description": "生物科技（GLP-1、mRNA 等）",
        "base_score": 7,
        "stage": "growth",
        "key_themes": ["Biotech", "GLP-1", "mRNA"],
    },
    "EV": {
        "description": "电动车",
        "base_score": 6,
        "stage": "mature",
        "key_themes": ["EV"],
    },
    "Fintech": {
        "description": "金融科技",
        "base_score": 5,
        "stage": "mature",
        "key_themes": ["Fintech"],
    },
    "Retail": {
        "description": "消费零售",
        "base_score": 4,
        "stage": "mature",
        "key_themes": ["Retail"],
    },
}


# ============================================================
# 华尔街/机构成长股指标 (v3.7: 保留默认值，Longbridge 实时覆盖)
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
    "BA":    {"eps_growth_est": -5, "revenue_growth_est": 8, "analyst_rating": "Hold", "target_upspace": 8},
    "LMT":   {"eps_growth_est": 8, "revenue_growth_est": 5, "analyst_rating": "Buy", "target_upside": 5},
    "JPM":   {"eps_growth_est": 5, "revenue_growth_est": 5, "analyst_rating": "Hold", "target_upside": 5},

    # 默认值（未列出的）
    "default": {"eps_growth_est": 5, "revenue_growth_est": 5, "analyst_rating": "Hold", "target_upside": 5},
}


# ============================================================
# v3.7 动态函数：从实时数据计算行业热度
# ============================================================

# 缓存动态行业热度，避免每次评分都查数据库
_dynamic_industry_cache: Dict[str, float] = {}
_dynamic_industry_last_update: Optional[datetime] = None
_DYNAMIC_HEAT_TTL_SECONDS = 3600  # 每小时刷新一次


def calc_dynamic_industry_heat(db=None, force_refresh: bool = False) -> Dict[str, float]:
    """
    从 data_points 中实时计算各行业热度分（1-10）。

    逻辑：
      1. 统计近 7 天各主题相关的新闻+Reddit 数量
      2. 归一化为 1-10 分
      3. 与 base_score 加权混合（动态 60% + 基础 40%）
    """
    global _dynamic_industry_cache, _dynamic_industry_last_update

    now = datetime.now()
    if not force_refresh and _dynamic_industry_last_update:
        if (now - _dynamic_industry_last_update).total_seconds() < _DYNAMIC_HEAT_TTL_SECONDS:
            return _dynamic_industry_cache

    if not db:
        # 没数据库 → 返回 base_score
        return {name: info["base_score"] for name, info in HOT_INDUSTRIES.items()}

    try:
        # 统计各 theme 近 7 天的 data_points 数量
        theme_counts = {}
        for ind_name, ind_info in HOT_INDUSTRIES.items():
            total = 0
            for theme in ind_info["key_themes"]:
                # 查 google_news + reddit 中与该 theme 相关股票的数据量
                # 先找属于该 theme 的股票
                theme_symbols = [s["symbol"] for s in STOCK_UNIVERSE if theme in s.get("themes", [])]
                if not theme_symbols:
                    continue
                placeholders = ",".join("?" for _ in theme_symbols)
                # google_news
                row = db.conn.execute(f"""
                    SELECT COUNT(*) FROM data_points
                    WHERE source IN ('google_news', 'reddit')
                    AND symbol IN ({placeholders})
                    AND timestamp >= datetime('now', '-7 days')
                """, theme_symbols).fetchone()
                total += row[0] if row else 0
            theme_counts[ind_name] = total

        # 归一化到 1-10
        max_count = max(theme_counts.values()) if theme_counts else 1
        if max_count == 0:
            max_count = 1

        result = {}
        for ind_name, count in theme_counts.items():
            dynamic_score = (count / max_count) * 10
            base_score = HOT_INDUSTRIES[ind_name]["base_score"]
            # 混合：动态 60% + 基础 40%
            result[ind_name] = round(dynamic_score * 0.6 + base_score * 0.4, 2)

        _dynamic_industry_cache = result
        _dynamic_industry_last_update = now

        logger.info(f"🔥 行业热度动态刷新: {result}")
        return result

    except Exception as e:
        logger.warning(f"动态行业热度计算失败，使用 base_score: {e}")
        return {name: info["base_score"] for name, info in HOT_INDUSTRIES.items()}


# ============================================================
# v3.7 动态函数：从 Longbridge 获取实时成长数据
# ============================================================

_growth_cache: Dict[str, Dict] = {}
_growth_last_update: Optional[datetime] = None
_GROWTH_TTL_SECONDS = 86400  # 每天刷新一次


def get_growth_info(symbol: str, db=None, force_refresh: bool = False) -> Dict:
    """
    获取股票成长指标，优先使用 Longbridge 实时数据，fallback 到静态默认值。

    从 Longbridge financial-report 提取 EPS YoY 增长率。
    """
    global _growth_cache, _growth_last_update

    now = datetime.now()
    if not force_refresh and _growth_last_update:
        if (now - _growth_last_update).total_seconds() < _GROWTH_TTL_SECONDS and symbol in _growth_cache:
            return _growth_cache[symbol]

    # 1. 先查静态配置
    static = GROWTH_METRICS.get(symbol)
    if not static and symbol != "default":
        static = GROWTH_METRICS["default"]

    # 2. 尝试从 Longbridge 获取实时数据覆盖
    try:
        import subprocess
        result = subprocess.run(
            ["longbridge", "financial-report", f"{symbol}.US", "--kind", "IS"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0 and result.stdout.strip():
            # 解析表格
            lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip() and not l.startswith('──')]
            if len(lines) >= 2:
                # 找 EPS 行
                for line in lines:
                    if '每股收益' in line:
                        parts = [p.strip() for p in line.split('|') if p.strip()]
                        if len(parts) >= 5:
                            try:
                                # Q1 2026 vs Q1 2025
                                current = float(parts[1])
                                prior = float(parts[4])
                                if prior > 0:
                                    eps_yoy = round(((current - prior) / prior) * 100, 1)
                                    merged = {
                                        "eps_growth_est": eps_yoy,
                                        "revenue_growth_est": static.get("revenue_growth_est", 5),
                                        "analyst_rating": static.get("analyst_rating", "Hold"),
                                        "target_upside": static.get("target_upside", 5),
                                        "source": "longbridge_realtime",
                                    }
                                    _growth_cache[symbol] = merged
                                    _growth_last_update = now
                                    logger.info(f"📈 {symbol} 成长数据从 Longbridge 获取: EPS YoY={eps_yoy}%")
                                    return merged
                            except (ValueError, IndexError):
                                pass
    except Exception as e:
        logger.debug(f"Longbridge 成长数据获取失败 {symbol}: {e}")

    # 3. Fallback 到静态
    _growth_cache[symbol] = static
    _growth_last_update = now
    return static


def get_all_symbols() -> List[str]:
    """Return all symbol tickers in the universe."""
    return [s["symbol"] for s in STOCK_UNIVERSE]


def get_symbols_by_theme(theme: str) -> List[str]:
    """Return symbols that belong to a specific theme."""
    return [s["symbol"] for s in STOCK_UNIVERSE if theme in s.get("themes", [])]


def get_symbols_by_hot_industries(top_n: int = 3, db=None) -> List[Dict]:
    """
    从动态行业热度中选出 top N 热门行业对应的股票。

    v3.7: 使用 calc_dynamic_industry_heat() 实时计算，不再硬排序。
    """
    dynamic_heat = calc_dynamic_industry_heat(db=db)

    # 按动态热度排序
    sorted_industries = sorted(dynamic_heat.items(), key=lambda x: x[1], reverse=True)

    top_themes = []
    for name, heat in sorted_industries[:top_n]:
        top_themes.extend(HOT_INDUSTRIES.get(name, {}).get("key_themes", []))

    # Find symbols matching any top theme
    seen = set()
    results = []
    for stock in STOCK_UNIVERSE:
        matching_themes = [t for t in stock.get("themes", []) if t in top_themes]
        if matching_themes and stock["symbol"] not in seen:
            seen.add(stock["symbol"])
            results.append({
                **stock,
                "industry_heat": dynamic_heat.get(
                    next((k for k, v in HOT_INDUSTRIES.items() if any(t in v["key_themes"] for t in matching_themes)), ""),
                    5
                ),
            })

    return results
