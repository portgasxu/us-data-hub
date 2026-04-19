"""
US Data Hub — Three-Layer Stock Screener (v3)
==============================================

三层漏斗选股：
  第一层：热度筛选 — 时事新闻 + Reddit 讨论热度，筛出关注度最高的股票
  第二层：行业周期 — 当前风口行业（AI/电力科技/航天等），匹配行业热度
  第三层：成长潜力 — 华尔街/机构看好的高成长股

最终输出：按综合得分排序，前 N 只进入 TradingAgents 决策分析
"""

import logging
import math
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from config.stock_universe import (
    STOCK_UNIVERSE, HOT_INDUSTRIES, GROWTH_METRICS,
    get_all_symbols, get_growth_info,
)

logger = logging.getLogger(__name__)


# ─── 评分权重（三层独立加权后融合） ───

LAYER_WEIGHTS = {
    "heat":     0.35,  # 第一层权重：市场热度
    "industry": 0.35,  # 第二层权重：行业周期匹配度
    "growth":   0.30,  # 第三层权重：成长潜力
}


class ThreeLayerScreener:
    """
    三层漏斗选股器：
      Layer 1 (热度)   → 从全量股票中筛出讨论度 Top N
      Layer 2 (行业)   → 按当前风口行业加权
      Layer 3 (成长)   → 按机构成长预期加权
    """

    def __init__(self, db=None):
        self.db = db

    def screen(self, top_n: int = 10, min_score: float = 0.0,
               universe: List[str] = None) -> List[Dict]:
        """
        执行三层筛选。

        Args:
            top_n: 最终返回的股票数量
            min_score: 最低分阈值 (0.0-1.0)
            universe: 自定义选股池 (None=全量)

        Returns:
            按综合得分降序排列的股票列表
        """
        if universe is None:
            universe = get_all_symbols()

        scored = []
        for symbol in universe:
            score = self._score_three_layers(symbol)
            if score and score["total_score"] >= min_score:
                scored.append(score)

        scored.sort(key=lambda x: x["total_score"], reverse=True)
        return scored[:top_n]

    def _score_three_layers(self, symbol: str) -> Optional[Dict]:
        """对一只股票执行三层评分，返回综合得分。"""
        try:
            # Layer 1: 热度评分
            heat_score, heat_detail = self._layer_heat(symbol)

            # Layer 2: 行业周期评分
            industry_score, industry_detail = self._layer_industry_cycle(symbol)

            # Layer 3: 成长潜力评分
            growth_score, growth_detail = self._layer_growth(symbol)

            # 综合得分
            total = (
                heat_score * LAYER_WEIGHTS["heat"] +
                industry_score * LAYER_WEIGHTS["industry"] +
                growth_score * LAYER_WEIGHTS["growth"]
            )

            return {
                "symbol": symbol,
                "total_score": round(total, 4),
                # 分层得分
                "heat_score": round(heat_score, 4),
                "industry_score": round(industry_score, 4),
                "growth_score": round(growth_score, 4),
                # 详情
                "heat_detail": heat_detail,
                "industry_detail": industry_detail,
                "growth_detail": growth_detail,
            }
        except Exception as e:
            logger.warning(f"Scoring failed for {symbol}: {e}")
            return None

    # ═══════════════════════════════════════════════
    # 第一层：热度筛选
    # 新闻讨论量 + Reddit 讨论量 + 情绪趋势 + 成交量异动
    # ═══════════════════════════════════════════════

    def _layer_heat(self, symbol: str):
        """
        热度评分：综合新闻、社交、情绪、量价异动。

        Returns:
            (score 0-1, detail dict)
        """
        detail = {}

        # 1. 新闻热度 (0-1): 近 3 天新闻量
        news_heat = self._calc_news_heat(symbol)
        detail["news"] = news_heat

        # 2. 社交热度 (0-1): 近 3 天 Reddit 讨论量
        social_heat = self._calc_social_heat(symbol)
        detail["social"] = social_heat

        # 3. 情绪趋势 (0-1): 近 3 天 vs 前 7 天的情绪变化
        sentiment_trend = self._calc_sentiment_trend(symbol)
        detail["sentiment_trend"] = sentiment_trend

        # 4. 成交量异动 (0-1): 近 2 日均量 vs 20 日均量
        volume_surge = self._calc_volume_surge(symbol)
        detail["volume_surge"] = volume_surge

        # 热度层综合 = 新闻×0.35 + 社交×0.25 + 情绪趋势×0.25 + 量增×0.15
        score = (
            news_heat * 0.35 +
            social_heat * 0.25 +
            sentiment_trend * 0.25 +
            volume_surge * 0.15
        )

        return score, detail

    def _calc_news_heat(self, symbol: str) -> float:
        """新闻热度：近 3 天新闻量，带趋势加成。"""
        try:
            if not self.db:
                return 0.3
            now = datetime.now()
            three_days_ago = (now - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
            seven_days_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")

            recent = self.db.conn.execute(
                "SELECT COUNT(*) FROM data_points WHERE symbol = ? AND source = 'google_news' AND timestamp >= ?",
                (symbol, three_days_ago)
            ).fetchone()[0]

            older = self.db.conn.execute(
                "SELECT COUNT(*) FROM data_points WHERE symbol = ? AND source = 'google_news' AND timestamp >= ? AND timestamp < ?",
                (symbol, seven_days_ago, three_days_ago)
            ).fetchone()[0]

            base = min(1.0, recent / 10.0)
            trend_boost = min(0.2, (recent - older) / max(older, 1) * 0.1) if older > 0 and recent > older else 0.0
            return min(1.0, base + trend_boost)
        except Exception:
            return 0.3

    def _calc_social_heat(self, symbol: str) -> float:
        """社交热度：近 3 天 Reddit 讨论量。"""
        try:
            if not self.db:
                return 0.0
            three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
            count = self.db.conn.execute(
                "SELECT COUNT(*) FROM data_points WHERE symbol = ? AND source = 'reddit' AND timestamp >= ?",
                (symbol, three_days_ago)
            ).fetchone()[0]
            return min(1.0, count / 5.0)
        except Exception:
            return 0.0

    def _calc_sentiment_trend(self, symbol: str) -> float:
        """情绪趋势：近期 vs 远期平均情绪差异。"""
        try:
            if not self.db:
                return 0.5
            now = datetime.now()
            three_days_ago = (now - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
            ten_days_ago = (now - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")

            recent = self.db.conn.execute(
                "SELECT AVG(sentiment_score) FROM data_points WHERE symbol = ? AND sentiment_score IS NOT NULL AND timestamp >= ?",
                (symbol, three_days_ago)
            ).fetchone()[0] or 0.0

            older = self.db.conn.execute(
                "SELECT AVG(sentiment_score) FROM data_points WHERE symbol = ? AND sentiment_score IS NOT NULL AND timestamp >= ? AND timestamp < ?",
                (symbol, ten_days_ago, three_days_ago)
            ).fetchone()[0] or 0.0

            delta = recent - older
            score = 0.5 + delta
            return max(0.0, min(1.0, score))
        except Exception:
            return 0.5

    def _calc_volume_surge(self, symbol: str) -> float:
        """成交量异动：近 2 日均量 vs 20 日均量。"""
        try:
            if not self.db:
                return 0.5
            rows = self.db.conn.execute(
                "SELECT volume FROM prices WHERE symbol = ? AND volume IS NOT NULL AND volume > 0 ORDER BY date DESC LIMIT 20",
                (symbol,)
            ).fetchall()
            if len(rows) < 5:
                return 0.5
            volumes = [r[0] for r in rows]
            ma2 = sum(volumes[:2]) / 2
            ma20 = sum(volumes) / len(volumes)
            if ma20 == 0:
                return 0.5
            ratio = ma2 / ma20
            score = (ratio - 0.5) / 1.5
            return max(0.0, min(1.0, score))
        except Exception:
            return 0.5

    # ═══════════════════════════════════════════════
    # 第二层：行业周期
    # 匹配当前风口行业，按行业热度加权
    # ═══════════════════════════════════════════════

    def _layer_industry_cycle(self, symbol: str):
        """
        行业周期评分：匹配风口行业 + 行业内部相对地位。

        Returns:
            (score 0-1, detail dict)
        """
        detail = {}

        # 找到股票的行业信息
        stock_info = next((s for s in STOCK_UNIVERSE if s["symbol"] == symbol), None)
        if not stock_info:
            return 0.2, {"matched_themes": [], "industry_heat": 0, "stage": "unknown"}

        themes = stock_info.get("themes", [])
        detail["themes"] = themes

        # 计算匹配的风口行业总分
        total_heat = 0.0
        matched_themes = []
        for theme in themes:
            # 找到包含该 theme 的行业
            for ind_name, ind_info in HOT_INDUSTRIES.items():
                if theme in ind_info.get("key_themes", []):
                    heat = ind_info.get("heat_score", 0) / 10.0  # 归一化到 0-1
                    total_heat += heat
                    matched_themes.append(f"{ind_name}(heat={heat:.1f})")

        detail["matched_themes"] = matched_themes
        detail["industry_heat"] = round(total_heat, 4)

        # 行业阶段加成：growth 阶段额外加分
        stage_bonus = 0.0
        stages_found = []
        for theme in themes:
            for ind_name, ind_info in HOT_INDUSTRIES.items():
                if theme in ind_info.get("key_themes", []):
                    stage = ind_info.get("stage", "mature")
                    stages_found.append(stage)
                    if stage == "growth":
                        stage_bonus += 0.15  # growth 行业额外 +15%

        detail["stages"] = list(set(stages_found))
        detail["stage_bonus"] = round(stage_bonus, 4)

        # 基础分 = 匹配行业热度平均
        base_score = total_heat / max(len(matched_themes), 1) if matched_themes else 0.1
        score = min(1.0, base_score + stage_bonus)

        return score, detail

    # ═══════════════════════════════════════════════
    # 第三层：成长潜力 — SEC 实时财报驱动
    # 从 SEC XBRL 提取真实 EPS/营收增速，fallback 到 stock_universe
    # ═══════════════════════════════════════════════

    def _layer_growth(self, symbol: str):
        """
        成长潜力评分：SEC 实时财报驱动的 EPS 增速 + 收入增速 + ROE + 利润率。

        Returns:
            (score 0-1, detail dict)
        """
        detail = {}
        sec_growth = self._get_sec_growth(symbol)

        if sec_growth:
            # SEC 实时数据可用
            eps_growth = sec_growth.get("eps_growth") or 5
            revenue_growth = sec_growth.get("revenue_growth") or 5
            roe = sec_growth.get("roe") or 10
            op_margin = sec_growth.get("operating_margin") or sec_growth.get("gross_margin") or 10
            detail["source"] = "sec"
        else:
            # Fallback: 硬编码
            growth = get_growth_info(symbol)
            eps_growth = growth.get("eps_growth_est", 5)
            revenue_growth = growth.get("revenue_growth_est", 5)
            roe = 15
            op_margin = 15
            detail["source"] = "fallback"

        detail["eps_growth"] = round(eps_growth, 1)
        detail["revenue_growth"] = round(revenue_growth, 1)
        detail["roe"] = round(roe, 1) if roe else None
        detail["operating_margin"] = round(op_margin, 1) if op_margin else None

        # EPS 增速 → 0-1: 0%→0, 60%→1
        eps_score = min(1.0, max(0, eps_growth) / 60.0)

        # 收入增速 → 0-1: 0%→0, 50%→1
        rev_score = min(1.0, max(0, revenue_growth) / 50.0)

        # ROE → 0-1: 0%→0, 30%→1
        roe_score = min(1.0, max(0, roe) / 30.0)

        # 利润率 → 0-1: 0%→0, 40%→1
        margin_score = min(1.0, max(0, op_margin) / 40.0)

        # 成长层综合 = EPS×0.3 + 收入×0.25 + ROE×0.25 + 利润率×0.2
        score = (
            eps_score * 0.30 +
            rev_score * 0.25 +
            roe_score * 0.25 +
            margin_score * 0.20
        )

        return score, detail

    def _get_sec_growth(self, symbol: str):
        """从 SEC XBRL 提取真实成长指标。"""
        try:
            from tradingagents.dataflows.sec_financials import get_sec_growth_metrics
            return get_sec_growth_metrics(symbol)
        except Exception as e:
            logger.debug(f"SEC growth extraction failed for {symbol}: {e}")
            return None


# ═══════════════════════════════════════════════
# 兼容旧接口
# ═══════════════════════════════════════════════

class StockScreener(ThreeLayerScreener):
    """Backward-compatible alias."""
    pass
