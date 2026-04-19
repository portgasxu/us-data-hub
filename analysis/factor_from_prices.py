"""
US Data Hub — Factor Calculations (多源数据)
=============================================
因子来源:
1. 价格因子 (Price): momentum, RSI, volatility, mean_reversion, value
2. 基本面因子 (Fundamental, SEC): ROE, profit_margin, debt_to_equity, PE, PB
3. 情绪因子 (Sentiment): news_sentiment, reddit_sentiment, combined_sentiment
4. 资金流因子 (CapitalFlow): main_flow, retail_flow, flow_trend
"""

import os
import sys
import logging
from typing import List, Dict, Optional
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)


class FactorCalculator:
    """多源因子计算器。"""

    # 因子权重配置
    FACTOR_WEIGHTS = {
        "price": 0.35,       # 价格因子权重
        "fundamental": 0.30, # SEC 基本面权重
        "sentiment": 0.20,   # 情绪因子权重
        "capital_flow": 0.15, # 资金流权重
    }

    def __init__(self, db=None):
        self.db = db

    def calculate_all(self, symbol: str, prices: List[Dict] = None) -> Dict:
        """
        计算所有因子（多源）。

        Returns:
            {
                'price_factors': {momentum, rsi, volatility, ...},
                'fundamental_factors': {roe, profit_margin, ...},
                'sentiment_factors': {news_sentiment, reddit_sentiment, ...},
                'capital_flow_factors': {main_flow, retail_flow, ...},
                'composite_score': float (-1 to 1),
            }
        """
        if not prices and self.db:
            prices = self.db.query_prices(symbol, days=365)

        result = {}
        result['price_factors'] = self._calc_price_factors(symbol, prices)
        result['fundamental_factors'] = self._calc_fundamental_factors(symbol)
        result['sentiment_factors'] = self._calc_sentiment_factors(symbol)
        result['capital_flow_factors'] = self._calc_capital_flow_factors(symbol)

        # 综合评分
        result['composite_score'] = self._composite_score(result)

        return result

    # ─── 1. 价格因子 ───

    def _calc_price_factors(self, symbol: str, prices: List[Dict]) -> Dict[str, float]:
        """纯价格序列因子。"""
        if not prices:
            return {}

        closes = [p.get("close", p.get("adj_close", 0)) for p in prices]
        if len(closes) < 20:
            return {}

        factors = {}
        factors["momentum"] = self._momentum(closes)
        factors["rsi"] = self._rsi(closes)
        factors["volatility"] = self._volatility(closes)
        factors["mean_reversion"] = self._mean_reversion(closes)
        factors["value"] = self._value_score(closes)
        factors["quality"] = self._quality_score(closes)

        return factors

    def _momentum(self, closes: List[float]) -> float:
        """12-month momentum (normalized to [-1, 1])."""
        if len(closes) < 252:
            period = min(len(closes) - 1, 60)
        else:
            period = 252
        if closes[0] == 0:
            return 0
        ret = (closes[-1] - closes[0]) / closes[0]
        return max(-1, min(1, ret))

    def _rsi(self, closes: List[float], period: int = 14) -> float:
        """RSI (normalized to [-1, 1] from [0, 100])."""
        if len(closes) < period + 1:
            return 0
        gains = []
        losses = []
        for i in range(1, len(closes)):
            change = closes[i] - closes[i - 1]
            gains.append(max(0, change))
            losses.append(max(0, -change))

        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period

        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        return (rsi - 50) / 50

    def _volatility(self, closes: List[float], period: int = 20) -> float:
        """Annualized volatility (normalized to [-1, 1])."""
        if len(closes) < period + 1:
            return 0
        returns = []
        for i in range(1, len(closes)):
            if closes[i - 1] != 0:
                returns.append((closes[i] - closes[i - 1]) / closes[i - 1])

        if not returns:
            return 0

        mean_ret = sum(returns[-period:]) / period
        variance = sum((r - mean_ret) ** 2 for r in returns[-period:]) / period
        vol = (variance ** 0.5) * (252 ** 0.5)

        if vol < 0.1:
            return 1.0
        elif vol > 1.0:
            return -1.0
        else:
            return 1.0 - vol

    def _mean_reversion(self, closes: List[float]) -> float:
        """Mean reversion score (price vs moving average)."""
        if len(closes) < 50:
            return 0
        ma20 = sum(closes[-20:]) / 20
        ma50 = sum(closes[-50:]) / 50
        if ma50 == 0:
            return 0
        deviation = (closes[-1] - ma50) / ma50
        return max(-1, min(1, -deviation * 2))

    def _value_score(self, closes: List[float]) -> float:
        """Simple value score based on price relative to recent range."""
        if len(closes) < 252:
            return 0
        recent = closes[-252:]
        high = max(recent)
        low = min(recent)
        if high == low:
            return 0
        percentile = (closes[-1] - low) / (high - low)
        return 1.0 - percentile

    def _quality_score(self, closes: List[float]) -> float:
        """Trend consistency score."""
        if len(closes) < 60:
            return 0
        positive_periods = 0
        total_periods = 0
        for i in range(20, len(closes), 20):
            if closes[i - 20] != 0:
                ret = (closes[i] - closes[i - 20]) / closes[i - 20]
                if ret > 0:
                    positive_periods += 1
                total_periods += 1
        if total_periods == 0:
            return 0
        return (positive_periods / total_periods) * 2 - 1

    # ─── 2. 基本面因子 (SEC XBRL) ───

    def _calc_fundamental_factors(self, symbol: str) -> Dict[str, float]:
        """从 SEC XBRL 提取基本面因子。"""
        try:
            from tradingagents.dataflows.sec_financials import (
                get_sec_growth_metrics, get_sec_fundamental_factors
            )

            growth = get_sec_growth_metrics(symbol)
            factors_raw = get_sec_fundamental_factors(symbol)

            if not growth and not factors_raw:
                return {}

            factors = {}

            # ROE (Return on Equity)
            roe = growth.get("roe") if growth else None
            if roe is None and factors_raw:
                roe = factors_raw.get("roe")
            if roe is not None:
                # ROE 0-50% → 0-1
                factors["roe"] = min(1.0, max(0, roe) / 50.0)

            # 利润率 (Profit Margin)
            margin = growth.get("operating_margin") or growth.get("gross_margin")
            if margin is not None:
                # Margin 0-40% → 0-1
                factors["profit_margin"] = min(1.0, max(0, margin) / 40.0)

            # 营收增速 (Revenue Growth)
            rev_growth = growth.get("revenue_growth")
            if rev_growth is not None:
                # Growth 0-60% → 0-1
                factors["revenue_growth"] = min(1.0, max(0, rev_growth) / 60.0)

            # 净利润增速 (Net Income Growth)
            ni_growth = growth.get("net_income_growth")
            if ni_growth is not None:
                factors["net_income_growth"] = min(1.0, max(0, ni_growth) / 60.0)

            # 资产负债率 (Debt to Equity) — 越低越好
            debt_to_equity = factors_raw.get("debt_to_equity") if factors_raw else None
            if debt_to_equity is not None:
                # D/E 0-3 → 1-0
                factors["debt_to_equity"] = max(0, 1 - debt_to_equity / 3.0)

            # 流动比率 (Current Ratio) — 1.5-2.5 最优
            current_ratio = factors_raw.get("current_ratio") if factors_raw else None
            if current_ratio is not None:
                if 1.5 <= current_ratio <= 2.5:
                    factors["current_ratio"] = 1.0
                else:
                    dist = min(abs(current_ratio - 1.5), abs(current_ratio - 2.5))
                    factors["current_ratio"] = max(0, 1 - dist / 2.0)

            return factors
        except Exception as e:
            logger.debug(f"Fundamental factors failed for {symbol}: {e}")
            return {}

    # ─── 3. 情绪因子 (News + Reddit) ───

    def _calc_sentiment_factors(self, symbol: str) -> Dict[str, float]:
        """从新闻和社交数据提取情绪因子。"""
        factors = {}
        if not self.db:
            return factors

        try:
            # News sentiment
            news_rows = self.db.query_data_points(
                symbol=symbol, source="google_news", days=30, limit=20
            )
            if news_rows:
                import json
                sentiments = []
                for row in news_rows:
                    content = json.loads(row["content"]) if isinstance(row["content"], str) else row["content"]
                    sent = content.get("sentiment_score")
                    if sent is not None:
                        sentiments.append(float(sent))
                if sentiments:
                    factors["news_sentiment"] = sum(sentiments) / len(sentiments)  # [-1, 1]
                    factors["news_volume"] = min(1.0, len(sentiments) / 20.0)  # 新闻量

            # Reddit sentiment
            reddit_rows = self.db.query_data_points(
                symbol=symbol, source="reddit", days=30, limit=20
            )
            if reddit_rows:
                import json
                sentiments = []
                for row in reddit_rows:
                    content = json.loads(row["content"]) if isinstance(row["content"], str) else row["content"]
                    sent = content.get("sentiment_score")
                    if sent is not None:
                        sentiments.append(float(sent))
                if sentiments:
                    factors["reddit_sentiment"] = sum(sentiments) / len(sentiments)  # [-1, 1]
                    factors["reddit_volume"] = min(1.0, len(sentiments) / 20.0)

            # Combined sentiment
            news_s = factors.get("news_sentiment", 0)
            reddit_s = factors.get("reddit_sentiment", 0)
            if news_s or reddit_s:
                weights = {"news_sentiment": 0.6, "reddit_sentiment": 0.4}
                total_w = 0
                combined = 0
                for k, w in weights.items():
                    if k in factors:
                        combined += factors[k] * w
                        total_w += w
                if total_w > 0:
                    factors["combined_sentiment"] = combined / total_w

            return factors
        except Exception as e:
            logger.debug(f"Sentiment factors failed for {symbol}: {e}")
            return factors

    # ─── 4. 资金流因子 (CapitalFlow) ───

    def _calc_capital_flow_factors(self, symbol: str) -> Dict[str, float]:
        """从资金流数据提取因子。"""
        factors = {}
        if not self.db:
            return factors

        try:
            cf_rows = self.db.query_data_points(
                symbol=symbol, source="capital_flow", days=30, limit=10
            )
            if cf_rows:
                import json
                main_flows = []
                for row in cf_rows:
                    content = json.loads(row["content"]) if isinstance(row["content"], str) else row["content"]
                    main = content.get("main_net")  # 主力净流入
                    if main is not None:
                        main_flows.append(float(main))

                if main_flows:
                    # 主力净流入方向
                    avg_main = sum(main_flows) / len(main_flows)
                    factors["main_flow_direction"] = 1 if avg_main > 0 else -1

                    # 主力净流入强度（归一化）
                    max_abs = max(abs(f) for f in main_flows) if main_flows else 1
                    if max_abs > 0:
                        factors["main_flow_strength"] = avg_main / max_abs

                    # 趋势（最近3次 vs 前3次）
                    if len(main_flows) >= 6:
                        recent_avg = sum(main_flows[-3:]) / 3
                        prior_avg = sum(main_flows[:3]) / 3
                        if prior_avg != 0:
                            factors["flow_trend"] = min(1.0, max(-1.0, (recent_avg - prior_avg) / abs(prior_avg)))
                        else:
                            factors["flow_trend"] = 1.0 if recent_avg > 0 else -1.0

            return factors
        except Exception as e:
            logger.debug(f"Capital flow factors failed for {symbol}: {e}")
            return factors

    # ─── 综合评分 ───

    def _composite_score(self, factors: Dict) -> float:
        """
        综合评分 [-1, 1]。

        维度权重:
        - 价格因子: 35%
        - 基本面因子: 30%
        - 情绪因子: 20%
        - 资金流因子: 15%
        """
        price = factors.get("price_factors", {})
        fundamental = factors.get("fundamental_factors", {})
        sentiment = factors.get("sentiment_factors", {})
        cap_flow = factors.get("capital_flow_factors", {})

        # 价格综合 (等权重子因子)
        if price:
            price_vals = [v for v in price.values() if isinstance(v, (int, float))]
            price_score = sum(price_vals) / len(price_vals) if price_vals else 0
        else:
            price_score = 0

        # 基本面综合
        if fundamental:
            fund_vals = [v for v in fundamental.values() if isinstance(v, (int, float))]
            fund_score = sum(fund_vals) / len(fund_vals) if fund_vals else 0
        else:
            fund_score = 0

        # 情绪综合
        if sentiment:
            combined = sentiment.get("combined_sentiment", 0)
            news_s = sentiment.get("news_sentiment", 0)
            reddit_s = sentiment.get("reddit_sentiment", 0)
            if combined:
                sent_score = combined
            elif news_s or reddit_s:
                sent_score = (news_s * 0.6 + reddit_s * 0.4)
            else:
                sent_score = 0
        else:
            sent_score = 0

        # 资金流综合
        if cap_flow:
            direction = cap_flow.get("main_flow_direction", 0)
            strength = cap_flow.get("main_flow_strength", 0)
            trend = cap_flow.get("flow_trend", 0)
            flow_score = (direction * 0.4 + strength * 0.3 + trend * 0.3)
        else:
            flow_score = 0

        # 加权综合
        composite = (
            price_score * self.FACTOR_WEIGHTS["price"] +
            fund_score * self.FACTOR_WEIGHTS["fundamental"] +
            sent_score * self.FACTOR_WEIGHTS["sentiment"] +
            flow_score * self.FACTOR_WEIGHTS["capital_flow"]
        )

        return max(-1, min(1, composite))

    def calculate_factors(self, symbol: str, prices: List[Dict] = None) -> Dict[str, float]:
        """Alias for calculate_all()."""
        return self.calculate_all(symbol, prices)


def calculate_factors(symbol: str, prices: List[Dict] = None, db=None) -> Dict:
    """Standalone function to calculate factors."""
    calc = FactorCalculator(db=db)
    return calc.calculate_all(symbol, prices)


def main():
    """CLI: 计算全 watchlist 的多源因子。"""
    import json
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("=== 多源因子计算 ===")

    from storage import Database
    from collectors.price import PriceCollector

    db = Database()
    price_collector = PriceCollector()
    factor_calculator = FactorCalculator(db=db)

    symbols = [row[0] for row in db.conn.execute("SELECT symbol FROM watchlist").fetchall()]

    if not symbols:
        logger.warning("No symbols in watchlist")
        db.close()
        return

    logger.info(f"计算 {len(symbols)} 个标的的多源因子: {symbols}")

    for symbol in symbols:
        try:
            prices = price_collector.collect(symbol, count=365)
            if not prices:
                logger.warning(f"No price data for {symbol}")
                continue

            factors = factor_calculator.calculate_all(symbol, prices)
            composite = factors.get("composite_score", 0)

            # 存储价格因子到 DB
            today = datetime.now().strftime("%Y-%m-%d")
            for fname, fval in factors.get("price_factors", {}).items():
                db.conn.execute(
                    "INSERT OR REPLACE INTO factors (date, symbol, factor_name, factor_value) "
                    "VALUES (?, ?, ?, ?)",
                    (today, symbol, fname, fval)
                )

            # 存储基本面因子
            for fname, fval in factors.get("fundamental_factors", {}).items():
                db.conn.execute(
                    "INSERT OR REPLACE INTO factors (date, symbol, factor_name, factor_value) "
                    "VALUES (?, ?, ?, ?)",
                    (today, symbol, f"fundamental_{fname}", fval)
                )

            # 存储情绪因子
            for fname, fval in factors.get("sentiment_factors", {}).items():
                db.conn.execute(
                    "INSERT OR REPLACE INTO factors (date, symbol, factor_name, factor_value) "
                    "VALUES (?, ?, ?, ?)",
                    (today, symbol, f"sentiment_{fname}", fval)
                )

            # 存储资金流因子
            for fname, fval in factors.get("capital_flow_factors", {}).items():
                db.conn.execute(
                    "INSERT OR REPLACE INTO factors (date, symbol, factor_name, factor_value) "
                    "VALUES (?, ?, ?, ?)",
                    (today, symbol, f"flow_{fname}", fval)
                )

            # 存储综合评分
            db.conn.execute(
                "INSERT OR REPLACE INTO factors (date, symbol, factor_name, factor_value) "
                "VALUES (?, ?, ?, ?)",
                (today, symbol, "composite_score", composite)
            )
            db.conn.commit()

            # 日志
            logger.info(f"{symbol}: composite={composite:.3f}")
            logger.info(f"  价格因子: {json.dumps({k: round(v, 3) for k, v in factors.get('price_factors', {}).items()})}")
            logger.info(f"  基本面: {json.dumps({k: round(v, 3) for k, v in factors.get('fundamental_factors', {}).items()})}")
            logger.info(f"  情绪: {json.dumps({k: round(v, 3) for k, v in factors.get('sentiment_factors', {}).items()})}")
            logger.info(f"  资金流: {json.dumps({k: round(v, 3) for k, v in factors.get('capital_flow_factors', {}).items()})}")

        except Exception as e:
            logger.error(f"Error calculating factors for {symbol}: {e}")

    db.close()
    logger.info("✅ 多源因子计算完成")
