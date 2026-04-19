#!/usr/bin/env python3
"""
US Data Hub — Feature Store (v6.0)
===================================
全局统一因子服务 — 所有模块通过此接口获取因子数据。

设计原则:
  1. 单一数据源: 所有因子计算集中在此
  2. 一致性: 同一 (symbol, date) 返回相同的因子值
  3. 缓存: 计算结果缓存，避免重复计算
  4. 可追溯: 记录因子计算时间和版本

消费者:
  - Screener: 通过 FeatureStore 获取因子作为筛选维度
  - SignalHub: _collect_factors() 通过 FeatureStore 获取
  - TradingAgents: dataflows/interface.py 通过 FeatureStore 获取
  - Backtest: 通过 FeatureStore 获取历史因子
"""

import sys
import os
import logging
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

# 全局因子定义 — 所有消费者共用
ALL_FACTOR_NAMES = [
    # 基础 5 因子
    "momentum", "rsi", "volatility", "value", "quality",
    # 扩展 8 因子
    "macd", "bollinger", "obv", "adx", "historical_volatility",
    "vwap", "mfi", "accumulation_distribution",
]


@dataclass
class FeatureFrame:
    """单一 (symbol, date) 的因子快照"""
    symbol: str
    date: str
    factors: Dict[str, float] = field(default_factory=dict)
    computed_at: str = ""

    def get(self, name: str, default: float = 0.0) -> float:
        return self.factors.get(name, default)

    def has(self, name: str) -> bool:
        return name in self.factors

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "date": self.date,
            "factors": self.factors,
            "computed_at": self.computed_at,
        }


class FeatureStore:
    """全局统一因子服务"""

    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "data", "us_data_hub.db"
            )
        self.db_path = db_path

    def get_features(self, symbol: str, date: str = None,
                     factor_names: List[str] = None) -> Optional[FeatureFrame]:
        """
        查询指定标的在指定日期的因子。
        如果 date 为 None，返回最新可用因子。
        如果 factor_names 为 None，返回全部因子。
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        if factor_names is None:
            factor_names = ALL_FACTOR_NAMES

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # 获取该 symbol 最新的因子值（每个 factor_name 取最近的一条）
            factors = {}
            computed_at = ""
            for fname in factor_names:
                row = conn.execute(
                    """SELECT factor_value, created_at FROM factors
                       WHERE symbol = ? AND factor_name = ? AND date <= ?
                       ORDER BY date DESC LIMIT 1""",
                    (symbol, fname, date)
                ).fetchone()
                if row and row["factor_value"] is not None:
                    factors[fname] = float(row["factor_value"])
                    if row["created_at"]:
                        computed_at = row["created_at"]

            if not factors:
                return None

            return FeatureFrame(
                symbol=symbol,
                date=date,
                factors=factors,
                computed_at=computed_at,
            )

    def get_features_batch(self, symbols: List[str],
                           date: str = None) -> Dict[str, FeatureFrame]:
        """批量查询多只标的的因子"""
        result = {}
        for symbol in symbols:
            ff = self.get_features(symbol, date)
            if ff:
                result[symbol] = ff
        return result

    def compute_and_store(self, symbol: str, prices: list,
                          date: str = None) -> FeatureFrame:
        """
        计算全部 13 个因子并存入 DB。
        这是唯一允许写入因子的方法。
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        # 调用现有因子计算模块
        try:
            from analysis.factor_from_prices import compute_factors
            from analysis.extended_factors import compute_extended_factors

            base = compute_factors(prices)
            extended = compute_extended_factors(prices)
            all_factors = {**base, **extended}
        except Exception as e:
            logger.warning(f"FeatureStore: factor computation failed for {symbol}: {e}")
            all_factors = {}

        if all_factors:
            # 写入 DB
            with sqlite3.connect(self.db_path) as conn:
                for name, value in all_factors.items():
                    conn.execute(
                        """INSERT OR REPLACE INTO factors
                           (date, symbol, factor_name, factor_value, created_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        (date, symbol, name, value, datetime.now().isoformat())
                    )

            logger.info(f"FeatureStore: computed {len(all_factors)} factors for {symbol} @ {date}")

        return FeatureFrame(
            symbol=symbol,
            date=date,
            factors=all_factors,
            computed_at=datetime.now().isoformat(),
        )

    def compute_all(self, symbols: List[str], prices_map: Dict[str, list],
                    date: str = None) -> Dict[str, FeatureFrame]:
        """批量计算多只标的的因子"""
        result = {}
        for symbol in symbols:
            if symbol in prices_map and prices_map[symbol]:
                result[symbol] = self.compute_and_store(
                    symbol, prices_map[symbol], date
                )
        return result
