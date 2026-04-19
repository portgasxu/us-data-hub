#!/usr/bin/env python3
"""
US Data Hub — 统一信号契约 (Signal Schema)
===========================================
全链路所有信号源必须输出此结构。

流向: 信号源 → SignalHub.add() → auto_execute.py 消费

设计原则:
  1. 单一格式: 所有信号源（持仓监控/选股/情感/因子/SEC/TA/watcher）统一输出
  2. 向后兼容: 与现有 signal_hub.py 的 Signal 类兼容
  3. 可追溯: 预留 signal_id 字段（Phase 2 填充）
"""

from dataclasses import dataclass, field, asdict
from typing import List, Optional
from datetime import datetime, timedelta
from enum import Enum


class SignalDirection(Enum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class SignalSource(Enum):
    HOLDING_MONITOR = "holding_monitor"
    SCREENER = "screener"
    SENTIMENT = "sentiment"
    FACTORS = "factors"
    SEC_FILING = "sec_filing"
    TRADING_AGENTS = "trading_agents"
    WATCHER = "watcher"
    MANUAL = "manual"


# 信号源优先级权重（用于排序和冲突解决）
SOURCE_PRIORITY = {
    SignalSource.HOLDING_MONITOR: 1.0,
    SignalSource.TRADING_AGENTS: 0.95,
    SignalSource.SENTIMENT: 0.8,
    SignalSource.FACTORS: 0.75,
    SignalSource.SCREENER: 0.7,
    SignalSource.SEC_FILING: 0.65,
    SignalSource.WATCHER: 0.6,
    SignalSource.MANUAL: 1.0,
}


@dataclass
class TradeSignal:
    """
    全链路统一交易信号。

    字段设计兼容现有 signal_hub.py 的 Signal 类：
    - symbol: 标的（自动去掉 .US 后缀）
    - direction: buy/sell/hold
    - confidence: 0.0-1.0
    - source: 信号来源枚举
    - strength: 0.0-1.0（信号强度）
    - reason: 人类可读的决策理由
    - quantity_suggestion: 建议数量
    - extra: 源系统原始数据
    - timestamp: 创建时间
    """

    # ── 核心字段 ──
    symbol: str
    direction: SignalDirection
    confidence: float
    source: SignalSource

    # ── 强度与理由 ──
    strength: float = 0.5
    reason: str = ""

    # ── 仓位 ──
    quantity_suggestion: int = 0

    # ── 价格参数（可选） ──
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    target_price: Optional[float] = None

    # ── 上下文扩展 ──
    extra: dict = field(default_factory=dict)

    # ── 追溯（Phase 2 填充） ──
    signal_id: str = ""

    # ── 时间 ──
    timestamp: str = ""

    def __post_init__(self):
        """初始化后处理"""
        # 标准化 symbol（去掉 .US 后缀）
        self.symbol = self.symbol.upper().replace(".US", "")
        # 确保 direction 是枚举
        if isinstance(self.direction, str):
            self.direction = SignalDirection(self.direction.lower())
        # 确保 source 是枚举
        if isinstance(self.source, str):
            self.source = SignalSource(self.source.lower())
        # 规范化 confidence/strength
        self.confidence = max(0.0, min(1.0, self.confidence))
        self.strength = max(0.0, min(1.0, self.strength))
        # 默认时间戳
        if not self.timestamp:
            self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def priority(self) -> float:
        """
        综合优先级分数（用于排序）。
        兼容现有 signal_hub.py Signal.priority()。
        公式: confidence×0.6 + strength×0.2 + source_weight×0.2
        """
        source_w = SOURCE_PRIORITY.get(self.source, 0.5)
        return self.confidence * 0.6 + self.strength * 0.2 + source_w * 0.2

    def to_dict(self) -> dict:
        """
        序列化为 dict（兼容现有 Signal.to_dict()）。
        auto_execute.py 消费此格式。
        """
        return {
            "symbol": self.symbol,
            "direction": self.direction.value,
            "confidence": self.confidence,
            "source": self.source.value,
            "strength": self.strength,
            "reason": self.reason,
            "quantity_suggestion": self.quantity_suggestion,
            "extra": self.extra,
            "timestamp": self.timestamp,
            "priority": self.priority(),
            "signal_id": self.signal_id,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "target_price": self.target_price,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TradeSignal":
        """从 dict 反序列化"""
        d = dict(d)
        direction = d.pop("direction", "hold")
        source = d.pop("source", "manual")
        d.pop("priority", None)  # computed field
        return cls(
            direction=SignalDirection(direction) if isinstance(direction, str) else direction,
            source=SignalSource(source) if isinstance(source, str) else source,
            **{k: v for k, v in d.items() if k in cls.__dataclass_fields__},
        )

    @classmethod
    def from_old_signal(cls, old) -> "TradeSignal":
        """
        从现有 signal_hub.py 的 Signal 对象转换。
        确保向后兼容 — 如果 old 已经是 TradeSignal，直接返回。
        """
        if isinstance(old, TradeSignal):
            return old
        return cls(
            symbol=old.symbol,
            direction=SignalDirection(old.direction),
            confidence=old.confidence,
            source=SignalSource(old.source),
            strength=getattr(old, "strength", 0.5),
            reason=getattr(old, "reason", ""),
            quantity_suggestion=getattr(old, "quantity_suggestion", 0),
            extra=getattr(old, "extra", {}),
            timestamp=getattr(old, "timestamp", ""),
        )

    def __repr__(self):
        return (
            f"TradeSignal({self.symbol} {self.direction.value} "
            f"conf={self.confidence:.2f} src={self.source.value} "
            f"pri={self.priority():.2f})"
        )


# ─── 便捷工厂函数 ───

def make_signal(symbol: str, direction: str, confidence: float,
                source: str, strength: float = 0.5, reason: str = "",
                quantity_suggestion: int = 0, extra: dict = None,
                **kwargs) -> TradeSignal:
    """
    便捷创建信号。
    所有模块都可以通过这个函数创建信号，无需直接 import TradeSignal。
    """
    return TradeSignal(
        symbol=symbol,
        direction=SignalDirection(direction.lower()),
        source=SignalSource(source.lower()),
        confidence=confidence,
        strength=strength,
        reason=reason,
        quantity_suggestion=quantity_suggestion,
        extra=extra or {},
        **kwargs,
    )
