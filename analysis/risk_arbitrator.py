"""
risk_arbitrator.py — 统一风控仲裁器
====================================

解决风控信号冲突问题，按优先级依次检查:
  P0 熔断 (Circuit Breaker)     — 最高优先，halt all
  P1 订单冷却 (Order Cooldown)   — 防止重复下单
  P2 信号 cooldown               — 信号去重
  P3 动态阈值 (Dynamic Threshold) — 信号质量过滤
  P4 Risk Manager               — 综合风控检查
"""

import logging
from dataclasses import dataclass
from typing import Tuple, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class ArbitrationResult:
    """仲裁结果"""
    passed: bool
    reason: str
    level: str = ""  # P0-P4
    details: dict = None


class RiskArbitrator:
    """统一风控仲裁器"""

    def __init__(self, db, circuit_breaker, dynamic_threshold, risk_manager=None):
        self.db = db
        self.circuit_breaker = circuit_breaker
        self.dynamic_threshold = dynamic_threshold
        self.risk_manager = risk_manager

    def arbitrate(self, signal) -> ArbitrationResult:
        """
        统一风控仲裁，按优先级依次检查

        Args:
            signal: 交易信号对象，需包含:
                - symbol: 股票代码
                - direction: "BUY" or "SELL"
                - confidence: 信号置信度
                - price: 当前价格
                - source: 信号来源

        Returns:
            ArbitrationResult: 仲裁结果
        """
        # P0: 熔断检查（最高优先）
        result = self._check_circuit_breaker()
        if not result.passed:
            return result

        # P1: 订单冷却检查
        result = self._check_order_cooldown(signal.symbol, signal.direction)
        if not result.passed:
            return result

        # P2: 信号 cooldown 检查
        result = self._check_signal_cooldown(signal)
        if not result.passed:
            return result

        # P3: 动态阈值检查
        result = self._check_dynamic_threshold(signal)
        if not result.passed:
            return result

        # P4: Risk Manager 综合检查
        if self.risk_manager:
            result = self._check_risk_manager(signal)
            if not result.passed:
                return result

        return ArbitrationResult(passed=True, reason="通过所有风控检查", level="PASS")

    def _check_circuit_breaker(self) -> ArbitrationResult:
        """P0: 熔断检查"""
        try:
            halted, reason = self.circuit_breaker.check_all()
            if halted:
                logger.warning(f"🔴 CIRCUIT BREAKER: {reason}")
                return ArbitrationResult(
                    passed=False,
                    reason=f"熔断: {reason}",
                    level="P0",
                    details={"halted": True, "reason": reason}
                )
        except Exception as e:
            logger.error(f"Circuit breaker check failed: {e}")
            # 熔断检查失败时，保守起见暂停交易
            return ArbitrationResult(
                passed=False,
                reason=f"熔断检查异常: {e}",
                level="P0",
                details={"error": str(e)}
            )

        return ArbitrationResult(passed=True, reason="熔断正常", level="P0")

    def _check_order_cooldown(self, symbol: str, direction: str,
                               minutes: int = 10) -> ArbitrationResult:
        """
        P1: 订单冷却检查
        同一标的同方向 N 分钟内不允许重复下单
        """
        try:
            row = self.db.conn.execute(
                "SELECT cooldown_until, reason FROM signal_cooldowns "
                "WHERE symbol = ? AND direction = ? AND source = 'order_lock' "
                "AND cooldown_until > datetime('now')",
                (symbol, direction)
            ).fetchone()

            if row:
                cooldown_until, reason = row
                logger.warning(f"⏳ ORDER COOLDOWN: {symbol} {direction} until {cooldown_until}")
                return ArbitrationResult(
                    passed=False,
                    reason=f"订单冷却中: {symbol} {direction} (至 {cooldown_until})",
                    level="P1",
                    details={"symbol": symbol, "direction": direction, "until": cooldown_until}
                )
        except Exception as e:
            logger.error(f"Order cooldown check failed: {e}")

        return ArbitrationResult(passed=True, reason="无订单冷却", level="P1")

    def _check_signal_cooldown(self, signal) -> ArbitrationResult:
        """P2: 信号 cooldown 检查"""
        try:
            # 检查 signal_cooldowns 表
            row = self.db.conn.execute(
                "SELECT cooldown_until FROM signal_cooldowns "
                "WHERE symbol = ? AND direction = ? AND source = ? "
                "AND cooldown_until > datetime('now')",
                (signal.symbol, signal.direction, signal.source)
            ).fetchone()

            if row:
                return ArbitrationResult(
                    passed=False,
                    reason=f"信号冷却中: {signal.symbol} {signal.direction}",
                    level="P2",
                    details={"symbol": signal.symbol, "source": signal.source}
                )
        except Exception as e:
            logger.error(f"Signal cooldown check failed: {e}")

        return ArbitrationResult(passed=True, reason="信号冷却正常", level="P2")

    def _check_dynamic_threshold(self, signal) -> ArbitrationResult:
        """P3: 动态阈值检查"""
        try:
            result = self.dynamic_threshold.analyze(signal)
            if not result.get("passes", True):
                rationale = result.get("rationale", "阈值未达")
                logger.info(f"📉 DYNAMIC THRESHOLD: {signal.symbol} - {rationale}")
                return ArbitrationResult(
                    passed=False,
                    reason=f"动态阈值未达: {rationale}",
                    level="P3",
                    details={"threshold_result": result}
                )
        except Exception as e:
            logger.error(f"Dynamic threshold check failed: {e}")
            # 动态阈值检查失败时，使用规则版 fallback
            try:
                result = self.dynamic_threshold.compute_threshold_rule_based(signal)
                if not result.get("passes", True):
                    return ArbitrationResult(
                        passed=False,
                        reason=f"动态阈值(规则版)未达: {result.get('rationale', '')}",
                        level="P3",
                        details={"fallback": True}
                    )
            except Exception as e2:
                logger.error(f"Rule-based threshold also failed: {e2}")

        return ArbitrationResult(passed=True, reason="动态阈值正常", level="P3")

    def _check_risk_manager(self, signal) -> ArbitrationResult:
        """P4: Risk Manager 综合检查"""
        try:
            if hasattr(self.risk_manager, 'check'):
                passed = self.risk_manager.check(signal)
                if not passed:
                    reason = getattr(self.risk_manager, 'last_reason', '风控拒绝')
                    return ArbitrationResult(
                        passed=False,
                        reason=f"风控拒绝: {reason}",
                        level="P4",
                        details={"reason": reason}
                    )
            elif hasattr(self.risk_manager, 'evaluate'):
                result = self.risk_manager.evaluate(signal)
                if not result.get("approved", True):
                    return ArbitrationResult(
                        passed=False,
                        reason=f"风控拒绝: {result.get('reason', '')}",
                        level="P4",
                        details=result
                    )
        except Exception as e:
            logger.error(f"Risk manager check failed: {e}")

        return ArbitrationResult(passed=True, reason="风控正常", level="P4")

    def lock_order(self, symbol: str, direction: str, minutes: int = 10):
        """
        下单后立即锁定，防止重复提交

        Args:
            symbol: 股票代码
            direction: "BUY" or "SELL"
            minutes: 冷却时间（分钟）
        """
        try:
            self.db.conn.execute(
                "INSERT INTO signal_cooldowns (symbol, direction, source, cooldown_until, reason) "
                "VALUES (?, ?, 'order_lock', datetime('now', ?), 'order_lock')",
                (symbol, direction, f'+{minutes} minutes')
            )
            self.db.conn.commit()
            logger.info(f"🔒 Order locked: {symbol} {direction} for {minutes}min")
        except Exception as e:
            logger.error(f"Failed to lock order: {e}")

    def log_arbitration(self, signal, result: ArbitrationResult):
        """记录仲裁结果"""
        try:
            self.db.conn.execute(
                "INSERT INTO trade_logs (symbol, direction, event, details, timestamp) "
                "VALUES (?, ?, 'arbitration', ?, datetime('now'))",
                (signal.symbol, signal.direction,
                 f"{result.level}: {'PASS' if result.passed else 'FAIL'} - {result.reason}")
            )
            self.db.conn.commit()
        except Exception as e:
            logger.error(f"Failed to log arbitration: {e}")
