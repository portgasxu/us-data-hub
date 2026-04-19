"""
US Data Hub — Holding Monitor (v4, Phase 2)
============================================
持仓风控：纯 LLM 动态决策

Phase 2 改造:
  - 集成 LLM Router，自动路由到 CodingPlan 端点
  - 支持双端点降级（CodingPlan → 百炼 fallback）

决策流程：
  1. LLM 动态分析持仓风险（考虑 P&L / 走势 / 情绪 / 波动 / Agent 信号 / 持仓上下文）
  2. LLM 输出每只标的的动态建议（HOLD / REDUCE / ADD / STOP_LOSS / TAKE_PROFIT）
"""

import logging
import json
import re
import os
from datetime import datetime, timedelta
from typing import Optional

from dayup_logger import log_risk

logger = logging.getLogger(__name__)


class HoldingMonitor:
    """Monitor holdings with LLM dynamic risk assessment + hard safety net."""

    def __init__(self, db, position_manager=None):
        self.db = db
        self.pm = position_manager
        # Phase 2: 使用 LLM Router 替代硬编码 LLM
        self._router = None

    def _get_router(self):
        """Phase 2: Lazy init LLM Router."""
        if self._router is None:
            from analysis.llm_router import LLMRouter
            self._router = LLMRouter()
        return self._router

    def _get_llm(self):
        """兼容旧接口，内部走 LLM Router。"""
        # 保持旧接口兼容，但实际通过 Router 调用
        return self._get_router()

    def _has_sell_trades(self, symbol: str) -> bool:
        """检查该标的是否曾经卖出过（减仓锁定利润）。
        
        这是判断"零成本持仓"的唯一依据——不依赖 P&L 百分比。
        """
        try:
            row = self.db.conn.execute(
                "SELECT COUNT(*) FROM trades WHERE symbol = ? AND direction = 'sell'",
                (symbol,)
            ).fetchone()
            return row and row[0] > 0
        except Exception:
            return False

    # ═══════════════════════════════════════════════════════
    # 主入口：LLM 动态分析
    # ═══════════════════════════════════════════════════════

    def run_full_check(self) -> list:
        """Run LLM dynamic risk analysis for holdings."""
        alerts = []

        try:
            holdings = self.db.conn.execute(
                """SELECT h.symbol, h.quantity, h.cost_price, h.company_name
                   FROM holdings h WHERE h.active = 1 AND h.quantity > 0"""
            ).fetchall()

            if not holdings:
                logger.info("No active holdings to monitor")
                return alerts

            logger.info(f"Monitoring {len(holdings)} holdings")

            # ── 收集持仓数据 ──
            total_value = 0
            holding_data = []
            for symbol, quantity, cost, name in holdings:
                price = self._get_latest_price(symbol, cost)
                market_value = quantity * price
                total_value += market_value

                pnl_pct = (price - cost) / cost if cost and cost > 0 else 0
                ma5, ma20 = self._get_moving_averages(symbol)
                recent_sentiment = self._get_recent_sentiment(symbol)
                agent_signal = self._get_recent_decision(symbol)
                volatility = self._get_volatility(symbol)

                holding_data.append({
                    'symbol': symbol,
                    'name': name,
                    'quantity': quantity,
                    'cost': cost,
                    'price': price,
                    'market_value': market_value,
                    'pnl_pct': pnl_pct,
                    'ma5': ma5,
                    'ma20': ma20,
                    'sentiment': recent_sentiment,
                    'agent_signal': agent_signal,
                    'volatility': volatility,
                })

            # ── 计算权重 ──
            if total_value > 0:
                for h in holding_data:
                    h['weight'] = h['market_value'] / total_value

            # ── LLM 动态分析 ──
            llm_alerts = self._llm_risk_analysis(holding_data)
            alerts.extend(llm_alerts)

            # ── Watchlist 新机会提示 ──
            held_symbols = set(h['symbol'] for h in holding_data)
            try:
                wl = self.db.conn.execute(
                    "SELECT symbol FROM watchlist WHERE active = 1"
                ).fetchall()
                new_opps = [w[0] for w in wl if w[0] not in held_symbols]
                if new_opps:
                    logger.info(f"Watchlist 未持仓标的: {new_opps}")
            except Exception:
                pass

        except Exception as e:
            logger.error(f"Monitoring check failed: {e}")
            alerts.append({
                'type': 'error',
                'message': f'监控异常: {e}',
                'severity': 'error',
            })

        return alerts

    # ═══════════════════════════════════════════════════════
    # ① LLM 动态风险分析
    # ═══════════════════════════════════════════════════════

    def _llm_risk_analysis(self, holdings: list) -> list:
        """Use LLM to dynamically assess risk for each holding."""
        alerts = []

        try:
            # 构建持仓上下文
            context_lines = []
            for h in holdings:
                ma5_str = f"{h['ma5']:.2f}" if h['ma5'] else 'N/A'
                ma20_str = f"{h['ma20']:.2f}" if h['ma20'] else 'N/A'
                trend = "↑" if h['ma5'] and h['ma20'] and h['ma5'] > h['ma20'] else "↓"
                signal = h['agent_signal'].get('direction', 'N/A') if h['agent_signal'] else 'N/A'
                sentiment_score = h['sentiment'] if h['sentiment'] is not None else 'N/A'
                vol = f"{h['volatility']:.1%}" if h['volatility'] else 'N/A'
                pnl_pct = h.get('pnl_pct', 0) or 0
                weight = h.get('weight', 0) or 0

                # 关键：直接查交易记录判断是否曾经减仓（而非猜测 P&L）
                has_sold = self._has_sell_trades(h['symbol'])
                sold_note = " ✅ 已减仓过（利润已锁定，剩余为零成本持仓）" if has_sold else ""

                line = (
                    f"- {h['symbol']} ({h['name']}): "
                    f"成本${h['cost']:.2f} → 现价${h['price']:.2f} (P&L {pnl_pct:+.1%}), "
                    f"权重{weight:.1%}, 趋势{trend} (MA5={ma5_str}/MA20={ma20_str}), "
                    f"波动{vol}, 情绪{sentiment_score}, Agent信号={signal}"
                    f"{sold_note}"
                )
                context_lines.append(line)

            prompt = f"""你是一个专业的美股持仓风险分析师。请分析以下持仓的风险和机会，给出每只标的的操作建议。

## 持仓信息
{chr(10).join(context_lines)}

## 重要说明
- 成本价使用券商的**摊薄成本法**：当你减仓获利后，剩余持仓的成本会被利润摊薄
- 标注"已减仓"的标的：之前已经卖出部分仓位锁定了利润，剩余仓位基本零成本、零风险
- 未标注的标的：正常持仓，成本和风险对等

## 分析要求
1. 对于"已减仓"的零成本持仓：重点评估趋势是否还在，而不是 P&L 绝对值。趋势还在就继续持有
2. 对于正常持仓：综合评估止盈/止损时机
3. 不是简单的固定阈值判断，需要综合评估
4. 如果某只标的趋势向上但短期回调，可以建议持有
5. 如果趋势向下且情绪恶化，即使未触及固定止损线也应建议减仓
6. 集中度高的标的需要特别关注

## 输出格式（JSON 数组）
[
  {{
    "symbol": "AAPL",
    "action": "HOLD|REDUCE|ADD|STOP_LOSS|TAKE_PROFIT",
    "confidence": 0.8,
    "reason": "简要分析理由（50字以内）",
    "suggested_weight": 0.25,
    "stop_loss_level": -0.12,
    "take_profit_level": 0.35
  }}
]

只输出 JSON 数组，不要其他文字。"""

            # Phase 2: 通过 LLM Router 调用，自动路由到 CodingPlan
            router = self._get_router()
            result = router.invoke("holding_monitor", [
                {"role": "user", "content": prompt}
            ])
            if not result.get("success"):
                logger.error(f"LLM Router 调用失败: {result.get('error')}")
                return alerts
            content = result["content"].strip()

            # 提取 JSON
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if not json_match:
                logger.warning(f"LLM 返回格式异常: {content[:200]}")
                return alerts

            recommendations = json.loads(json_match.group())

            for rec in recommendations:
                symbol = rec.get('symbol', '')
                action = rec.get('action', 'HOLD')
                confidence = rec.get('confidence', 0.5)
                reason = rec.get('reason', '')
                suggested_weight = rec.get('suggested_weight', 0)
                stop_loss_level = rec.get('stop_loss_level', -0.15)
                take_profit_level = rec.get('take_profit_level', 0.30)

                # 根据 LLM 建议生成告警
                if action == 'STOP_LOSS':
                    severity = 'critical' if confidence > 0.7 else 'warning'
                    alerts.append({
                        'type': 'llm_stop_loss',
                        'symbol': symbol,
                        'message': f'{symbol}: LLM 建议止损 (综合风险高) — {reason}',
                        'severity': severity,
                        'confidence': confidence,
                        'suggested_weight': suggested_weight,
                        'stop_loss_level': stop_loss_level,
                    })
                    log_risk(
                        risk_type='LLM止损',
                        trigger=f'{symbol} 综合风险高',
                        current=f'confidence={confidence:.0%}',
                        threshold=f'stop_loss={stop_loss_level:+.0%}',
                        action=f'建议: STOP_LOSS',
                        result=reason
                    )
                elif action == 'TAKE_PROFIT':
                    alerts.append({
                        'type': 'llm_take_profit',
                        'symbol': symbol,
                        'message': f'{symbol}: LLM 建议止盈 — {reason}',
                        'severity': 'info',
                        'confidence': confidence,
                        'take_profit_level': take_profit_level,
                    })
                    log_risk(
                        risk_type='LLM止盈',
                        trigger=f'{symbol} 到达盈利目标',
                        current=f'confidence={confidence:.0%}',
                        threshold=f'take_profit={take_profit_level:+.0%}',
                        action=f'建议: TAKE_PROFIT',
                        result=reason
                    )
                elif action == 'REDUCE':
                    alerts.append({
                        'type': 'llm_reduce',
                        'symbol': symbol,
                        'message': f'{symbol}: LLM 建议减仓 — {reason}',
                        'severity': 'warning',
                        'confidence': confidence,
                        'suggested_weight': suggested_weight,
                    })
                    log_risk(
                        risk_type='LLM减仓',
                        trigger=f'{symbol} 风险上升',
                        current=f'confidence={confidence:.0%}',
                        threshold=f'suggested_weight={suggested_weight:.0%}',
                        action=f'建议: REDUCE',
                        result=reason
                    )
                elif action == 'ADD':
                    alerts.append({
                        'type': 'llm_add',
                        'symbol': symbol,
                        'message': f'{symbol}: LLM 建议加仓 — {reason}',
                        'severity': 'info',
                        'confidence': confidence,
                    })
                    log_risk(
                        risk_type='LLM加仓',
                        trigger=f'{symbol} 机会出现',
                        current=f'confidence={confidence:.0%}',
                        threshold='',
                        action=f'建议: ADD',
                        result=reason
                    )
                elif action == 'HOLD' and confidence < 0.4:
                    # 低信心持有 = LLM 不确定，提示关注
                    alerts.append({
                        'type': 'llm_uncertain',
                        'symbol': symbol,
                        'message': f'{symbol}: LLM 持有但信心不足 ({confidence:.0%}) — {reason}',
                        'severity': 'warning',
                        'confidence': confidence,
                    })
                    log_risk(
                        risk_type='LLM信心不足',
                        trigger=f'{symbol} 持有信号弱',
                        current=f'confidence={confidence:.0%}',
                        threshold='40%',
                        action='建议: 关注',
                        result=reason
                    )

                logger.info(f"LLM [{symbol}] → {action} (confidence={confidence:.0%}) — {reason}")

        except Exception as e:
            logger.error(f"LLM 动态分析失败: {e}")
            alerts.append({
                'type': 'llm_error',
                'message': f'LLM 分析失败: {e}',
                'severity': 'warning',
            })

        return alerts

    # ═══════════════════════════════════════════════════════
    # 辅助方法
    # ═══════════════════════════════════════════════════════

    def _get_latest_price(self, symbol: str, fallback_cost: float) -> float:
        try:
            row = self.db.conn.execute(
                """SELECT close FROM prices
                   WHERE symbol = ? AND close IS NOT NULL AND close > 0
                   ORDER BY date DESC LIMIT 1""",
                (symbol,)
            ).fetchone()
            if row and row[0] > 0:
                return row[0]
        except Exception:
            pass
        return fallback_cost if fallback_cost and fallback_cost > 0 else 0.0

    def _get_moving_averages(self, symbol: str):
        try:
            rows = self.db.conn.execute(
                """SELECT close FROM prices
                   WHERE symbol = ? AND close IS NOT NULL AND close > 0
                   ORDER BY date DESC LIMIT 20""",
                (symbol,)
            ).fetchall()
            if len(rows) >= 5:
                prices = [r[0] for r in rows]
                ma5 = sum(prices[:5]) / 5
                ma20 = sum(prices) / len(prices) if len(prices) >= 20 else ma5
                return ma5, ma20
        except Exception:
            pass
        return None, None

    def _get_recent_sentiment(self, symbol: str) -> Optional[float]:
        try:
            now = datetime.now()
            three_days = (now - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
            row = self.db.conn.execute(
                """SELECT AVG(sentiment_score) FROM data_points
                   WHERE symbol = ? AND sentiment_score IS NOT NULL
                   AND timestamp >= ?""",
                (symbol, three_days)
            ).fetchone()
            if row and row[0] is not None:
                return float(row[0])
        except Exception:
            pass
        return None

    def _get_recent_decision(self, symbol: str) -> Optional[dict]:
        try:
            row = self.db.conn.execute(
                """SELECT timestamp, agent_signal, confidence
                   FROM trades WHERE symbol = ?
                   ORDER BY timestamp DESC LIMIT 1""",
                (symbol,)
            ).fetchone()
            if row:
                return {
                    'timestamp': row[0],
                    'direction': row[1] or 'HOLD',
                    'confidence': row[2] or 0.5,
                }
        except Exception:
            pass
        return None

    def _get_volatility(self, symbol: str) -> Optional[float]:
        try:
            import math
            rows = self.db.conn.execute(
                """SELECT close FROM prices
                   WHERE symbol = ? AND close IS NOT NULL AND close > 0
                   ORDER BY date DESC LIMIT 20""",
                (symbol,)
            ).fetchall()
            if len(rows) >= 5:
                prices = [r[0] for r in rows]
                returns = [(prices[i - 1] - prices[i]) / prices[i] for i in range(1, len(prices))]
                if returns:
                    mean = sum(returns) / len(returns)
                    var = sum((r - mean) ** 2 for r in returns) / len(returns)
                    return math.sqrt(var)
        except Exception:
            pass
        return None
