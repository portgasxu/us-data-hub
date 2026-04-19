#!/usr/bin/env python3
"""
US Data Hub — Signal Hub (v6.0)
===============================
统一信号中心，聚合多源信号供自动交易引擎消费。

v6.0 变更:
  - 新增 TradeSignal 统一契约（analysis/signal_schema.py）
  - SignalHub 同时兼容旧 Signal 类和新 TradeSignal
  - 所有内部信号统一转换为 TradeSignal 输出
  - 预留 signal_id 字段（Phase 2 全局 Trace ID）

信号来源：
  1. holding_monitor — LLM 持仓分析
  2. screener        — 选股评分
  3. sentiment       — News/Reddit 情感分析
  4. factors         — 因子策略信号
  5. sec_filing      — SEC 文件分析
  6. trading_agents  — TradingAgents 多智能体分析
  7. watcher         — 事件驱动触发
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# ─── v6.0: 引入统一信号契约 ───
from analysis.signal_schema import (
    TradeSignal, SignalDirection, SignalSource, SOURCE_PRIORITY, make_signal
)

logger = logging.getLogger(__name__)


class Signal:
    """
    Standardized trading signal (v5.x 兼容类).

    v6.0: 保留此类用于向后兼容，但所有信号最终都会转换为 TradeSignal。
    新代码应直接使用 TradeSignal。
    """

    def __init__(self, symbol: str, direction: str, confidence: float,
                 source: str, strength: float = 0.5, reason: str = "",
                 quantity_suggestion: int = 0, extra: dict = None):
        self.symbol = symbol.upper().replace(".US", "")
        self.direction = direction.lower()  # buy/sell/hold
        self.confidence = max(0.0, min(1.0, confidence))
        self.source = source
        self.strength = max(0.0, min(1.0, strength))
        self.reason = reason
        self.quantity_suggestion = quantity_suggestion
        self.extra = extra or {}
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def priority(self) -> float:
        """Combined priority score for sorting."""
        w = {"holding_monitor": 1.0, "trading_agents": 0.95,
             "sentiment": 0.8, "factors": 0.75, "screener": 0.7,
             "sec_filing": 0.65, "watcher": 0.6}
        source_w = w.get(self.source, 0.5)
        return self.confidence * 0.6 + self.strength * 0.2 + source_w * 0.2

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "confidence": self.confidence,
            "source": self.source,
            "strength": self.strength,
            "reason": self.reason,
            "quantity_suggestion": self.quantity_suggestion,
            "extra": self.extra,
            "timestamp": self.timestamp,
            "priority": self.priority(),
        }

    def __repr__(self):
        return f"Signal({self.symbol} {self.direction} conf={self.confidence:.2f} from={self.source})"

    # ─── v6.0: 转换为 TradeSignal ───
    def to_trade_signal(self) -> TradeSignal:
        """转换为 v6.0 TradeSignal 格式"""
        return TradeSignal(
            symbol=self.symbol,
            direction=SignalDirection(self.direction),
            confidence=self.confidence,
            source=SignalSource(self.source) if self.source in [s.value for s in SignalSource] else SignalSource.MANUAL,
            strength=self.strength,
            reason=self.reason,
            quantity_suggestion=self.quantity_suggestion,
            extra=self.extra,
            timestamp=self.timestamp,
        )


class SignalHub:
    """
    Aggregates and deduplicates signals from multiple sources.

    v6.0: Internally normalizes all signals to TradeSignal format.
    Accepts both legacy Signal objects and new TradeSignal objects.
    """

    def __init__(self, db, min_confidence: float = 0.5):
        self.db = db
        self.min_confidence = min_confidence
        self.signals: List[TradeSignal] = []  # v6.0: all normalized to TradeSignal
        self._dedup_key = lambda s: (s.symbol, s.direction)

    def add(self, signal):
        """
        Add a signal, replacing weaker duplicates.
        v6.0: Accepts both Signal and TradeSignal, normalizes to TradeSignal internally.
        Auto-generates signal_id if not present.
        """
        # Normalize to TradeSignal
        if isinstance(signal, TradeSignal):
            ts = signal
        elif isinstance(signal, Signal):
            ts = signal.to_trade_signal()
        else:
            raise TypeError(f"Expected Signal or TradeSignal, got {type(signal)}")

        # v6.0: Auto-generate signal_id
        if not ts.signal_id:
            from analysis.trace_id import generate_signal_id
            ts.signal_id = generate_signal_id()

        if ts.confidence < self.min_confidence:
            return

        key = self._dedup_key(ts)
        existing = None
        for s in self.signals:
            if self._dedup_key(s) == key:
                existing = s
                break
        if existing is None or ts.priority() > existing.priority():
            if existing:
                self.signals.remove(existing)
                logger.debug(f"Replaced weaker signal: {existing} (priority {existing.priority():.2f})")
            self.signals.append(ts)
            logger.info(f"Signal added: {ts}")

    def collect_all(self, symbols: List[str] = None) -> List[Signal]:
        """Collect signals from all sources."""
        self.signals = []

        logger.info("=== Signal Collection Start ===")

        # Clean up expired cooldowns
        try:
            self.db.conn.execute("DELETE FROM signal_cooldowns WHERE cooldown_until <= datetime('now')")
            self.db.conn.commit()
        except Exception:
            pass

        # 1. Holding monitor signals
        self._collect_holding_monitor()

        # 2. Screener signals
        self._collect_screener()

        # 3. Sentiment signals
        self._collect_sentiment()

        # 4. Factor signals
        self._collect_factors()

        # 5. SEC filing signals (Fix #8 - NEW)
        self._collect_sec_filing(symbols)

        # 6. TradingAgents (v6.0: enabled, outputs TradeSignal)
        self._collect_trading_agents(symbols)

        # Filter out signals that are in cooldown
        self._filter_cooldown_signals()

        # Sort by priority descending
        self.signals.sort(key=lambda s: s.priority(), reverse=True)

        logger.info(f"=== Signal Collection Complete: {len(self.signals)} signals ===")
        for s in self.signals:
            logger.info(f"  [{s.priority():.2f}] {s}")

        return self.signals

    def _filter_cooldown_signals(self):
        """Remove signals that are currently in cooldown (recently blocked)."""
        try:
            cooldown_keys = set()
            for row in self.db.conn.execute(
                "SELECT symbol, direction, source FROM signal_cooldowns WHERE cooldown_until > datetime('now')"
            ).fetchall():
                cooldown_keys.add((row[0], row[1], row[2]))

            before = len(self.signals)
            self.signals = [s for s in self.signals if (s.symbol, s.direction, s.source) not in cooldown_keys]
            filtered = before - len(self.signals)
            if filtered > 0:
                logger.info(f"Cooldown filter: removed {filtered} signals in cooldown period")
        except Exception as e:
            logger.debug(f"Cooldown filter failed: {e}")

    def _cleanup_expired_cooldowns(self):
        """Remove expired cooldown records."""
        try:
            self.db.conn.execute("DELETE FROM signal_cooldowns WHERE cooldown_until <= datetime('now')")
            self.db.conn.commit()
        except Exception:
            pass

    def _record_cooldown(self, symbol: str, direction: str, source: str, reason: str, hours: int = 4):
        """Record that a signal was blocked by risk control, preventing it from reappearing for `hours` hours.

        Bug fix #3: Cooldown escalation — if the same symbol+direction has been repeatedly blocked,
        extend the cooldown to prevent useless 4-hour reset loops (was: MSFT sell blocked → 4h cooldown
        → cooldown expires → blocked again → 4h cooldown → infinite loop).

        Escalation ladder: 4h → 8h → 16h → 24h (capped).
        """
        try:
            from datetime import timedelta

            # Count recent blocks for this symbol+direction to determine escalation level
            recent_blocks = self.db.conn.execute(
                "SELECT COUNT(*) FROM signal_log "
                "WHERE symbol = ? AND direction = ? AND action_taken = 'REJECTED' "
                "AND updated_at >= datetime('now', '-24 hours')",
                (symbol, direction)
            ).fetchone()[0]

            # Escalate: 0-1 blocks = 4h, 2-3 = 8h, 4-5 = 16h, 6+ = 24h
            if recent_blocks >= 6:
                hours = 24
            elif recent_blocks >= 4:
                hours = 16
            elif recent_blocks >= 2:
                hours = 8
            # else: default 4h

            now = datetime.now()
            until = now + timedelta(hours=hours)
            self.db.conn.execute(
                "INSERT INTO signal_cooldowns (symbol, direction, source, blocked_at, cooldown_until, reason) VALUES (?,?,?,?,?,?)",
                (symbol, direction, source, now.strftime("%Y-%m-%d %H:%M:%S"),
                 until.strftime("%Y-%m-%d %H:%M:%S"), reason)
            )
            self.db.conn.commit()
            logger.info(f"[{symbol}] 🕒 {direction} ({source}) cooldown until {until.strftime('%H:%M')} — {reason}")
        except Exception as e:
            logger.debug(f"Failed to record cooldown: {e}")

    def get_tradable_signals(self) -> List[TradeSignal]:
        """
        Get signals that should trigger trades (exclude hold).
        v6.0: Returns TradeSignal list.
        Includes dedup and conflict resolution (Fix #2):
          - Same symbol + direction → keep highest priority only.
          - Same symbol + BUY vs SELL → keep higher confidence (not skip both).
        """
        # ─── Phase 1: Dedup same symbol+direction, keep highest priority ───
        deduped: Dict[tuple, TradeSignal] = {}
        for sig in self.signals:
            if sig.direction not in (SignalDirection.BUY, SignalDirection.SELL):
                continue
            key = (sig.symbol, sig.direction.value)
            if key not in deduped:
                deduped[key] = sig
            else:
                existing = deduped[key]
                if sig.priority() > existing.priority() or (
                    sig.priority() == existing.priority() and sig.confidence > existing.confidence
                ):
                    deduped[key] = sig

        # ─── Phase 2: Cross-direction conflict resolution (Fix #2) ───
        buy_symbols = {sym for (sym, d) in deduped if d == "buy"}
        sell_symbols = {sym for (sym, d) in deduped if d == "sell"}
        conflicted = buy_symbols & sell_symbols

        for sym in conflicted:
            try:
                from dayup_logger import log_risk
                buy_sig = deduped[(sym, "buy")]
                sell_sig = deduped[(sym, "sell")]

                log_risk(
                    risk_type="信号冲突",
                    trigger=f"{sym}: {buy_sig.source.value} BUY (c={buy_sig.confidence:.0%}) vs {sell_sig.source.value} SELL (c={sell_sig.confidence:.0%})",
                    current="CONFLICT",
                    threshold="同标的双向信号",
                    action="保留高置信度方向"
                )
            except Exception:
                pass

            # Fix: keep higher-confidence signal instead of skipping both
            buy_sig = deduped.get((sym, "buy"))
            sell_sig = deduped.get((sym, "sell"))
            if buy_sig and sell_sig:
                if buy_sig.confidence >= sell_sig.confidence:
                    winner = buy_sig
                    loser = sell_sig
                else:
                    winner = sell_sig
                    loser = buy_sig
                logger.warning(f"⚠️ SIGNAL CONFLICT: {sym} BUY (c={buy_sig.confidence:.0%}) vs SELL (c={sell_sig.confidence:.0%}) — keeping {winner.direction.value.upper()} (higher confidence)")
                del deduped[(sym, loser.direction.value)]
            elif buy_sig:
                deduped.pop((sym, "sell"), None)
            elif sell_sig:
                deduped.pop((sym, "buy"), None)

        return list(deduped.values())

    # ═══════════════════════════════════════════════════════
    # Source: Holding Monitor (LLM position analysis)
    # ═══════════════════════════════════════════════════════

    def _collect_holding_monitor(self):
        """Generate signals from current holdings + LLM analysis."""
        try:
            from monitoring.holding_monitor import HoldingMonitor
            from management.position_manager import PositionManager
            from executors.longbridge import LongbridgeExecutor

            executor = LongbridgeExecutor()
            pm = PositionManager(self.db, executor)
            pm.sync_from_broker()
            monitor = HoldingMonitor(self.db, pm)
            alerts = monitor.run_full_check()

            for alert in alerts:
                symbol = alert.get("symbol", "")
                alert_type = alert.get("type", "")
                message = alert.get("message", "")
                confidence = alert.get("confidence", 0.5)

                if alert_type in ("llm_stop_loss",):
                    self.add(Signal(
                        symbol=symbol, direction="sell",
                        confidence=confidence, source="holding_monitor",
                        strength=0.9, reason=message, extra=alert
                    ))
                elif alert_type == "llm_take_profit":
                    self.add(Signal(
                        symbol=symbol, direction="sell",
                        confidence=confidence, source="holding_monitor",
                        strength=0.8, reason=message, extra=alert
                    ))
                elif alert_type == "llm_reduce":
                    # Bug fix #1: Carry suggested_weight so execution layer can calculate partial sell
                    # instead of dumping the entire position (which gets blocked by risk controls)
                    sig_extra = alert.copy()
                    sig_extra["_is_partial_sell"] = True
                    self.add(Signal(
                        symbol=symbol, direction="sell",
                        confidence=confidence, source="holding_monitor",
                        strength=0.7, reason=message, extra=sig_extra
                    ))
                elif alert_type == "llm_add":
                    self.add(Signal(
                        symbol=symbol, direction="buy",
                        confidence=confidence, source="holding_monitor",
                        strength=0.7, reason=message, extra=alert
                    ))
                elif alert_type == "仓位集中度":
                    # Hard-coded risk: concentration warning → sell signal
                    self.add(Signal(
                        symbol=symbol, direction="sell",
                        confidence=0.75, source="holding_monitor",
                        strength=0.6, reason=message, extra=alert
                    ))

        except Exception as e:
            logger.error(f"Holding monitor signal collection failed: {e}")

    # ═══════════════════════════════════════════════════════
    # Source: Screener (multi-factor stock screening)
    # ═══════════════════════════════════════════════════════

    def _collect_screener(self):
        """Generate buy signals from high-scoring screener results."""
        try:
            from analysis.screener import StockScreener
            screener = StockScreener(self.db)
            results = screener.screen(top_n=20, min_score=0.3)

            if not results:
                return

            # Get current holdings to avoid duplicates
            held = set()
            for row in self.db.conn.execute(
                "SELECT symbol FROM holdings WHERE active = 1 AND quantity > 0"
            ).fetchall():
                held.add(row[0])

            # Score threshold for buy signal
            avg_score = sum(r["total_score"] for r in results) / len(results)

            for r in results:
                symbol = r["symbol"]
                score = r["total_score"]

                # Already holding → skip (handled by holding_monitor)
                if symbol in held:
                    continue

                # Above average score → buy signal
                if score > avg_score:
                    confidence = min(0.9, 0.5 + score * 0.4)
                    strength = score

                    reasons = []
                    if r.get("momentum", 0.5) > 0.7: reasons.append("动量强劲")
                    if r.get("news_heat", 0.5) > 0.7: reasons.append("新闻热度高")
                    if r.get("social_heat", 0.5) > 0.7: reasons.append("社交讨论活跃")
                    if r.get("sentiment_delta", 0.5) > 0.7: reasons.append("情绪改善")
                    if r.get("volume_surge", 0.5) > 0.7: reasons.append("量能放大")

                    reason = " | ".join(reasons) if reasons else f"综合评分 {score:.3f}"

                    self.add(Signal(
                        symbol=symbol, direction="buy",
                        confidence=confidence, source="screener",
                        strength=strength, reason=reason, extra=r
                    ))

        except Exception as e:
            logger.error(f"Screener signal collection failed: {e}")

    # ═══════════════════════════════════════════════════════
    # Source: Sentiment (News/Reddit sentiment analysis)
    # ═══════════════════════════════════════════════════════

    def _collect_sentiment(self):
        """Generate signals from sentiment analysis of news and social media."""
        try:
            now = datetime.now()
            two_days = (now - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")

            # Get all symbols with recent data
            rows = self.db.conn.execute(
                """SELECT DISTINCT symbol FROM data_points WHERE timestamp >= ?""",
                (two_days,)
            ).fetchall()

            symbols = [r[0] for r in rows]

            for symbol in symbols:
                # Recent sentiment average
                recent_sent = self.db.conn.execute(
                    """SELECT AVG(sentiment_score), COUNT(*) FROM data_points
                       WHERE symbol = ? AND sentiment_score IS NOT NULL
                       AND timestamp >= ?""",
                    (symbol, two_days)
                ).fetchone()

                if not recent_sent or recent_sent[0] is None or recent_sent[1] < 3:
                    continue

                avg_sent, count = recent_sent

                # Compare with prior period (3-5 days ago)
                five_days = (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
                older_sent = self.db.conn.execute(
                    """SELECT AVG(sentiment_score) FROM data_points
                       WHERE symbol = ? AND sentiment_score IS NOT NULL
                       AND timestamp >= ? AND timestamp < ?""",
                    (symbol, five_days, two_days)
                ).fetchone()

                prev_sent = older_sent[0] if older_sent and older_sent[0] is not None else 0.0
                sentiment_change = avg_sent - prev_sent

                # Generate signal based on sentiment
                if avg_sent > 0.7 and sentiment_change > 0.1:
                    # Strong positive + improving
                    confidence = min(0.85, 0.5 + avg_sent * 0.3 + count * 0.01)
                    self.add(Signal(
                        symbol=symbol, direction="buy",
                        confidence=confidence, source="sentiment",
                        strength=avg_sent,
                        reason=f"情绪乐观 ({avg_sent:.2f}) 且持续改善 (+{sentiment_change:.2f}), {count} 条数据"
                    ))
                elif avg_sent < -0.5 and sentiment_change < -0.1:
                    # Strong negative + deteriorating
                    confidence = min(0.8, 0.5 + abs(avg_sent) * 0.3)
                    self.add(Signal(
                        symbol=symbol, direction="sell",
                        confidence=confidence, source="sentiment",
                        strength=abs(avg_sent),
                        reason=f"情绪恶化 ({avg_sent:.2f}) 且持续恶化 ({sentiment_change:.2f}), {count} 条数据"
                    ))

        except Exception as e:
            logger.error(f"Sentiment signal collection failed: {e}")

    # ═══════════════════════════════════════════════════════
    # Source: Factors (factor-based strategy signals)
    # ═══════════════════════════════════════════════════════

    def _collect_factors(self):
        """Generate signals from factor analysis."""
        try:
            now = datetime.now()
            two_days = (now - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")

            # Get latest factors for all symbols
            rows = self.db.conn.execute(
                """SELECT DISTINCT symbol FROM factors WHERE date >= ?""",
                (two_days,)
            ).fetchall()

            symbols = [r[0] for r in rows]

            for symbol in symbols:
                factors = {}
                for fname in ("momentum", "rsi", "volatility", "value", "quality"):
                    row = self.db.conn.execute(
                        """SELECT factor_value FROM factors
                           WHERE symbol = ? AND factor_name = ?
                           ORDER BY date DESC LIMIT 1""",
                        (symbol, fname)
                    ).fetchone()
                    if row and row[0] is not None:
                        factors[fname] = float(row[0])

                if not factors:
                    continue

                signals_generated = []

                # Momentum strategy: high momentum → buy
                if factors.get("momentum", 0) > 0.5:
                    confidence = min(0.85, 0.5 + factors["momentum"] * 0.3)
                    self.add(Signal(
                        symbol=symbol, direction="buy",
                        confidence=confidence, source="factors",
                        strength=factors["momentum"],
                        reason=f"动量因子 {factors['momentum']:.3f} 强势"
                    ))
                    signals_generated.append("momentum_buy")

                # RSI oversold → buy (mean reversion)
                if factors.get("rsi", 0.5) < 0.2:
                    self.add(Signal(
                        symbol=symbol, direction="buy",
                        confidence=0.7, source="factors",
                        strength=1 - factors["rsi"],
                        reason=f"RSI 超卖 ({factors['rsi']:.3f}), 均值回归机会"
                    ))
                    signals_generated.append("rsi_buy")

                # RSI overbought → sell
                elif factors.get("rsi", 0.5) > 0.8:
                    self.add(Signal(
                        symbol=symbol, direction="sell",
                        confidence=0.65, source="factors",
                        strength=factors["rsi"],
                        reason=f"RSI 超买 ({factors['rsi']:.3f}), 回调风险"
                    ))
                    signals_generated.append("rsi_sell")

                # Low quality → sell
                if factors.get("quality", 0) < -0.3:
                    self.add(Signal(
                        symbol=symbol, direction="sell",
                        confidence=0.6, source="factors",
                        strength=abs(factors["quality"]),
                        reason=f"质量因子恶化 ({factors['quality']:.3f})"
                    ))
                    signals_generated.append("quality_sell")

        except Exception as e:
            logger.error(f"Factor signal collection failed: {e}")

    # ═══════════════════════════════════════════════════════
    # Source: SEC Filings (Fix #8 - NEW)
    # ═══════════════════════════════════════════════════════

    def _collect_sec_filing(self, symbols: List[str] = None):
        """Generate signals from SEC filing analysis (insider trading, 13F, etc.)."""
        try:
            if not symbols:
                symbols = [r[0] for r in self.db.conn.execute(
                    "SELECT DISTINCT symbol FROM holdings WHERE active = 1"
                ).fetchall()]

            for symbol in symbols:
                # Get recent SEC filings (last 30 days)
                thirty_days = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
                filings = self.db.conn.execute(
                    """SELECT source, content, timestamp FROM data_points
                       WHERE symbol = ? AND source IN ('sec', 'sec_filing', 'sec_insider')
                       AND timestamp >= ?
                       ORDER BY timestamp DESC LIMIT 5""",
                    (symbol, thirty_days)
                ).fetchall()

                if not filings:
                    continue

                # Analyze filing types for signals
                insider_buys = 0
                insider_sells = 0
                for filing in filings:
                    source, content, ts = filing
                    content_lower = (content or "").lower()
                    if any(kw in content_lower for kw in ["purchase", "buy", "acquisition"]):
                        insider_buys += 1
                    if any(kw in content_lower for kw in ["sale", "sell", "disposition"]):
                        insider_sells += 1

                if insider_buys >= 2:
                    confidence = min(0.75, 0.5 + insider_buys * 0.1)
                    self.add(Signal(
                        symbol=symbol, direction="buy",
                        confidence=confidence, source="sec_filing",
                        strength=0.6,
                        reason=f"SEC 内幕交易: {insider_buys} 次买入信号 (30天)"
                    ))
                elif insider_sells >= 3:
                    self.add(Signal(
                        symbol=symbol, direction="sell",
                        confidence=0.6, source="sec_filing",
                        strength=0.5,
                        reason=f"SEC 内幕交易: {insider_sells} 次卖出信号 (30天)"
                    ))

        except Exception as e:
            logger.error(f"SEC filing signal collection failed: {e}")

    # ═══════════════════════════════════════════════════════
    # Source: TradingAgents (v6.0: enabled)
    # ═══════════════════════════════════════════════════════

    def _collect_trading_agents(self, symbols: List[str] = None):
        """
        Generate signals from TradingAgents multi-agent analysis.
        v6.0: Parses natural language output into structured TradeSignal.
        Only analyzes top candidates from screener (not all symbols).
        """
        try:
            # Only analyze symbols that already have some signal (from other sources)
            # This avoids running expensive TA analysis on every symbol
            candidate_symbols = set()
            for s in self.signals:
                candidate_symbols.add(s.symbol)

            if not candidate_symbols:
                logger.info("TradingAgents: no candidate symbols, skipping")
                return

            # Limit to top 5 by priority to control cost
            candidates = sorted(self.signals, key=lambda s: s.priority(), reverse=True)[:5]
            candidate_symbols = [s.symbol for s in candidates]

            logger.info(f"TradingAgents: analyzing top {len(candidate_symbols)} symbols: {candidate_symbols}")

            from tradingagents.main import run_trading_analysis
            trading_date = datetime.now().strftime("%Y-%m-%d")

            for symbol in candidate_symbols:
                try:
                    result = run_trading_analysis(
                        stock_symbol=symbol,
                        trading_date=trading_date,
                        market="US",
                    )

                    # Parse the decision
                    decision_text = result.get("decision", "")
                    trade_signal = self._parse_ta_decision(symbol, decision_text, result)

                    if trade_signal:
                        self.add(trade_signal)

                except Exception as e:
                    logger.warning(f"TradingAgents analysis failed for {symbol}: {e}")

        except Exception as e:
            logger.error(f"TradingAgents signal collection failed: {e}")

    def _parse_ta_decision(self, symbol: str, decision_text: str, result: dict) -> Optional[TradeSignal]:
        """
        Parse TradingAgents natural language decision into structured TradeSignal.
        v6.0: This is a fallback until TA outputs JSON directly (Phase 1 step B).
        """
        import re

        text_lower = decision_text.lower()

        # Determine direction
        direction = None
        if any(w in text_lower for w in ["buy", "purchase", "enter", "accumulate", "long"]):
            direction = SignalDirection.BUY
        elif any(w in text_lower for w in ["sell", "exit", "close", "liquidate", "short"]):
            direction = SignalDirection.SELL
        else:
            direction = SignalDirection.HOLD

        if direction == SignalDirection.HOLD:
            return None  # No action needed

        # Extract confidence (look for patterns like "confidence: 0.75" or "75%")
        confidence = 0.5
        conf_match = re.search(r'(?:confidence|conf)[\s:：]*(\d+\.?\d*)\s*%?', text_lower)
        if conf_match:
            val = float(conf_match.group(1))
            confidence = val / 100.0 if val > 1 else val

        # Extract rating if present
        rating_match = re.search(r'(?:rating|评级)[\s:：]*(buy|sell|hold|overweight|underweight)', text_lower)
        if rating_match:
            rating = rating_match.group(1).lower()
            if rating in ("buy", "overweight"):
                direction = SignalDirection.BUY
                confidence = max(confidence, 0.65)
            elif rating in ("sell", "underweight"):
                direction = SignalDirection.SELL
                confidence = max(confidence, 0.6)

        return TradeSignal(
            symbol=symbol,
            direction=direction,
            confidence=confidence,
            source=SignalSource.TRADING_AGENTS,
            strength=0.8,
            reason=decision_text[:500],  # Truncate for storage
            extra={"full_decision": decision_text, "result": result},
        )


def collect_all_signals(db=None, min_confidence: float = 0.5,
                        symbols: List[str] = None) -> List[Dict]:
    """Convenience function: collect and return signals as dicts."""
    from storage import Database

    need_close = db is None
    if db is None:
        db = Database()

    hub = SignalHub(db, min_confidence=min_confidence)
    signals = hub.collect_all(symbols)

    if need_close:
        db.close()

    return [s.to_dict() for s in hub.get_tradable_signals()]


def main():
    import argparse
    from storage import Database

    parser = argparse.ArgumentParser(description="Signal Hub — Aggregate trading signals")
    parser.add_argument("--min-confidence", type=float, default=0.5,
                        help="Minimum confidence threshold (0.0-1.0)")
    parser.add_argument("--symbol", default="", help="Filter by symbol")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    db = Database()
    symbols = [args.symbol] if args.symbol else None
    result = collect_all_signals(db, min_confidence=args.min_confidence, symbols=symbols)
    db.close()

    if args.json:
        import json
        print(json.dumps(result, indent=2, ensure_ascii=False))
    elif result:
        print(f"\n{'='*80}")
        print(f"Signal Hub Report ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        print(f"{'='*80}")
        for i, s in enumerate(result, 1):
            direction_emoji = {"buy": "🟢", "sell": "🔴"}.get(s["direction"], "⚪")
            print(f"  {i:2d}. {direction_emoji} {s['symbol']:8s} {s['direction'].upper():5s} "
                  f"conf={s['confidence']:.2f} strength={s['strength']:.2f} "
                  f"from={s['source']} priority={s['priority']:.2f}")
            print(f"      {s['reason']}")
        print(f"{'='*80}")
        print(f"Total: {len(result)} tradable signals")
    else:
        print("No tradable signals found")


if __name__ == "__main__":
    main()
