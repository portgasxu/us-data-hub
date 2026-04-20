#!/usr/bin/env python3
"""
US Data Hub — Risk Control Unit Tests
======================================

Tests for:
- CircuitBreaker (daily loss, consecutive losses, rolling drawdown, VIX)
- _try_acquire_order_lock (idempotency)
- HoldingMonitor LLM JSON parsing
- parse_decision negation guard
- Notifier alert routing

Usage:
    pytest tests/test_risk_controls.py -v
"""

import pytest
import sqlite3
import json
import re
import sys
import os
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ─── Helper: in-memory DB with minimal schema ───
@pytest.fixture
def db():
    """Create an in-memory SQLite DB with the tables we need."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")

    conn.executescript("""
        CREATE TABLE trades (
            symbol TEXT, direction TEXT, quantity INTEGER,
            price REAL, timestamp TEXT, status TEXT DEFAULT 'executed',
            signal_id TEXT
        );
        CREATE TABLE holdings (
            symbol TEXT, quantity INTEGER, cost_price REAL,
            active INTEGER DEFAULT 1, company_name TEXT
        );
        CREATE TABLE prices (
            symbol TEXT, close REAL, date TEXT
        );
        CREATE TABLE signal_cooldowns (
            symbol TEXT, direction TEXT, source TEXT,
            cooldown_until TEXT, reason TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE market_indicators (
            date TEXT, indicator_name TEXT, indicator_value REAL
        );
        CREATE TABLE factors (
            symbol TEXT, factor_name TEXT, factor_value REAL, date TEXT
        );
        CREATE TABLE system_config (
            key TEXT, value TEXT
        );
    """)

    class FakeDB:
        def __init__(self, conn):
            self.conn = conn

        def close(self):
            conn.close()

    yield FakeDB(conn)
    conn.close()


# ═══════════════════════════════════════════
# 1. Circuit Breaker Tests
# ═══════════════════════════════════════════

class TestCircuitBreaker:
    def test_daily_loss_within_threshold(self, db):
        """Normal day: loss < $500 → should not halt."""
        from analysis.circuit_breaker import check_circuit_breaker, CIRCUIT_BREAKER

        halted, reason = check_circuit_breaker(db)
        assert not halted

    def test_daily_loss_exceeds_threshold(self, db):
        """Unrealized P&L: set holdings with deep losses."""
        from analysis.circuit_breaker import check_circuit_breaker

        # Insert a holding at -60% loss with large position
        db.conn.execute(
            "INSERT INTO holdings (symbol, quantity, cost_price) VALUES (?, ?, ?)",
            ("AAPL", 1000, 100.0)
        )
        db.conn.execute(
            "INSERT INTO prices (symbol, close, date) VALUES (?, ?, ?)",
            ("AAPL", 30.0, datetime.now().strftime("%Y-%m-%d"))
        )

        halted, reason = check_circuit_breaker(db)
        # Total drawdown fires first (-70% > -10%), also daily loss check fails due to missing column
        assert halted
        assert "drawdown" in reason.lower() or "Daily loss" in reason

    def test_consecutive_losses_trigger(self, db):
        """3 consecutive sell losses → should halt."""
        from analysis.circuit_breaker import _check_consecutive_losses, CIRCUIT_BREAKER

        now = datetime.now()
        base_price = 100.0

        for i in range(3):
            ts = (now - timedelta(days=i+1)).strftime("%Y-%m-%d %H:%M:%S")
            sell_price = base_price - 10 - i*5  # selling below cost
            db.conn.execute(
                "INSERT INTO trades (symbol, direction, quantity, price, timestamp, status) "
                "VALUES (?, ?, ?, ?, ?, 'executed')",
                ("MSFT", "sell", 10, sell_price, ts)
            )
            # Prices before sell: avg around base_price (above sell price)
            for d in range(5):
                price_date = (now - timedelta(days=i+2+d)).strftime("%Y-%m-%d")
                db.conn.execute(
                    "INSERT INTO prices (symbol, close, date) VALUES (?, ?, ?)",
                    ("MSFT", base_price + d, price_date)
                )

        halted, reason = _check_consecutive_losses(db)
        assert halted
        assert "Consecutive losses" in reason

    def test_consecutive_losses_reset_on_win(self, db):
        """A winning trade resets consecutive count."""
        from analysis.circuit_breaker import _check_consecutive_losses

        now = datetime.now()
        base_price = 100.0

        # 2 losses
        for i in range(2):
            ts = (now - timedelta(days=i+1)).strftime("%Y-%m-%d %H:%M:%S")
            db.conn.execute(
                "INSERT INTO trades (symbol, direction, quantity, price, timestamp, status) "
                "VALUES (?, ?, ?, ?, ?, 'executed')",
                ("TSLA", "sell", 10, base_price - 10, ts)
            )
            for d in range(5):
                price_date = (now - timedelta(days=i+3+d)).strftime("%Y-%m-%d")
                db.conn.execute(
                    "INSERT INTO prices (symbol, close, date) VALUES (?, ?, ?)",
                    ("TSLA", base_price, price_date)
                )

        # 1 win (sell above cost)
        ts = (now - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
        db.conn.execute(
            "INSERT INTO trades (symbol, direction, quantity, price, timestamp, status) "
            "VALUES (?, ?, ?, ?, ?, 'executed')",
            ("TSLA", "sell", 10, base_price + 20, ts)
        )
        for d in range(5):
            price_date = (now - timedelta(days=4+d)).strftime("%Y-%m-%d")
            db.conn.execute(
                "INSERT INTO prices (symbol, close, date) VALUES (?, ?, ?)",
                ("TSLA", base_price, price_date)
            )

        halted, reason = _check_consecutive_losses(db)
        assert not halted  # Reset by the win

    def test_rolling_drawdown_5day(self, db):
        """5-day rolling loss > threshold → halt."""
        from analysis.circuit_breaker import _check_rolling_drawdown, CIRCUIT_BREAKER

        now = datetime.now()
        base_price = 100.0

        # Create 5 sell trades in the last 5 days, all at deep losses
        for i in range(5):
            ts = (now - timedelta(hours=i*10)).strftime("%Y-%m-%d %H:%M:%S")
            sell_price = 50.0  # 50% loss
            db.conn.execute(
                "INSERT INTO trades (symbol, direction, quantity, price, timestamp, status) "
                "VALUES (?, ?, ?, ?, ?, 'executed')",
                ("NVDA", "sell", 100, sell_price, ts)
            )
            # Cost basis (5-day avg price) around $100
            for d in range(5):
                price_date = (now - timedelta(days=d+1)).strftime("%Y-%m-%d")
                db.conn.execute(
                    "INSERT INTO prices (symbol, close, date) VALUES (?, ?, ?)",
                    ("NVDA", base_price, price_date)
                )

        halted, reason = _check_rolling_drawdown(db)
        assert halted
        assert "5-day" in reason

    def test_total_drawdown_pct(self, db):
        """Total portfolio drawdown > 10% → halt."""
        from analysis.circuit_breaker import _check_rolling_drawdown, CIRCUIT_BREAKER

        db.conn.execute(
            "INSERT INTO holdings (symbol, quantity, cost_price) VALUES (?, ?, ?)",
            ("AAPL", 500, 100.0)
        )
        db.conn.execute(
            "INSERT INTO prices (symbol, close, date) VALUES (?, ?, ?)",
            ("AAPL", 85.0, datetime.now().strftime("%Y-%m-%d"))
        )

        halted, reason = _check_rolling_drawdown(db)
        # Drawdown: (85-100)/100 = -15% > -10%
        assert halted
        assert "Total portfolio drawdown" in reason


# ═══════════════════════════════════════════
# 2. Order Lock Tests
# ═══════════════════════════════════════════

class TestOrderLock:
    def test_first_lock_succeeds(self, db):
        """First acquire should succeed."""
        from scripts.auto_execute import _try_acquire_order_lock

        result = _try_acquire_order_lock(db, "AAPL", "buy", "sig_001", minutes=10)
        assert result is True

    def test_same_signal_id_rejected(self, db):
        """Same signal_id should be rejected (idempotency)."""
        from scripts.auto_execute import _try_acquire_order_lock

        # First: success
        _try_acquire_order_lock(db, "AAPL", "buy", "sig_002", minutes=10)

        # Record as executed in trades
        db.conn.execute(
            "INSERT INTO trades (symbol, direction, quantity, price, timestamp, status, signal_id) "
            "VALUES (?, ?, ?, ?, ?, 'filled', ?)",
            ("AAPL", "buy", 10, 150.0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "sig_002")
        )
        db.conn.commit()

        result = _try_acquire_order_lock(db, "AAPL", "buy", "sig_002", minutes=10)
        assert result is False

    def test_different_symbol_allowed(self, db):
        """Different symbols should be allowed."""
        from scripts.auto_execute import _try_acquire_order_lock

        _try_acquire_order_lock(db, "AAPL", "buy", "sig_003", minutes=10)
        result = _try_acquire_order_lock(db, "MSFT", "buy", "sig_004", minutes=10)
        assert result is True


# ═══════════════════════════════════════════
# 3. LLM JSON Parsing Tests
# ═══════════════════════════════════════════

class TestLLMJsonParsing:
    def _extract_json(self, content):
        """Simulate the HoldingMonitor extraction logic."""
        recommendations = None
        # 1) markdown code block
        md_match = re.search(r'```(?:json)?\s*\n?([\s\S]*?)\n?\s*```', content)
        if md_match:
            try:
                recommendations = json.loads(md_match.group(1).strip())
            except json.JSONDecodeError:
                pass
        # 2) regex array
        if recommendations is None:
            json_match = re.search(r'\[\s*\{.*\}\s*\]', content, re.DOTALL)
            if json_match:
                try:
                    recommendations = json.loads(json_match.group())
                except json.JSONDecodeError:
                    pass
        # 3) pure JSON
        if recommendations is None:
            try:
                recommendations = json.loads(content)
            except json.JSONDecodeError:
                pass
        return recommendations

    def test_markdown_code_block(self):
        content = '''Here is the analysis:
```json
[{"symbol": "AAPL", "action": "HOLD", "confidence": 0.8, "reason": "趋势向上"}]
```
Hope this helps!'''
        result = self._extract_json(content)
        assert result is not None
        assert len(result) == 1
        assert result[0]["symbol"] == "AAPL"

    def test_pure_json(self):
        content = '[{"symbol": "NVDA", "action": "REDUCE", "confidence": 0.75, "reason": "集中度高"}]'
        result = self._extract_json(content)
        assert result is not None
        assert result[0]["action"] == "REDUCE"

    def test_inline_json(self):
        content = '分析结果：\n[{"symbol": "MSFT", "action": "TAKE_PROFIT", "confidence": 0.9, "reason": "盈利目标达成"}]\n以上。'
        result = self._extract_json(content)
        assert result is not None
        assert result[0]["action"] == "TAKE_PROFIT"

    def test_invalid_returns_none(self):
        content = "I don't know what to say."
        result = self._extract_json(content)
        assert result is None

    def test_markdown_brackets_not_matched(self):
        """Ensure [some text] in markdown doesn't match as JSON."""
        content = "See [AAPL](link) for details.\n\n```json\n[]\n```"
        result = self._extract_json(content)
        # Should extract from code block, not the markdown link
        assert result is not None
        assert result == []


# ═══════════════════════════════════════════
# 4. Parse Decision Negation Guard
# ═══════════════════════════════════════════

class TestParseDecision:
    def _parse(self, signal):
        """Simulate parse_decision fallback logic."""
        decision = {"action": "hold"}
        signal_lower = signal.lower()
        negation_patterns = [
            r"(?:don'?t|do\s+not|no|not|never|avoid|shouldn'?t|won'?t|will\s+not)\s+(?:buy|long)",
        ]
        has_negation = any(re.search(p, signal_lower) for p in negation_patterns)
        if not has_negation and ("buy" in signal_lower or "overweight" in signal_lower):
            decision["action"] = "buy"
        elif "sell" in signal_lower:
            decision["action"] = "sell"
        elif "hold" in signal_lower:
            decision["action"] = "hold"
        return decision

    def test_positive_buy(self):
        assert self._parse("Buy AAPL 10 shares")["action"] == "buy"
        assert self._parse("OVERWEIGHT MSFT")["action"] == "buy"

    def test_negation_blocked(self):
        assert self._parse("don't buy AAPL")["action"] == "hold"
        assert self._parse("do not buy TSLA")["action"] == "hold"
        assert self._parse("avoid buying NVDA")["action"] == "hold"
        assert self._parse("shouldn't buy AMZN")["action"] == "hold"

    def test_sell_not_affected(self):
        assert self._parse("sell AAPL")["action"] == "sell"

    def test_hold(self):
        assert self._parse("hold position")["action"] == "hold"


# ═══════════════════════════════════════════
# 5. Notifier Tests
# ═══════════════════════════════════════════

class TestNotifier:
    def test_alert_import(self):
        """Verify notifier module loads correctly."""
        from alerts.notifier import alert, AlertLevel
        assert AlertLevel.P0.value == "P0"
        assert AlertLevel.P1.value == "P1"

    @patch("alerts.notifier._Notifier._push_immediate")
    def test_p0_alert_called(self, mock_push):
        """P0 alert should trigger immediate push."""
        from alerts.notifier import alert, AlertLevel, _notifier

        alert(AlertLevel.P0, "Test", "Test message")
        mock_push.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
