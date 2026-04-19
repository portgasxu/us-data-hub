"""
US Data Hub — Storage Layer
Single Database class for all data operations.
Tables: data_points, prices, factors, holdings, trades,
        factor_performance, monitor_alerts, screener_history,
        collection_log, watchlist, schema_version
"""

import sqlite3
import os
import json
from datetime import datetime
from typing import Optional, List


class Database:
    """SQLite database manager — single instance for the entire project."""

    SCHEMA_VERSION = 2

    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "us_data_hub.db"
        )
        self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def init_schema(self):
        """Initialize ALL tables. Idempotent — safe to call multiple times."""
        conn = self.conn
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            -- Unified data points (all sources)
            CREATE TABLE IF NOT EXISTS data_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                symbol TEXT NOT NULL,
                source TEXT NOT NULL,
                type TEXT NOT NULL,
                title TEXT,
                content TEXT,
                sentiment_score REAL,
                tags TEXT,
                raw_data TEXT,
                source_id TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, symbol, source, type, source_id)
            );

            CREATE TABLE IF NOT EXISTS prices (
                date DATE NOT NULL,
                symbol TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                adj_close REAL, volume INTEGER,
                PRIMARY KEY (date, symbol)
            );

            CREATE TABLE IF NOT EXISTS factors (
                date DATE NOT NULL,
                symbol TEXT NOT NULL,
                factor_name TEXT NOT NULL,
                factor_value REAL,
                factor_meta TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol, factor_name)
            );

            CREATE TABLE IF NOT EXISTS holdings (
                symbol TEXT PRIMARY KEY,
                company_name TEXT,
                quantity INTEGER DEFAULT 0,
                cost_price REAL DEFAULT 0,
                available INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1,
                last_synced DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL,
                order_type TEXT DEFAULT 'LO',
                order_id TEXT,
                status TEXT DEFAULT 'Submitted',
                agent_signal TEXT,
                confidence REAL,
                factor_scores TEXT,
                stop_loss REAL,
                take_profit REAL,
                holding_period TEXT,
                actual_return REAL,
                signal_id TEXT DEFAULT '',
                execution_id TEXT DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS factor_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                factor_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                decision_date DATE NOT NULL,
                predicted_return REAL,
                actual_return REAL,
                accuracy INTEGER,
                holding_period TEXT,
                days_held INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS monitor_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                symbol TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                title TEXT,
                details TEXT,
                acknowledged INTEGER DEFAULT 0,
                signal_id TEXT DEFAULT '',
                execution_id TEXT DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS screener_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_time DATETIME NOT NULL,
                symbol TEXT NOT NULL,
                total_score REAL,
                dim_news_volume REAL,
                dim_social_heat REAL,
                dim_capital_flow REAL,
                dim_momentum REAL,
                dim_volatility REAL,
                dim_insider REAL,
                dim_mean_reversion REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS collection_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                collection_time DATETIME NOT NULL,
                symbol TEXT,
                records_fetched INTEGER DEFAULT 0,
                records_new INTEGER DEFAULT 0,
                status TEXT DEFAULT 'success',
                error_message TEXT,
                duration_ms INTEGER
            );

            CREATE TABLE IF NOT EXISTS watchlist (
                symbol TEXT PRIMARY KEY,
                company_name TEXT,
                sector TEXT,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                active INTEGER DEFAULT 1
            );

            -- Market indicators (VIX, macro data, etc.)
            CREATE TABLE IF NOT EXISTS market_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                indicator_name TEXT NOT NULL,
                indicator_value REAL,
                indicator_meta TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, indicator_name)
            );

            -- Signal cooldown tracking (Fix: prevent same blocked signal from repeating)
            CREATE TABLE IF NOT EXISTS signal_cooldowns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                source TEXT NOT NULL,
                blocked_at DATETIME NOT NULL,
                cooldown_until DATETIME NOT NULL,
                reason TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_cooldown_symbol_dir ON signal_cooldowns(symbol, direction);
            CREATE INDEX IF NOT EXISTS idx_cooldown_until ON signal_cooldowns(cooldown_until);

            -- v6.0: Execution log (one row per auto_execute --full-loop run)
            CREATE TABLE IF NOT EXISTS execution_log (
                execution_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT DEFAULT 'RUNNING',
                signals_collected INTEGER DEFAULT 0,
                trades_executed INTEGER DEFAULT 0,
                errors TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            -- v6.0: Signal log (tracks every signal's lifecycle)
            CREATE TABLE IF NOT EXISTS signal_log (
                signal_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                source TEXT NOT NULL,
                confidence REAL,
                execution_id TEXT,
                action_taken TEXT,
                rejection_reason TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            -- v6.0 indexes moved to migrate_v6() for existing DB compatibility

            -- Indexes
            CREATE INDEX IF NOT EXISTS idx_dp_symbol ON data_points(symbol);
            CREATE INDEX IF NOT EXISTS idx_dp_source ON data_points(source);
            CREATE INDEX IF NOT EXISTS idx_dp_timestamp ON data_points(timestamp);
            CREATE INDEX IF NOT EXISTS idx_dp_type ON data_points(type);
            CREATE INDEX IF NOT EXISTS idx_dp_sentiment ON data_points(sentiment_score);
            CREATE INDEX IF NOT EXISTS idx_prices_symbol_date ON prices(symbol, date);
            CREATE INDEX IF NOT EXISTS idx_factors_symbol ON factors(symbol);
            CREATE INDEX IF NOT EXISTS idx_factors_name ON factors(factor_name);
            CREATE INDEX IF NOT EXISTS idx_cl_source_time ON collection_log(source, collection_time);
            CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
            CREATE INDEX IF NOT EXISTS idx_factor_perf_symbol ON factor_performance(symbol);
            CREATE INDEX IF NOT EXISTS idx_alerts_symbol_time ON monitor_alerts(symbol, timestamp);
            CREATE INDEX IF NOT EXISTS idx_screener_run ON screener_history(run_time);
            CREATE INDEX IF NOT EXISTS idx_market_ind_name ON market_indicators(indicator_name);
            CREATE INDEX IF NOT EXISTS idx_market_ind_date ON market_indicators(date);
        """)

        conn.execute(
            "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
            (self.SCHEMA_VERSION,)
        )
        conn.commit()

    # ─── Data Points ───

    def insert_data_point(self, data: dict) -> bool:
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO data_points
                (timestamp, symbol, source, type, title, content,
                 sentiment_score, tags, raw_data, source_id)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                data.get('timestamp'), data.get('symbol'), data.get('source'),
                data.get('type'), data.get('title'),
                json.dumps(data.get('content', {})),
                data.get('sentiment_score'),
                ','.join(data.get('tags', [])) if data.get('tags') else None,
                json.dumps(data.get('raw_data', {})),
                data.get('source_id'),
            ))
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            return False

    def insert_bulk_data_points(self, data_list: list) -> tuple:
        new_count = 0
        for data in data_list:
            if self.insert_data_point(data):
                new_count += 1
        return new_count, len(data_list)

    def query_data_points(self, symbol: str = None, source: str = None,
                          type_filter: str = None, days: int = 30,
                          limit: int = 100) -> list:
        q = "SELECT * FROM data_points WHERE 1=1"
        params = []
        if symbol: q += " AND symbol = ?"; params.append(symbol)
        if source: q += " AND source = ?"; params.append(source)
        if type_filter: q += " AND type = ?"; params.append(type_filter)
        if days: q += " AND timestamp >= date('now', ?)"; params.append(f'-{days} days')
        q += " ORDER BY timestamp DESC LIMIT ?"; params.append(limit)
        return [dict(r) for r in self.conn.execute(q, params).fetchall()]

    # ─── Prices ───

    def insert_price(self, symbol: str, date_str: str, price_data: dict):
        self.conn.execute("""
            INSERT OR REPLACE INTO prices
            (date, symbol, open, high, low, close, adj_close, volume)
            VALUES (?,?,?,?,?,?,?,?)
        """, (date_str, symbol, price_data.get('open'), price_data.get('high'),
              price_data.get('low'), price_data.get('close'),
              price_data.get('adj_close'), price_data.get('volume')))
        self.conn.commit()

    def query_prices(self, symbol: str, days: int = 365) -> list:
        rows = self.conn.execute("""
            SELECT * FROM prices WHERE symbol = ? AND date >= date('now', ?)
            ORDER BY date DESC
        """, (symbol, f'-{days} days')).fetchall()
        return [dict(r) for r in rows]

    # ─── Factors ───

    def insert_factor(self, symbol: str, date_str: str,
                      factor_name: str, value: float, meta: dict = None):
        self.conn.execute("""
            INSERT OR REPLACE INTO factors
            (date, symbol, factor_name, factor_value, factor_meta)
            VALUES (?,?,?,?,?)
        """, (date_str, symbol, factor_name, value,
              json.dumps(meta) if meta else None))
        self.conn.commit()

    def query_factors(self, symbol: str = None, factor_name: str = None,
                      days: int = 90) -> list:
        q = "SELECT * FROM factors WHERE 1=1"; params = []
        if symbol: q += " AND symbol = ?"; params.append(symbol)
        if factor_name: q += " AND factor_name = ?"; params.append(factor_name)
        if days: q += " AND date >= date('now', ?)"; params.append(f'-{days} days')
        q += " ORDER BY date DESC"
        return [dict(r) for r in self.conn.execute(q, params).fetchall()]

    # ─── Holdings ───

    def get_holdings(self) -> list:
        rows = self.conn.execute("""
            SELECT h.*, p.close as current_price,
                   CASE WHEN p.close IS NOT NULL AND h.cost_price > 0
                        THEN (p.close - h.cost_price) / h.cost_price * 100 ELSE 0 END as pnl_pct,
                   CASE WHEN p.close IS NOT NULL
                        THEN (p.close - h.cost_price) * h.quantity ELSE 0 END as pnl_amount
            FROM holdings h
            LEFT JOIN (SELECT symbol, close FROM prices
                       WHERE (symbol, date) IN
                       (SELECT symbol, MAX(date) FROM prices GROUP BY symbol)) p
            ON h.symbol = p.symbol WHERE h.active = 1 ORDER BY h.symbol
        """).fetchall()
        return [dict(r) for r in rows]

    # ─── Collection Log ───

    def log_collection(self, source: str, symbol: str = None,
                       fetched: int = 0, new: int = 0,
                       status: str = 'success', error: str = None,
                       duration_ms: int = 0):
        self.conn.execute("""
            INSERT INTO collection_log
            (source, collection_time, symbol, records_fetched,
             records_new, status, error_message, duration_ms)
            VALUES (?,?,?,?,?,?,?,?)
        """, (source, datetime.now(), symbol, fetched, new, status, error, duration_ms))
        self.conn.commit()

    # ─── Stats ───

    # ─── v6.0: Migration for existing databases ───

    def migrate_v6(self):
        """
        v6.0 数据库迁移 — 为现有表添加新字段和新表。
        安全幂等：使用 IF NOT EXISTS 和异常捕获。
        """
        import logging
        logger = logging.getLogger(__name__)

        # 1. 给 trades 表添加 signal_id, execution_id
        try:
            self.conn.execute("ALTER TABLE trades ADD COLUMN signal_id TEXT DEFAULT ''")
            logger.info("Migration: added signal_id to trades")
        except Exception:
            pass
        try:
            self.conn.execute("ALTER TABLE trades ADD COLUMN execution_id TEXT DEFAULT ''")
            logger.info("Migration: added execution_id to trades")
        except Exception:
            pass

        # 2. 给 monitor_alerts 表添加 signal_id, execution_id
        try:
            self.conn.execute("ALTER TABLE monitor_alerts ADD COLUMN signal_id TEXT DEFAULT ''")
        except Exception:
            pass
        try:
            self.conn.execute("ALTER TABLE monitor_alerts ADD COLUMN execution_id TEXT DEFAULT ''")
        except Exception:
            pass

        # 3. 创建 execution_log 表
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS execution_log (
                    execution_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    status TEXT DEFAULT 'RUNNING',
                    signals_collected INTEGER DEFAULT 0,
                    trades_executed INTEGER DEFAULT 0,
                    errors TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception:
            pass

        # 4. 创建 signal_log 表
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS signal_log (
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    source TEXT NOT NULL,
                    confidence REAL,
                    execution_id TEXT,
                    action_taken TEXT,
                    rejection_reason TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception:
            pass

        # 5. 创建 event_bus 表
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS event_bus (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    consumed INTEGER NOT NULL DEFAULT 0,
                    consumed_by TEXT DEFAULT '',
                    consumed_at TEXT DEFAULT '',
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    parent_event_id INTEGER DEFAULT NULL,
                    priority INTEGER DEFAULT 0
                )
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_event_bus_type_consumed
                ON event_bus(event_type, consumed, priority DESC, created_at)
            """)
        except Exception:
            pass

        # 6. 创建 shadow_trades 表
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS shadow_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    quantity REAL,
                    entry_price REAL,
                    exit_price REAL DEFAULT 0,
                    signal_id TEXT,
                    execution_id TEXT,
                    strategy_name TEXT DEFAULT 'shadow_v1',
                    entry_at TEXT,
                    exit_at TEXT,
                    pnl REAL DEFAULT 0,
                    pnl_pct REAL DEFAULT 0,
                    status TEXT DEFAULT 'OPEN',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception:
            pass

        # 7. 创建索引
        try:
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_log_symbol ON signal_log(symbol)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_log_execution ON signal_log(execution_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_execution_log_status ON execution_log(status)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_signal_id ON trades(signal_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_execution_id ON trades(execution_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_signal_id ON monitor_alerts(signal_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_execution_id ON monitor_alerts(execution_id)")
        except Exception:
            pass

        self.conn.commit()
        logger.info("v6.0 migration completed")

    # ─── Stats ───

    def get_stats(self) -> dict:
        stats = {}
        for table in ['data_points', 'prices', 'factors', 'holdings',
                       'trades', 'collection_log', 'monitor_alerts']:
            count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            stats[table] = count
        source_counts = {}
        for r in self.conn.execute(
            "SELECT source, COUNT(*) FROM data_points GROUP BY source"
        ).fetchall():
            source_counts[r[0]] = r[1]
        stats['by_source'] = source_counts
        return stats

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()
