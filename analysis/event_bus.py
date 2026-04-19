#!/usr/bin/env python3
"""
US Data Hub — 轻量事件总线 (v6.0)
=================================

替代 crontab 直接调用的全局事件调度层。

架构:
  发布者 (crontab / watcher / auto_execute)
    → Event.publish() → SQLite event_bus 表
    ↓
  消费者 (screening_worker / analysis_worker / execution_worker)
    → EventBus.consume() → 处理任务
    → Event.publish() → 触发下游任务（链式调用）

事件流转示例:
  CRON_MORNING → publish SCREENING_TRIGGER
    → screening_worker consume → 选股完成 → publish ANALYSIS_TRIGGER
    → analysis_worker consume → TA 分析完成 → publish EXECUTION_TRIGGER
    → execution_worker consume → 执行完成 → publish DAILY_SUMMARY
"""

import sqlite3
import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Event:
    event_id: int
    event_type: str
    payload: dict
    created_at: str
    consumed: bool = False
    consumed_by: str = ""
    consumed_at: str = ""
    max_retries: int = 3
    retry_count: int = 0
    parent_event_id: int = None
    priority: int = 0


class EventType:
    """全链路事件类型定义"""
    # 采集层
    COLLECTION_TRIGGER = "COLLECTION_TRIGGER"
    COLLECTION_COMPLETE = "COLLECTION_COMPLETE"

    # 监控层
    NEWS_SURGE_DETECTED = "NEWS_SURGE_DETECTED"
    PRICE_ALERT = "PRICE_ALERT"

    # 选股层
    SCREENING_TRIGGER = "SCREENING_TRIGGER"
    SCREENING_COMPLETE = "SCREENING_COMPLETE"

    # 分析层
    ANALYSIS_TRIGGER = "ANALYSIS_TRIGGER"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"

    # 执行层
    EXECUTION_TRIGGER = "EXECUTION_TRIGGER"
    EXECUTION_COMPLETE = "EXECUTION_COMPLETE"

    # 风控层
    RISK_ALERT = "RISK_ALERT"
    CIRCUIT_BREAKER = "CIRCUIT_BREAKER"

    # 复盘层
    DAILY_REVIEW = "DAILY_REVIEW"
    STRATEGY_VALIDATION = "STRATEGY_VALIDATION"


class EventBus:
    """轻量事件总线 — 基于 SQLite"""

    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "data", "us_data_hub.db"
            )
        self.db_path = db_path
        self._init_table()

    def _init_table(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
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
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_event_bus_type_consumed
                ON event_bus(event_type, consumed, priority DESC, created_at)
            """)

    def publish(self, event_type: str, payload: dict,
                parent_event_id: int = None, priority: int = 0) -> int:
        """发布事件，返回 event_id"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """INSERT INTO event_bus
                   (event_type, payload, created_at, parent_event_id, priority)
                   VALUES (?, ?, ?, ?, ?)""",
                (event_type, json.dumps(payload, ensure_ascii=False),
                 datetime.now().isoformat(), parent_event_id, priority)
            )
            event_id = cursor.lastrowid
            logger.info(f"📨 Event published: {event_type} (id={event_id})")
            return event_id

    def consume(self, event_type: str, consumer: str, limit: int = 10) -> List[Event]:
        """消费指定类型的未处理事件"""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """SELECT event_id, event_type, payload, created_at,
                          consumed, consumed_by, consumed_at,
                          retry_count, max_retries, parent_event_id, priority
                   FROM event_bus
                   WHERE event_type = ? AND consumed = 0 AND retry_count < max_retries
                   ORDER BY priority DESC, created_at ASC LIMIT ?""",
                (event_type, limit)
            ).fetchall()

            events = []
            for row in rows:
                event = Event(
                    event_id=row[0], event_type=row[1],
                    payload=json.loads(row[2]), created_at=row[3],
                    consumed=bool(row[4]), consumed_by=row[5],
                    consumed_at=row[6], retry_count=row[7],
                    max_retries=row[8], parent_event_id=row[9],
                    priority=row[10],
                )
                events.append(event)

                conn.execute(
                    """UPDATE event_bus SET consumed = 1, consumed_by = ?, consumed_at = ?
                       WHERE event_id = ?""",
                    (consumer, datetime.now().isoformat(), event.event_id)
                )

            return events

    def fail_event(self, event_id: int, error: str = ""):
        """标记事件处理失败，增加重试计数"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """UPDATE event_bus SET retry_count = retry_count + 1,
                   consumed = 0, consumed_by = '', consumed_at = ''
                   WHERE event_id = ?""",
                (event_id,)
            )
            logger.warning(f"⚠️ Event {event_id} failed (retry): {error}")

    def cleanup(self, days: int = 7):
        """清理 N 天前的已消费事件"""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM event_bus WHERE consumed = 1 AND consumed_at < ?",
                (cutoff,)
            )

    def pending_count(self, event_type: str = None) -> int:
        """查询待处理事件数量"""
        with sqlite3.connect(self.db_path) as conn:
            if event_type:
                row = conn.execute(
                    "SELECT COUNT(*) FROM event_bus WHERE event_type = ? AND consumed = 0",
                    (event_type,)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) FROM event_bus WHERE consumed = 0"
                ).fetchone()
            return row[0] if row else 0

    def status(self) -> dict:
        """事件总线状态"""
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM event_bus").fetchone()[0]
            pending = conn.execute("SELECT COUNT(*) FROM event_bus WHERE consumed = 0").fetchone()[0]
            consumed = conn.execute("SELECT COUNT(*) FROM event_bus WHERE consumed = 1").fetchone()[0]

            by_type = {}
            for row in conn.execute(
                "SELECT event_type, COUNT(*) as cnt, SUM(CASE WHEN consumed=0 THEN 1 ELSE 0 END) as pending "
                "FROM event_bus GROUP BY event_type"
            ).fetchall():
                by_type[row[0]] = {"total": row[1], "pending": row[2]}

            return {
                "total": total,
                "pending": pending,
                "consumed": consumed,
                "by_type": by_type,
            }
