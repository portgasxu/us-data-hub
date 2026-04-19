#!/usr/bin/env python3
"""
US Data Hub — 全局追溯 ID 生成器 (v6.0)
=======================================

所有 ID 格式: {PREFIX}_YYYYMMDD_HHMMSS_6位短UUID

ID 类型:
  - signal_id:        SIG_YYYYMMDD_HHMMSS_abcdef  (每个信号)
  - execution_id:     EXE_YYYYMMDD_HHMMSS_abcdef  (一次 auto_execute --full-loop)
  - collection_id:    COL_YYYYMMDD_HHMMSS_abcdef  (一次数据采集批次)
  - decision_trace_id: DEC_YYYYMMDD_HHMMSS_abcdef (一次 TA 分析决策)

追溯链:
  crontab → execution_id → signal_id → decision_trace_id → trade
"""

import uuid
from datetime import datetime


def _short_id() -> str:
    """生成 6 位短 UUID 字符串"""
    return uuid.uuid4().hex[:6]


def _timestamp() -> str:
    """生成 YYYYMMDD_HHMMSS 时间戳"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def generate_signal_id() -> str:
    """生成信号 ID: SIG_YYYYMMDD_HHMMSS_abcdef"""
    return f"SIG_{_timestamp()}_{_short_id()}"


def generate_execution_id() -> str:
    """
    生成执行批次 ID: EXE_YYYYMMDD_HHMMSS_abcdef
    一次 auto_execute --full-loop 一个 ID
    """
    return f"EXE_{_timestamp()}_{_short_id()}"


def generate_collection_id() -> str:
    """
    生成数据采集批次 ID: COL_YYYYMMDD_HHMMSS_abcdef
    一次数据采集周期一个 ID
    """
    return f"COL_{_timestamp()}_{_short_id()}"


def generate_decision_trace_id() -> str:
    """
    生成决策追溯 ID: DEC_YYYYMMDD_HHMMSS_abcdef
    一次 TradingAgents 分析一个 ID
    """
    return f"DEC_{_timestamp()}_{_short_id()}"
