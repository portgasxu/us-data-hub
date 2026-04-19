"""
session_strategy.py — 交易时段策略模块
=======================================

功能:
  1. 精确判断当前交易时段（盘前/盘中/盘后/夜盘/休市）
  2. 自动区分冬令时和夏令时
  3. 美国节假日判断（Phase 3: 使用 holiday_calendar 模块）
  4. 不同时段对应不同的分析决策策略

时段划分（北京时间）:
  - 深度夜盘: 05:00-15:00 (冬) / 06:00-15:00 (夏) → 🟢 仅数据维护
  - 盘前准备: 15:00-21:30 (冬) / 15:00-22:30 (夏) → 🟡 选股+简报
  - 盘前交易: 21:30-22:30 (冬) / 22:30-23:30 (夏) → 🟠 仅持仓调整
  - 盘中:     22:30-04:00 (冬) / 23:30-05:00 (夏) → 🔴 全功能交易
  - 盘后:     04:00-05:00 (冬) / 05:00-06:00 (夏) → 🟠 仅止损/止盈
  - 休市:     周末/节假日 → 🟢 复盘+分析
"""

import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def is_us_holiday(dt: datetime = None) -> tuple:
    """
    判断是否是美国市场节假日 (Phase 3: 使用 holiday_calendar 模块)

    Returns:
        (is_holiday, holiday_name)
    """
    if dt is None:
        dt = datetime.now(timezone(timedelta(hours=8)))

    from analysis.holiday_calendar import is_us_market_holiday
    return is_us_market_holiday(dt)


def _get_nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> int:
    """
    获取某月第 n 个星期几的日期号
    weekday: 0=Monday, 6=Sunday
    """
    from calendar import monthcalendar
    cal = monthcalendar(year, month)
    # monthcalendar returns weeks; find the nth occurrence of the weekday
    count = 0
    for week in cal:
        if week[weekday] != 0:
            count += 1
            if count == n:
                return week[weekday]
    return week[weekday]  # fallback to last occurrence


def is_summer_time(dt: datetime = None) -> bool:
    """
    判断当前是否为美国夏令时
    美国夏令时: 3月第2个星期日 02:00 至 11月第1个星期日 02:00（美东时间）
    """
    if dt is None:
        dt = datetime.now(timezone(timedelta(hours=8)))  # 北京时间

    # 转为美东时间用于夏令时判断
    utc_dt = dt.astimezone(timezone.utc)
    # 先假设冬令时 (UTC-5)，计算出日期
    est = utc_dt + timedelta(hours=-5)
    year = est.year

    # 3月第2个星期日 → 只返回日期号
    dst_start_day = _get_nth_weekday_of_month(year, 3, 6, 2)  # 6=Sunday
    # 11月第1个星期日
    dst_end_day = _get_nth_weekday_of_month(year, 11, 6, 1)

    # 构造夏令时开始/结束时间（用 03:00 避开 02:00 不存在的时刻）
    start_dt = datetime(year, 3, dst_start_day, 3)
    end_dt = datetime(year, 11, dst_end_day, 3)

    # 去掉 est 的时区信息用于比较
    est_naive = est.replace(tzinfo=None)
    return start_dt <= est_naive < end_dt


# ═══════════════════════════════════════════════════════
# 时段定义（北京时间）
# ═══════════════════════════════════════════════════════

SESSION_WINTER = {
    # 冬令时时段定义 (UTC+8)
    "deep_night":      {"start": (5, 0),  "end": (15, 0)},
    "pre_market_prep": {"start": (15, 0), "end": (21, 30)},
    "pre_market_trade":{"start": (21, 30),"end": (22, 30)},
    "market_open":     {"start": (22, 30),"end": (4, 0)},   # 跨天
    "after_hours":     {"start": (4, 0),  "end": (5, 0)},
}

SESSION_SUMMER = {
    # 夏令时时段定义 (UTC+8)
    # 美东 9:30 AM = UTC-4 + 12h = 北京 21:30
    # 美东 16:00   = UTC-4 + 12h = 北京 04:00 (次日)
    "deep_night":      {"start": (6, 0),  "end": (15, 0)},
    "pre_market_prep": {"start": (15, 0), "end": (21, 30)},
    "pre_market_trade":{"start": (21, 30),"end": (22, 30)},
    "market_open":     {"start": (22, 30),"end": (4, 0)},   # 跨天
    "after_hours":     {"start": (4, 0),  "end": (6, 0)},
}


@dataclass
class SessionInfo:
    """时段信息"""
    session: str           # 时段名称
    session_name: str      # 时段中文名
    is_trading_allowed: bool   # 是否允许交易
    max_trades: int        # 最大交易次数
    trading_mode: str      # 交易模式: none / holdings_only / full / protective_only
    data_collection: str   # 数据采集级别: none / minimal / price_only / full / realtime
    llm_active: bool       # LLM 是否活跃
    deliverable: str       # 特殊交付物


SESSION_CONFIG = {
    "deep_night": SessionInfo(
        session="deep_night",
        session_name="深度夜盘",
        is_trading_allowed=False,
        max_trades=0,
        trading_mode="none",
        data_collection="minimal",
        llm_active=False,
        deliverable="",
    ),
    "pre_market_prep": SessionInfo(
        session="pre_market_prep",
        session_name="盘前准备",
        is_trading_allowed=False,
        max_trades=0,
        trading_mode="none",
        data_collection="full",
        llm_active=True,
        deliverable="盘前简报",
    ),
    "pre_market_trade": SessionInfo(
        session="pre_market_trade",
        session_name="盘前交易",
        is_trading_allowed=True,
        max_trades=2,
        trading_mode="holdings_only",
        data_collection="price_only",
        llm_active=True,
        deliverable="",
    ),
    "market_open": SessionInfo(
        session="market_open",
        session_name="盘中",
        is_trading_allowed=True,
        max_trades=5,
        trading_mode="full",
        data_collection="realtime",
        llm_active=True,
        deliverable="",
    ),
    "after_hours": SessionInfo(
        session="after_hours",
        session_name="盘后",
        is_trading_allowed=True,
        max_trades=2,
        trading_mode="protective_only",  # 仅止损/止盈
        data_collection="price_only",
        llm_active=True,
        deliverable="",
    ),
    "holiday": SessionInfo(
        session="holiday",
        session_name="休市",
        is_trading_allowed=False,
        max_trades=0,
        trading_mode="none",
        data_collection="minimal",
        llm_active=True,
        deliverable="周度复盘",
    ),
    "weekend": SessionInfo(
        session="weekend",
        session_name="周末",
        is_trading_allowed=False,
        max_trades=0,
        trading_mode="none",
        data_collection="minimal",
        llm_active=True,
        deliverable="周度复盘",
    ),
}


def get_market_session(dt: datetime = None) -> SessionInfo:
    """
    获取当前交易时段

    Args:
        dt: 时间（默认北京时间）

    Returns:
        SessionInfo: 当前时段信息
    """
    if dt is None:
        dt = datetime.now(timezone(timedelta(hours=8)))

    # 转为无时区的时间对象用于比较
    t = dt.replace(tzinfo=None)
    hour = t.hour
    minute = t.minute
    current_minutes = hour * 60 + minute

    # 检查周末
    if t.weekday() >= 5:  # Saturday=5, Sunday=6
        return SESSION_CONFIG["weekend"]

    # 检查节假日
    is_holiday, _ = is_us_holiday(dt)
    if is_holiday:
        return SESSION_CONFIG["holiday"]

    # 选择夏令时/冬令时配置
    summer = is_summer_time(dt)
    sessions = SESSION_SUMMER if summer else SESSION_WINTER

    for name, times in sessions.items():
        start_h, start_m = times["start"]
        end_h, end_m = times["end"]
        start_minutes = start_h * 60 + start_m
        end_minutes = end_h * 60 + end_m

        # 跳过 start == end 的占位时段
        if start_minutes == end_minutes:
            continue

        if name == "market_open":
            # 盘中时段跨天: 21:30 → 04:00
            # 使用 <= end 以包含精确边界点 (04:00 属于盘中)
            if current_minutes >= start_minutes or current_minutes < end_minutes:
                return SESSION_CONFIG[name]
        else:
            # 使用 <= end 以包含精确边界点
            if start_minutes <= current_minutes < end_minutes:
                return SESSION_CONFIG[name]

    # fallback: 深度夜盘
    logger.warning(f"无法匹配时段，使用 deep_night: {t}")
    return SESSION_CONFIG["deep_night"]


def should_execute_trade(session: SessionInfo = None) -> tuple:
    """
    判断是否应该执行交易

    Returns:
        (should_trade, reason)
    """
    if session is None:
        session = get_market_session()

    if not session.is_trading_allowed:
        return False, f"{session.session_name}: 交易已禁止"

    if session.trading_mode == "none":
        return False, f"{session.session_name}: 无交易模式"

    return True, f"{session.session_name}: {session.trading_mode}"


def get_session_strategy(session: SessionInfo = None) -> dict:
    """获取当前时段的完整策略配置"""
    if session is None:
        session = get_market_session()

    return {
        "session": session.session,
        "session_name": session.session_name,
        "trading_allowed": session.is_trading_allowed,
        "max_trades": session.max_trades,
        "trading_mode": session.trading_mode,
        "data_collection": session.data_collection,
        "llm_active": session.llm_active,
        "deliverable": session.deliverable,
    }


def print_session_status() -> str:
    """打印当前时段状态（用于日志/调试）"""
    session = get_market_session()
    summer = is_summer_time()
    holiday, holiday_name = is_us_holiday()

    lines = [
        f"📊 当前时段状态",
        f"  时制: {'夏令时' if summer else '冬令时'}",
        f"  节假日: {'是 - ' + holiday_name if holiday else '否'}",
        f"  当前时段: {session.session_name} ({session.session})",
        f"  交易权限: {'允许' if session.is_trading_allowed else '禁止'}",
        f"  交易模式: {session.trading_mode}",
        f"  最大交易: {session.max_trades} 笔",
        f"  数据采集: {session.data_collection}",
        f"  LLM 活跃: {'是' if session.llm_active else '否'}",
    ]
    if session.deliverable:
        lines.append(f"  交付物: {session.deliverable}")

    return "\n".join(lines)
