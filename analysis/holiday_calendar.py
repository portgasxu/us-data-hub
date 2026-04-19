"""
US Data Hub — 美国市场节假日日历 (Phase 3)
===========================================

Phase 3 新增:
  - 美国股市完整节假日判断（2025-2028）
  - 支持提前收盘日（Early Close）
  - 与 session_strategy.py 联动

节假日:
  - New Year's Day
  - Martin Luther King Jr. Day
  - Presidents' Day
  - Good Friday
  - Memorial Day
  - Juneteenth
  - Independence Day
  - Labor Day
  - Thanksgiving Day
  - Christmas Day
"""

from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List


def _get_nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> int:
    """获取某月第 n 个星期几 (0=Monday, 6=Sunday)，返回日期号"""
    first_day = datetime(year, month, 1)
    first_weekday = first_day.weekday()
    first_target = (weekday - first_weekday + 7) % 7 + 1
    return first_target + (n - 1) * 7


def _get_easter(year: int) -> datetime:
    """计算复活节日期"""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return datetime(year, month, day)


# 固定日期节假日 (月, 日)
FIXED_HOLIDAYS = {
    (1, 1): "New Year's Day",
    (6, 19): "Juneteenth National Independence Day",
    (7, 4): "Independence Day",
    (12, 25): "Christmas Day",
}


def get_us_holidays(year: int) -> List[Tuple[int, int, str]]:
    """获取某年的所有美国股市节假日。返回 [(month, day, name), ...]"""
    holidays = []

    # 固定节假日
    for (m, d), name in FIXED_HOLIDAYS.items():
        holidays.append((m, d, name))

    # Martin Luther King Jr. Day: 1月第3个星期一
    mlk_day = _get_nth_weekday_of_month(year, 1, 0, 3)
    holidays.append((1, mlk_day, "Martin Luther King Jr. Day"))

    # Presidents' Day: 2月第3个星期一
    pres_day = _get_nth_weekday_of_month(year, 2, 0, 3)
    holidays.append((2, pres_day, "Presidents' Day"))

    # Good Friday: 复活节前2天
    good_friday = _get_easter(year) - timedelta(days=2)
    holidays.append((good_friday.month, good_friday.day, "Good Friday"))

    # Memorial Day: 5月最后一个星期一
    mem4 = _get_nth_weekday_of_month(year, 5, 0, 4)
    mem5 = _get_nth_weekday_of_month(year, 5, 0, 5)
    mem_day = mem5 if mem5 <= 31 else mem4
    holidays.append((5, mem_day, "Memorial Day"))

    # Labor Day: 9月第1个星期一
    labor_day = _get_nth_weekday_of_month(year, 9, 0, 1)
    holidays.append((9, labor_day, "Labor Day"))

    # Thanksgiving: 11月第4个星期四
    thanks_day = _get_nth_weekday_of_month(year, 11, 3, 4)
    holidays.append((11, thanks_day, "Thanksgiving Day"))

    # 排序
    holidays.sort()
    return holidays


# 提前收盘日（通常 13:00 ET 收盘）
EARLY_CLOSES = {
    # (月, 日): "原因"
    (7, 3): "Independence Day Eve",
    (11, thanks_day if (thanks_day := _get_nth_weekday_of_month(datetime.now().year, 11, 3, 4)) else 28): "Thanksgiving Eve",
    (12, 24): "Christmas Eve",
}


def is_early_close(month: int, day: int, year: int = None) -> Tuple[bool, str]:
    """检查是否为提前收盘日"""
    if year is None:
        year = datetime.now().year

    for (m, d), reason in EARLY_CLOSES.items():
        if m == month and d == day:
            return True, reason
    return False, ""


def is_us_market_holiday(dt: datetime = None) -> Tuple[bool, str]:
    """
    检查是否为美国股市节假日

    Returns:
        (is_holiday, holiday_name)
    """
    if dt is None:
        dt = datetime.now(timezone(timedelta(hours=8)))

    # 转为无时区
    t = dt.replace(tzinfo=None)
    year = t.year
    month_day = (t.month, t.day)

    # 检查固定节假日
    if month_day in FIXED_HOLIDAYS:
        return True, FIXED_HOLIDAYS[month_day]

    # 检查动态节假日
    holidays = get_us_holidays(year)
    for m, d, name in holidays:
        if m == t.month and d == t.day:
            return True, name

    return False, None


def get_next_market_open(dt: datetime = None) -> datetime:
    """获取下一个美股开盘时间（北京时间）"""
    if dt is None:
        dt = datetime.now(timezone(timedelta(hours=8)))

    t = dt.replace(tzinfo=None)

    # 从当前时间开始逐天检查
    for i in range(10):
        check_dt = t + timedelta(days=i)
        is_holiday, _ = is_us_market_holiday(check_dt)
        is_weekend = check_dt.weekday() >= 5

        if not is_holiday and not is_weekend:
            # 找到下一个交易日
            year = check_dt.year
            from analysis.session_strategy import is_summer_time
            summer = is_summer_time(check_dt)

            # 返回开盘时间（北京时间）
            if summer:
                return datetime(year, check_dt.month, check_dt.day, 21, 30)  # 22:30
            else:
                return datetime(year, check_dt.month, check_dt.day, 22, 30)  # 23:30

    return None


def print_holiday_calendar(year: int = None):
    """打印某年的美国股市节假日"""
    if year is None:
        year = datetime.now().year

    holidays = get_us_holidays(year)

    print(f"\n{'='*50}")
    print(f"🇺🇸 US Market Holidays {year}")
    print(f"{'='*50}")
    for m, d, name in holidays:
        dt = datetime(year, m, d)
        weekday = dt.strftime("%A")
        print(f"  {m:2d}/{d:02d} ({weekday[:3]})  {name}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="美国股市节假日日历")
    parser.add_argument("--year", type=int, default=None, help="年份（默认今年）")
    parser.add_argument("--check", type=str, default=None, help="检查日期 (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.check:
        dt = datetime.strptime(args.check, "%Y-%m-%d")
        is_holiday, name = is_us_market_holiday(dt)
        print(f"{args.check}: {'节假日 - ' + name if is_holiday else '交易日'}")
    else:
        print_holiday_calendar(args.year)
