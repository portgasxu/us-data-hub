"""
Smart Drawdown — 智能最大回撤控制 (P0 Fix #4)

4 级动态防御，像游戏掉血一样逐步加强：
  Level 1: 轻微波动 (回撤 3-5%)   → 提高买入阈值 + 降低单笔上限
  Level 2: 中度回撤 (回撤 5-8%)   → 暂停买入 + 持仓健康检查
  Level 3: 严重回撤 (回撤 8-12%)  → 只卖不买 + 因子扫描清仓
  Level 4: 熔断 (回撤 >12%)       → 全部清仓 + 暂停 48 小时

从 portfolio 层面计算回撤，不是单只标的。
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger("smart_drawdown")

# ─── 防御等级配置 ─────────────────────────────────────────
DEFENSE_LEVELS = {
    0: {
        "name": "正常",
        "max_drawdown": 0.03,
        "buy_threshold": 0.75,       # 基础买入阈值
        "max_single_buy": 2000,      # 单笔上限
        "allow_buy": True,
        "allow_sell": True,
        "actions": [],
    },
    1: {
        "name": "轻微波动",
        "max_drawdown": 0.05,
        "buy_threshold": 0.80,
        "max_single_buy": 1500,
        "allow_buy": True,
        "allow_sell": True,
        "actions": ["提高买入阈值 +5%", "降低单笔上限 $2000→$1500"],
    },
    2: {
        "name": "中度回撤",
        "max_drawdown": 0.08,
        "buy_threshold": 0.85,
        "max_single_buy": 0,
        "allow_buy": False,
        "allow_sell": True,
        "actions": ["暂停所有新买入", "启动持仓健康检查"],
    },
    3: {
        "name": "严重回撤",
        "max_drawdown": 0.12,
        "buy_threshold": 1.0,
        "max_single_buy": 0,
        "allow_buy": False,
        "allow_sell": True,
        "actions": ["只允许卖出", "对所有持仓执行因子扫描", "清仓质量恶化的标的"],
    },
    4: {
        "name": "熔断",
        "max_drawdown": 999,
        "buy_threshold": 1.0,
        "max_single_buy": 0,
        "allow_buy": False,
        "allow_sell": True,
        "actions": ["全部清仓", "暂停交易 48 小时", "生成回撤分析报告"],
    },
}


def get_portfolio_value(db) -> float:
    """计算当前组合总价值。"""
    rows = db.conn.execute(
        """SELECT h.symbol, h.quantity, h.cost_price, p.close as current_price
           FROM holdings h
           LEFT JOIN (SELECT symbol, close FROM prices WHERE (symbol, date) IN
                      (SELECT symbol, MAX(date) FROM prices GROUP BY symbol)) p
           ON h.symbol = p.symbol
           WHERE h.active = 1 AND h.quantity > 0"""
    ).fetchall()

    total = 0.0
    for r in rows:
        price = r["current_price"] if r["current_price"] else r["cost_price"]
        total += r["quantity"] * price

    # 加现金 (如果有 cash 表)
    try:
        cash_row = db.conn.execute("SELECT cash FROM account LIMIT 1").fetchone()
        if cash_row:
            total += cash_row["cash"]
    except Exception:
        pass

    return total


def get_highest_portfolio_value(db) -> tuple[float, str]:
    """从交易历史中估算历史最高组合价值。
    Fix #11: Extended from 30 days to 90 days to capture historical peaks better."""
    # Use 90 days instead of 30
    rows = db.conn.execute(
        """SELECT h.symbol, h.quantity, p.close, p.date
           FROM holdings h
           JOIN prices p ON h.symbol = p.symbol
           WHERE h.active = 1 AND h.quantity > 0
           AND p.date >= date('now', '-90 days')"""
    ).fetchall()

    if not rows:
        return 0.0, datetime.now().strftime("%Y-%m-%d")

    # 按日期聚合
    by_date = {}
    for r in rows:
        date = r["date"]
        if date not in by_date:
            by_date[date] = 0.0
        by_date[date] += r["quantity"] * (r["close"] or 0)

    if not by_date:
        return 0.0, datetime.now().strftime("%Y-%m-%d")

    highest_date = max(by_date, key=by_date.get)
    return by_date[highest_date], highest_date


def get_current_drawdown(db) -> dict:
    """计算当前回撤幅度。"""
    current_value = get_portfolio_value(db)
    highest_value, highest_date = get_highest_portfolio_value(db)

    if highest_value == 0:
        return {
            "current_value": current_value,
            "highest_value": current_value,
            "drawdown_pct": 0.0,
            "highest_date": datetime.now().strftime("%Y-%m-%d"),
            "level": 0,
        }

    drawdown = (highest_value - current_value) / highest_value

    # 判断防御等级
    level = 0
    for lvl, cfg in DEFENSE_LEVELS.items():
        if drawdown < cfg["max_drawdown"]:
            level = lvl
            break
    else:
        level = 4

    return {
        "current_value": current_value,
        "highest_value": highest_value,
        "drawdown_pct": drawdown,
        "highest_date": highest_date,
        "level": level,
        "level_name": DEFENSE_LEVELS[level]["name"],
    }


def get_defense_config(db) -> dict:
    """获取当前防御配置（供 auto_execute 使用）。"""
    dd = get_current_drawdown(db)
    level = dd["level"]
    cfg = DEFENSE_LEVELS[level]

    return {
        "level": level,
        "level_name": cfg["name"],
        "drawdown_pct": dd["drawdown_pct"],
        "buy_threshold": cfg["buy_threshold"],
        "max_single_buy": cfg["max_single_buy"],
        "allow_buy": cfg["allow_buy"],
        "allow_sell": cfg["allow_sell"],
        "actions": cfg["actions"],
    }


def should_pause_trading(db) -> tuple[bool, str]:
    """检查是否处于熔断状态（Level 4），需要暂停交易。"""
    dd = get_current_drawdown(db)

    if dd["level"] >= 4:
        # Check if we're within 48-hour cooldown
        try:
            last_circuit = db.conn.execute(
                "SELECT blocked_at, cooldown_until FROM signal_cooldowns "
                "WHERE symbol = 'CIRCUIT_BREAKER' ORDER BY blocked_at DESC LIMIT 1"
            ).fetchone()

            if last_circuit:
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if now_str < last_circuit["cooldown_until"]:
                    remaining = (datetime.strptime(last_circuit["cooldown_until"], "%Y-%m-%d %H:%M:%S") - datetime.now())
                    hours_left = remaining.total_seconds() / 3600
                    return True, f"Circuit breaker active — paused for {hours_left:.0f}h"
                else:
                    # 48h passed, clear and allow trading again
                    db.conn.execute(
                        "DELETE FROM signal_cooldowns WHERE symbol = 'CIRCUIT_BREAKER'"
                    )
                    db.conn.commit()
                    logger.info("🔓 Circuit breaker cleared — trading resumed")

            # Record new circuit breaker cooldown
            now = datetime.now()
            cooldown_until = now + timedelta(hours=48)
            db.conn.execute(
                """INSERT INTO signal_cooldowns (symbol, direction, source, blocked_at, cooldown_until, reason)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ("CIRCUIT_BREAKER", "all", "drawdown",
                 now.strftime("%Y-%m-%d %H:%M:%S"),
                 cooldown_until.strftime("%Y-%m-%d %H:%M:%S"),
                 f"Drawdown {dd['drawdown_pct']:.1%} triggered circuit breaker"),
            )
            db.conn.commit()
            logger.warning(f"🔴 CIRCUIT BREAKER triggered — drawdown {dd['drawdown_pct']:.1%}, paused 48h")

        except Exception as e:
            logger.error(f"Circuit breaker check failed: {e}")
            return False, ""

        return True, f"Drawdown {dd['drawdown_pct']:.1%} triggered circuit breaker — paused 48h"

    return False, ""


def check_holdings_health(db) -> list[dict]:
    """
    Level 2/3 时对所有持仓执行健康检查。
    返回需要卖出的标的列表。
    """
    from analysis.smart_risk import check_all_stop_losses

    rows = db.conn.execute(
        """SELECT h.symbol, h.quantity, h.cost_price, p.close as current_price
           FROM holdings h
           LEFT JOIN (SELECT symbol, close FROM prices WHERE (symbol, date) IN
                      (SELECT symbol, MAX(date) FROM prices GROUP BY symbol)) p
           ON h.symbol = p.symbol
           WHERE h.active = 1 AND h.quantity > 0"""
    ).fetchall()

    unhealthy = []
    for r in rows:
        cost = r["cost_price"]
        price = r["current_price"] if r["current_price"] else cost
        triggers = check_all_stop_losses(db, r["symbol"], cost, price)

        if triggers:
            unhealthy.append({
                "symbol": r["symbol"],
                "quantity": r["quantity"],
                "cost_price": cost,
                "current_price": price,
                "triggers": triggers,
            })

    return unhealthy


def generate_drawdown_report(db) -> str:
    """生成回撤报告。"""
    dd = get_current_drawdown(db)
    cfg = get_defense_config(db)

    lines = ["=" * 60]
    lines.append("📉 回撤防御报告")
    lines.append(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 60)
    lines.append(f"当前组合价值: ${dd['current_value']:,.2f}")
    lines.append(f"历史最高价值: ${dd['highest_value']:,.2f} ({dd['highest_date']})")
    lines.append(f"当前回撤:     {dd['drawdown_pct']:.1%}")
    lines.append(f"防御等级:     Level {dd['level']} - {cfg['level_name']}")
    lines.append("")

    if cfg["actions"]:
        lines.append("激活防御措施:")
        for a in cfg["actions"]:
            lines.append(f"  • {a}")
    else:
        lines.append("✅ 运行正常，无需防御措施")

    lines.append("")
    lines.append(f"买入阈值调整: {cfg['buy_threshold']:.0%}")
    lines.append(f"单笔上限调整: ${cfg['max_single_buy']:,}" if cfg["max_single_buy"] > 0 else "单笔上限: 禁止买入")
    lines.append(f"允许买入: {'是' if cfg['allow_buy'] else '否'}")
    lines.append(f"允许卖出: {'是' if cfg['allow_sell'] else '否'}")

    # Level 2/3: 持仓健康检查
    if dd["level"] >= 2:
        lines.append("")
        lines.append("--- 持仓健康检查 ---")
        unhealthy = check_holdings_health(db)
        if unhealthy:
            for u in unhealthy:
                lines.append(f"  🔴 {u['symbol']}: 需要卖出 ({len(u['triggers'])} 个止损触发)")
                for t in u["triggers"]:
                    lines.append(f"    - [{t['type']}] {t['reason']}")
        else:
            lines.append("  ✅ 所有持仓健康")

    return "\n".join(lines)
