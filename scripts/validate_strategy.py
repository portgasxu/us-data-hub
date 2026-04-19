#!/usr/bin/env python3
"""
Strategy Validation — 盘前策略验证 + 定期评估

用途：
1. 盘前评估 (每天 08:00) — 跑 90 天回测，确认近期信号有效性
2. 周报评估 (每周五 20:00) — 跑 30 天回测，评估本周策略表现
3. 回撤复盘 (事件触发) — 熔断后分析"哪些信号拖后腿"

输出写入 dayup/performance/validation.log
如果胜率持续低于 45% → 触发告警
"""

import sys
import os
import logging
from datetime import datetime

# Setup path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import Database
from analysis.backtest import run_backtest, generate_backtest_report

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "../logs/strategy_validation.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("strategy_validation")

WIN_RATE_THRESHOLD = 0.45  # 胜率低于此值触发告警


def run_validation(db, days: int = 90, label: str = "盘前") -> dict:
    """运行策略验证。"""
    logger.info(f"{'='*50}")
    logger.info(f"策略验证开始 ({label}, {days}天)")
    logger.info(f"{'='*50}")

    result = run_backtest(db, days=days, min_score=0.65)

    if "error" in result:
        logger.error(f"回测失败: {result['error']}")
        return result

    # 输出报告
    report = generate_backtest_report(db, days)
    logger.info(report)

    # 判断是否需要告警
    alerts = []
    if result["win_rate"] < WIN_RATE_THRESHOLD:
        alerts.append(f"胜率 {result['win_rate']:.1%} 低于阈值 {WIN_RATE_THRESHOLD:.0%}，建议暂停交易")

    if result["excess_return"] < 0:
        alerts.append(f"超额收益为负 ({result['excess_return']:+.1%})，策略不如买入持有")

    if result["max_drawdown"] > 0.15:
        alerts.append(f"最大回撤 {result['max_drawdown']:.1%} 超过 15%，风险偏高")

    # Phase 3: 策略反馈闭环 — 将验证结果写回 screener_config.json
    try:
        from analysis.feedback_loop import apply_validation_feedback
        feedback = apply_validation_feedback(result, label=label)
        logger.info(f"🔄 反馈闭环: {len(feedback.get('changes', []))} 项配置调整")
        for change in feedback.get('changes', []):
            logger.info(f"  → {change}")
    except Exception as e:
        logger.warning(f"反馈闭环失败: {e}")

    # 写入 dayup/performance/
    _save_validation_result(label, days, result, alerts)

    # 输出告警
    if alerts:
        logger.warning("--- 告警 ---")
        for a in alerts:
            logger.warning(f"  ⚠️ {a}")
    else:
        logger.info("✅ 无告警，策略运行正常")

    return result


def _save_validation_result(label: str, days: int, result: dict, alerts: list):
    """保存验证结果到 dayup/performance/。"""
    perf_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dayup", "performance")
    os.makedirs(perf_dir, exist_ok=True)

    log_path = os.path.join(perf_dir, "validation.log")

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"验证类型: {label}\n")
        f.write(f"回测区间: {days}天 ({result['start_date']} ~ {result['end_date']})\n")
        f.write(f"{'='*60}\n")
        f.write(f"信号总数:     {result['total_signals']}\n")
        f.write(f"成交笔数:     {result['total_trades']}\n")
        f.write(f"胜率:         {result['win_rate']:.1%}\n")
        f.write(f"策略收益率:   {result['total_return']:+.1%}\n")
        f.write(f"买入持有收益: {result['buy_hold_return']:+.1%}\n")
        f.write(f"超额收益:     {result['excess_return']:+.1%}\n")
        f.write(f"Sharpe Ratio: {result['sharpe_ratio']:.2f}\n")
        f.write(f"最大回撤:     {result['max_drawdown']:.1%}\n")
        f.write(f"最佳标的:     {result['best_symbol']}\n")
        f.write(f"最差标的:     {result['worst_symbol']}\n")

        if alerts:
            f.write(f"\n告警:\n")
            for a in alerts:
                f.write(f"  ⚠️ {a}\n")
        else:
            f.write(f"\n状态: ✅ 无告警\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Strategy Validation")
    parser.add_argument("--days", type=int, default=90, help="Backtest days (default: 90)")
    parser.add_argument("--label", type=str, default="盘前", help="Validation label (default: 盘前)")
    args = parser.parse_args()

    db = Database()
    try:
        run_validation(db, days=args.days, label=args.label)
    finally:
        db.close()
