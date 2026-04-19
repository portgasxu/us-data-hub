#!/usr/bin/env python3
"""
US Data Hub — 全链路追溯查询工具 (v6.0)
======================================

用法:
    python scripts/trace_query --execution EXE_20260419_161500_a3f2b1
    python scripts/trace_query --signal SIG_20260419_161500_a3f2b1
    python scripts/trace_query --symbol AAPL --date 2026-04-19
    python scripts/trace_query --list-executions  # 列出最近执行批次
"""

import argparse
import sqlite3
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "us_data_hub.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def query_execution(execution_id: str):
    """查询一次完整执行的所有关联记录"""
    db = get_db()

    print(f"\n{'='*80}")
    print(f"📋 执行批次追溯: {execution_id}")
    print(f"{'='*80}")

    # 1. 执行批次信息
    exec_info = db.execute(
        "SELECT * FROM execution_log WHERE execution_id = ?",
        (execution_id,)
    ).fetchone()
    if exec_info:
        print(f"\n⏱️  执行信息:")
        print(f"    开始: {exec_info['started_at']}")
        print(f"    结束: {exec_info['ended_at'] or '进行中'}")
        print(f"    状态: {exec_info['status']}")
        print(f"    信号数: {exec_info['signals_collected']}")
        print(f"    交易数: {exec_info['trades_executed']}")
        if exec_info['errors']:
            print(f"    错误: {exec_info['errors']}")
    else:
        print(f"\n❌ 未找到执行批次: {execution_id}")
        db.close()
        return

    # 2. 信号列表
    signals = db.execute(
        "SELECT * FROM signal_log WHERE execution_id = ?",
        (execution_id,)
    ).fetchall()
    if signals:
        print(f"\n📡 信号 ({len(signals)} 条):")
        for s in signals:
            action_icon = {"EXECUTED": "✅", "SKIPPED": "⏭️", "REJECTED": "❌", "DRY_RUN": "🧪",
                           "HOLD": "⏸️", "PENDING": "⏳", "ERROR": "💥"}.get(s["action_taken"], "❓")
            print(f"    {action_icon} {s['signal_id']} | {s['symbol']} {s['direction']} "
                  f"conf={s['confidence']:.2f} from={s['source']} → {s['action_taken']}")
            if s['rejection_reason']:
                print(f"       原因: {s['rejection_reason']}")

    # 3. 交易记录
    trades = db.execute(
        "SELECT * FROM trades WHERE execution_id = ?",
        (execution_id,)
    ).fetchall()
    if trades:
        print(f"\n💰 交易 ({len(trades)} 笔):")
        for t in trades:
            print(f"    {t['symbol']} {t['direction']} {t['quantity']} @ ${t['price']:.2f} "
                  f"| signal_id={t['signal_id']}")
            if t.get('actual_return') is not None:
                ret = t['actual_return']
                emoji = "📈" if ret > 0 else "📉"
                print(f"       {emoji} 实际收益: {ret:.2%}")

    # 4. 风控告警
    alerts = db.execute(
        "SELECT * FROM monitor_alerts WHERE execution_id = ?",
        (execution_id,)
    ).fetchall()
    if alerts:
        print(f"\n🛡️  风控告警 ({len(alerts)} 条):")
        for a in alerts:
            print(f"    [{a['severity']}] {a['symbol']} - {a['alert_type']}: {a['title']}")

    db.close()


def query_signal(signal_id: str):
    """查询单个信号的完整追溯链"""
    db = get_db()

    print(f"\n{'='*80}")
    print(f"📡 信号追溯: {signal_id}")
    print(f"{'='*80}")

    # 1. 信号信息
    sig = db.execute(
        "SELECT * FROM signal_log WHERE signal_id = ?",
        (signal_id,)
    ).fetchone()
    if sig:
        print(f"\n📡 信号:")
        print(f"    标的: {sig['symbol']} {sig['direction']}")
        print(f"    来源: {sig['source']}")
        print(f"    置信度: {sig['confidence']:.2f}")
        print(f"    执行批次: {sig['execution_id']}")
        print(f"    结果: {sig['action_taken']}")
        if sig['rejection_reason']:
            print(f"    原因: {sig['rejection_reason']}")

        # 2. 关联交易
        if sig['execution_id']:
            trades = db.execute(
                "SELECT * FROM trades WHERE signal_id = ?",
                (signal_id,)
            ).fetchall()
            if trades:
                print(f"\n💰 关联交易 ({len(trades)} 笔):")
                for t in trades:
                    print(f"    {t['symbol']} {t['direction']} {t['quantity']} @ ${t['price']:.2f}")

    else:
        print(f"\n❌ 未找到信号: {signal_id}")

    db.close()


def query_symbol(symbol: str, date: str = None):
    """查询某只标的在指定日期的所有活动"""
    db = get_db()
    if date is None:
        date = "date('now')"
    else:
        date = f"'{date}'"

    print(f"\n{'='*80}")
    print(f"📊 {symbol} 在 {date} 的活动")
    print(f"{'='*80}")

    # 信号
    signals = db.execute(
        f"SELECT * FROM signal_log WHERE symbol = ? AND date(created_at) = {date} ORDER BY created_at",
        (symbol,)
    ).fetchall()
    if signals:
        print(f"\n📡 信号 ({len(signals)} 条):")
        for s in signals:
            print(f"    {s['signal_id']} | {s['direction']} from={s['source']} "
                  f"conf={s['confidence']:.2f} → {s['action_taken']}")

    # 交易
    trades = db.execute(
        f"SELECT * FROM trades WHERE symbol = ? AND date(timestamp) = {date} ORDER BY timestamp",
        (symbol,)
    ).fetchall()
    if trades:
        print(f"\n💰 交易 ({len(trades)} 笔):")
        for t in trades:
            print(f"    {t['direction']} {t['quantity']} @ ${t['price']:.2f} "
                  f"| signal={t['signal_id']} exec={t['execution_id']}")

    db.close()


def list_executions(limit: int = 10):
    """列出最近的执行批次"""
    db = get_db()

    print(f"\n{'='*80}")
    print(f"📋 最近 {limit} 次执行批次")
    print(f"{'='*80}")

    executions = db.execute(
        "SELECT * FROM execution_log ORDER BY started_at DESC LIMIT ?",
        (limit,)
    ).fetchall()

    if executions:
        print(f"\n{'Execution ID':<38} {'开始时间':<22} {'状态':<12} {'信号':<6} {'交易':<6}")
        print("-" * 90)
        for e in executions:
            status_icon = {"COMPLETED": "✅", "RUNNING": "🔄", "FAILED": "❌"}.get(e['status'], "❓")
            print(f"{e['execution_id']:<38} {e['started_at']:<22} "
                  f"{status_icon} {e['status']:<8} {e['signals_collected']:<6} {e['trades_executed']:<6}")
    else:
        print("\n暂无执行记录")

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="全链路追溯查询工具")
    parser.add_argument("--execution", "-e", help="执行批次 ID")
    parser.add_argument("--signal", "-s", help="信号 ID")
    parser.add_argument("--symbol", help="股票代码")
    parser.add_argument("--date", "-d", help="日期 (YYYY-MM-DD)")
    parser.add_argument("--list-executions", "-l", action="store_true", help="列出最近执行批次")
    parser.add_argument("--limit", type=int, default=10, help="列出数量")

    args = parser.parse_args()

    if args.execution:
        query_execution(args.execution)
    elif args.signal:
        query_signal(args.signal)
    elif args.symbol:
        query_symbol(args.symbol, args.date)
    elif args.list_executions:
        list_executions(args.limit)
    else:
        parser.print_help()
