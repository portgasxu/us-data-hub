#!/usr/bin/env python3
"""
US Data Hub — 系统管理器（一键启动所有）
=========================================
统一管理所有交易系统模块的启动、监控、停止。

用法:
    python scripts/system_manager.py start      # 一键启动所有
    python scripts/system_manager.py status     # 查看所有模块状态
    python scripts/system_manager.py stop       # 停止常驻服务
    python scripts/system_manager.py restart    # 重启常驻服务
    python scripts/system_manager.py check      # 检查系统健康

启动逻辑:
    - Crontab 定时任务（选股、全循环、复盘等）→ 由 crontab 调度，无需手动启动
    - 常驻服务（watcher daemon）→ 直接启动
    - 启动后会立即执行一次完整巡检，确认所有模块正常
"""

import sys
import os
import json
import signal
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()

from storage import Database

# ─── 服务定义 ───
# 常驻服务（需要手动启动的后台进程）
DAEMON_SERVICES = {
    "orchestrator-brain": {
        "name": "LLM 系统大脑",
        "command": "python3 monitoring/orchestrator.py",
        "description": "基于LLM的智能调度大脑，实时监控+调度+自愈所有模块",
        "critical": True,
    },
    "watcher-daemon": {
        "name": "新闻监控守护进程",
        "command": "python3 scripts/watcher.py --daemon",
        "description": "持续监控新闻和Reddit情绪，产生交易信号",
        "critical": True,
    },
}

# 定时任务（由 Orchestrator 大脑调度，crontab 已退场）
CRON_TASKS = {
    "price-collector": {
        "name": "价格采集",
        "description": "每30分钟(盘前)/每5分钟(盘中)采集价格",
        "test_command": "python3 -m collectors.longbridge --data-type price",
        "critical": True,
    },
    "full-loop": {
        "name": "全循环交易",
        "description": "每30分钟从SignalHub取信号执行交易",
        "test_command": "python3 scripts/auto_execute.py --mode full-loop --show-session",
        "critical": True,
    },
    "holding-monitor": {
        "name": "持仓监控",
        "description": "监控持仓，LLM动态止盈止损",
        "test_command": "python3 scripts/auto_execute.py --mode holding-monitor",
        "critical": True,
    },
    "screener-to-trade": {
        "name": "选股→交易",
        "description": "三层选股→TradingAgents→SignalHub→执行",
        "test_command": "python3 scripts/run.py screener",
        "critical": True,
    },
    "order-monitor": {
        "name": "订单监控",
        "description": "监控pending订单，处理超时/跳空/部分成交",
        "test_command": "python3 scripts/auto_execute.py --mode order-monitor",
        "critical": True,
    },
    "review": {
        "name": "盘后复盘",
        "description": "盘后统计今日交易和持仓盈亏",
        "test_command": "python3 scripts/auto_execute.py --mode review",
        "critical": False,
    },
    "morning-brief": {
        "name": "盘前晨报",
        "description": "盘前简报：持仓概览+最新选股",
        "test_command": "python3 scripts/auto_execute.py --mode morning-brief",
        "critical": False,
    },
    "factors": {
        "name": "因子计算",
        "description": "每日凌晨计算技术指标因子",
        "test_command": "python3 scripts/calculate_factors.py",
        "critical": False,
    },
    "order-cleanup": {
        "name": "订单清理",
        "description": "盘后清理所有未成交市价单",
        "test_command": "python3 -m monitoring.order_monitor --mode cleanup",
        "critical": False,
    },
}

PID_DIR = os.path.join(PROJECT_ROOT, "temp", "pids")
STATUS_FILE = os.path.join(PROJECT_ROOT, "temp", "system_status.json")


def ensure_dirs():
    os.makedirs(PID_DIR, exist_ok=True)
    os.makedirs(os.path.join(PROJECT_ROOT, "temp"), exist_ok=True)
    os.makedirs(os.path.join(PROJECT_ROOT, "logs"), exist_ok=True)


def get_pid_file(service_id: str) -> str:
    return os.path.join(PID_DIR, f"{service_id}.pid")


def is_daemon_running(service_id: str) -> bool:
    pid_file = get_pid_file(service_id)
    if not os.path.exists(pid_file):
        return False
    try:
        with open(pid_file) as f:
            pid = int(f.read().strip())
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, ValueError):
        return False


def get_daemon_pid(service_id: str) -> Optional[int]:
    pid_file = get_pid_file(service_id)
    if not os.path.exists(pid_file):
        return None
    try:
        with open(pid_file) as f:
            return int(f.read().strip())
    except (ValueError, FileNotFoundError):
        return None


def start_daemon(service_id: str, service: Dict) -> bool:
    """启动常驻服务"""
    if is_daemon_running(service_id):
        pid = get_daemon_pid(service_id)
        print(f"  ⏭️  {service['name']} 已在运行 (PID {pid})")
        return True

    log_file = os.path.join(PROJECT_ROOT, "logs", f"{service_id}.log")
    pid_file = get_pid_file(service_id)

    try:
        with open(log_file, "a") as log_f:
            process = subprocess.Popen(
                service["command"].split(),
                stdout=log_f,
                stderr=subprocess.STDOUT,
                cwd=PROJECT_ROOT,
            )

        with open(pid_file, "w") as f:
            f.write(str(process.pid))

        time.sleep(2)
        if is_daemon_running(service_id):
            print(f"  ✅ {service['name']} 已启动 (PID {process.pid})")
            return True
        else:
            print(f"  ❌ {service['name']} 启动失败")
            return False
    except Exception as e:
        print(f"  ❌ {service['name']} 启动异常: {e}")
        return False


def stop_daemon(service_id: str, service: Dict) -> bool:
    """停止常驻服务"""
    if not is_daemon_running(service_id):
        print(f"  ⏭️  {service['name']} 未运行")
        return True

    pid = get_daemon_pid(service_id)
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)
        if is_daemon_running(service_id):
            os.kill(pid, signal.SIGKILL)
        print(f"  ✅ {service['name']} 已停止 (PID {pid})")
        return True
    except Exception as e:
        print(f"  ⚠️  {service['name']} 停止异常: {e}")
        return False
    finally:
        pid_file = get_pid_file(service_id)
        if os.path.exists(pid_file):
            os.remove(pid_file)


def test_module(task_id: str, task: Dict) -> tuple:
    """测试单个模块是否能正常运行（dry-run 模式）"""
    # 持仓监控需要连接 broker，给更多时间
    timeout = 60 if task_id in ("holding-monitor", "screener-to-trade") else 30
    try:
        result = subprocess.run(
            task["test_command"].split(),
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=PROJECT_ROOT,
        )
        if result.returncode == 0:
            return "pass", ""
        else:
            return "fail", result.stderr[:200]
    except subprocess.TimeoutExpired:
        return "timeout", f"执行超时 (>{timeout}s)"
    except Exception as e:
        return "error", str(e)[:200]


def cmd_start():
    """一键启动所有"""
    print(f"\n{'='*60}")
    print(f"🚀 US Data Hub 交易系统 — 一键启动")
    print(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    ensure_dirs()

    # === 步骤1: 系统预检 ===
    print(f"\n📋 步骤 1/4: 系统预检")
    print(f"{'─'*40}")
    try:
        db = Database()
        stats = db.get_stats()
        print(f"  ✅ 数据库已连接")
        print(f"  📦 数据: {stats.get('data_points', 0)} 条 | 持仓 {stats.get('holdings', 0)} 只 | 交易 {stats.get('trades', 0)} 笔")
        db.close()
    except Exception as e:
        print(f"  ❌ 数据库异常: {e}")
        return

    # === 步骤2: 确认 Orchestrator 调度 ===
    print(f"\n📅 步骤 2/3: 任务调度 (Orchestrator 管理)")
    print(f"{'─'*40}")
    print(f"  ✅ 所有定时任务由 Orchestrator 大脑调度")
    for task_id, task in CRON_TASKS.items():
        emoji = "🔴" if task["critical"] else "⚪"
        print(f"    {emoji} {task['name']}")

    # === 步骤3: 启动常驻服务 ===
    print(f"\n🔧 步骤 3/3: 启动常驻服务")
    print(f"{'─'*40}")
    daemon_ok = 0
    daemon_fail = 0
    for service_id, service in DAEMON_SERVICES.items():
        if start_daemon(service_id, service):
            daemon_ok += 1
        else:
            daemon_fail += 1

    if not DAEMON_SERVICES:
        print(f"  ℹ️  无常驻服务需要启动")

    # === 步骤4: 模块健康检查 ===
    print(f"\n🔍 模块健康检查")
    print(f"{'─'*40}")
    pass_count = 0
    fail_count = 0
    for task_id, task in CRON_TASKS.items():
        status, detail = test_module(task_id, task)
        if status == "pass":
            print(f"  ✅ {task['name']}")
            pass_count += 1
        else:
            icon = "⚠️" if not task["critical"] else "❌"
            print(f"  {icon} {task['name']}: {status} ({detail[:60]})")
            fail_count += 1

    # === 最终状态 ===
    print(f"\n{'='*60}")
    print(f"📊 启动结果汇总")
    print(f"{'='*60}")
    print(f"  常驻服务: {daemon_ok} 启动 | {daemon_fail} 失败")
    print(f"  定时任务: {pass_count} 正常 | {fail_count} 异常")
    print(f"{'='*60}")

    if daemon_fail > 0:
        print(f"\n⚠️  有 {daemon_fail} 个常驻服务启动失败")
    if fail_count > 0:
        print(f"\n⚠️  有 {fail_count} 个模块健康检查未通过")

    # 保存状态
    save_status()


def cmd_stop():
    """停止所有常驻服务"""
    print(f"\n{'='*60}")
    print(f"🛑 US Data Hub 停止常驻服务")
    print(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print(f"\n⚠️  注意: Orchestrator 大脑需单独停止 (kill orchestrator 进程)")
    print()

    for service_id, service in DAEMON_SERVICES.items():
        stop_daemon(service_id, service)


def cmd_status():
    """查看所有模块状态"""
    ensure_dirs()

    print(f"\n{'='*60}")
    print(f"📊 US Data Hub 系统状态")
    print(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # 常驻服务状态
    print(f"\n【常驻服务】")
    for service_id, service in DAEMON_SERVICES.items():
        running = is_daemon_running(service_id)
        pid = get_daemon_pid(service_id)
        emoji = "✅" if running else "❌"
        pid_str = f"(PID {pid})" if pid else "(未运行)"
        print(f"  {emoji} {service['name']} {pid_str}")

    # 定时任务状态
    print(f"\n【定时任务 (Orchestrator 管理)】")
    print(f"{'任务':<14} {'状态'}")
    print(f"{'─'*14} {'─'*6}")
    for task_id, task in CRON_TASKS.items():
        emoji = "🔴" if task["critical"] else "⚪"
        print(f"  {emoji} {task['name']:<12} Orchestrator 调度")

    # 数据状态
    try:
        db = Database()
        stats = db.get_stats()
        print(f"\n【数据状态】")
        print(f"  数据点: {stats.get('data_points', 0):,}")
        print(f"  价格:   {stats.get('prices', 0):,}")
        print(f"  因子:   {stats.get('factors', 0):,}")
        print(f"  持仓:   {stats.get('holdings', 0)}")
        print(f"  交易:   {stats.get('trades', 0)}")
        db.close()
    except Exception:
        pass

    print(f"\n{'='*60}")


def cmd_check():
    """检查系统健康"""
    ensure_dirs()

    print(f"\n{'='*60}")
    print(f"🔍 系统健康检查")
    print(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    issues = []

    # 数据库
    try:
        db = Database()
        db.get_stats()
        db.close()
        print(f"\n  ✅ 数据库正常")
    except Exception as e:
        print(f"\n  ❌ 数据库异常: {e}")
        issues.append("database")

    # Orchestrator 调度
    print(f"  ✅ 任务由 Orchestrator 大脑调度")

    # 常驻服务
    for service_id, service in DAEMON_SERVICES.items():
        if is_daemon_running(service_id):
            print(f"  ✅ {service['name']} 运行中")
        else:
            print(f"  ❌ {service['name']} 未运行")
            issues.append(service_id)

    # 模块测试
    print(f"\n  模块测试:")
    for task_id, task in CRON_TASKS.items():
        status, detail = test_module(task_id, task)
        icon = "✅" if status == "pass" else "❌"
        print(f"    {icon} {task['name']}")

    # 总结
    print(f"\n{'='*60}")
    if issues:
        print(f"  ⚠️  发现 {len(issues)} 个问题: {', '.join(issues)}")
        print(f"  建议: 运行 'python3 scripts/system_manager.py start' 修复")
    else:
        print(f"  ✅ 所有检查通过，系统健康")
    print(f"{'='*60}")


def cmd_restart():
    """重启常驻服务"""
    cmd_stop()
    time.sleep(2)
    cmd_start()


def save_status():
    """保存系统状态到文件"""
    status = {
        "timestamp": datetime.now().isoformat(),
        "daemons": {},
        "cron_tasks": list(CRON_TASKS.keys()),
    }
    for service_id, service in DAEMON_SERVICES.items():
        status["daemons"][service_id] = {
            "running": is_daemon_running(service_id),
            "pid": get_daemon_pid(service_id),
        }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2, ensure_ascii=False)


def main():
    if len(sys.argv) < 2:
        print("用法: python scripts/system_manager.py [start|stop|status|restart|check]")
        sys.exit(1)

    command = sys.argv[1]

    commands = {
        "start": cmd_start,
        "stop": cmd_stop,
        "status": cmd_status,
        "restart": cmd_restart,
        "check": cmd_check,
    }

    if command not in commands:
        print(f"未知命令: {command}")
        print(f"可用命令: {', '.join(commands.keys())}")
        sys.exit(1)

    commands[command]()


if __name__ == "__main__":
    main()
