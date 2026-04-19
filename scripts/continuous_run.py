#!/usr/bin/env python3
"""
US Data Hub 持续运行脚本
根据当前时段自动调整运行策略
"""
import os, time, subprocess, sys
from datetime import datetime

# Load environment
with open(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, v = line.split('=', 1)
            os.environ[k] = v

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from analysis.session_strategy import get_market_session

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

def run_cmd(name, cmd, timeout=120):
    """Run a command and log result"""
    now = datetime.now().strftime("%H:%M:%S")
    print(f"\n{'='*60}")
    print(f"[{now}] 执行: {name}")
    print(f"{'='*60}")
    try:
        r = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, cwd=BASE_DIR
        )
        if r.stdout:
            lines = r.stdout.strip().split('\n')
            for line in lines[-8:]:
                line = line.strip()
                if line:
                    print(f"  {line[:120]}")
        if r.returncode != 0:
            if r.stderr:
                for line in r.stderr.strip().split('\n')[-3:]:
                    line = line.strip()
                    if line and 'RuntimeWarning' not in line:
                        print(f"  ⚠️ {line[:100]}")
            print(f"  ❌ 失败 (exit={r.returncode})")
        else:
            print(f"  ✅ 完成")
        return r.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"  ⏰ 超时 ({timeout}s)")
        return False
    except Exception as e:
        print(f"  ❌ {e}")
        return False

def run_price_collection():
    """采集所有持仓股票的价格数据"""
    from collectors.longbridge import LongbridgeCollector
    c = LongbridgeCollector()
    symbols = ["AAPL", "TSLA", "NVDA", "META", "MSFT", "GOOGL", "AMZN"]
    results = []
    for sym in symbols:
        try:
            r = c.collect(sym, count=5)
            results.append((sym, len(r)))
        except Exception as e:
            results.append((sym, f"error: {e}"))
    for sym, r in results:
        if isinstance(r, int):
            print(f"  {sym}: {r} 条数据")
        else:
            print(f"  {sym}: {r}")

def main():
    print("🚀 US Data Hub 持续运行中...")
    print(f"📅 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📂 工作目录: {BASE_DIR}")
    sys.stdout.flush()

    cycle = 0
    while True:
        cycle += 1
        now = datetime.now()
        
        try:
            session = get_market_session()
        except Exception as e:
            print(f"  ❌ 获取时段失败: {e}")
            time.sleep(60)
            continue
        
        print(f"\n{'#'*60}")
        print(f"🔄 第 {cycle} 轮循环 | {now.strftime('%H:%M:%S')}")
        print(f"📊 当前时段: {session.session_name} | 交易: {'允许' if session.is_trading_allowed else '禁止'}")
        print(f"{'#'*60}")
        sys.stdout.flush()
        
        # Always: Price data collection (inline, not subprocess)
        now_ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{now_ts}] 执行: 价格采集")
        try:
            run_price_collection()
            print(f"  ✅ 完成")
        except Exception as e:
            print(f"  ❌ {e}")
        
        if session.session == "weekend":
            # Weekend: Weekly review + data collection only
            if cycle % 3 == 1:
                run_cmd("持仓监控", ["python3", "-m", "monitoring.holding_monitor"], timeout=120)
            if cycle % 5 == 1:
                run_cmd("策略验证", ["python3", "scripts/validate_strategy.py"], timeout=180)
            print(f"  💤 周末模式 - 等待下一轮 (60s)")
            time.sleep(60)
            
        elif session.is_trading_allowed:
            # Trading hours: Full loop
            if cycle % 6 == 1:
                run_cmd("完整循环", ["python3", "scripts/auto_execute.py", "--mode", "full-loop"], timeout=300)
            if cycle % 3 == 0:
                run_cmd("Watcher", ["python3", "scripts/watcher.py"], timeout=120)
            if cycle % 12 == 0:
                run_cmd("持仓监控", ["python3", "-m", "monitoring.holding_monitor"], timeout=120)
            print(f"  ⏳ 等待下一轮 (30s)")
            time.sleep(30)
            
        elif session.session == "pre_market_prep":
            # Pre-market prep: Screener + briefing
            if cycle % 6 == 1:
                run_cmd("选股", ["python3", "scripts/auto_execute.py", "--mode", "screener"], timeout=120)
            print(f"  ⏳ 盘前准备 - 等待下一轮 (60s)")
            time.sleep(60)
            
        elif session.session == "after_hours":
            # After hours: Review + data fill
            if cycle % 4 == 1:
                run_cmd("复盘", ["python3", "scripts/auto_execute.py", "--mode", "review"], timeout=180)
            print(f"  ⏳ 盘后模式 - 等待下一轮 (60s)")
            time.sleep(60)
            
        else:
            # Deep night / holiday: Minimal collection only
            mode_name = "深度夜盘" if session.session == "deep_night" else "休市"
            print(f"  💤 {mode_name} - 最低频采集 (120s)")
            time.sleep(120)
        
        sys.stdout.flush()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 收到中断信号，停止运行")
    except Exception as e:
        print(f"\n\n❌ 致命错误: {e}")
        import traceback
        traceback.print_exc()
