#!/usr/bin/env python3
"""
US Data Hub — v6.0 全链路功能测试
==================================

测试覆盖:
  1. Signal Schema: TradeSignal 创建/序列化/反序列化
  2. Signal Hub: Signal/TradeSignal 兼容
  3. Trace ID: 生成器
  4. Event Bus: 发布/消费/失败重试
  5. Feature Store: 查询
  6. Storage Schema: 新表创建
  7. Auto Execute: 导入和函数签名
  8. Trace Query: 命令行
  9. Shadow Executor: 初始化
  10. Vector Memory: 降级兼容
"""

import sys
import os
import json
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

passed = 0
failed = 0
total = 0

def test(name):
    global total
    total += 1
    print(f"\n{'='*60}")
    print(f"测试 {total}: {name}")
    print(f"{'='*60}")
    return True

def ok(msg):
    global passed
    passed += 1
    print(f"  ✅ {msg}")

def fail(msg):
    global failed
    print(f"  ❌ {msg}")

def section(name):
    print(f"\n{'#'*60}")
    print(f"# {name}")
    print(f"{'#'*60}")

# ============================================================
section("1. Signal Schema — TradeSignal")
# ============================================================

if test("TradeSignal 创建"):
    try:
        from analysis.signal_schema import TradeSignal, SignalDirection, SignalSource, make_signal

        ts = TradeSignal(
            symbol="AAPL.US",
            direction="buy",
            confidence=0.8,
            source="trading_agents",
            strength=0.9,
            reason="Strong buy signal",
            stop_loss=180.0,
            take_profit=220.0,
        )

        assert ts.symbol == "AAPL", f"Expected AAPL, got {ts.symbol}"
        assert ts.direction == SignalDirection.BUY
        assert ts.source == SignalSource.TRADING_AGENTS
        assert 0.7 <= ts.priority() <= 1.0
        ok(f"TradeSignal 创建成功: {ts}")
    except Exception as e:
        fail(f"TradeSignal 创建失败: {e}")

if test("TradeSignal 序列化/反序列化"):
    try:
        from analysis.signal_schema import TradeSignal, SignalDirection, SignalSource

        ts = TradeSignal(
            symbol="MSFT",
            direction=SignalDirection.BUY,
            confidence=0.75,
            source=SignalSource.TRADING_AGENTS,
            reason="Tech strength",
            stop_loss=400.0,
        )

        d = ts.to_dict()
        assert d["symbol"] == "MSFT"
        assert d["direction"] == "buy"
        assert d["confidence"] == 0.75
        assert "priority" in d

        ts2 = TradeSignal.from_dict(d)
        assert ts2.symbol == ts.symbol
        assert ts2.direction == ts.direction
        assert ts2.confidence == ts.confidence
        ok(f"序列化/反序列化成功: {ts2}")
    except Exception as e:
        fail(f"序列化/反序列化失败: {e}")

if test("make_signal 工厂函数"):
    try:
        from analysis.signal_schema import make_signal

        ts = make_signal("GOOGL", "buy", 0.7, "screener", strength=0.8, reason="Score high")
        assert ts.symbol == "GOOGL"
        assert ts.source.value == "screener"
        ok(f"工厂函数成功: {ts}")
    except Exception as e:
        fail(f"工厂函数失败: {e}")

if test("Signal 向后兼容 (TradeSignal.from_old_signal)"):
    try:
        from analysis.signal_schema import TradeSignal
        from analysis.signal_hub import Signal

        old = Signal(
            symbol="NVDA",
            direction="buy",
            confidence=0.9,
            source="holding_monitor",
            strength=0.85,
            reason="LLM analysis",
        )

        ts = TradeSignal.from_old_signal(old)
        assert ts.symbol == "NVDA"
        assert ts.confidence == 0.9
        ok(f"向后兼容成功: {ts}")
    except Exception as e:
        fail(f"向后兼容失败: {e}")

# ============================================================
section("2. Signal Hub — 兼容性")
# ============================================================

if test("SignalHub 初始化"):
    try:
        from storage import Database
        from analysis.signal_hub import SignalHub

        db = Database()
        db.init_schema()
        hub = SignalHub(db, min_confidence=0.3)
        assert hub is not None
        ok("SignalHub 初始化成功")
        db.close()
    except Exception as e:
        fail(f"SignalHub 初始化失败: {e}")

if test("SignalHub 接受 Signal 对象"):
    try:
        from storage import Database
        from analysis.signal_hub import SignalHub, Signal

        db = Database()
        db.init_schema()
        hub = SignalHub(db, min_confidence=0.3)

        hub.add(Signal("AAPL", "buy", 0.8, "screener", strength=0.7, reason="Score high"))
        assert len(hub.signals) == 1
        assert hub.signals[0].symbol == "AAPL"
        assert hub.signals[0].signal_id != "", "signal_id should be auto-generated"
        ok(f"接受 Signal 对象成功, signal_id={hub.signals[0].signal_id}")
        db.close()
    except Exception as e:
        fail(f"接受 Signal 对象失败: {e}")

if test("SignalHub 接受 TradeSignal 对象"):
    try:
        from storage import Database
        from analysis.signal_hub import SignalHub
        from analysis.signal_schema import TradeSignal, SignalDirection, SignalSource

        db = Database()
        db.init_schema()
        hub = SignalHub(db, min_confidence=0.3)

        ts = TradeSignal(
            symbol="MSFT",
            direction=SignalDirection.BUY,
            confidence=0.85,
            source=SignalSource.TRADING_AGENTS,
            strength=0.9,
            reason="Strong fundamentals",
        )
        hub.add(ts)
        assert len(hub.signals) == 1
        assert hub.signals[0].source == SignalSource.TRADING_AGENTS
        ok(f"接受 TradeSignal 对象成功")
        db.close()
    except Exception as e:
        fail(f"接受 TradeSignal 对象失败: {e}")

if test("SignalHub 去重逻辑"):
    try:
        from storage import Database
        from analysis.signal_hub import SignalHub, Signal

        db = Database()
        db.init_schema()
        hub = SignalHub(db, min_confidence=0.3)

        # 添加相同 symbol+direction 的信号
        hub.add(Signal("AAPL", "buy", 0.6, "screener", strength=0.5))
        hub.add(Signal("AAPL", "buy", 0.9, "holding_monitor", strength=0.8))
        
        # 应该只保留优先级高的那个
        assert len(hub.signals) == 1
        assert hub.signals[0].confidence == 0.9
        ok(f"去重逻辑成功: 保留高优先级信号")
        db.close()
    except Exception as e:
        fail(f"去重逻辑失败: {e}")

# ============================================================
section("3. Trace ID — 生成器")
# ============================================================

if test("generate_signal_id"):
    try:
        from analysis.trace_id import generate_signal_id

        sid = generate_signal_id()
        assert sid.startswith("SIG_")
        assert len(sid) > 10
        ok(f"signal_id: {sid}")
    except Exception as e:
        fail(f"generate_signal_id 失败: {e}")

if test("generate_execution_id"):
    try:
        from analysis.trace_id import generate_execution_id

        eid = generate_execution_id()
        assert eid.startswith("EXE_")
        assert len(eid) > 10
        ok(f"execution_id: {eid}")
    except Exception as e:
        fail(f"generate_execution_id 失败: {e}")

if test("generate_decision_trace_id"):
    try:
        from analysis.trace_id import generate_decision_trace_id

        did = generate_decision_trace_id()
        assert did.startswith("DEC_")
        assert len(did) > 10
        ok(f"decision_trace_id: {did}")
    except Exception as e:
        fail(f"generate_decision_trace_id 失败: {e}")

if test("ID 唯一性"):
    try:
        from analysis.trace_id import generate_signal_id

        ids = set()
        for _ in range(100):
            sid = generate_signal_id()
            assert sid not in ids, f"Duplicate ID: {sid}"
            ids.add(sid)
        ok(f"100 个 ID 全部唯一")
    except Exception as e:
        fail(f"ID 唯一性失败: {e}")

# ============================================================
section("4. Event Bus — 发布/消费")
# ============================================================

if test("EventBus 初始化"):
    try:
        from analysis.event_bus import EventBus

        bus = EventBus()
        assert bus is not None
        ok("EventBus 初始化成功")
    except Exception as e:
        fail(f"EventBus 初始化失败: {e}")

if test("EventBus 发布事件"):
    try:
        from analysis.event_bus import EventBus, EventType

        bus = EventBus()
        eid = bus.publish(EventType.SCREENING_TRIGGER, {
            "session": "US_PRE_MARKET",
            "target_count": 10,
        })
        assert eid > 0
        ok(f"事件发布成功: event_id={eid}")
    except Exception as e:
        fail(f"EventBus 发布失败: {e}")

if test("EventBus 消费事件"):
    try:
        from analysis.event_bus import EventBus, EventType

        bus = EventBus()
        eid = bus.publish(EventType.ANALYSIS_TRIGGER, {"symbol": "AAPL"})
        
        events = bus.consume(EventType.ANALYSIS_TRIGGER, "test_worker")
        assert len(events) > 0
        assert events[0].event_id == eid
        assert events[0].payload["symbol"] == "AAPL"
        ok(f"事件消费成功: {len(events)} 个事件")
    except Exception as e:
        fail(f"EventBus 消费失败: {e}")

if test("EventBus 失败重试"):
    try:
        from analysis.event_bus import EventBus, EventType

        bus = EventBus()
        eid = bus.publish(EventType.RISK_ALERT, {"symbol": "TSLA"})
        
        events = bus.consume(EventType.RISK_ALERT, "test_worker")
        assert len(events) == 1
        bus.fail_event(eid, "test error")
        
        # 应该可以再次消费（retry_count < max_retries）
        events2 = bus.consume(EventType.RISK_ALERT, "test_worker2")
        assert len(events2) == 1
        assert events2[0].retry_count == 1
        ok(f"失败重试成功: retry_count={events2[0].retry_count}")
    except Exception as e:
        fail(f"EventBus 失败重试失败: {e}")

# ============================================================
section("5. Feature Store — 查询")
# ============================================================

if test("FeatureStore 初始化"):
    try:
        from analysis.feature_store import FeatureStore

        fs = FeatureStore()
        assert fs is not None
        ok("FeatureStore 初始化成功")
    except Exception as e:
        fail(f"FeatureStore 初始化失败: {e}")

if test("FeatureStore get_features"):
    try:
        from analysis.feature_store import FeatureStore

        fs = FeatureStore()
        # 查询任意存在的 symbol
        ff = fs.get_features("AAPL")
        # 可能返回 None（如果 DB 中没有该 symbol 的因子）
        if ff:
            assert ff.symbol == "AAPL"
            assert len(ff.factors) > 0
            ok(f"查询成功: {len(ff.factors)} 个因子")
        else:
            ok("查询返回 None（DB 中无因子数据，预期行为）")
    except Exception as e:
        fail(f"FeatureStore 查询失败: {e}")

# ============================================================
section("6. Storage Schema — 新表")
# ============================================================

if test("execution_log 表"):
    try:
        from storage import Database

        db = Database()
        db.init_schema()
        row = db.conn.execute("PRAGMA table_info(execution_log)").fetchall()
        cols = [r[1] for r in row]
        assert "execution_id" in cols
        assert "started_at" in cols
        assert "status" in cols
        ok(f"execution_log 表存在，列: {cols}")
        db.close()
    except Exception as e:
        fail(f"execution_log 表检查失败: {e}")

if test("signal_log 表"):
    try:
        from storage import Database

        db = Database()
        db.init_schema()
        row = db.conn.execute("PRAGMA table_info(signal_log)").fetchall()
        cols = [r[1] for r in row]
        assert "signal_id" in cols
        assert "symbol" in cols
        assert "action_taken" in cols
        ok(f"signal_log 表存在，列: {cols}")
        db.close()
    except Exception as e:
        fail(f"signal_log 表检查失败: {e}")

if test("trades 表新增字段"):
    try:
        from storage import Database

        db = Database()
        db.init_schema()
        row = db.conn.execute("PRAGMA table_info(trades)").fetchall()
        cols = [r[1] for r in row]
        assert "signal_id" in cols
        assert "execution_id" in cols
        ok(f"trades 表新增字段: signal_id, execution_id")
        db.close()
    except Exception as e:
        fail(f"trades 表字段检查失败: {e}")

if test("event_bus 表"):
    try:
        from storage import Database

        db = Database()
        db.init_schema()
        row = db.conn.execute("PRAGMA table_info(event_bus)").fetchall()
        cols = [r[1] for r in row]
        assert "event_type" in cols
        assert "consumed" in cols
        ok(f"event_bus 表存在，列: {cols}")
        db.close()
    except Exception as e:
        fail(f"event_bus 表检查失败: {e}")

# ============================================================
section("7. Auto Execute — 导入和函数签名")
# ============================================================

if test("auto_execute.py 导入"):
    try:
        from scripts.auto_execute import execute_signals, check_risk_rules, execute_trade
        assert callable(execute_signals)
        assert callable(check_risk_rules)
        assert callable(execute_trade)
        ok("auto_execute.py 导入成功，函数存在")
    except Exception as e:
        fail(f"auto_execute.py 导入失败: {e}")

if test("_update_signal_log 函数存在"):
    try:
        from scripts.auto_execute import _update_signal_log
        assert callable(_update_signal_log)
        ok("_update_signal_log 函数存在")
    except Exception as e:
        fail(f"_update_signal_log 函数不存在: {e}")

# ============================================================
section("8. Trace Query — 命令行")
# ============================================================

if test("trace_query.py 导入"):
    try:
        from scripts.trace_query import query_execution, query_signal, list_executions
        assert callable(query_execution)
        assert callable(query_signal)
        assert callable(list_executions)
        ok("trace_query.py 导入成功")
    except Exception as e:
        fail(f"trace_query.py 导入失败: {e}")

if test("trace_query --list-executions"):
    try:
        from scripts.trace_query import list_executions
        list_executions(5)
        ok("list_executions 执行成功")
    except Exception as e:
        fail(f"list_executions 失败: {e}")

# ============================================================
section("9. Shadow Executor — 初始化")
# ============================================================

if test("ShadowExecutor 初始化"):
    try:
        from executors.shadow_executor import ShadowExecutor

        executor = ShadowExecutor()
        assert executor is not None
        ok("ShadowExecutor 初始化成功")
    except Exception as e:
        fail(f"ShadowExecutor 初始化失败: {e}")

if test("shadow_trades 表"):
    try:
        from storage import Database

        db = Database()
        db.init_schema()
        # ShadowExecutor 创建表
        from executors.shadow_executor import ShadowExecutor
        ShadowExecutor()
        
        row = db.conn.execute("PRAGMA table_info(shadow_trades)").fetchall()
        if row:
            cols = [r[1] for r in row]
            assert "symbol" in cols
            assert "pnl" in cols
            ok(f"shadow_trades 表存在，列: {cols}")
        else:
            ok("shadow_trades 表待首次使用时创建")
        db.close()
    except Exception as e:
        fail(f"shadow_trades 表检查失败: {e}")

# ============================================================
section("10. Vector Memory — 降级兼容")
# ============================================================

if test("VectorMemory 初始化（无 embedding 模型时降级）"):
    try:
        from analysis.vector_memory import VectorMemory
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            mem = VectorMemory("test", tmpdir)
            mem.load()
            # 没有 sentence-transformers 时应该使用 fallback
            results = mem.search("test query")
            ok(f"VectorMemory 初始化成功（fallback 模式），搜索结果: {len(results)}")
    except Exception as e:
        fail(f"VectorMemory 初始化失败: {e}")

# ============================================================
section("汇总")
# ============================================================

print(f"\n{'#'*60}")
print(f"# 测试汇总")
print(f"{'#'*60}")
print(f"\n  总计: {total}")
print(f"  ✅ 通过: {passed}")
print(f"  ❌ 失败: {failed}")
print(f"  通过率: {passed/total*100:.1f}%" if total > 0 else "")

if failed == 0:
    print(f"\n🎉 全部通过！v6.0 系统功能正常！")
else:
    print(f"\n⚠️  {failed} 个测试失败，请检查上方日志。")

sys.exit(0 if failed == 0 else 1)
