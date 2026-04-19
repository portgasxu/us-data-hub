# v6.0 全局优化实施计划

**创建时间**: 2026-04-19 00:52
**修订时间**: 2026-04-19 01:00 — 全局架构视角重审
**目标**: 在 v5.3 稳定运行基础上，引入标准化契约、全局追溯、轻量事件总线
**核心原则**: 每个优化必须作用于**全链路**，不是单点补丁

---

## 全局架构总览（v5.3 现状）

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          CRONTAB 调度层                                  │
│  数据采集(每30min) │ 事件监听(每5min) │ 全链路交易(每15min) │ 策略验证    │
└────────┬──────────────────┬──────────────────┬──────────────────┬────────┘
         │                  │                  │                  │
    ┌────▼────┐      ┌──────▼──────┐    ┌──────▼──────┐   ┌────▼─────┐
    │ 采集层   │      │  watcher.py │    │ auto_execute│   │ validate │
    │ (5个     │      │  新闻异动   │    │   --full    │   │ _strategy│
    │ Collector)    │  触发选股   │    │   -loop     │   │ .py      │
    └────┬────┘      └──────┬──────┘    └──────┬──────┘   └────┬─────┘
         │                  │                  │               │
    ┌────▼──────────────────▼──────────────────▼───────────────▼────┐
    │                     Storage Layer (SQLite)                     │
    │  data_points │ prices │ factors │ holdings │ trades │ alerts  │
    └────┬──────────────────┬──────────────────┬───────────────────┘
         │                  │                  │
    ┌────▼────┐      ┌──────▼──────┐    ┌──────▼──────┐
    │ Screener│      │ Signal Hub  │    │ Holding     │
    │ 三层漏斗 │      │ 6源信号聚合  │    │ Monitor     │
    └────┬────┘      └──────┬──────┘    └──────┬──────┘
         │                  │                  │
         │           ┌──────▼──────┐           │
         │           │TradingAgents│           │
         │           │ 5路并行LLM  │           │
         │           └──────┬──────┘           │
         │                  │                  │
    ┌────▼──────────────────▼──────────────────▼────┐
    │              auto_execute.py (执行层)           │
    │  信号消费 → 动态阈值 → 智能风控4级 → 长桥下单    │
    └─────────────────────────┬─────────────────────┘
                              │
                         ┌────▼────┐
                         │ DayUp   │
                         │ Logger  │
                         │ 4类日志  │
                         └─────────┘
```

**全链路数据流**: 采集 → 存储 → 因子计算 → Signal Hub (聚合 Screener/持仓/情感/因子/SEC/TA) → auto_execute → 下单 → 日志

---

## 一、总体路线图

```
Phase 1: 全链路信号契约统一 (P0, 最优先) — 影响 6个信号源 + Signal Hub + 执行层
  ↓
Phase 2: 全局 Trace ID 贯穿 (P0, 依赖 Phase 1) — 影响 采集→存储→分析→执行→日志
  ↓
Phase 3: 轻量事件总线 (P1) — 影响 crontab + watcher + auto_execute + screen_to_trade
  ↓
Phase 4: Feature Store 统一因子 (P1) — 影响 Screener + TradingAgents + Backtest + Alphalens
  ↓
Phase 5: 语义向量检索升级 (P2) — 影响 TradingAgents 全部 5 个 Memory 实例
  ↓
Phase 6: 影子策略验证 (P2) — 与主链路并行，不影响现有流程
```

---

## Phase 1: 全链路信号契约统一（⚠️ P0 最高优先级）

### 1.1 全局视角的问题分析

**现状**: 系统有 **6 个信号源**，每个产生的信号格式不一致：

| 信号源 | 当前格式 | 问题 |
|--------|---------|------|
| `holding_monitor` | `Signal` 对象 (signal_hub.py) | ✅ 已统一 |
| `screener` | dict with `total_score` | ❌ 没有方向(direction)，只有分数 |
| `sentiment` | `Signal` 对象 | ✅ 已统一 |
| `factors` | `Signal` 对象 | ✅ 已统一 |
| `sec_filing` | `Signal` 对象 | ✅ 已统一 |
| `trading_agents` | **自然语言文本** | ❌❌ 最严重，下游靠正则解析 |
| `watcher.py` | 直接调 screener | ❌ 不经过 Signal Hub |
| `screen_to_trade.py` | 直接调 TradingAgents | ❌ 不经过 Signal Hub |

**全局问题**:
1. **两条并行路径**: 一条经过 Signal Hub (auto_execute --full-loop)，一条绕过 Signal Hub (watcher.py 直调 screener、screen_to_trade.py 直调 TradingAgents)
2. **Signal Hub 的 `Signal` 类是好的**，但不是所有模块都遵守
3. **TradingAgents 是唯一输出自然语言的信号源**，是全链路最薄弱的环节

### 1.2 交付物

#### A. 升级 Signal 类（向后兼容）

```python
# analysis/signal_schema.py — 新文件
"""
统一信号契约 — 全链路所有信号源必须输出此结构。
兼容现有 signal_hub.py 的 Signal 类。
"""
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from datetime import datetime, timedelta
from enum import Enum

class SignalDirection(Enum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"

class SignalSource(Enum):
    HOLDING_MONITOR = "holding_monitor"
    SCREENER = "screener"
    SENTIMENT = "sentiment"
    FACTORS = "factors"
    SEC_FILING = "sec_filing"
    TRADING_AGENTS = "trading_agents"
    WATCHER = "watcher"          # 新增：事件驱动触发
    MANUAL = "manual"            # 新增：人工干预

@dataclass
class TradeSignal:
    """
    全链路统一信号 — 所有信号源的最终输出格式。
    
    流向: 信号源 → SignalHub.add() → auto_execute.py 消费
    """
    
    # ── 核心字段 ──
    symbol: str                           # "AAPL"
    direction: SignalDirection            # buy / sell / hold
    confidence: float                     # 0.0 ~ 1.0
    source: SignalSource                  # 信号来源
    
    # ── 价格参数（可选） ──
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    target_price: Optional[float] = None
    
    # ── 仓位参数 ──
    quantity_suggestion: int = 0          # 建议数量（0=由执行层计算）
    position_size_pct: float = 0.0        # 占总仓位百分比 (0-100)
    
    # ── 上下文 ──
    strength: float = 0.5                 # 信号强度 0-1
    reason: str = ""                      # 决策理由（人类可读）
    extra: dict = field(default_factory=dict)  # 源系统原始数据
    
    # ── 追溯（Phase 2 填充） ──
    signal_id: str = ""
    
    # ── 时间 ──
    timestamp: str = ""
    
    # ═══ 兼容方法 ═══
    
    @property
    def source_name(self) -> str:
        """兼容 signal_hub.py 的 source 字段"""
        return self.source.value
    
    def priority(self) -> float:
        """
        兼容 signal_hub.py 的 priority() 方法。
        排序权重: 持仓监控 > TradingAgents > 情感 > 因子 > 选股 > 其他
        """
        w = {
            SignalSource.HOLDING_MONITOR: 1.0,
            SignalSource.TRADING_AGENTS: 0.95,
            SignalSource.SENTIMENT: 0.8,
            SignalSource.FACTORS: 0.75,
            SignalSource.SCREENER: 0.7,
            SignalSource.SEC_FILING: 0.65,
            SignalSource.WATCHER: 0.6,
            SignalSource.MANUAL: 1.0,
        }
        source_w = w.get(self.source, 0.5)
        return self.confidence * 0.6 + self.strength * 0.2 + source_w * 0.2
    
    def to_dict(self) -> dict:
        """兼容 signal_hub.py 的 to_dict() — auto_execute.py 消费此格式"""
        d = asdict(self)
        d["direction"] = self.direction.value
        d["source"] = self.source.value
        d["priority"] = self.priority()
        return d
    
    @classmethod
    def from_dict(cls, d: dict) -> "TradeSignal":
        """从 dict 反序列化"""
        d = dict(d)  # copy
        if isinstance(d.get("direction"), str):
            d["direction"] = SignalDirection(d["direction"])
        if isinstance(d.get("source"), str):
            d["source"] = SignalSource(d["source"])
        d.pop("priority", None)  # computed field
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})
    
    @classmethod
    def from_old_signal(cls, old_signal) -> "TradeSignal":
        """
        从现有 signal_hub.py 的 Signal 对象转换。
        确保向后兼容。
        """
        return cls(
            symbol=old_signal.symbol,
            direction=SignalDirection(old_signal.direction),
            confidence=old_signal.confidence,
            source=SignalSource(old_signal.source),
            strength=old_signal.strength,
            reason=old_signal.reason,
            quantity_suggestion=old_signal.quantity_suggestion,
            extra=old_signal.extra,
        )
```

#### B. 改造 TradingAgents 输出结构化信号

**影响范围**: `tradingagents/main.py` → `portfolio_manager.py` → `signal_hub.py`

**改造点**:

```python
# tradingagents/main.py — 修改 run_trading_analysis()
def run_trading_analysis(stock_symbol, signal_id=None, trading_date=None, market="US"):
    # ... 现有代码 ...
    
    # 调用 TradingAgentsGraph
    result = graph.run(state, company_of_interest=stock_symbol, 
                      market=market, trade_date=trading_date)
    
    # ── 新增：从结构化输出构建 TradeSignal ──
    trade_signal = _build_trade_signal_from_result(result, stock_symbol)
    
    return {
        "symbol": stock_symbol,
        "date": trading_date,
        "decision": result.get("final_trade_decision", ""),
        "trade_signal": trade_signal.to_dict(),  # 新增：结构化输出
        "risk_debate": result.get("risk_debate_state", {}),
        "company_report": company_report,
    }

def _build_trade_signal_from_result(result, symbol):
    """从 TradingAgents 结果构建 TradeSignal"""
    from analysis.signal_schema import TradeSignal, SignalDirection, SignalSource
    
    risk_state = result.get("risk_debate_state", {})
    decision_text = result.get("final_trade_decision", "").lower()
    
    # 从决策文本提取方向
    direction = SignalDirection.HOLD
    if any(w in decision_text for w in ["buy", "purchase", "enter"]):
        direction = SignalDirection.BUY
    elif any(w in decision_text for w in ["sell", "exit", "close", "liquidate"]):
        direction = SignalDirection.SELL
    
    # 提取置信度（如果有）
    confidence = 0.5
    import re
    conf_match = re.search(r'confidence[:\s]*(\d+\.?\d*)', decision_text)
    if conf_match:
        confidence = float(conf_match.group(1)) / 100.0
    
    return TradeSignal(
        symbol=symbol,
        direction=direction,
        confidence=confidence,
        source=SignalSource.TRADING_AGENTS,
        strength=0.8,
        reason=result.get("final_trade_decision", ""),
        extra={"full_result": result},
    )
```

#### C. 改造 Screener 输出 Signal 对象

**影响范围**: `analysis/screener.py` → `signal_hub._collect_screener()`

**改造点**:

```python
# signal_hub.py — _collect_screener() 改造
def _collect_screener(self):
    """Screener 信号 → 标准化 TradeSignal"""
    try:
        from analysis.screener import ThreeLayerScreener
        from analysis.signal_schema import TradeSignal, SignalDirection, SignalSource
        
        screener = ThreeLayerScreener(self.db)
        results = screener.screen(top_n=20, min_score=0.3)
        
        # 获取当前持仓
        held = set()
        for row in self.db.conn.execute(
            "SELECT symbol FROM holdings WHERE active = 1 AND quantity > 0"
        ).fetchall():
            held.add(row[0])
        
        avg_score = sum(r["total_score"] for r in results) / len(results) if results else 0
        
        for r in results:
            symbol = r["symbol"]
            score = r["total_score"]
            
            if symbol in held:
                continue
            
            if score > avg_score:
                # ── 改造：直接创建 TradeSignal ──
                signal = TradeSignal(
                    symbol=symbol,
                    direction=SignalDirection.BUY,
                    confidence=min(0.9, 0.5 + score * 0.4),
                    source=SignalSource.SCREENER,
                    strength=score,
                    reason=self._build_screener_reason(r),
                    extra=r,
                )
                self.add(signal)  # SignalHub.add() 接受 TradeSignal
    
    except Exception as e:
        logger.error(f"Screener signal collection failed: {e}")
```

#### D. 改造 watcher.py 通过 Signal Hub 触发

**影响范围**: `scripts/watcher.py` → `signal_hub.py`

**改造点**:

```python
# watcher.py — 改造后
def run_watcher():
    """新闻异动 → 通过 Signal Hub 触发事件驱动选股"""
    from storage import Database
    from analysis.signal_hub import SignalHub, Signal, SignalDirection, SignalSource
    
    db = Database()
    
    # 检测新闻异动（现有逻辑不变）
    alerts = detect_news_surges(db)
    
    if alerts:
        # ── 改造：不再直接调 Screener，而是通过 Signal Hub ──
        hub = SignalHub(db)
        
        # 1. 发布 watcher 信号
        for symbol in alerts:
            hub.add(Signal(
                symbol=symbol,
                direction=SignalDirection.BUY,
                confidence=0.7,
                source=SignalSource.WATCHER,
                strength=0.8,
                reason=f"新闻异动: {symbol}",
            ))
        
        # 2. 通过 Signal Hub 触发 Screener
        hub._collect_screener()
        
        # 3. 返回聚合后的信号
        signals = hub.get_tradable_signals()
        logger.info(f"Watcher triggered {len(signals)} signals via Signal Hub")
    
    db.close()
```

#### E. 改造 screen_to_trade.py 通过 Signal Hub

**影响范围**: `scripts/screen_to_trade.py`

**改造点**: 与 watcher.py 类似，不再直接调 TradingAgents，而是将结果输出为 `TradeSignal`，经过 Signal Hub 聚合后再执行。

### 1.3 全局影响矩阵

| 组件 | 改造前 | 改造后 | 兼容性 |
|------|--------|--------|--------|
| `signal_hub.py` 的 `Signal` | 独立类 | 与 `TradeSignal` 兼容 | ✅ 向后兼容 |
| `holding_monitor` | 输出 Signal | 输出 Signal（不变） | ✅ 无需改 |
| `screener` | 输出 dict | 输出 TradeSignal | ⚠️ 需要改 |
| `sentiment` | 输出 Signal | 输出 TradeSignal | ⚠️ 小改 |
| `factors` | 输出 Signal | 输出 TradeSignal | ⚠️ 小改 |
| `sec_filing` | 输出 Signal | 输出 TradeSignal | ⚠️ 小改 |
| `trading_agents` | 输出自然语言 | 输出 TradeSignal | ⚠️ 大改 |
| `watcher.py` | 直调 Screener | 通过 Signal Hub | ⚠️ 需要改 |
| `screen_to_trade.py` | 直调 TA | 通过 Signal Hub | ⚠️ 需要改 |
| `auto_execute.py` | 消费 dict | 消费 TradeSignal | ✅ 兼容（to_dict()） |

### 1.4 验收标准

- [ ] `signal_schema.py` 创建，所有信号源都 import 它
- [ ] `signal_hub.py` 的 `Signal` 类和 `TradeSignal` 双向兼容
- [ ] 6 个信号源全部输出 `TradeSignal`（或兼容格式）
- [ ] `watcher.py` 和 `screen_to_trade.py` 不再绕过 Signal Hub
- [ ] `auto_execute.py` 只消费 `TradeSignal.to_dict()`
- [ ] 单次 full-loop 测试通过，信号链路完整

---

## Phase 2: 全局 Trace ID 贯穿（⚠️ P0，依赖 Phase 1）

### 2.1 全局视角的问题分析

**现状**: 系统的追溯链路断裂在多个节点：

```
采集层    →  无 trace_id
  ↓
存储层    →  无 trace_id
  ↓
因子计算  →  无 trace_id
  ↓
Signal Hub →  无 trace_id
  ↓
TradingAgents →  有 decision 文本，无结构化 ID
  ↓
auto_execute →  有 trade 记录，无 signal_id 关联
  ↓
DayUp Logger →  有分类日志，无全局 ID 关联
```

**问题**: 当出现一笔异常交易时，需要从 4 类日志 (risk/screening/decision/market) + DB 中手动拼接，耗时且容易遗漏。

### 2.2 交付物

#### A. Trace ID 生成器

```python
# analysis/trace_id.py — 新文件
"""
全局追溯 ID 生成器。
所有 ID 格式: {PREFIX}_YYYYMMDD_HHMMSS_6位短UUID
"""
import uuid
from datetime import datetime

def generate_signal_id() -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:6]
    return f"SIG_{ts}_{short}"

def generate_execution_id() -> str:
    """执行批次 ID — 一次 auto_execute --full-loop 一个 ID"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:6]
    return f"EXE_{ts}_{short}"

def generate_collection_id() -> str:
    """数据采集批次 ID"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:6]
    return f"COL_{ts}_{short}"
```

#### B. 全链路 ID 传递

```
┌──────────────────────────────────────────────────────────────┐
│  全链路 Trace ID 流向                                         │
│                                                              │
│  crontab 触发 auto_execute --full-loop                       │
│    → 生成 execution_id (EXE_20260419_161500_a3f2b1)          │
│    → 写入 execution_log 表                                    │
│    ↓                                                         │
│  SignalHub.collect_all()                                     │
│    → 每个 Signal 分配 signal_id (SIG_...)                     │
│    → signal_id + execution_id 写入 signal_log 表              │
│    ↓                                                         │
│  TradingAgents.run_trading_analysis(signal_id=SIG_xxx)       │
│    → decision 日志记录 signal_id                              │
│    ↓                                                         │
│  auto_execute._execute_signal(signal)                        │
│    → trade 表记录 signal_id + execution_id                    │
│    → 风控日志记录 signal_id                                   │
│    ↓                                                         │
│  DayUp Logger                                                │
│    → 所有 4 类日志都包含 execution_id 和 signal_id            │
└──────────────────────────────────────────────────────────────┘
```

#### C. 数据库变更

```sql
-- 所有表新增追溯字段
ALTER TABLE trades ADD COLUMN signal_id TEXT DEFAULT '';
ALTER TABLE trades ADD COLUMN execution_id TEXT DEFAULT '';

ALTER TABLE monitor_alerts ADD COLUMN signal_id TEXT DEFAULT '';
ALTER TABLE monitor_alerts ADD COLUMN execution_id TEXT DEFAULT '';

-- 新增执行批次表
CREATE TABLE IF NOT EXISTS execution_log (
    execution_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT DEFAULT 'RUNNING',  -- RUNNING / COMPLETED / FAILED
    signals_collected INTEGER DEFAULT 0,
    trades_executed INTEGER DEFAULT 0,
    errors TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 新增信号追溯表
CREATE TABLE IF NOT EXISTS signal_log (
    signal_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    source TEXT NOT NULL,
    confidence REAL,
    execution_id TEXT,
    action_taken TEXT,  -- EXECUTED / SKIPPED / REJECTED
    rejection_reason TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### D. Trace 查询工具

```python
# scripts/trace_query.py — 新文件
"""
全链路追溯查询工具。
用法:
    python scripts/trace_query --execution EXE_20260419_161500_a3f2b1
    python scripts/trace_query --signal SIG_20260419_161500_a3f2b1
    python scripts/trace_query --symbol AAPL --date 2026-04-19
"""
import argparse
import sqlite3
import json
import glob
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "us_data_hub.db")

def query_execution(execution_id: str):
    """查询一次完整执行的所有关联记录"""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    
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
    
    # 2. 信号列表
    signals = db.execute(
        "SELECT * FROM signal_log WHERE execution_id = ?",
        (execution_id,)
    ).fetchall()
    if signals:
        print(f"\n📡 信号 ({len(signals)} 条):")
        for s in signals:
            action_icon = {"EXECUTED": "✅", "SKIPPED": "⏭️", "REJECTED": "❌"}.get(s["action_taken"], "❓")
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
                  f"| signal={t['signal_id']}")
    
    # 4. 关联日志文件
    print(f"\n📝 关联日志:")
    for log_dir in ["logs/risk", "logs/screening", "logs/decision", "logs/market"]:
        if os.path.exists(log_dir):
            for log_file in glob.glob(f"{log_dir}/*.json"):
                # 检查日志中是否包含 execution_id
                with open(log_file) as f:
                    content = f.read()
                    if execution_id in content:
                        print(f"    📄 {log_file}")
    
    db.close()
```

### 2.3 全局影响矩阵

| 组件 | 需要做什么 |
|------|-----------|
| `auto_execute.py` | 启动时生成 execution_id，传递到所有子模块 |
| `signal_hub.py` | 每个 Signal 生成 signal_id，写入 signal_log |
| `tradingagents/main.py` | 接收 signal_id，记录到 decision 日志 |
| `executors/longbridge.py` | 记录 execution_id 到 trade 表 |
| `monitoring/holding_monitor.py` | 生成 alert 时记录 signal_id |
| `dayup_logger.py` | 所有日志函数增加 execution_id/signal_id 参数 |
| `scripts/run.py` | 各命令支持 --trace-id 参数 |

### 2.4 验收标准

- [ ] `trace_id.py` 创建
- [ ] 数据库新增 execution_log、signal_log 表
- [ ] 所有 DB 写入包含 signal_id / execution_id
- [ ] `trace_query.py` 能完整展示一条追溯链
- [ ] 日终日志归档中包含追溯 ID

---

## Phase 3: 轻量事件总线（P1）

### 3.1 全局视角的问题分析

**现状**: 全链路由 **crontab** 驱动，本质是"定时器 + 直接函数调用"：

```
crontab → auto_execute.py
           ├── SignalHub.collect_all()    # 直接调用
           ├── 动态阈值检查                # 直接调用
           ├── 智能风控                    # 直接调用
           └── 长桥下单                    # 直接调用

crontab → watcher.py
           └── screener.screen()          # 直接调用，绕过 Signal Hub

crontab → screen_to_trade.py
           └── run_trading_analysis()     # 直接调用，绕过 Signal Hub
```

**全局问题**:
1. **三条独立路径**，没有统一的事件调度
2. **新增任务类型**需要改 crontab + 写新脚本
3. **任务间无法联动**: watcher 发现异动不能自动触发 TradingAgents 分析
4. **crontab 是单点的**: 一个 timeout 就整批丢失

### 3.2 方案

用 **SQLite 事件表** 实现轻量事件总线（文档建议 Kafka，规模太重）：

```python
# analysis/event_bus.py — 新文件
"""
轻量事件总线 — 替代 crontab 直接调用的全局事件调度层。

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
from datetime import datetime, timedelta
from typing import List, Optional
from dataclasses import dataclass, asdict

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
    parent_event_id: int = None  # 链式追溯

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
    ANALYSIS_TRIGGER = "ANALYSIS_TRIGGER"       # 触发 TradingAgents
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    
    # 执行层
    EXECUTION_TRIGGER = "EXECUTION_TRIGGER"      # 触发 auto_execute
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
                os.path.dirname(os.path.dirname(__file__)),
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
                    priority INTEGER DEFAULT 0  -- 高优先级先消费
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_event_type_consumed
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
            logger.info(f"📨 Event published: {event_type} (id={cursor.lastrowid})")
            return cursor.lastrowid
    
    def consume(self, event_type: str, consumer: str, limit: int = 10) -> List[Event]:
        """消费指定类型的未处理事件"""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """SELECT * FROM event_bus
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
        """标记事件处理失败"""
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
```

### 3.3 全链路事件流

```
┌─────────────────────────────────────────────────────────────────┐
│                     改造后的 crontab                             │
│                                                                  │
│  采集: publish COLLECTION_TRIGGER                                │
│  监听: publish NEWS_SURGE_DETECTED (watcher 检测到)               │
│  交易: publish EXECUTION_TRIGGER (不再直接调 auto_execute)        │
│  验证: publish STRATEGY_VALIDATION                               │
└────────────────────────────┬────────────────────────────────────┘
                             │
                    ┌────────▼────────┐
                    │   Event Bus     │
                    │  (SQLite 表)    │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼────┐  ┌──────▼──────┐  ┌────▼─────┐
     │ screening   │  │ analysis    │  │ execution│
     │ worker      │  │ worker      │  │ worker   │
     │ consume     │  │ consume     │  │ consume  │
     │ SCREENING   │  │ ANALYSIS    │  │ EXECUTION│
     │ _TRIGGER    │  │ _TRIGGER    │  │ _TRIGGER │
     └─────────────┘  └─────────────┘  └──────────┘
          │                  │                │
          ▼                  ▼                ▼
     publish            publish          publish
     ANALYSIS_          EXECUTION_       DAILY_
     TRIGGER            TRIGGER          SUMMARY
          │                  │
          └──────┬───────────┘
                 ▼
          链式触发，自动流转
```

### 3.4 Crontab 改造

```bash
# 改造后：crontab 只负责发布事件，不直接调用业务逻辑

# 采集触发
0 8,10,12,14,15,16,17,18,19,20,21,22,23,0,1,2,3,4 * * * \
  cd /path && python3 -c "from analysis.event_bus import EventBus, EventType; \
  EventBus().publish(EventType.COLLECTION_TRIGGER, {'sources': ['longbridge']})"

# 事件监听
*/5 * * * * cd /path && python3 scripts/watcher.py --once

# 全链路执行
*/15 16-23,0-4 * * 1-5 \
  cd /path && python3 -c "from analysis.event_bus import EventBus, EventType; \
  EventBus().publish(EventType.EXECUTION_TRIGGER, {'min_confidence': 0.75})"

# 策略验证
0 8 * * 1-5 \
  cd /path && python3 -c "from analysis.event_bus import EventBus, EventType; \
  EventBus().publish(EventType.STRATEGY_VALIDATION, {'days': 90})"
```

### 3.5 全局影响矩阵

| 组件 | 改造前 | 改造后 |
|------|--------|--------|
| `crontab` | 直接调脚本 | 只发布事件 |
| `watcher.py` | 直调 screener | 发布 NEWS_SURGE → 消费者处理 |
| `auto_execute.py` | crontab 直接调 | 消费 EXECUTION_TRIGGER |
| `screen_to_trade.py` | crontab 直接调 | 消费 SCREENING_COMPLETE → 发布 ANALYSIS_TRIGGER |
| 新增 | 无 | 事件 worker 进程 |

### 3.6 验收标准

- [ ] `event_bus.py` 创建
- [ ] crontab 只发布事件，不直接调业务逻辑
- [ ] 至少一个完整事件链跑通（SCREENING → ANALYSIS → EXECUTION）
- [ ] 失败重试机制正常
- [ ] 事件清理机制正常

---

## Phase 4: Feature Store 统一因子服务（P1）

### 4.1 全局视角的问题分析

**现状**: 因子数据被 **4 个消费者**以不同方式查询：

| 消费者 | 查询方式 | 问题 |
|--------|---------|------|
| `screener.py` | 直接在 `_calc_news_heat` 等函数中查 DB | 每次独立查询，可能读到不同时间的数据 |
| `signal_hub._collect_factors()` | 逐因子查询 `factors` 表 | N × M 次 DB 查询 (N 标的 × M 因子) |
| `TradingAgents` | `dataflows/interface.py` → `analyze_stock()` | 独立数据流，可能和 screener 因子不一致 |
| `backtest.py` / `alphalens_backtest.py` | 直接查 `factors` 表 | 与实盘因子可能口径不同 |

**全局问题**:
1. **因子口径不一致**: screener 的 momentum 和 TA 的 momentum 可能计算方式不同
2. **因子时间不一致**: 不同模块查询时可能读到不同时间的因子值
3. **重复计算**: 每次查询都从 prices 实时算，没有缓存
4. **扩展困难**: 新增因子需要改所有消费者

### 4.2 方案

```
┌─────────────────────────────────────────────────────────────┐
│                    Feature Store (全局统一)                   │
│                                                              │
│  输入: prices (raw) + SEC filings (raw)                     │
│    ↓                                                         │
│  compute_all() → 批量计算 13 因子 → 写入 factors 表          │
│    ↓                                                         │
│  get_features(symbol, date) → 一次返回所有因子                │
│    ↓                                                         │
│  消费者: Screener / TradingAgents / Backtest / Alphalens     │
│         全部通过 FeatureStore 获取因子                        │
└─────────────────────────────────────────────────────────────┘
```

```python
# analysis/feature_store.py — 新文件
"""
全局统一因子服务 — 所有模块通过此接口获取因子数据。

设计原则:
  1. 单一数据源: 所有因子计算集中在此
  2. 一致性: 同一 (symbol, date) 返回相同的因子值
  3. 缓存: 计算结果缓存，避免重复计算
  4. 可追溯: 记录因子计算时间和版本
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import sqlite3
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# 全局因子定义
ALL_FACTOR_NAMES = [
    # 基础 5 因子
    "momentum", "rsi", "volatility", "value", "quality",
    # 扩展 8 因子
    "macd", "bollinger", "obv", "adx", "historical_volatility",
    "vwap", "mfi", "accumulation_distribution",
]

@dataclass
class FeatureFrame:
    """单一 (symbol, date) 的因子快照"""
    symbol: str
    date: str
    factors: Dict[str, float] = field(default_factory=dict)
    computed_at: str = ""
    
    def get(self, name: str, default: float = 0.0) -> float:
        return self.factors.get(name, default)
    
    def has(self, name: str) -> bool:
        return name in self.factors
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "date": self.date,
            "factors": self.factors,
            "computed_at": self.computed_at,
        }

class FeatureStore:
    """全局统一因子服务"""
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data", "us_data_hub.db"
            )
        self.db_path = db_path
    
    def get_features(self, symbol: str, date: str = None,
                     factor_names: List[str] = None) -> Optional[FeatureFrame]:
        """
        查询指定标的在指定日期的因子。
        
        如果 date 为 None，返回最新可用因子。
        如果 factor_names 为 None，返回全部因子。
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        if factor_names is None:
            factor_names = ALL_FACTOR_NAMES
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT date, factor_name, factor_value, factor_meta, created_at
                   FROM factors
                   WHERE symbol = ? AND date <= ?
                   AND factor_name IN ({})
                   ORDER BY date DESC
                   LIMIT {}""".format(
                    ", ".join("?" for _ in factor_names),
                    len(factor_names)
                ),
                (symbol, date, *factor_names)
            ).fetchall()
            
            if not row:
                return None
            
            factors = {}
            computed_at = ""
            for r in row:
                factors[r["factor_name"]] = r["factor_value"]
                computed_at = r["created_at"]
            
            return FeatureFrame(
                symbol=symbol,
                date=date,
                factors=factors,
                computed_at=computed_at,
            )
    
    def get_features_batch(self, symbols: List[str],
                           date: str = None) -> Dict[str, FeatureFrame]:
        """批量查询多只标的的因子"""
        result = {}
        for symbol in symbols:
            ff = self.get_features(symbol, date)
            if ff:
                result[symbol] = ff
        return result
    
    def compute_and_store(self, symbol: str, prices: list,
                          date: str = None) -> FeatureFrame:
        """
        计算全部 13 个因子并存入 DB。
        这是唯一允许写入因子的方法。
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        # 调用现有因子计算模块
        from analysis.factor_from_prices import compute_factors
        from analysis.extended_factors import compute_extended_factors
        
        base = compute_factors(prices)
        extended = compute_extended_factors(prices)
        all_factors = {**base, **extended}
        
        # 写入 DB
        with sqlite3.connect(self.db_path) as conn:
            for name, value in all_factors.items():
                conn.execute(
                    """INSERT OR REPLACE INTO factors
                       (date, symbol, factor_name, factor_value, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (date, symbol, name, value, datetime.now().isoformat())
                )
        
        logger.info(f"FeatureStore: computed {len(all_factors)} factors for {symbol} @ {date}")
        
        return FeatureFrame(
            symbol=symbol,
            date=date,
            factors=all_factors,
            computed_at=datetime.now().isoformat(),
        )
    
    def compute_all(self, symbols: List[str], prices_map: Dict[str, list],
                    date: str = None) -> Dict[str, FeatureFrame]:
        """批量计算多只标的的因子"""
        result = {}
        for symbol in symbols:
            if symbol in prices_map:
                result[symbol] = self.compute_and_store(
                    symbol, prices_map[symbol], date
                )
        return result
```

### 4.3 全局消费者迁移计划

| 消费者 | 迁移方式 | 优先级 |
|--------|---------|--------|
| `signal_hub._collect_factors()` | 改用 `FeatureStore.get_features()` | 高 |
| `screener.py` 的 `_calc_volume_surge` | 改用 FeatureStore | 高 |
| `tradingagents/dataflows/interface.py` | 改用 FeatureStore | 中 |
| `backtest.py` | 改用 FeatureStore | 中 |
| `alphalens_backtest.py` | 改用 FeatureStore | 低 |

### 4.4 验收标准

- [ ] `feature_store.py` 创建
- [ ] 所有因子计算集中到 FeatureStore
- [ ] Screener、Signal Hub、TradingAgents 都通过 FeatureStore 获取因子
- [ ] 同一 (symbol, date) 查询结果一致
- [ ] 新增因子只需改 FeatureStore，不改消费者

---

## Phase 5: 语义向量检索升级（P2）

### 5.1 全局视角的问题分析

**现状**: TradingAgents 有 **5 个 Memory 实例**，全部使用 BM25：

```
TradingAgentsGraph.run():
  └── build_risk_committee():
       ├── risk_mem = KnowledgeMemory("risk", "risk_lessons")
       ├── bull_mem = KnowledgeMemory("bull", "bull_lessons")
       ├── bear_mem = KnowledgeMemory("bear", "bear_lessons")
       ├── macro_mem = KnowledgeMemory("macro", "macro_lessons")
       └── judge_mem = KnowledgeMemory("judge", "judge_lessons")
```

**全局问题**:
1. **BM25 只能字面匹配**: "盈利预警" 匹配不上 "earnings miss"
2. **5 个 Memory 独立存储**: 复盘结论散落在 5 个目录
3. **知识不互通**: bull 学到的经验，bear 不知道

### 5.2 方案

#### 选项 A：本地 embedding（推荐，零成本）
- `bge-small-en-v1.5` (~130MB)，CPU 推理 ~50条/秒
- 5 个 Memory 共享同一个 embedding model

#### 选项 B：DashScope embedding（低成本）
- `text-embedding-v3`，几分钱/次
- 质量更好，有 API 依赖

```python
# analysis/vector_memory.py — 新文件
"""
语义向量记忆 — 替代 BM25，支持语义相似性匹配。
全 TradingAgents 的 5 个 Memory 实例共享同一个 embedding model。
"""
import json
import os
import numpy as np
from typing import List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class VectorMemory:
    """基于向量检索的记忆系统"""
    
    _model = None  # 全局共享
    
    @classmethod
    def get_model(cls):
        """懒加载 embedding model（全局单例）"""
        if cls._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                cls._model = SentenceTransformer('BAAI/bge-small-en-v1.5')
                logger.info("VectorMemory: embedding model loaded")
            except ImportError:
                raise ImportError("pip install sentence-transformers")
        return cls._model
    
    def __init__(self, name: str, lessons_dir: str):
        self.name = name
        self.lessons_dir = os.path.join(lessons_dir, f"{name}_lessons")
        self.situations: List[str] = []
        self.recommendations: List[str] = []
        self.vectors: np.ndarray = None
    
    def load(self):
        """从磁盘加载"""
        if not os.path.exists(self.lessons_dir):
            return
        for file in os.listdir(self.lessons_dir):
            if file.endswith(".json"):
                with open(os.path.join(self.lessons_dir, file)) as f:
                    data = json.load(f)
                    self.situations.append(data.get("situation", ""))
                    self.recommendations.append(data.get("recommendation", ""))
        
        if self.situations:
            model = self.get_model()
            self.vectors = model.encode(self.situations, normalize_embeddings=True)
            logger.info(f"VectorMemory[{self.name}]: loaded {len(self.situations)} memories")
    
    def search(self, query: str, n: int = 2) -> List[dict]:
        """语义搜索"""
        if not self.vectors or len(self.vectors) == 0:
            return []
        
        model = self.get_model()
        query_vec = model.encode([query], normalize_embeddings=True)
        scores = np.dot(self.vectors, query_vec.T).flatten()
        top_idx = scores.argsort()[::-1][:n]
        
        results = []
        for idx in top_idx:
            results.append({
                "matched_situation": self.situations[idx],
                "recommendation": self.recommendations[idx],
                "similarity": float(scores[idx]),
            })
        return results
    
    def add(self, situation: str, recommendation: str):
        """新增记忆"""
        self.situations.append(situation)
        self.recommendations.append(recommendation)
        
        model = self.get_model()
        self.vectors = model.encode(self.situations, normalize_embeddings=True)
        
        # 持久化
        os.makedirs(self.lessons_dir, exist_ok=True)
        idx = len(self.situations) - 1
        path = os.path.join(self.lessons_dir, f"memory_{idx}.json")
        with open(path, "w") as f:
            json.dump({
                "situation": situation,
                "recommendation": recommendation,
            }, f, ensure_ascii=False)
```

### 5.3 全局影响

| 组件 | 影响 |
|------|------|
| `memory.py` 的 `KnowledgeMemory` | 保留 BM25 作为 fallback，新增 VectorMemory |
| `trading_graph.py` 的 `build_risk_committee()` | 5 个 Memory 改用 VectorMemory |
| `judge_mem` | 跨类型检索（bull + bear + risk 一起搜） |
| 磁盘存储 | 保持 JSON 格式，只是检索方式变了 |

### 5.4 验收标准

- [ ] `vector_memory.py` 创建
- [ ] 语义检索能匹配关键词不同但意思相近的文本
- [ ] 5 个 Memory 实例都改用 VectorMemory
- [ ] BM25 作为 fallback 保留
- [ ] 单次检索 < 100ms

---

## Phase 6: 影子策略验证（P2）

### 6.1 全局视角的问题分析

**现状**: 系统有策略验证，但是**离线回测**（`validate_strategy.py`），不是**实时影子执行**。

```
现有: crontab → validate_strategy.py → 回测历史信号 → 输出报告
期望: 影子策略与主策略同时执行同一信号 → 实时对比 P&L
```

**全局问题**:
1. **回测 ≠ 实盘**: 回测用历史数据，实盘有滑点、流动性等
2. **验证周期长**: 每周才跑一次，策略变更可能要等一周才被发现
3. **无法对比多策略**: 只能回测一个策略，不能同时跑多个对比

### 6.2 方案

```
┌──────────────────────────────────────────────────────────────┐
│                    影子策略并行执行                            │
│                                                               │
│  Signal Hub                                                   │
│    → 信号 A (AAPL BUY conf=0.8)                               │
│      ↓                                                        │
│  ┌──────────────────┬──────────────────┐                     │
│  │  主策略执行       │  影子策略执行     │                     │
│  │  (auto_execute)  │  (shadow_execute) │                     │
│  │                  │                  │                     │
│  │  ✅ 真实下单      │  📝 只记账       │                     │
│  │  💰 实际 P&L     │  📊 模拟 P&L     │                     │
│  └────────┬─────────┴────────┬─────────┘                     │
│           │                  │                               │
│           ▼                  ▼                               │
│  ┌─────────────────────────────────────────┐                │
│  │          对比引擎                         │                │
│  │                                          │                │
│  │  主策略: 30天 P&L +12.5%, 胜率 65%       │                │
│  │  影子:   30天 P&L +15.2%, 胜率 72%       │                │
│  │                                          │                │
│  │  ⚠️ 影子策略表现更好! 建议切换            │                │
│  └─────────────────────────────────────────┘                │
└──────────────────────────────────────────────────────────────┘
```

```python
# executors/shadow_executor.py — 新文件
"""
影子策略执行器 — 与主策略并行运行，只记账不下单。

全局设计:
  1. 影子策略接收与主策略完全相同的信号
  2. 使用不同的参数/规则处理（可配置）
  3. 获取实时价格用于模拟成交
  4. 每日/每周对比主策略 vs 影子策略 P&L
  5. 影子持续优于主策略时告警
"""
import sqlite3
import logging
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class ShadowConfig:
    """影子策略配置 — 与主策略的区别点"""
    
    # 动态阈值参数（更激进或更保守）
    BUY_THRESHOLD = 0.30      # 主策略 0.35
    SELL_THRESHOLD = -0.25    # 主策略 -0.20
    STOP_LOSS_PCT = 0.06      # 主策略 0.08
    TAKE_PROFIT_PCT = 0.15    # 主策略 0.10
    MAX_POSITIONS = 8         # 主策略 5
    
    # 信号权重（更依赖某些信号源）
    SIGNAL_WEIGHTS = {
        "trading_agents": 1.2,   # 更信赖 TA
        "screener": 0.8,
        "sentiment": 0.9,
        "factors": 1.0,
    }

class ShadowExecutor:
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "data", "us_data_hub.db"
            )
        self.db_path = db_path
        self._init_table()
    
    def _init_table(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS shadow_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    quantity REAL,
                    entry_price REAL,
                    exit_price REAL DEFAULT 0,
                    signal_id TEXT,
                    execution_id TEXT,
                    strategy_name TEXT DEFAULT 'shadow_v1',
                    entry_at TEXT,
                    exit_at TEXT,
                    pnl REAL DEFAULT 0,
                    pnl_pct REAL DEFAULT 0,
                    status TEXT DEFAULT 'OPEN',  -- OPEN / CLOSED
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
    
    def execute_signal(self, signal, execution_id: str = None):
        """记录影子交易（不实际下单）"""
        from analysis.signal_schema import TradeSignal, SignalDirection
        
        price = self._get_current_price(signal.symbol)
        if not price:
            return
        
        quantity = signal.quantity_suggestion or 10
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO shadow_trades
                   (symbol, action, quantity, entry_price, signal_id, execution_id, entry_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (signal.symbol, signal.direction.value, quantity,
                 price, getattr(signal, 'signal_id', ''), execution_id,
                 datetime.now().isoformat())
            )
        
        logger.info(f"👻 Shadow: {signal.symbol} {signal.direction.value} "
                     f"{quantity} @ ${price:.2f}")
    
    def update_positions(self):
        """更新持仓的当前价格和 P&L"""
        # 获取所有 OPEN 的影子持仓
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            open_trades = conn.execute(
                "SELECT * FROM shadow_trades WHERE status = 'OPEN'"
            ).fetchall()
            
            for trade in open_trades:
                current_price = self._get_current_price(trade["symbol"])
                if current_price:
                    pnl = (current_price - trade["entry_price"]) * trade["quantity"]
                    pnl_pct = (current_price - trade["entry_price"]) / trade["entry_price"]
                    
                    conn.execute(
                        """UPDATE shadow_trades SET pnl = ?, pnl_pct = ?
                           WHERE id = ?""",
                        (pnl, pnl_pct, trade["id"])
                    )
    
    def compare_with_main(self, days: int = 30) -> Dict:
        """对比影子策略 vs 主策略"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            main = conn.execute(
                """SELECT COUNT(*) as trades,
                          AVG(CASE WHEN actual_return > 0 THEN 1 ELSE 0 END) as win_rate,
                          AVG(actual_return) as avg_return,
                          SUM(actual_return) as total_return
                   FROM trades
                   WHERE timestamp >= date('now', ?)
                   AND actual_return IS NOT NULL""",
                (f"-{days} days",)
            ).fetchone()
            
            shadow = conn.execute(
                """SELECT COUNT(*) as trades,
                          AVG(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as win_rate,
                          AVG(pnl_pct) as avg_return,
                          SUM(pnl) as total_pnl
                   FROM shadow_trades
                   WHERE entry_at >= date('now', ?)""",
                (f"-{days} days",)
            ).fetchone()
            
            return {
                "main": {
                    "trades": main["trades"],
                    "win_rate": main["win_rate"] or 0,
                    "avg_return": main["avg_return"] or 0,
                    "total_return": main["total_return"] or 0,
                },
                "shadow": {
                    "trades": shadow["trades"],
                    "win_rate": shadow["win_rate"] or 0,
                    "avg_return": shadow["avg_return"] or 0,
                    "total_return": shadow["total_pnl"] or 0,
                },
                "shadow_better": (shadow["total_pnl"] or 0) > (main["total_return"] or 0),
            }
```

### 6.3 集成到全链路

```python
# auto_execute.py — 改造后
def main():
    # ... 现有代码 ...
    
    signals = hub.get_tradable_signals()
    
    # 主策略执行
    for sig in signals:
        execute_signal(sig)
    
    # ── 新增：影子策略并行执行 ──
    try:
        from executors.shadow_executor import ShadowExecutor
        shadow = ShadowExecutor()
        for sig in signals:
            shadow.execute_signal(sig, execution_id=execution_id)
        shadow.update_positions()
    except Exception as e:
        logger.warning(f"Shadow executor failed: {e}")
```

### 6.4 验收标准

- [ ] `shadow_executor.py` 创建
- [ ] 影子策略接收与主策略相同信号
- [ ] 影子策略只记账不下单
- [ ] 对比报告能生成
- [ ] 影子策略 P&L 与主策略对比正确

---

## 附录 A: 全局文件变更清单

| Phase | 新增文件 | 修改文件 | 全局影响 |
|-------|---------|----------|---------|
| 1 | `analysis/signal_schema.py` | `signal_hub.py`, `screener.py`, `watcher.py`, `screen_to_trade.py`, `tradingagents/main.py`, `auto_execute.py` | 全链路所有信号源 |
| 2 | `analysis/trace_id.py`, `scripts/trace_query.py` | `storage/__init__.py`(schema), `signal_hub.py`, `auto_execute.py`, `dayup_logger.py`, `tradingagents/main.py` | 采集→存储→分析→执行→日志 |
| 3 | `analysis/event_bus.py` | `crontab`, `watcher.py`, `auto_execute.py`, `screen_to_trade.py` | 调度层 + 所有 worker |
| 4 | `analysis/feature_store.py` | `signal_hub.py`, `screener.py`, `tradingagents/dataflows/`, `backtest.py` | 所有因子消费者 |
| 5 | `analysis/vector_memory.py` | `tradingagents/agents/utils/memory.py`, `trading_graph.py` | TradingAgents 全部 5 个 Memory |
| 6 | `executors/shadow_executor.py` | `auto_execute.py`, `crontab` | 主链路 + 影子链路 |

## 附录 B: 数据库变更

```sql
-- Phase 2: 追溯字段
ALTER TABLE trades ADD COLUMN signal_id TEXT DEFAULT '';
ALTER TABLE trades ADD COLUMN execution_id TEXT DEFAULT '';
ALTER TABLE monitor_alerts ADD COLUMN signal_id TEXT DEFAULT '';
ALTER TABLE monitor_alerts ADD COLUMN execution_id TEXT DEFAULT '';

-- Phase 2: 新增表
CREATE TABLE IF NOT EXISTS execution_log (...);
CREATE TABLE IF NOT EXISTS signal_log (...);

-- Phase 3: 事件总线
CREATE TABLE IF NOT EXISTS event_bus (...);

-- Phase 6: 影子策略
CREATE TABLE IF NOT EXISTS shadow_trades (...);
```

## 附录 C: 里程碑与预估

| 里程碑 | 包含 Phase | 预估工作量 | 风险等级 | 全局影响 |
|--------|-----------|-----------|---------|---------|
| M1: 信号契约统一 | Phase 1 | 3-4 天 | 中 | 全链路 6 个信号源 |
| M2: 可追溯性 | Phase 2 | 2 天 | 低 | 全链路 + 所有 DB 表 |
| M3: 事件解耦 | Phase 3 | 3-4 天 | 中 | 调度层 + 所有 worker |
| M4: 因子统一 | Phase 4 | 2-3 天 | 低 | 所有因子消费者 |
| M5: 语义记忆 | Phase 5 | 2 天 | 低 | TA 全部 5 个 Memory |
| M6: 影子策略 | Phase 6 | 2-3 天 | 低 | 主链路 + 影子链路 |

**总预估**: 14-18 天（Phase 4+5 可并行，Phase 6 随时可做）

## 附录 D: 全局架构检查清单

每个 Phase 完成后，必须回答以下问题：

1. **数据流**: 这个改动影响了哪些数据流？上下游都适配了吗？
2. **兼容性**: 旧代码还能跑吗？有没有 fallback？
3. **存储**: 数据库 schema 变了吗？迁移脚本写了吗？
4. **调度**: crontab 需要改吗？事件流通了吗？
5. **日志**: 新改动在日志中可见吗？追溯 ID 记录了吗？
6. **监控**: 如果这个改动出问题，能快速发现吗？
7. **回滚**: 如果必须回滚，步骤是什么？

---

## 📌 执行纪律

1. **按 Phase 顺序执行**，不要跳步
2. 每个 Phase 完成后**全链路测试**再合并
3. **保持向后兼容**，每个 Phase 有 fallback
4. 完成后更新本文件，标记 ✅
5. 遇到问题记录到 `memory/YYYY-MM-DD.md`
6. **每次改动都要从全局架构视角审视**，不能只看局部

---

*本文档由全局架构分析生成，作为 v6.0 开发的唯一参考。每个 Phase 都经过全链路审视，确保不是单点补丁。*
