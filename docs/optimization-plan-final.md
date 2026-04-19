# 美股自动交易系统深度优化方案（终版）

> 创建时间: 2026-04-19 14:35 | 终版: 2026-04-19 14:54 | Phase 1-3 完成: 2026-04-19 15:40
> 覆盖：架构 / LLM 分流 / 模块设计 / 超时并发 / 运行频率 / 交易执行 / 模块耦合 / 时段策略 / 顶层闭环

---

## 〇、系统全局总览

```
                    ┌─────────────────────────────────────────┐
                    │            顶层架构闭环                   │
                    │                                         │
                    │  定期选股 → 综合决策 → 自动交易           │
                    │     ↑                              ↓     │
                    │     └──── 定期复盘 ← 策略优化 ← 记录     │
                    └─────────────────────────────────────────┘

┌─────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  数据采集层  │──→│  三层选股器   │──→│  多智能体分析  │──→│  信号聚合中心  │
│ Collectors  │   │ Screener     │   │ TradingAgents│   │ SignalHub    │
└─────────────┘   └──────────────┘   └──────────────┘   └──────┬───────┘
                                                               │
              ┌────────────────────────────────────────────────┤
              │                                                │
    ┌─────────▼─────────┐   ┌──────────────┐   ┌──────────────┐
    │  动态阈值          │   │  持仓监控     │   │  熔断器       │
    │ DynamicThreshold  │   │ HoldingMon   │   │ CircuitBreak │
    └─────────┬─────────┘   └──────┬───────┘   └──────┬───────┘
              │                    │                   │
              └────────────────────┼───────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │       交易执行层              │
                    │  auto_execute.py (待拆分)    │
                    └─────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                     LLM 双端点调度层                              │
│                                                                  │
│  CodingPlan (主力): TradingAgents/阈值/持仓 → qwen3.6-plus       │
│  百炼 (分流):       情感/SEC/监控 → flash/coder/turbo            │
│  详见第一章 LLM 分流方案                                          │
└──────────────────────────────────────────────────────────────────┘
```

---

## 一、LLM 双端点并行分流方案

### 1.1 端点配置

```
┌──────────────────┬────────────────────┬───────────────────┐
│                  │ CodingPlan (主力)   │ 百炼 (辅助分流)   │
├──────────────────┼────────────────────┼───────────────────┤
│ Base URL         │ coding.dashscope.  │ dashscope.        │
│                  │ aliyuncs.com/v1    │ aliyuncs.com/     │
│                  │                    │ compatible-mode/v1│
├──────────────────┼────────────────────┼───────────────────┤
│ API Key          │ sk-sp-2ff8d56f...  │ sk-64b866f9...    │
├──────────────────┼────────────────────┼───────────────────┤
│ 主力模型         │ qwen3.6-plus       │ 按需分流          │
├──────────────────┼────────────────────┼───────────────────┤
│ 定位             │ 核心交易决策        │ 轻量/辅助任务      │
└──────────────────┴────────────────────┴───────────────────┘
```

### 1.2 分流原则

```
是否核心交易决策？
  ├─ 是 → CodingPlan (qwen3.6-plus)
  │        包括：买卖信号、牛熊辩论、风控、持仓监控、动态阈值
  │
  └─ 否 → 百炼分流
           ├─ 简单分类/情感 → qwen3.6-flash (最快最便宜)
           ├─ 结构化解析    → qwen3-coder-plus (格式转换专长)
           ├─ 极速判断      → qwen-turbo (10s 级响应)
           └─ 中等分析      → qwen3.6-plus (均衡之选)
```

### 1.3 任务-端点映射表

| 任务 | 端点 | 模型 | 超时 | 频率 |
|------|------|------|------|------|
| TradingAgents 牛熊辩论 | CodingPlan | qwen3.6-plus | 120s | 每次 Full Loop |
| TradingAgents 投资法官 | CodingPlan | qwen3.6-plus | 120s | 每次 Full Loop |
| TradingAgents 风控辩论 | CodingPlan | qwen3.6-plus | 90s | 每次 Full Loop |
| 动态阈值分析 | CodingPlan | qwen3.6-plus | 90s | 每次 Full Loop |
| 持仓监控 LLM | CodingPlan | qwen3.6-plus | 90s | 盘中每 2h |
| 周度复盘 | CodingPlan | qwen3.6-plus | 180s | 每周六 |
| 盘前简报 | CodingPlan | qwen3.6-plus | 120s | 每日盘前 |
| 情感分析 batch | 百炼 | qwen3.6-flash | 30s | 每 15min |
| 新闻分类/标签 | 百炼 | qwen3.6-flash | 15s | 实时 |
| SEC XBRL 解析 | 百炼 | qwen3-coder-plus | 90s | 每 30min |
| 市场情绪监控 | 百炼 | qwen3.6-flash | 15s | 每 5min |
| 异常检测 (watcher) | 百炼 | qwen-turbo | 10s | 每 5min |
| 策略验证回测 | 百炼 | qwen3.6-flash | 30s | 每日 8:00 |

### 1.4 并行执行架构

```
Full Loop 触发
  │
  ├─ 数据采集（串行，快）
  │
  ├─ 并行 LLM 调用组 ───────────────────────────────────┐
  │                                                      │
  │  CodingPlan 端点 (qwen3.6-plus):                     │
  │  ├─ TradingAgents × 5只候选股 (并发 5)               │
  │  ├─ 持仓监控 × 1                                     │
  │  └─ 动态阈值 × N (串行错峰)                           │
  │                                                      │
  │  百炼端点 (多模型):                                  │
  │  ├─ qwen3.6-flash: 情感分析 batch (并发 10)          │
  │  ├─ qwen3-coder:  SEC 解析 × 2 (并发 2)             │
  │  └─ qwen-turbo:   异常检测 (并发 1)                  │
  │                                                      │
  ├─ SignalHub 聚合所有结果                              │
  └─ 风控仲裁 → 交易执行（串行）                          │

总耗时：从 ~25min (串行) 降至 ~60-120s (双端点并行)
```

### 1.5 限流与降级策略

| 端点/模型 | RPM 限制 | 最大并发 | 降级路径 |
|-----------|---------|---------|---------|
| CodingPlan (qwen3.6-plus) | 100 | 8 | → 百炼 qwen3.6-plus |
| 百炼 qwen3.6-plus | 100 | 8 | → qwen3.6-flash → 固定阈值 |
| 百炼 qwen3.6-flash | 200 | 15 | → 跳过 |
| 百炼 qwen3-coder-plus | 50 | 3 | → qwen3.6-flash |
| 百炼 qwen-turbo | 300 | 20 | → qwen3.6-flash |

**四大核心机制**：
1. **双端点独立限流** — CodingPlan 和百炼各自独立的 RPM 跟踪，互不影响
2. **Semaphore 并发控制** — 每个模型/端点独立的并发信号量
3. **跨端点降级** — CodingPlan 失败 → 自动降级到百炼同模型
4. **指数退避重试** — 1s → 2s → 4s，最多重试 3 次

### 1.6 环境变量配置

```bash
# .env
CODING_PLAN_KEY=sk-sp-2ff8d56f399d49c3b83f9db670627f46
BAILIAN_KEY=sk-64b866f9e6fa4e3588e7326bcc487d6d
# 兼容旧配置
OPENAI_API_KEY=sk-sp-2ff8d56f399d49c3b83f9db670627f46
DASHSCOPE_API_KEY=sk-64b866f9e6fa4e3588e7326bcc487d6d
```

### 1.7 LLMRouter 核心实现

```python
"""
llm_router.py — 双端点 LLM 并行调度器
架构: CodingPlan (主力决策) + 百炼 (辅助分流)
"""

import asyncio, time, os, logging
from openai import AsyncOpenAI, OpenAI

# ─── 端点配置 ───
CODING_PLAN_URL = "https://coding.dashscope.aliyuncs.com/v1"
CODING_PLAN_KEY = os.getenv("CODING_PLAN_KEY")
CODING_PLAN_MODEL = "qwen3.6-plus"

BAILIAN_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
BAILIAN_KEY = os.getenv("BAILIAN_KEY")

# ─── 百炼模型配置 ───
BAILIAN_MODELS = {
    "qwen3.6-plus": {"rpm": 100, "concurrent": 8, "timeout": 90, "temp": 0.3, "fallback": "qwen3.6-flash"},
    "qwen3.6-flash": {"rpm": 200, "concurrent": 15, "timeout": 30, "temp": 0.2, "fallback": None},
    "qwen3-coder-plus": {"rpm": 50, "concurrent": 3, "timeout": 90, "temp": 0.1, "fallback": "qwen3.6-flash"},
    "qwen-turbo": {"rpm": 300, "concurrent": 20, "timeout": 10, "temp": 0.1, "fallback": "qwen3.6-flash"},
}

# ─── 任务路由表 ───
TASK_ROUTING = {
    "trading_agents_debate":  {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "trading_agents_judge":   {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "trading_agents_risk":    {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "dynamic_threshold":      {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "holding_monitor":        {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "weekly_review":          {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "morning_brief":          {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "sentiment_analysis":     {"endpoint": "bailian", "model": "qwen3.6-flash"},
    "news_classification":    {"endpoint": "bailian", "model": "qwen3.6-flash"},
    "sec_parsing":            {"endpoint": "bailian", "model": "qwen3-coder-plus"},
    "market_watcher":         {"endpoint": "bailian", "model": "qwen-turbo"},
    "strategy_validate":      {"endpoint": "bailian", "model": "qwen3.6-flash"},
}

class LLMRouter:
    def __init__(self):
        # CodingPlan 客户端
        self.cp_async = AsyncOpenAI(api_key=CODING_PLAN_KEY, base_url=CODING_PLAN_URL, timeout=120)
        self.cp_sync = OpenAI(api_key=CODING_PLAN_KEY, base_url=CODING_PLAN_URL, timeout=120)
        self.cp_semaphore = asyncio.Semaphore(8)

        # 百炼多模型客户端
        self.bl_async = {}
        self.bl_sync = {}
        self.bl_semaphores = {}
        for m, cfg in BAILIAN_MODELS.items():
            self.bl_async[m] = AsyncOpenAI(api_key=BAILIAN_KEY, base_url=BAILIAN_URL, timeout=cfg["timeout"])
            self.bl_sync[m] = OpenAI(api_key=BAILIAN_KEY, base_url=BAILIAN_URL, timeout=cfg["timeout"])
            self.bl_semaphores[m] = asyncio.Semaphore(cfg["concurrent"])

    async def parallel_invoke(self, tasks: dict) -> dict:
        """并行调用: tasks = {"name": (task_type, messages)}"""
        coros = {n: self.invoke_async(t, m) for n, (t, m) in tasks.items()}
        results = await asyncio.gather(*coros.values(), return_exceptions=True)
        return {k: (r if not isinstance(r, Exception) else {"error": str(r)})
                for k, r in zip(coros.keys(), results)}

    # ... (invoke_sync / invoke_async / _invoke_coding_plan / _invoke_bailian 省略)
```

### 1.8 各模块改造清单

| 模块 | 改造前 | 改造后 |
|------|--------|--------|
| `auto_execute.py` | 硬编码 LLM 调用 | `router.invoke("dynamic_threshold", msg)` |
| `holding_monitor.py` | 固定 `ChatOpenAI(model="qwen3.6-plus")` | `router.invoke("holding_monitor", msg)` |
| `tradingagents/main.py` | 单只串行分析 | `router.parallel_invoke({"AAPL_ta": ("trading_agents_debate", msg), ...})` |
| `processors/sentiment.py` | `SENTIMENT_MODEL = "qwen3-coder-plus"` | `router.invoke("sentiment_analysis", msg)` → 自动走百炼 flash |

---

## 二、整体架构核心缺陷与修复

### 🔴 缺陷 1：双轨调度冲突

**现状**：Crontab 直调（`collect.py`/`watcher.py`/`auto_execute.py`）与 Event Bus 空壳并存，Event Bus 发布了事件但没有消费者。

**修复**：实现 Event-driven worker 链式消费：
```
CRON → Event Bus → 采集 Worker → COLLECTION_COMPLETE
                   → 分析 Worker → ANALYSIS_COMPLETE
                   → 执行 Worker → EXECUTION_COMPLETE
                   → 复盘 → 反馈到 Screener 参数
```

### 🔴 缺陷 2：auto_execute.py 承担过多职责

**现状**：1053 行代码同时承担数据采集、因子计算、选股、信号聚合、动态阈值、风控、交易执行、盘前简报、复盘分析，任何一环超时都会被 `timeout 200` 杀掉。

**修复**：拆分为独立服务：
```
auto_execute.py (1053行)
  ├── collectors_worker.py    ← 数据采集
  ├── analysis_worker.py      ← 信号聚合 + 动态阈值
  └── executor_worker.py      ← 交易执行
```

### 🔴 缺陷 3：交易时间逻辑硬编码缺陷

**现状**：`get_market_session()` 只区分夏令时/冬令时盘中时段，无盘前/盘后/夜盘/节假日处理。

**修复**：新增完整时段策略模块（详见第七章）。

---

## 三、逐模块拆解评估

### 3.1 数据采集层（collectors/）

| 维度 | 现状 | 评估 | 优化方案 |
|------|------|------|---------|
| 设计 | Collector 基类 + rate limiting | ✅ 良好 | 保持 |
| 并发 | 逐个 symbol 串行遍历 | ❌ 7只×5源=35次串行，最坏 17.5min | ThreadPoolExecutor 并行采集，按数据源分组 |
| 超时 | timeout=30s | ⚠️ 无重试 | 增加 2 次重试 + 指数退避 |
| LLM | 无 | N/A | 不涉及 |

**改造示例**：
```python
# 改造前：串行
for collector in collectors:
    for symbol in symbols:
        collector.collect(symbol)

# 改造后：并行
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(c.collect, s) for c in collectors for s in symbols]
```

### 3.2 三层选股器（screener.py）

| 维度 | 现状 | 评估 | 优化方案 |
|------|------|------|---------|
| 设计 | 热度→行业→成长三层漏斗 | ✅ 清晰 | 保持 |
| LLM | 全规则驱动 | ❌ 未利用 LLM | 加 LLM 行业轮动判断（CodingPlan qwen3.6-plus） |
| 性能 | 纯 DB 查询 | ✅ 快速 | 保持 |

**优化**：用 LLM 判断当前市场行业周期，动态更新 `HOT_INDUSTRIES`，而非硬编码。

### 3.3 TradingAgents 多智能体

| 维度 | 现状 | 评估 | 优化方案 |
|------|------|------|---------|
| 设计 | 牛熊辩论 + 风控辩论 | ✅ 架构先进 | 保持 |
| LLM | qwen3.6-plus | ✅ 已利用 | 改为 CodingPlan qwen3.6-plus 主力 |
| 问题 | `max_debate_rounds=1` 硬编码覆盖 | ❌ 削弱多 Agent 价值 | 恢复 rounds=2，用 quick_llm 控制成本 |
| 并发 | 单只串行 | ❌ Top-5 串行 = 15-25min | 5 只并行分析 via `parallel_invoke` |

### 3.4 信号中心（signal_hub.py）

| 维度 | 现状 | 评估 | 优化方案 |
|------|------|------|---------|
| 设计 | 统一契约 + 去重 + 冲突解决 | ✅ 良好 | 保持 |
| 并发 | 串行遍历 Top 5 | ❌ 每只 3-5min，总 25min | 并行调用 LLMRouter，降至 5min |

### 3.5 持仓监控（holding_monitor.py）

| 维度 | 现状 | 评估 | 优化方案 |
|------|------|------|---------|
| 设计 | LLM 一次分析所有持仓 | ✅ 高效 | 保持 |
| LLM | qwen3.6-plus | ✅ 合理 | 改为 CodingPlan 端点 |
| 超时 | 90s | ✅ 合理 | 保持 |

### 3.6 动态阈值（dynamic_threshold.py）

| 维度 | 现状 | 评估 | 优化方案 |
|------|------|------|---------|
| 设计 | 规则版 + LLM 版双模式 | ✅ 良好 | 保持 |
| 并发 | 逐个串行处理信号 | ❌ 每只 90s | N 信号 via `parallel_invoke`，总 90s |
| LLM | qwen3.6-plus | ✅ 合理 | 改为 CodingPlan 端点 |

### 3.7 交易执行（auto_execute.py）

详见第四章深度分析。

### 3.8 熔断机制（circuit_breaker.py）

| 维度 | 现状 | 评估 | 优化方案 |
|------|------|------|---------|
| 设计 | 三层保护（日亏损/连亏/VIX） | ✅ 良好 | 保持 |
| 参数 | `max_daily_loss_usd=500` 硬编码 | ⚠️ 不随账户规模调整 | 改为动态百分比 |

---

## 四、交易执行模块深度优化（重点）

### 4.1 问题一：重复提交订单

**根因**：`_execute_trades()` 遍历信号时无跨信号全局去重锁，同一信号在相邻两次 loop 中被重复拾取。

**修复**：增加订单冷却机制
```python
def _check_order_cooldown(self, symbol, direction, minutes=10) -> bool:
    """同一标的同方向 N 分钟内不允许重复下单"""
    row = self.db.conn.execute(
        "SELECT cooldown_until FROM signal_cooldowns "
        "WHERE symbol=? AND direction=? AND source='order_lock' AND cooldown_until > datetime('now')",
        (symbol, direction)
    ).fetchone()
    return row is not None

def _lock_order(self, symbol, direction, minutes=10):
    """下单后立即锁定"""
    self.db.conn.execute(
        "INSERT INTO signal_cooldowns (symbol,direction,source,cooldown_until,reason) "
        "VALUES (?,?, 'order_lock', datetime('now', ?), 'order_lock')",
        (symbol, direction, f'+{minutes} minutes')
    )
    self.db.conn.commit()
```

### 4.2 问题二：风控信号冲突

**根因**：熔断、订单冷却、信号 cooldown、动态阈值、Risk Manager 多层风控无优先级定义，串行检查无仲裁。

**修复**：统一风控仲裁器（按优先级依次检查）
```
P0 熔断 (Circuit Breaker)    — 最高优先，halt all
P1 订单冷却 (Order Cooldown)  — 防止重复下单
P2 信号 cooldown              — 信号去重
P3 动态阈值 (Dynamic Threshold) — 信号质量过滤
P4 Risk Manager              — 综合风控检查
```

```python
def _risk_arbitration(self, signal) -> Tuple[bool, str]:
    if self._circuit_breaker_halted():
        return False, "熔断: 系统暂停"
    if self._check_order_cooldown(signal.symbol, signal.direction):
        return False, "订单冷却中"
    if self._is_signal_in_cooldown(signal):
        return False, "信号冷却中"
    if not self._passes_dynamic_threshold(signal):
        return False, "阈值未达"
    if not self.risk_mgr.check(signal):
        return False, f"风控拒绝: {self.risk_mgr.reason}"
    return True, "通过"
```

### 4.3 问题三：订单执行后状态不一致

**根因**：`record_sell_trade → log_trade → cleanup_position` 多个写操作无事务包裹，中间失败导致持仓状态分裂。

**修复**：SQLite 事务包裹
```python
def _execute_sell(self, signal, risk_result, session_type):
    try:
        with self.db.transaction():  # 原子事务
            result = self._place_order(signal, "sell")
            self._record_sell_trade(result, signal)
            self.db.log_trade(signal, "SELL", f"EXECUTED | {result[:150]}")
            self._cleanup_position_if_sold(result, signal)
            self._lock_order(signal.symbol, "sell", minutes=10)  # 防重复
    except Exception as e:
        logger.error(f"Sell transaction failed, rolled back: {e}")
        self._record_failed_trade(signal, e)
```

### 4.4 问题四：超时导致执行数据不完整

**根因**：前置步骤（数据采集/TA分析/信号聚合）超时后，执行逻辑拿到不完整数据导致混乱。

**修复**：数据完整性检查 + 安全模式
```python
def _validate_execution_data(self, signals) -> bool:
    """执行前检查数据完整性"""
    if not signals:
        return False
    for s in signals:
        if not s.has_price or not s.has_volume:
            logger.warning(f"Signal {s.symbol} missing critical data")
            return False
    return True

# 如果数据不完整 → 进入安全模式：仅执行硬编码止损/止盈
```

---

## 五、运行频率优化

### 5.1 当前 vs 优化后

| 任务 | 当前频率 | 优化后频率 | 调整理由 |
|------|---------|-----------|---------|
| Longbridge 价格采集 | 盘中 30min | **盘中 5min** | 高频价格对实时交易关键 |
| Google News | 30min | **盘中 15min** | 新闻驱动交易机会 |
| Auto Execute Full Loop | 盘中 15min | **盘中 30min** | 减少过度交易，降低成本 |
| Holding Monitor | 无定时 | **盘中每 2h** | 定期持仓风险检查 |
| Screener 选股 | 无定时 | **盘前每 2h** | 保持选股池更新 |
| SEC 采集 | 30min | 30min | ✅ 合理，保持 |
| Reddit 采集 | 每小时 | 每小时 | ✅ 合理，保持 |
| Watcher 监控 | 5min | 5min | ✅ 合理，保持 |
| 策略验证 | 工作日 8:00 + 周六 | 工作日 8:00 + 周六 | ✅ 合理，保持 |
| 因子计算 | 每天 4:00 | 每天 4:00 | ✅ 合理，保持 |

### 5.2 优化后完整调度表

```bash
# ═══════════════════════════════════════════════════════
# 盘前（北京时间 15:00-21:30）→ 低频数据采集 + 选股更新
# ═══════════════════════════════════════════════════════
*/30 15-21 * * 1-5  collect (price only)
0 16,18,20 * * 1-5  screener + 选股简报 (CodingPlan)

# ═══════════════════════════════════════════════════════
# 盘中（冬 21:30-04:00 / 夏 22:30-05:00）→ 高频 + 全功能
# ═══════════════════════════════════════════════════════
*/5  21-03 * * 1-5  collect (price + news)
*/15 21-03 * * 1-5  watcher (百炼 turbo)
*/30 21-03 * * 1-5  auto_execute (signal + risk)
0 22,0,2 * * 1-5   holding_monitor (CodingPlan)

# ═══════════════════════════════════════════════════════
# 盘后（北京时间 04:00-08:00）→ 复盘 + 数据补全
# ═══════════════════════════════════════════════════════
0 5 * * 1-5  复盘分析 (CodingPlan) + 策略验证 (百炼 flash)
0 6 * * 1-5  盘前简报生成 (CodingPlan)

# ═══════════════════════════════════════════════════════
# 非交易日
# ═══════════════════════════════════════════════════════
0 10 * * 6   周度深度复盘 (CodingPlan)
0 4 * * *    因子计算
*/60 * * * 0 最低频数据采集
```

---

## 六、时间段策略设计

### 6.1 时段划分（北京时间）

| 时段 | 冬令时 | 夏令时 | 策略模式 | LLM 活动 | 交易权限 |
|------|--------|--------|---------|---------|---------|
| **深度夜盘** | 05:00-15:00 | 06:00-15:00 | 🟢 数据维护 | 无 | 禁止 |
| **盘前准备** | 15:00-21:30 | 15:00-22:30 | 🟡 选股+简报 | CodingPlan: 简报/选股 | 禁止 |
| **盘前交易** | 21:30-22:30 | 22:30-23:30 | 🟠 持仓调整 | CodingPlan: 持仓监控 | 仅持仓股调整，≤2笔 |
| **盘中** | 22:30-04:00 | 23:30-05:00 | 🔴 全功能 | 双端点全活跃 | 全功能交易，≤5笔 |
| **盘后** | 04:00-05:00 | 05:00-06:00 | 🟠 保护模式 | CodingPlan: 仅止损判断 | 仅止损/止盈，≤2笔 |
| **休市** | 全天 | 全天 | 🟢 复盘分析 | CodingPlan: 复盘 | 禁止 |

### 6.2 各时段策略配置

```python
SESSION_STRATEGY = {
    "deep_night": {
        "screener": False, "trading_agents": False, "holding_monitor": False,
        "data_collection": "minimal", "trading": False,
    },
    "pre_market_prep": {
        "screener": True, "trading_agents": False, "holding_monitor": True,
        "data_collection": "full", "trading": False,
        "deliverable": "盘前简报",
    },
    "pre_market_trade": {
        "screener": False, "trading_agents": False, "holding_monitor": True,
        "data_collection": "price_only", "trading": "holdings_only",
        "max_trades": 2,
    },
    "market_open": {
        "screener": False, "trading_agents": True, "holding_monitor": True,
        "data_collection": "realtime", "trading": "full",
        "max_trades": 5,
    },
    "after_hours": {
        "screener": False, "trading_agents": False, "holding_monitor": True,
        "data_collection": "price_only", "trading": "protective_only",
        "max_trades": 2,
    },
    "holiday": {
        "screener": False, "trading_agents": False, "holding_monitor": False,
        "data_collection": "minimal", "trading": False,
        "deliverable": "周度复盘",
    },
}
```

### 6.3 节假日日历

新增美国市场节假日判断（新年、MLK、总统日、复活节、阵亡将士纪念日、六月节、独立日、劳工日、感恩节、圣诞节），节假日自动切换到 `holiday` 模式。

---

## 七、模块耦合与协作优化

### 7.1 当前孤立/弱连接问题

| 模块对 | 问题 | 影响 |
|--------|------|------|
| Screener → TradingAgents | SignalHub 串行调用 TA，阻塞整个信号收集 | 总耗时 25min |
| Event Bus → 所有 Worker | Event Bus 无消费者 | 事件流断裂 |
| Watcher → Screener | Watcher 绕过 Event Bus 直调 Screener | 调度不统一 |
| 策略验证 → 交易决策 | validate_strategy 结果无反馈 | 无法自优化 |
| 复盘分析 → 选股 | 复盘结果不调整选股参数 | 闭环断裂 |

### 7.2 Event-driven 闭环实现

```
                    ┌──────────────────────────────────────┐
                    │          CRON 定时触发                 │
                    └──────────────┬───────────────────────┘
                                   │
                    ┌──────────────▼───────────────────────┐
                    │          Event Bus (调度中枢)          │
                    └──┬──────────┬──────────┬─────────────┘
                       │          │          │
            ┌──────────▼──┐  ┌───▼────┐  ┌──▼──────────┐
            │ 采集 Worker  │  │分析Worker│  │ 执行 Worker  │
            └──────┬──────┘  └───┬────┘  └────┬────────┘
                   │             │             │
            COLLECTION_    ANALYSIS_     EXECUTION_
            COMPLETE       COMPLETE      COMPLETE
                   │             │             │
                   └──────┬──────┴──────┬──────┘
                          │             │
                   ┌──────▼─────────────▼──────┐
                   │     DAILY_REVIEW          │
                   │     (复盘 + 策略优化)      │
                   └──────────────┬────────────┘
                                  │
                          反馈到 SCREENER 参数
                          更新 HOT_INDUSTRIES
                          调整动态阈值
```

### 7.3 关键改动

1. **实现 3 个 Worker 进程**，分别消费 Event Bus 的 `COLLECTION_TRIGGER` → `ANALYSIS_TRIGGER` → `EXECUTION_TRIGGER`
2. **链式调用**：采集完成发布 `COLLECTION_COMPLETE` → 触发分析 Worker → 分析完成发布 `ANALYSIS_COMPLETE` → 触发执行 Worker
3. **复盘反馈**：`DAILY_REVIEW` 结果写回 `screener_config.json`，动态调整 `HOT_INDUSTRIES` 和选股权重，实现策略自优化

---

## 八、顶层架构验证

### 8.1 当前流程 vs 理想流程

```
当前（有断裂）:
  选股 → 决策分析 → 交易执行 → 记录
   ↑                                    │
   └────── 复盘（手动）← 交易记录 ←─────┘

理想（自动闭环）:
  选股 → 决策分析 → 交易执行 → 记录 → 复盘 → 策略优化 → 选股
   ↑                                                          │
   └──────────────────── 反馈回路 ─────────────────────────────┘
```

### 8.2 关键缺失

| 缺失项 | 现状 | 优化后 |
|--------|------|--------|
| 策略反馈 | validate_strategy 结果未写入配置 | 写入 `screener_config.json` |
| 自学习机制 | TA memory 未与主系统联动 | memory 结果 → 选股权重调整 |
| 性能追踪 | 无统一胜率/盈亏比/回撤面板 | 新增 performance dashboard |

---

## 九、实施路线图

### 9.1 总览

```
Phase 1 (P0) — 止血：解决交易混乱
  ├── 双端点 LLM Router (2h)
  ├── .env 环境变量配置 (10min)
  ├── 交易执行防重复 + 事务 (2h)
  ├── 风控仲裁器 (2h)
  └── 时段策略模块 (3h)

Phase 2 (P1) — 提速：并行化 + 拆分
  ├── auto_execute.py 并行改造 (3h)
  ├── holding_monitor.py 改造 (30min)
  ├── tradingagents/main.py 并行化 (2h)
  ├── sentiment.py 改造 (30min)
  ├── auto_execute 拆分为模块 (4h)
  └── Event Bus 消费者 (4h)

Phase 3 (P2) — 闭环：自优化
  ├── 数据采集并行化 (2h)
  ├── 策略反馈闭环 (4h)
  └── 节假日日历 (1h)
```

### 9.2 详细优先级

| 阶段 | 优先级 | 优化项 | 工作量 | 产出文件 |
|------|--------|--------|--------|---------|
| **Phase 1** | P0 | 双端点 LLM Router | 2h | `analysis/llm_router.py` |
| | P0 | `.env` 环境变量 | 10min | `.env` |
| | P0 | 交易执行防重复 + 事务 | 2h | `scripts/auto_execute.py` 改造 |
| | P0 | 风控仲裁器 | 2h | `analysis/risk_arbitrator.py` |
| | P0 | 时段策略模块 | 3h | `analysis/session_strategy.py` |
| **Phase 2** | P1 | auto_execute.py 并行化 | 3h | `scripts/auto_execute.py` 改造 |
| | P1 | holding_monitor.py | 30min | `monitoring/holding_monitor.py` |
| | P1 | TA 并行化 | 2h | `tradingagents/main.py` |
| | P1 | sentiment.py | 30min | `processors/sentiment.py` |
| | P1 | 拆分为独立模块 | 4h | `workers/` 目录 |
| | P1 | Event Bus 消费者 | 4h | `workers/` + `event_bus.py` |
| **Phase 3** | P2 | 数据采集并行化 | 2h | `collectors/` 改造 |
| | P2 | 策略反馈闭环 | 4h | `analysis/feedback_loop.py` |
| | P2 | 节假日日历 | 1h | `analysis/holiday_calendar.py` |

**总工作量：约 33 小时**

---

## 十、配置速查表

### 10.1 环境变量

```bash
# CodingPlan (主力决策)
CODING_PLAN_KEY=sk-sp-2ff8d56f399d49c3b83f9db670627f46
CODING_PLAN_URL=https://coding.dashscope.aliyuncs.com/v1
CODING_PLAN_MODEL=qwen3.6-plus

# 百炼 (辅助分流)
BAILIAN_KEY=sk-64b866f9e6fa4e3588e7326bcc487d6d
BAILIAN_URL=https://dashscope.aliyuncs.com/compatible-mode/v1

# 兼容旧配置
OPENAI_API_KEY=sk-sp-2ff8d56f399d49c3b83f9db670627f46
DASHSCOPE_API_KEY=sk-64b866f9e6fa4e3588e7326bcc487d6d
```

### 10.2 Crontab 模板

```bash
# 盘前
*/30 15-21 * * 1-5  cd /path && timeout 60 python -m collectors.longbridge --data-type price >> logs/collect.log 2>&1
0 16,18,20 * * 1-5  cd /path && timeout 120 python -m scripts.auto_execute --mode screener >> logs/screener.log 2>&1

# 盘中（冬令时 21:30-04:00）
*/5  21-03 * * 1-5  cd /path && timeout 60 python -m collectors.longbridge --data-type price,news >> logs/collect.log 2>&1
*/15 21-03 * * 1-5  cd /path && timeout 180 python -m scripts.watcher >> logs/watcher.log 2>&1
*/30 21-03 * * 1-5  cd /path && timeout 300 python -m scripts.auto_execute --mode full-loop >> logs/auto_exec.log 2>&1
0 22,0,2 * * 1-5   cd /path && timeout 120 python -m monitoring.holding_monitor >> logs/holding.log 2>&1

# 盘后
0 5 * * 1-5  cd /path && timeout 180 python -m scripts.auto_execute --mode review >> logs/review.log 2>&1
0 6 * * 1-5  cd /path && timeout 120 python -m scripts.auto_execute --mode morning-brief >> logs/brief.log 2>&1

# 周末
0 10 * * 6   cd /path && timeout 300 python -m scripts.validate_strategy >> logs/validate.log 2>&1
0 4 * * *    cd /path && timeout 120 python scripts/calculate_factors.py >> logs/factors.log 2>&1
```

### 10.3 关键参数

| 参数 | 值 | 位置 |
|------|-----|------|
| TradingAgents max_debate_rounds | 2 | `tradingagents/main.py` |
| 订单冷却时间 | 10 分钟 | `auto_execute.py` |
| 最大日亏损 | 动态（账户 2%） | `circuit_breaker.py` |
| 动态阈值 LLM | CodingPlan qwen3.6-plus | `analysis/dynamic_threshold.py` |
| 情感分析 LLM | 百炼 qwen3.6-flash | `processors/sentiment.py` |
| SEC 解析 LLM | 百炼 qwen3-coder-plus | `dataflows/sec_financials.py` |

---

## 附录：成本估算

| 调用项 | 端点 | 模型 | 单次 token | 次数/loop | 单次成本 |
|--------|------|------|-----------|----------|---------|
| TradingAgents × 5 | CodingPlan | qwen3.6-plus | ~15K | 5 | 按 CodingPlan 计费 |
| 持仓监控 | CodingPlan | qwen3.6-plus | ~8K | 1 | 按 CodingPlan 计费 |
| 动态阈值 × 5 | CodingPlan | qwen3.6-plus | ~3K | 5 | 按 CodingPlan 计费 |
| 情感分析 × 10 | 百炼 | qwen3.6-flash | ~2K | 10 | ¥0.14 |
| SEC 解析 × 2 | 百炼 | qwen3-coder-plus | ~10K | 2 | TBD |
| 异常检测 × 5 | 百炼 | qwen-turbo | ~1K | 5 | 极低 |

> 核心决策走 CodingPlan（费用视平台定价），辅助任务走百炼 flash/turbo 大幅降本。
> 优化 Full Loop 频率到每 30min 后，百炼侧辅助任务月成本可控制在 ¥100-200。
