# 美股自动交易系统 — Agent Loop 架构图 v3.5

> 更新时间: 2026-04-20 16:57
> 更新内容: v3.5 P1 代码质量加固 + 滚动回撤 + 实时告警 + 小盘股限价单 + 20 个单元测试
> 覆盖: JVS大脑 / 订单监控 / 价格采集 / 系统管理器 / 持仓去重 / Pending去重 / Screener-to-Trade 全链路 / 独立复盘晨报

---

## 〇、系统全局总览（v3.3 优化后）

```
                    ┌─────────────────────────────────────────────┐
                    │         JVS 系统大脑 (Orchestrator)          │
                    │  ┌─────────┐  ┌──────────┐  ┌────────────┐  │
                    │  │Monitor  │→ │ LLM决策  │→ │ Executor   │  │
                    │  │(30s轮询) │  │ (5min)   │  │ 启动/停止   │  │
                    │  └─────────┘  └──────────┘  └────────────┘  │
                    │        ↑                          ↓          │
                    │  ┌──────────────────────────────────────┐    │
                    │  │      Brain Memory (持久化)            │    │
                    │  │  temp/brain_state.json                │    │
                    │  └──────────────────────────────────────┘    │
                    └──────────────┬───────────────────────────────┘
                                   │ 智能调度所有模块 (Crontab 已退场)
  ┌─────────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
  │价格采集  │ │新闻  │ │选股  │ │全循环│ │持仓  │ │订单  │
  │ daemon  │ │监控  │ │系统  │ │交易  │ │监控  │ │daemon│
  └─────────┘ └──────┘ └──────┘ └──────┘ └──────┘ └──────┘

┌────────────────────────────────────────────────────────────────────┐
│                        交易执行链路                                  │
│                                                                    │
│  ┌──────────┐   ┌──────────┐   ┌──────────────┐   ┌─────────────┐ │
│  │ 数据采集  │──→│ 三层选股  │──→│ 多智能体分析  │──→│ 信号聚合中心 │ │
│  │ 并行采集  │   │ Screener │   │ TradingAgents│   │ SignalHub   │ │
│  └────┬─────┘   └────┬─────┘   └──────┬───────┘   └──────┬──────┘ │
│       │              │                │                   │        │
│       │       ┌──────▼────────────────▼───────────────┐    │        │
│       │       │     持仓去重 + Pending订单去重          │    │        │
│       │       │     (Fix #12: 防止重复下单)             │    │        │
│       │       └──────────────────┬────────────────────┘    │        │
│       │              │           │                         │        │
│       ▼              ▼           ▼                         ▼        │
│  ┌─────────┐   ┌──────────┐   ┌──────────────┐   ┌─────────────┐  │
│  │ 订单监控 │   │ 持仓监控  │   │ 统一风控仲裁  │   │ 交易执行    │  │
│  │ Pending  │   │ LLM止盈  │   │ P0→P1→P2→P3→P4│   │ 订单冷却锁  │  │
│  └────┬────┘   └────┬─────┘   └──────┬───────┘   └──────┬──────┘  │
│       │             │                │                   │         │
│       └─────────────┴────────────────┴───────────────────┘         │
│                             │                                      │
│              ┌──────────────▼──────────────┐                       │
│              │    复盘 + 策略反馈闭环        │                       │
│              │  → screener_config.json      │                       │
│              │  → 选股权重自优化              │                       │
│              └─────────────────────────────┘                       │
│                                                                    │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │           支撑模块                                           │   │
│  │  时段策略 / 节假日日历 / 熔断器 / Event Bus / 日志系统     │   │
│  └────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
```

---

## 一、核心模块清单（v3.3）

### 1.1 🆕 v3.3 新增模块

| 模块 | 文件 | 状态 | 说明 |
|------|------|------|-----------|
| JVS 系统大脑 | `monitoring/orchestrator.py` | 🟢 新增 | LLM 驱动的智能调度守护进程 |
| 订单监控 | `monitoring/order_monitor.py` | 🟢 新增 | Pending 订单监控/超时/跳空/部分成交 |
| 系统管理器 | `scripts/system_manager.py` | 🟢 新增 | 一键启动/停止/状态检查 |
| 因子计算入口 | `scripts/calculate_factors.py` | 🟢 新增 | 修复缺失的因子计算脚本 |
| TradeSignal 数据结构 | `analysis/signal_schema.py` | 🟢 新增 | 统一信号数据结构 |

### 1.2 🆕 v3.5 P1 加固

| 模块 | 路径 | 状态 | 新增内容 |
|------|------|------|----------|
| 告警通道 | `alerts/notifier.py` | 🆕 | Telegram + Webhook 双通道推送 |
| 限价单 | `executors/auto_trade.py` | 🆕 | 自动 LO/MO 切换 + 大盘股白名单 |
| 滚动回撤 | `analysis/circuit_breaker.py` | 🆕 | 5日/20日/总回撤三维检查 |
| 测试 | `tests/test_risk_controls.py` | 🆕 | 20 个单元测试 |

### 1.3 v3.4 修复

| 模块 | 文件 | 状态 | 说明 |
|------|------|------|-----------|
| 价格采集守护进程 | `scripts/price_collector_daemon.py` | 🟢 新增 | 每 5 分钟采集，进程常驻，修复秒退问题 |
| 订单监控守护进程 | `scripts/order_monitor_daemon.py` | 🟢 新增 | 每 30 分钟检查，进程常驻，修复秒退问题 |
| Crontab 退场 | — | ⚪ 已废弃 | 所有调度统一归 JVS 大脑管理 |
| Longbridge 订单命令 | `executors/longbridge.py` | 🟢 修复 | `orders` → `order --format json` |

### 1.4 v3.1 审计优化

| 模块 | 文件 | 状态 | 改动 |
|------|------|------|-----------|
| 自动交易 | `scripts/auto_execute.py` | 🟢 审计优化 | 竞态修复+Kill Switch DB+启动对账+幂等性+Pending去重 |
| 持仓监控 | `monitoring/holding_monitor.py` | 🟢 v3.5 加固 | LLM动态止盈+三步JSON解析+schema校验 |
| 成本计算 | `management/position_manager.py` | 🟢 审计优化 | 券商摊薄成本法 |
| 公司行动 | `analysis/corporate_actions.py` | 🟢 新增 | 拆股/分红处理 |
| Prompt 防护 | `analysis/prompt_guard.py` | 🟢 新增 | 注入检测+LLM输出校验 |
| Longbridge 执行 | `executors/longbridge.py` | 🟢 审计优化 | 指数退避重试 |
| 存储层 | `storage/__init__.py` | 🟢 审计优化 | system_config 表 |
| LLM 多模型路由 | `analysis/llm_router.py` | 🔴 全面升级 | 8 模型 + 20 任务类型 |
| 统一风控仲裁 | `analysis/risk_arbitrator.py` | 🟢 新增 | P0: 风控仲裁器 |
| 交易时段策略 | `analysis/session_strategy.py` | 🟡 修复 | Bug 修复: 语法损坏/夏令时/缺失函数 |
| 节假日日历 | `analysis/holiday_calendar.py` | 🟢 新增 | P3: 完整节假日 |
| 策略反馈闭环 | `analysis/feedback_loop.py` | 🟢 新增 | P3: 自优化 |
| 并行采集 | `collectors/parallel_collector.py` | 🟢 新增 | P3: 并行采集 |

---

## 二、JVS 系统大脑（v3.3 核心新增）

### 2.1 架构

```
┌─────────────────────────────────────────────────┐
│           JVS Orchestrator Brain                 │
│  每30秒 tick → 每5分钟 LLM 决策周期              │
│                                                   │
│  ┌─────────┐  ┌──────────┐  ┌────────────┐      │
│  │ Monitor │→ │ LLM决策  │→ │ Executor   │      │
│  │ 30s轮询  │  │ (5min)   │  │ 启动/停止   │      │
│  └─────────┘  └──────────┘  └────────────┘      │
│        ↑                           ↓              │
│  ┌─────────────────────────────────────┐         │
│  │      Brain Memory (持久化记忆)        │         │
│  │  temp/brain_state.json               │         │
│  │  运行历史 + 决策日志 + 自愈记录        │         │
│  └─────────────────────────────────────┘         │
└─────────────────────────────────────────────────┘
```

### 2.2 核心能力

| 能力 | 实现方式 |
|------|---------|
| **实时监控** | 每 30 秒轮询所有模块状态（运行/失败/空闲） |
| **智能调度** | LLM 根据市场时段+系统状态决定何时启动/停止/跳过 |
| **自愈** | 模块连续失败→自动重启→超过阈值→标记 disabled 并告警 |
| **跨模块协调** | 模块间依赖检查，自动串联 |
| **上下文记忆** | `brain_state.json` 持久化所有运行历史和决策 |

### 2.3 管理的模块

| 模块 | ID | 调度模式 | 说明 |
|------|----|----------|------|
| 价格采集 | price_collector | **continuous (daemon)** | 每 5 分钟采集，进程常驻 |
| 订单监控 | order_monitor | **continuous (daemon)** | 每 30 分钟检查，进程常驻 |
| 新闻监控 | watcher | */15 分钟 | 定时任务 |
| 选股→交易 | screener | 每小时整点 | 定时任务 |
| 全循环交易 | full_loop | */30 分钟 | 定时任务 |
| 持仓监控 | holding_monitor | 每小时整点 | 定时任务 |
| 盘后复盘 | review | 05:00 | 定时任务 |
| 盘前晨报 | morning_brief | 06:00 | 定时任务 |
| 因子计算 | factors | 04:00 | 定时任务 |

### 2.4 LLM 决策

JVS 大脑使用 CodingPlan (qwen3.6-plus) 作为决策引擎，每 5 分钟执行一次决策周期：

```json
{
  "decisions": [
    {"module": "screener", "action": "start", "reason": "整点到达，选股调度时间"},
    {"module": "order_monitor", "action": "restart", "reason": "连续2次失败，尝试重启"}
  ],
  "alerts": [
    {"level": "critical", "module": "full_loop", "message": "连续3次失败，已停止"}
  ],
  "summary": "夜盘模式，保持基础监控"
}
```

**降级策略**：LLM 不可用时自动切换规则引擎（交易时段+关键模块失败→自动重启）。

### 2.5 使用方式

```bash
# 启动大脑（常驻守护进程）
python3 monitoring/orchestrator.py

# 查看大脑状态
python3 monitoring/orchestrator.py --status

# 或使用系统管理器
python3 scripts/system_manager.py start
python3 scripts/system_manager.py status
```

---

## 三、订单监控（v3.3 新增）

### 3.1 监控场景

| 场景 | 处理方式 |
|------|---------|
| 已成交 | 同步到 trades 表 |
| 部分成交 (>90%) | 同步已成交 + 取消剩余 |
| 部分成交 (<90%) | 同步 + TradingAgents 重新评估 |
| 盘外市价单 | 自动取消（不隔夜） |
| 价格跳空 (>3%) | 取消原单 → TradingAgents 重评 → 新信号注入 SignalHub |
| 超时未成交 (15min) | 检查原始信号 → 有效则保留，无效则取消 |

### 3.2 重新评估对接点

| 场景 | 对接模块 | 函数 |
|------|---------|------|
| 价格跳空 | TradingAgents | `run_trading_analysis()` |
| 信号有效性 | SignalHub | `collect_all()` |
| 风控检查 | auto_execute.py | `check_risk_rules()` |
| 持仓更新 | PositionManager | `sync_from_broker()` |

### 3.3 Pending 订单去重（Fix #12）

```python
# 下单前检查 broker 端是否有 pending 订单
pending = _get_symbols_with_pending_orders()  # NotReported/Submitted/Queued
if symbol in pending:
    skip()  # 防止盘外重复下单
```

**启动时自动清理**：每次 `execute_signals()` 或 `execute_alerts()` 启动时，扫描 broker 端所有 NotReported 市价单，自动取消陈旧挂单。

---

## 四、完整 Loop 流程图（v3.3 优化后）

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           CRON 定时触发 / JVS 大脑调度                                │
│  盘前: */30 15-21  |  盘中: */5 价格, */15 watcher, */30 auto_execute  |  盘后: 复盘  │
│  JVS: 每30秒tick, 每5min LLM决策周期                                                  │
└───────────────────────────────┬─────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 0: 启动对账 (P0 审计修复)                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  1. sync_from_broker() — 从券商同步最新持仓                                    │
│  │  2. process_corporate_actions() — 处理拆股/分红等公司行动                      │
│  │  3. 检查 Kill Switch (环境变量 + DB system_config 表)                          │
│  │     → 若激活则拒绝启动                                                         │
│  │  4. Fix #12: 扫描并取消 broker 端陈旧 NotReported 市价单                       │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────┬─────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 1: 时段策略判断 (session_strategy.py)                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  当前时段: deep_night / pre_market_prep / pre_market_trade /                 │   │
│  │            market_open / after_hours / holiday / weekend                     │   │
│  │  夏令时/冬令时自动识别                                                        │   │
│  │  美国节假日自动判断 (holiday_calendar.py)                                     │   │
│  │                                                                              │   │
│  │  输出: {trading_allowed, max_trades, trading_mode, data_collection, llm}    │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│  判断结果:                                                                         │
│    ├─ 禁止交易 → 跳到 STEP 7（数据维护/复盘）                                        │
│    └─ 允许交易 → 继续 STEP 2                                                        │
└───────────────────────────────┬─────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 2: 数据采集 (Phase 3: 并行采集)                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  collectors/parallel_collector.py                                            │   │
│  │  ThreadPoolExecutor(max_workers=5) 并行采集                                   │   │
│  │  7只 × 5源 = 35次 → 3-5min (原 17.5min)                                      │   │
│  │                                                                              │   │
│  │  ┌──────────┐ ┌───────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐         │   │
│  │  │ Price    │ │ SEC       │ │ GNews    │ │ LongB    │ │ Reddit   │         │   │
│  │  │ (串行)   │ │ (并行)    │ │ (并行)   │ │ (并行)   │ │ (并行)   │         │   │
│  │  └──────────┘ └───────────┘ └──────────┘ └──────────┘ └──────────┘         │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│  输出: 最新价格、新闻、财报、资金流向、Reddit 帖子                                    │
└───────────────────────────────┬─────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 3: 情感分析 (百炼 qwen3.6-flash)                                                │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  LLM Router → "sentiment_analysis" → 百炼 qwen3.6-flash                     │   │
│  │  Batch 模式: 15条/次，降低 API 调用量                                          │   │
│  │  Fallback: 规则打分 (关键词匹配)                                               │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│  输出: 每只股票的情感分数 (-1.0 ~ +1.0)                                             │
└───────────────────────────────┬─────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 4: 三层选股 (screener.py)                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  L1: 热度筛选 (volume > 1M, market_cap > $1B, 行业过滤)                       │   │
│  │  L2: 行业轮动 (HOT_INDUSTRIES 动态更新)                                        │   │
│  │  L3: 成长+动量 (revenue_growth > 20%, earnings_growth > 25%)                  │   │
│  │                                                                              │   │
│  │  Phase 3: screener_config.json 由 feedback_loop 动态调整                      │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│  输出: Top 5 候选股 + 候选原因                                                       │
└───────────────────────────────┬─────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 5: 多智能体分析 (CodingPlan 并行)                                                │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  TradingAgents (tradingagents/main.py)                                        │   │
│  │  max_debate_rounds = 2                                                        │   │
│  │                                                                              │   │
│  │  5 只候选股并行分析                                                            │   │
│  │  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐             │   │
│  │  │ AAPL × CodingPlan│ │ NVDA × CodingPlan│ │ MSFT × CodingPlan│              │   │
│  │  │ qwen3.6-plus (并发)│ │ qwen3.6-plus (并发)│ │ qwen3.6-plus (并发)│          │   │
│  │  └──────────────────┘ └──────────────────┘ └──────────────────┘             │   │
│  │                                                                              │   │
│  │  每个分析流程:                                                                 │   │
│  │  ┌─────────┐  ┌──────┐  ┌──────────┐  ┌──────────┐  ┌──────┐               │   │
│  │  │ 基本面   │→│ 牛熊  │→│ 投资法官  │→│ 风控辩论  │→│ 组合  │               │   │
│  │  │ 研究    │  │ 辩论  │  │          │  │          │  │ 管理  │               │   │
│  │  │         │  │(2轮) │  │          │  │          │  │      │               │   │
│  │  └─────────┘  └──────┘  └──────────┘  └──────────┘  └──────┘               │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│  输出: 每只候选股的交易信号 (direction, confidence, reasoning)                       │
└───────────────────────────────┬─────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 6: 信号聚合与动态阈值                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  SignalHub 聚合所有信号 → 去重 + 冲突解决                                     │   │
│  │  动态阈值 → LLM Router → CodingPlan qwen3.6-plus                             │   │
│  │  Fallback: 规则版阈值计算                                                      │   │
│  │                                                                              │   │
│  │  ┌─────────────────────────────────────────────────────────────────────┐   │   │
│  │  │  统一风控仲裁 (risk_arbitrator.py)                                   │   │   │
│  │  │  P0: 熔断 (Circuit Breaker) → halt all                               │   │   │
│  │  │  P1: 订单冷却 (Order Cooldown) → 防重复下单                           │   │   │
│  │  │  P2: 信号 cooldown → 信号去重                                         │   │   │
│  │  │  P3: 动态阈值 → 信号质量过滤                                          │   │   │
│  │  │  P4: Risk Manager → 综合风控检查                                      │   │   │
│  │  └─────────────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│  输出: 最终交易决策列表 (通过/拒绝 + 原因)                                           │
└───────────────────────────────┬─────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 7: 交易执行 (v3.3: Pending去重 + 启动清理)                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  根据时段策略限制交易数量:                                                     │   │
│  │  ├─ market_open: max 5 笔 (全功能)                                            │   │
│  │  ├─ pre_market_trade: max 2 笔 (仅持仓调整)                                   │   │
│  │  └─ after_hours: max 2 笔 (仅止损/止盈)                                       │   │
│  │                                                                              │   │
│  │  执行流程:                                                                     │   │
│  │  1. 数据完整性检查 (价格有效)                                                   │   │
│  │  2. 幂等性检查: signal_id 是否已执行                                           │   │
│  │  3. Fix #12: Pending 订单去重 — 检查 broker 端是否有 pending 订单              │   │
│  │  4. 原子化竞态修复: _try_acquire_order_lock                                   │   │
│  │  5. Kill Switch 检查: DB system_config 表                                     │   │
│  │  6. Dry Run 检查 (可选)                                                        │   │
│  │  7. 下单 → Longbridge API (指数退避重试 3 次)                                  │   │
│  │  8. 交易记录 (trades 表，含 signal_id)                                         │   │
│  │  9. 订单冷却锁 (10min)                                                         │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│  输出: 执行结果 + 交易记录                                                           │
└───────────────────────────────┬─────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  STEP 8: 复盘与反馈闭环 (Phase 3: 策略自优化)                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  validate_strategy.py → feedback_loop.py → screener_config.json             │   │
│  │                                                                              │   │
│  │  自优化规则:                                                                  │   │
│  │  ├─ 胜率 < 45% → 提高最低置信度                                                │   │
│  │  ├─ 胜率 > 65% → 放宽最低置信度                                                │   │
│  │  ├─ Sharpe > 1.5 → 增加 momentum 权重                                         │   │
│  │  ├─ 最大回撤 > 15% → 增加 quality 权重                                        │   │
│  │  └─ 新最佳标的 → 记录到 best_performers                                       │   │
│  │                                                                              │   │
│  │  反馈历史: dayup/performance/feedback_history.jsonl                           │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│  输出: 更新后的选股配置 (screener_config.json)                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 五、LLM 多模型并行分流

### 5.1 核心规则

> **⚠️ CodingPlan 优先：所有 CodingPlan 支持的任务（qwen3.6-plus）优先走 CodingPlan 端点。**
> **仅当 CodingPlan 不支持该模型时（deepseek-r1 / qwq-plus / coder / turbo / max），才走百炼端点。**

### 5.2 端点配置

| 端点 | 模型 | 定位 |
|------|------|------|
| CodingPlan | qwen3.6-plus | 核心交易决策（主力） |
| 百炼 | qwen3-max / deepseek-r1 / qwq-plus | 深度推理 + 复杂决策 |
| 百炼 | qwen3-coder-plus / qwen3-coder-flash | SEC 解析 + 代码生成 |
| 百炼 | qwen3.6-flash | 情感分析（快速轻量） |
| 百炼 | qwen-turbo | 市场监控（最便宜） |

### 5.3 降级链路

```
CodingPlan 失败 → 百炼同模型 → qwen3.6-flash（末端兜底）
```

### 5.4 任务路由表

**CodingPlan 任务（15 个，qwen3.6-plus 全部走 CodingPlan）：**

| 任务类型 | 端点 | 模型 | 超时 |
|---------|------|------|------|
| trading_agents_debate | CodingPlan | qwen3.6-plus | 120s |
| trading_agents_judge | CodingPlan | qwen3.6-plus | 120s |
| trading_agents_risk | CodingPlan | qwen3.6-plus | 90s |
| dynamic_threshold | CodingPlan | qwen3.6-plus | 90s |
| holding_monitor | CodingPlan | qwen3.6-plus | 90s |
| weekly_review | CodingPlan | qwen3.6-plus | 180s |
| morning_brief | CodingPlan | qwen3.6-plus | 120s |
| portfolio_manager | CodingPlan | qwen3.6-plus | 90s |
| screener_industry | CodingPlan | qwen3.6-plus | 90s |
| earnings_analysis | CodingPlan | qwen3.6-plus | 90s |
| risk_assessment | CodingPlan | qwen3.6-plus | 90s |
| report_generation | CodingPlan | qwen3.6-plus | 120s |
| strategy_validate | CodingPlan | qwen3.6-plus | 90s |
| text_summarize | CodingPlan | qwen3.6-plus | 90s |
| news_classification | CodingPlan | qwen3.6-plus | 60s |

---

## 六、交易执行模块

### 6.1 统一风控仲裁

```
P0 熔断 (Circuit Breaker)    ← 最高优先，halt all
  ↓
P1 订单冷却 (Order Cooldown)  ← 防止重复下单 (10min)
  ↓
P2 信号 cooldown              ← 信号去重
  ↓
P3 动态阈值 (Dynamic Threshold) ← 信号质量过滤
  ↓
P4 Risk Manager              ← 综合风控检查
```

### 6.2 v3.3 Pending 订单去重

```python
# 下单前检查 broker 端是否有 pending 订单
pending = _get_symbols_with_pending_orders()  # NotReported/Submitted/Queued
if symbol in pending:
    skip()  # 防止盘外重复下单
```

### 6.3 启动时自动清理

```python
# 每次启动时扫描并取消陈旧 NotReported 市价单
orders = executor.get_orders()
for o in orders:
    if o.status == "NotReported" and o.order_type == "MO":
        executor.cancel_order(o.order_id)
```

### 6.4 持仓止盈/止损策略 (v3.2)

> **止盈/止损完全由 LLM 动态评估，不设硬性阈值。**
> **减仓订单详情交给 LLM 参考，不做任何先验假设。**

---

## 七、时段策略设计

| 时段 | 冬令时 | 夏令时 | 交易模式 | 最大交易 |
|------|--------|--------|---------|---------|
| 深度夜盘 | 05:00-15:00 | 06:00-15:00 | 禁止 | 0 |
| 盘前准备 | 15:00-21:30 | 15:00-22:30 | 禁止 | 0 |
| 盘前交易 | 21:30-22:30 | 22:30-23:30 | 仅持仓调整 | 2 |
| 盘中 | 22:30-04:00 | 23:30-05:00 | 全功能 | 5 |
| 盘后 | 04:00-05:00 | 05:00-06:00 | 仅止损/止盈 | 2 |
| 休市/节假日 | 全天 | 全天 | 禁止 | 0 |

---

## 八、数据采集并行化

```
改造前 (串行):
  for collector in collectors:
      for symbol in symbols:
          collector.collect(symbol)
  → 7只 × 5源 × 30s = 17.5min

改造后 (并行):
  ThreadPoolExecutor(max_workers=5):
      按数据源分组并行
  → 最慢数据源耗时 ≈ 3-5min
```

---

## 九、策略反馈闭环

```
validate_strategy.py → feedback_loop.py → screener_config.json → screener → 新信号
       ↓                                          ↑
   胜率/Sharpe/回撤 ─────────────────────────── 选股权重/阈值调整
```

---

## 十、Crontab 调度表（v3.4 已废弃）

> ⚠️ **v3.4 起 Crontab 已全面退场。** 所有调度统一归 JVS 大脑管理。
> 以下为历史参考，不再维护。

~~~bash
# 以下调度已不再使用，改为 JVS 大脑统一管理
# 盘前（15:00-21:30）
*/30 15-21 * * 1-5  collect (price only)
0 16,18,20 * * 1-5  auto_execute --mode screener-to-trade

# 盘中（21:30-04:00 夏令时）→ 高频 + 全功能
*/5  21-03 * * 1-5  collect (price + news)
*/15 21-03 * * 1-5  watcher
*/30 21-03 * * 1-5  auto_execute --mode full-loop
0 22,0,2 * * 1-5   auto_execute --mode holding-monitor

# 订单监控
0 21 * * 1-5        auto_execute --mode order-monitor
*/30 22-04 * * 1-5  auto_execute --mode order-monitor

# 盘后（04:00-08:00）→ 复盘 + 数据补全
0 5 * * 1-5         auto_execute --mode review
0 6 * * 1-5         auto_execute --mode morning-brief
0 4 * * *           calculate_factors + order_cleanup

# 非交易日
0 10 * * 6          validate_strategy
*/60 * * * 0        collect (最低频)
~~~

---

## 十一、关键性能指标

| 指标 | 优化前 | v3.3 |
|------|--------|------|
| 数据采集耗时 | ~17.5min | ~3-5min |
| TA 分析耗时 (5只) | ~25min | ~5min |
| 单次 Full Loop | ~45min | ~10-15min |
| 重复下单风险 | 高 | 消除（原子锁+幂等+pending去重） |
| 风控冲突 | 有 (无仲裁) | 无 (P0-P4 仲裁) |
| LLM 成本 | 全部 qwen3.6-plus | 分级 (8 模型按优势分配) |
| 模块管理 | 手动/crontab | JVS 大脑全自动 |
| 订单监控 | 无 | 实时监控+自动自愈 |
| 信号链路 | 断裂 | 全链路打通 |

---

## 十二、审计优化清单

### P0 核心修复（资金安全）

| # | 问题 | 修复方案 | 状态 |
|---|------|----------|------|
| 1 | 竞态条件: `_check_order_cooldown` + `_lock_order` 非原子 | 合并为 `_try_acquire_order_lock` | ✅ |
| 2 | 幂等性缺失: signal_id 无唯一约束 | signal_id 预检查 + DB 约束 | ✅ |
| 3 | Kill Switch 需重启进程 | 新增 `system_config` 表，实时读取 | ✅ |
| 4 | 启动时未与券商对账 | 启动时先 `sync_from_broker()` | ✅ |

### P1 高优先级

| # | 问题 | 修复方案 | 状态 |
|---|------|----------|------|
| 5 | 拆股/分红无处理逻辑 | `analysis/corporate_actions.py` | ✅ |
| 6 | Prompt 注入风险 | `analysis/prompt_guard.py` | ✅ |
| 7 | 接口失败无重试 | Longbridge 指数退避重试 (3 次) | ✅ |
| 8 | 止盈/止损硬规则兜底 | 移除，完全由 LLM 动态评估 | ✅ |

### v3.2 持仓止盈策略优化

| # | 问题 | 修复方案 | 状态 |
|---|------|----------|------|
| 9 | 减仓判断依赖 P&L 猜测 | 改为查交易记录 `_has_sell_trades()` | ✅ |
| 10 | LLM 缺少减仓上下文 | `_get_sell_trades()` 提供卖出订单详情 | ✅ |
| 11 | 假设"减仓=利润锁定" | 移除假设，仅作为中性事实标注 | ✅ |
| 12 | 持仓成本价不准确 | 券商摊薄成本法，不自行计算 | ✅ |

### v3.3 JVS 系统大脑 + 全模块对接

| # | 问题 | 修复方案 | 状态 |
|---|------|----------|------|
| 13 | 模块各自为战，无统一协调 | JVS Orchestrator 大脑 | ✅ |
| 14 | 选股→交易链路断裂 | Screener-to-Trade 全链路打通 | ✅ |
| 15 | 重复下单（盘外市价单） | Pending 订单去重 + 启动清理 | ✅ |
| 16 | 模块失败无人处理 | 自动重启 + 连续失败告警 | ✅ |
| 17 | Review/Morning-Brief 错误执行交易 | 独立逻辑，不执行交易 | ✅ |
| 18 | 选股未去重持仓 | 持仓去重 + 顺延逻辑 | ✅ |
| 19 | 一键启动缺失 | System Manager 一键启动所有 | ✅ |
| 20 | 代码重复定义/解析脆弱/无回撤/无告警 | v3.5 P1 代码质量加固 | ✅ |

### v3.5 P1 代码质量加固

- **重复定义清理** — `auto_execute.py` 中 `_try_acquire_order_lock` 4→1、`_check_kill_switch` 2→1、`RISK_RULES` 2→1
- **LLM 输出解析** — 三步降级（markdown → 正则 → 纯 JSON）+ schema 校验
- **滚动回撤** — 新增 5日/$1500、20日/$3000、总回撤 10% 三维检查
- **告警通道** — `alerts/notifier.py` 支持 Telegram + Webhook
- **小盘股限价单** — 大盘股列表自动 MO，其余自动 LO
- **SQLite busy_timeout** — 30s 超时等待
- **单元测试** — 20 个测试覆盖 CircuitBreaker、订单锁、解析、告警

### v3.4 Crontab 退场 + 统一调度

| # | 问题 | 修复方案 | 状态 |
|---|------|----------|------|
| 20 | 三套调度打架（Crontab + continuous_run + Orchestrator） | Crontab 全面退场，大脑统一调度 | ✅ |
| 21 | 价格采集脚本秒退被误判为崩溃 | 新建守护进程 `price_collector_daemon.py` | ✅ |
| 22 | 订单监控脚本秒退被误判为崩溃 | 新建守护进程 `order_monitor_daemon.py` | ✅ |
| 23 | Longbridge 订单命令错误 | `orders` → `order --format json` | ✅ |

---

## 十三、环境变量配置

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

---

## 十四、Kill Switch 使用方式

```bash
# 方式1: 环境变量 (旧方式，需重启进程)
export TRADING_KILL_SWITCH=1

# 方式2: 数据库 (新方式，实时生效)
python3 -c "
import sqlite3
conn = sqlite3.connect('dayup/us_data_hub.db')
conn.execute(\"INSERT OR REPLACE INTO system_config (key, value) VALUES ('kill_switch', '1')\")
conn.commit()
conn.close()
print('Kill Switch 已激活 (实时生效)')
"

# 关闭 Kill Switch
python3 -c "
import sqlite3
conn = sqlite3.connect('dayup/us_data_hub.db')
conn.execute(\"DELETE FROM system_config WHERE key = 'kill_switch'\")
conn.commit()
conn.close()
print('Kill Switch 已关闭')
"
```
