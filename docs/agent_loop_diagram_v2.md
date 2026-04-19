# 美股自动交易系统 — Agent Loop 架构图 v3.2

> 更新时间: 2026-04-20 00:03
> 更新内容: v3.1 审计优化 + v3.2 持仓止盈策略优化
> 覆盖: 原子化订单锁 / Kill Switch DB / 启动对账 / 公司行动 / Prompt防护 / 减仓详情 / LLM动态止盈

---

## 〇、系统全局总览（优化后）

```
                    ┌─────────────────────────────────────────┐
                    │          顶层架构闭环（自动优化）          │
                    │                                         │
                    │  定期选股 → 综合决策 → 自动交易           │
                    │     ↑                              ↓     │
                    │     └──── 定期复盘 ← 反馈闭环 ← 记录     │
                    └─────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────┐
│                        系统架构全景图                               │
│                                                                    │
│  ┌──────────┐   ┌──────────┐   ┌──────────────┐   ┌─────────────┐ │
│  │ 数据采集  │──→│ 三层选股  │──→│ 多智能体分析  │──→│ 信号聚合中心 │ │
│  │ 并行采集  │   │ Screener │   │ TradingAgents│   │ SignalHub   │ │
│  └────┬─────┘   └────┬─────┘   └──────┬───────┘   └──────┬──────┘ │
│       │              │                │                   │        │
│       │              ▼                ▼                   │        │
│       │       ┌──────────────────────────────┐            │        │
│       │       │       LLM 双端点调度层        │            │        │
│       │       │  CodingPlan (主力) + 百炼     │            │        │
│       │       └──────────────────────────────┘            │        │
│       │              │                │                   │        │
│       ▼              ▼                ▼                   ▼        │
│  ┌─────────┐   ┌──────────┐   ┌──────────────┐   ┌─────────────┐  │
│  │ 动态阈值 │   │ 持仓监控  │   │ 统一风控仲裁  │   │ 交易执行    │  │
│  │ LLM路由  │   │ LLM路由  │   │ P0→P1→P2→P3→P4│   │ 订单冷却锁  │  │
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

## 一、核心模块清单（审计优化后 v3.1）

| 模块 | 文件 | 状态 | 改动 |
|------|------|------|-----------|
| 自动交易 | `scripts/auto_execute.py` | 🟢 审计优化 | 竞态修复+Kill Switch DB+启动对账+幂等性 |
| 持仓监控 | `monitoring/holding_monitor.py` | 🟢 v3.2 优化 | LLM动态止盈+减仓订单详情 |
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

## 二、完整 Loop 流程图（优化后）

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           CRON 定时触发                                               │
│  盘前: */30 15-21  |  盘中: */5 价格, */15 watcher, */30 auto_execute  |  盘后: 复盘  │
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
│  STEP 7: 交易执行 (审计优化 v3.1: 原子锁 + 幂等 + Kill Switch DB)                    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  根据时段策略限制交易数量:                                                     │   │
│  │  ├─ market_open: max 5 笔 (全功能)                                            │   │
│  │  ├─ pre_market_trade: max 2 笔 (仅持仓调整)                                   │   │
│  │  └─ after_hours: max 2 笔 (仅止损/止盈)                                       │   │
│  │                                                                              │   │
│  │  执行流程:                                                                     │   │
│  │  1. 数据完整性检查 (价格有效)                                                   │   │
│  │  2. 幂等性检查: signal_id 是否已执行 (P0 审计修复)                             │   │
│  │  3. 原子化竞态修复: _try_acquire_order_lock (P0 审计修复)                      │   │
│  │     → INSERT OR IGNORE + signal_id 防重复                                    │   │
│  │  4. Kill Switch 检查: DB system_config 表 (实时生效，无需重启)                  │   │
│  │  5. Dry Run 检查 (可选)                                                        │   │
│  │  6. 下单 → Longbridge API (指数退避重试 3 次)                                  │   │
│  │  7. 交易记录 (trades 表，含 signal_id)                                         │   │
│  │  8. 订单冷却锁 (10min)                                                         │   │
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

## 三、LLM 多模型并行分流（全面升级后）

### 3.1 核心规则

> **⚠️ CodingPlan 优先：所有 CodingPlan 支持的任务（qwen3.6-plus）优先走 CodingPlan 端点。**
> **仅当 CodingPlan 不支持该模型时（deepseek-r1 / qwq-plus / coder / turbo / max），才走百炼端点。**

### 3.2 端点配置

```
┌──────────────────┬──────────────────────────────┬───────────────────────────────┐
│                  │ CodingPlan (主力)             │ 百炼 (8 模型分流)             │
├──────────────────┼──────────────────────────────┼───────────────────────────────┤
│ Base URL         │ coding.dashscope.            │ dashscope.                    │
│                  │ aliyuncs.com/v1              │ aliyuncs.com/                 │
│                  │                              │ compatible-mode/v1            │
├──────────────────┼──────────────────────────────┼───────────────────────────────┤
│ 模型             │ qwen3.6-plus                 │ qwen3-max / deepseek-r1 /     │
│                  │                              │ qwq-plus / qwen3-coder-plus / │
│                  │                              │ qwen3-coder-flash / qwen3.6-  │
│                  │                              │ flash / qwen-turbo            │
├──────────────────┼──────────────────────────────┼───────────────────────────────┤
│ 定位             │ 核心交易决策                  │ 按模型优势分流                 │
├──────────────────┼──────────────────────────────┼───────────────────────────────┤
│ RPM 限制         │ 100                          │ 30-300 (按模型)               │
├──────────────────┼──────────────────────────────┼───────────────────────────────┤
│ 最大并发         │ 8                            │ 2-20 (按模型)                 │
└──────────────────┴──────────────────────────────┴───────────────────────────────┘
```

### 3.3 百炼模型能力画像

| 模型 | 角色 | 核心优势 | 适合场景 | Fallback |
|---|---|---|---|---|
| **qwen3.6-plus** | fallback-only | CodingPlan 主力模型同版本 | CodingPlan 失败兜底 | → qwen3.6-flash |
| **qwen3-max** | 百炼专用 | 最强推理，复杂决策 | critical_decision, final_judgment | → qwen3.6-flash |
| **deepseek-r1** | 百炼专用 | 思维链深度推理 | strategy_deep_analysis, complex_reasoning | → qwen3.6-flash |
| **qwq-plus** | 百炼专用 | 数学计算/逻辑推导 | threshold_calculation, backtest_analysis | → qwen3.6-flash |
| **qwen3-coder-plus** | 百炼专用 | 代码理解/结构化解析 | sec_parsing, code_generation | → qwen3.6-flash |
| **qwen3-coder-flash** | 百炼专用 | 代码生成轻量 | script_generation | → qwen3.6-flash |
| **qwen3.6-flash** | 百炼专用 | 快速轻量，成本低 | sentiment_analysis | 末端 |
| **qwen-turbo** | 百炼专用 | 最便宜最快 | market_watcher, anomaly_detection, health_check | → qwen3.6-flash |

### 3.4 完整任务路由表

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

**百炼端点任务（16 个，仅 CodingPlan 不支持的模型）：**

| 任务类型 | 端点 | 模型 | 超时 |
|---------|------|------|------|
| strategy_deep_analysis | 百炼 | deepseek-r1 | 180s |
| complex_reasoning | 百炼 | deepseek-r1 | 180s |
| threshold_calculation | 百炼 | qwq-plus | 120s |
| backtest_analysis | 百炼 | qwq-plus | 120s |
| math_validation | 百炼 | qwq-plus | 120s |
| sec_parsing | 百炼 | qwen3-coder-plus | 90s |
| code_generation | 百炼 | qwen3-coder-plus | 90s |
| script_generation | 百炼 | qwen3-coder-flash | 60s |
| sentiment_analysis | 百炼 | qwen3.6-flash | 30s |
| market_watcher | 百炼 | qwen-turbo | 10s |
| anomaly_detection | 百炼 | qwen-turbo | 10s |
| health_check | 百炼 | qwen-turbo | 10s |
| system_monitor | 百炼 | qwen-turbo | 10s |
| data_quality_check | 百炼 | qwen-turbo | 10s |
| critical_decision | 百炼 | qwen3-max | 120s |
| final_judgment | 百炼 | qwen3-max | 120s |

### 3.5 降级链路

```
CodingPlan (qwen3.6-plus) 失败
  ↓ fallback 1
百炼 qwen3.6-plus (同模型兜底)
  ↓ fallback 2
百炼 qwen3.6-flash (通用兜底)

百炼各专用模型失败
  ↓ fallback (各自的 fallback)
百炼 qwen3.6-flash (末端兜底)
```

### 3.6 限流与并发控制

```
LLMRouter 核心机制:
  ├─ 双端点独立限流 (RPM 跟踪 60s 窗口)
  ├─ Semaphore 并发控制 (每模型独立)
  ├─ CodingPlan 优先：所有 qwen3.6-plus 任务走 CodingPlan
  ├─ 跨端点降级 (CodingPlan → 百炼 qwen3.6-plus → qwen3.6-flash)
  ├─ 百炼降级 (deepseek-r1/qwq-plus/qwen3-max → qwen3.6-flash)
  └─ per_model 分模型统计 (记录每个模型的 calls/tokens/errors)
```

---

## 四、交易执行模块（Phase 1-2 改造后）

### 4.1 统一风控仲裁（risk_arbitrator.py）

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

### 4.2 订单冷却机制 (v3.1 原子化修复)

```python
# P0 修复: 原子化检查+锁定，解决竞态条件
if not _try_acquire_order_lock(db, symbol, direction, signal_id, minutes=10):
    skip()  # 已被执行或正在被其他进程处理

# 内部逻辑:
# 1. 幂等性检查: signal_id 是否已在 trades 表中
# 2. INSERT OR IGNORE 原子化插入 order_lock 记录
# 3. 验证锁定结果 (reason 匹配则成功)
```

### 4.3 Kill Switch DB 版 (v3.1 新增)

```python
# 同时检查环境变量 + DB system_config 表
if _check_kill_switch(db):
    return False, "KILL SWITCH ACTIVE — all trading halted"

# DB 版优势: 修改后立即生效，无需重启进程
```

### 4.4 持仓止盈/止损策略 (v3.2 优化)

> **止盈/止损完全由 LLM 动态评估，不设硬性阈值。**
> 
> **成本价使用券商摊薄成本法：** 减仓卖出后，剩余持仓成本被摊薄。
> 
> **减仓订单详情交给 LLM 参考：** 列出每只标的的历史卖出记录（时间/数量/价格/触发原因），
> 让 LLM 综合判断是止盈、止损还是调仓，不做任何先验假设。
> 
> LLM 综合考虑 P&L、趋势、波动率、市场情绪、集中度、减仓历史等因素，
> 自主决定 HOLD / REDUCE / ADD / STOP_LOSS / TAKE_PROFIT。

### 4.3 数据完整性检查

```python
if current_price is None or current_price <= 0:
    skip()  # 价格无效，不执行
```

---

## 五、时段策略设计（Phase 1）

### 5.1 6 时段 + 2 特殊模式

| 时段 | 冬令时 | 夏令时 | 交易模式 | 最大交易 |
|------|--------|--------|---------|---------|
| 深度夜盘 | 05:00-15:00 | 06:00-15:00 | 禁止 | 0 |
| 盘前准备 | 15:00-21:30 | 15:00-22:30 | 禁止 | 0 |
| 盘前交易 | 21:30-22:30 | 22:30-23:30 | 仅持仓调整 | 2 |
| 盘中 | 22:30-04:00 | 23:30-05:00 | 全功能 | 5 |
| 盘后 | 04:00-05:00 | 05:00-06:00 | 仅止损/止盈 | 2 |
| 休市 | 全天 | 全天 | 禁止 | 0 |

### 5.2 节假日日历

```
美国股市节假日 (2025-2028):
  固定: New Year, Juneteenth, Independence Day, Christmas
  动态: MLK Day, Presidents' Day, Good Friday, Memorial Day,
        Labor Day, Thanksgiving
  提前收盘: 7/3, Thanksgiving Eve, Christmas Eve
```

---

## 六、数据采集并行化（Phase 3）

### 6.1 改造前 vs 改造后

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

## 七、策略反馈闭环（Phase 3）

```
validate_strategy.py → feedback_loop.py → screener_config.json → screener → 新信号
       ↓                                          ↑
   胜率/Sharpe/回撤 ─────────────────────────── 选股权重/阈值调整

自优化规则:
  胜率 < 45%  → min_confidence +0.05
  胜率 > 65%  → min_confidence -0.05
  Sharpe > 1.5 → momentum weight +0.05
  回撤 > 15%  → quality weight +0.05
  新最佳标的  → best_performers 记录
```

---

## 八、Crontab 调度表（优化后）

```bash
# 盘前（15:00-21:30）→ 选股更新 + 简报
*/30 15-21 * * 1-5  collect (price only)
0 16,18,20 * * 1-5  screener + 选股简报

# 盘中（冬 21:30-04:00 / 夏 22:30-05:00）→ 高频 + 全功能
*/5  21-03 * * 1-5  collect (price + news)
*/15 21-03 * * 1-5  watcher (百炼 qwen-turbo)
*/30 21-03 * * 1-5  auto_execute (signal + risk)
0 22,0,2 * * 1-5   holding_monitor (CodingPlan qwen3.6-plus)

# 盘后（04:00-08:00）→ 复盘 + 数据补全
0 5 * * 1-5  复盘分析 + 策略验证
0 6 * * 1-5  盘前简报生成

# 非交易日
0 10 * * 6   周度深度复盘
0 4 * * *    因子计算
*/60 * * * 0 最低频数据采集
```

---

## 九、环境变量配置

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

## 十、关键性能指标（优化后 vs 优化前）

| 指标 | 优化前 | 优化后 | 提升 |
|------|--------|--------|------|
| 数据采集耗时 | ~17.5min | ~3-5min | **↓70%** |
| TA 分析耗时 (5只) | ~25min | ~5min | **↓80%** |
| 单次 Full Loop | ~45min | ~10-15min | **↓70%** |
| 重复下单风险 | 高 | 无 (原子锁+幂等) | **消除** |
| 风控冲突 | 有 (无仲裁) | 无 (P0-P4 仲裁) | **消除** |
| LLM 成本 | 全部 qwen3.6-plus | 分级 (8 模型按优势分配) | **↓50%** |
| 节假日处理 | 无 | 完整 10天/年 | **新增** |
| 策略自优化 | 手动 | 自动反馈闭环 | **新增** |
| 时段策略 | 仅夏令时/冬令时 | 6时段 + 节假日 | **全面** |
| Kill Switch | 需重启进程 | DB 实时生效 | **新增** |
| 止盈/止损 | 无 | LLM 动态评估，含减仓订单详情 | **新增** |
| 公司行动处理 | 无 | 拆股/分红自动调整 | **新增** |
| 接口重试 | 无 | 指数退避 3 次 | **新增** |

---

## 十一、审计优化清单（v3.1 新增）

> 基于外部量化投资专家审计报告的辩证评估结果，聚焦真正影响资金安全的核心问题。

### P0 核心修复（资金安全）

| # | 问题 | 修复方案 | 状态 |
|---|------|----------|------|
| 1 | 竞态条件: `_check_order_cooldown` + `_lock_order` 非原子 | 合并为 `_try_acquire_order_lock` (INSERT OR IGNORE) | ✅ |
| 2 | 幂等性缺失: signal_id 无唯一约束 | signal_id 预检查 + DB 约束 | ✅ |
| 3 | Kill Switch 需重启进程 | 新增 `system_config` 表，实时读取 | ✅ |
| 4 | 启动时未与券商对账 | 启动时先 `sync_from_broker()` | ✅ |

### P1 高优先级

| # | 问题 | 修复方案 | 状态 |
|---|------|----------|------|
| 5 | 拆股/分红无处理逻辑 | `analysis/corporate_actions.py` | ✅ |
| 6 | Prompt 注入风险 | `analysis/prompt_guard.py` (注入检测+LLM输出校验) | ✅ |
| 7 | 接口失败无重试 | Longbridge 指数退避重试 (3 次) | ✅ |
| 8 | 止盈/止损硬规则兜底 | 移除，完全由 LLM 动态评估 | ✅ |

### v3.2 持仓止盈策略优化

| # | 问题 | 修复方案 | 状态 |
|---|------|----------|------|
| 9 | 减仓判断依赖 P&L 猜测 | 改为查交易记录 `_has_sell_trades()` | ✅ |
| 10 | LLM 缺少减仓上下文 | `_get_sell_trades()` 提供卖出订单详情 | ✅ |
| 11 | 假设"减仓=利润锁定" | 移除假设，仅作为中性事实标注 | ✅ |
| 12 | 持仓成本价不准确 | 券商摊薄成本法，不自行计算 | ✅ |

### Kill Switch 使用方式

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
