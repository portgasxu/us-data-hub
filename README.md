# 📈 US Data Hub — 美股自动交易系统

> 基于多智能体分析 + 动态阈值 + 统一风控的美股自动化交易框架
> 
> 🧠 **v3.4: Crontab 退场 + Orchestrator 统一调度 + 守护进程修复**

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-green.svg)](https://www.python.org/)
[![Version](https://img.shields.io/badge/v3.4-Orchestrator统一调度-purple.svg)](CHANGELOG.md)

---

## ✨ 核心特性

- **🧠 JVS 系统大脑** — LLM 驱动的 Orchestrator，实时监控+智能调度+自动自愈所有模块
- **多智能体分析** — TradingAgents 框架（牛熊辩论 → 投资法官 → 风控辩论 → 组合管理）
- **LLM 双端点调度** — CodingPlan（主力决策）+ 百炼（8 模型按优势分流）
- **统一风控仲裁** — P0~P4 五级优先级（熔断 → 订单冷却 → 信号去重 → 动态阈值 → 综合风控）
- **6 时段策略** — 自动识别夏令时/冬令时 + 美国节假日日历
- **并行数据采集** — 7 只股票 × 5 数据源并行采集，耗时从 17min 降至 3-5min
- **策略反馈闭环** — 胜率/Sharpe/回撤自动反馈 → 动态调整选股权重
- **全链路可追溯** — Trace ID + Event Bus + 结构化日志

### 🧠 v3.4 Crontab 退场 + Orchestrator 统一调度（2026-04-20）

- **Crontab 全面退场** — 所有定时任务统一归 JVS 大脑管理，消除多调度冲突
- **守护进程修复** — 价格采集和订单监控从一次性脚本改为 daemon 常驻进程
- **订单命令修复** — `longbridge orders` → `longbridge order --format json`
- **先斩后奏原则** — JVS 遇到问题直接排查修复，不需请示

### 🧠 v3.3 JVS 系统大脑（2026-04-20）

- **一键启动** — 用户说「启动交易系统」，JVS 一键接管所有模块，无需手动启动
- **智能调度** — 根据市场时段自动调整策略（夜盘低功耗/盘前准备/盘中全功能）
- **实时自愈** — 模块挂了自动重启、连续失败自动升级处理、信号链路断裂自动修复
- **全模块对接** — Screener → TradingAgents → SignalHub → Auto-Execute → 下单，全链路打通
- **LLM 决策引擎** — 每 5 分钟执行一次 LLM 决策周期（CodingPlan qwen3.6-plus）
- **降级策略** — LLM 不可用时自动切换规则引擎

### 🛡️ v3.1 审计优化

- **原子化竞态修复** — `_try_acquire_order_lock` 合并检查+锁定，消除重复下单风险
- **幂等性保证** — signal_id 预检查 + DB 唯一约束，同一信号不重复执行
- **Kill Switch DB 版** — 实时生效，无需重启进程
- **启动对账机制** — 系统启动时自动同步券商持仓 + 处理公司行动
- **公司行动处理** — 拆股/分红自动识别与持仓调整
- **LLM 动态止盈/止损** — 止盈止损全权由 LLM 综合评估，不设硬性阈值
- **Prompt 注入防护** — 外部内容清洗 + LLM 输出校验
- **接口重试机制** — Longbridge 指数退避重试（3 次）

### 📊 v3.2 持仓止盈策略优化

- **成本价修正** — 使用券商摊薄成本法（减仓后成本摊薄，不自行计算）
- **减仓判断** — 直接查交易记录 `_has_sell_trades()`，不再依赖 P&L 猜测
- **减仓详情** — `_get_sell_trades()` 提供完整卖出订单详情给 LLM（时间/数量/价格/触发原因）
- **中性标注** — 减仓仅作为事实标注，不假设"减仓=利润锁定"

---

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        JVS 系统大脑 (Orchestrator)                   │
│  ┌─────────┐  ┌──────────┐  ┌────────────┐  ┌──────────────────┐   │
│  │Monitor  │→ │ LLM决策  │→ │ Executor   │→ │ Brain Memory     │   │
│  │(30s轮询) │  │ (5min)   │  │ 启动/停止   │  │ 持久化状态       │   │
│  └─────────┘  └──────────┘  └────────────┘  └──────────────────┘   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ 智能调度所有模块
  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
  │价格  │ │新闻  │ │选股  │ │全循环│ │持仓  │ │订单  │
  │采集  │ │监控  │ │系统  │ │交易  │ │监控  │ │监控  │
  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘ └──────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
     ┌──────────────┐   ┌──────────────┐
     │ Screener     │──→│ TradingAgents│──→│ SignalHub  │
     │ 持仓去重     │   │ 多Agent分析   │   │ 信号聚合   │
     └──────────────┘   └──────────────┘   └──────┬─────┘
                                                   │
                     ┌─────────────────────────────┘
                     ▼
              ┌──────────────┐
              │ 统一风控仲裁  │──→│ 交易执行    │
              │ P0→P4 五级    │   │ 订单冷却锁   │
              └──────────────┘   └──────────────┘
```

详细的架构图见 → [docs/agent_loop_diagram_v2.md](docs/agent_loop_diagram_v2.md)

---

## 📂 项目结构

```
us-data-hub/
├── monitoring/            # 监控模块 🆕 v3.3
│   ├── holding_monitor.py      # 持仓监控（LLM动态止盈+减仓订单详情）
│   ├── order_monitor.py        # 🆕 订单监控（pending订单/超时/跳空/部分成交）
│   └── orchestrator.py         # 🆕 JVS 系统大脑（LLM智能调度守护进程）
│
├── analysis/              # 核心分析模块
│   ├── llm_router.py           # LLM 多模型路由（CodingPlan + 百炼 8 模型）
│   ├── risk_arbitrator.py      # 统一风控仲裁器（P0-P4）
│   ├── session_strategy.py     # 6 时段策略（夏令时/冬令时/节假日）
│   ├── holiday_calendar.py     # 美国节假日日历
│   ├── feedback_loop.py        # 策略反馈闭环
│   ├── signal_hub.py           # 信号聚合与冲突解决
│   ├── signal_schema.py        # 🆕 TradeSignal 数据结构
│   ├── dynamic_threshold.py    # 动态阈值计算
│   ├── circuit_breaker.py      # 熔断器
│   ├── screener.py             # 三层选股
│   ├── market_regime.py        # 市场状态识别
│   ├── corporate_actions.py    # 公司行动处理（拆股/分红）
│   └── prompt_guard.py         # Prompt 注入防护 + LLM 输出校验
│
├── collectors/            # 数据采集
│   ├── parallel_collector.py   # 并行采集引擎
│   ├── price.py                # 价格数据
│   ├── longbridge.py           # Longbridge 行情
│   ├── google_news.py          # Google 新闻
│   ├── reddit.py               # Reddit 情绪
│   └── sec.py                  # SEC 财报
│
├── executors/             # 交易执行
│   ├── smart_executor.py       # 智能执行器
│   ├── longbridge.py           # Longbridge 下单
│   └── shadow_executor.py      # 影子执行（Dry Run）
│
├── management/            # 管理
│   └── position_manager.py     # 仓位管理（券商摊薄成本法）
│
├── tradingagents/         # TradingAgents 多智能体框架
│   ├── tradingagents/agents/   # 智能体（牛/熊/风控/组合）
│   ├── tradingagents/dataflows/# 数据流
│   └── tradingagents/graph/    # 图编排
│
├── config/                # 配置文件
│   ├── sources.yaml            # 数据源配置
│   ├── screener_config.json    # 选股权重（反馈闭环自动更新）
│   └── stock_universe.py       # 股票池
│
├── scripts/               # 可执行脚本
│   ├── run.py                  # 统一 CLI
│   ├── auto_execute.py         # 自动交易执行（多模式）
│   ├── system_manager.py       # 🆕 系统管理器（一键启动/停止/状态）
│   ├── screen_to_trade.py      # 选股→交易流水线（带持仓去重）
│   ├── watcher.py              # 事件驱动监控
│   ├── calculate_factors.py    # 🆕 因子计算入口
│   ├── price_collector_daemon.py  # 🆕 v3.4 价格采集守护进程
│   ├── order_monitor_daemon.py    # 🆕 v3.4 订单监控守护进程
│   └── validate_strategy.py    # 策略验证
│
├── docs/                  # 文档
│   └── agent_loop_diagram_v2.md # 系统架构全景图
│
├── temp/                  # 运行时状态
│   ├── brain_state.json        # 🆕 JVS 大脑持久化状态
│   └── pids/                   # 🆕 守护进程 PID 管理
│
├── logs/                  # 日志目录
│   ├── orchestrator.log        # 🆕 大脑运行日志
│   ├── order_monitor.log       # 🆕 订单监控日志
│   └── *.log                   # 各模块日志
│
├── Makefile               # 快捷命令
├── requirements.txt       # Python 依赖
└── .env.example           # 环境变量模板
```

---

## 🚀 快速开始

### 1. 环境准备

```bash
git clone https://github.com/portgasxu/us-data-hub.git
cd us-data-hub
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env 填入 API Keys
```

### 3. 初始化系统

```bash
make init
make status
```

### 4. 一键启动（推荐）

```bash
# 🧠 启动 JVS 系统大脑 — 全自动接管
python3 monitoring/orchestrator.py

# 或使用系统管理器
python3 scripts/system_manager.py start
python3 scripts/run.py start

# 查看大脑状态
python3 monitoring/orchestrator.py --status
python3 scripts/system_manager.py status
```

### 5. 单独运行各模块

```bash
python3 scripts/run.py screener           # 选股
python3 scripts/run.py screen-to-trade    # 选股→交易完整链路
python3 scripts/run.py order-monitor      # 订单监控
python3 scripts/auto_execute.py --mode full-loop       # 全循环交易
python3 scripts/auto_execute.py --mode holding-monitor # 持仓监控
python3 scripts/auto_execute.py --mode review          # 盘后复盘
python3 scripts/auto_execute.py --mode morning-brief   # 盘前晨报
```

---

## ⚙️ 环境变量

```bash
# LLM 端点
CODING_PLAN_KEY=your_key              # CodingPlan 主力决策
CODING_PLAN_URL=https://coding.dashscope.aliyuncs.com/v1
CODING_PLAN_MODEL=qwen3.6-plus
BAILIAN_KEY=your_key                  # 百炼辅助分流
OPENAI_API_KEY=your_key               # 兼容旧配置

# Longbridge（行情 + 交易）
LONGPORT_API_SECRET=your_secret
LONGPORT_API_ACCESS_KEY=your_key
LONGPORT_APP_ID=your_app_id
LONGPORT_ENVIRONMENT=live
```

---

## 🧠 JVS 系统大脑（v3.3 核心新增）

### 架构

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

### 核心能力

| 能力 | 实现方式 |
|------|---------|
| **实时监控** | 每 30 秒轮询所有模块状态 |
| **智能调度** | LLM 根据市场时段+系统状态决定何时启动/停止 |
| **自愈** | 模块连续失败→自动重启→超过阈值→标记 disabled 并告警 |
| **跨模块协调** | 模块间依赖检查，自动串联 |
| **上下文记忆** | `brain_state.json` 持久化所有运行历史和决策 |

### 管理的模块

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

### LLM 决策

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

---

## 📋 订单监控（v3.3 新增）

### 监控场景

| 场景 | 处理方式 |
|------|---------|
| 已成交 | 同步到 trades 表 |
| 部分成交 (>90%) | 同步已成交 + 取消剩余 |
| 部分成交 (<90%) | 同步 + TradingAgents 重新评估 |
| 盘外市价单 | 自动取消（不隔夜） |
| 价格跳空 (>3%) | 取消原单 → TradingAgents 重评 → 新信号注入 SignalHub |
| 超时未成交 (15min) | 检查原始信号 → 有效则保留，无效则取消 |

### 重新评估对接点

| 场景 | 对接模块 | 函数 |
|------|---------|------|
| 价格跳空 | TradingAgents | `run_trading_analysis()` |
| 信号有效性 | SignalHub | `collect_all()` |
| 风控检查 | auto_execute.py | `check_risk_rules()` |
| 持仓更新 | PositionManager | `sync_from_broker()` |

---

## 🔒 风控体系

### 五级优先级仲裁

| 优先级 | 检查项 | 动作 |
|--------|--------|------|
| **P0** | 熔断器 | 暂停所有交易 |
| **P1** | 订单冷却（10min） | 防重复下单 |
| **P2** | 信号 cooldown | 信号去重 |
| **P3** | 动态阈值 | 信号质量过滤 |
| **P4** | 综合风控 | 最终检查 |

### v3.3 新增：Pending 订单去重（Fix #12）

```python
# 下单前检查 broker 端是否有 pending 订单
pending = _get_symbols_with_pending_orders()  # NotReported/Submitted/Queued
if symbol in pending:
    skip()  # 防止盘外重复下单
```

### 启动时自动清理

每次 `execute_signals()` 或 `execute_alerts()` 启动时：
1. 扫描 broker 端所有 NotReported 市价单
2. 自动取消陈旧挂单
3. 记录清理日志

---

## ⏰ 时段策略

| 时段 | 交易模式 | 最大交易数 |
|------|---------|-----------|
| 深度夜盘 | 禁止 | 0 |
| 盘前准备 | 禁止 | 0 |
| 盘前交易 | 仅持仓调整 | 2 |
| **盘中** | **全功能** | **5** |
| 盘后 | 仅止损/止盈 | 2 |
| 休市/节假日 | 禁止 | 0 |

---

## 📋 调度方式（v3.4: JVS 大脑统一管理）

> ⚠️ **Crontab 已全面退场**。所有模块调度统一归 JVS 大脑管理：
> - 每 30 秒 tick 轮询
> - 每 5 分钟 LLM 决策
> - 模块失败自动重启 + 超过阈值告警
> - 价格采集/订单监控改为 daemon 常驻进程

```bash
# v3.4: Crontab 已废弃，以下为历史参考
# 所有调度统一归 JVS 大脑管理
```

---

## 📊 性能指标

| 指标 | 优化前 | v3.3 |
|------|--------|------|
| 数据采集耗时 | ~17.5 min | ~3-5 min |
| 多智能体分析 (5只) | ~25 min | ~5 min |
| 单次完整 Loop | ~45 min | ~10-15 min |
| 重复下单风险 | 高 | 消除（原子锁+幂等+pending去重） |
| LLM 成本 | 全部高配 | 分级分流，降低 ~50% |
| 模块管理 | 手动/crontab | JVS 大脑全自动（v3.4 Crontab 退场） |
| 订单监控 | 无 | 实时监控+自动自愈 |
| 信号链路 | 断裂 | 全链路打通 |

---

## 📄 License

MIT License — 详见 [LICENSE](LICENSE)

> 🧠 v3.4: Crontab 退场 + Orchestrator 统一调度 + 守护进程修复
> 
> 📐 详细架构图 → [docs/agent_loop_diagram_v2.md](docs/agent_loop_diagram_v2.md)
