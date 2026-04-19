# 📈 US Data Hub — 美股自动交易系统

> 基于多智能体分析 + 动态阈值 + 统一风控的美股自动化交易框架

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-green.svg)](https://www.python.org/)
[![Version](https://img.shields.io/badge/v3.1-审计优化版-orange.svg)](CHANGELOG.md)

---

## ✨ 核心特性

- **多智能体分析** — TradingAgents 框架（牛熊辩论 → 投资法官 → 风控辩论 → 组合管理）
- **LLM 双端点调度** — CodingPlan（主力决策）+ 百炼（8 模型按优势分流）
- **统一风控仲裁** — P0~P4 五级优先级（熔断 → 订单冷却 → 信号去重 → 动态阈值 → 综合风控）
- **6 时段策略** — 自动识别夏令时/冬令时 + 美国节假日日历
- **并行数据采集** — 7 只股票 × 5 数据源并行采集，耗时从 17min 降至 3-5min
- **策略反馈闭环** — 胜率/Sharpe/回撤自动反馈 → 动态调整选股权重
- **全链路可追溯** — Trace ID + Event Bus + 结构化日志

### 🛡️ v3.1 审计优化新增

- **原子化竞态修复** — `_try_acquire_order_lock` 合并检查+锁定，消除重复下单风险
- **幂等性保证** — signal_id 预检查 + DB 唯一约束，同一信号不重复执行
- **Kill Switch DB 版** — 实时生效，无需重启进程
- **启动对账机制** — 系统启动时自动同步券商持仓 + 处理公司行动
- **公司行动处理** — 拆股/分红自动识别与持仓调整
- **LLM 动态止盈/止损** — 止盈止损全权由 LLM 综合评估，不设硬性阈值
- **Prompt 注入防护** — 外部内容清洗 + LLM 输出校验
- **接口重试机制** — Longbridge 指数退避重试（3 次）

---

## 🏗️ 系统架构

```
┌──────────────┐   ┌──────────┐   ┌──────────────┐   ┌─────────────┐
│ 数据采集      │──→│ 三层选股  │──→│ 多智能体分析  │──→│ 信号聚合中心 │
│ 并行采集      │   │ Screener │   │ TradingAgents│   │ SignalHub   │
└──────┬───────┘   └────┬─────┘   └──────┬───────┘   └──────┬──────┘
       │                │                │                   │
       ▼                ▼                ▼                   ▼
┌─────────────┐   ┌──────────┐   ┌──────────────┐   ┌─────────────┐
│ LLM 路由     │   │ 持仓监控  │   │ 统一风控仲裁  │   │ 交易执行     │
│ 8 模型分流   │   │          │   │ P0→P1→P2→P3→P4│   │ 订单冷却锁   │
└─────────────┘   └──────────┘   └──────────────┘   └─────────────┘
       │                │                │                   │
       └────────────────┴────────────────┴───────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │ 复盘 + 策略反馈闭环 │
                    │ → 选股权重自优化    │
                    └───────────────────┘
```

详细的架构图见 → [docs/agent_loop_diagram_v2.md](docs/agent_loop_diagram_v2.md)

---

## 📂 项目结构

```
us-data-hub/
├── analysis/              # 核心分析模块
│   ├── llm_router.py          # LLM 多模型路由（CodingPlan + 百炼 8 模型）
│   ├── risk_arbitrator.py     # 统一风控仲裁器（P0-P4）
│   ├── session_strategy.py    # 6 时段策略（夏令时/冬令时/节假日）
│   ├── holiday_calendar.py    # 美国节假日日历
│   ├── feedback_loop.py       # 策略反馈闭环
│   ├── signal_hub.py          # 信号聚合与冲突解决
│   ├── dynamic_threshold.py   # 动态阈值计算
│   ├── circuit_breaker.py     # 熔断器
│   ├── screener.py            # 三层选股
│   ├── market_regime.py       # 市场状态识别
│   ├── corporate_actions.py   # 🆕 公司行动处理（拆股/分红）
│   └── prompt_guard.py        # 🆕 Prompt 注入防护 + LLM 输出校验
│
├── collectors/            # 数据采集
│   ├── parallel_collector.py    # 并行采集引擎
│   ├── price.py                 # 价格数据
│   ├── longbridge.py            # Longbridge 行情
│   ├── google_news.py           # Google 新闻
│   ├── reddit.py                # Reddit 情绪
│   └── sec.py                   # SEC 财报
│
├── executors/             # 交易执行
│   ├── smart_executor.py        # 智能执行器
│   ├── longbridge.py            # Longbridge 下单
│   └── shadow_executor.py       # 影子执行（Dry Run）
│
├── monitoring/            # 监控
│   └── holding_monitor.py       # 持仓监控（LLM 动态止盈/止损评估）
│
├── management/            # 管理
│   └── position_manager.py      # 仓位管理
│
├── tradingagents/         # TradingAgents 多智能体框架
│   ├── tradingagents/agents/    # 智能体（牛/熊/风控/组合）
│   ├── tradingagents/dataflows/ # 数据流
│   └── tradingagents/graph/     # 图编排
│
├── config/                # 配置文件
│   ├── sources.yaml             # 数据源配置
│   ├── screener_config.json     # 选股权重（反馈闭环自动更新）
│   └── stock_universe.py        # 股票池
│
├── scripts/               # 可执行脚本
│   ├── run.py                   # 统一 CLI
│   ├── auto_execute.py          # 自动交易执行
│   ├── watcher.py               # 事件驱动监控
│   ├── collect.py               # 数据采集
│   └── validate_strategy.py     # 策略验证
│
├── docs/                  # 文档
│   └── agent_loop_diagram_v2.md # 系统架构全景图
│
├── Makefile               # 快捷命令
├── requirements.txt       # Python 依赖
└── .env.example           # 环境变量模板
```

---

## 🚀 快速开始

### 1. 环境准备

```bash
# 克隆仓库
git clone https://github.com/portgasxu/us-data-hub.git
cd us-data-hub

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
# 复制并编辑环境变量
cp .env.example .env
# 编辑 .env 填入你的 API Keys
```

### 3. 初始化系统

```bash
make init       # 初始化数据库和目录
make status     # 查看系统状态
```

### 4. 运行

```bash
# 完整数据管道（采集 → 选股 → 分析）
make pipeline

# 单独运行各模块
make collect      # 数据采集
make screener     # 选股
make monitor      # 持仓监控
make report       # 生成报告

# 事件驱动监控
make watch          # 启动
make watch-status   # 查看状态
make watch-stop     # 停止
```

---

## ⚙️ 环境变量

复制 `.env.example` 为 `.env` 后配置：

```bash
# LLM 端点
CODING_PLAN_KEY=your_coding_plan_key       # CodingPlan 主力决策
BAILIAN_KEY=your_bailian_key               # 百炼辅助分流
OPENAI_API_KEY=your_openai_key             # 兼容旧配置

# Longbridge（行情 + 交易）
LONGPORT_API_SECRET=your_secret
LONGPORT_API_ACCESS_KEY=your_access_key
LONGPORT_APP_ID=your_app_id
LONGPORT_ENVIRONMENT=live                  # live | paper

# 代理（按需配置）
HTTP_PROXY=http://127.0.0.1:7890
HTTPS_PROXY=http://127.0.0.1:7890
```

> ⚠️ **安全提示：** `.env` 已在 `.gitignore` 中排除，不会被提交到仓库。

---

## 🧠 LLM 模型分流

### 端点配置

| 端点 | 模型 | 定位 |
|------|------|------|
| CodingPlan | qwen3.6-plus | 核心交易决策（主力） |
| 百炼 | qwen3-max / deepseek-r1 / qwq-plus | 深度推理 + 复杂决策 |
| 百炼 | qwen3-coder-plus / qwen3-coder-flash | SEC 解析 + 代码生成 |
| 百炼 | qwen3.6-flash | 情感分析（快速轻量） |
| 百炼 | qwen-turbo | 市场监控（最便宜） |

### 降级链路

```
CodingPlan 失败 → 百炼同模型 → qwen3.6-flash（末端兜底）
```

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

### v3.1 安全增强

| 特性 | 说明 |
|------|------|
| **原子化竞态修复** | `_try_acquire_order_lock` 合并检查+锁定，消除并发冲突 |
| **幂等性** | signal_id 预检查 + DB 唯一约束，同一信号不重复执行 |
| **Kill Switch DB** | `system_config` 表存储，修改后实时生效，无需重启 |
| **启动对账** | 每次启动先 `sync_from_broker()` 同步券商持仓 |
| **硬止损/止盈** | LLM 动态评估止盈/止损，不设硬性阈值 |
| **公司行动处理** | 拆股/分红自动识别与持仓成本调整 |
| **Prompt 防护** | 外部内容注入检测 + LLM 输出格式/范围校验 |
| **接口重试** | Longbridge 指数退避重试（3 次，最大 4s 退避） |

---

## ⏰ 时段策略

自动识别夏令时/冬令时 + 美国节假日：

| 时段 | 交易模式 | 最大交易数 |
|------|---------|-----------|
| 深度夜盘 | 禁止 | 0 |
| 盘前准备 | 禁止 | 0 |
| 盘前交易 | 仅持仓调整 | 2 |
| **盘中** | **全功能** | **5** |
| 盘后 | 仅止损/止盈 | 2 |
| 休市/节假日 | 禁止 | 0 |

---

## 📊 性能指标

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| 数据采集耗时 | ~17.5 min | ~3-5 min |
| 多智能体分析 (5只) | ~25 min | ~5 min |
| 单次完整 Loop | ~45 min | ~10-15 min |
| 重复下单风险 | 高 | 消除（原子锁+幂等） |
| LLM 成本 | 全部高配 | 分级分流，降低 ~50% |
| Kill Switch | 需重启进程 | DB 实时生效 |
| 硬止损/止盈 | 无 | LLM 动态评估 |
| 公司行动处理 | 无 | 拆股/分红自动调整 |
| 接口重试 | 无 | 指数退避 3 次 |

---

## 📋 Crontab 调度

```bash
# 盘前（15:00-21:30）
*/30 15-21 * * 1-5   collect (price only)
0 16,18,20 * * 1-5   screener + 选股简报

# 盘中（22:30-05:00 夏令时）
*/5  21-03 * * 1-5   collect (price + news)
*/15 21-03 * * 1-5   watcher
*/30 21-03 * * 1-5   auto_execute

# 盘后
0 5 * * 1-5          复盘分析
0 6 * * 1-5          盘前简报

# 周度
0 10 * * 6           深度复盘
```

---

## 📄 License

MIT License — 详见 [LICENSE](LICENSE)

---

> 🤖 本系统使用 AI 辅助构建，持续迭代中。v3.1 审计优化详见 [CHANGELOG.md](CHANGELOG.md)
> 
> 📐 详细架构图 → [docs/agent_loop_diagram_v2.md](docs/agent_loop_diagram_v2.md)
