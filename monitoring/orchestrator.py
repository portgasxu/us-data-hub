#!/usr/bin/env python3
"""
US Data Hub — Orchestrator Brain (LLM 系统大脑)
=================================================
基于 LLM 的智能调度守护进程，作为整个交易系统的"大脑"。

职责:
  1. 实时监控 — 每 30 秒检查所有模块状态
  2. 智能调度 — 根据市场时段+系统状态动态决定何时启动/停止
  3. 自愈 — 模块失败自动重启，连续失败自动升级处理
  4. 跨模块协调 — 模块间依赖自动串联（选股→TradingAgents→执行）
  5. 上下文记忆 — 记住运行历史和决策逻辑

架构:
  ┌─────────────────────────────────────────────┐
  │              Orchestrator Brain              │
  │  ┌─────────┐  ┌──────────┐  ┌────────────┐  │
  │  │Monitor  │→ │ LLM决策  │→ │Executor    │  │
  │  │(30s轮询) │  │(每5min)  │  │(启动/停止) │  │
  │  └─────────┘  └──────────┘  └────────────┘  │
  │        ↑                           ↓         │
  │  ┌─────────────────────────────────────┐     │
  │  │         Brain State Memory          │     │
  │  │  (运行历史 + 决策日志 + 自愈记录)    │     │
  │  └─────────────────────────────────────┘     │
  └─────────────────────────────────────────────┘
                    ↓ 调度各模块
  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
  │价格  │ │新闻  │ │选股  │ │交易  │ │订单  │
  │采集  │ │监控  │ │系统  │ │执行  │ │监控  │
  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘

用法:
    python monitoring/orchestrator.py              # 前台运行
    nohup python monitoring/orchestrator.py &      # 后台运行
    python monitoring/orchestrator.py --status      # 查看大脑状态
"""

import sys
import os
import json
import signal
import time
import subprocess
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()

from storage import Database
from dayup_logger import setup_root_logger
import logging

setup_root_logger(level=logging.INFO)
logger = logging.getLogger("orchestrator")


# ═══════════════════════════════════════════════
# 枚举
# ═══════════════════════════════════════════════

class ModuleStatus(str, Enum):
    IDLE = "idle"               # 未启动
    RUNNING = "running"         # 正常运行
    COMPLETED = "completed"     # 执行完毕（一次性任务）
    FAILED = "failed"           # 执行失败
    RESTARTING = "restarting"   # 正在重启
    DISABLED = "disabled"       # 被禁用


class MarketPhase(str, Enum):
    PRE_MARKET = "pre_market"
    MARKET_OPEN = "market_open"
    AFTER_HOURS = "after_hours"
    NIGHT_SESSION = "night_session"
    WEEKEND = "weekend"


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


# ═══════════════════════════════════════════════
# 模块定义
# ═══════════════════════════════════════════════

@dataclass
class ModuleConfig:
    """模块配置"""
    id: str
    name: str
    command: str
    critical: bool = True
    max_consecutive_failures: int = 3
    restart_cooldown: int = 300  # 5分钟
    run_mode: str = "scheduled"  # scheduled | continuous
    schedule: str = ""           # cron-like: "*/30" 或 "0,15,30,45"
    depends_on: List[str] = field(default_factory=list)
    description: str = ""


# 全部模块配置
MODULE_REGISTRY: Dict[str, ModuleConfig] = {
    "price_collector": ModuleConfig(
        id="price_collector",
        name="价格采集",
        command="python3 scripts/price_collector_daemon.py",
        critical=True,
        run_mode="continuous",  # daemon keeps running, collects every 5 min
        schedule="",
        description="采集实时价格数据",
    ),
    "watcher": ModuleConfig(
        id="watcher",
        name="新闻监控",
        command="python3 scripts/watcher.py --once",
        critical=True,
        run_mode="scheduled",
        schedule="*/15",
        description="扫描新闻和Reddit情绪",
    ),
    "screener": ModuleConfig(
        id="screener",
        name="选股系统",
        command="python3 scripts/auto_execute.py --mode screener-to-trade",
        critical=True,
        max_consecutive_failures=2,
        run_mode="scheduled",
        schedule="0",  # 每小时整点
        description="三层选股→TradingAgents→交易",
    ),
    "full_loop": ModuleConfig(
        id="full_loop",
        name="全循环交易",
        command="python3 scripts/auto_execute.py --mode full-loop",
        critical=True,
        run_mode="scheduled",
        schedule="*/30",
        description="从SignalHub取信号执行交易",
    ),
    "holding_monitor": ModuleConfig(
        id="holding_monitor",
        name="持仓监控",
        command="python3 scripts/auto_execute.py --mode holding-monitor",
        critical=True,
        run_mode="scheduled",
        schedule="0",
        description="LLM动态止盈止损",
    ),
    "order_monitor": ModuleConfig(
        id="order_monitor",
        name="订单监控",
        command="python3 scripts/order_monitor_daemon.py",
        critical=True,
        run_mode="continuous",
        schedule="",
        description="监控pending订单状态",
    ),
    "review": ModuleConfig(
        id="review",
        name="盘后复盘",
        command="python3 scripts/auto_execute.py --mode review",
        critical=False,
        run_mode="scheduled",
        schedule="5",  # 05:00
        description="盘后统计",
    ),
    "morning_brief": ModuleConfig(
        id="morning_brief",
        name="盘前晨报",
        command="python3 scripts/auto_execute.py --mode morning-brief",
        critical=False,
        run_mode="scheduled",
        schedule="6",  # 06:00
        description="盘前简报",
    ),
    "factors": ModuleConfig(
        id="factors",
        name="因子计算",
        command="python3 scripts/calculate_factors.py",
        critical=False,
        run_mode="scheduled",
        schedule="4",  # 04:00
        description="技术指标因子计算",
    ),
}


# ═══════════════════════════════════════════════
# 状态管理
# ═══════════════════════════════════════════════

@dataclass
class ModuleState:
    """模块运行时状态"""
    status: str = ModuleStatus.IDLE.value
    last_run: str = ""
    last_result: str = ""
    last_error: str = ""
    consecutive_failures: int = 0
    total_runs: int = 0
    total_failures: int = 0
    pid: Optional[int] = None
    next_scheduled: str = ""
    last_decision: str = ""  # LLM 的决策记录


@dataclass
class BrainState:
    """大脑全局状态"""
    started_at: str = ""
    last_tick: str = ""
    last_llm_cycle: str = ""
    market_phase: str = MarketPhase.NIGHT_SESSION.value
    modules: Dict[str, ModuleState] = field(default_factory=dict)
    alerts: List[Dict] = field(default_factory=list)
    llm_decisions: List[Dict] = field(default_factory=list)
    extra_data: Dict[str, Any] = field(default_factory=dict)  # 自愈尝试计数等扩展数据
    uptime_seconds: int = 0


class BrainMemory:
    """大脑持久记忆"""

    def __init__(self, state_file: str = None):
        self.state_file = state_file or os.path.join(
            PROJECT_ROOT, "temp", "brain_state.json"
        )
        self.state = BrainState()
        self._load()

    def _load(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file) as f:
                    data = json.load(f)
                self.state = BrainState(**data)
            except Exception as e:
                logger.warning(f"Failed to load brain state: {e}")
        if not self.state.started_at:
            self.state.started_at = datetime.now().isoformat()

    def save(self):
        self.state.last_tick = datetime.now().isoformat()
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        with open(self.state_file, "w") as f:
            json.dump(asdict(self.state), f, indent=2, ensure_ascii=False)

    def add_alert(self, level: str, module: str, message: str):
        alert = {
            "time": datetime.now().isoformat(),
            "level": level,
            "module": module,
            "message": message,
        }
        self.state.alerts.append(alert)
        # 只保留最近 100 条
        if len(self.state.alerts) > 100:
            self.state.alerts = self.state.alerts[-100:]
        self.save()

    def add_decision(self, module: str, action: str, reason: str):
        decision = {
            "time": datetime.now().isoformat(),
            "module": module,
            "action": action,
            "reason": reason,
        }
        self.state.llm_decisions.append(decision)
        if len(self.state.llm_decisions) > 200:
            self.state.llm_decisions = self.state.llm_decisions[-200:]
        self.save()

    def get_module_state(self, module_id: str) -> ModuleState:
        if module_id not in self.state.modules:
            self.state.modules[module_id] = ModuleState()
        val = self.state.modules[module_id]
        # JSON load produces dicts, convert back to ModuleState
        if isinstance(val, dict):
            ms = ModuleState(**{k: v for k, v in val.items() if k in ModuleState.__dataclass_fields__})
            self.state.modules[module_id] = ms
            return ms
        return val


# ═══════════════════════════════════════════════
# 市场时段判断
# ═══════════════════════════════════════════════

def get_market_phase() -> MarketPhase:
    """判断当前市场时段（北京时间）"""
    from datetime import timezone, timedelta
    cst = timezone(timedelta(hours=8))
    now = datetime.now(cst)
    weekday = now.weekday()
    if weekday >= 5:
        return MarketPhase.WEEKEND

    hour = now.hour
    minute = now.minute
    t = hour * 60 + minute

    # 夏令时（简化判断，3月第二周~11月第一周）
    dst = now.month >= 4 and now.month <= 10

    if dst:
        pre_start = 16 * 60       # 16:00
        market_start = 21 * 60 + 30  # 21:30
        market_end = 4 * 60        # 04:00
        after_end = 8 * 60         # 08:00
    else:
        pre_start = 17 * 60        # 17:00
        market_start = 22 * 60 + 30  # 22:30
        market_end = 5 * 60        # 05:00
        after_end = 9 * 60         # 09:00

    if t >= market_start or t < market_end:
        return MarketPhase.MARKET_OPEN
    elif t >= market_end and t < after_end:
        return MarketPhase.AFTER_HOURS
    elif t >= after_end and t < pre_start:
        return MarketPhase.NIGHT_SESSION
    elif t >= pre_start and t < market_start:
        return MarketPhase.PRE_MARKET
    return MarketPhase.NIGHT_SESSION


def is_trading_time() -> bool:
    phase = get_market_phase()
    return phase in (MarketPhase.MARKET_OPEN, MarketPhase.PRE_MARKET)


def is_deep_night() -> bool:
    return get_market_phase() in (MarketPhase.NIGHT_SESSION, MarketPhase.WEEKEND)


# ═══════════════════════════════════════════════
# LLM 决策引擎
# ═══════════════════════════════════════════════

class LLMBrain:
    """
    LLM 决策引擎 — 系统大脑的核心

    工作流程:
    1. 收集所有模块状态
    2. 构建系统状态摘要
    3. 调用 LLM 做决策
    4. 解析 LLM 输出为具体行动
    """

    def __init__(self, memory: BrainMemory):
        self.memory = memory

    def make_cycle_decision(self) -> List[Dict]:
        """
        执行一次 LLM 决策周期（每5分钟调用一次）

        Returns:
            决策列表: [{"module": str, "action": str, "reason": str}]
        """
        # 步骤1: 收集系统状态
        status_report = self._build_status_report()

        # 步骤2: 调用 LLM
        decisions = self._call_llm(status_report)

        # 步骤3: 解析决策
        actions = self._parse_decisions(decisions)

        return actions

    def _build_status_report(self) -> Dict:
        """构建系统状态摘要"""
        phase = get_market_phase()

        modules_status = {}
        for module_id, config in MODULE_REGISTRY.items():
            state = self.memory.get_module_state(module_id)
            modules_status[module_id] = {
                "name": config.name,
                "status": state.status,
                "consecutive_failures": state.consecutive_failures,
                "last_run": state.last_run,
                "last_error": state.last_error,
                "critical": config.critical,
            }

        # 数据库状态
        try:
            db = Database()
            stats = db.get_stats()
            db.close()
            db_status = "healthy"
        except Exception:
            stats = {}
            db_status = "error"

        # 最近告警
        recent_alerts = [
            a for a in self.memory.state.alerts[-10:]
            if datetime.fromisoformat(a["time"]) > datetime.now() - timedelta(minutes=30)
        ]

        return {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "market_phase": phase.value,
            "is_trading_time": is_trading_time(),
            "is_deep_night": is_deep_night(),
            "database": db_status,
            "data_stats": stats,
            "modules": modules_status,
            "recent_alerts": recent_alerts,
            "recent_decisions": self.memory.state.llm_decisions[-5:],
        }

    def _call_llm(self, status_report: Dict) -> str:
        """调用 LLM 做决策"""
        system_prompt = """你是 US Data Hub 美股自动交易系统的调度大脑。

你的职责:
1. 根据系统状态，决定每个模块是否需要启动/停止/重启/代码自愈
2. 识别潜在问题并提前处理
3. 在市场状态变化时调整调度策略

规则:
- 非交易时段，只需保持基础监控（价格采集+订单监控）
- 交易时段，所有关键模块必须运行
- 模块连续失败超过阈值，使用 auto_fix 尝试代码级修复
- 如果 auto_fix 已尝试过且仍失败，标记为 disabled 并告警
- 模块完成了一次性任务后，状态变为 completed

可用 action:
- start: 启动模块
- stop: 停止模块（连续失败超阈值时）
- restart: 重启模块（进程意外退出时）
- auto_fix: 代码级自愈（重启无法解决的重复性故障）
- skip: 跳过（非交易时段或一次性任务已完成）
- monitor: 保持观察（轻微异常但不影响运行）

请严格以 JSON 格式返回决策，不要输出其他内容:
{
  "decisions": [
    {"module": "模块ID", "action": "start|stop|restart|auto_fix|skip|monitor", "reason": "原因"}
  ],
  "alerts": [
    {"level": "info|warning|critical", "module": "模块ID", "message": "告警内容"}
  ],
  "summary": "整体评估一句话"
}"""

        report_json = json.dumps(status_report, indent=2, ensure_ascii=False)

        try:
            # 尝试多种方式调用 LLM
            response = self._try_llm_call(system_prompt, report_json)
            return response
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            # 降级为规则引擎
            return self._rule_based_fallback(status_report)

    def _try_llm_call(self, system_prompt: str, user_input: str, max_retries: int = 2) -> str:
        """尝试调用 LLM，优先使用系统现有 LLM 路由（CodingPlan 主力）"""
        errors = []

        # 方式1: 通过 llm_router → CodingPlan (qwen3.6-plus)
        try:
            from analysis.llm_router import LLMRouter
            router = LLMRouter()
            result = router.invoke(
                task_type="portfolio_manager",  # 走 CodingPlan 路由
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_input},
                ],
                temperature=0.1,
                max_tokens=2000,
            )
            if result.get("success"):
                return result["content"]
            else:
                errors.append(f"llm_router: {result.get('error', 'unknown')}")
        except Exception as e:
            errors.append(f"llm_router: {e}")

        # 方式2: 直连 CodingPlan 端点
        try:
            from openai import OpenAI
            client = OpenAI(
                api_key=os.getenv("CODING_PLAN_KEY", ""),
                base_url=os.getenv("CODING_PLAN_URL", "https://coding.dashscope.aliyuncs.com/v1"),
            )
            resp = client.chat.completions.create(
                model=os.getenv("CODING_PLAN_MODEL", "qwen3.6-plus"),
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_input},
                ],
                temperature=0.1,
                max_tokens=2000,
                timeout=30,
            )
            return resp.choices[0].message.content
        except Exception as e:
            errors.append(f"coding_plan_direct: {e}")

        # 方式3: 百炼兜底
        try:
            from openai import OpenAI
            client = OpenAI(
                api_key=os.getenv("DASHSCOPE_API_KEY", ""),
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            )
            resp = client.chat.completions.create(
                model="qwen3.6-plus",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_input},
                ],
                temperature=0.1,
                max_tokens=2000,
                timeout=30,
            )
            return resp.choices[0].message.content
        except Exception as e:
            errors.append(f"bailian: {e}")

        raise Exception(f"All LLM methods failed: {'; '.join(errors)}")

    def _rule_based_fallback(self, status_report: Dict) -> str:
        """LLM 不可用时的规则引擎降级"""
        decisions = []
        alerts = []
        phase = status_report["market_phase"]
        is_trading = status_report["is_trading_time"]

        for module_id, state in status_report["modules"].items():
            config = MODULE_REGISTRY.get(module_id)
            if not config:
                continue

            failures = state["consecutive_failures"]
            status = state["status"]

            if status == "failed" and failures >= config.max_consecutive_failures:
                # 超过阈值但未超过 L3 尝试上限 → 先试代码自愈
                auto_fix_key = f"{module_id}_auto_fix_attempts"
                fix_attempts = self.memory.state.extra_data.get(auto_fix_key, 0)
                if fix_attempts < 2 and is_trading:
                    decisions.append({
                        "module": module_id,
                        "action": "auto_fix",
                        "reason": f"连续失败 {failures} 次，重启无效，尝试代码级自愈",
                    })
                else:
                    decisions.append({
                        "module": module_id,
                        "action": "stop",
                        "reason": f"连续失败 {failures} 次，自动修复已耗尽，停止等待人工介入",
                    })
                alerts.append({
                    "level": "critical",
                    "module": module_id,
                    "message": f"连续失败 {failures} 次",
                })
            elif status == "failed" and is_trading:
                decisions.append({
                    "module": module_id,
                    "action": "restart",
                    "reason": f"交易时段失败，尝试重启",
                })
            elif status == "idle" and config.critical and is_trading:
                decisions.append({
                    "module": module_id,
                    "action": "start",
                    "reason": "交易时段关键模块未运行",
                })

        result = {
            "decisions": decisions,
            "alerts": alerts,
            "summary": f"规则引擎降级决策: 市场{phase}, 交易{is_trading}",
        }
        return json.dumps(result, ensure_ascii=False)

    def _parse_decisions(self, raw_response: str) -> List[Dict]:
        """解析 LLM 输出为具体行动"""
        try:
            # 尝试从响应中提取 JSON
            json_str = raw_response
            if "```json" in raw_response:
                json_str = raw_response.split("```json")[1].split("```")[0].strip()
            elif "```" in raw_response:
                json_str = raw_response.split("```")[1].strip()

            data = json.loads(json_str)

            decisions = []
            for d in data.get("decisions", []):
                decisions.append({
                    "module": d.get("module", ""),
                    "action": d.get("action", "skip"),
                    "reason": d.get("reason", ""),
                })

            # 处理告警
            for a in data.get("alerts", []):
                self.memory.add_alert(
                    level=a.get("level", "info"),
                    module=a.get("module", "system"),
                    message=a.get("message", ""),
                )

            # 记录 LLM 总结
            if "summary" in data:
                logger.info(f"🧠 LLM 总结: {data['summary']}")

            return decisions

        except json.JSONDecodeError:
            logger.warning(f"Failed to parse LLM response: {raw_response[:200]}")
            return []


# ═══════════════════════════════════════════════
# 模块执行器
# ═══════════════════════════════════════════════

class ModuleExecutor:
    """模块执行管理"""

    def __init__(self, memory: BrainMemory):
        self.memory = memory
        self._processes: Dict[str, subprocess.Popen] = {}

    def should_run(self, module_id: str, config: ModuleConfig) -> bool:
        """检查模块是否应该运行（基于时间和状态）"""
        state = self.memory.get_module_state(module_id)

        # Handle both ModuleState objects and dicts
        if isinstance(state, dict):
            status = state.get("status", "idle")
            failures = state.get("consecutive_failures", 0)
        else:
            status = state.status
            failures = state.consecutive_failures

        # 冷却中
        if status == ModuleStatus.RESTARTING.value:
            return False

        # 已达最大失败次数
        if failures >= config.max_consecutive_failures:
            return False

        # 已在运行（非持续性任务不需要重复启动）
        if status == ModuleStatus.RUNNING.value:
            return False

        # 连续运行模式（daemon）：只要没在运行就启动
        if config.run_mode == "continuous":
            return True

        # 检查调度时间
        now = datetime.now()
        minute = now.minute
        schedule = config.schedule

        if not schedule:
            return False

        if "/" in schedule:
            # 例如 "*/5" = 每5分钟
            interval = int(schedule.split("/")[1])
            return minute % interval == 0
        elif "," in schedule:
            # 例如 "0,15,30,45"
            return str(minute) in schedule.split(",")
        else:
            # 例如 "0" = 整点
            try:
                return minute == int(schedule)
            except ValueError:
                return False

    def start_module(self, module_id: str, config: ModuleConfig) -> bool:
        """启动模块"""
        state = self.memory.get_module_state(module_id)

        # Handle both ModuleState objects and dicts
        if isinstance(state, dict):
            status = state.get("status", "idle")
            pid = state.get("pid")
        else:
            status = state.status
            pid = state.pid

        if status == ModuleStatus.RUNNING.value and pid:
            try:
                os.kill(pid, 0)
                return True  # 已在运行
            except ProcessLookupError:
                pass  # 进程已退出

        log_file = os.path.join(PROJECT_ROOT, "logs", f"{module_id}.log")
        cmd = f"cd {PROJECT_ROOT} && {config.command}"

        try:
            if isinstance(state, dict):
                state["status"] = ModuleStatus.RUNNING.value
                state["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                state["last_error"] = ""
                state["total_runs"] = state.get("total_runs", 0) + 1
            else:
                state.status = ModuleStatus.RUNNING.value
                state.last_run = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                state.last_error = ""
                state.total_runs += 1

            with open(log_file, "a") as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"[{datetime.now().isoformat()}] Started by Orchestrator\n")
                f.write(f"{'='*60}\n")
                process = subprocess.Popen(
                    cmd,
                    shell=True,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    cwd=PROJECT_ROOT,
                )

            # 保存 Popen 对象引用，用于后续检查 exit code
            self._processes[module_id] = process

            if isinstance(state, dict):
                state["pid"] = process.pid
            else:
                state.pid = process.pid
            self.memory.save()

            logger.info(f"✅ Started {config.name} (PID {process.pid})")
            return True

        except Exception as e:
            if isinstance(state, dict):
                state["status"] = ModuleStatus.FAILED.value
                state["last_error"] = str(e)
                state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
                state["total_failures"] = state.get("total_failures", 0) + 1
            else:
                state.status = ModuleStatus.FAILED.value
                state.last_error = str(e)
                state.consecutive_failures += 1
                state.total_failures += 1
            self.memory.add_alert("critical", module_id, f"启动失败: {e}")
            self.memory.save()
            logger.error(f"❌ Failed to start {config.name}: {e}")
            return False

    def _is_process_alive(self, pid: int) -> bool:
        """检查进程是否存在"""
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def stop_module(self, module_id: str, config: ModuleConfig):
        """停止模块"""
        state = self.memory.get_module_state(module_id)

        if isinstance(state, dict):
            pid = state.get("pid")
        else:
            pid = state.pid

        if pid:
            try:
                os.kill(pid, signal.SIGTERM)
                time.sleep(3)
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                logger.info(f"🛑 Stopped {config.name} (PID {pid})")
            except ProcessLookupError:
                pass

        if isinstance(state, dict):
            state["status"] = ModuleStatus.IDLE.value
            state["pid"] = None
        else:
            state.status = ModuleStatus.IDLE.value
            state.pid = None
        self.memory.save()


# ═══════════════════════════════════════════════
# 主循环
# ═══════════════════════════════════════════════

class OrchestratorBrain:
    """系统大脑 — 主循环"""

    def __init__(self):
        self.memory = BrainMemory()
        self.llm = LLMBrain(self.memory)
        self.executor = ModuleExecutor(self.memory)
        self._running = True
        self._tick_count = 0

        # 注册信号
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info(f"🛑 Received signal {signum}, shutting down...")
        self._running = False

    def run(self):
        """主循环"""
        logger.info(f"{'='*60}")
        logger.info(f"🧠 US Data Hub Orchestrator Brain 启动")
        logger.info(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"   市场时段: {get_market_phase().value}")
        logger.info(f"{'='*60}")

        self.memory.state.started_at = datetime.now().isoformat()
        self.memory.save()

        # 初始检查
        self._initial_check()

        # 主循环
        while self._running:
            try:
                self._tick()
                self._tick_count += 1
                self.memory.save()

                # LLM 决策周期（每 10 个 tick = 5 分钟）
                if self._tick_count % 10 == 0:
                    self._llm_cycle()

                time.sleep(30)  # 每 30 秒一个 tick

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"💥 Tick error: {e}", exc_info=True)
                self.memory.add_alert("critical", "brain", f"Tick error: {e}")
                time.sleep(60)

        # 清理
        self._shutdown()

    def _initial_check(self):
        """启动时的初始检查"""
        logger.info("🔍 初始系统检查...")
        phase = get_market_phase()
        self.memory.state.market_phase = phase.value

        for module_id, config in MODULE_REGISTRY.items():
            state = self.memory.get_module_state(module_id)
            if isinstance(state, dict):
                failures = state.get("consecutive_failures", 0)
                name = config.name
            else:
                failures = state.consecutive_failures
                name = config.name
            # 检查上次失败
            if failures >= config.max_consecutive_failures:
                logger.warning(f"⚠️  {name}: 连续失败 {failures} 次")
                self.memory.add_alert(
                    "critical", module_id,
                    f"启动时检测: 连续失败 {failures} 次"
                )

        self.memory.save()

    def _tick(self):
        """每个 tick 执行的操作"""
        phase = get_market_phase()
        self.memory.state.market_phase = phase.value

        # 检查所有模块
        for module_id, config in MODULE_REGISTRY.items():
            # 检查是否应该运行
            if self.executor.should_run(module_id, config):
                # 检查依赖
                deps_met = self._check_dependencies(module_id, config)
                if deps_met:
                    self.executor.start_module(module_id, config)
                else:
                    logger.debug(f"⏭️  {config.name}: 依赖不满足，跳过")

            # 检查运行中的模块是否还活着
            state = self.memory.get_module_state(module_id)
            if isinstance(state, dict):
                status = state.get("status", "idle")
                pid = state.get("pid")
            else:
                status = state.status
                pid = state.pid
            if status == ModuleStatus.RUNNING.value and pid:
                if not self.executor._is_process_alive(pid):
                    # 检查 exit code：区分正常完成 vs 异常退出
                    popen = self.executor._processes.get(module_id)
                    exit_code = None
                    if popen is not None:
                        try:
                            exit_code = popen.wait(timeout=0.1)
                        except Exception:
                            exit_code = popen.returncode

                    is_normal_exit = (exit_code is not None and exit_code == 0)

                    if is_normal_exit and config.run_mode == "scheduled":
                        # scheduled 一次性任务正常完成 → 标记 COMPLETED，不增加失败计数
                        logger.info(f"✅ {config.name} (PID {pid}) 正常完成 (exit {exit_code})")
                        if isinstance(state, dict):
                            state["status"] = ModuleStatus.COMPLETED.value
                            state["consecutive_failures"] = 0
                            state["pid"] = None
                        else:
                            state.status = ModuleStatus.COMPLETED.value
                            state.consecutive_failures = 0
                            state.pid = None
                    else:
                        # 异常退出或 daemon 意外退出 → FAILED
                        logger.warning(f"💀 {config.name} (PID {pid}) 异常退出 (exit {exit_code})")
                        if isinstance(state, dict):
                            state["status"] = ModuleStatus.FAILED.value
                            state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
                            state["total_failures"] = state.get("total_failures", 0) + 1
                            state["pid"] = None
                        else:
                            state.status = ModuleStatus.FAILED.value
                            state.consecutive_failures += 1
                            state.total_failures += 1
                            state.pid = None
                        self.memory.add_alert("warning", module_id, f"进程异常退出 (exit {exit_code})")

        self._tick_count += 1

    def _llm_cycle(self):
        """LLM 决策周期"""
        logger.info("🧠 LLM 决策周期...")

        try:
            decisions = self.llm.make_cycle_decision()

            for decision in decisions:
                module_id = decision.get("module", "")
                action = decision.get("action", "skip")
                reason = decision.get("reason", "")

                config = MODULE_REGISTRY.get(module_id)
                if not config:
                    continue

                logger.info(f"  🤖 {config.name}: {action} — {reason}")

                if action == "start":
                    self.executor.start_module(module_id, config)
                elif action == "stop":
                    self.executor.stop_module(module_id, config)
                elif action == "restart":
                    self.executor.stop_module(module_id, config)
                    time.sleep(5)
                    self.executor.start_module(module_id, config)
                elif action == "auto_fix":
                    self._auto_fix_module(module_id, config, reason)

                # 记录决策
                state = self.memory.get_module_state(module_id)
                if isinstance(state, dict):
                    state["last_decision"] = f"{action}: {reason}"
                else:
                    state.last_decision = f"{action}: {reason}"

                self.memory.add_decision(module_id, action, reason)

        except Exception as e:
            logger.error(f"LLM cycle error: {e}")
            self.memory.add_alert("warning", "brain", f"LLM 决策失败: {e}")

    def _auto_fix_module(self, module_id: str, config: ModuleConfig, reason: str):
        """L3 自愈：LLM 根因分析 + 代码自动修复

        流程：
        1. 收集上下文（日志 + 错误堆栈 + 数据库状态）
        2. 调用 CodingPlan LLM 分析根因并生成修复代码
        3. 自动执行修复（写入文件）
        4. Git commit 存档
        5. 重启模块并验证
        6. 生成修复报告
        """
        logger.info(f"🔧 [L3 自愈] 开始对 {config.name} 进行根因分析 + 代码修复")

        try:
            # ── 步骤1: 收集上下文 ──
            context = self._collect_module_context(module_id, config)
            self.memory.add_alert("info", module_id, f"L3 自愈启动: 收集到 {len(context)} 行上下文")

            # ── 步骤2: LLM 根因分析 + 修复方案 ──
            fix_result = self._llm_analyze_and_fix(module_id, config, context)
            if not fix_result:
                logger.warning(f"🔧 [L3 自愈] {config.name}: LLM 无法生成修复方案")
                self.memory.add_alert("warning", module_id, "L3 自愈: LLM 无法生成修复方案，需人工介入")
                return

            # ── 步骤3: 执行修复 ──
            success = self._apply_fix(fix_result)
            if not success:
                logger.warning(f"🔧 [L3 自愈] {config.name}: 修复应用失败")
                self.memory.add_alert("critical", module_id, f"L3 自愈: 修复应用失败")
                return

            # ── 步骤4: Git commit ──
            self._commit_fix(fix_result, module_id)

            # ── 步骤5: 重启模块 ──
            self.executor.stop_module(module_id, config)
            time.sleep(3)
            started = self.executor.start_module(module_id, config)

            # 记录自愈尝试次数
            auto_fix_key = f"{module_id}_auto_fix_attempts"
            self.memory.state.extra_data[auto_fix_key] = self.memory.state.extra_data.get(auto_fix_key, 0) + 1

            if started:
                logger.info(f"✅ [L3 自愈] {config.name}: 修复完成，模块已重启")
                self.memory.add_alert("info", module_id,
                    f"L3 自愈完成: {fix_result.get('summary', '修复成功')}")
                # 重置失败计数
                state = self.memory.get_module_state(module_id)
                if isinstance(state, dict):
                    state["consecutive_failures"] = 0
                else:
                    state.consecutive_failures = 0
            else:
                logger.error(f"❌ [L3 自愈] {config.name}: 修复后重启失败")
                self.memory.add_alert("critical", module_id, "L3 自愈: 修复成功但重启失败")

        except Exception as e:
            logger.error(f"🔧 [L3 自愈] {config.name}: 异常 {e}")
            self.memory.add_alert("critical", module_id, f"L3 自愈异常: {e}")

    def _collect_module_context(self, module_id: str, config: ModuleConfig) -> str:
        """收集模块相关上下文信息"""
        lines = []

        # 1. 模块日志
        log_files = [
            f"logs/{module_id}.log",
            f"logs/orchestrator_stdout.log",
        ]
        for lf in log_files:
            if os.path.exists(lf):
                try:
                    with open(lf, 'r') as f:
                        all_lines = f.readlines()
                        last_lines = all_lines[-200:]  # 最近 200 行
                        lines.append(f"=== {lf} (最后200行) ===")
                        lines.extend(last_lines)
                except Exception:
                    pass

        # 2. 模块配置
        lines.append(f"=== 模块配置 ===")
        lines.append(f"ID: {config.id}")
        lines.append(f"Command: {config.command}")
        lines.append(f"Critical: {config.critical}")
        lines.append(f"Max failures: {config.max_consecutive_failures}")
        lines.append(f"Restart cooldown: {config.restart_cooldown}s")

        # 3. 模块状态
        state = self.memory.get_module_state(module_id)
        lines.append(f"=== 当前状态 ===")
        if isinstance(state, dict):
            lines.append(json.dumps(state, indent=2, ensure_ascii=False))
        else:
            lines.append(f"status={state.status}, failures={state.consecutive_failures}")

        # 4. 最近决策历史
        lines.append(f"=== 最近决策 ===")
        for d in self.memory.state.llm_decisions[-10:]:
            lines.append(json.dumps(d, ensure_ascii=False))

        # 5. 数据库状态检查
        try:
            from storage import Database
            db = Database()
            for table in ("prices", "trades", "holdings"):
                try:
                    row = db.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    lines.append(f"DB {table}: {row[0]} rows")
                except Exception as e:
                    lines.append(f"DB {table}: ERROR {e}")
            db.close()
        except Exception as e:
            lines.append(f"DB check: ERROR {e}")

        return "\n".join(lines)

    def _llm_analyze_and_fix(self, module_id: str, config: ModuleConfig, context: str) -> Optional[Dict]:
        """调用 CodingPlan LLM 分析根因并生成修复代码"""
        system_prompt = """你是资深 Python 工程师，负责维护一个美股自动交易系统。
现在有一个模块出现问题，你需要：
1. 分析日志和上下文，找出根因
2. 定位到具体的文件和代码行
3. 给出精确的修复方案（包含修改前后的代码对比）
4. 评估影响范围

请以 JSON 格式返回：
{
  "root_cause": "根因描述",
  "file_path": "需要修改的文件路径（相对路径）",
  "fix_type": "code_fix|config_change|log_level",
  "old_code": "需要替换的旧代码（精确匹配）",
  "new_code": "新代码",
  "rationale": "为什么这样修复",
  "impact": "影响范围评估",
  "summary": "一句话总结"
}

如果无法从上下文中找到明确的代码级问题，返回 {"fixable": false, "reason": "原因"}。"""

        user_input = f"模块 {config.name} ({module_id}) 出现问题：\n\n{context}"

        try:
            from analysis.llm_router import LLMRouter
            router = LLMRouter()
            result = router.invoke(
                task_type="portfolio_manager",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_input},
                ],
                temperature=0.1,
                max_tokens=2000,
            )
            if result.get("success"):
                content = result["content"].strip()
                # 提取 JSON
                import re
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0].strip()
                elif "```" in content:
                    content = content.split("```")[1].strip()
                return json.loads(content)
        except Exception as e:
            logger.warning(f"LLM 根因分析失败: {e}")

        return None

    def _apply_fix(self, fix_result: Dict) -> bool:
        """执行 LLM 生成的修复方案"""
        if fix_result.get("fixable") is False:
            logger.info(f"LLM 判定无法自动修复: {fix_result.get('reason', '')}")
            return False

        file_path = fix_result.get("file_path")
        if not file_path:
            return False

        # 确保路径正确
        if not os.path.isabs(file_path):
            file_path = os.path.join(PROJECT_ROOT, file_path)

        if not os.path.exists(file_path):
            logger.warning(f"修复文件不存在: {file_path}")
            return False

        fix_type = fix_result.get("fix_type", "code_fix")
        old_code = fix_result.get("old_code", "")
        new_code = fix_result.get("new_code", "")

        try:
            with open(file_path, 'r') as f:
                original_content = f.read()

            if fix_type == "code_fix" and old_code and new_code:
                if old_code not in original_content:
                    logger.warning(f"旧代码未在文件中找到: {file_path}")
                    return False
                updated_content = original_content.replace(old_code, new_code, 1)
            elif fix_type == "log_level":
                # 简单替换日志级别
                updated_content = original_content.replace(old_code, new_code, 1)
            else:
                logger.warning(f"未知修复类型: {fix_type}")
                return False

            with open(file_path, 'w') as f:
                f.write(updated_content)

            logger.info(f"✅ 修复已应用: {file_path} ({fix_type})")
            return True

        except Exception as e:
            logger.error(f"修复应用异常: {e}")
            return False

    def _commit_fix(self, fix_result: Dict, module_id: str):
        """自动 Git commit 修复"""
        try:
            summary = fix_result.get("summary", "auto fix")
            commit_msg = f"auto-fix: [{module_id}] {summary}\n\n根因: {fix_result.get('root_cause', 'N/A')}\n方案: {fix_result.get('rationale', 'N/A')}"

            result = subprocess.run(
                ["git", "add", "-A"],
                cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=30
            )
            result = subprocess.run(
                ["git", "commit", "-m", commit_msg],
                cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                logger.info(f"✅ Git commit: {commit_msg[:80]}")
            else:
                logger.warning(f"Git commit 失败: {result.stderr[:200]}")
        except Exception as e:
            logger.warning(f"Git commit 异常: {e}")

    def _check_dependencies(self, module_id: str, config: ModuleConfig) -> bool:
        """检查模块依赖是否满足"""
        for dep_id in config.depends_on:
            dep_state = self.memory.get_module_state(dep_id)
            dep_config = MODULE_REGISTRY.get(dep_id)
            if not dep_config:
                continue
            # 依赖模块必须成功运行过
            if isinstance(dep_state, dict):
                dep_status = dep_state.get("status", "idle")
            else:
                dep_status = dep_state.status
            if dep_status not in (ModuleStatus.RUNNING.value, ModuleStatus.COMPLETED.value):
                return False
        return True

    def _is_process_alive(self, pid: int) -> bool:
        """检查进程是否存在"""
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def _shutdown(self):
        """关闭时清理"""
        logger.info("🛑 系统大脑关闭...")

        for module_id, config in MODULE_REGISTRY.items():
            state = self.memory.get_module_state(module_id)
            if isinstance(state, dict):
                status = state.get("status", "idle")
            else:
                status = state.status
            if status == ModuleStatus.RUNNING.value:
                self.executor.stop_module(module_id, config)

        self.memory.save()
        logger.info("✅ 清理完成")

    def print_status(self):
        """打印当前状态"""
        self.memory._load()

        print(f"\n{'='*60}")
        print(f"🧠 Orchestrator Brain 状态")
        print(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   市场: {self.memory.state.market_phase}")
        print(f"   启动: {self.memory.state.started_at}")
        print(f"{'='*60}")

        print(f"\n{'模块':<14} {'状态':<12} {'失败':<6} {'最近决策'}")
        print(f"{'─'*14} {'─'*12} {'─'*6} {'─'*30}")

        for module_id, config in MODULE_REGISTRY.items():
            state = self.memory.get_module_state(module_id)
            # Handle both ModuleState objects and dicts (from JSON load)
            if isinstance(state, dict):
                status = state.get("status", "idle")
                failures = state.get("consecutive_failures", 0)
                decision = state.get("last_decision", "")
            else:
                status = state.status
                failures = state.consecutive_failures
                decision = state.last_decision
            emoji = {"running": "✅", "failed": "❌", "idle": "⚪"}.get(status, "❓")
            print(f"{emoji} {config.name:<12} {status:<12} {failures:<6} {decision[:30]}")

        # 最近告警
        recent_alerts = [a for a in self.memory.state.alerts[-5:]]
        if recent_alerts:
            print(f"\n【最近告警】")
            for a in recent_alerts:
                level_emoji = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}.get(a["level"], "❓")
                print(f"  {level_emoji} [{a['module']}] {a['message']}")

        # 最近决策
        recent_decisions = [d for d in self.memory.state.llm_decisions[-5:]]
        if recent_decisions:
            print(f"\n【最近决策】")
            for d in recent_decisions:
                print(f"  🤖 [{d['module']}] {d['action']}: {d['reason'][:40]}")

        print(f"\n{'='*60}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Orchestrator Brain")
    parser.add_argument("--status", action="store_true", help="查看大脑状态")
    args = parser.parse_args()

    if args.status:
        brain = OrchestratorBrain()
        brain.print_status()
        return

    brain = OrchestratorBrain()
    brain.run()


if __name__ == "__main__":
    main()
