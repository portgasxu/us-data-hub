"""
llm_router.py — 双端点 LLM 并行调度器
=====================================

架构: CodingPlan (主力决策) + 百炼 (辅助分流)

功能:
  1. 按任务类型自动路由到正确端点+模型
  2. 双端点独立限流，互不影响
  3. 并行调度: 独立任务并发，依赖任务串行
  4. 自动降级: 失败 fallback + 跨端点降级
  5. 成本追踪: 记录每次请求的 token 消耗
"""

import asyncio
import time
import os
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from openai import AsyncOpenAI, OpenAI

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════
# 端点配置
# ═══════════════════════════════════════════════════════

CODING_PLAN_URL = os.getenv("CODING_PLAN_URL", "https://coding.dashscope.aliyuncs.com/v1")
CODING_PLAN_KEY = os.getenv("CODING_PLAN_KEY")
CODING_PLAN_MODEL = os.getenv("CODING_PLAN_MODEL", "qwen3.6-plus")

BAILIAN_URL = os.getenv("BAILIAN_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
BAILIAN_KEY = os.getenv("BAILIAN_KEY")

# ═══════════════════════════════════════════════════════
# 百炼可用模型配置（按模型优势配置）
# ═══════════════════════════════════════════════════════

BAILIAN_MODELS = {
    # ─── 旗舰推理 ───
    "qwen3-max": {
        "rpm_limit": 50,
        "max_concurrent": 5,
        "timeout": 120,
        "temperature": 0.3,
        "fallback": "qwen3.6-flash",
        "strength": "最强推理能力，适合复杂决策和多步骤分析",
    },
    # ─── 均衡全能（CodingPlan 主力模型，百炼仅作 fallback） ───
    "qwen3.6-plus": {
        "rpm_limit": 100,
        "max_concurrent": 8,
        "timeout": 90,
        "temperature": 0.3,
        "fallback": "qwen3.6-flash",
        "strength": "CodingPlan 主力模型，百炼仅作 fallback",
    },
    # ─── 快速轻量 ───
    "qwen3.6-flash": {
        "rpm_limit": 200,
        "max_concurrent": 15,
        "timeout": 30,
        "temperature": 0.2,
        "fallback": None,
        "strength": "简单任务速度快成本低，文本分类/情感分析",
    },
    # ─── 代码专用 ───
    "qwen3-coder-plus": {
        "rpm_limit": 50,
        "max_concurrent": 3,
        "timeout": 90,
        "temperature": 0.1,
        "fallback": "qwen3.6-flash",
        "strength": "代码理解/结构化解析，SEC/财报解析",
    },
    # ─── 深度推理 ───
    "deepseek-r1": {
        "rpm_limit": 30,
        "max_concurrent": 2,
        "timeout": 180,
        "temperature": 0.6,
        "fallback": "qwen3.6-flash",
        "strength": "思维链深度推理，复杂策略分析",
    },
    # ─── 数学/逻辑推理 ───
    "qwq-plus": {
        "rpm_limit": 30,
        "max_concurrent": 3,
        "timeout": 120,
        "temperature": 0.5,
        "fallback": "qwen3.6-flash",
        "strength": "数学计算/逻辑推导，阈值计算/回测分析",
    },
    # ─── 极速轻量 ───
    "qwen-turbo": {
        "rpm_limit": 300,
        "max_concurrent": 20,
        "timeout": 10,
        "temperature": 0.1,
        "fallback": "qwen3.6-flash",
        "strength": "最便宜最快，简单监控/异常检测/心跳检查",
    },
    # ─── 代码轻量 ───
    "qwen3-coder-flash": {
        "rpm_limit": 100,
        "max_concurrent": 5,
        "timeout": 60,
        "temperature": 0.1,
        "fallback": "qwen3.6-flash",
        "strength": "代码生成/脚本编写轻量级任务",
    },
}


# ═══════════════════════════════════════════════════════
# 任务路由表（基于模型优势拆分）
# ═══════════════════════════════════════════════════════
#
# 设计原则（最高优先级）：
# 1. CodingPlan 支持的模型（qwen3.6-plus）→ 优先走 CodingPlan 端点
# 2. CodingPlan 没有的模型 → 才走百炼端点
# 3. fallback: CodingPlan 失败 → 百炼同模型 → 百炼通用兜底
#

TASK_ROUTING = {
    # ─── CodingPlan 主力任务（所有 qwen3.6-plus 任务优先走 CodingPlan） ───
    "trading_agents_debate":  {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "trading_agents_judge":   {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "trading_agents_risk":    {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "dynamic_threshold":      {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "holding_monitor":        {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "weekly_review":          {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "morning_brief":          {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "portfolio_manager":      {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "screener_industry":      {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "earnings_analysis":      {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "risk_assessment":        {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "report_generation":      {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "strategy_validate":      {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "text_summarize":         {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},
    "news_classification":    {"endpoint": "coding_plan", "model": CODING_PLAN_MODEL},

    # ─── 百炼端点（仅 CodingPlan 不支持的模型） ───
    # 🔹 深度推理（思维链，CodingPlan 不支持 deepseek 系列）
    "strategy_deep_analysis": {"endpoint": "bailian", "model": "deepseek-r1"},
    "complex_reasoning":      {"endpoint": "bailian", "model": "deepseek-r1"},

    # 🔹 数学/逻辑推理（CodingPlan 不支持 qwq 系列）
    "threshold_calculation":  {"endpoint": "bailian", "model": "qwq-plus"},
    "backtest_analysis":      {"endpoint": "bailian", "model": "qwq-plus"},
    "math_validation":        {"endpoint": "bailian", "model": "qwq-plus"},

    # 🔹 代码/结构化解析（CodingPlan 不支持 coder 系列）
    "sec_parsing":            {"endpoint": "bailian", "model": "qwen3-coder-plus"},
    "code_generation":        {"endpoint": "bailian", "model": "qwen3-coder-plus"},
    "script_generation":      {"endpoint": "bailian", "model": "qwen3-coder-flash"},

    # 🔹 极速轻量（qwen-turbo，CodingPlan 不支持）
    "market_watcher":         {"endpoint": "bailian", "model": "qwen-turbo"},
    "anomaly_detection":      {"endpoint": "bailian", "model": "qwen-turbo"},
    "health_check":           {"endpoint": "bailian", "model": "qwen-turbo"},
    "system_monitor":         {"endpoint": "bailian", "model": "qwen-turbo"},
    "data_quality_check":     {"endpoint": "bailian", "model": "qwen-turbo"},

    # 🔹 旗舰推理（qwen3-max，CodingPlan 不支持）
    "critical_decision":      {"endpoint": "bailian", "model": "qwen3-max"},
    "final_judgment":         {"endpoint": "bailian", "model": "qwen3-max"},

    # 🔹 纯情感分析（百炼 flash 成本最低，CodingPlan 资源留给核心任务）
    "sentiment_analysis":     {"endpoint": "bailian", "model": "qwen3.6-flash"},
}


class RateLimiter:
    """单模型 RPM 限流器"""

    def __init__(self, rpm_limit: int):
        self.rpm_limit = rpm_limit
        self._timestamps = []

    def acquire_sync(self):
        """同步限流"""
        now = time.time()
        self._timestamps = [t for t in self._timestamps if now - t < 60]
        if len(self._timestamps) >= self.rpm_limit:
            wait = 60 - (now - self._timestamps[0])
            if wait > 0:
                time.sleep(wait + 0.1)
        self._timestamps.append(time.time())

    async def acquire(self):
        """异步限流"""
        now = time.time()
        self._timestamps = [t for t in self._timestamps if now - t < 60]
        if len(self._timestamps) >= self.rpm_limit:
            wait = 60 - (now - self._timestamps[0])
            if wait > 0:
                await asyncio.sleep(wait + 0.1)
        self._timestamps.append(time.time())


@dataclass
class LLMStats:
    """LLM 调用统计"""
    coding_plan_calls: int = 0
    coding_plan_tokens: int = 0
    coding_plan_errors: int = 0
    bailian_calls: int = 0
    bailian_tokens: int = 0
    bailian_errors: int = 0
    fallback_count: int = 0
    # 百炼分模型统计
    per_model: Dict[str, dict] = field(default_factory=dict)

    def record(self, endpoint: str, model: str, tokens: int, success: bool, is_fallback: bool = False):
        """记录一次调用"""
        if is_fallback:
            self.fallback_count += 1
        if endpoint == "coding_plan":
            self.coding_plan_calls += 1
            self.coding_plan_tokens += tokens
            if not success:
                self.coding_plan_errors += 1
        else:
            self.bailian_calls += 1
            self.bailian_tokens += tokens
            if not success:
                self.bailian_errors += 1
        if model:
            if model not in self.per_model:
                self.per_model[model] = {"calls": 0, "tokens": 0, "errors": 0}
            self.per_model[model]["calls"] += 1
            self.per_model[model]["tokens"] += tokens
            if not success:
                self.per_model[model]["errors"] += 1

    def to_dict(self) -> dict:
        return {
            "coding_plan": {
                "calls": self.coding_plan_calls,
                "tokens": self.coding_plan_tokens,
                "errors": self.coding_plan_errors,
            },
            "bailian": {
                "calls": self.bailian_calls,
                "tokens": self.bailian_tokens,
                "errors": self.bailian_errors,
                "per_model": self.per_model,
            },
            "fallback_count": self.fallback_count,
        }


class LLMRouter:
    """双端点 LLM 调度器"""

    def __init__(self):
        if not CODING_PLAN_KEY:
            logger.warning("CODING_PLAN_KEY not set, CodingPlan endpoint unavailable")
        if not BAILIAN_KEY:
            logger.warning("BAILIAN_KEY not set, Bailian endpoint unavailable")

        # CodingPlan 客户端
        self.cp_async = AsyncOpenAI(
            api_key=CODING_PLAN_KEY or "dummy",
            base_url=CODING_PLAN_URL,
            timeout=120,
        ) if CODING_PLAN_KEY else None
        self.cp_sync = OpenAI(
            api_key=CODING_PLAN_KEY or "dummy",
            base_url=CODING_PLAN_URL,
            timeout=120,
        ) if CODING_PLAN_KEY else None
        self.cp_semaphore = asyncio.Semaphore(8)
        self.cp_limiter = RateLimiter(100)

        # 百炼多模型客户端
        self.bl_async = {}
        self.bl_sync = {}
        self.bl_semaphores = {}
        self.bl_limiters = {}
        if BAILIAN_KEY:
            for m, cfg in BAILIAN_MODELS.items():
                self.bl_async[m] = AsyncOpenAI(
                    api_key=BAILIAN_KEY,
                    base_url=BAILIAN_URL,
                    timeout=cfg["timeout"],
                )
                self.bl_sync[m] = OpenAI(
                    api_key=BAILIAN_KEY,
                    base_url=BAILIAN_URL,
                    timeout=cfg["timeout"],
                )
                self.bl_semaphores[m] = asyncio.Semaphore(cfg["max_concurrent"])
                self.bl_limiters[m] = RateLimiter(cfg["rpm_limit"])

        self.stats = LLMStats()

    def _get_route(self, task_type: str) -> dict:
        """获取任务路由配置"""
        return TASK_ROUTING.get(task_type, {
            "endpoint": "bailian", "model": "qwen3.6-flash"
        })

    # ═══════════════════════════════════════════════
    # 同步调用
    # ═══════════════════════════════════════════════

    def invoke(self, task_type: str, messages: list, **kwargs) -> dict:
        """同步调用（crontab 脚本用）"""
        route = self._get_route(task_type)
        endpoint = route["endpoint"]
        model = route["model"]

        if endpoint == "coding_plan":
            return self._invoke_coding_plan_sync(messages, **kwargs)
        else:
            return self._invoke_bailian_sync(model, messages, **kwargs)

    def _invoke_coding_plan_sync(self, messages: list, **kwargs) -> dict:
        """CodingPlan 同步调用"""
        if not self.cp_sync:
            logger.error("CodingPlan client not initialized")
            return {"error": "CodingPlan not configured", "success": False}

        self.cp_limiter.acquire_sync()
        try:
            resp = self.cp_sync.chat.completions.create(
                model=CODING_PLAN_MODEL,
                messages=messages,
                temperature=kwargs.get("temperature", 0.3),
                max_tokens=kwargs.get("max_tokens"),
            )
            tokens = resp.usage.total_tokens if resp.usage else 0
            self.stats.record("coding_plan", CODING_PLAN_MODEL, tokens, True)
            return {
                "content": resp.choices[0].message.content,
                "tokens": tokens,
                "model": CODING_PLAN_MODEL,
                "endpoint": "coding_plan",
                "success": True,
            }
        except Exception as e:
            self.stats.record("coding_plan", CODING_PLAN_MODEL, 0, False, is_fallback=True)
            logger.error(f"CodingPlan failed: {e}")
            # fallback: 百炼 qwen3.6-plus → qwen3.6-flash
            result = self._invoke_bailian_sync("qwen3.6-plus", messages, **kwargs)
            if not result.get("success"):
                return self._invoke_bailian_sync("qwen3.6-flash", messages, **kwargs)
            return result

    def _invoke_bailian_sync(self, model: str, messages: list, **kwargs) -> dict:
        """百炼同步调用"""
        if not self.bl_sync.get(model):
            logger.error(f"Bailian client for {model} not initialized")
            return {"error": f"Bailian {model} not configured", "success": False}

        cfg = BAILIAN_MODELS.get(model, BAILIAN_MODELS["qwen3.6-flash"])
        self.bl_limiters[model].acquire_sync()

        try:
            resp = self.bl_sync[model].chat.completions.create(
                model=model,
                messages=messages,
                temperature=kwargs.get("temperature", cfg["temperature"]),
            )
            tokens = resp.usage.total_tokens if resp.usage else 0
            self.stats.record("bailian", model, tokens, True)
            return {
                "content": resp.choices[0].message.content,
                "tokens": tokens,
                "model": model,
                "endpoint": "bailian",
                "success": True,
            }
        except Exception as e:
            self.stats.record("bailian", model, 0, False)
            logger.error(f"Bailian {model} failed: {e}")
            fallback = cfg.get("fallback")
            if fallback:
                self.stats.record("bailian", model, 0, False, is_fallback=True)
                return self._invoke_bailian_sync(fallback, messages, **kwargs)
            return {"error": str(e), "success": False, "endpoint": "bailian"}

    # ═══════════════════════════════════════════════
    # 异步调用
    # ═══════════════════════════════════════════════

    async def invoke_async(self, task_type: str, messages: list, **kwargs) -> dict:
        """异步调用"""
        route = self._get_route(task_type)
        if route["endpoint"] == "coding_plan":
            return await self._invoke_coding_plan_async(messages, **kwargs)
        else:
            return await self._invoke_bailian_async(route["model"], messages, **kwargs)

    async def _invoke_coding_plan_async(self, messages: list, **kwargs) -> dict:
        if not self.cp_async:
            return {"error": "CodingPlan not configured", "success": False}

        async with self.cp_semaphore:
            await self.cp_limiter.acquire()
            try:
                resp = await self.cp_async.chat.completions.create(
                    model=CODING_PLAN_MODEL,
                    messages=messages,
                    temperature=kwargs.get("temperature", 0.3),
                )
                tokens = resp.usage.total_tokens if resp.usage else 0
                self.stats.record("coding_plan", CODING_PLAN_MODEL, tokens, True)
                return {
                    "content": resp.choices[0].message.content,
                    "tokens": tokens,
                    "model": CODING_PLAN_MODEL,
                    "endpoint": "coding_plan",
                    "success": True,
                }
            except Exception as e:
                self.stats.record("coding_plan", CODING_PLAN_MODEL, 0, False, is_fallback=True)
                # fallback: 百炼 qwen3.6-plus → qwen3.6-flash
                result = await self._invoke_bailian_async("qwen3.6-plus", messages, **kwargs)
                if not result.get("success"):
                    return await self._invoke_bailian_async("qwen3.6-flash", messages, **kwargs)
                return result

    async def _invoke_bailian_async(self, model: str, messages: list, **kwargs) -> dict:
        if not self.bl_async.get(model):
            return {"error": f"Bailian {model} not configured", "success": False}

        cfg = BAILIAN_MODELS.get(model, BAILIAN_MODELS["qwen3.6-flash"])
        async with self.bl_semaphores[model]:
            await self.bl_limiters[model].acquire()
            try:
                resp = await self.bl_async[model].chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=kwargs.get("temperature", cfg["temperature"]),
                )
                tokens = resp.usage.total_tokens if resp.usage else 0
                self.stats.record("bailian", model, tokens, True)
                return {
                    "content": resp.choices[0].message.content,
                    "tokens": tokens,
                    "model": model,
                    "endpoint": "bailian",
                    "success": True,
                }
            except Exception as e:
                self.stats.record("bailian", model, 0, False)
                fallback = cfg.get("fallback")
                if fallback:
                    self.stats.record("bailian", model, 0, False, is_fallback=True)
                    return await self._invoke_bailian_async(fallback, messages, **kwargs)
                return {"error": str(e), "success": False}

    # ═══════════════════════════════════════════════
    # 并行调用
    # ═══════════════════════════════════════════════

    async def parallel_invoke(self, tasks: dict) -> dict:
        """
        并行调用多个独立任务

        tasks = {
            "AAPL_ta":     ("trading_agents_debate", [messages]),
            "NVDA_ta":     ("trading_agents_debate", [messages]),
            "sentiment":   ("sentiment_analysis", [messages]),
            "sec_parse":   ("sec_parsing", [messages]),
        }

        → CodingPlan: AAPL_ta + NVDA_ta (并行)
        → 百炼:       sentiment (flash) + sec_parse (coder) (并行)
        """
        coros = {}
        for task_name, (task_type, messages) in tasks.items():
            coros[task_name] = self.invoke_async(task_type, messages)

        results_list = await asyncio.gather(*coros.values(), return_exceptions=True)

        output = {}
        for (task_name, _), result in zip(coros.items(), results_list):
            if isinstance(result, Exception):
                output[task_name] = {"error": str(result), "success": False}
            else:
                output[task_name] = result

        return output

    def get_stats(self) -> dict:
        """获取统计信息"""
        return self.stats.to_dict()

    @staticmethod
    def get_model_info() -> dict:
        """获取所有模型的能力说明"""
        info = {}
        for name, cfg in BAILIAN_MODELS.items():
            info[name] = {
                "strength": cfg.get("strength", ""),
                "rpm_limit": cfg["rpm_limit"],
                "max_concurrent": cfg["max_concurrent"],
                "temperature": cfg["temperature"],
                "fallback": cfg.get("fallback"),
            }
        return info

    @staticmethod
    def get_routing_map() -> dict:
        """获取任务路由映射表"""
        return {k: {"endpoint": v["endpoint"], "model": v["model"]}
                for k, v in TASK_ROUTING.items()}
