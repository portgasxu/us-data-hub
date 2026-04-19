"""
US Data Hub — 策略反馈闭环 (Phase 3)
=====================================

Phase 3 新增:
  - 验证结果写回 screener_config.json，实现策略自优化
  - 根据胜率动态调整选股权重和阈值
  - 记录策略演变历史

闭环: validate_strategy → feedback_loop → screener_config → screener → 新信号
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# 配置文件路径
SCREENER_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "config", "screener_config.json"
)

# 反馈历史
FEEDBACK_LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dayup", "performance", "feedback_history.jsonl"
)


class FeedbackLoop:
    """策略反馈闭环 — 将验证结果写回选股配置。"""

    def __init__(self, config_path: str = None):
        self.config_path = config_path or SCREENER_CONFIG_PATH
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """加载选股配置"""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data
            except Exception as e:
                logger.warning(f"Failed to load screener config: {e}")
        return self._default_config()

    def _default_config(self) -> dict:
        """默认选股配置"""
        return {
            "version": "1.0",
            "last_updated": None,
            "screener_weights": {
                "momentum": 0.3,
                "value": 0.2,
                "growth": 0.2,
                "quality": 0.15,
                "sentiment": 0.15,
            },
            "dynamic_thresholds": {
                "min_confidence": 0.6,
                "min_volume": 1000000,
                "min_market_cap": 1e9,
            },
            "hot_industries": ["Technology", "Healthcare", "Consumer Cyclical"],
            "excluded_industries": ["Real Estate", "Utilities"],
            "optimization_history": [],
        }

    def _update_nested(self, section: str, key: str, value):
        """更新嵌套配置"""
        if section not in self.config:
            self.config[section] = {}
        if not isinstance(self.config[section], dict):
            self.config[section] = {}
        self.config[section][key] = value

    def apply_feedback(self, validation_result: dict, label: str = "auto"):
        """
        将策略验证结果应用到选股配置。

        Args:
            validation_result: validate_strategy.py 的输出
            label: 验证类型标签
        """
        win_rate = validation_result.get("win_rate", 0)
        total_trades = validation_result.get("total_trades", 0)
        best_symbol = validation_result.get("best_symbol", "")
        worst_symbol = validation_result.get("worst_symbol", "")
        sharpe = validation_result.get("sharpe_ratio", 0)
        max_dd = validation_result.get("max_drawdown", 0)

        feedback = {
            "timestamp": datetime.now().isoformat(),
            "label": label,
            "win_rate": win_rate,
            "total_trades": total_trades,
            "best_symbol": best_symbol,
            "worst_symbol": worst_symbol,
            "sharpe": sharpe,
            "max_drawdown": max_dd,
            "changes": [],
        }

        # ─── 规则 1: 胜率低于阈值 → 降低选股权重 ───
        if win_rate < 0.45:
            old_min_conf = self.config.get("dynamic_thresholds", {}).get("min_confidence", 0.6)
            new_min_conf = min(0.8, old_min_conf + 0.05)
            self._update_nested("dynamic_thresholds", "min_confidence", new_min_conf)
            feedback["changes"].append(
                f"min_confidence: {old_min_conf:.2f} → {new_min_conf:.2f} (胜率 {win_rate:.0%} 过低)"
            )

        # ─── 规则 2: 胜率高于阈值 → 放宽选股条件 ───
        elif win_rate > 0.65:
            old_min_conf = self.config.get("dynamic_thresholds", {}).get("min_confidence", 0.6)
            new_min_conf = max(0.5, old_min_conf - 0.05)
            self._update_nested("dynamic_thresholds", "min_confidence", new_min_conf)
            feedback["changes"].append(
                f"min_confidence: {old_min_conf:.2f} → {new_min_conf:.2f} (胜率 {win_rate:.0%} 优秀)"
            )

        # ─── 规则 3: Sharpe > 1.5 → 增加 momentum 权重 ───
        if sharpe > 1.5:
            weights = self.config.get("screener_weights", {})
            if not isinstance(weights, dict):
                weights = {}
            old_mom = weights.get("momentum", 0.3)
            new_mom = min(0.5, old_mom + 0.05)
            weights["momentum"] = new_mom
            # 相应降低其他权重
            for k in ["value", "growth"]:
                if k in weights:
                    weights[k] = max(0.1, weights[k] - 0.025)
            self.config["screener_weights"] = weights
            feedback["changes"].append(
                f"momentum weight: {old_mom:.2f} → {new_mom:.2f} (Sharpe {sharpe:.1f})"
            )

        # ─── 规则 4: 最大回撤过高 → 降低风险偏好 ───
        if max_dd > 0.15:
            weights = self.config.get("screener_weights", {})
            if not isinstance(weights, dict):
                weights = {}
            old_quality = weights.get("quality", 0.15)
            new_quality = min(0.3, old_quality + 0.05)
            weights["quality"] = new_quality
            self.config["screener_weights"] = weights
            feedback["changes"].append(
                f"quality weight: {old_quality:.2f} → {new_quality:.2f} (回撤 {max_dd:.0%})"
            )

        # ─── 规则 5: 记录最佳/最差标的 ───
        if best_symbol:
            best_list = self.config.get("best_performers", [])
            if not isinstance(best_list, list):
                best_list = []
            if best_symbol not in best_list:
                best_list.append(best_symbol)
                self.config["best_performers"] = best_list[-10:]  # 保留最近 10 个
                feedback["changes"].append(f"best_performer added: {best_symbol}")

        # 保存配置
        self.config["last_updated"] = datetime.now().isoformat()
        if "optimization_history" not in self.config:
            self.config["optimization_history"] = []
        self.config["optimization_history"].append(feedback)

        self._save_config()
        self._log_feedback(feedback)

        logger.info(f"🔄 反馈闭环完成: {len(feedback['changes'])} 项配置调整")
        for change in feedback["changes"]:
            logger.info(f"  → {change}")

        return feedback

    def _save_config(self):
        """保存配置到文件"""
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w') as f:
            json.dump(self.config, f, indent=2, ensure_ascii=False)
        logger.info(f"💾 Screener config saved: {self.config_path}")

    def _log_feedback(self, feedback: dict):
        """追加反馈到历史日志"""
        os.makedirs(os.path.dirname(FEEDBACK_LOG_PATH), exist_ok=True)
        with open(FEEDBACK_LOG_PATH, 'a') as f:
            f.write(json.dumps(feedback, ensure_ascii=False) + "\n")

    def get_history(self, limit: int = 10) -> list:
        """获取最近 N 条反馈历史"""
        if os.path.exists(FEEDBACK_LOG_PATH):
            with open(FEEDBACK_LOG_PATH, 'r') as f:
                lines = f.readlines()
            return [json.loads(l) for l in lines[-limit:]]
        return []

    def get_current_config(self) -> dict:
        """获取当前选股配置"""
        return self.config


def apply_validation_feedback(validation_result: dict, label: str = "auto"):
    """便捷函数：直接应用验证结果到选股配置。"""
    loop = FeedbackLoop()
    return loop.apply_feedback(validation_result, label)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="策略反馈闭环")
    parser.add_argument("--show-config", action="store_true", help="显示当前配置")
    parser.add_argument("--history", type=int, default=0, help="显示最近 N 条历史")
    args = parser.parse_args()

    loop = FeedbackLoop()

    if args.show_config:
        print(json.dumps(loop.get_current_config(), indent=2, ensure_ascii=False))
    elif args.history > 0:
        for entry in loop.get_history(args.history):
            print(json.dumps(entry, indent=2, ensure_ascii=False))
            print("---")
    else:
        print("用法: python feedback_loop.py --show-config 或 --history 5")
