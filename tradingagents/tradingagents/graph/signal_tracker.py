"""Signal quality tracker for TradingAgents.

Records each prediction and optionally compares against actual
outcomes to compute accuracy statistics.
"""

import json
from datetime import datetime
from pathlib import Path

from tradingagents.strategies.dynamic_thresholds import DynamicThresholds
from tradingagents.dataflows.config import get_config

try:
    from dayup_logger import log_decision, log_market
except ImportError:
    # Fallback when running outside us-data-hub context
    log_decision = None
    log_market = None


class SignalTracker:
    """Tracks prediction vs actual performance for signal quality analysis."""

    def __init__(self, results_dir: str):
        self.log_path = Path(results_dir) / "signal_quality_log.json"
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.dynamic_thresholds = DynamicThresholds(get_config())

    def record_prediction(self, ticker: str, trade_date: str,
                          direction: str, full_state: dict):
        """Record a prediction with its metadata.

        Args:
            ticker: Ticker symbol
            trade_date: Trading date (YYYY-MM-DD)
            direction: BUY / HOLD / SELL
            full_state: Complete graph state for reference
        """
        entry = {
            "ticker": ticker,
            "trade_date": trade_date,
            "prediction": {
                "direction": direction,
            },
            "investment_plan": full_state.get("investment_plan", ""),
            "trader_investment_plan": full_state.get("trader_investment_plan", ""),
            "final_trade_decision": full_state.get("final_trade_decision", ""),
            "contradiction_report": full_state.get("contradiction_report", ""),
            "timestamp": datetime.now().isoformat(),
            "actual_outcome": None,
        }

        records = self._load()
        records.append(entry)
        self._save(records)

        # 记录决策到 dayup
        if log_decision:
            signal_text = full_state.get("final_trade_decision", "")[:200]
            log_decision(
                decision_type=direction,
                content=f"{direction} {ticker} ({trade_date})",
                basis=signal_text or full_state.get("investment_plan", "")[:200],
                expected=f"待验证"
            )

    def record_actual(self, ticker: str, trade_date: str,
                      price_change_pct: float):
        """Update a past prediction with actual outcome.

        Args:
            ticker: Ticker symbol
            trade_date: Trading date that was predicted
            price_change_pct: Actual price change percentage after the trade date
        """
        records = self._load()
        for entry in records:
            if entry["ticker"] == ticker and entry["trade_date"] == trade_date:
                entry["actual_outcome"] = {
                    "price_change_pct": price_change_pct,
                    "recorded_at": datetime.now().isoformat(),
                }
                direction = entry["prediction"].get("direction", "").upper()
                entry["was_correct"] = self._evaluate_correctness(
                    direction, price_change_pct, ticker=ticker
                )

                # 记录信号验证到 dayup market
                if log_market:
                    verif = "✅正确" if entry["was_correct"] else "❌错误"
                    log_market(
                        indicator_type='signal_verification',
                        symbol=ticker,
                        value=f"{price_change_pct:+.2%}",
                        signal=direction,
                        strength=verif
                    )

                break
        self._save(records)

    def get_accuracy_stats(self, lookback_days: int = 30) -> dict:
        """Compute accuracy statistics over recent predictions.

        Args:
            lookback_days: Only consider predictions within this window (unused currently)

        Returns:
            dict with total_predictions, correct_predictions, accuracy
        """
        records = self._load()
        evaluated = [r for r in records if r.get("actual_outcome") is not None]
        if not evaluated:
            return {"total_predictions": 0, "correct_predictions": 0, "accuracy": None}

        correct = sum(1 for r in evaluated if r.get("was_correct", False))
        return {
            "total_predictions": len(evaluated),
            "correct_predictions": correct,
            "accuracy": round(correct / len(evaluated), 4),
        }

    def _load(self) -> list:
        if self.log_path.exists():
            try:
                with open(self.log_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return []
        return []

    def _save(self, records: list):
        with open(self.log_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)

    def _evaluate_correctness(self, direction: str, actual_pct: float, ticker: str = "") -> bool:
        """Evaluate if a prediction was correct based on actual outcome.

        Uses dynamic threshold per ticker when available.
        BUY is correct if price goes up (>threshold)
        SELL is correct if price goes down (<-threshold)
        HOLD is correct if price stays flat (within +/-threshold)

        Args:
            direction: BUY / HOLD / SELL
            actual_pct: Actual price change percentage
            ticker: Ticker symbol for dynamic threshold lookup
        """
        threshold = self.dynamic_thresholds.get_signal_threshold(ticker)
        if direction == "BUY" and actual_pct > threshold:
            return True
        if direction == "SELL" and actual_pct < -threshold:
            return True
        if direction == "HOLD" and abs(actual_pct) <= threshold:
            return True
        return False
