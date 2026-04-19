"""
Smart Executor — 智能订单执行器 (v5.3)

限价单默认 + 滑点控制 + 订单超时重试 + 部分成交处理
"""

import logging
import time
from datetime import datetime

logger = logging.getLogger("smart_executor")

# ─── 配置 ─────────────────────────────────────────────
SLIPPAGE_TOLERANCE = 0.005  # 滑点容忍 0.5%
ORDER_TIMEOUT = 60          # 订单超时 60 秒
RETRY_INTERVAL = 10         # 重试间隔 10 秒
MAX_RETRIES = 3             # 最大重试次数
LIMIT_ORDER_BUFFER = 0.001  # 限价单缓冲 0.1%（买入加价，卖出减价）


class SmartOrder:
    """智能订单。"""
    def __init__(self, symbol: str, direction: str, quantity: int,
                 reference_price: float, order_type: str = "limit"):
        self.symbol = symbol
        self.direction = direction
        self.quantity = quantity
        self.reference_price = reference_price
        self.order_type = order_type
        self.limit_price = self._calc_limit_price()
        self.status = "pending"  # pending | submitted | filled | partial | cancelled | expired
        self.filled_quantity = 0
        self.filled_price = 0
        self.submitted_at = None
        self.updated_at = None
        self.retry_count = 0
        self.history = []

    def _calc_limit_price(self) -> float:
        """计算限价。买入略高于参考价，卖出略低于参考价。"""
        if self.direction == "buy":
            return round(self.reference_price * (1 + LIMIT_ORDER_BUFFER), 2)
        else:
            return round(self.reference_price * (1 - LIMIT_ORDER_BUFFER), 2)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "quantity": self.quantity,
            "order_type": self.order_type,
            "limit_price": self.limit_price,
            "reference_price": self.reference_price,
            "status": self.status,
            "filled_quantity": self.filled_quantity,
            "filled_price": self.filled_price,
            "retry_count": self.retry_count,
        }


def check_slippage(executed_price: float, reference_price: float) -> tuple[bool, float]:
    """检查滑点是否在容忍范围内。返回 (is_within_tolerance, slippage_pct)"""
    if reference_price == 0:
        return False, 0.0

    slippage = abs(executed_price - reference_price) / reference_price
    return slippage <= SLIPPAGE_TOLERANCE, slippage


def execute_smart_order(executor, order: SmartOrder) -> dict:
    """
    执行智能订单。

    流程：
    1. 提交限价单
    2. 轮询订单状态（最多 ORDER_TIMEOUT 秒）
    3. 检查滑点
    4. 如未成交，调整价格重试（最多 MAX_RETRIES 次）
    5. 处理部分成交

    返回: 执行结果字典
    """
    result = {
        "symbol": order.symbol,
        "direction": order.direction,
        "quantity": order.quantity,
        "order_type": order.order_type,
        "limit_price": order.limit_price,
        "reference_price": order.reference_price,
        "status": "pending",
        "filled_quantity": 0,
        "filled_price": 0,
        "slippage_pct": 0,
        "retries": 0,
        "message": "",
    }

    # 尝试执行
    for attempt in range(MAX_RETRIES + 1):
        order.retry_count = attempt
        order.submitted_at = datetime.now()

        logger.info(f"[{order.symbol}] Order attempt {attempt + 1}/{MAX_RETRIES + 1}: "
                    f"{order.direction} {order.quantity} @ limit ${order.limit_price:.2f}")

        # 提交订单
        try:
            order_result = _submit_order(executor, order)
        except Exception as e:
            logger.error(f"[{order.symbol}] Order submission failed: {e}")
            result["message"] = f"提交失败: {e}"
            order.status = "cancelled"
            return result

        # 轮询订单状态
        poll_result = _poll_order_status(executor, order_result, timeout=ORDER_TIMEOUT)

        if poll_result["status"] == "filled":
            # 检查滑点
            exec_price = poll_result.get("executed_price", order.limit_price)
            within_tol, slippage = check_slippage(exec_price, order.reference_price)

            if not within_tol:
                logger.warning(f"[{order.symbol}] Slippage {slippage:.2%} exceeds tolerance {SLIPPAGE_TOLERANCE:.2%}")
                result["message"] = f"滑点 {slippage:.2%} 超过容忍 {SLIPPAGE_TOLERANCE:.2%}"
                result["status"] = "slippage_exceeded"
                result["slippage_pct"] = slippage
                return result

            # 成交成功
            result["status"] = "filled"
            result["filled_quantity"] = poll_result.get("filled_quantity", order.quantity)
            result["filled_price"] = exec_price
            result["slippage_pct"] = slippage
            result["message"] = "成交成功"
            order.status = "filled"
            order.filled_quantity = result["filled_quantity"]
            order.filled_price = exec_price
            return result

        elif poll_result["status"] == "partial":
            # 部分成交
            result["status"] = "partial"
            result["filled_quantity"] = poll_result.get("filled_quantity", 0)
            result["filled_price"] = poll_result.get("executed_price", 0)
            result["message"] = f"部分成交 {result['filled_quantity']}/{order.quantity}"
            order.status = "partial"
            order.filled_quantity = result["filled_quantity"]
            order.filled_price = result["filled_price"]
            return result

        elif poll_result["status"] == "timeout":
            # 超时未成交，调整价格重试
            if attempt < MAX_RETRIES:
                order.limit_price = _adjust_limit_price(order, direction="more_aggressive")
                logger.info(f"[{order.symbol}] Timeout, adjusting limit to ${order.limit_price:.2f}")
                result["retries"] = attempt + 1
                time.sleep(RETRY_INTERVAL)
            else:
                result["status"] = "expired"
                result["message"] = f"超时，已重试 {MAX_RETRIES} 次"
                order.status = "expired"
                return result

    return result


def _submit_order(executor, order: SmartOrder) -> dict:
    """提交订单到券商。"""
    if order.direction == "buy":
        return executor.buy(order.symbol, order.quantity, order.limit_price)
    else:
        return executor.sell(order.symbol, order.quantity, order.limit_price)


def _poll_order_status(executor, order_result: dict, timeout: int = ORDER_TIMEOUT) -> dict:
    """轮询订单状态。"""
    start = time.time()
    order_id = order_result.get("order_id")

    while time.time() - start < timeout:
        try:
            status = executor.get_order_status(order_id) if order_id else None

            if status:
                if status.get("status") == "filled":
                    return {
                        "status": "filled",
                        "filled_quantity": status.get("filled_quantity", 0),
                        "executed_price": status.get("executed_price", 0),
                    }
                elif status.get("status") == "partial":
                    return {
                        "status": "partial",
                        "filled_quantity": status.get("filled_quantity", 0),
                        "executed_price": status.get("executed_price", 0),
                    }
                elif status.get("status") == "cancelled":
                    return {"status": "cancelled"}

        except Exception as e:
            logger.debug(f"Poll error: {e}")

        time.sleep(2)  # 每 2 秒轮询一次

    return {"status": "timeout"}


def _adjust_limit_price(order: SmartOrder, direction: str = "more_aggressive") -> float:
    """调整限价。more_aggressive = 更有利于成交（买入加价，卖出减价）。"""
    adjustment = 0.002  # 每次调整 0.2%

    if direction == "more_aggressive":
        if order.direction == "buy":
            return round(order.limit_price * (1 + adjustment), 2)
        else:
            return round(order.limit_price * (1 - adjustment), 2)
    else:
        if order.direction == "buy":
            return round(order.limit_price * (1 - adjustment), 2)
        else:
            return round(order.limit_price * (1 + adjustment), 2)
