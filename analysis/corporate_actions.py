#!/usr/bin/env python3
"""
US Data Hub — Corporate Actions Handler
处理拆股、分红、合并等公司行动对持仓和价格的影响。

P1 审计修复: 防止拆股/分红事件导致系统误判。

公司行动类型:
- stock_split: 拆股 (如 AAPL 4:1 split)
- reverse_split: 合股
- cash_dividend: 现金分红
- stock_dividend: 股票分红
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class CorporateAction:
    """表示一次公司行动"""
    def __init__(self, symbol: str, action_type: str, ratio: float, ex_date: str, **kwargs):
        self.symbol = symbol
        self.action_type = action_type
        self.ratio = ratio  # 拆股比例如 4.0 表示 1→4；合股如 0.1 表示 10→1
        self.ex_date = ex_date  # 除权除息日 YYYY-MM-DD
        self.per_share_amount = kwargs.get('per_share_amount', 0)  # 每股分红金额
        self.raw_data = kwargs


def detect_pending_actions(db) -> list:
    """检查数据库中是否有未处理的公司行动记录。
    
    从 market_indicators 表中查找类型为公司行动的记录。
    """
    try:
        rows = db.conn.execute("""
            SELECT date, indicator_name, indicator_value, indicator_meta
            FROM market_indicators 
            WHERE indicator_name IN ('stock_split', 'reverse_split', 'cash_dividend', 'stock_dividend')
              AND indicator_meta IS NOT NULL
            ORDER BY date DESC
        """).fetchall()
        
        actions = []
        for row in rows:
            try:
                import json
                meta = json.loads(row["indicator_meta"]) if row["indicator_meta"] else {}
                action = CorporateAction(
                    symbol=meta.get("symbol", ""),
                    action_type=row["indicator_name"],
                    ratio=float(meta.get("ratio", 1)),
                    ex_date=row["date"],
                    per_share_amount=meta.get("per_share_amount", 0),
                )
                actions.append(action)
            except Exception as e:
                logger.warning(f"Failed to parse corporate action record: {e}")
        
        return actions
    except Exception as e:
        logger.error(f"Failed to detect pending corporate actions: {e}")
        return []


def adjust_holding_for_split(db, symbol: str, split_ratio: float):
    """根据拆股比例调整持仓。
    
    Args:
        split_ratio: 拆股比例，如 4.0 表示 1股变4股
    """
    try:
        row = db.conn.execute(
            "SELECT quantity, cost_price FROM holdings WHERE symbol = ? AND active = 1",
            (symbol,)
        ).fetchone()
        
        if not row:
            logger.info(f"[{symbol}] No active holding, split adjustment skipped")
            return
        
        old_qty = row["quantity"]
        old_cost = row["cost_price"]
        new_qty = int(old_qty * split_ratio)
        new_cost = old_cost / split_ratio
        
        db.conn.execute("""
            UPDATE holdings 
            SET quantity = ?, cost_price = ?, last_synced = ?
            WHERE symbol = ? AND active = 1
        """, (new_qty, round(new_cost, 4), datetime.now(), symbol))
        db.conn.commit()
        
        logger.info(f"[{symbol}] 📊 Split adjustment: {split_ratio}:1 | "
                    f"qty {old_qty} → {new_qty}, cost ${old_cost:.2f} → ${new_cost:.2f}")
        
        # 记录审计日志
        db.conn.execute("""
            INSERT INTO monitor_alerts (timestamp, symbol, alert_type, severity, title, details)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(), symbol, "corporate_action", "info",
            f"{symbol} 拆股调整 {split_ratio}:1",
            f"qty: {old_qty}→{new_qty}, cost: ${old_cost:.2f}→${new_cost:.2f}"
        ))
        db.conn.commit()
        
    except Exception as e:
        logger.error(f"[{symbol}] Failed to adjust holding for split: {e}")
        db.conn.rollback()


def adjust_holding_for_dividend(db, symbol: str, per_share_amount: float):
    """处理现金分红 — 调整持仓成本价。
    
    Args:
        per_share_amount: 每股分红金额
    """
    try:
        row = db.conn.execute(
            "SELECT quantity, cost_price FROM holdings WHERE symbol = ? AND active = 1",
            (symbol,)
        ).fetchone()
        
        if not row:
            return
        
        total_dividend = row["quantity"] * per_share_amount
        new_cost = row["cost_price"] - per_share_amount  # 分红降低持仓成本
        
        if new_cost < 0:
            new_cost = 0.01  # 防止负成本
        
        db.conn.execute("""
            UPDATE holdings 
            SET cost_price = ?, last_synced = ?
            WHERE symbol = ? AND active = 1
        """, (round(new_cost, 4), datetime.now(), symbol))
        db.conn.commit()
        
        logger.info(f"[{symbol}] 💰 Dividend adjustment: ${per_share_amount:.4f}/share | "
                    f"total dividend ${total_dividend:.2f}, cost ${row['cost_price']:.2f} → ${new_cost:.2f}")
        
    except Exception as e:
        logger.error(f"[{symbol}] Failed to adjust holding for dividend: {e}")
        db.conn.rollback()


def adjust_prices_for_split(db, symbol: str, split_ratio: float):
    """调整历史价格数据以反映拆股。
    
    Args:
        split_ratio: 拆股比例
    """
    try:
        db.conn.execute("""
            UPDATE prices 
            SET open = open / ?, high = high / ?, low = low / ?, close = close / ?, adj_close = adj_close / ?
            WHERE symbol = ?
        """, (split_ratio, split_ratio, split_ratio, split_ratio, split_ratio, symbol))
        db.conn.commit()
        
        logger.info(f"[{symbol}] 📈 Price data adjusted for split {split_ratio}:1")
        
    except Exception as e:
        logger.error(f"[{symbol}] Failed to adjust prices for split: {e}")
        db.conn.rollback()


def process_corporate_actions(db):
    """主入口: 处理所有待处理的公司行动。
    
    应在以下时机调用:
    1. 系统启动时 (启动对账阶段)
    2. 每日数据采集完成后
    """
    logger.info("🏢 Checking for pending corporate actions...")
    
    actions = detect_pending_actions(db)
    
    if not actions:
        logger.info("✅ No pending corporate actions")
        return
    
    processed = 0
    for action in actions:
        symbol = action.symbol
        logger.info(f"Processing: {symbol} {action.action_type} ratio={action.ratio} ex_date={action.ex_date}")
        
        try:
            if action.action_type in ("stock_split", "reverse_split"):
                adjust_holding_for_split(db, symbol, action.ratio)
                adjust_prices_for_split(db, symbol, action.ratio)
            elif action.action_type == "cash_dividend":
                adjust_holding_for_dividend(db, symbol, action.per_share_amount)
            elif action.action_type == "stock_dividend":
                adjust_holding_for_split(db, symbol, 1 + action.ratio)
                adjust_prices_for_split(db, symbol, 1 + action.ratio)
            
            processed += 1
        except Exception as e:
            logger.error(f"Failed to process {symbol} {action.action_type}: {e}")
    
    logger.info(f"✅ Corporate actions processed: {processed}/{len(actions)}")
