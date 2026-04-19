"""
US Data Hub — Position Manager
Manages portfolio holdings synced from Longbridge.

Features:
  - Sync positions from Longbridge CLI
  - Track buy/sell history
  - Calculate P&L
  - Generate holding analysis reports

Usage:
    python scripts/position_manager.py sync           # Sync from Longbridge
    python scripts/position_manager.py list            # List current holdings
    python scripts/position_manager.py pnl             # Show P&L
    python scripts/position_manager.py report          # Full holding report
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import Database
from executors.longbridge import LongbridgeExecutor

from dayup_logger import setup_root_logger, log_position, log_trade, log_risk, log_performance
setup_root_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


class PositionManager:
    """Manages portfolio holdings."""

    def __init__(self, db: Database, executor: LongbridgeExecutor = None):
        self.db = db
        self.executor = executor or LongbridgeExecutor()

    def _get_current_price(self, symbol: str) -> float:
        """Get latest price with fallback to cost basis."""
        prices = self.db.query_prices(symbol, days=5)
        if prices:
            return prices[0]['close']
        # Fallback: check holdings for cost_price
        row = self.db.conn.execute(
            "SELECT cost_price FROM holdings WHERE symbol = ?", (symbol,)
        ).fetchone()
        if row and row['cost_price'] > 0:
            return row['cost_price']
        return 0.0

    def _calculate_cost_from_trades(self, symbol: str) -> float:
        """从本地交易记录计算持仓成本价（加权平均法）。
        
        比券商返回的成本价更可靠，因为券商可能使用不同的会计方法
        或返回过时数据。
        """
        rows = self.db.conn.execute("""
            SELECT direction, quantity, price FROM trades 
            WHERE symbol = ? AND quantity > 0 AND price > 0
            ORDER BY timestamp ASC
        """, (symbol,)).fetchall()
        
        if not rows:
            return 0.0
        
        total_cost = 0.0
        total_qty = 0
        
        for direction, qty, price in rows:
            if direction.lower() == 'buy':
                total_cost += qty * price
                total_qty += qty
            elif direction.lower() == 'sell':
                # 卖出时按比例减少总成本，但不改变剩余股份的单位成本
                if total_qty > 0:
                    sell_ratio = qty / total_qty
                    total_cost *= (1 - sell_ratio)
                    total_qty -= qty
        
        return total_cost / total_qty if total_qty > 0 else 0.0

    def sync_from_broker(self) -> int:
        """
        Sync current positions from Longbridge broker.
        Handles stocks AND options. Detects closed positions and marks them inactive.

        Returns number of positions synced.
        """
        positions = self.executor.get_positions()

        # Get symbols currently reported by broker (both stocks and options)
        broker_symbols = set()
        if positions:
            for pos in positions:
                symbol_raw = pos.get('symbol', '')
                # Keep full symbol for options (e.g. BABA270115C185000), strip .US suffix for stocks
                if '.' in symbol_raw:
                    base = symbol_raw.split('.')[0].upper()
                    # Options have long symbols (typically > 10 chars with digits)
                    if any(c.isdigit() for c in base) and len(base) > 8:
                        broker_symbols.add(base)  # Keep full option symbol
                    else:
                        broker_symbols.add(base)  # Stock symbol
                else:
                    broker_symbols.add(symbol_raw.upper())

        # Get all currently active symbols in local DB
        local_active = set()
        for row in self.db.conn.execute(
            "SELECT symbol FROM holdings WHERE active = 1"
        ).fetchall():
            local_active.add(row['symbol'])

        # Detect closed positions: in local DB but NOT in broker
        closed_symbols = local_active - broker_symbols
        if closed_symbols:
            logger.info(f"🔍 Detected {len(closed_symbols)} closed position(s): {', '.join(sorted(closed_symbols))}")
            for sym in closed_symbols:
                self.db.conn.execute(
                    "UPDATE holdings SET active = 0 WHERE symbol = ?",
                    (sym,)
                )
                log_trade(
                    symbol=sym, action='SYNC_CLOSE', price=0, quantity=0,
                    note="自动检测：券商已无此持仓，标记为 inactive"
                )
                logger.info(f"  ✅ Marked {sym} as closed (no longer in broker)")

        if not positions:
            # Broker returned empty — ALL local positions should be closed
            if local_active:
                logger.info(f"Broker returned empty, closing all {len(local_active)} local positions")
                self.db.conn.execute("UPDATE holdings SET active = 0 WHERE active = 1")
                for sym in local_active:
                    log_trade(
                        symbol=sym, action='SYNC_CLOSE', price=0, quantity=0,
                        note="券商空仓，全部标记为 inactive"
                    )
            else:
                logger.info("No positions in broker, no local positions to close")
            self.db.conn.commit()
            return 0

        synced = 0
        total_cost = 0

        # ── Pass 1: upsert positions and compute total value ──
        position_values = {}  # symbol -> (quantity, cost_price, name, current_price, market_value)
        for pos in positions:
            symbol_raw = pos.get('symbol', '')
            symbol = symbol_raw.split('.')[0].upper() if '.' in symbol_raw else symbol_raw.upper()
            quantity = int(pos.get('quantity', 0))
            cost_price = float(pos.get('cost_price', 0))
            available = int(pos.get('available', 0))
            name = pos.get('name', '')

            # Handle quantity states:
            #   positive  = long position → active
            #   zero      = closed → inactive
            #   negative  = short position → active (needs cover)
            active_flag = 1 if quantity != 0 else 0

            # Use our own trade history to calculate cost basis (more reliable than broker)
            # Broker cost can be wrong (e.g., NVDA returned $5.56 instead of ~$200)
            local_cost = self._calculate_cost_from_trades(symbol)
            
            # Only fall back to broker cost if we have no local trade data
            effective_cost = local_cost if local_cost > 0 else cost_price

            # Upsert: update quantity from broker, use our calculated cost
            self.db.conn.execute("""
                INSERT INTO holdings (symbol, company_name, quantity, cost_price, available, last_synced)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    quantity=excluded.quantity,
                    cost_price=excluded.cost_price,
                    available=excluded.available,
                    last_synced=excluded.last_synced,
                    active=?
            """, (symbol, name, quantity, effective_cost, available, datetime.now(), active_flag))

            # Log if this is a short position (negative qty) that needs attention
            if quantity < 0:
                logger.warning(f"  ⚠️ SHORT POSITION: {symbol} qty={quantity} — needs to be covered")
                log_risk(
                    risk_type='空头持仓',
                    trigger=f'{symbol} 出现做空头寸 (qty={quantity})',
                    current=f'{quantity} shares',
                    threshold='应为 ≥ 0',
                    action='建议平空'
                )

            synced += 1
            total_cost += quantity * effective_cost

            # Fetch current price for value calculation
            current_price = self._get_current_price(symbol)
            market_value = quantity * current_price if current_price > 0 else quantity * effective_cost
            position_values[symbol] = (quantity, effective_cost, name, current_price, market_value)

        self.db.conn.commit()

        total_portfolio_value = sum(v[4] for v in position_values.values())

        # ── Pass 2: log positions with correct weights ──
        for symbol, (quantity, cost_price, name, current_price, market_value) in position_values.items():
            weight = market_value / total_portfolio_value if total_portfolio_value > 0 else 0
            log_position(
                symbol=symbol, quantity=quantity,
                cost_price=cost_price, current_price=current_price if current_price > 0 else None,
                holding_days=0, weight=weight
            )
            logger.info(f"  Synced: {symbol} {name} — {quantity} shares @ ${cost_price:.2f} (current: ${current_price:.2f}, weight={weight:.1%})")

        logger.info(f"✅ Synced {synced} positions from broker, total value=${total_portfolio_value:,.2f}")

        # 风控: 检查单票集中度
        if total_portfolio_value > 0:
            for symbol, (quantity, cost_price, name, current_price, market_value) in position_values.items():
                weight = market_value / total_portfolio_value
                if weight > 0.35:
                    log_risk(
                        risk_type='仓位集中度',
                        trigger=f'{symbol} 仓位占比过高',
                        current=f'{weight:.1%}',
                        threshold='35%',
                        action='建议关注'
                    )
                    logger.warning(f"  ⚠️ {symbol} weight {weight:.1%} > 35% threshold")

        return synced

    def get_holdings(self) -> List[Dict]:
        """Get all active holdings."""
        rows = self.db.conn.execute("""
            SELECT h.*,
                   p.close as current_price,
                   CASE WHEN p.close IS NOT NULL AND h.cost_price > 0
                        THEN (p.close - h.cost_price) / h.cost_price * 100
                        ELSE 0 END as pnl_pct,
                   CASE WHEN p.close IS NOT NULL
                        THEN (p.close - h.cost_price) * h.quantity
                        ELSE 0 END as pnl_amount
            FROM holdings h
            LEFT JOIN (
                SELECT symbol, close FROM prices
                WHERE (symbol, date) IN (
                    SELECT symbol, MAX(date) FROM prices GROUP BY symbol
                )
            ) p ON h.symbol = p.symbol
            WHERE h.active = 1
            ORDER BY h.symbol
        """).fetchall()
        return [dict(r) for r in rows]

    def add_holding(self, symbol: str, quantity: int, price: float,
                    company_name: str = "", date: str = None):
        """Manually add a holding record."""
        self.db.conn.execute("""
            INSERT INTO holdings (symbol, company_name, quantity, cost_price, available, last_synced)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                quantity=quantity+excluded.quantity,
                cost_price=(cost_price*quantity + excluded.cost_price*excluded.quantity)/(quantity+excluded.quantity),
                available=available+excluded.available,
                last_synced=excluded.last_synced
        """, (symbol.upper(), company_name, quantity, price, quantity,
              date or datetime.now().strftime('%Y-%m-%d')))
        self.db.conn.commit()
        logger.info(f"Added holding: {symbol} {quantity} shares @ ${price}")
        log_trade(
            symbol=symbol.upper(), action='BUY', price=price,
            quantity=quantity, note="MANUAL_ADD"
        )

    def get_quantity(self, symbol: str) -> int:
        """Get current holding quantity for a symbol."""
        row = self.db.conn.execute(
            "SELECT quantity FROM holdings WHERE symbol = ? AND active = 1",
            (symbol.upper(),)
        ).fetchone()
        return row['quantity'] if row else 0

    def remove_holding(self, symbol: str, quantity: int, price: float, date: str = None):
        """Record a sale (reduce holding)."""
        symbol = symbol.upper()
        row = self.db.conn.execute(
            "SELECT quantity, cost_price FROM holdings WHERE symbol = ? AND active = 1",
            (symbol,)
        ).fetchone()

        if not row:
            logger.warning(f"Holding {symbol} not found")
            return

        old_qty = row['quantity']
        old_cost = row['cost_price']
        new_qty = old_qty - quantity

        if new_qty <= 0:
            # Fully sold
            self.db.conn.execute(
                "UPDATE holdings SET active = 0 WHERE symbol = ?", (symbol,)
            )
        else:
            # Partially sold, keep average cost
            self.db.conn.execute(
                "UPDATE holdings SET quantity = ?, available = ?, last_synced = ? WHERE symbol = ?",
                (new_qty, new_qty, datetime.now().strftime('%Y-%m-%d'), symbol)
            )

        # Record trade
        self.db.conn.execute("""
            INSERT INTO trades (timestamp, symbol, direction, quantity, price,
                               order_type, status, agent_signal)
            VALUES (?, ?, 'sell', ?, ?, 'MO', 'Filled', 'manual')
        """, (date or datetime.now().strftime('%Y-%m-%d %H:%M:%S'), symbol, quantity, price))

        self.db.conn.commit()

        # Calculate P&L for this trade
        pnl = (price - old_cost) * quantity
        logger.info(f"Sold {quantity} {symbol} @ ${price:.2f} (cost ${old_cost:.2f}) → P&L: ${pnl:+.2f}")
        log_trade(
            symbol=symbol, action='SELL', price=price,
            quantity=quantity, pnl=pnl, note=f"MANUAL_REMOVE | cost=${old_cost:.2f}"
        )

    def get_pnl_summary(self) -> Dict:
        """Get overall portfolio P&L summary."""
        holdings = self.get_holdings()

        total_cost = sum(h['cost_price'] * h['quantity'] for h in holdings)
        total_current = sum(
            (h['current_price'] * h['quantity']) if h.get('current_price')
            else (h['cost_price'] * h['quantity'])
            for h in holdings
        )
        total_pnl = total_current - total_cost
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

        result = {
            'total_cost': round(total_cost, 2),
            'total_current': round(total_current, 2),
            'total_pnl': round(total_pnl, 2),
            'total_pnl_pct': round(total_pnl_pct, 2),
            'holding_count': len(holdings),
            'holdings': holdings,
        }

        log_performance(
            period='snapshot',
            total_return=f"{total_pnl_pct:+.2f}%",
            max_drawdown='N/A',
            trades=len(holdings)
        )

        return result

    def get_holding_symbols(self) -> List[str]:
        """Get list of symbols currently held."""
        rows = self.db.conn.execute(
            "SELECT symbol FROM holdings WHERE active = 1"
        ).fetchall()
        return [r['symbol'] for r in rows]


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Position Manager")
    parser.add_argument("action", choices=["sync", "list", "pnl", "report", "add", "remove"])
    parser.add_argument("--symbol", help="Symbol (for add/remove)")
    parser.add_argument("--qty", type=int, help="Quantity")
    parser.add_argument("--price", type=float, help="Price")
    parser.add_argument("--name", help="Company name")
    args = parser.parse_args()

    db = Database()
    pm = PositionManager(db)

    if args.action == "sync":
        pm.sync_from_broker()

    elif args.action == "list":
        holdings = pm.get_holdings()
        if not holdings:
            print("No holdings. Run 'sync' first.")
            return

        print(f"\n{'='*80}")
        print(f"📊 Current Holdings ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
        print(f"{'='*80}")
        print(f"{'Symbol':<8} {'Name':<20} {'Qty':<6} {'Cost':<10} {'Current':<10} "
              f"P&L%     P&L$")
        print(f"{'-'*80}")

        for h in holdings:
            price = h.get('current_price') or h['cost_price']
            pnl_pct = h.get('pnl_pct', 0)
            pnl_amt = h.get('pnl_amount', 0)
            print(f"{h['symbol']:<8} {h.get('company_name', '')[:20]:<20} "
                  f"{h['quantity']:<6} ${h['cost_price']:<9.2f} ${price:<9.2f} "
                  f"{pnl_pct:>+7.2f}%  ${pnl_amt:>+10.2f}")

        print(f"{'-'*80}")

    elif args.action == "pnl":
        summary = pm.get_pnl_summary()
        print(f"\n{'='*50}")
        print(f"💰 Portfolio P&L Summary")
        print(f"{'='*50}")
        print(f"  Total Cost:     ${summary['total_cost']:>12,.2f}")
        print(f"  Current Value:  ${summary['total_current']:>12,.2f}")
        print(f"  Total P&L:      ${summary['total_pnl']:>+12,.2f} ({summary['total_pnl_pct']:>+6.2f}%)")
        print(f"  Holdings:       {summary['holding_count']}")
        print(f"{'='*50}")

    elif args.action == "report":
        summary = pm.get_pnl_summary()
        print(f"\n{'='*100}")
        print(f"📊 Portfolio Report ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
        print(f"{'='*100}")

        for h in summary['holdings']:
            price = h.get('current_price') or h['cost_price']
            pnl_pct = h.get('pnl_pct', 0)
            pnl_amt = h.get('pnl_amount', 0)
            market_value = price * h['quantity']

            print(f"\n  {h['symbol']} — {h.get('company_name', 'N/A')}")
            print(f"    Shares:      {h['quantity']}")
            print(f"    Cost Basis:  ${h['cost_price']:.2f}")
            print(f"    Current:     ${price:.2f}")
            print(f"    Market Value: ${market_value:,.2f}")
            print(f"    P&L:         ${pnl_amt:+,.2f} ({pnl_pct:+.2f}%)")

            # Recent news
            news = db.query_data_points(symbol=h['symbol'], type_filter='news', days=3, limit=5)
            if news:
                print(f"    Recent News ({len(news)} items):")
                for n in news[:3]:
                    sent = n.get('sentiment_score', '')
                    sent_str = f"({float(sent):+.2f})" if sent else ""
                    print(f"      [{n['timestamp'][:16]}] {sent_str} {n['title'][:60]}")

            # Recent filings
            filings = db.query_data_points(symbol=h['symbol'], source='sec', type_filter='filing', days=30, limit=3)
            if filings:
                print(f"    Recent Filings:")
                for f in filings[:2]:
                    print(f"      [{f['timestamp'][:10]}] {f['title']}")

        print(f"\n{'='*100}")
        print(f"  Total P&L: ${summary['total_pnl']:+,.2f} ({summary['total_pnl_pct']:+.2f}%)")
        print(f"{'='*100}")

    elif args.action == "add":
        if not args.symbol or not args.qty or not args.price:
            print("Usage: position_manager add --symbol AAPL --qty 100 --price 200")
            return
        pm.add_holding(args.symbol, args.qty, args.price, args.name or "")

    elif args.action == "remove":
        if not args.symbol or not args.qty or not args.price:
            print("Usage: position_manager remove --symbol AAPL --qty 50 --price 210")
            return
        pm.remove_holding(args.symbol, args.qty, args.price)

    db.close()


if __name__ == "__main__":
    main()
