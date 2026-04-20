#!/usr/bin/env python3
"""
US Data Hub — CLI Entry Point
Unified interface for all operations.

Usage:
    python scripts/run.py init          # Initialize database
    python scripts/run.py status        # Show system status
    python scripts/run.py collect       # Collect from all sources
    python scripts/run.py screener      # Run stock screener
    python scripts/run.py factors       # Calculate factors
    python scripts/run.py monitor       # Check holdings
    python scripts/run.py report        # Full portfolio report
    python scripts/run.py backtest      # Run factor backtest
    python scripts/run.py auto-trade    # Execute trading decision
    python scripts/run.py pipeline      # Full pipeline: collect → factors → report
    python scripts/run.py trading-agent # Run multi-agent trading analysis
"""

import sys
import os
import json
import logging
from datetime import datetime

# Load .env file before any other imports
from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import Database

from dayup_logger import setup_root_logger, log_error
setup_root_logger(level=logging.INFO)
logger = logging.getLogger(__name__)


def cmd_init():
    """Initialize database and schema."""
    db = Database()
    db.init_schema()

    watchlist = [
        ("AAPL", "Apple Inc.", "Technology"),
        ("MSFT", "Microsoft Corporation", "Technology"),
        ("GOOGL", "Alphabet Inc.", "Technology"),
        ("AMZN", "Amazon.com Inc.", "Consumer Cyclical"),
        ("TSLA", "Tesla Inc.", "Consumer Cyclical"),
        ("NVDA", "NVIDIA Corporation", "Technology"),
        ("META", "Meta Platforms Inc.", "Technology"),
    ]
    for symbol, name, sector in watchlist:
        db.conn.execute(
            "INSERT OR IGNORE INTO watchlist (symbol, company_name, sector) VALUES (?,?,?)",
            (symbol, name, sector)
        )
    db.conn.commit()
    stats = db.get_stats()
    db.close()
    print(f"✅ Database initialized. Tables: {len(stats)}")


def cmd_status():
    """Show system status."""
    db = Database()
    stats = db.get_stats()
    print(f"\n{'='*50}")
    print(f"📊 US Data Hub Status ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
    print(f"{'='*50}")
    for key, val in stats.items():
        if isinstance(val, dict):
            print(f"  {key}:")
            for k, v in val.items():
                print(f"    {k}: {v}")
        else:
            print(f"  {key}: {val}")
    print(f"{'='*50}")
    db.close()


def cmd_collect():
    """Collect data from all sources."""
    from scripts.data_pipeline import collect_all
    stats = collect_all()
    print(f"✅ Collected: {stats['total_fetched']} fetched, {stats['total_new']} new")


def cmd_screener():
    """Run stock screener with holding deduplication."""
    import json
    from analysis.screener import StockScreener
    from executors.longbridge import LongbridgeExecutor
    from management.position_manager import PositionManager

    db = Database()

    # 获取当前持仓
    executor = LongbridgeExecutor()
    pm = PositionManager(db, executor)
    pm.sync_from_broker()
    holdings = pm.get_holdings()
    holding_symbols = set(h["symbol"] for h in holdings)

    # 选股：先选 top_n * 2，确保去重后仍有足够的候选
    screener = StockScreener(db)
    results = screener.screen(top_n=40, min_score=0.3)
    if not results:
        print("No stocks passed screening")
        db.close()
        return

    # 去重：过滤掉已持仓的股票，再取前 20
    filtered = [r for r in results if r["symbol"] not in holding_symbols]
    top_filtered = filtered[:20]

    if not top_filtered:
        print("⚠️ 所有选股结果均为已持仓股票")
        db.close()
        return

    # 输出持仓去重信息
    print(f"📦 当前持仓 ({len(holding_symbols)}): {', '.join(sorted(holding_symbols))}")
    print(f"🔍 原始候选: {len(results)} 只 → 去重后: {len(filtered)} 只 → 取前 {len(top_filtered)} 只")
    print()

    # 保存结果
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "holdings": sorted(holding_symbols),
        "top_filtered": [
            {"rank": i, "symbol": r["symbol"], "score": r["total_score"]}
            for i, r in enumerate(top_filtered, 1)
        ],
    }
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    output_path = f"output/screen_{ts}.json"
    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"💾 已保存: {output_path}")
    print()

    # 打印结果
    print(f"🔍 今日选股 Top {len(top_filtered)} (已去重持仓):")
    for i, r in enumerate(top_filtered, 1):
        print(f"  {i:2d}. {r['symbol']:6s} score={r['total_score']:.3f}")

    db.close()


def cmd_factors():
    """Calculate factors from prices."""
    from analysis.factor_from_prices import main as factor_main
    sys.argv = ['factor_from_prices.py']
    factor_main()


def cmd_monitor():
    """Check holdings for alerts."""
    from monitoring.holding_monitor import HoldingMonitor
    from management.position_manager import PositionManager
    from executors.longbridge import LongbridgeExecutor
    db = Database()
    executor = LongbridgeExecutor()
    pm = PositionManager(db, executor)
    monitor = HoldingMonitor(db, pm)
    pm.sync_from_broker()
    alerts = monitor.run_full_check()
    print(f"✅ Monitoring complete: {len(alerts)} alerts")
    db.close()


def cmd_report():
    """Full portfolio report + performance stats (Fix #4)."""
    from monitoring.holding_monitor import HoldingMonitor
    from management.position_manager import PositionManager
    db = Database()
    pm = PositionManager(db)
    pm.sync_from_broker()
    pnl = pm.get_pnl_summary()
    print(f"\n💰 Portfolio: ${pnl['total_cost']:,.2f} cost → ${pnl['total_current']:,.2f} current")
    print(f"   P&L: ${pnl['total_pnl']:+,.2f} ({pnl['total_pnl_pct']:+.2f}%)")
    print(f"   Holdings: {pnl['holding_count']}")
    for h in pnl['holdings']:
        price = h.get('current_price') or h['cost_price']
        pnl_pct = h.get('pnl_pct', 0)
        print(f"   {h['symbol']:6s} {h['quantity']:>4} shares @ ${h['cost_price']:.2f} → ${price:.2f} ({pnl_pct:+.2f}%)")

    # Fix #4: Performance stats
    try:
        from analysis.performance import compute_performance
        perf = compute_performance(db)
        print(f"\n📊 Performance Metrics:")
        print(f"   Sharpe Ratio:   {perf['sharpe_ratio']:.3f}")
        print(f"   Max Drawdown:   {perf['max_drawdown']:.1%}")
        print(f"   Win Rate:       {perf['win_rate']:.1%} ({perf['total_trades']} trades)")
        print(f"   Total Return:   {perf['total_return']:+.1%}")
        print(f"   Avg Return:     {perf['avg_return_per_trade']:+.1%}")
        print(f"   Profit Factor:  {perf['profit_factor']:.2f}")
        print(f"   Best Trade:     {perf['best_trade']:+.1%}")
        print(f"   Worst Trade:    {perf['worst_trade']:+.1%}")
        print(f"   Consec Wins:    {perf['consecutive_wins']}")
        print(f"   Consec Losses:  {perf['consecutive_losses']}")
        print(f"   Avg Hold Days:  {perf['avg_holding_period_days']:.1f}")
    except Exception as e:
        print(f"\n📊 Performance: error — {e}")

    db.close()


def cmd_backtest():
    """Run factor backtest with alphalens."""
    import argparse as _ap
    from analysis.alphalens_backtest import AlphalensBacktest

    db = Database()
    backtest = AlphalensBacktest(db)
    results = backtest.run_all_factors(days=180)

    print(f"\n{'='*50}")
    print("📈 Factor Backtest Results")
    print(f"{'='*50}")
    for factor, result in results.items():
        status = result.get("status", "unknown")
        if status == "success":
            print(f"  ✅ {factor}: {result['records']} records, {result['symbols']} symbols")
            print(f"     Date range: {result['date_range']}")
            if "ic" in result:
                print(f"     IC: {result['ic']}")
            if "quantile_mean_returns" in result:
                for period, q_returns in result["quantile_mean_returns"].items():
                    returns_str = ", ".join(f"Q{k}: {v:+.4f}" for k, v in sorted(q_returns.items()))
                    print(f"     Mean returns ({period}): {returns_str}")
        else:
            print(f"  ❌ {factor}: {result.get('error', status)}")
    print(f"{'='*50}")
    db.close()


def cmd_auto_trade():
    """Execute a trading decision (dry run by default)."""
    import argparse as _ap
    from executors.auto_trade import run_auto_trade

    parser = _ap.ArgumentParser(description="Auto Trade Executor", add_help=False)
    parser.add_argument("--symbol", default="AAPL", help="Stock symbol")
    parser.add_argument("--signal", default="", help="Trading signal text")
    parser.add_argument("--signal-file", default="", help="Path to signal file")
    parser.add_argument("--live", action="store_true", help="Execute real orders")
    args, _ = parser.parse_known_args()

    signal = args.signal
    if args.signal_file and os.path.exists(args.signal_file):
        with open(args.signal_file) as f:
            signal = f.read()

    if not signal:
        signal = f"HOLD {args.symbol} - no signal provided"

    result = run_auto_trade(args.symbol, signal, dry_run=not args.live)
    print(json.dumps(result, indent=2, ensure_ascii=False))


def cmd_trading_agent():
    """Run multi-agent trading analysis for a symbol."""
    import argparse as _ap
    from tradingagents.main import run_trading_analysis

    parser = _ap.ArgumentParser(description="Trading Agent", add_help=False)
    parser.add_argument("--symbol", default="AAPL", help="Stock symbol")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"), help="Trading date")
    parser.add_argument("--market", default="US", help="Market (US, HK, CN)")
    args, _ = parser.parse_known_args()

    result = run_trading_analysis(
        stock_symbol=args.symbol,
        trading_date=args.date,
        market=args.market,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


def cmd_pipeline():
    """Full pipeline: collect → factors → report."""
    logger.info("=== Full Pipeline ===")
    cmd_collect()
    cmd_factors()
    cmd_report()


def cmd_screen_to_trade():
    """Three-layer screening → TradingAgents analysis."""
    import argparse as _ap
    from scripts.screen_to_trade import screen_and_analyze, print_report

    parser = _ap.ArgumentParser(description="Screen to Trade", add_help=False)
    parser.add_argument("--top", type=int, default=5, help="Top N stocks for TradingAgents")
    parser.add_argument("--min-score", type=float, default=0.2, help="Minimum score threshold")
    parser.add_argument("--no-trading", action="store_true", help="Skip TradingAgents analysis")
    args, _ = parser.parse_known_args()

    result = screen_and_analyze(
        top_n=args.top,
        min_score=args.min_score,
        run_trading=not args.no_trading,
    )
    print_report(result)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US Data Hub CLI")
    parser.add_argument("command", nargs="?", default="status",
                        choices=["init", "status", "collect", "screener",
                                 "factors", "monitor", "report", "backtest",
                                 "auto-trade", "trading-agent", "alphalens",
                                 "pipeline", "screen-to-trade", "order-monitor",
                                 "start", "stop", "restart", "check"])
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + remaining  # Pass remaining args to sub-commands

    commands = {
        "init": cmd_init,
        "status": cmd_status,
        "collect": cmd_collect,
        "screener": cmd_screener,
        "factors": cmd_factors,
        "monitor": cmd_monitor,
        "report": cmd_report,
        "backtest": cmd_backtest,
        "auto-trade": cmd_auto_trade,
        "trading-agent": cmd_trading_agent,
        "alphalens": cmd_alphalens,
        "pipeline": cmd_pipeline,
        "screen-to-trade": cmd_screen_to_trade,
        "order-monitor": cmd_order_monitor,
        "start": lambda: __import__('scripts.system_manager', fromlist=['cmd_start']).cmd_start(),
        "stop": lambda: __import__('scripts.system_manager', fromlist=['cmd_stop']).cmd_stop(),
        "restart": lambda: __import__('scripts.system_manager', fromlist=['cmd_restart']).cmd_restart(),
        "check": lambda: __import__('scripts.system_manager', fromlist=['cmd_check']).cmd_check(),
    }

    commands[args.command]()


def cmd_alphalens():
    """Run Alphalens factor analysis."""
    from analysis.alphalens_analysis import run_alphalens_analysis
    run_alphalens_analysis(
        factor_name='momentum',
        symbols=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META'],
        days=180, quantiles=5, periods=(1, 5, 10),
        output_dir='data/processed'
    )


def cmd_order_monitor():
    """Monitor pending orders."""
    import argparse as _ap
    from monitoring.order_monitor import OrderMonitor

    parser = _ap.ArgumentParser(description="Order Monitor", add_help=False)
    parser.add_argument("--mode", choices=["check", "cleanup"], default="check",
                       help="check=监控 pending 订单, cleanup=盘后清理")
    args, _ = parser.parse_known_args()

    db = Database()
    db.init_schema()
    executor = LongbridgeExecutor()
    monitor = OrderMonitor(db, executor)

    if args.mode == "check":
        results = monitor.run_full_check()
    elif args.mode == "cleanup":
        results = monitor.run_cleanup()

    print(f"\n{'='*60}")
    print(f"📋 Order Monitor Results")
    print(f"{'='*60}")
    for k, v in results.items():
        if isinstance(v, list):
            print(f"  {k}: {len(v)}")
        else:
            print(f"  {k}: {v}")
    print(f"{'='*60}")

    db.close()


if __name__ == "__main__":
    main()
