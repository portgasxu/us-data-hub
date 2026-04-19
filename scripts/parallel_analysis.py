#!/usr/bin/env python3
"""
US Data Hub — Parallel TradingAgents Analysis
==============================================
同时分析多只标的，大幅降低总耗时。

单只标的分析时间: ~60-120 秒
串行 5 只:        ~300-600 秒 (超时!)
并行 5 只:        ~60-120 秒 (搞定!)

Usage:
    python3 scripts/parallel_analysis.py --symbols AAPL MSFT NVDA GOOGL AMZN --top 5
    python3 scripts/parallel_analysis.py --top 5  # 从筛选器取 Top 5
"""

import json
import logging
import argparse
import time
from datetime import datetime
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


def _analyze_one_stock(symbol: str, trading_date: str) -> Dict:
    """分析单只标的（供线程池调用）"""
    try:
        t0 = time.time()
        logger.info(f"[{symbol}] 开始分析...")

        from tradingagents.main import run_trading_analysis
        result = run_trading_analysis(
            stock_symbol=symbol,
            trading_date=trading_date,
            market="US",
        )

        elapsed = time.time() - t0
        logger.info(f"[{symbol}] ✅ 完成 ({elapsed:.0f}s)")
        return {"symbol": symbol, "status": "ok", "result": result, "elapsed": elapsed}

    except Exception as e:
        logger.error(f"[{symbol}] ❌ 失败: {e}")
        return {"symbol": symbol, "status": "error", "error": str(e), "elapsed": 0}


def parallel_analysis(symbols: List[str], max_workers: int = None, trading_date: str = None) -> Dict:
    """
    并行分析多只标的。

    Args:
        symbols: 待分析的股票列表
        max_workers: 最大并发数（默认 = min(len(symbols), 5)）
        trading_date: 分析日期（默认今天）

    Returns:
        包含所有分析结果的字典
    """
    if trading_date is None:
        trading_date = datetime.now().strftime("%Y-%m-%d")

    if max_workers is None:
        max_workers = min(len(symbols), 5)

    logger.info(f"开始并行分析 {len(symbols)} 只标的，max_workers={max_workers}")
    logger.info(f"标的: {', '.join(symbols)}")

    t0 = time.time()
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_analyze_one_stock, sym, trading_date): sym
            for sym in symbols
        }

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                results[symbol] = result
            except Exception as e:
                results[symbol] = {"symbol": symbol, "status": "error", "error": str(e), "elapsed": 0}

    total_time = time.time() - t0

    # 统计
    ok_count = sum(1 for r in results.values() if r["status"] == "ok")
    err_count = sum(1 for r in results.values() if r["status"] == "error")
    avg_time = sum(r["elapsed"] for r in results.values() if r["status"] == "ok") / max(ok_count, 1)

    logger.info(f"\n{'='*50}")
    logger.info(f"分析完成! 总计 {total_time:.0f}s")
    logger.info(f"  ✅ 成功: {ok_count}")
    logger.info(f"  ❌ 失败: {err_count}")
    logger.info(f"  ⏱️  平均单只: {avg_time:.0f}s")
    logger.info(f"  🚀 加速比: {avg_time * len(symbols) / total_time:.1f}x")
    logger.info(f"{'='*50}\n")

    return {
        "trading_date": trading_date,
        "total_time": round(total_time, 1),
        "total_symbols": len(symbols),
        "success_count": ok_count,
        "error_count": err_count,
        "avg_time_per_stock": round(avg_time, 1),
        "results": results,
    }


def parallel_screen_and_trade(top_n: int = 5, min_score: float = 0.2, max_workers: int = None) -> Dict:
    """
    完整并行流水线：筛选 → 并行分析。
    替代原来的 screen_to_trade 串行版本。
    """
    from storage import Database
    from analysis.screener import ThreeLayerScreener

    db = Database()
    screener = ThreeLayerScreener(db)

    # 获取当前持仓（用于去重）
    holdings = _get_holdings(db)

    # 筛选
    logger.info(f"Running three-layer screening (top_n={top_n})...")
    results = screener.screen(top_n=top_n * 4, min_score=min_score)

    # 去重
    top_picks = []
    for r in results:
        if r["symbol"] not in holdings:
            top_picks.append(r["symbol"])
            if len(top_picks) >= top_n:
                break

    if not top_picks:
        logger.warning("All top candidates already in portfolio")
        db.close()
        return {"error": "All candidates held", "symbols": []}

    db.close()

    # 并行分析
    return parallel_analysis(top_picks, max_workers=max_workers)


def _get_holdings(db) -> list:
    """获取当前持仓 symbols"""
    try:
        from management.position_manager import PositionManager
        pm = PositionManager(db)
        pm.sync_from_broker()
    except Exception:
        pass
    rows = db.conn.execute("SELECT DISTINCT symbol FROM holdings WHERE quantity > 0").fetchall()
    return [r[0] for r in rows]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Parallel TradingAgents Analysis")
    parser.add_argument("--symbols", nargs="+", help="Stock symbols to analyze")
    parser.add_argument("--top", type=int, default=5, help="Number of top picks to analyze (from screener)")
    parser.add_argument("--workers", type=int, default=None, help="Max concurrent workers")
    parser.add_argument("--date", type=str, default=None, help="Trading date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.symbols:
        result = parallel_analysis(args.symbols, max_workers=args.workers, trading_date=args.date)
    else:
        result = parallel_screen_and_trade(top_n=args.top, max_workers=args.workers)

    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
