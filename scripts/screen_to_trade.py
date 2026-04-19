"""
US Data Hub — Screen-to-Trade Pipeline
========================================
完整选股→决策链路：
  1. 三层筛选（热度 → 行业周期 → 成长潜力）
  2. Top 10 只股票进入 TradingAgents 多 Agent 分析
  3. 持仓去重：与现有持仓重复则顺延，确保最终 10 只都是新票
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

from analysis.screener import ThreeLayerScreener
from analysis.signal_hub import SignalHub, Signal, SignalDirection, SignalSource
from storage import Database

logger = logging.getLogger(__name__)


def _get_current_holdings(db) -> List[str]:
    """从数据库获取当前持仓 symbol 列表 + 今日已交易标的（防重复买入）。"""
    symbols = []
    # 先同步长桥持仓
    try:
        from management.position_manager import PositionManager
        pm = PositionManager(db)
        pm.sync_from_broker()
    except Exception:
        pass
    # 查 holdings 表
    try:
        rows = db.conn.execute(
            "SELECT DISTINCT symbol FROM holdings WHERE quantity > 0"
        ).fetchall()
        symbols = [row[0] for row in rows]
    except Exception:
        pass
    # 备选 positions 表
    if not symbols:
        try:
            rows = db.conn.execute(
                "SELECT DISTINCT symbol FROM positions WHERE quantity > 0"
            ).fetchall()
            symbols = [row[0] for row in rows]
        except Exception:
            pass
    # Fix #11: Also exclude symbols already traded today
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        traded_rows = db.conn.execute(
            "SELECT DISTINCT symbol FROM trades WHERE timestamp >= ?",
            (f"{today} 00:00:00",)
        ).fetchall()
        traded_today = [row[0] for row in traded_rows]
        for s in traded_today:
            if s not in symbols:
                symbols.append(s)
        if traded_today:
            logger.info(f"今日已交易（去重）: {traded_today}")
    except Exception:
        pass
    return symbols


def screen_and_analyze(top_n: int = 10,
                       universe: List[str] = None,
                       min_score: float = 0.2,
                       run_trading: bool = False) -> Dict:
    """
    完整选股→决策流水线。

    Args:
        top_n: 最终进入 TradingAgents 分析的股票数量（默认 10）
        universe: 自定义选股池 (None=全量)
        min_score: 最低分阈值
        run_trading: 是否运行 TradingAgents 深度分析

    Returns:
        包含筛选结果和交易计划的字典
    """
    db = Database()
    screener = ThreeLayerScreener(db)

    # 获取当前持仓（用于去重）
    holdings = _get_current_holdings(db)
    if holdings:
        logger.info(f"当前持仓: {holdings}")

    # 第一层：三层筛选（拉足够多候选，用于去重后补足 top_n）
    candidate_pool = max(top_n * 5, 50)  # 至少拉 50 只，确保去重后够 top_n
    logger.info(f"Running three-layer screening (candidate_pool={candidate_pool}, top_n={top_n})")
    results = screener.screen(top_n=candidate_pool, min_score=min_score, universe=universe)

    if not results:
        logger.warning("No stocks passed screening")
        db.close()
        return {"error": "No stocks passed screening", "results": []}

    # 持仓去重：跳过持仓股，按顺位往后顺延
    top_picks = []
    skipped = []
    for r in results:
        if r["symbol"] in holdings:
            skipped.append({
                "symbol": r["symbol"],
                "score": r["total_score"],
                "reason": "已在持仓中",
            })
            continue
        top_picks.append(r)
        if len(top_picks) >= top_n:
            break

    if not top_picks:
        logger.warning("All top candidates are already in portfolio")
        db.close()
        return {"error": "All top candidates held", "results": [], "skipped": skipped}

    # 写入 screener_history（结果持久化）
    try:
        for r in results:
            heat_d = r.get("heat_detail", {})
            db.conn.execute(
                """INSERT INTO screener_history
                   (run_time, symbol, total_score, dim_news_volume, dim_social_heat,
                    dim_capital_flow, dim_momentum, dim_volatility, dim_insider, dim_mean_reversion)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    r["symbol"],
                    r["total_score"],
                    heat_d.get("news", 0),
                    heat_d.get("social", 0),
                    0,  # capital_flow (待接入)
                    0,  # momentum (可用 factor 表数据)
                    0,  # volatility
                    0,  # insider
                    0,  # mean_reversion
                )
            )
        db.conn.commit()
        logger.info(f"✅ Screener results saved to screener_history ({len(results)} records)")
    except Exception as e:
        logger.warning(f"Failed to save screener_history: {e}")
        db.conn.rollback()

    # 汇总筛选结果
    screen_summary = {
        "screen_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_scored": len(results),
        "holdings_excluded": holdings,
        "skipped_holdings": skipped,
        "final_count": len(top_picks),
        "top_picks": [],
    }

    for r in top_picks:
        pick = {
            "symbol": r["symbol"],
            "total_score": r["total_score"],
            "heat_score": r["heat_score"],
            "industry_score": r["industry_score"],
            "growth_score": r["growth_score"],
        }
        screen_summary["top_picks"].append(pick)
        rank = len(screen_summary["top_picks"])
        logger.info(
            f"  #{rank} {r['symbol']}: "
            f"total={r['total_score']:.3f} "
            f"(heat={r['heat_score']:.2f}, industry={r['industry_score']:.2f}, growth={r['growth_score']:.2f})"
        )

    if skipped:
        for s in skipped:
            logger.info(f"  ⏭️  跳过 {s['symbol']} (score={s['score']:.3f}) — {s['reason']}")

    # 第二层：TradingAgents 深度分析 → 输出 TradeSignal
    trading_results = {}
    signals_from_ta = []
    if run_trading:
        logger.info(f"Running TradingAgents analysis for {len(top_picks)} stocks...")
        from tradingagents.main import run_trading_analysis

        for pick in top_picks:
            symbol = pick["symbol"]
            logger.info(f"  Analyzing {symbol}...")
            try:
                result = run_trading_analysis(
                    stock_symbol=symbol,
                    trading_date=datetime.now().strftime("%Y-%m-%d"),
                    market="US",
                )
                trading_results[symbol] = result

                # v6.0: 解析 TA 输出为 TradeSignal
                decision_text = result.get("decision", "")
                text_lower = decision_text.lower()

                direction = None
                if any(w in text_lower for w in ["buy", "purchase", "enter", "accumulate"]):
                    direction = SignalDirection.BUY
                elif any(w in text_lower for w in ["sell", "exit", "close", "liquidate"]):
                    direction = SignalDirection.SELL

                if direction:
                    import re
                    confidence = 0.5
                    conf_match = re.search(r'(?:confidence|conf)[\s:：]*(\d+\.?\d*)\s*%?', text_lower)
                    if conf_match:
                        val = float(conf_match.group(1))
                        confidence = val / 100.0 if val > 1 else val

                    from analysis.signal_schema import TradeSignal
                    ts = TradeSignal(
                        symbol=symbol,
                        direction=direction,
                        confidence=confidence,
                        source=SignalSource.TRADING_AGENTS,
                        strength=0.8,
                        reason=decision_text[:500],
                        extra={"full_decision": decision_text, "result": result},
                    )
                    signals_from_ta.append(ts)
            except Exception as e:
                logger.error(f"Trading analysis failed for {symbol}: {e}")
                trading_results[symbol] = {"error": str(e)}

    db.close()

    return {
        "screen_summary": screen_summary,
        "trading_results": trading_results if run_trading else None,
        "signals_from_ta": [s.to_dict() for s in signals_from_ta],  # v6.0
    }


def print_report(result: Dict):
    """Print a human-readable report of the screen-to-trade pipeline."""
    summary = result.get("screen_summary", {})

    print(f"\n{'='*60}")
    print(f"📊 三层筛选报告 ({summary.get('screen_time', 'N/A')})")
    print(f"{'='*60}")
    print(f"  评分股票: {summary.get('total_scored', 0)} 只")

    holdings = summary.get("holdings_excluded", [])
    if holdings:
        print(f"  当前持仓: {', '.join(holdings)}")

    skipped = summary.get("skipped_holdings", [])
    if skipped:
        print(f"  持仓去重: {len(skipped)} 只")
        for s in skipped:
            print(f"    ⏭️  {s['symbol']} (score={s['score']:.3f}) — {s['reason']}")

    print(f"\n  ✅ 最终入选: {summary.get('final_count', 0)} 只")
    print()

    for i, pick in enumerate(summary.get("top_picks", []), 1):
        print(f"  #{i}  {pick['symbol']:6s}  总分={pick['total_score']:.3f}")
        print(f"       热度={pick['heat_score']:.2f}  行业={pick['industry_score']:.2f}  成长={pick['growth_score']:.2f}")

    trading = result.get("trading_results")
    if trading:
        print(f"\n{'='*60}")
        print("🤖 TradingAgents 决策结果")
        print(f"{'='*60}")
        for symbol, t_result in trading.items():
            if "error" in t_result:
                print(f"  {symbol}: ❌ {t_result['error']}")
            else:
                decision = t_result.get("decision", {})
                rating = decision.get("final_trade_decision", {}).get("rating", "N/A")
                print(f"  {symbol}: {rating}")

    print(f"{'='*60}")
