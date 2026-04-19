#!/usr/bin/env python3
"""
US Data Hub — 24/7 全天候自动交易守护进程
===========================================
功能:
  - 根据市场时段自动调整采集/分析频率
  - 新闻采集 → 数据分析 → 持仓管理 → 交易执行
  - 所有报错实时记录到 MD 文件
  - 限流保护 + 自动重试

用法:
  python daemon/daemon_247.py              # 前台运行
  nohup python daemon/daemon_247.py > /dev/null 2>&1 &   # 后台运行

美股时段（北京时间）:
  盘前: 16:00 - 21:30
  交易中: 21:30 - 04:00 (次日)
  盘后: 04:00 - 08:00
  休市: 08:00 - 16:00
  周末: 全天休市
"""

import sys
import os
import time
import json
import signal
import traceback
from datetime import datetime, timezone, timedelta

# 项目路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

# 导入配置
from daemon.config import (
    WATCHLIST, LLM_MODEL, TICKER_SPACING, MAX_RETRIES,
    FREQ_MARKET, FREQ_PRE_AFTER, FREQ_OFF, FREQ_WEEKEND,
    LOG_DIR, DAEMON_DIR,
)
from daemon.error_logger import ErrorLogger

# 当前时间 (北京时间)
CST = timezone(timedelta(hours=8))


def now_str():
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def is_weekend():
    return datetime.now(CST).weekday() >= 5


def is_us_dst():
    """
    判断美国当前是否为夏令时。
    夏令时：3月第二个周日 02:00 ET → 11月第一个周日 02:00 ET
    夏令时 UTC-4（EDT），冬令时 UTC-5（EST）
    北京时间 = ET + 12h（夏令）/ +13h（冬令）
    """
    now = datetime.now(CST)  # 北京时间（timezone-aware）

    def get_second_sunday_march(year):
        """返回3月第二个周日 02:00 ET（北京时间）"""
        mar1 = datetime(year, 3, 1)
        days_to_sun = (6 - mar1.weekday()) % 7
        first_sun = mar1.replace(day=1 + days_to_sun)
        second_sun = first_sun.replace(day=first_sun.day + 7, hour=2)
        # 02:00 EDT = UTC 06:00 = 北京时间 14:00，但切换是 ET 时间
        # 用 UTC 计算最准确：ET 02:00 夏令时=UTC 06:00=北京 14:00
        return second_sun.replace(tzinfo=CST).replace(hour=14, minute=0)

    def get_first_sunday_nov(year):
        """返回11月第一个周日 02:00 ET（北京时间）"""
        nov1 = datetime(year, 11, 1)
        days_to_sun = (6 - nov1.weekday()) % 7
        first_sun = nov1.replace(day=1 + days_to_sun, hour=2)
        # 02:00 EST = UTC 07:00 = 北京时间 15:00
        return first_sun.replace(tzinfo=CST).replace(hour=15, minute=0)

    dst_start = get_second_sunday_march(now.year)
    dst_end = get_first_sunday_nov(now.year)

    # 处理跨年边界
    if now < dst_start:
        # 还没到今年夏令时，看去年11月是否已切回
        prev_end = get_first_sunday_nov(now.year - 1)
        return False  # 11月到3月间是冬令时
    elif now >= dst_end:
        return False  # 11月后是冬令时
    else:
        return True  # 3月到11月间是夏令时


def get_market_phase():
    """
    返回当前市场时段 (北京时间)。
    'pre_market' | 'market' | 'after_hours' | 'night_session' | 'weekend'

    夏令时 (EDT): 盘前 16:00, 交易 21:30-04:00, 盘后 04:00-08:00, 夜盘 08:00-16:00
    冬令时 (EST): 盘前 17:00, 交易 22:30-05:00, 盘后 05:00-09:00, 夜盘 09:00-17:00
    """
    if is_weekend():
        return "weekend"

    now = datetime.now(CST)
    minutes = now.hour * 60 + now.minute

    dst = is_us_dst()
    if dst:
        # 夏令时
        pre_start = 16 * 60         # 16:00
        market_start = 21 * 60 + 30  # 21:30
        market_end = 4 * 60          # 04:00
        after_end = 8 * 60           # 08:00
    else:
        # 冬令时
        pre_start = 17 * 60          # 17:00
        market_start = 22 * 60 + 30  # 22:30
        market_end = 5 * 60          # 05:00
        after_end = 9 * 60           # 09:00

    # 跨日处理
    if minutes >= market_start or minutes < market_end:
        return "market"
    elif minutes >= market_end and minutes < after_end:
        return "after_hours"
    elif minutes >= after_end and minutes < pre_start:
        return "night_session"  # 08:00-16:00 (夏) / 09:00-17:00 (冬)
    elif minutes >= pre_start and minutes < market_start:
        return "pre_market"
    else:
        return "off"


def get_freq():
    """根据市场时段获取频率配置"""
    phase = get_market_phase()
    if phase == "weekend":
        return FREQ_WEEKEND, "🏖️ 周末休市"
    elif phase == "market":
        return FREQ_MARKET, "🟢 交易中"
    elif phase in ("pre_market", "after_hours", "night_session"):
        label = {"pre_market": "🟡 盘前", "after_hours": "🟡 盘后", "night_session": "🌙 夜盘"}[phase]
        return FREQ_PRE_AFTER, label
    else:
        return FREQ_OFF, "⚪ 休市中"


def safe_run(func, logger, source, ticker="", **kwargs):
    """安全执行函数，捕获异常并记录"""
    for attempt in range(MAX_RETRIES):
        try:
            return func()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            error_msg = str(e).lower()
            is_rate_limit = any(kw in error_msg for kw in ["rate limit", "429", "too many requests", "throttl"])

            if is_rate_limit and attempt < MAX_RETRIES - 1:
                wait = 60 * (attempt + 1)
                logger.log_warning(source, f"限流触发，等待 {wait}s 后重试 ({attempt+1}/{MAX_RETRIES})", {"ticker": ticker or "N/A"})
                time.sleep(wait)
            elif attempt < MAX_RETRIES - 1:
                logger.log_warning(source, f"失败，等待后重试 ({attempt+1}/{MAX_RETRIES}): {e}", {"ticker": ticker or "N/A"})
                time.sleep(10 * (attempt + 1))
            else:
                logger.log_error(source, e, {"ticker": ticker or "N/A"})
                return None
    return None


# ============================================================
# 数据采集
# ============================================================

def collect_data(logger, symbols):
    """全量数据采集"""
    logger.log_info("COLLECT", f"开始数据采集: {', '.join(symbols)}")
    start = time.time()

    try:
        from scripts.data_pipeline import collect_all
        stats = collect_all(symbols)
        elapsed_ms = int((time.time() - start) * 1000)
        logger.log_success("COLLECT", "数据采集完成", {
            "获取": stats["total_fetched"],
            "新增": stats["total_new"],
            "耗时": f"{elapsed_ms}ms",
        })
        return True
    except Exception as e:
        logger.log_error("COLLECT", e, {"symbols": ", ".join(symbols)})
        return False


def collect_price_only(logger, symbols):
    """仅采集价格数据（更快，适合高频）"""
    logger.log_info("PRICE", f"价格采集: {', '.join(symbols)}")

    try:
        from collectors.price import PriceCollector
        from storage import Database

        db = Database()
        db.init_schema()
        pc = PriceCollector()
        count = 0

        for symbol in symbols:
            try:
                items = pc.collect(symbol, count=1)
                for item in items:
                    db.insert_price(symbol, item["date"], item)
                    count += 1
            except Exception as e:
                logger.log_warning("PRICE", f"{symbol} 价格采集失败: {e}", {"symbol": symbol})
            time.sleep(1)

        db.close()
        logger.log_success("PRICE", f"价格采集完成", {"采集": count})
        return True
    except Exception as e:
        logger.log_error("PRICE", e)
        return False


def collect_news(logger, symbols):
    """采集新闻和社交数据"""
    logger.log_info("NEWS", f"新闻采集: {', '.join(symbols)}")

    try:
        from collectors.google_news import GoogleNewsCollector
        from collectors.reddit import RedditCollector
        from processors.sentiment import batch_score_sentiment
        from normalizers.schemas import normalize_news, normalize_reddit_post, validate_data_point
        from storage import Database

        db = Database()
        db.init_schema()
        proxy = "http://127.0.0.1:7890"

        gn = GoogleNewsCollector(proxy=proxy)
        rc = RedditCollector(proxy=proxy)
        count = 0

        for symbol in symbols:
            try:
                # Google News
                news = gn.collect(symbol, count=10)
                news = batch_score_sentiment(news)
                for item in news:
                    dp = normalize_news(item)
                    db.insert_data_point(dp.to_dict())
                    count += 1

                # Reddit
                posts = rc.collect(symbol, count=10)
                posts = batch_score_sentiment(posts)
                for item in posts:
                    dp = normalize_reddit_post(item)
                    db.insert_data_point(dp.to_dict())
                    count += 1
            except Exception as e:
                logger.log_warning("NEWS", f"{symbol} 采集失败: {e}", {"symbol": symbol})
            time.sleep(1)

        db.log_collection(source="news", fetched=count, new=count, status="success")
        db.close()
        logger.log_success("NEWS", f"新闻采集完成", {"采集": count})
        return True
    except Exception as e:
        logger.log_error("NEWS", e)
        return False


# ============================================================
# 交易分析
# ============================================================

def run_trading_analysis(logger, symbols, from_screener=False):
    """多标的交易分析。

    Args:
        symbols: 要分析的标的列表
        from_screener: 如果为 True，表示这是由选股扫描自动触发的分析
    """
    phase = get_market_phase()
    if phase == "weekend":
        logger.log_info("ANALYSIS", "周末休市，跳过交易分析")
        return False

    source_label = "选股自动触发" if from_screener else "定时任务"
    logger.log_info("ANALYSIS", f"开始交易分析 [{source_label}]: {', '.join(symbols)}")
    decisions = {}
    today = datetime.now(CST).strftime("%Y-%m-%d")

    try:
        from dotenv import load_dotenv
        load_dotenv()

        from tradingagents.graph.trading_graph import TradingAgentsGraph
        from tradingagents.default_config import DEFAULT_CONFIG

        config = DEFAULT_CONFIG.copy()
        config["deep_think_llm"] = LLM_MODEL
        config["quick_think_llm"] = LLM_MODEL
        config["max_debate_rounds"] = 1
        config["max_risk_discuss_rounds"] = 1
        config["data_vendors"] = {
            "core_stock_apis": "longbridge",
            "technical_indicators": "longbridge",
            "fundamental_data": "longbridge",
            "news_data": "longbridge",
        }

        ta = TradingAgentsGraph(debug=False, config=config)

        for i, ticker in enumerate(symbols):
            if i > 0:
                time.sleep(TICKER_SPACING)

            def _analyze():
                return ta.propagate(ticker, today)

            result = safe_run(_analyze, logger, "ANALYSIS", ticker=ticker)
            if result:
                _, decision = result
                decisions[ticker] = decision
                direction = decision.get("direction", "unknown") if isinstance(decision, dict) else "unknown"
                logger.log_success("ANALYSIS", f"{ticker} → {direction}", {
                    "决策": json.dumps(decision, ensure_ascii=False, default=str)[:200],
                })
            else:
                logger.log_warning("ANALYSIS", f"{ticker} 分析失败", {"ticker": ticker})

        # 保存决策
        ts = datetime.now(CST).strftime("%Y%m%d_%H%M")
        decision_file = os.path.join(LOG_DIR, f"decisions_{ts}.json")
        with open(decision_file, "w", encoding="utf-8") as f:
            json.dump(decisions, f, indent=2, ensure_ascii=False, default=str)
        logger.log_success("ANALYSIS", f"决策已保存", {"文件": decision_file})

        # 汇总
        logger.log_info("ANALYSIS", "交易决策汇总:")
        for ticker, dec in decisions.items():
            if isinstance(dec, dict):
                direction = dec.get("direction", "unknown")
                logger.log_info("ANALYSIS", f"  {ticker}: {direction}")

        return True
    except Exception as e:
        logger.log_error("ANALYSIS", e, {"symbols": ", ".join(symbols)})
        return False


# ============================================================
# 持仓检查
# ============================================================

def check_position(logger):
    """持仓检查和报告"""
    logger.log_info("POSITION", "开始持仓检查")

    try:
        from storage import Database
        from management.position_manager import PositionManager
        from monitoring.holding_monitor import HoldingMonitor

        db = Database()
        db.init_schema()

        # 尝试同步 Longbridge 持仓
        try:
            from executors.longbridge import LongbridgeExecutor
            executor = LongbridgeExecutor()
            pm = PositionManager(db, executor)
            pm.sync_from_broker()
        except Exception as e:
            logger.log_warning("POSITION", f"Longbridge 同步失败，使用本地持仓: {e}")
            pm = PositionManager(db)

        # 持仓监控 + LLM 动态决策
        monitor = HoldingMonitor(db, pm)
        alerts = monitor.run_full_check()

        # ── 自动执行链路（LLM 高置信度决策） ──
        executed = _auto_execute(logger, alerts, pm, db)

        # 获取 P&L
        pnl = pm.get_pnl_summary()

        # 生成报告
        ts = datetime.now(CST).strftime("%Y%m%d_%H%M")
        report_file = os.path.join(LOG_DIR, f"position_{ts}.md")
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(f"# 📊 持仓报告\n\n")
            f.write(f"**时间:** {now_str()}\n\n")
            f.write(f"**总成本:** ${pnl['total_cost']:,.2f}\n\n")
            f.write(f"**当前价值:** ${pnl['total_current']:,.2f}\n\n")
            f.write(f"**盈亏:** ${pnl['total_pnl']:+,.2f} ({pnl['total_pnl_pct']:+.2f}%)\n\n")
            f.write(f"**持仓数:** {pnl['holding_count']}\n\n")

            if pnl['holdings']:
                f.write("| 标的 | 数量 | 成本 | 现价 | 盈亏 |\n")
                f.write("|------|------|------|------|------|\n")
                for h in pnl['holdings']:
                    price = h.get('current_price') or h['cost_price']
                    pnl_pct = h.get('pnl_pct', 0)
                    f.write(f"| {h['symbol']} | {h['quantity']} | ${h['cost_price']:.2f} | ${price:.2f} | {pnl_pct:+.2f}% |\n")

            if alerts:
                f.write(f"\n## ⚠️ 告警\n\n")
                for a in alerts:
                    f.write(f"- [{a.get('severity', 'info')}] {a.get('symbol', '')}: {a.get('message', '')}\n")

        db.close()

        logger.log_success("POSITION", f"持仓检查完成", {
            "持仓数": pnl['holding_count'],
            "盈亏": f"${pnl['total_pnl']:+,.2f} ({pnl['total_pnl_pct']:+.2f}%)",
            "告警": len(alerts),
            "报告": report_file,
        })
        return True
    except Exception as e:
        logger.log_error("POSITION", e)
        return False


# ============================================================
# 自动执行链路（LLM 高置信度决策）
# ============================================================

def _auto_execute(logger, alerts, position_manager, db):
    """
    根据 LLM 高置信度决策自动执行交易。

    执行规则：
    - STOP_LOSS confidence >= 0.7 → 全部卖出
    - TAKE_PROFIT confidence >= 0.7 → 全部卖出
    - REDUCE confidence >= 0.8 → 卖出 50%
    """
    EXECUTE_THRESHOLD = {
        'llm_stop_loss':   {'min_conf': 0.7, 'action': 'sell_all'},
        'llm_take_profit': {'min_conf': 0.7, 'action': 'sell_all'},
        'llm_reduce':      {'min_conf': 0.8, 'action': 'sell_half'},
    }

    executed = []

    try:
        from executors.longbridge import LongbridgeExecutor
        executor = LongbridgeExecutor()

        for alert in alerts:
            alert_type = alert.get('type', '')
            if alert_type not in EXECUTE_THRESHOLD:
                continue

            rule = EXECUTE_THRESHOLD[alert_type]
            confidence = alert.get('confidence', 0)
            symbol = alert.get('symbol', '')

            if confidence < rule['min_conf']:
                logger.log_info("AUTO_EXEC", f"{symbol}: LLM {alert_type} confidence={confidence:.0%} < {rule['min_conf']:.0%}，跳过")
                continue

            # 获取持仓数量
            qty = position_manager.get_quantity(symbol)
            if qty <= 0:
                logger.log_warning("AUTO_EXEC", f"{symbol}: 无持仓，跳过")
                continue

            # 计算执行数量
            if rule['action'] == 'sell_all':
                sell_qty = qty
            elif rule['action'] == 'sell_half':
                sell_qty = max(1, qty // 2)
            else:
                continue

            logger.log_success("AUTO_EXEC", f"{symbol}: LLM {alert_type} (confidence={confidence:.0%}) → 执行卖出 {sell_qty} 股")

            # 执行卖出（Dry Run 模式，实际执行改为 dry_run=False）
            try:
                result = executor.sell(symbol, sell_qty)
                logger.log_success("AUTO_EXEC", f"{symbol}: 卖出 {sell_qty} 股 → {result}")

                # 记录交易到数据库
                db.conn.execute(
                    """INSERT INTO trades (timestamp, symbol, action, quantity, price, source, status)
                       VALUES (?, ?, 'SELL', ?, 0, 'LLM_AUTO', 'EXECUTED')""",
                    (datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"), symbol, sell_qty)
                )
                db.conn.commit()

                executed.append({
                    'symbol': symbol,
                    'action': 'SELL',
                    'quantity': sell_qty,
                    'alert_type': alert_type,
                    'confidence': confidence,
                })
            except Exception as e:
                logger.log_error("AUTO_EXEC", Exception(f"{symbol} 执行失败: {e}"))

    except Exception as e:
        logger.log_error("AUTO_EXEC", e)

    return executed


# ============================================================
# 选股扫描
# ============================================================

def run_screener(logger, auto_analyze_top_n: int = 10):
    """
    选股扫描 + 自动触发交易分析。

    Args:
        auto_analyze_top_n: 选股完成后，自动将 Top N 喂给交易分析做深度研判。
                            设为 0 则仅扫描不触发分析。
    """
    logger.log_info("SCREENER", "开始选股扫描")

    try:
        from storage import Database
        from analysis.screener import StockScreener

        db = Database()
        db.init_schema()
        screener = StockScreener(db)
        results = screener.screen(top_n=20, min_score=0.3)
        db.close()

        ts = datetime.now(CST).strftime("%Y%m%d_%H%M")
        report_file = os.path.join(LOG_DIR, f"screener_{ts}.md")

        with open(report_file, "w", encoding="utf-8") as f:
            f.write(f"# 🔍 选股扫描结果\n\n**时间:** {now_str()}\n\n")
            if not results:
                f.write("无股票通过筛选\n")
                logger.log_info("SCREENER", "无股票通过筛选")
            else:
                for i, r in enumerate(results, 1):
                    f.write(f"{i}. **{r['symbol']}** score={r['total_score']:.3f}\n")
                logger.log_success("SCREENER", f"筛选完成", {"通过": len(results)})

                # ─── 自动触发交易分析 ───
                if auto_analyze_top_n > 0:
                    # 获取已持仓标的，Top 5 跳过持仓股
                    try:
                        db3 = Database()
                        db3.init_schema()
                        held = db3.conn.execute(
                            "SELECT symbol FROM holdings WHERE active = 1 AND quantity > 0"
                        ).fetchall()
                        held_symbols = set(r[0] for r in held)
                        db3.close()
                    except Exception:
                        held_symbols = set()

                    # 从选股结果中排除持仓股，向后顺延
                    non_held = [r for r in results if r["symbol"] not in held_symbols]
                    top_symbols = [r["symbol"] for r in non_held[:auto_analyze_top_n]]
                    
                    skipped = [r["symbol"] for r in results[:auto_analyze_top_n] 
                               if r["symbol"] in held_symbols]
                    if skipped:
                        logger.log_info("SCREENER", f"跳过持仓股: {', '.join(skipped)}，向后顺延")

                    logger.log_info("SCREENER", f"选股完成，自动触发交易分析: {', '.join(top_symbols)}")

                    # 将选股结果写入 watchlist 供后续分析使用
                    try:
                        from storage import Database
                        db2 = Database()
                        db2.init_schema()
                        for r in results[:auto_analyze_top_n]:
                            db2.conn.execute(
                                "INSERT OR REPLACE INTO watchlist (symbol, active) VALUES (?, 1)",
                                (r["symbol"],)
                            )
                        db2.conn.commit()
                        db2.close()
                    except Exception as e:
                        logger.log_warning("SCREENER", f"更新 watchlist 失败: {e}")

                    # 直接触发交易分析
                    run_trading_analysis(logger, top_symbols, from_screener=True)

        return results
    except Exception as e:
        logger.log_error("SCREENER", e)
        return []


# ============================================================
# 主守护进程
# ============================================================

class TradingDaemon:
    """24/7 交易守护进程"""

    def __init__(self, symbols=None):
        self.symbols = symbols or WATCHLIST
        self.running = True
        self.logger = ErrorLogger(LOG_DIR)

        # 上次执行时间
        self.last_run = {
            "collect": 0,
            "news": 0,
            "analysis": 0,
            "position": 0,
            "screener": 0,
            "report": 0,
        }

        # 选股与持仓错峰执行：选股在持仓检查的半周期偏移处触发
        self.task_offsets = {
            "screener": 0.5,   # 选股偏移 50% 周期
        }

        # 信号处理
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        self.running = False
        self.logger.log_info("DAEMON", f"收到信号 {signum}，安全退出...")

    def should_run(self, task, interval):
        now = time.time()
        elapsed = now - self.last_run[task]
        # 应用错峰偏移
        offset_ratio = self.task_offsets.get(task, 0)
        if offset_ratio > 0 and self.last_run[task] == 0:
            # 首次执行：应用偏移，延迟一半周期后再开始
            return elapsed >= interval * (1 - offset_ratio)
        return elapsed >= interval

    def _staggered_init(self):
        """初始化错峰时间：选股在持仓的半周期偏移处启动"""
        now = time.time()
        # 持仓立即执行（last_run=0），选股延迟半周期
        self.last_run["screener"] = now  # 首次会在 half-interval 后触发

    def run(self):
        """主循环"""
        self.logger.log_info("DAEMON", "=" * 60)
        self.logger.log_info("DAEMON", "🚀 US Data Hub 24/7 交易系统启动")
        self.logger.log_info("DAEMON", f"标的: {', '.join(self.symbols)}")
        self.logger.log_info("DAEMON", f"日志目录: {LOG_DIR}")
        self.logger.log_info("DAEMON", "=" * 60)

        # 初始全量采集
        self.logger.log_info("DAEMON", "执行初始全量数据采集...")
        collect_data(self.logger, self.symbols)
        self.last_run["collect"] = time.time()

        # 持仓与选股错峰：首次运行错开半周期
        self._staggered_init()

        self.logger.log_info("DAEMON", "进入主循环...")

        while self.running:
            try:
                freq, phase_label = get_freq()
                now = time.time()

                # 状态显示
                next_collect = int(freq.get("collect", 900) - (now - self.last_run["collect"]))
                next_analysis = int(freq.get("analysis", 1800) - (now - self.last_run["analysis"]))
                next_position = int(freq.get("position", 3600) - (now - self.last_run["position"]))
                next_screener = int(freq.get("screener", 3600) - (now - self.last_run["screener"]))

                print(f"\n{'='*60}")
                print(f"📡 市场状态: {phase_label}  |  {now_str()}")
                print(f"   下次采集: {max(0, next_collect)}s")
                print(f"   下次分析: {max(0, next_analysis)}s")
                print(f"   下次持仓: {max(0, next_position)}s")
                print(f"   下次选股: {max(0, next_screener)}s")
                print(f"   错误: {self.logger.error_count} | 警告: {self.logger.warning_count} | 成功: {self.logger.task_count}")
                print(f"{'='*60}")

                # 1. 数据采集
                if self.should_run("collect", freq.get("collect", 900)):
                    self.logger.log_info("SCHEDULER", f"触发数据采集 (interval={freq.get('collect', 900)}s)")
                    collect_data(self.logger, self.symbols)
                    self.last_run["collect"] = time.time()

                # 2. 新闻采集
                if self.should_run("news", freq.get("news", 1800)):
                    self.logger.log_info("SCHEDULER", f"触发新闻采集 (interval={freq.get('news', 1800)}s)")
                    collect_news(self.logger, self.symbols)
                    self.last_run["news"] = time.time()

                # 3. 交易分析
                if self.should_run("analysis", freq.get("analysis", 1800)):
                    self.logger.log_info("SCHEDULER", f"触发交易分析 (interval={freq.get('analysis', 1800)}s)")
                    run_trading_analysis(self.logger, self.symbols)
                    self.last_run["analysis"] = time.time()

                # 4. 持仓检查
                if self.should_run("position", freq.get("position", 900)):
                    self.logger.log_info("SCHEDULER", f"触发持仓检查 (interval={freq.get('position', 900)}s)")
                    check_position(self.logger)
                    self.last_run["position"] = time.time()

                # 5. 选股扫描 → 自动触发 Top 5 交易分析
                if self.should_run("screener", freq.get("screener", 3600)):
                    self.logger.log_info("SCHEDULER", f"触发选股扫描 (interval={freq.get('screener', 3600)}s)")
                    run_screener(self.logger, auto_analyze_top_n=10)
                    self.last_run["screener"] = time.time()

                # 6. 定期汇总
                if self.should_run("report", freq.get("report", 7200)):
                    self.logger.log_summary()
                    self.last_run["report"] = time.time()

                # 心跳等待
                time.sleep(30)

            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.log_error("DAEMON", e, {"traceback": traceback.format_exc()})
                time.sleep(60)  # 出错后等待 1 分钟

        # 退出
        self.logger.log_summary()
        self.logger.log_info("DAEMON", "=" * 60)
        self.logger.log_info("DAEMON", "🏁 交易系统退出")
        self.logger.log_info("DAEMON", f"错误: {self.logger.error_count} | 警告: {self.logger.warning_count} | 成功: {self.logger.task_count}")
        self.logger.log_info("DAEMON", f"错误日志: {self.logger.error_log}")
        self.logger.log_info("DAEMON", f"运行报告: {self.logger.run_log}")
        self.logger.log_info("DAEMON", "=" * 60)


if __name__ == "__main__":
    daemon = TradingDaemon()
    daemon.run()
