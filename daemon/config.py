"""
US Data Hub — 24/7 守护进程配置
"""
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAEMON_DIR = os.path.join(PROJECT_ROOT, "daemon")
LOG_DIR = os.path.join(DAEMON_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# 标的列表
WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"]

# LLM
LLM_MODEL = "qwen3.6-plus"

# 频率配置（秒）—— 按市场时段划分
# 美股时段 (EDT): 21:30 - 04:00 北京时间
# 盘前/盘后: 非核心时段
# 周末: 休市

FREQ_MARKET = {
    "price":      300,    # 价格采集: 5 分钟
    "news":       600,    # 新闻采集: 10 分钟
    "lb":         600,    # Longbridge: 10 分钟
    "reddit":     1800,   # Reddit: 30 分钟
    "sec":        14400,  # SEC: 4 小时
    "analysis":   1800,   # 交易分析: 30 分钟
    "position":   3600,   # 持仓检查: 1 小时
    "screener":   3600,   # 选股: 1 小时（同频，错位执行）
    "report":     7200,   # 报告: 2 小时
}

FREQ_PRE_AFTER = {
    "price":      600,    # 价格采集: 10 分钟
    "news":       3600,   # 新闻采集: 1 小时
    "lb":         1200,   # Longbridge: 20 分钟
    "reddit":     3600,   # Reddit: 1 小时
    "sec":        28800,  # SEC: 8 小时
    "analysis":   3600,   # 交易分析: 1 小时
    "position":   7200,   # 持仓检查: 2 小时
    "screener":   7200,   # 选股: 2 小时（同频，错位执行）
    "report":     14400,  # 报告: 4 小时
}

FREQ_OFF = {
    "price":      1800,   # 价格采集: 30 分钟
    "news":       3600,   # 新闻采集: 1 小时
    "lb":         3600,   # Longbridge: 1 小时
    "reddit":     7200,   # Reddit: 2 小时
    "sec":        43200,  # SEC: 12 小时
    "analysis":   7200,   # 交易分析: 2 小时
    "position":   43200,  # 持仓检查: 12 小时
    "screener":   43200,  # 选股: 12 小时（同频，错位执行）
    "report":     28800,  # 报告: 8 小时
}

FREQ_WEEKEND = {
    "price":      3600,   # 价格采集: 1 小时
    "news":       7200,   # 新闻采集: 2 小时
    "lb":         7200,   # Longbridge: 2 小时
    "reddit":     14400,  # Reddit: 4 小时
    "sec":        86400,  # SEC: 24 小时
    "analysis":   14400,  # 交易分析: 4 小时
    "position":   43200,  # 持仓检查: 12 小时
    "screener":   43200,  # 选股: 12 小时（同频，错位执行）
    "report":     43200,  # 报告: 12 小时
}

# Yahoo Finance 限流保护（已废弃，yfinance 已移除）
# YAHOO_COOLDOWN = 60  # 不再需要
TICKER_SPACING = 15      # 标的间等待时间（秒）
MAX_RETRIES = 3          # 最大重试次数
