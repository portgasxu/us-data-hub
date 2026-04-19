"""
News dataflow for TradingAgents using local SQLite + Google News.
Replaces yfinance news provider.
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta
import sqlite3
import os
import logging

from tradingagents.strategies.dynamic_thresholds import DynamicThresholds
from tradingagents.dataflows.config import get_config

logger = logging.getLogger(__name__)

# Path to the us-data-hub database
DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "data", "us_data_hub.db"
)


def _get_db():
    """Get database connection."""
    if not os.path.exists(DB_PATH):
        return None
    return sqlite3.connect(DB_PATH)


def get_news_yfinance(
    ticker: str,
    start_date: str,
    end_date: str,
) -> str:
    """
    Retrieve news for a stock ticker from the local database.
    Sources: Google News, Longbridge News.
    """
    conn = _get_db()
    if conn is None:
        return f"No local database found. News not available for {ticker}."

    try:
        cursor = conn.execute("""
            SELECT title, content, source, timestamp, sentiment_score
            FROM data_points
            WHERE symbol = ? AND type = 'news'
            AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp DESC
        """, (ticker, start_date, end_date + "T23:59:59"))

        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return f"No news found for {ticker} between {start_date} and {end_date}"

        news_str = f"## {ticker} News, from {start_date} to {end_date}:\n\n"
        for row in rows:
            title, content, source, ts, sentiment = row
            sent_str = ""
            if sentiment is not None:
                try:
                    sent_val = float(sentiment)
                    sent_str = f" [Sentiment: {sent_val:+.2f}]"
                except (ValueError, TypeError):
                    pass
            news_str += f"### {title}{sent_str} (source: {source})\n"
            if content:
                import json
                try:
                    c = json.loads(content) if isinstance(content, str) else content
                    if isinstance(c, dict):
                        summary = c.get("summary", c.get("snippet", c.get("description", "")))
                        url = c.get("url", "")
                        if summary:
                            news_str += f"{summary}\n"
                        if url:
                            news_str += f"Link: {url}\n"
                except (json.JSONDecodeError, TypeError):
                    pass
            news_str += f"Date: {ts}\n\n"

        return news_str
    except Exception as e:
        conn.close()
        return f"Error fetching news for {ticker}: {str(e)}"


def get_global_news_yfinance(
    curr_date: str,
    look_back_days: int = None,
    limit: int = 10,
) -> str:
    """
    Retrieve global/macro economic news from the local database.

    Args:
        curr_date: Current date
        look_back_days: Days to look back (falls back to config default)
        limit: Max articles to return
    """
    dt = DynamicThresholds(get_config())
    if look_back_days is None:
        look_back_days = dt.get_analysis_window("global_news")
    conn = _get_db()
    if conn is None:
        return "No local database found. Global news not available."

    try:
        start_dt = datetime.strptime(curr_date, "%Y-%m-%d") - relativedelta(days=look_back_days)
        start_str = start_dt.strftime("%Y-%m-%dT00:00:00")

        # Get general financial/economic news (no specific symbol)
        cursor = conn.execute("""
            SELECT title, content, source, timestamp, symbol
            FROM data_points
            WHERE type = 'news'
            AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (start_str, curr_date + "T23:59:59", limit))

        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return f"No global news found for period ending {curr_date}"

        news_str = f"## Global Market News, from {start_dt.strftime('%Y-%m-%d')} to {curr_date}:\n\n"
        for row in rows:
            title, content, source, ts, symbol = row
            sym_tag = f" ({symbol})" if symbol else ""
            news_str += f"### {title}{sym_tag} (source: {source})\n"
            news_str += f"Date: {ts}\n\n"

        return news_str
    except Exception as e:
        conn.close()
        return f"Error fetching global news: {str(e)}"
