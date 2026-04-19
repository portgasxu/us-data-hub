"""Google News RSS data source for TradingAgents.

Fetches news from Google News RSS feed — free, no API key required.
Uses dynamic locale detection based on ticker market.
"""

from typing import Annotated
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import urllib.parse
import re

from tradingagents.strategies.dynamic_thresholds import DynamicThresholds
from tradingagents.dataflows.config import get_config


def _get_rss_url(query: str, ticker: str = "") -> str:
    """Build Google News RSS URL with dynamic locale.

    Args:
        query: Search query string
        ticker: Ticker symbol for locale detection

    Returns:
        Full RSS URL with locale parameters
    """
    dt = DynamicThresholds(get_config())
    hl, gl = dt.get_news_locale(ticker)
    return (
        f"https://news.google.com/rss/search?"
        f"q={urllib.parse.quote(query)}&hl={hl}&gl={gl}&ceid={hl.split('-')[0]}:{gl.split('-')[-1].lower()}"
    )


def _parse_rss(xml_content: bytes, limit: int = 20) -> list:
    """Parse Google News RSS XML into a list of article dicts."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return []

    items = root.findall('.//item')
    articles = []
    for item in items[:limit]:
        title_elem = item.find('title')
        source_elem = item.find('source')
        date_elem = item.find('pubDate')
        link_elem = item.find('link')
        desc_elem = item.find('description')

        title = title_elem.text if title_elem is not None else ""
        source = source_elem.text if source_elem is not None else ""
        date = date_elem.text if date_elem is not None else ""
        link = link_elem.text if link_elem is not None else ""
        desc = desc_elem.text if desc_elem is not None else ""

        articles.append({
            "title": title,
            "source": source,
            "date": date,
            "link": link,
            "description": desc,
        })

    return articles


def _format_articles(articles: list, ticker: str = "") -> str:
    """Format article list into a readable report string."""
    if not articles:
        return f"No Google News articles found for {ticker}" if ticker else "No Google News articles found"

    lines = [f"## Google News Articles ({len(articles)} articles)\n"]
    for i, art in enumerate(articles, 1):
        lines.append(f"### {i}. {art['title']}")
        if art.get('source'):
            lines.append(f"   **Source**: {art['source']}")
        if art.get('date'):
            lines.append(f"   **Date**: {art['date']}")
        if art.get('link'):
            lines.append(f"   **Link**: {art['link']}")
        if art.get('description'):
            clean_desc = re.sub(r'<[^>]+>', '', art['description'])
            lines.append(f"   **Summary**: {clean_desc}")
        lines.append("")

    return "\n".join(lines)


def get_news_google_news(
    ticker: Annotated[str, "Ticker symbol"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
) -> str:
    """
    Retrieve news data for a given ticker symbol from Google News RSS.
    Free, no API key required.

    Uses dynamic locale based on ticker market.

    Args:
        ticker: Ticker symbol
        start_date: Start date in yyyy-mm-dd format
        end_date: End date in yyyy-mm-dd format

    Returns:
        str: Formatted string containing news data
    """
    try:
        # LLM-generated search query based on ticker context
        dt = DynamicThresholds(get_config())
        cfg = get_config()
        sector = ""
        queries = dt.generate_news_queries(ticker, sector)
        query = queries[0] if queries else f"{ticker} stock"

        url = _get_rss_url(query, ticker)
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()

        articles = _parse_rss(resp.content, limit=20)

        if start_date and end_date and articles:
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")

                filtered = []
                for art in articles:
                    try:
                        art_dt = datetime.strptime(art['date'], "%a, %d %b %Y %H:%M:%S %Z")
                        if start_dt <= art_dt <= end_dt:
                            filtered.append(art)
                    except (ValueError, KeyError):
                        filtered.append(art)

                if filtered:
                    articles = filtered
            except ValueError:
                pass

        header = (
            f"# Google News for {ticker} from {start_date} to {end_date}\n"
            f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        )
        return header + _format_articles(articles, ticker)

    except requests.RequestException as e:
        return f"Error fetching Google News for {ticker}: {str(e)}"
    except Exception as e:
        return f"Error processing Google News for {ticker}: {str(e)}"


def get_global_news_google_news(
    curr_date: Annotated[str, "Current date in yyyy-mm-dd format"],
    look_back_days: Annotated[int, "Number of days to look back"] = None,
    limit: Annotated[int, "Maximum number of articles to return"] = 15,
) -> str:
    """
    Retrieve global/macro news from Google News RSS.
    Free, no API key required.

    Uses LLM-generated search queries based on market context.

    Args:
        curr_date: Current date in yyyy-mm-dd format
        look_back_days: Number of days to look back (falls back to config default)
        limit: Maximum number of articles to return (default 15)

    Returns:
        str: Formatted string containing global news data
    """
    dt = DynamicThresholds(get_config())
    if look_back_days is None:
        look_back_days = dt.get_analysis_window("global_news")

    try:
        # LLM-generated global news queries
        queries = dt.generate_global_news_queries()
        query = queries[0] if queries else "market economy Federal Reserve stock market"
        url = _get_rss_url(query)
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()

        articles = _parse_rss(resp.content, limit=limit)

        header = (
            f"# Global/Macro News as of {curr_date} (looking back {look_back_days} days)\n"
            f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        )
        return header + _format_articles(articles, "Global")

    except requests.RequestException as e:
        return f"Error fetching Google Global News: {str(e)}"
    except Exception as e:
        return f"Error processing Google Global News: {str(e)}"
