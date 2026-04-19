from langchain_core.tools import tool
from typing import Annotated
from tradingagents.dataflows.interface import route_to_vendor
from tradingagents.strategies.dynamic_thresholds import DynamicThresholds
from tradingagents.dataflows.config import get_config


@tool
def get_news(
    query: Annotated[str, "Search query (e.g., ticker symbol or company name)"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"] = None,
    end_date: Annotated[str, "End date in yyyy-mm-dd format"] = None,
) -> str:
    """
    Retrieve news data for a given query.
    Uses the configured news_data vendor.
    Args:
        query (str): Search query (e.g., ticker symbol or company name)
        start_date (str): Start date in yyyy-mm-dd format (optional, defaults to config window)
        end_date (str): End date in yyyy-mm-dd format (optional, defaults to today)
    Returns:
        str: A formatted string containing news data
    """
    dt = DynamicThresholds(get_config())
    if start_date is None or end_date is None:
        from datetime import datetime, timedelta
        window_days = dt.get_analysis_window("news", query)
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=window_days)).strftime("%Y-%m-%d")
    return route_to_vendor("get_news", query, start_date, end_date)


@tool
def get_global_news(
    curr_date: Annotated[str, "Current date in yyyy-mm-dd format"] = None,
    look_back_days: Annotated[int, "Number of days to look back"] = None,
    limit: Annotated[int, "Maximum number of articles to return"] = 5,
) -> str:
    """
    Retrieve global/macro economic news data.
    Uses the configured news_data vendor.
    Args:
        curr_date (str): Current date in yyyy-mm-dd format (optional, defaults to today)
        look_back_days (int): Number of days to look back (falls back to config default)
        limit (int): Maximum number of articles to return (default 5)
    Returns:
        str: A formatted string containing global news data
    """
    dt = DynamicThresholds(get_config())
    if look_back_days is None:
        look_back_days = dt.get_analysis_window("global_news")
    if curr_date is None:
        from datetime import datetime
        curr_date = datetime.now().strftime("%Y-%m-%d")
    return route_to_vendor("get_global_news", curr_date, look_back_days, limit)


@tool
def get_insider_transactions(
    ticker: Annotated[str, "ticker symbol"],
) -> str:
    """
    Retrieve insider transaction information about a company.
    Uses the configured news_data vendor.
    Args:
        ticker (str): Ticker symbol of the company
    Returns:
        str: A report of insider transaction data
    """
    return route_to_vendor("get_insider_transactions", ticker)
