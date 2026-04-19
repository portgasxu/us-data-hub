from langchain_core.tools import tool
from typing import Annotated
from tradingagents.dataflows.interface import route_to_vendor
from tradingagents.strategies.dynamic_thresholds import DynamicThresholds
from tradingagents.dataflows.config import get_config


@tool
def get_stock_data(
    symbol: Annotated[str, "ticker symbol of the company"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"] = None,
    end_date: Annotated[str, "End date in yyyy-mm-dd format"] = None,
) -> str:
    """
    Retrieve stock price data (OHLCV) for a given ticker symbol.
    Uses the configured core_stock_apis vendor.
    Args:
        symbol (str): Ticker symbol of the company, e.g. AAPL, TSM
        start_date (str): Start date in yyyy-mm-dd format (optional, defaults to config window)
        end_date (str): End date in yyyy-mm-dd format (optional, defaults to today)
    Returns:
        str: A formatted dataframe containing the stock price data for the specified ticker symbol in the specified date range.
    """
    if start_date is None or end_date is None:
        from datetime import datetime, timedelta
        dt = DynamicThresholds(get_config())
        window_days = dt.get_analysis_window("stock_data", symbol)
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=window_days)).strftime("%Y-%m-%d")
    return route_to_vendor("get_stock_data", symbol, start_date, end_date)


@tool
def get_vix_data(
    curr_date: Annotated[str, "Current date in YYYY-MM-DD format"] = None,
    look_back_days: Annotated[int, "Number of days to look back"] = None,
) -> str:
    """
    Retrieve VIX (CBOE Volatility Index) data for market volatility analysis.
    Uses the configured market_data vendor.
    Args:
        curr_date: Current date in YYYY-MM-DD format (optional, defaults to today)
        look_back_days: Number of days to look back (falls back to config default)
    Returns:
        str: Formatted string containing VIX data and interpretation
    """
    dt = DynamicThresholds(get_config())
    if look_back_days is None:
        look_back_days = dt.get_analysis_window("vix")
    if curr_date is None:
        from datetime import datetime
        curr_date = datetime.now().strftime("%Y-%m-%d")
    return route_to_vendor("get_vix_data", curr_date, look_back_days)
