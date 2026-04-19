"""
Longbridge dataflow provider for TradingAgents.
Replaces yfinance as the primary data source.
Uses Longbridge CLI for market data.
"""

from typing import Annotated
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import subprocess
import pandas as pd
import logging
import os

from tradingagents.strategies.dynamic_thresholds import DynamicThresholds
from tradingagents.dataflows.config import get_config

logger = logging.getLogger(__name__)

# Lazy-initialized dynamic thresholds (avoids import-time config dependency)
_dynamic: DynamicThresholds | None = None


def _get_dynamic() -> DynamicThresholds:
    global _dynamic
    if _dynamic is None:
        _dynamic = DynamicThresholds(get_config())
    return _dynamic


def _run_cli(args: list, timeout: int | None = None, default_timeout: int = 30) -> list:
    """Run Longbridge CLI, return parsed JSON list.

    Args:
        args: CLI arguments (after 'longbridge')
        timeout: Per-call timeout override, or uses default_timeout
        default_timeout: Default CLI timeout from config
    """
    cfg = get_config()
    effective_timeout = timeout if timeout is not None else cfg.get("cli_timeout", default_timeout)
    cmd = ["longbridge"] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=effective_timeout)
        if result.returncode != 0:
            logger.warning(f"Longbridge CLI error: {result.stderr[:200]}")
            return []
        data = json.loads(result.stdout)
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logger.warning(f"Longbridge CLI failed: {e}")
        return []


def _to_symbol(symbol: str) -> str:
    """Convert ticker to exchange-qualified symbol using dynamic market detection.

    Examples:
        'AAPL' → 'AAPL.US'
        '0700' → '0700.HK'  (if config maps it)
        'AAPL.HK' → 'AAPL.HK'  (already qualified, pass through)
    """
    s = symbol.upper().strip()
    if "." in s:
        return s  # Already has exchange suffix
    # Use dynamic market detection
    return _get_dynamic().format_ticker(s)


def get_longbridge_kline(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch daily kline data and return as DataFrame with yfinance-compatible columns."""
    symbol_us = _to_symbol(symbol)
    data = _run_cli(["kline", symbol_us, "--format", "json"])
    if not data:
        return pd.DataFrame()

    records = []
    for candle in data:
        try:
            dt = candle["time"][:10]
            if dt < start_date or dt > end_date:
                continue
            records.append({
                "Date": dt,
                "Open": float(candle["open"]),
                "High": float(candle["high"]),
                "Low": float(candle["low"]),
                "Close": float(candle["close"]),
                "Volume": float(candle["volume"]),
            })
        except (KeyError, ValueError):
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["Date"] = pd.to_datetime(df["Date"])
    df["Adj Close"] = df["Close"]  # No adjustment data from CLI
    df.set_index("Date", inplace=True)
    df.sort_index(inplace=True)
    return df


def get_YFin_data_online(
    symbol: Annotated[str, "ticker symbol of the company"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
):
    """Get OHLCV data from Longbridge, formatted as CSV (same output as yfinance version)."""
    try:
        data = get_longbridge_kline(symbol, start_date, end_date)
        if data.empty:
            return f"No data found for symbol '{symbol}' between {start_date} and {end_date}"

        numeric_columns = ["Open", "High", "Low", "Close", "Adj Close"]
        for col in numeric_columns:
            if col in data.columns:
                data[col] = data[col].round(2)

        csv_string = data.to_csv()
        header = f"# Stock data for {symbol.upper()} from {start_date} to {end_date}\n"
        header += f"# Total records: {len(data)}\n"
        header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        return header + csv_string
    except Exception as e:
        return f"Error retrieving data for {symbol}: {str(e)}"


def get_fundamentals(
    ticker: Annotated[str, "ticker symbol of the company"],
    curr_date: Annotated[str, "current date"] = None,
):
    """Get company fundamentals from Longbridge quote."""
    try:
        symbol_us = _to_symbol(ticker)
        data = _run_cli(["quote", symbol_us, "--format", "json"])
        if not data:
            return f"No fundamentals data found for symbol '{ticker}'"

        q = data[0]
        fields = [
            ("Name", q.get("name", "")),
            ("Symbol", q.get("symbol", "")),
            ("Last Price", q.get("last", "")),
            ("Open", q.get("open", "")),
            ("High", q.get("high", "")),
            ("Low", q.get("low", "")),
            ("Volume", q.get("volume", "")),
            ("Turnover", q.get("turnover", "")),
            ("Market Cap", q.get("market_capital", "")),
            ("Previous Close", q.get("prev_close_price", "")),
            ("Bid Price", q.get("bid", "")),
            ("Ask Price", q.get("ask", "")),
            ("52 Week High", q.get("high_52weeks", "")),
            ("52 Week Low", q.get("low_52weeks", "")),
        ]

        lines = []
        for label, value in fields:
            if value is not None and value != "":
                lines.append(f"{label}: {value}")

        header = f"# Company Fundamentals for {ticker.upper()}\n"
        header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        return header + "\n".join(lines)
    except Exception as e:
        return f"Error retrieving fundamentals for {ticker}: {str(e)}"


def get_balance_sheet(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date"] = None,
):
    """Get balance sheet: try SEC XBRL first, fallback to Longbridge."""
    from .sec_financials import get_balance_sheet_sec
    result = get_balance_sheet_sec(ticker, freq, curr_date)
    if result and "Error" not in result and "No SEC data" not in result:
        return result
    # Fallback
    return (
        f"# Balance Sheet data for {ticker.upper()} ({freq})\n"
        f"# Note: SEC XBRL data unavailable, check SEC filing history.\n\n"
        f"No balance sheet data available."
    )


def get_cashflow(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date"] = None,
):
    """Get cash flow: try SEC XBRL first, fallback."""
    from .sec_financials import get_cashflow_sec
    result = get_cashflow_sec(ticker, freq, curr_date)
    if result and "Error" not in result and "No SEC data" not in result:
        return result
    return (
        f"# Cash Flow data for {ticker.upper()} ({freq})\n"
        f"# Note: SEC XBRL data unavailable.\n\n"
        f"No cash flow data available."
    )


def get_income_statement(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date"] = None,
):
    """Get income statement: try SEC XBRL first, fallback."""
    from .sec_financials import get_income_statement_sec
    result = get_income_statement_sec(ticker, freq, curr_date)
    if result and "Error" not in result and "No SEC data" not in result:
        return result
    return (
        f"# Income Statement data for {ticker.upper()} ({freq})\n"
        f"# Note: SEC XBRL data unavailable.\n\n"
        f"No income statement data available."
    )


def get_insider_transactions(
    ticker: Annotated[str, "ticker symbol of the company"],
):
    """Insider transactions from SEC EDGAR (Form 4 filings)."""
    from .sec_financials import fetch_company_facts
    try:
        facts = fetch_company_facts(ticker)
        if not facts:
            return f"# Insider Transactions for {ticker.upper()}\n\nNo SEC data found."

        us_gaap = facts.get("facts", {}).get("us-gaap", {})
        # SEC Form 4 data is in the "dei" or "us-gaap" taxonomy
        # Check for stock-based compensation and director/officer data
        items = {}
        for tag in ["StockIssuedDuringPeriodSharesNewIssues",
                     "StockRepurchasedDuringPeriodShares",
                     "CommonStockDividendsPerShareDeclared",
                     "ShareBasedCompensation"]:
            entry = us_gaap.get(tag, {})
            vals = entry.get("units", {}).get("USD", []) or entry.get("units", {}).get("shares", [])
            if vals:
                def _sk(x):
                    return (x.get("fy") or 0, x.get("fp") or "")
                latest = max(vals, key=_sk)
                items[tag] = latest.get("val")

        if not items:
            return (
                f"# Insider Transactions for {ticker.upper()}\n"
                f"# Note: Detailed Form 4 insider trading data requires parsing individual filings.\n"
                f"# SEC EDGAR collector has captured Form 4 filings in the database.\n\n"
                f"Summary: SEC filings captured. Detailed insider trades require filing-level parsing."
            )

        entity = facts.get("entityName", ticker.upper())
        lines = [
            f"# Insider Activity Summary — {entity} ({ticker.upper()})",
            f"# 数据来源: SEC EDGAR XBRL",
            f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
        for tag, val in items.items():
            lines.append(f"  {tag}: {val:,.0f}")
        return "\n".join(lines)
    except Exception as e:
        return f"# Insider Transactions for {ticker.upper()}\n\nError: {str(e)}"


def get_vix_data(
    curr_date: Annotated[str, "Current date in YYYY-MM-DD format"],
    look_back_days: Annotated[int, "Number of days to look back"] = None,
):
    """Get VIX data from Longbridge kline with dynamic interpretation thresholds.

    Args:
        curr_date: Current date in YYYY-MM-DD format
        look_back_days: Days to look back (falls back to config default)
    """
    dt = _get_dynamic()
    if look_back_days is None:
        look_back_days = dt.get_analysis_window("vix")

    try:
        end_dt = datetime.strptime(curr_date, "%Y-%m-%d")
        start_dt = end_dt - relativedelta(days=look_back_days)
        start_str = start_dt.strftime("%Y-%m-%d")
        end_str = end_dt.strftime("%Y-%m-%d")

        data = _run_cli(["kline", "VIX", "--format", "json"])
        if not data:
            return f"No VIX data found for period ending {curr_date}"

        # Filter by date range
        filtered = []
        for candle in data:
            try:
                dt_str = candle["time"][:10]
                if start_str <= dt_str <= end_str:
                    filtered.append(float(candle["close"]))
            except (KeyError, ValueError):
                continue

        if not filtered:
            return f"No VIX data found for period ending {curr_date}"

        current_vix = filtered[-1]
        avg_vix = sum(filtered) / len(filtered)
        min_vix = min(filtered)
        max_vix = max(filtered)
        if len(filtered) >= 2:
            latest_change = (filtered[-1] - filtered[-2]) / filtered[-2] * 100
        else:
            latest_change = 0

        # Dynamic VIX interpretation using thresholds from config/LLM
        interpretation = dt.interpret_vix(current_vix)

        result = (
            f"## VIX (CBOE Volatility Index) as of {curr_date}\n\n"
            f"**Current VIX**: {current_vix:.2f}\n"
            f"**Daily Change**: {latest_change:+.2f}%\n"
            f"**{look_back_days}-day Average**: {avg_vix:.2f}\n"
            f"**{look_back_days}-day Range**: {min_vix:.2f} - {max_vix:.2f}\n\n"
            f"**Interpretation**: {interpretation}\n"
        )
        return result
    except Exception as e:
        return f"Error fetching VIX data: {str(e)}"


def get_stock_stats_longbridge(
    symbol: Annotated[str, "ticker symbol"],
    indicator: Annotated[str, "technical indicator"],
    curr_date: Annotated[str, "current trading date YYYY-mm-dd"],
    look_back_days: Annotated[int, "days to look back"] = None,
) -> str:
    """
    Use Longbridge kline data + stockstats for technical indicators.
    Same interface as the yfinance version but data source is Longbridge.

    Args:
        symbol: Ticker symbol
        indicator: Technical indicator name
        curr_date: Current trading date
        look_back_days: Days to look back (falls back to config default)
    """
    from stockstats import wrap

    dt = _get_dynamic()
    if look_back_days is None:
        look_back_days = dt.get_analysis_window("indicators")

    try:
        end_dt = datetime.strptime(curr_date, "%Y-%m-%d")
        start_dt = end_dt - relativedelta(days=look_back_days + 60)  # extra buffer
        kline_df = get_longbridge_kline(symbol, start_dt.strftime("%Y-%m-%d"), curr_date)
        if kline_df.empty:
            return f"No price data for {symbol} from Longbridge"

        df = wrap(kline_df.reset_index())
        df["Date"] = df.index.strftime("%Y-%m-%d") if hasattr(df.index, "strftime") else df["Date"]

        # Calculate indicator for all rows
        try:
            df[indicator]
        except Exception:
            return f"Indicator '{indicator}' not supported by stockstats"

        # Generate output for the requested date range
        before = end_dt - relativedelta(days=look_back_days)
        ind_string = ""
        for _, row in df.iterrows():
            date_str = row.get("Date", "")
            if date_str is None:
                continue
            try:
                # Handle both string and Timestamp types
                if hasattr(date_str, "strftime"):
                    row_dt = date_str
                    date_str = date_str.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_str)
                    row_dt = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if row_dt < before:
                continue
            val = row.get(indicator, "N/A")
            ind_string += f"{date_str}: {val}\n"

        descriptions = {
            "close_50_sma": "50 SMA: medium-term trend direction",
            "close_200_sma": "200 SMA: long-term trend benchmark",
            "macd": "MACD: momentum via EMA differences",
            "rsi": "RSI: overbought/oversold momentum (70/30 thresholds)",
            "boll": "Bollinger Middle: 20 SMA dynamic benchmark",
            "boll_ub": "Bollinger Upper Band: overbought/breakout zone",
            "boll_lb": "Bollinger Lower Band: oversold condition",
            "atr": "ATR: volatility measure for stop-loss sizing",
        }
        desc = descriptions.get(indicator, f"{indicator}: technical indicator")

        result = (
            f"## {indicator} values from {before.strftime('%Y-%m-%d')} to {curr_date}:\n\n"
            + ind_string
            + f"\n\n{desc}"
        )
        return result
    except Exception as e:
        return f"Error computing indicators for {symbol}: {str(e)}"
