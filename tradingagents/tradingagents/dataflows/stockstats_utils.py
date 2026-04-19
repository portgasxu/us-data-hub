import time
import json
import logging
import subprocess
import os

import pandas as pd
from stockstats import wrap
from typing import Annotated
from .config import get_config

logger = logging.getLogger(__name__)


def safe_retry(func, max_retries=3, base_delay=2.0):
    """Safe retry wrapper — Longbridge doesn't rate-limit like yfinance.
    Just calls the function directly."""
    return func()


def _run_cli(args: list, timeout: int = 30) -> list:
    """Run Longbridge CLI, return parsed JSON list."""
    cmd = ["longbridge"] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            logger.warning(f"Longbridge CLI error: {result.stderr[:200]}")
            return []
        data = json.loads(result.stdout)
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logger.warning(f"Longbridge CLI failed: {e}")
        return []


def _to_symbol(symbol: str) -> str:
    s = symbol.upper().strip()
    if "." not in s:
        return f"{s}.US"
    return s


def _clean_dataframe(data: pd.DataFrame) -> pd.DataFrame:
    """Normalize a stock DataFrame for stockstats."""
    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")
    data = data.dropna(subset=["Date"])
    price_cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in data.columns]
    data[price_cols] = data[price_cols].apply(pd.to_numeric, errors="coerce")
    data = data.dropna(subset=["Close"])
    data[price_cols] = data[price_cols].ffill().bfill()
    return data


def load_ohlcv(symbol: str, curr_date: str) -> pd.DataFrame:
    """Fetch OHLCV data from Longbridge CLI, cached per symbol.

    Uses dynamic cache window from config (default 2 years).
    """
    config = get_config()
    curr_date_dt = pd.to_datetime(curr_date)

    # Dynamic cache window: config "cache_ohlcv_years" or default 2
    cache_years = config.get("cache_ohlcv_years", 2)
    today_date = pd.Timestamp.today()
    start_date = today_date - pd.DateOffset(years=cache_years)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = today_date.strftime("%Y-%m-%d")

    os.makedirs(config["data_cache_dir"], exist_ok=True)
    data_file = os.path.join(
        config["data_cache_dir"],
        f"{symbol}-LB-data-{start_str}-{end_str}.csv",
    )

    if os.path.exists(data_file):
        data = pd.read_csv(data_file, on_bad_lines="skip")
    else:
        symbol_us = _to_symbol(symbol)
        kline_data = _run_cli(["kline", symbol_us, "--format", "json"])
        if not kline_data:
            return pd.DataFrame()

        records = []
        for candle in kline_data:
            try:
                records.append({
                    "Date": candle["time"][:10],
                    "Open": float(candle["open"]),
                    "High": float(candle["high"]),
                    "Low": float(candle["low"]),
                    "Close": float(candle["close"]),
                    "Volume": float(candle["volume"]),
                })
            except (KeyError, ValueError):
                continue

        data = pd.DataFrame(records)
        if not data.empty:
            data.to_csv(data_file, index=False)

    if data.empty:
        return pd.DataFrame()

    data = _clean_dataframe(data)
    data = data[data["Date"] <= curr_date_dt]
    return data


def filter_financials_by_date(data: pd.DataFrame, curr_date: str) -> pd.DataFrame:
    """Drop financial statement columns after curr_date."""
    if not curr_date or data.empty:
        return data
    cutoff = pd.Timestamp(curr_date)
    mask = pd.to_datetime(data.columns, errors="coerce") <= cutoff
    return data.loc[:, mask]


class StockstatsUtils:
    @staticmethod
    def get_stock_stats(
        symbol: Annotated[str, "ticker symbol"],
        indicator: Annotated[str, "quantitative indicators"],
        curr_date: Annotated[str, "current date YYYY-mm-dd"],
    ):
        data = load_ohlcv(symbol, curr_date)
        if data.empty:
            return "N/A: No price data available"
        df = wrap(data)
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
        curr_date_str = pd.to_datetime(curr_date).strftime("%Y-%m-%d")

        try:
            df[indicator]
        except Exception as e:
            logger.warning(f"stockstats indicator error: {e}")
            return f"N/A: indicator '{indicator}' not supported"

        matching_rows = df[df["Date"].str.startswith(curr_date_str)]
        if not matching_rows.empty:
            return matching_rows[indicator].values[0]
        return "N/A: Not a trading day (weekend or holiday)"
