"""
US Data Hub — Alphalens Factor Backtesting
Analyzes factor performance using alphalens-reloaded.
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Alphalens import — gracefully degrade if missing
try:
    import alphalens
    HAS_ALPHALENS = True
except ImportError:
    HAS_ALPHALENS = False


class AlphalensBacktest:
    """Run factor backtests using alphalens."""

    def __init__(self, db):
        self.db = db
        self.has_alphalens = HAS_ALPHALENS

    def run_factor_test(
        self,
        factor_name: str = "momentum",
        days: int = 180,
        quantiles: int = 5,
        periods: List[int] = None,
    ) -> Dict:
        """
        Run alphalens factor test.

        Returns:
            Dict with results summary (HTML/images not generated in CLI mode)
        """
        if not self.has_alphalens:
            return {"error": "alphalens not installed", "status": "skipped"}

        if periods is None:
            periods = [1, 5, 10]

        # Fetch price data from DB
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 60)  # buffer for calculations

        prices_data = self.db.conn.execute("""
            SELECT symbol, date, close
            FROM prices
            WHERE date >= ? AND date <= ?
            ORDER BY date
        """, (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))).fetchall()

        if not prices_data:
            return {"error": "No price data in database", "status": "skipped"}

        # Build price DataFrame (columns = symbols, index = dates)
        price_df = pd.DataFrame(prices_data, columns=["symbol", "date", "close"])
        price_df["date"] = pd.to_datetime(price_df["date"])
        price_pivot = price_df.pivot(index="date", columns="symbol", values="close")
        price_pivot = price_pivot.sort_index()

        # Fetch factor data
        factor_data_raw = self.db.conn.execute("""
            SELECT symbol, date, factor_value
            FROM factors
            WHERE factor_name = ? AND date >= ? AND date <= ?
        """, (factor_name, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))).fetchall()

        if not factor_data_raw:
            return {"error": f"No factor data for '{factor_name}'", "status": "skipped"}

        # Build factor DataFrame
        factor_df = pd.DataFrame(factor_data_raw, columns=["symbol", "date", "factor_value"])
        factor_df["date"] = pd.to_datetime(factor_df["date"])
        factor_pivot = factor_df.pivot(index="date", columns="symbol", values="factor_value")
        factor_pivot = factor_pivot.sort_index()

        # Align indices
        common_dates = price_pivot.index.intersection(factor_pivot.index)
        price_pivot = price_pivot.loc[common_dates]
        factor_pivot = factor_pivot.loc[common_dates]

        # Create multi-index factor data
        factor_stacked = factor_pivot.stack()
        factor_stacked.index.names = ["date", "asset"]

        # Calculate forward returns
        from alphalens.utils import get_clean_factor_and_forward_returns

        try:
            clean_data = get_clean_factor_and_forward_returns(
                factor=factor_stacked,
                prices=price_pivot,
                quantiles=quantiles,
                periods=periods,
            )
        except Exception as e:
            return {"error": f"alphalens processing failed: {str(e)}", "status": "error"}

        if clean_data.empty:
            return {"error": "No clean factor data after alignment", "status": "skipped"}

        # Summary statistics
        result = {
            "status": "success",
            "factor": factor_name,
            "days": days,
            "records": len(clean_data),
            "symbols": len(clean_data.index.get_level_values("asset").unique()),
            "date_range": f"{clean_data.index.get_level_values('date').min().strftime('%Y-%m-%d')} to {clean_data.index.get_level_values('date').max().strftime('%Y-%m-%d')}",
            "quantile_mean_returns": {},
        }

        # Mean returns by quantile
        for period in periods:
            col = f"{period}D"
            if col in clean_data.columns:
                q_means = clean_data.groupby("factor_quantile")[col].mean()
                result["quantile_mean_returns"][col] = q_means.to_dict()

        # Information Coefficient (IC) — rank correlation between factor and forward return
        try:
            first_period_col = f"{periods[0]}D"
            if first_period_col in clean_data.columns:
                ic = clean_data["factor"].corr(clean_data[first_period_col], method="spearman")
                result["ic"] = round(ic, 4)
        except Exception:
            result["ic"] = "N/A"

        return result

    def run_all_factors(self, days: int = 180) -> Dict:
        """Run backtest for all available factors."""
        factor_names = self.db.conn.execute(
            "SELECT DISTINCT factor_name FROM factors WHERE factor_value IS NOT NULL"
        ).fetchall()

        results = {}
        for (name,) in factor_names:
            logger.info(f"Backtesting factor: {name}")
            results[name] = self.run_factor_test(factor_name=name, days=days)

        return results


def main():
    import argparse
    from storage import Database

    from dayup_logger import setup_root_logger; setup_root_logger(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Alphalens Factor Backtest")
    parser.add_argument("--factor", default=None, help="Specific factor name (default: all)")
    parser.add_argument("--days", type=int, default=180, help="Look-back days")
    parser.add_argument("--quantiles", type=int, default=5, help="Number of quantiles")
    args = parser.parse_args()

    db = Database()
    backtest = AlphalensBacktest(db)

    if args.factor:
        result = backtest.run_factor_test(args.factor, days=args.days, quantiles=args.quantiles)
        print(json.dumps(result, indent=2))
    else:
        results = backtest.run_all_factors(days=args.days)
        import json
        print(json.dumps(results, indent=2))

    db.close()


if __name__ == "__main__":
    main()
