"""
SEC EDGAR XBRL Financial Statements Parser
============================================
Extract structured balance sheet, income statement, and cash flow
from SEC's Company Facts API (XBRL data).

Replaces the yfinance financials with real SEC filing data.

API: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
"""

import requests
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ─── CIK Lookup ───

_CIK_CACHE = {}
_CIK_FILE = None

def _get_cik_file():
    global _CIK_FILE
    if _CIK_FILE is None:
        import os
        _CIK_FILE = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
            "data", "cik_mapping.json"
        )
    return _CIK_FILE

def load_cik_mapping() -> dict:
    """Load cached CIK mapping from local file or SEC."""
    global _CIK_CACHE
    if _CIK_CACHE:
        return _CIK_CACHE

    cik_file = _get_cik_file()
    if cik_file and os.path.exists(cik_file):
        try:
            with open(cik_file) as f:
                _CIK_CACHE = json.load(f)
            return _CIK_CACHE
        except Exception:
            pass

    # Fetch fresh from SEC
    try:
        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        headers = {"User-Agent": "us-data-hub admin@example.com"}
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            data = r.json()
            mapping = {}
            for item in data.get("data", []):
                mapping[item["ticker"].upper()] = f"{item['cik']:010d}"
            _CIK_CACHE = mapping
            if cik_file:
                os.makedirs(os.path.dirname(cik_file), exist_ok=True)
                with open(cik_file, "w") as f:
                    json.dump(mapping, f)
            return _CIK_CACHE
    except Exception as e:
        logger.warning(f"Failed to fetch CIK mapping: {e}")

    # Fallback: known tickers
    _CIK_CACHE = {
        "AAPL": "0000320193", "MSFT": "0000789019", "GOOGL": "0001652044",
        "GOOG": "0001652044", "AMZN": "0001018724", "NVDA": "0001045810",
        "META": "0001326801", "TSLA": "0001318605", "JPM": "0000019617",
        "V": "0001403161", "JNJ": "0000200406", "WMT": "0000104169",
        "MA": "0001141391", "PG": "0000080424", "HD": "0000354950",
        "DIS": "0001744489", "BAC": "0000070858", "XOM": "0000034088",
        "PFE": "0000078003", "CSCO": "0000858877", "INTC": "0000050863",
        "AMD": "0000002488", "NFLX": "0001065280", "CRM": "0001108524",
        "ORCL": "0001341439", "ADBE": "0000796343", "PYPL": "0001633917",
        "UBER": "0001543151", "COIN": "0001679788", "SHOP": "0001594805",
        "LLY": "0000059478", "NVO": "0000353278", "TSM": "0001046179",
        "ARM": "0001947610", "MU": "0000723125", "AVGO": "0001730168",
        "PLTR": "0001321655", "CRWD": "0001535527", "SNOW": "0001640147",
        "CEG": "0001868274", "VST": "0000021734", "LMT": "0000936340",
    }
    return _CIK_CACHE


def get_cik(symbol: str) -> Optional[str]:
    """Get CIK for a ticker symbol. Returns None if not found (non-US stock)."""
    mapping = load_cik_mapping()
    return mapping.get(symbol.upper())


# ─── Fetch XBRL Facts ───

_HEADERS = {"User-Agent": "us-data-hub admin@example.com", "Accept-Encoding": "gzip, deflate"}

def fetch_company_facts(symbol: str) -> Optional[dict]:
    """Fetch all XBRL facts from SEC Company Facts API."""
    cik = get_cik(symbol)
    if not cik:
        logger.debug(f"No CIK found for {symbol} (may be non-US listing)")
        return None

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.debug(f"SEC Company Facts API failed for {symbol} (CIK={cik}): {e}")
        return None


# ─── Financial Statement Extraction ───

# Mapping of US-GAAP taxonomy tags to financial statement line items
BALANCE_SHEET_ITEMS = {
    "Assets": {"label": "总资产 (Total Assets)", "order": 1},
    "AssetsCurrent": {"label": "流动资产 (Current Assets)", "order": 2},
    "CashAndCashEquivalentsAtCarryingValue": {"label": "现金及等价物", "order": 3},
    "MarketableSecuritiesCurrent": {"label": "短期投资", "order": 4},
    "AccountsReceivableNetCurrent": {"label": "应收账款", "order": 5},
    "InventoryNet": {"label": "存货", "order": 6},
    "PropertyPlantAndEquipmentNet": {"label": "固定资产净值", "order": 7},
    "Goodwill": {"label": "商誉", "order": 8},
    "IntangibleAssetsNetExcludingGoodwill": {"label": "无形资产", "order": 9},
    "Liabilities": {"label": "总负债 (Total Liabilities)", "order": 10},
    "LiabilitiesCurrent": {"label": "流动负债 (Current Liabilities)", "order": 11},
    "AccountsPayableCurrent": {"label": "应付账款", "order": 12},
    "LongTermDebtNoncurrent": {"label": "长期债务", "order": 13},
    "StockholdersEquity": {"label": "股东权益 (Total Equity)", "order": 14},
    "RetainedEarningsAccumulatedDeficit": {"label": "留存收益", "order": 15},
    "CommonStocksOutstanding": {"label": "流通股数 (shares)", "order": 16, "unit": "shares"},
}

INCOME_STATEMENT_ITEMS = {
    "Revenues": {"label": "营业收入 (Revenue)", "order": 1},
    "CostOfRevenue": {"label": "营业成本 (COGS)", "order": 2},
    "GrossProfit": {"label": "毛利润 (Gross Profit)", "order": 3},
    "OperatingExpenses": {"label": "营业费用 (Operating Expenses)", "order": 4},
    "ResearchAndDevelopmentExpense": {"label": "研发费用 (R&D)", "order": 5},
    "SellingGeneralAndAdministrativeExpense": {"label": "销售管理费用 (SG&A)", "order": 6},
    "OperatingIncomeLoss": {"label": "营业利润 (Operating Income)", "order": 7},
    "InterestExpense": {"label": "利息支出", "order": 8},
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": {
        "label": "税前利润", "order": 9},
    "IncomeTaxExpenseBenefit": {"label": "所得税", "order": 10},
    "NetIncomeLoss": {"label": "净利润 (Net Income)", "order": 11},
    "EarningsPerShareBasic": {"label": "EPS (基本)", "order": 12},
    "EarningsPerShareDiluted": {"label": "EPS (稀释)", "order": 13},
    "WeightedAverageNumberOfSharesOutstandingBasic": {"label": "加权平均股数 (基本)", "order": 14, "unit": "shares"},
}

CASH_FLOW_ITEMS = {
    "NetIncomeLoss": {"label": "净利润 (Net Income)", "order": 1},
    "DepreciationDepletionAndAmortization": {"label": "折旧摊销 (D&A)", "order": 2},
    "ShareBasedCompensation": {"label": "股权激励 (SBC)", "order": 3},
    "NetCashProvidedByUsedInOperatingActivities": {"label": "经营现金流 (OCF)", "order": 4},
    "PaymentsToAcquirePropertyPlantAndEquipment": {"label": "资本支出 (CapEx)", "order": 5},
    "PaymentsToAcquireBusinessesNetOfCashAcquired": {"label": "收购支出", "order": 6},
    "PaymentsForRepurchaseOfCommonStock": {"label": "股票回购", "order": 7},
    "PaymentsOfDividends": {"label": "分红支出", "order": 8},
    "NetCashProvidedByUsedInInvestingActivities": {"label": "投资现金流 (ICF)", "order": 9},
    "NetCashProvidedByUsedInFinancingActivities": {"label": "融资现金流 (FCF)", "order": 10},
    "NetCashProvidedByUsedInContinuingOperations": {"label": "净现金流变化", "order": 11},
    "CashAndCashEquivalentsAtCarryingValue": {"label": "期末现金余额", "order": 12},
}


def _extract_value(us_gaap: dict, tag: str, unit: str = "USD") -> Optional[float]:
    """Extract the latest value for a given XBRL tag."""
    if tag not in us_gaap:
        return None
    entry = us_gaap[tag]
    units = entry.get("units", {})
    values = units.get(unit, [])
    if not values:
        return None
    # Get the most recent value (handle None fy/fp safely)
    def sort_key(x):
        fy = x.get("fy") or 0
        fp = x.get("fp") or ""
        return (fy, fp)
    latest = max(values, key=sort_key)
    return latest.get("val")


def _format_value(value: Optional[float], unit: str = "USD") -> str:
    """Format a value for display."""
    if value is None:
        return "N/A"
    if unit == "shares":
        if value >= 1e9:
            return f"{value/1e9:.2f}B"
        elif value >= 1e6:
            return f"{value/1e6:.2f}M"
        else:
            return f"{value:,.0f}"
    # USD values
    if abs(value) >= 1e12:
        return f"${value/1e12:.2f}T"
    elif abs(value) >= 1e9:
        return f"${value/1e9:.2f}B"
    elif abs(value) >= 1e6:
        return f"${value/1e6:.2f}M"
    elif abs(value) >= 1:
        return f"${value:,.2f}"
    else:
        return f"${value:.4f}"


def _get_filing_period(us_gaap: dict) -> str:
    """Determine the most recent filing period from available data."""
    for tag in ["Assets", "NetIncomeLoss", "Revenues"]:
        if tag in us_gaap:
            units = us_gaap[tag].get("units", {})
            for unit in units.values():
                if unit:
                    def sort_key(x):
                        fy = x.get("fy") or 0
                        fp = x.get("fp") or ""
                        return (fy, fp)
                    latest = max(unit, key=sort_key)
                    fy = latest.get("fy") or "?"
                    fp = latest.get("fp") or "?"
                    fp_map = {"FY": "年报", "Q1": "Q1", "Q2": "Q2", "Q3": "Q3", "Q4": "Q4"}
                    return f"{fy}年 {fp_map.get(fp, fp)}"
    return "最近可用期间"


# ─── Public API (same interface as longbridge.py financial functions) ───

def get_balance_sheet_sec(
    ticker: str,
    freq: str = "quarterly",
    curr_date: str = None,
) -> str:
    """Get balance sheet from SEC XBRL data."""
    try:
        facts = fetch_company_facts(ticker)
        if not facts:
            return f"# Balance Sheet for {ticker.upper()}\n\nNo SEC data found for {ticker}."

        us_gaap = facts.get("facts", {}).get("us-gaap", {})
        if not us_gaap:
            return f"# Balance Sheet for {ticker.upper()}\n\nNo us-gaap data found."

        period = _get_filing_period(us_gaap)
        entity = facts.get("entityName", ticker.upper())

        lines = [
            f"# Balance Sheet — {entity} ({ticker.upper()})",
            f"# 报告期间: {period}",
            f"# 数据来源: SEC EDGAR XBRL (us-gaap taxonomy)",
            f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]

        items = sorted(BALANCE_SHEET_ITEMS.items(), key=lambda x: x[1]["order"])
        for tag, info in items:
            unit = info.get("unit", "USD")
            val = _extract_value(us_gaap, tag, unit)
            formatted = _format_value(val, unit)
            lines.append(f"  {info['label']}: {formatted}")

        return "\n".join(lines)
    except Exception as e:
        return f"# Balance Sheet for {ticker.upper()}\n\nError: {str(e)}"


def get_income_statement_sec(
    ticker: str,
    freq: str = "quarterly",
    curr_date: str = None,
) -> str:
    """Get income statement from SEC XBRL data."""
    try:
        facts = fetch_company_facts(ticker)
        if not facts:
            return f"# Income Statement for {ticker.upper()}\n\nNo SEC data found for {ticker}."

        us_gaap = facts.get("facts", {}).get("us-gaap", {})
        if not us_gaap:
            return f"# Income Statement for {ticker.upper()}\n\nNo us-gaap data found."

        period = _get_filing_period(us_gaap)
        entity = facts.get("entityName", ticker.upper())

        lines = [
            f"# Income Statement — {entity} ({ticker.upper()})",
            f"# 报告期间: {period}",
            f"# 数据来源: SEC EDGAR XBRL (us-gaap taxonomy)",
            f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]

        items = sorted(INCOME_STATEMENT_ITEMS.items(), key=lambda x: x[1]["order"])
        for tag, info in items:
            unit = info.get("unit", "USD")
            val = _extract_value(us_gaap, tag, unit)
            formatted = _format_value(val, unit)
            lines.append(f"  {info['label']}: {formatted}")

        return "\n".join(lines)
    except Exception as e:
        return f"# Income Statement for {ticker.upper()}\n\nError: {str(e)}"


def get_cashflow_sec(
    ticker: str,
    freq: str = "quarterly",
    curr_date: str = None,
) -> str:
    """Get cash flow statement from SEC XBRL data."""
    try:
        facts = fetch_company_facts(ticker)
        if not facts:
            return f"# Cash Flow for {ticker.upper()}\n\nNo SEC data found for {ticker}."

        us_gaap = facts.get("facts", {}).get("us-gaap", {})
        if not us_gaap:
            return f"# Cash Flow for {ticker.upper()}\n\nNo us-gaap data found."

        period = _get_filing_period(us_gaap)
        entity = facts.get("entityName", ticker.upper())

        lines = [
            f"# Cash Flow Statement — {entity} ({ticker.upper()})",
            f"# 报告期间: {period}",
            f"# 数据来源: SEC EDGAR XBRL (us-gaap taxonomy)",
            f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]

        items = sorted(CASH_FLOW_ITEMS.items(), key=lambda x: x[1]["order"])
        for tag, info in items:
            unit = info.get("unit", "USD")
            val = _extract_value(us_gaap, tag, unit)
            # CapEx is typically negative (outflow), show as positive for readability
            if tag == "PaymentsToAcquirePropertyPlantAndEquipment" and val is not None:
                formatted = _format_value(abs(val), unit)
            else:
                formatted = _format_value(val, unit)
            lines.append(f"  {info['label']}: {formatted}")

        return "\n".join(lines)
    except Exception as e:
        return f"# Cash Flow for {ticker.upper()}\n\nError: {str(e)}"


# ─── Growth Metrics (for Screener Layer 3) ───

def _parse_date(s: str) -> datetime:
    """Parse SEC date string."""
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except:
        return None

def _get_sec_values(tag: str, us_gaap: dict, units: list = None) -> list:
    """Get all values with parsed date metadata. Supports multiple unit types."""
    if tag not in us_gaap:
        return []
    entry = us_gaap[tag]
    all_units = entry.get("units", {})
    if units is None:
        units = ["USD"]
    raw = []
    for u in units:
        raw.extend(all_units.get(u, []))
    result = []
    for v in raw:
        val = v.get("val")
        start = _parse_date(v.get("start", ""))
        end = _parse_date(v.get("end", ""))
        if val is not None and end:
            days = (end - start).days if start else 365
            result.append({
                "val": val,
                "start": start,
                "end": end,
                "days": days,
                "fy": v.get("fy"),
                "fp": v.get("fp", ""),
            })
    return result

def get_sec_growth_metrics(symbol: str) -> Optional[dict]:
    """
    Extract real growth metrics from SEC XBRL data.
    Uses annual filing date ranges to separate FY vs quarterly data.

    Returns:
        {
            'revenue_growth': float,    # YoY revenue growth %
            'net_income_growth': float, # YoY net income growth %
            'eps_growth': float,        # YoY EPS growth %
            'roe': float,               # Return on Equity %
            'gross_margin': float,      # Gross Profit Margin %
            'operating_margin': float,  # Operating Margin %
            'revenue_ttm': float,       # Trailing twelve months revenue
            'net_income_ttm': float,    # TTM net income
        }
    """
    try:
        facts = fetch_company_facts(symbol)
        if not facts:
            return None

        us_gaap = facts.get("facts", {}).get("us-gaap", {})
        if not us_gaap:
            return None

        def get_annuals(tag: str, units: list = None) -> list:
            """Get annual (FY) entries, identified by date range > 270 days."""
            all_vals = _get_sec_values(tag, us_gaap, units)
            return sorted([v for v in all_vals if v["days"] > 270], key=lambda x: x["end"])

        def get_quarters(tag: str, units: list = None) -> list:
            """Get quarterly entries, identified by date range 60-180 days."""
            all_vals = _get_sec_values(tag, us_gaap, units)
            return sorted([v for v in all_vals if 60 <= v["days"] <= 180], key=lambda x: x["end"])

        def calc_annual_yoy(tag: str, units: list = None) -> Optional[float]:
            """YoY growth from last two annual filings."""
            annuals = get_annuals(tag, units)
            if len(annuals) < 2:
                return None
            recent, prior = annuals[-1], annuals[-2]
            if prior["val"] == 0:
                return None
            return ((recent["val"] - prior["val"]) / abs(prior["val"])) * 100

        def calc_ttm_from_quarters(tag: str, count: int = 4, units: list = None) -> Optional[float]:
            """Sum last N quarters as TTM."""
            quarters = get_quarters(tag, units)
            if len(quarters) < 2:
                return None
            recent = quarters[-min(count, len(quarters)):]
            total = sum(v["val"] for v in recent)
            return total if total != 0 else None

        def latest_value(tag: str, units: list = None) -> Optional[float]:
            """Get the most recent value regardless of period type."""
            all_vals = _get_sec_values(tag, us_gaap, units)
            if not all_vals:
                return None
            return max(all_vals, key=lambda x: x["end"])["val"]

        # Revenue (try multiple possible tags)
        revenue_growth = None
        revenue_ttm = None
        rev_latest = None
        for rev_tag in ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
                         "SalesOfGoodsAndServices", "Revenue", "SalesRevenueNet"]:
            annuals = get_annuals(rev_tag)
            if annuals:
                revenue_growth = calc_annual_yoy(rev_tag)
                revenue_ttm = calc_ttm_from_quarters(rev_tag)
                rev_latest = latest_value(rev_tag)
                break

        # Net income (try multiple tags)
        net_income_growth = None
        net_income_ttm = None
        for ni_tag in ["NetIncomeLoss", "NetIncomeLossFromContinuingOperations",
                        "ProfitLoss", "NetIncome", "NetIncomeAttributableToParent"]:
            annuals = get_annuals(ni_tag)
            if annuals:
                net_income_growth = calc_annual_yoy(ni_tag)
                net_income_ttm = calc_ttm_from_quarters(ni_tag)
                break

        # EPS (uses USD/shares unit)
        eps_growth = calc_annual_yoy("EarningsPerShareDiluted", units=["USD/shares", "shares"])

        # ROE = Net Income / Equity
        roe = None
        ni_latest = latest_value("NetIncomeLoss")
        eq_latest = latest_value("StockholdersEquity")
        if ni_latest is not None and eq_latest and eq_latest != 0:
            roe = (ni_latest / eq_latest) * 100

        # Gross margin = Gross Profit / Revenue (must use same period type)
        gross_margin = None
        gp_annuals = get_annuals("GrossProfit")
        rev_annuals = get_annuals("Revenues") or get_annuals("SalesOfGoodsAndServices")
        if gp_annuals and rev_annuals:
            # Find matching end dates (same fiscal period)
            gp_latest = gp_annuals[-1]
            best_rev = min(rev_annuals, key=lambda x: abs((x["end"] - gp_latest["end"]).days))
            if abs(best_rev["end"] - gp_latest["end"]).days <= 30 and best_rev["val"] != 0:
                gross_margin = (gp_latest["val"] / best_rev["val"]) * 100
            elif rev_latest and rev_latest != 0:
                gross_margin = (gp_latest["val"] / rev_latest) * 100

        # Operating margin
        operating_margin = None
        oi_annuals = get_annuals("OperatingIncomeLoss")
        if oi_annuals and rev_annuals:
            oi_latest = oi_annuals[-1]
            best_rev = min(rev_annuals, key=lambda x: abs((x["end"] - oi_latest["end"]).days))
            if abs(best_rev["end"] - oi_latest["end"]).days <= 30 and best_rev["val"] != 0:
                operating_margin = (oi_latest["val"] / best_rev["val"]) * 100

        return {
            "revenue_growth": round(revenue_growth, 2) if revenue_growth is not None else None,
            "net_income_growth": round(net_income_growth, 2) if net_income_growth is not None else None,
            "eps_growth": round(eps_growth, 2) if eps_growth is not None else None,
            "roe": round(roe, 2) if roe is not None else None,
            "gross_margin": round(gross_margin, 2) if gross_margin is not None else None,
            "operating_margin": round(operating_margin, 2) if operating_margin is not None else None,
            "revenue_ttm": round(revenue_ttm, 2) if revenue_ttm is not None else None,
            "net_income_ttm": round(net_income_ttm, 2) if net_income_ttm is not None else None,
        }
    except Exception as e:
        logger.warning(f"Failed to extract growth metrics for {symbol}: {e}")
        return None


# ─── Fundamental Factors (for Factor Calculator) ───

def get_sec_fundamental_factors(symbol: str) -> Optional[dict]:
    """
    Extract fundamental factors for the factor calculator.

    Returns:
        {
            'pe_ratio': float,          # P/E ratio (MarketCap / NetIncome)
            'pb_ratio': float,          # P/B ratio (MarketCap / Equity)
            'roe': float,               # Return on Equity %
            'profit_margin': float,     # Net Profit Margin %
            'debt_to_equity': float,    # Total Debt / Equity
            'current_ratio': float,     # Current Assets / Current Liabilities
        }
    """
    try:
        facts = fetch_company_facts(symbol)
        if not facts:
            return None

        us_gaap = facts.get("facts", {}).get("us-gaap", {})

        def get_val(tag: str, unit: str = "USD") -> Optional[float]:
            if tag not in us_gaap:
                return None
            entry = us_gaap[tag]
            units = entry.get("units", {})
            values = units.get(unit, [])
            if not values:
                return None
            def sort_key(x):
                return (x.get("fy") or 0, x.get("fp") or "")
            latest = max(values, key=sort_key)
            return latest.get("val")

        # Get key values
        net_income = get_val("NetIncomeLoss")
        equity = get_val("StockholdersEquity")
        total_debt = get_val("LongTermDebtNoncurrent")
        current_assets = get_val("AssetsCurrent")
        current_liabilities = get_val("LiabilitiesCurrent")
        revenue = get_val("Revenues")

        if not net_income or net_income == 0:
            return None  # Can't compute meaningful factors

        result = {}

        # ROE
        if equity and equity != 0:
            result["roe"] = round((net_income / equity) * 100, 2)

        # Profit margin
        if revenue and revenue != 0:
            result["profit_margin"] = round((net_income / revenue) * 100, 2)

        # P/B ratio (need market cap from quote)
        # P/E ratio (need market cap from quote)
        # These require price data, so we return what we can from SEC alone
        result["net_income"] = net_income
        result["equity"] = equity

        # Debt-to-equity
        if total_debt and equity and equity != 0:
            result["debt_to_equity"] = round(total_debt / equity, 2)

        # Current ratio
        if current_assets and current_liabilities and current_liabilities != 0:
            result["current_ratio"] = round(current_assets / current_liabilities, 2)

        return result
    except Exception as e:
        logger.warning(f"Failed to extract fundamental factors for {symbol}: {e}")
        return None


# ─── Test ───

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    print(f"=== {symbol} SEC Financial Statements ===\n")

    bs = get_balance_sheet_sec(symbol)
    print(bs)
    print("\n" + "=" * 60 + "\n")

    is_ = get_income_statement_sec(symbol)
    print(is_)
    print("\n" + "=" * 60 + "\n")

    cf = get_cashflow_sec(symbol)
    print(cf)
