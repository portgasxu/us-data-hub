"""
US Data Hub — SEC EDGAR Collector
Source: SEC EDGAR Submissions API
API: https://data.sec.gov/submissions/CIK{cik}.json
Docs: https://www.sec.gov/os/accessing-edgar-data
Rate limit: 10 req/s max (configured to 5)
Auth: User-Agent only (no API key needed)
Network: 国内直连 (no proxy needed)
"""

import os
import json
import time
import logging
from typing import List, Dict, Optional
from datetime import datetime

from collectors.base import BaseCollector

logger = logging.getLogger(__name__)


class SECCollector(BaseCollector):
    """SEC EDGAR filings collector using Submissions API."""

    def __init__(self, proxy: Optional[str] = None):
        super().__init__(
            proxy=proxy,
            requires_proxy=False,  # SEC EDGAR is accessible without proxy
            user_agent="USDataHub admin@localhost",
            rate_limit=0.2,  # 5 requests per second
        )
        self.base_url = "https://data.sec.gov"
        self.filing_url = "https://www.sec.gov/Archives/edgar/data"
        self.cik_map = self._load_cik_mapping()

    def _get_cik(self, symbol: str) -> Optional[str]:
        """Get CIK number for a stock symbol from local mapping."""
        return self.cik_map.get(symbol.upper())

    def collect(self, symbol: str, count: int = 10, **kwargs) -> List[Dict]:
        """
        Collect SEC filings for a symbol.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            count: Number of recent filings to fetch

        Returns:
            List of filing dicts
        """
        symbol = symbol.upper()
        cik = self._get_cik(symbol)
        if not cik:
            logger.warning(f"No CIK mapping for {symbol}, skipping SEC collection")
            return []

        url = f"{self.base_url}/submissions/CIK{cik}.json"
        try:
            resp = self._request(url)
            data = resp.json()
        except Exception as e:
            logger.error(f"SEC Submissions API failed for {symbol} (CIK={cik}): {e}")
            return []

        # Parse recent filings (array-based format)
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        accession_numbers = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        descriptions = recent.get("primaryDocDescription", [])

        results = []
        company_name = data.get("name", symbol)

        for i in range(min(count, len(forms))):
            form_type = forms[i]
            filing_date = filing_dates[i] if i < len(filing_dates) else ""
            report_date = report_dates[i] if i < len(report_dates) else ""
            accession = accession_numbers[i] if i < len(accession_numbers) else ""
            doc = primary_docs[i] if i < len(primary_docs) else ""
            desc = descriptions[i] if i < len(descriptions) else ""

            # Build filing URL for archives
            filing_url = None
            if accession:
                acc_num_clean = accession.replace("-", "")
                cik_num = cik.lstrip("0")
                filing_url = f"{self.filing_url}/{cik_num}/{acc_num_clean}/{doc}" if doc else f"{self.filing_url}/{cik_num}/{acc_num_clean}/"

            results.append({
                "symbol": symbol,
                "cik": cik,
                "company_name": company_name,
                "form_type": form_type,
                "filing_date": filing_date,
                "report_date": report_date,
                "accession_number": accession,
                "primary_document": doc,
                "description": desc,
                "filing_url": filing_url,
                "timestamp": filing_date if filing_date else datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "source": "sec",
                "type": "filing",
                "title": f"Form {form_type} — {symbol}",
                "content": {
                    "form_type": form_type,
                    "filing_date": filing_date,
                    "report_date": report_date,
                    "accession_number": accession,
                    "document": doc,
                    "description": desc,
                    "company_name": company_name,
                },
                "tags": [f"sec_{form_type.lower()}", symbol.lower()],
                "raw_data": {
                    "cik": cik,
                    "form": form_type,
                    "filingDate": filing_date,
                    "reportDate": report_date,
                    "accessionNumber": accession,
                    "document": doc,
                    "description": desc,
                },
            })

        logger.info(f"SEC: {symbol} → {len(results)} filings (CIK: {cik})")
        return results
