"""
US Data Hub — Longbridge CLI Collector
Source: Longbridge OpenAPI via CLI
Docs: https://open.longportapp.com/docs/cli/intro
Commands: news, filing, capital, financial-report, topic, market-temp
Network: Requires proxy + OAuth
"""

import subprocess
import json
import logging
from typing import List, Dict, Optional

from collectors.base import BaseCollector

logger = logging.getLogger(__name__)


class LongbridgeCollector(BaseCollector):
    """Longbridge data collector using CLI commands."""

    DATA_TYPES = ["news", "filing", "capital", "financial-report", "topic", "market-temp"]

    def __init__(self, proxy: Optional[str] = None, cli_path: str = "longbridge"):
        super().__init__(
            proxy=proxy,
            requires_proxy=True,
            user_agent="USDataHub/1.0",
            rate_limit=1.0,
        )
        self.cli_path = cli_path
        self.symbol_suffix = ".US"

    def _run_cli(self, args: list, timeout: int = 30) -> Optional[str]:
        """Run a Longbridge CLI command and return stdout."""
        cmd = [self.cli_path] + args
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout
            )
            if result.returncode != 0:
                logger.warning(f"Longbridge CLI failed: {' '.join(cmd)} → {result.stderr[:200]}")
                return None
            return result.stdout
        except subprocess.TimeoutExpired:
            logger.error(f"Longbridge CLI timeout: {' '.join(cmd)}")
            return None
        except FileNotFoundError:
            logger.error(f"Longbridge CLI not found at: {self.cli_path}")
            return None

    def _parse_table_output(self, text: str) -> List[Dict]:
        """Parse Longbridge CLI table output into list of dicts."""
        results = []
        lines = [l.strip() for l in text.strip().split('\n') if l.strip()]
        if len(lines) < 2:
            return results

        # First line is header
        headers = [h.strip() for h in lines[0].split('|')]
        headers = [h for h in headers if h]  # Remove empty

        for line in lines[1:]:
            if line.startswith('---') or line.startswith('==='):
                continue
            values = [v.strip() for v in line.split('|')]
            values = [v for v in values if v]
            if len(values) == len(headers):
                results.append(dict(zip(headers, values)))

        return results

    def collect(self, symbol: str, count: int = 5, **kwargs) -> List[Dict]:
        """
        Collect data from Longbridge for a symbol.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            count: Number of items to fetch
            **kwargs: data_type='news'|'filing'|'capital'|'topic'

        Returns:
            List of data dicts
        """
        data_type = kwargs.get("data_type", "news")
        symbol_us = f"{symbol.upper()}{self.symbol_suffix}"

        if data_type == "news":
            return self._collect_news(symbol_us, count)
        elif data_type == "filing":
            return self._collect_filing(symbol_us, count)
        elif data_type == "capital":
            return self._collect_capital(symbol_us)
        elif data_type == "topic":
            return self._collect_topic(symbol_us, count)
        elif data_type == "market-temp":
            return self._collect_market_temp()
        elif data_type == "financial-report":
            return self._collect_financial_report(symbol_us, kwargs.get("kind", "IS"))
        else:
            logger.warning(f"Unknown Longbridge data type: {data_type}")
            return []

    def _collect_news(self, symbol_us: str, count: int) -> List[Dict]:
        """Collect news for a symbol."""
        output = self._run_cli(["news", symbol_us, "--count", str(count)])
        if not output:
            return []

        results = []
        try:
            # Try parsing as JSON first
            data = json.loads(output)
            for item in data if isinstance(data, list) else [data]:
                results.append({
                    "symbol": symbol_us.replace(".US", ""),
                    "title": item.get("title", ""),
                    "content_text": item.get("content", ""),
                    "pub_date": item.get("publish_time", item.get("created_at", "")),
                    "timestamp": item.get("publish_time", item.get("created_at", "")),
                    "source": "longbridge",
                    "type": "news",
                    "url": item.get("url", ""),
                    "content": {
                        "title": item.get("title", ""),
                        "content": item.get("content", ""),
                        "publish_time": item.get("publish_time", ""),
                    },
                    "tags": ["news", "longbridge", symbol_us.replace(".US", "").lower()],
                    "raw_data": item,
                })
        except json.JSONDecodeError:
            # Fall back to table parsing
            rows = self._parse_table_output(output)
            for row in rows[:count]:
                results.append({
                    "symbol": symbol_us.replace(".US", ""),
                    "title": row.get("标题", row.get("title", "")),
                    "pub_date": row.get("时间", row.get("time", "")),
                    "timestamp": row.get("时间", row.get("time", "")),
                    "source": "longbridge",
                    "type": "news",
                    "content": {"title": row.get("标题", ""), **row},
                    "tags": ["news", "longbridge"],
                    "raw_data": row,
                })

        logger.info(f"Longbridge news: {symbol_us} → {len(results)} items")
        return results

    def _collect_filing(self, symbol_us: str, count: int) -> List[Dict]:
        """Collect SEC filings via Longbridge."""
        output = self._run_cli(["filing", symbol_us, "--count", str(count)])
        if not output:
            return []

        results = []
        try:
            data = json.loads(output)
            for item in data if isinstance(data, list) else [data]:
                results.append({
                    "symbol": symbol_us.replace(".US", ""),
                    "title": item.get("title", ""),
                    "pub_date": item.get("publish_time", item.get("created_at", "")),
                    "timestamp": item.get("publish_time", ""),
                    "source": "longbridge",
                    "type": "filing",
                    "content": {"title": item.get("title", ""), **item},
                    "tags": ["filing", "longbridge"],
                    "raw_data": item,
                })
        except json.JSONDecodeError:
            rows = self._parse_table_output(output)
            for row in rows[:count]:
                results.append({
                    "symbol": symbol_us.replace(".US", ""),
                    "title": row.get("标题", ""),
                    "timestamp": row.get("时间", ""),
                    "source": "longbridge",
                    "type": "filing",
                    "content": {**row},
                    "tags": ["filing", "longbridge"],
                    "raw_data": row,
                })

        logger.info(f"Longbridge filing: {symbol_us} → {len(results)} items")
        return results

    def _collect_capital(self, symbol_us: str) -> List[Dict]:
        """Collect capital flow data."""
        output = self._run_cli(["capital", symbol_us])
        if not output:
            return []

        results = []
        try:
            data = json.loads(output)
            if isinstance(data, list):
                for item in data:
                    results.append({
                        "symbol": symbol_us.replace(".US", ""),
                        "timestamp": item.get("date", ""),
                        "source": "longbridge",
                        "type": "capital_flow",
                        "content": item,
                        "tags": ["capital", "longbridge"],
                        "raw_data": item,
                    })
            else:
                results.append({
                    "symbol": symbol_us.replace(".US", ""),
                    "timestamp": data.get("date", ""),
                    "source": "longbridge",
                    "type": "capital_flow",
                    "content": data,
                    "tags": ["capital", "longbridge"],
                    "raw_data": data,
                })
        except json.JSONDecodeError:
            results.append({
                "symbol": symbol_us.replace(".US", ""),
                "source": "longbridge",
                "type": "capital_flow",
                "content": {"raw": output},
                "tags": ["capital", "longbridge"],
                "raw_data": {"raw": output},
            })

        logger.info(f"Longbridge capital: {symbol_us} → {len(results)} items")
        return results

    def _collect_topic(self, symbol_us: str, count: int) -> List[Dict]:
        """Collect community discussion topics."""
        output = self._run_cli(["topic", symbol_us, "--count", str(count)])
        if not output:
            return []

        results = []
        try:
            data = json.loads(output)
            for item in data if isinstance(data, list) else [data]:
                results.append({
                    "symbol": symbol_us.replace(".US", ""),
                    "title": item.get("title", ""),
                    "pub_date": item.get("created_at", ""),
                    "timestamp": item.get("created_at", ""),
                    "source": "longbridge",
                    "type": "topic",
                    "content": {
                        "title": item.get("title", ""),
                        "likes": item.get("likes", 0),
                        "comments": item.get("comments", 0),
                    },
                    "tags": ["topic", "longbridge"],
                    "raw_data": item,
                })
        except json.JSONDecodeError:
            rows = self._parse_table_output(output)
            for row in rows[:count]:
                results.append({
                    "symbol": symbol_us.replace(".US", ""),
                    "title": row.get("标题", ""),
                    "timestamp": row.get("时间", ""),
                    "source": "longbridge",
                    "type": "topic",
                    "content": {**row},
                    "tags": ["topic", "longbridge"],
                    "raw_data": row,
                })

        logger.info(f"Longbridge topic: {symbol_us} → {len(results)} items")
        return results

    def _collect_market_temp(self) -> List[Dict]:
        """Collect market temperature/sentiment data."""
        output = self._run_cli(["market-temp", "US"])
        if not output:
            return []

        try:
            data = json.loads(output)
            return [{
                "symbol": "US",
                "timestamp": data.get("date", ""),
                "source": "longbridge",
                "type": "market_temp",
                "content": {
                    "temperature": data.get("temperature"),
                    "valuation": data.get("valuation"),
                    "sentiment": data.get("sentiment"),
                },
                "tags": ["market-temp", "longbridge"],
                "raw_data": data,
            }]
        except json.JSONDecodeError:
            return [{
                "symbol": "US",
                "source": "longbridge",
                "type": "market_temp",
                "content": {"raw": output},
                "tags": ["market-temp", "longbridge"],
                "raw_data": {"raw": output},
            }]

    def _collect_financial_report(self, symbol_us: str, kind: str = "IS") -> List[Dict]:
        """Collect financial report data (IS/BS/CF)."""
        output = self._run_cli(["financial-report", symbol_us, "--kind", kind])
        if not output:
            return []

        try:
            data = json.loads(output)
            return [{
                "symbol": symbol_us.replace(".US", ""),
                "timestamp": "",
                "source": "longbridge",
                "type": f"financial_{kind.lower()}",
                "content": data,
                "tags": ["financial", "longbridge", kind.lower()],
                "raw_data": data,
            }]
        except json.JSONDecodeError:
            return [{
                "symbol": symbol_us.replace(".US", ""),
                "source": "longbridge",
                "type": f"financial_{kind.lower()}",
                "content": {"raw": output},
                "tags": ["financial", "longbridge"],
                "raw_data": {"raw": output},
            }]
