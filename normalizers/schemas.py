"""
US Data Hub — Data Normalization Schemas
Convert raw data from different sources into unified DataPoint format.
"""

import json
import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class DataPoint:
    """Unified data point for all sources."""
    timestamp: str
    symbol: str
    source: str
    type: str  # filing, news, post, capital_flow, market_temp
    title: Optional[str] = None
    content: Optional[Dict] = field(default_factory=dict)
    sentiment_score: Optional[float] = None
    tags: Optional[List[str]] = field(default_factory=list)
    raw_data: Optional[Dict] = field(default_factory=dict)
    source_id: Optional[str] = None

    def to_dict(self) -> Dict:
        return asdict(self)


def _ensure_timestamp(ts) -> str:
    """Ensure timestamp is in ISO format."""
    if not ts:
        return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    ts = str(ts)
    # Already has T separator
    if "T" in ts:
        return ts[:19]
    # Date only
    if len(ts) >= 10 and ts[4] == "-" and ts[7] == "-":
        return ts[:10] + "T00:00:00"
    return ts[:19]


def normalize_sec_filing(raw: Dict) -> DataPoint:
    """Normalize SEC EDGAR filing data."""
    filing_date = raw.get("filing_date", raw.get("timestamp", ""))
    form_type = raw.get("form_type", "unknown")
    symbol = raw.get("symbol", "")

    return DataPoint(
        timestamp=_ensure_timestamp(filing_date),
        symbol=symbol,
        source="sec",
        type="filing",
        title=f"Form {form_type} — {symbol}",
        content=raw.get("content", {
            "form_type": form_type,
            "filing_date": filing_date,
            "accession_number": raw.get("accession_number"),
            "company_name": raw.get("company_name"),
        }),
        tags=["sec", f"sec_{form_type.lower()}", symbol.lower()],
        raw_data=raw.get("raw_data", raw),
        source_id=raw.get("accession_number", f"{symbol}_{filing_date}"),
    )


def normalize_news(raw: Dict) -> DataPoint:
    """Normalize news article data (Google News / Longbridge)."""
    title = raw.get("title", "")
    pub_date = raw.get("pub_date", raw.get("publish_time", raw.get("timestamp", "")))
    symbol = raw.get("symbol", "")

    # Determine source from context
    source = raw.get("source", "news")
    if not source or source == "news":
        source = raw.get("_source_collector", "google_news")

    return DataPoint(
        timestamp=_ensure_timestamp(pub_date),
        symbol=symbol,
        source=source,
        type="news",
        title=title,
        content=raw.get("content", {
            "title": title,
            "source": raw.get("source_name", raw.get("source")),
            "url": raw.get("url"),
        }),
        sentiment_score=raw.get("sentiment_score"),
        tags=raw.get("tags", ["news", symbol.lower()]),
        raw_data=raw.get("raw_data", raw),
        source_id=raw.get("url", title)[:100],
    )


def normalize_reddit_post(raw: Dict) -> DataPoint:
    """Normalize Reddit post data."""
    title = raw.get("title", "")
    symbol = raw.get("symbol", "")

    return DataPoint(
        timestamp=_ensure_timestamp(raw.get("timestamp", "")),
        symbol=symbol,
        source="reddit",
        type="post",
        title=title,
        content=raw.get("content", {}),
        sentiment_score=raw.get("sentiment_score"),
        tags=raw.get("tags", ["reddit", symbol.lower()]),
        raw_data=raw.get("raw_data", raw),
        source_id=raw.get("post_id", title[:80]),
    )


def normalize_capital_flow(raw: Dict) -> DataPoint:
    """Normalize Longbridge capital flow data."""
    symbol = raw.get("symbol", "")
    content = raw.get("content", raw)

    return DataPoint(
        timestamp=_ensure_timestamp(raw.get("timestamp", datetime.now())),
        symbol=symbol,
        source="longbridge",
        type="capital_flow",
        title=f"Capital Flow — {symbol}",
        content=content,
        tags=["capital_flow", "longbridge", symbol.lower()],
        raw_data=raw.get("raw_data", raw),
        source_id=f"capital_{symbol}_{raw.get('timestamp', '')[:10]}",
    )


def validate_data_point(dp: DataPoint) -> List[str]:
    """Validate a DataPoint. Returns list of errors (empty = valid)."""
    errors = []
    if not dp.timestamp:
        errors.append("missing timestamp")
    if not dp.symbol:
        errors.append("missing symbol")
    if not dp.source:
        errors.append("missing source")
    if not dp.type:
        errors.append("missing type")
    if not dp.title:
        errors.append("missing title")
    return errors
