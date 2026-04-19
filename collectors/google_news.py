"""
US Data Hub — Google News RSS Collector
Source: Google News RSS feed
API: https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en
Network: Requires proxy
"""

import re
import logging
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
from datetime import datetime
from urllib.parse import quote

from collectors.base import BaseCollector

logger = logging.getLogger(__name__)


class GoogleNewsCollector(BaseCollector):
    """Google News RSS collector."""

    def __init__(self, proxy: Optional[str] = None):
        super().__init__(
            proxy=proxy,
            requires_proxy=True,
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            rate_limit=1.0,
        )
        self.search_url = "https://news.google.com/rss/search"
        self.homepage_url = "https://news.google.com/rss"

    def collect(self, symbol: str, count: int = 20, **kwargs) -> List[Dict]:
        """
        Collect news for a symbol from Google News RSS.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            count: Number of articles to fetch

        Returns:
            List of news item dicts
        """
        symbol = symbol.upper()
        query = f"{symbol} stock"
        url = f"{self.search_url}?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"

        try:
            resp = self._request(url)
            items = self._parse_rss(resp.text)
        except Exception as e:
            logger.error(f"Google News RSS failed for {symbol}: {e}")
            return []

        results = []
        for item in items[:count]:
            title = item.get("title", "")
            source = item.get("source", "")
            pub_date = item.get("pubDate", "")
            link = item.get("link", "")

            results.append({
                "symbol": symbol,
                "title": title,
                "source_name": source,
                "url": link,
                "pub_date": pub_date,
                "timestamp": pub_date if pub_date else datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "source": "google_news",
                "type": "news",
                "content": {
                    "title": title,
                    "source": source,
                    "url": link,
                    "pub_date": pub_date,
                },
                "tags": ["news", symbol.lower()],
                "raw_data": item,
            })

        logger.info(f"Google News: {symbol} → {len(results)} articles")
        return results

    def _parse_rss(self, xml_text: str) -> List[Dict]:
        """Parse Google News RSS XML into list of dicts."""
        items = []
        # Find all <item> blocks
        item_pattern = re.compile(r'<item>(.*?)</item>', re.DOTALL)
        for match in item_pattern.finditer(xml_text):
            item_xml = match.group(1)
            item = {}

            # Extract title
            title_match = re.search(r'<title>(.*?)</title>', item_xml)
            if title_match:
                item["title"] = title_match.group(1).strip()

            # Extract link
            link_match = re.search(r'<link>(.*?)</link>', item_xml)
            if link_match:
                item["link"] = link_match.group(1).strip()

            # Extract pubDate
            pub_match = re.search(r'<pubDate>(.*?)</pubDate>', item_xml)
            if pub_match:
                item["pubDate"] = pub_match.group(1).strip()

            # Extract source (within description or separate tag)
            source_match = re.search(r'<source[^>]*>(.*?)</source>', item_xml)
            if source_match:
                item["source"] = source_match.group(1).strip()
            else:
                # Sometimes source is embedded in title like "Title - Source"
                title = item.get("title", "")
                if " — " in title:
                    item["source"] = title.split(" — ")[-1].strip()
                elif " - " in title:
                    parts = title.rsplit(" - ", 1)
                    if len(parts) == 2 and len(parts[1]) < 30:
                        item["source"] = parts[1].strip()

            if item.get("title"):
                items.append(item)

        return items
