"""
US Data Hub — Reddit Collector
Source: Reddit API (JSON endpoints)
APIs:
  - Hot posts:  https://www.reddit.com/r/{subreddit}/hot.json
  - New posts:  https://www.reddit.com/r/{subreddit}/new.json
  - Search:     https://www.reddit.com/search.json?q={query}
Auth: User-Agent only (no OAuth needed for read-only)
Network: Requires proxy
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime

from collectors.base import BaseCollector

logger = logging.getLogger(__name__)


class RedditCollector(BaseCollector):
    """Reddit posts/comments collector."""

    def __init__(self, proxy: Optional[str] = None, user_agent: Optional[str] = None):
        super().__init__(
            proxy=proxy,
            requires_proxy=True,
            user_agent=user_agent or "linux:us-data-hub:v0.1",
            rate_limit=1.0,
        )
        self.base_url = "https://www.reddit.com"
        self.subreddits = ["wallstreetbets", "investing", "stocks", "StockMarket"]

    def collect(self, symbol: str, count: int = 10, **kwargs) -> List[Dict]:
        """
        Collect Reddit posts for a symbol.

        Combines: search results + hot posts from relevant subreddits.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            count: Total number of posts to collect
            **kwargs: subreddits (optional list override)

        Returns:
            List of post dicts
        """
        symbol = symbol.upper()
        results = []

        # 1. Search for symbol across all of Reddit
        search_results = self._search(symbol, count=max(count // 2, 5))
        results.extend(search_results)

        # 2. Get hot posts from relevant subreddits, filter for symbol mentions
        subs = kwargs.get("subreddits", self.subreddits)
        per_sub = max(3, count // len(subs))
        for sub in subs:
            if len(results) >= count:
                break
            hot_posts = self._hot_posts(sub, limit=per_sub)
            for post in hot_posts:
                title = post.get("raw_data", {}).get("title", "").upper()
                if symbol in title:
                    results.append(post)

        # Deduplicate by post ID
        seen = set()
        unique = []
        for r in results:
            pid = r.get("raw_data", {}).get("id", r.get("title", ""))
            if pid not in seen:
                seen.add(pid)
                unique.append(r)

        logger.info(f"Reddit: {symbol} → {len(unique)} posts")
        return unique[:count]

    def _search(self, query: str, count: int = 10) -> List[Dict]:
        """Search Reddit for a query."""
        url = f"{self.base_url}/search.json"
        params = {"q": query, "sort": "new", "limit": count, "type": "link"}
        try:
            resp = self._request(url, params=params)
            data = resp.json()
        except Exception as e:
            logger.error(f"Reddit search failed for '{query}': {e}")
            return []
        return self._parse_posts(data, query)

    def _hot_posts(self, subreddit: str, limit: int = 10) -> List[Dict]:
        """Get hot posts from a subreddit."""
        url = f"{self.base_url}/r/{subreddit}/hot.json"
        params = {"limit": limit}
        try:
            resp = self._request(url, params=params)
            data = resp.json()
        except Exception as e:
            logger.error(f"Reddit hot posts failed for r/{subreddit}: {e}")
            return []
        return self._parse_posts(data, subreddit)

    def _parse_posts(self, data: Dict, context: str = "") -> List[Dict]:
        """Parse Reddit API response into list of post dicts."""
        results = []
        posts = data.get("data", {}).get("children", [])

        for child in posts:
            d = child.get("data", {})
            if not d or d.get("stickied", False):
                continue

            post_id = d.get("id", "")
            title = d.get("title", "")
            subreddit = d.get("subreddit", "")
            ups = d.get("ups", 0)
            num_comments = d.get("num_comments", 0)
            created_utc = d.get("created_utc", 0)
            permalink = d.get("permalink", "")
            selftext = d.get("selftext", "")[:500]
            url = d.get("url", "")

            if created_utc:
                ts = datetime.utcfromtimestamp(created_utc).strftime("%Y-%m-%dT%H:%M:%S")
            else:
                ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

            results.append({
                "symbol": context.upper() if context.isupper() else "",
                "title": f"[r/{subreddit}] {title}",
                "pub_date": ts,
                "timestamp": ts,
                "source": "reddit",
                "type": "post",
                "content": {
                    "title": title,
                    "subreddit": subreddit,
                    "ups": ups,
                    "num_comments": num_comments,
                    "selftext": selftext,
                    "url": url,
                },
                "tags": ["reddit", subreddit.lower()],
                "raw_data": {
                    "id": post_id,
                    "title": title,
                    "subreddit": subreddit,
                    "ups": ups,
                    "num_comments": num_comments,
                    "created_utc": created_utc,
                    "permalink": permalink,
                },
            })

        return results
