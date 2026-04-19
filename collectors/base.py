"""
US Data Hub — Base Collector
Abstract base class for all data source collectors.
"""

import os
import json
import time
import logging
import requests
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Base class for all data collectors."""

    def __init__(self, proxy: Optional[str] = None, requires_proxy: bool = False,
                 user_agent: Optional[str] = None, rate_limit: float = 1.0):
        """
        Args:
            proxy: Proxy URL (e.g., 'http://127.0.0.1:7890')
            requires_proxy: Whether this collector requires proxy
            user_agent: User-Agent header
            rate_limit: Minimum seconds between requests
        """
        self.proxy = proxy
        self.requires_proxy = requires_proxy
        self.user_agent = user_agent or 'USDataHub/1.0'
        self.rate_limit = rate_limit
        self._last_request_time = 0.0
        self._session = None

    def _get_session(self) -> requests.Session:
        """Get or create HTTP session with optional proxy."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({'User-Agent': self.user_agent})
            if self.requires_proxy and self.proxy:
                self._session.proxies = {'http': self.proxy, 'https': self.proxy}
        return self._session

    def _request(self, url: str, params: Optional[Dict] = None,
                 headers: Optional[Dict] = None, timeout: int = 30) -> requests.Response:
        """Make rate-limited HTTP GET request."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

        session = self._get_session()
        req_headers = {'User-Agent': self.user_agent}
        if headers:
            req_headers.update(headers)
        resp = session.get(url, params=params, headers=req_headers, timeout=timeout)
        resp.raise_for_status()
        return resp

    def _load_cik_mapping(self) -> Dict[str, str]:
        """Load CIK mapping from data/cik_mapping.json."""
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cik_path = os.path.join(project_root, 'data', 'cik_mapping.json')
        if os.path.exists(cik_path):
            with open(cik_path) as f:
                return json.load(f)
        return {}

    @abstractmethod
    def collect(self, symbol: str, count: int = 10, **kwargs) -> List[Dict]:
        """Collect data for a symbol. Returns list of raw data dicts."""
        pass
