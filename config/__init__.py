"""
US Data Hub — Config Loader
Reads configuration from config/sources.yaml with sensible defaults.
"""

import os
import yaml
from typing import Dict, Any

# Default config (used if YAML file not found)
DEFAULT_CONFIG = {
    'proxy': {
        'enabled': True,
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890',
    },
    'watchlist': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META'],
    'storage': {
        'db_path': 'data/us_data_hub.db',
        'raw_dir': 'data/raw',
        'processed_dir': 'data/processed',
    },
    'screener': {
        'default_top': 20,
        'min_score': 0.3,
    },
    'alphalens': {
        'default_days': 180,
        'quantiles': 5,
        'periods': [1, 5, 10],
    },
}


class Config:
    """Global config loader."""

    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'config', 'sources.yaml'
            )
        self._config = dict(DEFAULT_CONFIG)
        self._load_file(config_path)

    def _load_file(self, path: str):
        try:
            with open(path) as f:
                file_config = yaml.safe_load(f)
            if file_config:
                self._deep_merge(self._config, file_config)
        except FileNotFoundError:
            pass  # Use defaults
        except Exception:
            pass  # Use defaults

    def _deep_merge(self, base: dict, override: dict):
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value

    def get(self, key_path: str, default=None) -> Any:
        """Get config value using dot notation: 'proxy.http'"""
        keys = key_path.split('.')
        value = self._config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    @property
    def proxy_url(self) -> str:
        return self.get('proxy.http', 'http://127.0.0.1:7890')

    @property
    def proxy_enabled(self) -> bool:
        return self.get('proxy.enabled', True)

    @property
    def watchlist(self) -> list:
        return self.get('watchlist', [])

    @property
    def db_path(self) -> str:
        return self.get('storage.db_path', 'data/us_data_hub.db')

    def to_dict(self) -> dict:
        return dict(self._config)


# Global config instance
config = Config()
