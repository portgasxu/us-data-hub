"""
US Data Hub — Error Logger
Records errors, warnings, successes to JSON + Markdown files.
"""

import os
import json
import traceback
from datetime import datetime, timezone, timedelta

CST = timezone(timedelta(hours=8))


def now_str():
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def now_date():
    return datetime.now(CST).strftime("%Y-%m-%d")


class ErrorLogger:
    """Structured error/success logger for the trading daemon."""

    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

        self.error_count = 0
        self.warning_count = 0
        self.task_count = 0
        self.errors = []
        self.error_log = os.path.join(log_dir, "errors.json")
        self.run_log = os.path.join(log_dir, "run_log.json")

    def log_info(self, source: str, message: str, data: dict = None):
        """Log an info event."""
        entry = {
            "time": now_str(),
            "level": "INFO",
            "source": source,
            "message": message,
            "data": data or {},
        }
        print(f"[{now_str()}] ℹ️ [{source}] {message}")
        self._append_json(self.error_log, entry)

    def log_success(self, source: str, message: str, data: dict = None):
        """Log a success event."""
        self.task_count += 1
        entry = {
            "time": now_str(),
            "level": "SUCCESS",
            "source": source,
            "message": message,
            "data": data or {},
        }
        print(f"[{now_str()}] ✅ [{source}] {message}")
        self._append_json(self.error_log, entry)

    def log_warning(self, source: str, message: str, data: dict = None):
        """Log a warning event."""
        self.warning_count += 1
        entry = {
            "time": now_str(),
            "level": "WARNING",
            "source": source,
            "message": message,
            "data": data or {},
        }
        print(f"[{now_str()}] ⚠️ [{source}] {message}")
        self._append_json(self.error_log, entry)

    def log_error(self, source: str, error: Exception, data: dict = None):
        """Log an error event."""
        self.error_count += 1
        tb = traceback.format_exc() if error else ""
        entry = {
            "time": now_str(),
            "level": "ERROR",
            "source": source,
            "message": str(error),
            "traceback": tb,
            "data": data or {},
        }
        print(f"[{now_str()}] ❌ [{source}] {error}")
        if tb:
            print(tb[:500])
        self._append_json(self.error_log, entry)

    def log_summary(self):
        """Log run summary to run_log.json."""
        summary = {
            "date": now_date(),
            "time": now_str(),
            "errors": self.error_count,
            "warnings": self.warning_count,
            "successes": self.task_count,
        }
        self._append_json(self.run_log, summary)

    def _append_json(self, filepath: str, entry: dict):
        """Append a JSON entry to a log file."""
        try:
            entries = []
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        entries = json.loads(content)
            entries.append(entry)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(entries, f, indent=2, ensure_ascii=False, default=str)
        except Exception:
            pass
