#!/usr/bin/env python3
"""
US Data Hub — Alert Notifier
=============================

Push critical events to external channels (Telegram, Webhook, etc.).
P0/P1 events → immediate push; P2+ → batch.

Usage:
    from alerts.notifier import alert, AlertLevel

    alert(AlertLevel.P0, "CIRCUIT BREAKER", "Daily loss exceeded $500")
    alert(AlertLevel.P1, "Trade blocked", "AAPL buy rejected by risk control")
"""

import os
import json
import logging
import urllib.request
import urllib.error
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    P0 = "P0"   # Critical — immediate push (circuit breaker, kill switch, exec failure)
    P1 = "P1"   # Warning — immediate push (risk block, signal rejected)
    P2 = "P2"   # Info — batch (daily summary, watchlist alert)


# ─── Configuration (env vars) ───
NOTIFIER_CONFIG = {
    # Telegram bot (optional)
    "telegram_bot_token": os.getenv("ALERT_TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id": os.getenv("ALERT_TELEGRAM_CHAT_ID", ""),
    # Generic webhook (optional) — POST JSON to URL
    "webhook_url": os.getenv("ALERT_WEBHOOK_URL", ""),
    # P2 batch interval (seconds)
    "batch_interval_sec": 3600,
}


class _Notifier:
    """Singleton notifier — manages channel state and batching."""

    def __init__(self):
        self._p2_buffer: list = []
        self._last_flush = datetime.now()

    def send(self, level: AlertLevel, title: str, message: str, details: dict = None):
        """Send an alert. P0/P1 → immediate; P2 → batch."""
        entry = {
            "level": level.value,
            "title": title,
            "message": message,
            "details": details or {},
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        if level in (AlertLevel.P0, AlertLevel.P1):
            self._push_immediate(entry)
        else:
            self._p2_buffer.append(entry)
            self._maybe_flush()

    def _push_immediate(self, entry: dict):
        """Push to all configured channels."""
        text = self._format_text(entry)

        # Telegram
        if NOTIFIER_CONFIG["telegram_bot_token"] and NOTIFIER_CONFIG["telegram_chat_id"]:
            self._send_telegram(text, entry)

        # Webhook
        if NOTIFIER_CONFIG["webhook_url"]:
            self._send_webhook(entry)

        # Fallback: log
        if not (NOTIFIER_CONFIG["telegram_bot_token"] or NOTIFIER_CONFIG["webhook_url"]):
            emoji = "🛑" if entry["level"] == "P0" else "⚠️"
            logger.info(f"{emoji} [{entry['level']}] {entry['title']}: {entry['message']}")

    def _send_telegram(self, text: str, entry: dict):
        """Send via Telegram Bot API."""
        try:
            url = (
                f"https://api.telegram.org/bot{NOTIFIER_CONFIG['telegram_bot_token']}"
                f"/sendMessage"
            )
            data = json.dumps({
                "chat_id": NOTIFIER_CONFIG["telegram_chat_id"],
                "text": text,
                "parse_mode": "HTML",
            }).encode("utf-8")
            req = urllib.request.Request(
                url, data=data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.debug(f"Telegram alert sent: {resp.status}")
        except Exception as e:
            logger.warning(f"Telegram push failed: {e}")

    def _send_webhook(self, entry: dict):
        """Send via generic HTTP webhook."""
        try:
            data = json.dumps(entry).encode("utf-8")
            req = urllib.request.Request(
                NOTIFIER_CONFIG["webhook_url"], data=data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.debug(f"Webhook alert sent: {resp.status}")
        except Exception as e:
            logger.warning(f"Webhook push failed: {e}")

    def _format_text(self, entry: dict) -> str:
        """Format alert as Telegram HTML."""
        level_emoji = {"P0": "🛑", "P1": "⚠️", "P2": "ℹ️"}.get(entry["level"], "📋")
        return (
            f"{level_emoji} <b>[{entry['level']}] {self._html_escape(entry['title'])}</b>\n"
            f"{self._html_escape(entry['message'])}\n"
            f"⏰ {entry['timestamp']}"
        )

    @staticmethod
    def _html_escape(text: str) -> str:
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def _maybe_flush(self):
        """Flush P2 batch if interval exceeded."""
        now = datetime.now()
        if (now - self._last_flush).total_seconds() >= NOTIFIER_CONFIG["batch_interval_sec"]:
            self._flush_p2()

    def _flush_p2(self):
        """Send batched P2 alerts."""
        if not self._p2_buffer:
            return
        text = "ℹ️ <b>[P2 汇总]</b>\n\n"
        for entry in self._p2_buffer[-20:]:  # cap at 20
            text += f"• <b>{self._html_escape(entry['title'])}</b>: {self._html_escape(entry['message'])}\n"
        text += f"\n⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        if NOTIFIER_CONFIG["telegram_bot_token"] and NOTIFIER_CONFIG["telegram_chat_id"]:
            self._send_telegram(text, {"level": "P2", "title": "Batch", "message": ""})
        else:
            logger.info(f"[P2 Batch] {len(self._p2_buffer)} alerts flushed")

        self._p2_buffer.clear()
        self._last_flush = datetime.now()


# Singleton
_notifier = _Notifier()


def alert(level: AlertLevel, title: str, message: str, details: dict = None):
    """Convenience function: send an alert."""
    _notifier.send(level, title, message, details)
