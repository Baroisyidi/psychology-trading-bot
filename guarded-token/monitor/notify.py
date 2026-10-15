"""
Sending alerts: Telegram Bot API, or the console when no bot is configured.

The Notifier decides what is worth sending (score >= MIN_SCORE), groups
repeats of the same alert so one incident does not become 200 messages,
signs every message, and keeps a JSON-lines log of everything it saw.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import requests

from .alerts import Alert
from .incidents import IncidentTracker
from .signing import Signer

TELEGRAM_API = "https://api.telegram.org"


class ConsoleSender:
    """Dry-run mode: prints messages instead of sending them."""

    def __init__(self):
        self.sent: list[str] = []

    def send(self, text: str) -> None:
        self.sent.append(text)
        print("\n" + "-" * 60 + "\n" + text + "\n" + "-" * 60)


class TelegramSender:
    """Posts messages to one or more allowlisted Telegram chats."""

    def __init__(self, bot_token: str, chat_ids: list[str], session=None, timeout=10):
        if not bot_token or not chat_ids:
            raise ValueError("bot token and at least one chat id are required")
        self._url = f"{TELEGRAM_API}/bot{bot_token}/sendMessage"
        self.chat_ids = chat_ids
        self.session = session or requests.Session()
        self.timeout = timeout

    def send(self, text: str) -> None:
        headline = text.splitlines()[0]
        for chat_id in self.chat_ids:
            payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
            for _attempt in range(3):
                try:
                    resp = self.session.post(self._url, json=payload, timeout=self.timeout)
                except requests.RequestException as err:
                    # Never print the URL: it contains the bot token.
                    print(f"Telegram unreachable for chat {chat_id} ({type(err).__name__}); retrying")
                    time.sleep(2)
                    continue
                if resp.status_code == 429:  # rate limited: wait as told, retry
                    retry = resp.json().get("parameters", {}).get("retry_after", 1)
                    time.sleep(min(retry, 30))
                    continue
                if resp.status_code == 200:
                    print(f"  sent to Telegram chat {chat_id}: {headline}")
                else:
                    print(f"Telegram error {resp.status_code} for chat {chat_id}: {resp.text[:200]}")
                break


class Notifier:
    def __init__(
        self,
        sender,
        signer: Signer,
        min_score: int = 40,
        group_seconds: int = 300,
        log_path: str | Path | None = "alerts.jsonl",
        incidents: bool = True,
    ):
        self.sender = sender
        # Transfer alerts are grouped into incidents; other alerts use simple de-duplication.
        self.tracker = IncidentTracker() if incidents else None
        self.quiet_txs: set[str] = set()  # the guardian's own transactions: log, don't re-alert
        self.signer = signer
        self.min_score = min_score
        self.group_seconds = group_seconds
        self.log_path = Path(log_path) if log_path else None
        # dedup key -> (last sent time, last sent score, suppressed count)
        self._recent: dict[tuple, list] = {}

    def _log(self, alert: Alert, sent: bool) -> None:
        if not self.log_path:
            return
        rec = alert.to_record() | {"sent": sent}
        with self.log_path.open("a") as f:
            f.write(json.dumps(rec) + "\n")

    def handle(self, alert: Alert) -> bool:
        """Send the alert if it should be sent. Returns True if sent."""
        if alert.score < self.min_score or (alert.tx_hash and alert.tx_hash.lower() in self.quiet_txs):
            self._log(alert, sent=False)
            return False

        if self.tracker is not None and alert.related:
            message = self.tracker.add(alert)
            if message:
                self.sender.send(self.signer.sign_text(message))
            self._log(alert, sent=bool(message))
            return bool(message)

        key = alert.dedup_key
        prev = self._recent.get(key)
        if prev:
            last_time, last_score, suppressed = prev
            same_window = alert.timestamp - last_time < self.group_seconds
            if same_window and alert.score <= last_score:
                prev[2] += 1  # group it; mention the count next time
                self._log(alert, sent=False)
                return False
            alert.similar_suppressed = suppressed

        self.sender.send(self.signer.sign_text(alert.to_text()))
        self._recent[key] = [alert.timestamp, alert.score, 0]
        self._log(alert, sent=True)
        return True

    def tick(self, now: int) -> None:
        """Close incidents that have gone quiet (now = latest block time)."""
        if self.tracker is None:
            return
        for message in self.tracker.tick(now):
            self.sender.send(self.signer.sign_text(message))

    @property
    def open_incidents(self) -> int:
        return len(self.tracker.open) if self.tracker else 0

    def send_status(self, body: str) -> None:
        """Heartbeats and watchdog messages bypass scoring but are still signed."""
        self.sender.send(self.signer.sign_text(body))
