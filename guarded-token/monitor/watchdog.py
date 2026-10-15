"""
Independent watchdog: alerts if the monitor goes silent.

Run it as a separate process (ideally on separate infrastructure):
    python -m monitor.watchdog

It reads the heartbeat file the monitor rewrites after every poll. If the
file is older than WATCHDOG_STALE_SECONDS, it sends one "MONITOR DOWN"
message, then a "recovered" message once heartbeats resume. It signs with
its own key, so a compromised monitor cannot impersonate it.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from .config import Settings
from .notify import Notifier


class Watchdog:
    def __init__(
        self,
        notifier: Notifier,
        heartbeat_path: str | Path,
        stale_after: int = 30,
        started_at: float | None = None,
    ):
        self.notifier = notifier
        self.heartbeat_path = Path(heartbeat_path)
        self.stale_after = stale_after
        self.started_at = started_at if started_at is not None else time.time()
        self.down = False

    def heartbeat_age(self, now: float | None = None) -> float | None:
        now = now or time.time()
        try:
            data = json.loads(self.heartbeat_path.read_text())
            return now - float(data["time"])
        except (FileNotFoundError, ValueError, KeyError):
            return None

    def check(self, now: float | None = None) -> str:
        """Returns 'ok', 'down' (just went down) or 'recovered'."""
        now = now or time.time()
        age = self.heartbeat_age(now)
        if age is None and now - self.started_at <= self.stale_after:
            return "ok"  # grace period: the monitor may still be starting up
        stale = age is None or age > self.stale_after
        if stale and not self.down:
            self.down = True
            seen = "never" if age is None else f"{age:.0f} seconds ago"
            self.notifier.send_status(
                f"[CRITICAL] MONITOR DOWN\n"
                f"No heartbeat from the chain monitor (last seen {seen}).\n"
                f"Alerts are NOT being checked. Investigate now."
            )
            return "down"
        if not stale and self.down:
            self.down = False
            self.notifier.send_status("[RECOVERED] Monitor heartbeat is back.")
            return "recovered"
        return "ok"

    def run(self, every: float = 10.0) -> None:
        while True:
            self.check()
            time.sleep(every)


def main():
    settings = Settings.load()
    notifier = settings.make_notifier(key_path="keys/watchdog_ed25519.key", log_path=None)
    dog = Watchdog(notifier, settings.heartbeat_path, settings.watchdog_stale_after)
    print(f"Watching {settings.heartbeat_path}; stale after {settings.watchdog_stale_after}s.")
    try:
        dog.run()
    except KeyboardInterrupt:
        print("\nWatchdog stopped.")


if __name__ == "__main__":
    main()
