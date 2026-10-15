"""
Incident grouping: one attack = one incident = a handful of messages.

Transfer alerts that share a wallet (the sender, or a brand-new receiver)
are merged into the same incident. Established wallets such as exchanges
are never used as links, so unrelated attacks that both cash out at the
same exchange stay separate.

Messages per incident:
  opened    on the first alert
  update    when the severity band rises, or every UPDATE_EVERY seconds of
            chain time while new alerts keep arriving
  closed    after QUIET_FOR seconds of chain time with no new alerts,
            with a summary
  reopened  if linked activity resumes within REOPEN_WITHIN of closing
            (patient attackers wait between moves)

Every individual alert is still written to the alert log.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from .alerts import Alert, fmt_tokens, severity, short

UPDATE_EVERY = 600  # 10 minutes
QUIET_FOR = 7200  # 2 hours
REOPEN_WITHIN = 86_400  # 24 hours
RANK = {"LOG": 0, "WARNING": 1, "ALERT": 2, "CRITICAL": 3}


def _duration(seconds: int) -> str:
    if seconds < 90:
        return f"{seconds} s"
    if seconds < 5400:
        return f"{round(seconds / 60)} min"
    return f"{seconds / 3600:.1f} h"


@dataclass
class Incident:
    number: int
    opened_at: int
    wallets: set = field(default_factory=set)
    alerts: list = field(default_factory=list)
    senders: Counter = field(default_factory=Counter)
    total_value: int = 0
    peak: int = 0
    last_alert_at: int = 0
    last_message_at: int = 0
    last_message_band: str = "LOG"
    alerts_at_last_message: int = 0
    merged: list = field(default_factory=list)

    def add(self, alert: Alert):
        self.alerts.append(alert)
        self.wallets.update(alert.related)
        if alert.subject:
            self.senders[alert.subject] += 1
        self.total_value += alert.value or 0
        self.peak = max(self.peak, alert.score)
        self.last_alert_at = max(self.last_alert_at, alert.timestamp)

    def absorb(self, other: "Incident"):
        for a in other.alerts:
            self.add(a)
        self.merged.append(other.number)
        self.opened_at = min(self.opened_at, other.opened_at)

    def totals(self, now: int) -> str:
        return (
            f"Incident #{self.number}: {len(self.alerts)} suspicious transfer(s), "
            f"{len(self.wallets)} wallets, {fmt_tokens(self.total_value)} in flagged transfers, "
            f"active for {_duration(max(0, now - self.opened_at))}"
        )

    def main_wallets(self, n: int = 3) -> str:
        return ", ".join(f"{short(w)} ({c} sent)" for w, c in self.senders.most_common(n))


class IncidentTracker:
    def __init__(self, update_every=UPDATE_EVERY, quiet_for=QUIET_FOR, reopen_within=REOPEN_WITHIN):
        self.update_every = update_every
        self.quiet_for = quiet_for
        self.reopen_within = reopen_within
        self.open: list[Incident] = []
        self.closed: list[Incident] = []  # recently closed, can reopen
        self._next = 1

    # ------------------------------------------------------------ grouping

    def _find(self, alert: Alert) -> tuple[Incident | None, bool]:
        """Returns (incident, reopened)."""
        wallets = set(alert.related)
        self.closed = [i for i in self.closed if alert.timestamp - i.last_alert_at < self.reopen_within]
        reopened = False
        for old in [i for i in self.closed if i.wallets & wallets]:
            self.closed.remove(old)
            self.open.append(old)
            reopened = True
        linked = [i for i in self.open if i.wallets & wallets]
        if not linked:
            return None, False
        linked.sort(key=lambda i: i.number)
        main = linked[0]
        for other in linked[1:]:  # this alert connects incidents: merge them
            main.absorb(other)
            self.open.remove(other)
        return main, reopened

    def add(self, alert: Alert) -> str | None:
        """Record an alert. Returns a message body to send, or None to stay quiet."""
        incident, reopened = self._find(alert)
        if incident is None:
            incident = Incident(self._next, opened_at=alert.timestamp)
            self._next += 1
            self.open.append(incident)
            incident.add(alert)
            alert.incident = incident.number
            return self._opened(incident, alert)

        quiet_since = incident.last_alert_at
        incident.add(alert)
        alert.incident = incident.number
        if reopened:
            return self._update(incident, alert, False, reopened_after=alert.timestamp - quiet_since)
        band = severity(incident.peak)
        escalated = RANK[band] > RANK[incident.last_message_band]
        due = alert.timestamp - incident.last_message_at >= self.update_every
        if escalated or due:
            return self._update(incident, alert, escalated)
        return None

    def tick(self, now: int) -> list[str]:
        """Close incidents that have gone quiet. Returns closing messages."""
        messages = []
        for incident in list(self.open):
            if now - incident.last_alert_at >= self.quiet_for:
                self.open.remove(incident)
                self.closed.append(incident)
                messages.append(self._closed(incident))
        return messages

    # ------------------------------------------------------------ messages

    def _mark(self, incident: Incident, at: int):
        incident.last_message_at = at
        incident.last_message_band = severity(incident.peak)
        incident.alerts_at_last_message = len(incident.alerts)

    def _opened(self, incident: Incident, alert: Alert) -> str:
        self._mark(incident, alert.timestamp)
        body = alert.to_text().splitlines()
        body[0] = f"[{alert.severity} {alert.score}] Incident #{incident.number} opened: {alert.title}"
        body.insert(-2, f"Related alerts will be grouped into incident #{incident.number}.")
        return "\n".join(body)

    def _update(self, incident: Incident, alert: Alert, escalated: bool, reopened_after: int | None = None) -> str:
        new = len(incident.alerts) - incident.alerts_at_last_message
        since = _duration(max(0, alert.timestamp - incident.last_message_at))
        self._mark(incident, alert.timestamp)
        head = f"[{severity(incident.peak)} {incident.peak}] Incident #{incident.number} update"
        if reopened_after is not None:
            head = (
                f"[{alert.severity} {alert.score}] Incident #{incident.number} REOPENED: "
                f"activity resumed after {_duration(reopened_after)} quiet"
            )
        elif escalated:
            head += " (severity raised)"
        lines = [
            head,
            f"{new} new suspicious transfer(s) in the last {since}",
            incident.totals(alert.timestamp),
            f"Most active: {incident.main_wallets()}",
            f"Latest: {fmt_tokens(alert.value or 0)} from {short(alert.subject)}, score {alert.score}",
        ]
        lines += [d for d in alert.details if d.startswith("why:")][:2]
        if incident.merged:
            lines.append(f"Includes merged incident(s): {', '.join(f'#{n}' for n in incident.merged)}")
        return "\n".join(lines)

    def _closed(self, incident: Incident) -> str:
        quiet = _duration(self.quiet_for)
        last = incident.last_alert_at
        return "\n".join(
            [
                f"[CLOSED] Incident #{incident.number} (no new alerts for {quiet})",
                f"Peak score {incident.peak} ({severity(incident.peak)})",
                incident.totals(last),
                f"Most active: {incident.main_wallets()}",
                "Review the wallets above; the full alert list is in the alert log.",
            ]
        )
