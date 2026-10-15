"""Alert objects, severity bands and the plain-text message format."""

from __future__ import annotations

import secrets
from dataclasses import dataclass, field

TOKEN = 10**18

# Severity bands from the architecture plan.
BANDS = [
    (90, "CRITICAL"),
    (70, "ALERT"),
    (40, "WARNING"),
    (0, "LOG"),
]


def severity(score: int) -> str:
    for floor, name in BANDS:
        if score >= floor:
            return name
    return "LOG"


def short(addr: str | None) -> str:
    if not addr:
        return "-"
    return f"{addr[:6]}...{addr[-4:]}"


def fmt_tokens(raw: int) -> str:
    return f"{raw / TOKEN:,.2f}".rstrip("0").rstrip(".") + " GTT"


@dataclass
class Alert:
    score: int
    title: str
    details: list[str]
    rules: list[str]
    block: int
    timestamp: int
    tx_hash: str | None = None
    subject: str | None = None  # the address the alert is about, if any
    alert_id: str = field(default_factory=lambda: secrets.token_hex(4))
    similar_suppressed: int = 0
    # Wallets that link this alert to an incident: the sender, plus the
    # receiver when it is a new wallet (so exchanges don't glue incidents together).
    related: list[str] = field(default_factory=list)
    value: int | None = None  # transfer amount (raw units), if any
    incident: int | None = None  # incident number, once grouped

    @property
    def severity(self) -> str:
        return severity(self.score)

    @property
    def dedup_key(self) -> tuple:
        return (tuple(sorted(self.rules)), self.subject)

    def to_text(self) -> str:
        """Message body that gets signed and sent (no signature line)."""
        lines = [f"[{self.severity} {self.score}] {self.title}"]
        where = f"Block {self.block}"
        if self.tx_hash:
            where += f" | tx {self.tx_hash[:10]}...{self.tx_hash[-6:]}"
        lines.append(where)
        lines.extend(f"- {d}" for d in self.details)
        if self.similar_suppressed:
            lines.append(f"- (+{self.similar_suppressed} similar alerts grouped)")
        lines.append(f"Rules: {', '.join(self.rules)}")
        lines.append(f"Alert id: {self.alert_id}")
        return "\n".join(lines)

    def to_record(self) -> dict:
        return {
            "id": self.alert_id,
            "score": self.score,
            "severity": self.severity,
            "title": self.title,
            "details": self.details,
            "rules": self.rules,
            "block": self.block,
            "timestamp": self.timestamp,
            "tx_hash": self.tx_hash,
            "subject": self.subject,
            "related": self.related,
            "value": self.value,
            "incident": self.incident,
        }
