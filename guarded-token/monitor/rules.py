"""
Rule-based detection for Phase 2.

Each decoded contract event goes through `Detector.observe`, which returns
zero or more scored alerts (0-100). Phase 3 will add machine-learning
detectors alongside these rules; the scores use the same bands.
"""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timezone
from dataclasses import dataclass

from .alerts import Alert, fmt_tokens, short

ZERO = "0x0000000000000000000000000000000000000000"

# Thresholds (tune these on simulated traffic in Phase 3).
LARGE_TRANSFER_SHARE = 0.50  # >= 50% of the hourly limit in one transfer
DRAIN_PACE_SHARE = 0.80  # >= 80% of the hourly limit sent within an hour
BURST_COUNT = 3  # this many transfers ...
BURST_WINDOW = 60  # ... within this many seconds
FRESH_WALLET_AGE = 600  # wallet first funded less than 10 minutes ago ...
FRESH_DUMP_SHARE = 0.50  # ... and has already sent at least half of what it received
MINT_NEAR_CAP_SHARE = 0.90  # supply reaches 90% of the cap
EXTRA_RULE_BONUS = 8  # each extra rule matching the same event adds this

# Fixed scores for admin / guardian actions seen on chain.
ADMIN_EVENT_SCORES = {
    "Paused": (92, "Token paused"),
    "Unpaused": (60, "Token unpaused"),
    "Blocklisted": (55, "Address blocklisted"),
    "Unblocked": (60, "Address removed from blocklist"),
    "LimitChanged": (45, "Transfer limit changed"),
    "LimitProposed": (55, "Transfer limit change proposed (48h timelock)"),
    "GuardianProposed": (75, "New guardian proposed (48h timelock)"),
    "GuardianChanged": (75, "Guardian changed"),
    "GuardianRevoked": (85, "Guardian key revoked"),
    "AdminProposed": (85, "New admin proposed (48h timelock)"),
    "AdminChanged": (88, "Admin changed"),
    "PendingCancelled": (40, "Pending admin changes cancelled"),
}


@dataclass
class Context:
    """Live contract settings read once per poll."""

    transfer_limit: int
    admin: str
    guardian: str
    total_supply: int
    max_supply: int


@dataclass
class Meta:
    block: int
    timestamp: int
    tx_hash: str


class Detector:
    def __init__(self):
        # sender -> deque of (timestamp, value) for the last hour
        self.sent = defaultdict(deque)
        # address -> (first funded timestamp, total received so far)
        self.first_funded: dict[str, list[int]] = {}

    # ------------------------------------------------------------ helpers

    def _role(self, addr: str, ctx: Context) -> str:
        if addr == ctx.admin:
            return " (admin)"
        if addr == ctx.guardian:
            return " (guardian)"
        return ""

    def _prune(self, sender: str, now: int):
        q = self.sent[sender]
        while q and q[0][0] <= now - 3600:
            q.popleft()

    # ------------------------------------------------------------ entry point

    def observe(self, name: str, args: dict, meta: Meta, ctx: Context) -> list[Alert]:
        if name == "Transfer":
            return self._on_transfer(args, meta, ctx)
        if name in ADMIN_EVENT_SCORES:
            return [self._on_admin_event(name, args, meta, ctx)]
        return []

    # ------------------------------------------------------------ admin events

    def _on_admin_event(self, name: str, args: dict, meta: Meta, ctx: Context) -> Alert:
        score, title = ADMIN_EVENT_SCORES[name]
        details = []
        subject = None
        for key, value in args.items():
            if isinstance(value, str) and value.startswith("0x") and len(value) == 42:
                details.append(f"{key}: {value}{self._role(value, ctx)}")
                if key in ("account", "new_admin", "new_guardian"):
                    subject = value
            elif key in ("old_limit", "new_limit"):
                details.append(f"{key}: {fmt_tokens(value) if value else 'no limit'}")
            elif key == "eta":
                when = datetime.fromtimestamp(value, tz=timezone.utc)
                details.append(f"earliest effect: {when:%Y-%m-%d %H:%M} UTC (chain time)")
            else:
                details.append(f"{key}: {value}")
        if name == "LimitProposed" and args.get("new_limit") == 0:
            score, title = 70, "Proposal to REMOVE the transfer limit (48h timelock)"
        return Alert(
            score=score,
            title=title,
            details=details,
            rules=[f"admin_event:{name}"],
            block=meta.block,
            timestamp=meta.timestamp,
            tx_hash=meta.tx_hash,
            subject=subject,
        )

    # ------------------------------------------------------------ transfers

    def _on_transfer(self, args: dict, meta: Meta, ctx: Context) -> list[Alert]:
        sender, receiver, value = args["sender"], args["receiver"], args["value"]
        now = meta.timestamp

        if sender == ZERO:  # mint
            return self._on_mint(receiver, value, meta, ctx)
        if receiver == ZERO:  # burn: harmless, not tracked
            return []

        # Remember when each wallet was first funded.
        if receiver not in self.first_funded:
            self.first_funded[receiver] = [now, value]
        else:
            self.first_funded[receiver][1] += value

        self._prune(sender, now)
        self.sent[sender].append((now, value))

        if sender == ctx.admin:
            return []  # admin distributions are expected

        matched: list[tuple[str, int, str]] = []
        limit = ctx.transfer_limit

        if limit and value >= LARGE_TRANSFER_SHARE * limit:
            matched.append(
                ("large_transfer", 45, f"single transfer is {value / limit:.0%} of the hourly limit")
            )

        hour_total = sum(v for _, v in self.sent[sender])
        if limit and hour_total >= DRAIN_PACE_SHARE * limit:
            matched.append(
                (
                    "drain_pace",
                    72,
                    f"sent {fmt_tokens(hour_total)} in the last hour "
                    f"({hour_total / limit:.0%} of the limit)",
                )
            )

        recent = [t for t, _ in self.sent[sender] if t > now - BURST_WINDOW]
        if len(recent) >= BURST_COUNT:
            matched.append(
                ("burst", 55, f"{len(recent)} transfers within {BURST_WINDOW} seconds")
            )

        funded = self.first_funded.get(sender)
        if funded:
            age = now - funded[0]
            # hour_total covers everything sent since funding, as age < 1 hour.
            if age < FRESH_WALLET_AGE and hour_total >= FRESH_DUMP_SHARE * funded[1]:
                matched.append(
                    (
                        "fresh_wallet_dump",
                        60,
                        f"wallet first funded {age} seconds ago and has already sent "
                        f"{hour_total / funded[1]:.0%} of what it received",
                    )
                )

        if not matched:
            return []

        matched.sort(key=lambda m: m[1], reverse=True)
        score = min(100, matched[0][1] + EXTRA_RULE_BONUS * (len(matched) - 1))
        details = [
            f"from {sender}",
            f"to {short(receiver)}",
            f"amount {fmt_tokens(value)}",
        ] + [m[2] for m in matched]
        title = {
            "drain_pace": "Wallet moving funds at the hourly limit",
            "fresh_wallet_dump": "New wallet dumping tokens",
            "burst": "Burst of transfers from one wallet",
            "large_transfer": "Large transfer",
        }[matched[0][0]]
        return [
            Alert(
                score=score,
                title=title,
                details=details,
                rules=[m[0] for m in matched],
                block=meta.block,
                timestamp=now,
                tx_hash=meta.tx_hash,
                subject=sender,
                related=[sender] + ([receiver] if self._young(receiver, now) else []),
                value=value,
            )
        ]

    def _young(self, addr: str, now: int) -> bool:
        funded = self.first_funded.get(addr)
        return bool(funded) and now - funded[0] < 86_400

    def _on_mint(self, receiver: str, value: int, meta: Meta, ctx: Context) -> list[Alert]:
        if receiver not in self.first_funded:
            self.first_funded[receiver] = [meta.timestamp, value]
        else:
            self.first_funded[receiver][1] += value
        share = ctx.total_supply / ctx.max_supply if ctx.max_supply else 0
        score, rule = (65, "mint_near_cap") if share >= MINT_NEAR_CAP_SHARE else (20, "mint")
        return [
            Alert(
                score=score,
                title="Tokens minted" if score < 40 else "Supply close to the cap",
                details=[
                    f"to {receiver}",
                    f"amount {fmt_tokens(value)}",
                    f"supply now {share:.0%} of the cap",
                ],
                rules=[rule],
                block=meta.block,
                timestamp=meta.timestamp,
                tx_hash=meta.tx_hash,
                subject=receiver,
            )
        ]
