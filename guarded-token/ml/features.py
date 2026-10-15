"""
Streaming features for each token transfer.

The extractor sees transfers one at a time, in order, and describes each
one using ONLY what happened before it (plus the transfer itself). That is
exactly what the live monitor can know, so the same code is used for
training and for live detection.

The four Phase 2 rules are included as yes/no features: the model learns
when a rule firing actually means trouble (e.g. "drain pace" from an
exchange hot wallet is normal; from a sleepy personal wallet it is not).
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from statistics import median

from monitor.rules import Context, Detector, Meta

from .simulate import LIMIT, ZERO

WEI = 10**18
DAY = 86_400
HOUR = 3_600

FEATURES = [
    "log_amount",
    "amount_to_limit",
    "frac_of_balance",
    "sender_age_h",
    "sender_history",
    "receiver_new",
    "receiver_age_h",
    "sender_tx_10m",
    "sender_tx_1h",
    "sender_out_1h_to_limit",
    "sender_out_24h_share",
    "sender_new_recv_1h",
    "sender_distinct_recv_1h",
    "amount_vs_own_median",
    "out_1h_vs_own_typical",
    "has_history",
    "secs_since_prev_send",
    "repeat_pair_24h",
    "receiver_young_senders_1h",
    "receiver_in_1h",
    "funder_new_wallets_24h",
    "reciprocal_24h",
    "two_hop_cycle_24h",
    "rule_drain_pace",
    "rule_burst",
    "rule_fresh_dump",
    "rule_large",
]

# Plain-English descriptions used to explain alerts (value formatting in explain()).
DESCRIPTIONS = {
    "amount_to_limit": "amount is {pct} of the hourly limit",
    "frac_of_balance": "sends {pct} of the wallet's balance",
    "sender_tx_1h": "{n} transfers from this wallet in the last hour",
    "sender_out_24h_share": "wallet moved out {pct} of its funds in 24h",
    "sender_new_recv_1h": "paid {n} brand-new wallets in the last hour",
    "sender_distinct_recv_1h": "paid {n} different wallets in the last hour",
    "amount_vs_own_median": "amount is {x} times this wallet's usual size",
    "out_1h_vs_own_typical": "hourly outflow is {x} times this wallet's normal",
    "repeat_pair_24h": "{n} transfers to the same receiver in 24h",
    "receiver_young_senders_1h": "receiver got funds from {n} new wallets in the last hour",
    "funder_new_wallets_24h": "this wallet's funder created {n} new wallets in 24h",
    "reciprocal_24h": "money flowing back and forth between the same two wallets",
    "two_hop_cycle_24h": "funds moving in a circle (A to B to C to A)",
}


class FeatureExtractor:
    def __init__(self, limit: float = LIMIT, admin: str | None = None, guardian: str = ZERO):
        self.limit = limit
        self.admin = admin
        self.ctx = Context(
            transfer_limit=int(limit * WEI),
            admin=admin or ZERO,
            guardian=guardian,
            total_supply=0,
            max_supply=0,
        )
        self.rules = Detector()
        self.balance: dict[str, float] = defaultdict(float)
        self.first_seen: dict[str, int] = {}
        self.first_funder: dict[str, str] = {}
        self.out: dict[str, deque] = defaultdict(deque)  # (t, receiver, value, receiver_was_new)
        self.inc: dict[str, deque] = defaultdict(deque)  # (t, sender, sender_was_young)
        self.n_sent: dict[str, int] = defaultdict(int)
        self.amounts: dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self.hours: dict[str, deque] = defaultdict(deque)  # [hour_index, sum], last 14 days
        self.last_send: dict[str, int] = {}
        self.fundings: dict[str, deque] = defaultdict(deque)  # times this wallet funded a new wallet

    # ------------------------------------------------------------ state upkeep

    @staticmethod
    def _prune(q: deque, now: int, horizon: int):
        while q and q[0][0] <= now - horizon:
            q.popleft()

    def _age_h(self, addr: str, now: int) -> float:
        return (now - self.first_seen[addr]) / HOUR if addr in self.first_seen else 0.0

    def _rule_flags(self, t, sender, receiver, value) -> tuple[list[str], int]:
        args = {"sender": sender, "receiver": receiver, "value": int(round(value * WEI))}
        alerts = self.rules.observe("Transfer", args, Meta(0, t, ""), self.ctx)
        if not alerts:
            return [], 0
        return alerts[0].rules, alerts[0].score

    # ------------------------------------------------------------ main entry

    def observe(self, t: int, sender: str, receiver: str, value: float):
        """
        Process one transfer (value in GTT). Returns (features, rule_score, rules)
        for transfers worth scoring, or None for mints, burns and admin sends.
        State is always updated.
        """
        rules, rule_score = self._rule_flags(t, sender, receiver, value)

        if sender == ZERO:  # mint
            self._credit(t, sender, receiver, value)
            return None
        if receiver == ZERO:  # burn
            self.balance[sender] -= value
            return None
        if sender == self.admin:
            self._debit(t, sender, receiver, value, receiver not in self.first_seen)
            self._credit(t, sender, receiver, value)
            return None

        for q in (self.out[sender], self.inc[sender], self.out[receiver], self.inc[receiver]):
            self._prune(q, t, DAY)

        bal_before = max(self.balance[sender], value)
        receiver_new = receiver not in self.first_seen
        out_1h = [o for o in self.out[sender] if o[0] > t - HOUR]
        out_10m = [o for o in out_1h if o[0] > t - 600]
        out_1h_sum = sum(o[2] for o in out_1h) + value
        out_24h_prev = sum(o[2] for o in self.out[sender])

        # This wallet's normal hourly outflow (median of its past active hours).
        hour_now = t // HOUR
        past_hours = [s for h, s in self.hours[sender] if h != hour_now]
        has_history = len(past_hours) >= 3 and self.n_sent[sender] >= 3
        typical = median(past_hours) if has_history else 0.0
        own_median = median(self.amounts[sender]) if len(self.amounts[sender]) >= 3 else 0.0

        new_recv = {o[1] for o in out_1h if o[3]} | ({receiver} if receiver_new else set())
        distinct_recv = {o[1] for o in out_1h} | {receiver}
        repeat_pair = sum(1 for o in self.out[sender] if o[1] == receiver)

        recv_in_1h = [i for i in self.inc[receiver] if i[0] > t - HOUR]
        sender_young = self._age_h(sender, t) < 24
        young_senders = {i[1] for i in recv_in_1h if i[2]} | ({sender} if sender_young else set())

        funder = self.first_funder.get(sender)
        funder_count = 0
        if funder and funder != ZERO:
            self._prune(self.fundings[funder], t, DAY)
            funder_count = len(self.fundings[funder])

        reciprocal = any(o[1] == sender for o in self.out[receiver])
        recv_outs = {o[1] for o in self.out[receiver]}
        sender_ins = {i[1] for i in self.inc[sender]}
        two_hop = bool(recv_outs & sender_ins)

        prev = self.last_send.get(sender)
        x = [
            math.log10(1 + value),
            value / self.limit,
            min(value / bal_before, 1.0) if bal_before > 0 else 1.0,
            math.log1p(self._age_h(sender, t)),
            math.log1p(self.n_sent[sender]),
            1.0 if receiver_new else 0.0,
            0.0 if receiver_new else math.log1p(self._age_h(receiver, t)),
            len(out_10m) + 1,
            len(out_1h) + 1,
            out_1h_sum / self.limit,
            min((out_24h_prev + value) / (bal_before + out_24h_prev), 1.0) if bal_before > 0 else 1.0,
            len(new_recv),
            len(distinct_recv),
            math.log10(value / own_median) if own_median > 0 else 0.0,
            math.log10(out_1h_sum / typical) if typical > 0 else 0.0,
            1.0 if has_history else 0.0,
            math.log1p(t - prev) if prev is not None else math.log1p(30 * DAY),
            math.log1p(repeat_pair),
            len(young_senders),
            len(recv_in_1h) + 1,
            funder_count,
            1.0 if reciprocal else 0.0,
            1.0 if two_hop else 0.0,
            1.0 if "drain_pace" in rules else 0.0,
            1.0 if "burst" in rules else 0.0,
            1.0 if "fresh_wallet_dump" in rules else 0.0,
            1.0 if "large_transfer" in rules else 0.0,
        ]

        self._debit(t, sender, receiver, value, receiver_new)
        self._credit(t, sender, receiver, value, sender_young)
        return x, rule_score, rules

    def _debit(self, t, sender, receiver, value, receiver_new):
        self.balance[sender] -= value
        self.first_seen.setdefault(sender, t)
        self.out[sender].append((t, receiver, value, receiver_new))
        self.n_sent[sender] += 1
        self.amounts[sender].append(value)
        self.last_send[sender] = t
        hours = self.hours[sender]
        h = t // HOUR
        if hours and hours[-1][0] == h:
            hours[-1][1] += value
        else:
            hours.append([h, value])
        while hours and hours[0][0] < h - 14 * 24:
            hours.popleft()
        if receiver_new:
            self.fundings[sender].append((t,))

    def _credit(self, t, sender, receiver, value, sender_young=False):
        self.balance[receiver] += value
        if receiver not in self.first_seen:
            self.first_seen[receiver] = t
            self.first_funder[receiver] = sender
        self.inc[receiver].append((t, sender, sender_young))


def explain(x: list[float], normal_p99: dict[str, float], top: int = 3) -> list[str]:
    """Plain-English reasons: the features most above what is normal."""
    values = dict(zip(FEATURES, x))
    scored = []
    for name, template in DESCRIPTIONS.items():
        v = values[name]
        ref = normal_p99.get(name, 0.0)
        if v > ref and v > 0:
            scored.append(((v - ref) / (abs(ref) + 1e-6), name))
    scored.sort(reverse=True)
    reasons = []
    for _, name in scored[: top + 1]:
        if len(reasons) >= top:
            break
        v = values[name]
        if name in ("amount_vs_own_median", "out_1h_vs_own_typical"):
            text = DESCRIPTIONS[name].format(x=f"{10 ** v:,.0f}")
        elif name in ("amount_to_limit", "frac_of_balance", "sender_out_24h_share"):
            text = DESCRIPTIONS[name].format(pct=f"{v:.0%}")
        elif name == "repeat_pair_24h":
            text = DESCRIPTIONS[name].format(n=round(math.expm1(v)) + 1)
        else:
            text = DESCRIPTIONS[name].format(n=int(v))
        reasons.append(text)
    # Youth is suspicious in the other direction (low values), so add it separately.
    age_h = math.expm1(values["sender_age_h"])
    if len(reasons) < top and age_h < 24:
        reasons.append(f"sending wallet first appeared {_ago(age_h)} ago")
    if len(reasons) < top and values["receiver_new"]:
        reasons.append("receiver is a brand-new wallet")
    return reasons


def _ago(hours: float) -> str:
    minutes = hours * 60
    if minutes < 90:
        return f"{max(1, round(minutes))} min"
    return f"{hours:.1f} h"

