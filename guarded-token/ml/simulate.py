"""
Traffic simulator for training and testing the detectors.

It produces a time-ordered list of token transfers between simulated
wallets, each labeled normal (0) or attack (1). Simulating at the event
level (instead of sending real transactions to a chain) lets us create
weeks of traffic in seconds; the live monitor sees the same kind of
Transfer events from the chain.

Normal behaviour deliberately includes things that LOOK suspicious, so a
detector cannot win just by flagging anything unusual:
  - exchanges withdrawing close to the hourly limit at busy times
  - brand-new users spending most of their first funds within minutes
  - a payroll wallet paying dozens of people at once
  - market-maker bots sending bursts of transfers
  - merchants sweeping most of their balance to an exchange

Attack types (all transfers sent by attacker-controlled or compromised
wallets during an attack are labeled 1):
  drain       stolen wallet emptied into a fresh wallet that dumps fast
  fan_out     stolen funds split across many fresh "mule" wallets, which
              later forward everything to one collector (peel chain)
  low_slow    stolen wallet drained a little at a time over many hours
  sybil_farm  one funder creates dozens of fresh wallets that later act
              together (airdrop farming / coordinated dumping)
  wash_cycle  a ring of wallets passes tokens around to fake volume

Every transfer respects balances and the contract's hourly limit.
"""

from __future__ import annotations

import heapq
import math
import random
from collections import defaultdict, deque
from dataclasses import dataclass

LIMIT = 10_000.0  # GTT per wallet per hour, as in the contract
DAY = 86_400
HOUR = 3_600
ATTACK_KINDS = ["drain", "fan_out", "low_slow", "sybil_farm", "wash_cycle"]

ZERO = "0x" + "0" * 40


@dataclass
class Transfer:
    t: int
    sender: str
    receiver: str
    value: float  # GTT
    label: int  # 1 = attack
    kind: str  # behaviour that produced it, e.g. "user", "exchange", "fan_out"
    incident: int = -1  # attack incident id, -1 for normal traffic


class World:
    def __init__(self, seed: int, days: int, n_users: int = 400, incidents_per_kind: int = 8):
        self.rng = random.Random(seed)
        self.days = days
        self.end = days * DAY
        self.n_users = n_users
        self.incidents_per_kind = incidents_per_kind

        self.balance: dict[str, float] = defaultdict(float)
        self.sent_window: dict[str, deque] = defaultdict(deque)  # for the hourly limit
        self.events: list[Transfer] = []
        self._queue: list = []
        self._seq = 0
        self._incident = 0

        self.admin = self._addr()
        self.exchanges = [self._addr() for _ in range(3)]
        self.merchants = [self._addr() for _ in range(6)]
        self.merchant_weights = [1 / (i + 1) for i in range(len(self.merchants))]
        self.payroll = self._addr()
        self.marketing = self._addr()
        self.market_makers = [self._addr() for _ in range(5)]
        self.users: list[str] = []
        self.friends: dict[str, list[str]] = {}
        self.profile: dict[str, tuple[float, float]] = {}  # rate per day, typical amount

    # ------------------------------------------------------------ helpers

    def _addr(self) -> str:
        return f"0x{self.rng.getrandbits(160):040x}"

    def at(self, t: float, fn, *args):
        if t < self.end:
            self._seq += 1
            heapq.heappush(self._queue, (int(t), self._seq, fn, args))

    def lognormal(self, median: float, sigma: float) -> float:
        return median * math.exp(self.rng.gauss(0, sigma))

    def daytime_ok(self, t: float) -> bool:
        """Thin out activity at night (simulated local time)."""
        hour = (t % DAY) / HOUR
        weight = 0.15 if hour < 6 else 0.6 if hour < 8 else 1.0 if hour < 23 else 0.4
        return self.rng.random() < weight

    def allowance(self, sender: str, t: float) -> float:
        q = self.sent_window[sender]
        while q and q[0][0] <= t - HOUR:
            q.popleft()
        return LIMIT - sum(v for _, v in q)

    def transfer(self, t, sender, receiver, amount, label=0, kind="user", incident=-1) -> float:
        """Execute a transfer if balance and the hourly limit allow. Returns the amount sent."""
        if sender == receiver:
            return 0.0
        amount = min(amount, self.balance[sender])
        if sender != self.admin:
            amount = min(amount, self.allowance(sender, t))
        amount = round(amount, 2)
        if amount < 0.01:
            return 0.0
        self.balance[sender] -= amount
        self.balance[receiver] += amount
        if sender != self.admin:
            self.sent_window[sender].append((t, amount))
        self.events.append(Transfer(int(t), sender, receiver, amount, label, kind, incident))
        return amount

    def mint(self, t, receiver, amount):
        self.balance[receiver] += amount
        self.events.append(Transfer(int(t), ZERO, receiver, amount, 0, "mint"))

    # ------------------------------------------------------------ normal actors

    def add_user(self, t: float, addr: str):
        self.users.append(addr)
        rate = self.lognormal(1.5, 0.8)  # transfers per day
        typical = self.lognormal(80, 0.9)  # GTT
        self.profile[addr] = (rate, typical)
        pool = [u for u in self.users if u != addr]
        self.friends[addr] = self.rng.sample(pool, min(len(pool), self.rng.randint(3, 8)))
        self.at(t + self.rng.expovariate(rate / DAY), self.user_act, addr)

    def user_act(self, t, addr):
        rate, typical = self.profile[addr]
        self.at(t + self.rng.expovariate(rate / DAY), self.user_act, addr)
        if not self.daytime_ok(t):
            return
        amount = typical * self.lognormal(1, 0.6)
        r = self.rng.random()
        if r < 0.03:  # occasional big payment (rent, a car...)
            amount *= self.rng.uniform(8, 25)
        if self.rng.random() < 0.004 / max(rate, 0.1) and self.balance[addr] > 200:
            # Moves (almost) everything to a new wallet of their own: looks like a drain.
            self.migrate(t, addr)
            return
        if self.rng.random() < 0.005 and self.balance[addr] > 500:
            # Privacy-minded user spreads funds over several new wallets of their own.
            share = self.rng.uniform(0.3, 0.6) / (n := self.rng.randint(3, 6))
            for i in range(n):
                new = self._addr()
                self.at(t + i * self.rng.uniform(20, 300), self._split, addr, new, share)
            return
        if self.rng.random() < 0.02:
            # Onboards a friend (or tops up their own second wallet).
            new = self._addr()
            if self.transfer(t, addr, new, amount * 2, kind="user"):
                self.at(t + self.rng.uniform(600, DAY), self.add_user, new)
            return
        roll = self.rng.random()
        if roll < 0.45 and self.friends[addr]:
            to = self.rng.choice(self.friends[addr])
        elif roll < 0.70:
            to = self.rng.choices(self.merchants, self.merchant_weights)[0]
        elif roll < 0.90:
            to = self.rng.choice(self.exchanges)
            amount *= 3
        else:
            to = self.rng.choice(self.users)
        self.transfer(t, addr, to, amount)

    def migrate(self, t, addr):
        new = self._addr()
        parts = self.rng.choice([1, 1, 2])
        for i in range(parts):
            share = 1.0 if i == parts - 1 else self.rng.uniform(0.4, 0.6)
            self.at(t + i * self.rng.uniform(30, 600), self._move, addr, new, share * self.rng.uniform(0.92, 1.0))
        self.profile[addr] = (0.05, self.profile[addr][1])  # the old wallet goes quiet
        self.at(t + self.rng.uniform(HOUR, 2 * DAY), self.add_user, new)

    def _split(self, t, addr, new, share):
        if self.transfer(t, addr, new, self.balance[addr] * share / max(1 - share, 0.1), kind="split"):
            self.at(t + self.rng.uniform(HOUR, 2 * DAY), self.add_user, new)

    def _move(self, t, addr, new, share):
        self.transfer(t, addr, new, self.balance[addr] * share, kind="migration")

    def promo_airdrop(self, t):
        # A marketing campaign sends small gifts to many brand-new wallets.
        self.at(t + self.rng.uniform(5, 9) * DAY, self.promo_airdrop)
        tt = t
        for _ in range(self.rng.randint(20, 40)):
            new = self._addr()
            self.at(tt, self._gift, new)
            tt += self.rng.uniform(10, 60)

    def _gift(self, t, new):
        if self.transfer(t, self.marketing, new, self.rng.uniform(50, 120), kind="promo"):
            if self.rng.random() < 0.4:  # some cash out or spend it quickly
                to = self.rng.choice(self.exchanges + self.merchants)
                self.at(t + self.rng.uniform(300, 4 * HOUR), self._spend_all, new, to)
            self.at(t + self.rng.uniform(HOUR, 3 * DAY), self.add_user, new)

    def _spend_all(self, t, addr, to):
        self.transfer(t, addr, to, self.balance[addr] * self.rng.uniform(0.8, 1.0), kind="promo")

    def exchange_withdrawal(self, t, ex):
        # Busier during the day; peaks push hot wallets close to the limit.
        hour = (t % DAY) / HOUR
        busy = 2.5 if 14 <= hour < 20 else 1.0 if 8 <= hour < 23 else 0.3
        self.at(t + self.rng.expovariate(busy * 130 / DAY), self.exchange_withdrawal, ex)
        amount = self.lognormal(150, 1.0)
        if self.rng.random() < 0.02:
            amount *= self.rng.uniform(10, 30)  # whale withdrawal
        if self.rng.random() < 0.15:
            # A brand-new customer withdraws to a fresh wallet.
            new = self._addr()
            sent = self.transfer(t, ex, new, amount * 2, kind="exchange")
            if sent:
                if self.rng.random() < 0.35:
                    # Spends most of it right away (looks like a "fresh wallet dump").
                    merchant = self.rng.choices(self.merchants, self.merchant_weights)[0]
                    self.at(t + self.rng.uniform(60, 500), self.new_user_spend, new, merchant)
                self.at(t + self.rng.uniform(60, 6 * HOUR), self.add_user, new)
            return
        if self.users:
            self.transfer(t, ex, self.rng.choice(self.users), amount, kind="exchange")

    def new_user_spend(self, t, addr, merchant):
        self.transfer(t, addr, merchant, self.balance[addr] * self.rng.uniform(0.6, 1.0), kind="new_user")

    def merchant_sweep(self, t, m):
        self.at(t + self.rng.uniform(0.6, 1.6) * DAY, self.merchant_sweep, m)
        self.transfer(t, m, self.rng.choice(self.exchanges), self.balance[m] * self.rng.uniform(0.5, 0.9), kind="merchant")

    def payroll_run(self, t):
        self.at(t + 7 * DAY, self.payroll_run)
        staff = self.rng.sample(self.users, min(len(self.users), self.rng.randint(25, 40)))
        for i, u in enumerate(staff):
            self.at(t + i * self.rng.uniform(5, 20), self._pay, u)

    def _pay(self, t, u):
        self.transfer(t, self.payroll, u, self.rng.uniform(100, 280), kind="payroll")

    def market_maker(self, t, mm):
        self.at(t + self.rng.expovariate(60 / DAY), self.market_maker, mm)
        ex = self.rng.choice(self.exchanges)
        n = self.rng.choice([1, 1, 2, 3, 4])  # sometimes a burst
        for i in range(n):
            self.at(t + i * self.rng.uniform(3, 20), self._mm_send, mm, ex)

    def _mm_send(self, t, mm, ex):
        self.transfer(t, mm, ex, self.lognormal(400, 0.7), kind="market_maker")
        if self.rng.random() < 0.5:  # and gets some back
            self.at(t + 5, self._mm_refund, ex, mm)

    def _mm_refund(self, t, ex, mm):
        self.transfer(t, ex, mm, self.lognormal(400, 0.7), kind="exchange")

    # ------------------------------------------------------------ attacks

    def _victim(self, min_balance):
        rich = [u for u in self.users if self.balance[u] >= min_balance]
        return self.rng.choice(rich) if rich else None

    def _next_incident(self):
        self._incident += 1
        return self._incident

    def prepare_aged(self, t_attack, n):
        """Wallets the attacker prepares days earlier with small exchange withdrawals."""
        wallets = [self._addr() for _ in range(n)]
        for w in wallets:
            when = max(60, t_attack - self.rng.uniform(1, 5) * DAY)
            self.at(when, self._dust, w)
        return wallets

    def _dust(self, t, w):
        self.transfer(t, self.rng.choice(self.exchanges), w, self.rng.uniform(5, 40), kind="exchange")

    def atk_drain(self, t, aged=None):
        victim = self._victim(2000)
        if not victim:
            return
        inc = self._next_incident()
        a = aged[0] if aged else self._addr()
        exits = [self._addr() for _ in range(self.rng.randint(1, 2))]
        stolen = self.transfer(t, victim, a, self.balance[victim] * self.rng.uniform(0.85, 1.0), 1, "drain", inc)
        tt = t
        for k in range(self.rng.randint(2, 4)):
            tt += self.rng.uniform(10, 120)
            self.at(tt, self._send_share, a, self.rng.choice(exits), 1 / (4 - k) if k < 3 else 1.0, "drain", inc)
        for e in exits:
            self.at(tt + self.rng.uniform(300, 1800), self._send_share, e, self.rng.choice(self.exchanges), 1.0, "drain", inc)
        return stolen

    def _send_share(self, t, sender, receiver, share, kind, inc):
        self.transfer(t, sender, receiver, self.balance[sender] * share, 1, kind, inc)

    def atk_fan_out(self, t, mules=None):
        victim = self._victim(3000)
        if not victim:
            return
        inc = self._next_incident()
        mules = mules or [self._addr() for _ in range(self.rng.randint(10, 25))]
        k = len(mules)
        collector = self._addr()
        each = self.balance[victim] * self.rng.uniform(0.6, 0.9) / k
        each = min(each, 0.75 * LIMIT / k)  # stay under the obvious per-hour thresholds
        tt = t
        for m in mules:
            self.at(tt, self._fixed, victim, m, each, "fan_out", inc)
            fwd = tt + self.rng.uniform(15 * 60, 2 * HOUR)  # wait past "fresh wallet" checks
            self.at(fwd, self._send_share, m, collector, 1.0, "fan_out", inc)
            tt += self.rng.uniform(60, 300)
        cash = tt + 2 * HOUR + self.rng.uniform(HOUR, 6 * HOUR)
        for i in range(self.rng.randint(1, 3)):
            self.at(cash + i * 600, self._send_share, collector, self.rng.choice(self.exchanges), 0.5 if i < 2 else 1.0, "fan_out", inc)

    def _fixed(self, t, sender, receiver, amount, kind, inc):
        self.transfer(t, sender, receiver, amount, 1, kind, inc)

    def atk_low_slow(self, t):
        victim = self._victim(3000)
        if not victim:
            return
        inc = self._next_incident()
        b = self._addr()
        tt = t
        for _ in range(self.rng.randint(6, 12)):
            self.at(tt, self._fixed, victim, b, LIMIT * self.rng.uniform(0.15, 0.35), "low_slow", inc)
            tt += self.rng.uniform(1.5 * HOUR, 3 * HOUR)
        for i in range(3):
            self.at(t + (i + 1) * 6 * HOUR + self.rng.uniform(0, HOUR), self._send_share, b, self.rng.choice(self.exchanges), 0.8, "low_slow", inc)

    def atk_sybil_farm(self, t):
        inc = self._next_incident()
        funder = self._addr()
        ex = self.rng.choice(self.exchanges)
        k = self.rng.randint(20, 40)
        self.transfer(t, ex, funder, k * 130, kind="exchange")  # looks like a normal withdrawal
        wallets = [self._addr() for _ in range(k)]
        tt = t + self.rng.uniform(120, 900)
        gap = (10, 40) if self.rng.random() < 0.7 else (300, 900)  # some farms fund slowly
        for w in wallets:
            self.at(tt, self._fixed, funder, w, self.rng.uniform(80, 130), "sybil_farm", inc)
            tt += self.rng.uniform(*gap)
        target = self._addr()
        go = tt + self.rng.uniform(HOUR, 3 * HOUR)
        for w in wallets:
            self.at(go + self.rng.uniform(0, 300), self._send_share, w, target, self.rng.uniform(0.9, 1.0), "sybil_farm", inc)
        self.at(go + 1200, self._send_share, target, ex, 1.0, "sybil_farm", inc)

    def atk_wash_cycle(self, t):
        inc = self._next_incident()
        ring = [self._addr() for _ in range(self.rng.randint(3, 4))]
        for w in ring:
            self.transfer(t, self.rng.choice(self.exchanges), w, self.rng.uniform(1500, 3000), kind="exchange")
        amount = self.rng.uniform(400, 1500)
        tt = t + self.rng.uniform(300, 1800)
        end = tt + self.rng.uniform(1, 3) * HOUR
        i = 0
        while tt < end:
            s, r = ring[i % len(ring)], ring[(i + 1) % len(ring)]
            self.at(tt, self._fixed, s, r, amount * self.rng.uniform(0.95, 1.05), "wash_cycle", inc)
            tt += self.rng.uniform(120, 360)
            i += 1

    # ------------------------------------------------------------ run

    def run(self) -> list[Transfer]:
        rng = self.rng
        # Day 0: the project mints starting balances.
        for ex in self.exchanges:
            self.mint(0, ex, 2_000_000)
        for m in self.merchants:
            self.mint(0, m, rng.uniform(2_000, 10_000))
        for mm in self.market_makers:
            self.mint(0, mm, 50_000)
        self.mint(0, self.payroll, 200_000)
        self.mint(0, self.marketing, 100_000)
        for _ in range(self.n_users):
            u = self._addr()
            self.mint(0, u, self.lognormal(2000, 1.0))
            self.add_user(0, u)
        for ex in self.exchanges:
            self.at(rng.uniform(0, 600), self.exchange_withdrawal, ex)
        for m in self.merchants:
            self.at(rng.uniform(0.3, 1.0) * DAY, self.merchant_sweep, m)
        for mm in self.market_makers:
            self.at(rng.uniform(0, DAY), self.market_maker, mm)
        self.at(rng.uniform(0.5, 1.5) * DAY, self.payroll_run)
        self.at(rng.uniform(0.5, 3) * DAY, self.promo_airdrop)

        # Attacks start after a one-day warm-up, so baselines exist.
        attacks = {
            "drain": self.atk_drain,
            "fan_out": self.atk_fan_out,
            "low_slow": self.atk_low_slow,
            "sybil_farm": self.atk_sybil_farm,
            "wash_cycle": self.atk_wash_cycle,
        }
        for kind in ATTACK_KINDS:
            for _ in range(self.incidents_per_kind):
                t = rng.uniform(DAY, self.end - DAY)
                if kind == "drain" and rng.random() < 0.4:
                    self.at(t, attacks[kind], self.prepare_aged(t, 1))
                elif kind == "fan_out" and rng.random() < 0.4:
                    self.at(t, attacks[kind], self.prepare_aged(t, rng.randint(10, 25)))
                else:
                    self.at(t, attacks[kind])

        while self._queue:
            t, _, fn, args = heapq.heappop(self._queue)
            fn(t, *args)
        self.events.sort(key=lambda e: e.t)
        return self.events


def simulate(seed: int, days: int = 14, n_users: int = 400, incidents_per_kind: int = 8) -> list[Transfer]:
    return World(seed, days, n_users, incidents_per_kind).run()


def summary(events: list[Transfer]) -> str:
    transfers = [e for e in events if e.kind != "mint"]
    attacks = [e for e in transfers if e.label]
    by_kind = defaultdict(set)
    for e in attacks:
        by_kind[e.kind].add(e.incident)
    days = (transfers[-1].t - transfers[0].t) / DAY if transfers else 0
    parts = ", ".join(f"{k}: {len(by_kind[k])}" for k in ATTACK_KINDS)
    return (
        f"{len(transfers):,} transfers over {days:.1f} days "
        f"({len(transfers) / max(days, 1):,.0f}/day), "
        f"{len(attacks):,} attack transfers; incidents: {parts}"
    )


if __name__ == "__main__":
    import time

    start = time.time()
    ev = simulate(1, days=14)
    print(summary(ev), f"in {time.time() - start:.1f}s")
