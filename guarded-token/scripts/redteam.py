"""
Red-team drill: every attack type, on-chain, with and without the guardian.

    ape run redteam

For each attack, a fresh token is deployed, 20 users trade normally for a few
simulated days (with an exchange), then the attack runs while normal trading
continues. Each attack runs twice:
  alerts only   models + Telegram-style alerts, guardian off
  guardian on   the Phase 4 guardian acts on incidents

Measured:
  escaped   tokens the attacker cashed out at the exchange, plus tokens left in
            attacker wallets that are NOT blocklisted (usable later)
  frozen    tokens stuck in blocklisted attacker wallets
  harm      innocent users blocklisted, normal transfers that failed, token paused

Writes reports/redteam.md.
"""

import os
import random
from collections import Counter
from pathlib import Path

from ape.logging import LogLevel, logger

logger.set_level(LogLevel.ERROR)
from ape import accounts, chain, project
from ape.exceptions import ContractLogicError
from nacl.signing import SigningKey

from monitor.chain import ChainMonitor
from monitor.guardian import Executor, Responder, local_test_key
from monitor.ml_detector import GuardDetector
from monitor.notify import Notifier
from monitor.signing import Signer

TOKEN = 10**18
MIN, HOUR, DAY = 60, 3600, 86_400
MODEL = os.environ.get("MODEL_PATH", "models/guard_model.joblib")
REPORT = Path("reports/redteam.md")


class Collect:
    def __init__(self):
        self.sent = []

    def send(self, text):
        self.sent.append(text)


class Run:
    """One attack, one mode, on a fresh token."""

    def __init__(self, guardian_on: bool, seed: int):
        self.rng = random.Random(seed)
        acct = accounts.test_accounts
        self.admin, self.guardian = acct[0], acct[1]
        self.users = list(acct[2:22])
        self.exchange = acct[22]
        self.pool = list(acct[23:80])  # fresh wallets for attackers
        self.token = self.admin.deploy(
            project.GuardedToken, "Guarded Test Token", "GTT",
            10_000_000 * TOKEN, self.admin, self.guardian, 10_000 * TOKEN,
        )
        self.sender = Collect()
        self.notifier = Notifier(self.sender, Signer(SigningKey.generate()), log_path=None)
        self.responder = None
        if guardian_on:
            ex = Executor(chain.provider.web3, self.token.address, local_test_key(1))
            self.responder = Responder(ex, self.notifier, mode="on", protected={self.exchange.address})
        self.monitor = ChainMonitor(
            chain.provider.web3, self.token.address, self.notifier,
            detector=GuardDetector.load(MODEL), heartbeat_path=None, heartbeat_every=0,
            responder=self.responder,
        )
        self.attacker_wallets: list = []
        self.cashed_out = 0
        self.attempted = self.succeeded = 0
        self.normal_attempted = self.normal_failed = 0
        self.fail_reasons = Counter()
        self.friends = {}

    # ------------------------------------------------------------ helpers

    def wait(self, seconds):
        chain.pending_timestamp += int(seconds)
        chain.mine()

    def send(self, frm, to, amount, attack=True) -> int:
        amount = int(min(amount, self.token.balanceOf(frm)))
        if amount <= 0:
            return 0
        if attack:
            self.attempted += 1
        else:
            self.normal_attempted += 1
        try:
            with logger.at_level(LogLevel.ERROR + 10):
                self.token.transfer(to, amount, sender=frm)
        except ContractLogicError as err:
            if not attack:
                self.normal_failed += 1
                self.fail_reasons[err.message] += 1
            return 0
        if attack:
            self.succeeded += 1
            if to == self.exchange and frm in self.attacker_wallets:
                self.cashed_out += amount
        self.monitor.poll_once()
        return amount

    def fresh(self, n=1):
        out = [self.pool.pop(0) for _ in range(n)]
        self.attacker_wallets += out
        return out

    def normal_step(self):
        """One piece of normal background activity."""
        r = self.rng.random()
        u = self.rng.choice(self.users)
        if r < 0.6:
            self.send(u, self.rng.choice(self.friends[u]), self.rng.randint(20, 250) * TOKEN, attack=False)
        elif r < 0.8:
            self.send(u, self.exchange, self.rng.randint(50, 600) * TOKEN, attack=False)
        else:
            self.send(self.exchange, u, self.rng.randint(50, 600) * TOKEN, attack=False)

    def setup(self):
        for u in self.users:
            self.token.mint(u, self.rng.randint(2_000, 8_000) * TOKEN, sender=self.admin)
        self.token.mint(self.exchange, 500_000 * TOKEN, sender=self.admin)
        self.friends = {u: self.rng.sample([v for v in self.users if v != u], 4) for u in self.users}
        for _ in range(110):  # ~3 days of normal activity
            self.wait(self.rng.uniform(20 * MIN, 50 * MIN))
            self.normal_step()
        self.monitor.poll_once()

    def step(self, seconds):
        """Let time pass, with a little normal traffic in between."""
        self.wait(seconds)
        if self.rng.random() < 0.5:
            self.normal_step()

    # ------------------------------------------------------------ attacks

    def drain(self):
        victim = self.users[0]
        self.token.mint(victim, 6_000 * TOKEN, sender=self.admin)
        a, = self.fresh(1)
        exits = self.fresh(2)
        self.send(victim, a, self.token.balanceOf(victim) * 9 // 10)
        for k in range(3):
            self.step(self.rng.uniform(30, 90))
            self.send(a, exits[k % 2], self.token.balanceOf(a) // (3 - k))
        self.step(10 * MIN)
        for e in exits:
            self.send(e, self.exchange, self.token.balanceOf(e))
        return {"victim": victim}

    def fan_out(self):
        victim = self.users[0]
        self.token.mint(victim, 6_000 * TOKEN, sender=self.admin)
        mules = self.fresh(12)
        collector, = self.fresh(1)
        for m in mules:
            self.step(self.rng.uniform(2 * MIN, 4 * MIN))
            self.send(victim, m, self.rng.randint(380, 460) * TOKEN)
        self.step(40 * MIN)
        for m in mules:
            self.step(self.rng.uniform(1 * MIN, 3 * MIN))
            self.send(m, collector, self.token.balanceOf(m))
        self.step(2 * HOUR)
        self.send(collector, self.exchange, self.token.balanceOf(collector))
        return {"victim": victim}

    def low_slow(self):
        victim = self.users[0]
        self.token.mint(victim, 12_000 * TOKEN, sender=self.admin)
        b, = self.fresh(1)
        for i in range(6):
            self.send(victim, b, self.rng.randint(2_000, 3_000) * TOKEN)
            for _ in range(4):
                self.step(30 * MIN)
            if i % 2 == 1:
                self.send(b, self.exchange, self.token.balanceOf(b) * 8 // 10)
        self.send(b, self.exchange, self.token.balanceOf(b))
        return {"victim": victim}

    def sybil_farm(self):
        funder, = self.fresh(1)
        self.send(self.exchange, funder, 2_600 * TOKEN, attack=False)  # looks like a withdrawal
        farm = self.fresh(20)
        target, = self.fresh(1)
        for w in farm:
            self.step(self.rng.uniform(20, 40))
            self.send(funder, w, self.rng.randint(100, 120) * TOKEN)
        self.step(HOUR)
        for w in farm:
            self.step(self.rng.uniform(5, 15))
            self.send(w, target, self.token.balanceOf(w))
        self.step(20 * MIN)
        self.send(target, self.exchange, self.token.balanceOf(target))
        return {}

    def wash_cycle(self):
        ring = self.fresh(3)
        for w in ring:
            self.send(self.exchange, w, 2_000 * TOKEN, attack=False)
        self.step(15 * MIN)
        self.fake_volume = 0
        for i in range(30):  # ~2 hours of passing tokens around
            self.step(self.rng.uniform(3 * MIN, 5 * MIN))
            self.fake_volume += self.send(ring[i % 3], ring[(i + 1) % 3], 700 * TOKEN)
        return {}

    # ------------------------------------------------------------ result

    def result(self, kind, info):
        t = self.token
        frozen = sum(t.balanceOf(w) for w in self.attacker_wallets if t.blocklisted(w))
        loose = sum(t.balanceOf(w) for w in self.attacker_wallets if not t.blocklisted(w))
        innocent = sum(1 for u in self.users if t.blocklisted(u) and u != info.get("victim"))
        return {
            "kind": kind,
            "escaped": (self.cashed_out + loose) / TOKEN,
            "frozen": frozen / TOKEN,
            "victim_kept": t.balanceOf(info["victim"]) / TOKEN if info.get("victim") else None,
            "fake_volume": getattr(self, "fake_volume", 0) / TOKEN,
            "attempted": self.attempted,
            "succeeded": self.succeeded,
            "innocent_blocked": innocent,
            "normal_failed": self.normal_failed,
            "normal_attempted": self.normal_attempted,
            "paused": t.paused(),
            "messages": len(self.sender.sent),
            "actions": len(self.responder.actions_taken) if self.responder else 0,
            "fail_reasons": dict(self.fail_reasons),
        }


def fmt(x):
    return "-" if x is None else f"{x:,.0f}"


def main():
    kinds = ["drain", "fan_out", "low_slow", "sybil_farm", "wash_cycle"]
    results = []
    for i, kind in enumerate(kinds):
        for on in (False, True):
            label = "guardian on " if on else "alerts only "
            print(f"Running {kind:11s} ({label}) ...", flush=True)
            run = Run(guardian_on=on, seed=100 + i)
            run.setup()
            info = getattr(run, kind)()
            for _ in range(3):  # let incidents settle
                run.step(30 * MIN)
            r = run.result(kind, info) | {"guardian": on}
            results.append(r)
            print(
                f"    escaped {fmt(r['escaped'])} GTT, frozen {fmt(r['frozen'])}, "
                f"attack transfers {r['succeeded']}/{r['attempted']} succeeded, "
                f"innocent blocked {r['innocent_blocked']}, paused {r['paused']}"
            )
    write_report(results)
    print(f"\nReport written to {REPORT}")


def write_report(results):
    REPORT.parent.mkdir(exist_ok=True)
    L = ["# Red-team drill: attacks vs the Phase 4 guardian\n"]
    L.append(
        "Each attack ran on a fresh token after ~3 simulated days of normal trading by 20 users "
        "and an exchange, with normal trading continuing during the attack. Generated by "
        "`ape run redteam`.\n"
    )
    L.append("| Attack | Mode | Escaped (GTT) | Frozen (GTT) | Victim kept (GTT) | Fake volume (GTT) "
             "| Attack transfers that went through | Innocent users blocklisted | Normal transfers failed | Token paused |")
    L.append("|---|---|---|---|---|---|---|---|---|---|")
    for r in results:
        L.append(
            f"| {r['kind']} | {'guardian on' if r['guardian'] else 'alerts only'} | {fmt(r['escaped'])} | "
            f"{fmt(r['frozen'])} | {fmt(r['victim_kept'])} | "
            f"{fmt(r['fake_volume']) if r['kind'] == 'wash_cycle' else '-'} | "
            f"{r['succeeded']}/{r['attempted']} | {r['innocent_blocked']} | "
            f"{r['normal_failed']}/{r['normal_attempted']} | {'yes' if r['paused'] else 'no'} |"
        )
    L.append("")
    reasons = Counter()
    for r in results:
        reasons.update(r["fail_reasons"])
    if reasons:
        L.append("Why normal transfers failed: " + ", ".join(f"{k} ({v})" for k, v in reasons.items())
                 + ". The guardian's limit tightening applies to everyone, so busy wallets such as the "
                 "exchange hit the lower hourly limit until the admin raises it again.\n")
    L.append("**Escaped** = cashed out at the exchange + left in attacker wallets that are not "
             "blocklisted. **Frozen** = stuck in blocklisted attacker wallets. The victim's own wallet "
             "is blocklisted too when it is part of an incident (its key is presumed stolen); the "
             "admin unblocks it after the owner secures a new key.\n")
    L.append("**Limits:** one run per attack with a fixed random seed, on a small simulated "
             "population; the models were trained on the Phase 3 simulator, not on these exact "
             "wallets. Treat the numbers as a drill, not a guarantee.")
    REPORT.write_text("\n".join(L) + "\n")
