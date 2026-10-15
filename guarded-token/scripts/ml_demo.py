"""
Phase 3 + 4 demo: an attack the Phase 2 rules miss, caught by the trained
models and stopped by the guardian.

    ape run ml_demo                       # guardian ON (default for this demo)
    DEMO_GUARDIAN=off ape run ml_demo     # alerts only, to compare

Two monitors watch the same token side by side:
  - the Phase 2 rules (results printed at the end)
  - the Phase 3 models (alerts go to Telegram if configured, else printed)

Story: 20 users trade normally for a few days. Then a stolen wallet is
emptied through a "fan-out": small payments to 12 brand-new mule wallets,
minutes apart, each under every rule threshold. Later the mules forward
everything to one collector wallet.
"""

import os
import random

from ape.logging import LogLevel, logger

logger.set_level(LogLevel.SUCCESS)  # QUIET: hide 'INFO: Confirmed ...' lines
from ape import accounts, chain, project
from nacl.signing import SigningKey

from ape.exceptions import ContractLogicError

from monitor.chain import ChainMonitor
from monitor.config import Settings
from monitor.notify import Notifier
from monitor.rules import Detector
from monitor.signing import Signer

TOKEN = 10**18
MIN = 60


class Collect:
    """A sender that just remembers messages (for the rules monitor)."""

    def __init__(self):
        self.sent = []

    def send(self, text):
        self.sent.append(text.splitlines()[0])


def wait(seconds):
    chain.pending_timestamp += int(seconds)
    chain.mine()


def main():
    rng = random.Random(7)
    acct = accounts.test_accounts
    admin, guardian = acct[0], acct[1]
    users = list(acct[2:22])  # 20 normal users
    victim = users[0]
    mules = list(acct[22:34])  # 12 wallets that never touched the token
    collector = acct[34]

    token = admin.deploy(
        project.GuardedToken, "Guarded Test Token", "GTT",
        1_000_000 * TOKEN, admin, guardian, 10_000 * TOKEN,
    )

    settings = Settings.load()
    if settings.detector == "rules":
        settings.detector = "auto"  # this demo is about the models
    settings.guardian_mode = os.environ.get("DEMO_GUARDIAN", "on").lower()
    notifier = settings.make_notifier()
    ml_monitor = ChainMonitor(
        chain.provider.web3, token.address,
        notifier,
        detector=settings.make_detector(),
        heartbeat_path=None, heartbeat_every=0,
        responder=settings.make_responder(chain.provider.web3, token.address, notifier),
    )
    sent = {"n": 0}
    real_send = ml_monitor.notifier.sender.send

    def counting_send(text):
        sent["n"] += 1
        real_send(text)

    ml_monitor.notifier.sender.send = counting_send
    rules_sender = Collect()
    rules_monitor = ChainMonitor(
        chain.provider.web3, token.address,
        Notifier(rules_sender, Signer(SigningKey.generate()), log_path=None),
        detector=Detector(),
        heartbeat_path=None, heartbeat_every=0,
    )

    if ml_monitor.responder is not None:  # the rules monitor shouldn't re-alert on the guardian's actions
        rules_monitor.notifier.quiet_txs = ml_monitor.responder.own_txs

    def poll():
        ml = ml_monitor.poll_once()
        rules_monitor.poll_once()
        return ml

    print("\n######## Day 0: the project funds 20 users")
    for u in users:
        token.mint(u, rng.randint(1_500, 6_000) * TOKEN, sender=admin)
    token.mint(victim, 8_000 * TOKEN, sender=admin)
    poll()

    print("######## Days 1-4: normal trading (about 140 transfers)")
    friends = {u: rng.sample([v for v in users if v != u], 4) for u in users}
    for _ in range(140):
        wait(rng.uniform(20 * MIN, 50 * MIN))
        u = rng.choice(users)
        token.transfer(rng.choice(friends[u]), rng.randint(20, 250) * TOKEN, sender=u)
        poll()
    print(f"  normal trading done; rules raised {len(rules_sender.sent)} message(s)")

    blocked = []

    def attempt(label, fn, *args, **kw):
        try:
            with logger.at_level(LogLevel.ERROR + 10):
                fn(*args, **kw)
        except ContractLogicError as err:
            blocked.append(err.message)
            print(f"  [stopped] {label}: {err.message}")

    print("\n######## Attack: victim's key stolen; funds fanned out to 12 new wallets")
    for i, m in enumerate(mules, 1):
        wait(rng.uniform(2 * MIN, 4 * MIN))
        attempt(f"payment to mule {i}", token.transfer, m, rng.randint(380, 460) * TOKEN, sender=victim)
        poll()

    print("######## 40 minutes later, the mules try to forward everything to one collector")
    wait(40 * MIN)
    for i, m in enumerate(mules, 1):
        wait(rng.uniform(1 * MIN, 3 * MIN))
        if token.balanceOf(m) > 0:
            attempt(f"mule {i} forwarding", token.transfer, collector, token.balanceOf(m), sender=m)
        poll()

    print("######## 2 quiet hours later")
    wait(125 * MIN)
    poll()

    stolen = token.balanceOf(collector) / TOKEN
    frozen = sum(token.balanceOf(m) for m in mules if token.blocklisted(m)) / TOKEN
    loose = sum(token.balanceOf(m) for m in mules if not token.blocklisted(m)) / TOKEN
    print("\n######## Result")
    print(f"  Collector (attacker) holds {stolen:,.0f} GTT.")
    print(f"  Frozen in blocklisted mule wallets: {frozen:,.0f} GTT; loose in mules: {loose:,.0f} GTT.")
    print(f"  Victim still holds {token.balanceOf(victim) / TOKEN:,.0f} GTT "
          f"(blocklisted: {token.blocklisted(victim)}); {len(blocked)} attacker transfer(s) stopped.")
    print(f"  Phase 2 rules: {len(rules_sender.sent)} alert message(s)" + (
        ": " + "; ".join(rules_sender.sent) if rules_sender.sent else " - the attack went unnoticed."))
    print(f"  Phase 3 models: {sent['n']} message(s) for the whole attack, grouped as one incident"
          " (see above or in Telegram).")
