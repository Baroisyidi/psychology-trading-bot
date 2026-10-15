"""
Play an attack against a running local node while the monitor watches.

    ape run attack_sim --network ethereum:local:node            # drain (default)
    ATTACK=fanout ape run attack_sim --network ethereum:local:node

With Docker:
    docker compose run --rm attack          # drain
    docker compose run --rm attack fanout   # fan-out (Phase 3 demo)

Needs TOKEN_ADDRESS (Docker sets it; otherwise put it in .env).
"""

import os
import random
import time

from ape.logging import LogLevel, logger

logger.set_level(LogLevel.SUCCESS)  # QUIET: hide 'INFO: Confirmed ...' lines
from ape import accounts, chain, project
from ape.exceptions import ApeException
from dotenv import load_dotenv

TOKEN = 10**18
MIN = 60


def jump(seconds):
    """Fast-forward the local chain's clock (Ganache / Anvil / Hardhat)."""
    w3 = chain.provider.web3
    w3.provider.make_request("evm_increaseTime", [int(seconds)])
    w3.provider.make_request("evm_mine", [])


STOPPED = []


def attempt(token, label, frm, to, amount) -> bool:
    """Try an attacker transfer; report it if the guardian has stopped it."""
    if amount <= 0:
        return False
    try:
        with logger.at_level(LogLevel.ERROR + 10):
            token.transfer(to, amount, sender=frm)
        return True
    except ApeException:
        why = ("sender blocklisted" if token.blocklisted(frm)
               else "token paused" if token.paused()
               else "hourly limit exceeded")
        STOPPED.append(label)
        print(f"  [stopped] {label}: {why}")
        return False


def drain(token):
    admin, guardian, alice, bob, carol, attacker, mule = accounts.test_accounts[:7]
    if token.blocklisted(attacker):
        raise SystemExit("This attacker wallet is already blocklisted. Restart the stack first: "
                         "docker compose down; docker compose up -d")

    print("Normal user trades a small amount...")
    attempt(token, "normal 50 GTT payment", alice, bob, 50 * TOKEN)
    time.sleep(3)

    print("Alice's key is stolen: 9,500 GTT moved to a fresh wallet...")
    if not attempt(token, "theft of 9,500 GTT", alice, attacker, 9_500 * TOKEN):
        print("  (The guardian's tightened limit from an earlier incident still applies. "
              "Restart the stack for a fresh demo: docker compose down; docker compose up -d)")
        return
    time.sleep(3)

    print("Fresh wallet dumps in quick bursts...")
    for amount in (4_000, 3_000, 1_500):
        attempt(token, f"dump of {amount:,} GTT", attacker, mule, amount * TOKEN)
        time.sleep(2)

    print(f"Mule wallet holds {token.balanceOf(mule) / TOKEN:,.0f} GTT; "
          f"attacker wallet blocklisted: {token.blocklisted(attacker)}.")


def fanout(token):
    """An attack built to slip under every Phase 2 rule. Uses its own accounts."""
    rng = random.Random()
    acct = accounts.test_accounts
    admin = acct[0]
    users = list(acct[7:19])
    victim = users[0]
    mules = list(acct[20:32])
    collector = acct[33]

    if token.balanceOf(mules[0]) > 0:
        print("Note: these mule wallets were used before, so they are no longer 'new'.")
        print("For the clearest result, restart the stack first: docker compose down; docker compose up -d")

    print("Funding 12 users and letting them trade normally for ~2 simulated days...")
    for u in users:
        token.mint(u, rng.randint(1_500, 6_000) * TOKEN, sender=admin)
    token.mint(victim, 8_000 * TOKEN, sender=admin)
    friends = {u: rng.sample([v for v in users if v != u], 4) for u in users}
    for i in range(60):
        jump(rng.uniform(25 * MIN, 60 * MIN))
        u = rng.choice(users)
        token.transfer(rng.choice(friends[u]), rng.randint(20, 250) * TOKEN, sender=u)
        if i % 10 == 9:
            time.sleep(1)  # let the monitor keep up

    print("Victim's key stolen: small payments to 12 brand-new wallets, minutes apart...")
    for i, m in enumerate(mules, 1):
        jump(rng.uniform(2 * MIN, 4 * MIN))
        attempt(token, f"payment to mule {i}", victim, m, rng.randint(380, 460) * TOKEN)
        time.sleep(1.5)  # give the monitor a moment, as in real time

    print("40 minutes later, the mules try to forward everything to one collector...")
    jump(40 * MIN)
    for i, m in enumerate(mules, 1):
        jump(rng.uniform(1 * MIN, 3 * MIN))
        attempt(token, f"mule {i} forwarding", m, collector, token.balanceOf(m))
        time.sleep(1)
    frozen = sum(token.balanceOf(m) for m in mules if token.blocklisted(m)) / TOKEN
    print(f"Collector (attacker) holds {token.balanceOf(collector) / TOKEN:,.0f} GTT; "
          f"{frozen:,.0f} GTT frozen in blocklisted mules; {len(STOPPED)} attacker transfer(s) stopped.")
    print("Every transfer was designed to stay under the Phase 2 rule thresholds.")


def main():
    load_dotenv()
    address = os.environ.get("TOKEN_ADDRESS")
    if not address:
        raise SystemExit("Set TOKEN_ADDRESS in .env first (run `ape run deploy`).")
    token = project.GuardedToken.at(address)
    scenario = os.environ.get("ATTACK", "drain").lower()
    if scenario == "fanout":
        fanout(token)
    elif scenario == "drain":
        drain(token)
    else:
        raise SystemExit(f"Unknown attack '{scenario}'. Use: drain or fanout")
    print("Done. Check your Telegram chat or the monitor's console.")
