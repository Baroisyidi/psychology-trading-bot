"""
Walk-through of an attack and the guardian's response, on a local chain.

Run with:   ape run demo

Nothing here costs money: Ape's local test chain gives every test account
plenty of fake ETH.
"""

from ape import accounts, chain, project
from ape.exceptions import ContractLogicError
from ape.logging import LogLevel, logger

logger.set_level(LogLevel.SUCCESS)  # QUIET: hide 'INFO: Confirmed ...' lines

TOKEN = 10**18


def fmt(amount):
    return f"{amount / TOKEN:,.0f} GTT"


def step(title):
    print(f"\n=== {title} ===")


def try_tx(label, fn, *args, **kwargs):
    try:
        # Hide Ape's traceback noise; we print the revert reason ourselves.
        with logger.at_level(LogLevel.ERROR + 10):
            fn(*args, **kwargs)
        print(f"  [ok]      {label}")
    except ContractLogicError as err:
        print(f"  [blocked] {label}  ->  {err.message}")


def main():
    admin, guardian, alice, bob, attacker = accounts.test_accounts[:5]

    step("1. Deploy the token")
    token = admin.deploy(
        project.GuardedToken,
        "Guarded Test Token",
        "GTT",
        1_000_000 * TOKEN,  # max supply
        admin,
        guardian,
        10_000 * TOKEN,  # hourly transfer limit per wallet
    )
    print(f"  Token deployed at {token.address}")
    print(f"  Admin:    {admin.address}")
    print(f"  Guardian: {guardian.address}  (the AI service's key)")

    step("2. Normal activity")
    token.mint(alice, 50_000 * TOKEN, sender=admin)
    token.transfer(bob, 500 * TOKEN, sender=alice)
    print(f"  Alice: {fmt(token.balanceOf(alice))}   Bob: {fmt(token.balanceOf(bob))}")

    step("3. An attacker gets tokens and starts draining")
    # Simulates stolen funds landing in the attacker's wallet.
    token.mint(attacker, 20_000 * TOKEN, sender=admin)
    print(f"  Attacker holds {fmt(token.balanceOf(attacker))}")
    try_tx("attacker sends 6,000 GTT", token.transfer, bob, 6_000 * TOKEN, sender=attacker)
    try_tx("attacker sends 3,000 GTT", token.transfer, bob, 3_000 * TOKEN, sender=attacker)
    try_tx("attacker sends 3,000 GTT more", token.transfer, bob, 3_000 * TOKEN, sender=attacker)
    print("  The hourly limit already slowed the drain before any AI reacted.")

    step("4. Guardian (AI, risk score 75) tightens the limit and blocklists")
    token.tighten_limit(1_000 * TOKEN, sender=guardian)
    print(f"  Hourly limit now {fmt(token.transfer_limit())}")
    token.block_address(attacker, sender=guardian)
    print(f"  Attacker blocklisted: {token.blocklisted(attacker)}")
    try_tx("attacker sends 1 GTT", token.transfer, bob, TOKEN, sender=attacker)

    step("5. Risk score hits 92: guardian pauses everything")
    token.pause(sender=guardian)
    try_tx("Alice sends 10 GTT while paused", token.transfer, bob, 10 * TOKEN, sender=alice)
    try_tx("guardian tries to unpause", token.unpause, sender=guardian)
    try_tx("guardian tries to mint itself tokens", token.mint, guardian, TOKEN, sender=guardian)

    step("6. Humans review, then the admin recovers")
    token.unpause(sender=admin)
    try_tx("Alice sends 10 GTT after unpause", token.transfer, bob, 10 * TOKEN, sender=alice)

    step("7. Loosening the limit must wait 48 hours")
    token.propose_limit(10_000 * TOKEN, sender=admin)
    try_tx("execute limit change immediately", token.execute_limit, sender=admin)
    chain.pending_timestamp += 48 * 3600
    chain.mine()
    try_tx("execute limit change after 48h", token.execute_limit, sender=admin)
    print(f"  Hourly limit now {fmt(token.transfer_limit())}")

    step("Summary")
    print(f"  Total supply: {fmt(token.totalSupply())}")
    print(f"  Attacker still holds {fmt(token.balanceOf(attacker))}, frozen by the blocklist.")
