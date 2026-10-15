"""
Deploy the token to a long-running local node (Anvil, Ganache, Hardhat).

    ape run deploy --network ethereum:local:node

Uses Ape's test accounts, which match the node's default funded accounts.
Prints the token address to put in .env as TOKEN_ADDRESS.

If TOKEN_ADDRESS_FILE is set (Docker does this), the address is also
written there, and an existing deployment at that address is reused
instead of deploying a second token.
"""

import os
from pathlib import Path

from ape import accounts, chain, project

TOKEN = 10**18


def existing_deployment(address_file):
    """Return the address in the file if a contract still lives there."""
    if not address_file or not address_file.exists():
        return None
    address = address_file.read_text().strip()
    if address and chain.provider.get_code(address):
        return address
    return None


def main():
    address_file = Path(os.environ["TOKEN_ADDRESS_FILE"]) if os.environ.get("TOKEN_ADDRESS_FILE") else None

    address = existing_deployment(address_file)
    if address:
        print(f"Token already deployed at {address}; reusing it.")
        return

    admin, guardian, alice, bob, carol = accounts.test_accounts[:5]
    token = admin.deploy(
        project.GuardedToken,
        "Guarded Test Token",
        "GTT",
        1_000_000 * TOKEN,
        admin,
        guardian,
        10_000 * TOKEN,
    )
    for user, amount in [(alice, 40_000), (bob, 20_000), (carol, 15_000)]:
        token.mint(user, amount * TOKEN, sender=admin)

    if address_file:
        address_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = address_file.with_suffix(".tmp")
        tmp.write_text(token.address)
        tmp.replace(address_file)
        print(f"\nToken deployed at {token.address}")
    else:
        print(f"\nTOKEN_ADDRESS={token.address}")
        print("Add that line to your .env, then start the monitor: python -m monitor")
