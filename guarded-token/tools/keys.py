"""
Create or import the wallets for a public deployment.

    python -m tools.keys new admin        # make a brand-new admin wallet
    python -m tools.keys new guardian     # make a brand-new guardian wallet
    python -m tools.keys import admin     # use an existing key (pasted, hidden)
    python -m tools.keys list             # show the addresses you have

Key files are encrypted with your password and saved in the secrets folder.
BACK UP that folder and remember the passwords: without them the wallets
(and control of the token) are gone for good.

Use NEW wallets only for this project, never one that holds real crypto.
"""

from __future__ import annotations

import getpass
import secrets
import sys

from . import cmd
from .keystore import KEYS_DIR, address_of, save


def main(argv=None):
    args = argv if argv is not None else sys.argv[1:]
    if not args or args[0] not in ("new", "import", "list"):
        print(cmd(__doc__))
        raise SystemExit(2)
    if args[0] == "list":
        files = sorted(KEYS_DIR.glob("*.json")) if KEYS_DIR.exists() else []
        if not files:
            print(cmd(f"No keys in {KEYS_DIR}/ yet. Create one with: python -m tools.keys new admin"))
        for f in files:
            print(f"{f.stem:12s} {address_of(f)}")
        return
    if len(args) < 2:
        raise SystemExit(cmd(f"Usage: python -m tools.keys {args[0]} <name>   (e.g. admin, guardian)"))
    name = args[1].lower()
    if args[0] == "new":
        key = "0x" + secrets.token_hex(32)
    else:
        key = getpass.getpass("Paste the private key (input is hidden): ").strip()
        if not key.startswith("0x"):
            key = "0x" + key
    address = save(name, key)
    print(f"\nSaved the {name} key, encrypted, as {name}.json in your secrets folder.")
    print(f"Address: {address}")
    print("Send it some Sepolia ETH (from a faucet or MetaMask) so it can pay for transactions.")


if __name__ == "__main__":
    main()
