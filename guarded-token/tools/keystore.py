"""
Encrypted key files (the standard Ethereum "keystore v3" format that
MetaMask, Geth and others use). The private key is encrypted with your
password (scrypt + AES), so the file alone is useless to a thief.

Passwords come from the environment variable <NAME>_KEYSTORE_PASSWORD
(e.g. GUARDIAN_KEYSTORE_PASSWORD) if set, otherwise you are asked.
"""

from __future__ import annotations

import getpass
import json
import os
from pathlib import Path

from eth_account import Account
from web3 import Web3

from . import cmd

KEYS_DIR = Path(os.getenv("KEYS_DIR", "secrets"))


def key_path(name: str) -> Path:
    return KEYS_DIR / f"{name}.json"


def _password(name: str, confirm: bool) -> str:
    env = os.getenv(f"{name.upper()}_KEYSTORE_PASSWORD")
    if env:
        return env
    while True:
        pw = getpass.getpass(f"Password for the {name} key: ")
        if len(pw) < 8:
            print("Use at least 8 characters.")
            continue
        if confirm and getpass.getpass("Repeat it: ") != pw:
            print("The passwords did not match; try again.")
            continue
        return pw


def save(name: str, private_key: str, overwrite: bool = False) -> str:
    """Encrypt and save a key. Returns its address."""
    path = key_path(name)
    if path.exists() and not overwrite:
        raise SystemExit(f"{path} already exists. Delete it first if you really want to replace it.")
    account = Account.from_key(private_key)
    data = Account.encrypt(account.key, _password(name, confirm=True))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass  # Windows bind mounts may ignore permissions
    tmp.replace(path)
    return account.address


def address_of(name_or_path: str | Path) -> str:
    """The address stored in a key file (no password needed)."""
    path = Path(name_or_path) if str(name_or_path).endswith(".json") else key_path(str(name_or_path))
    data = json.loads(path.read_text())
    return Web3.to_checksum_address("0x" + data["address"].removeprefix("0x"))


def load_key(name_or_path: str | Path, password: str | None = None) -> str:
    """Decrypt a key file and return the private key as hex."""
    path = Path(name_or_path) if str(name_or_path).endswith(".json") else key_path(str(name_or_path))
    if not path.exists():
        raise SystemExit(cmd(f"Key file {path} not found. Create it with: python -m tools.keys new <name>"))
    name = path.stem
    pw = password or _password(name, confirm=False)
    try:
        return Account.decrypt(json.loads(path.read_text()), pw).hex()
    except ValueError:
        raise SystemExit(f"Wrong password for {path}.") from None
