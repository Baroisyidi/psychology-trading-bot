"""Shared helpers for the public-network tools."""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

from eth_account import Account
from web3 import Web3

from monitor.chain import is_connection_error, is_rate_limited, load_abi
from monitor.redact import redact

from . import cmd

DATA_DIR = Path(os.getenv("DATA_DIR", "deployment"))
NETWORKS = {11155111: "Sepolia", 17000: "Holesky", 560048: "Hoodi", 1337: "local", 31337: "local"}
EXPLORERS = {11155111: "https://sepolia.etherscan.io"}
MAINNETS = {1, 10, 56, 137, 8453, 42161, 43114}  # never deploy this prototype here
GTT = 10**18


def connect(rpc_url: str | None = None) -> Web3:
    url = rpc_url or os.getenv("RPC_URL") or os.getenv("SEPOLIA_RPC_URL")
    if not url:
        raise SystemExit("Set SEPOLIA_RPC_URL in .env.sepolia (your Alchemy or Infura Sepolia URL).")
    w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 30}))
    last = None
    for attempt in range(3):  # ride out short network / DNS hiccups
        try:
            w3.eth.chain_id
            return w3
        except Exception as err:
            last = err
            if attempt < 2:
                time.sleep(5)
    text = redact(f"{type(last).__name__}: {last}")
    if is_connection_error(last):
        hint = ("This computer (or Docker) could not reach the provider: check your internet. A "
                "'name resolution' error is a DNS problem; see README, 'If something goes wrong'.")
    elif "401" in text or "authenticat" in text.lower():
        hint = "The provider rejected the key: copy the Sepolia URL from your Alchemy dashboard into .env.sepolia again."
    elif is_rate_limited(last):
        hint = "The provider is rate-limiting you: wait a minute and try again."
    else:
        hint = "Check SEPOLIA_RPC_URL in .env.sepolia."
    raise SystemExit(f"Cannot reach the RPC provider ({text[:200]}).\n{hint}")


def check_network(w3: Web3) -> str:
    cid = w3.eth.chain_id
    if cid in MAINNETS:
        raise SystemExit(f"Chain id {cid} is a MAIN network with real money. This prototype is "
                         "not audited and must only be used on test networks.")
    return NETWORKS.get(cid, f"chain {cid}")


def explorer(w3: Web3, kind: str, value: str) -> str:
    base = EXPLORERS.get(w3.eth.chain_id)
    return f"{base}/{kind}/{value}" if base else value


def save_deployment(info: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "deployment.json").write_text(json.dumps(info, indent=1))
    (DATA_DIR / "token_address").write_text(info["token"])
    (DATA_DIR / "start_block").write_text(str(info["block"]))


def load_deployment() -> dict:
    path = DATA_DIR / "deployment.json"
    if not path.exists():
        raise SystemExit(cmd(f"No deployment found in {DATA_DIR}/. Deploy first: python -m tools.deploy_public"))
    return json.loads(path.read_text())


def token_contract(w3: Web3, address: str):
    return w3.eth.contract(address=Web3.to_checksum_address(address), abi=load_abi())


def send(w3: Web3, private_key: str, tx: dict, timeout: int = 300) -> dict:
    """Fill in nonce/chain/fees, sign, send and wait. Returns the receipt."""
    acct = Account.from_key(private_key)
    tx = dict(tx)
    tx.setdefault("from", acct.address)
    tx.setdefault("chainId", w3.eth.chain_id)
    tx.setdefault("nonce", w3.eth.get_transaction_count(acct.address, "pending"))
    if "gas" not in tx:
        tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.2)
    if "maxFeePerGas" not in tx and "gasPrice" not in tx:
        base = w3.eth.get_block("latest").get("baseFeePerGas")
        if base is not None:
            tip = w3.to_wei(1, "gwei")
            tx["maxPriorityFeePerGas"] = tip
            tx["maxFeePerGas"] = base * 2 + tip
        else:
            tx["gasPrice"] = w3.eth.gas_price
    signed = acct.sign_transaction(tx)
    h = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(h, timeout=timeout)
    if receipt["status"] != 1:
        raise SystemExit(f"Transaction failed: {Web3.to_hex(h)}")
    return receipt


def call_tx(w3: Web3, private_key: str, fn, timeout: int = 300) -> dict:
    """Send a contract function call as a transaction."""
    acct = Account.from_key(private_key)
    tx = fn.build_transaction({"from": acct.address, "chainId": w3.eth.chain_id,
                               "nonce": w3.eth.get_transaction_count(acct.address, "pending")})
    for k in ("maxFeePerGas", "maxPriorityFeePerGas", "gasPrice"):
        tx.pop(k, None)  # let send() choose fees consistently
    return send(w3, private_key, tx, timeout)


def revert_reason(e: BaseException) -> str:
    """The contract's own words ('timelock active') instead of a wall of RPC detail."""
    for a in getattr(e, "args", ()):
        if isinstance(a, dict) and a.get("reason"):
            return a["reason"]
    m = re.search(r"revert(?:ed)?:?\s*(?:VM Exception while processing transaction: revert )?(.+?)['\"]?$",
                  str(getattr(e, "message", None) or (e.args[0] if e.args else e)))
    return redact(m.group(1).strip() if m else str(e)[:200])


def eth(w3: Web3, address: str) -> float:
    return w3.eth.get_balance(Web3.to_checksum_address(address)) / 10**18
