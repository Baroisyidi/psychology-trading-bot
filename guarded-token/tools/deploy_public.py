"""
Deploy the token to a public TEST network (Sepolia).

    python -m tools.deploy_public                 # asks before sending anything
    python -m tools.deploy_public --yes           # no questions (for scripts)

Needs:
  SEPOLIA_RPC_URL                     your Alchemy / Infura Sepolia URL
  secrets/admin.json, guardian.json   made with: python -m tools.keys new admin|guardian
  some Sepolia ETH in the admin wallet

Writes deployment/deployment.json (+ token_address, start_block) for the
monitor, and prints Etherscan links.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from vyper import compile_code
from web3 import Web3

from .chain import (GTT, call_tx, check_network, connect, eth, explorer, load_deployment,
                    save_deployment, send, token_contract)
from . import cmd
from .keystore import address_of, key_path, load_key

CONTRACT = Path(__file__).resolve().parent.parent / "contracts" / "GuardedToken.vy"


def compile_token() -> tuple[list, str]:
    out = compile_code(CONTRACT.read_text(), output_formats=["abi", "bytecode"])
    return out["abi"], out["bytecode"]


def deploy(w3, admin_key: str, guardian: str, max_supply: int, limit: int, mint: int,
           name: str = "Guarded Test Token", symbol: str = "GTT") -> dict:
    from eth_account import Account

    admin = Account.from_key(admin_key).address
    abi, bytecode = compile_token()
    factory = w3.eth.contract(abi=abi, bytecode=bytecode)
    tx = factory.constructor(name, symbol, max_supply * GTT, admin, guardian, limit * GTT).build_transaction(
        {"from": admin, "chainId": w3.eth.chain_id, "nonce": w3.eth.get_transaction_count(admin, "pending")}
    )
    for k in ("maxFeePerGas", "maxPriorityFeePerGas", "gasPrice"):
        tx.pop(k, None)
    print("Deploying the token (waiting for it to be mined, usually under a minute)...", flush=True)
    receipt = send(w3, admin_key, tx)
    token = receipt["contractAddress"]
    info = {
        "network": check_network(w3),
        "chain_id": w3.eth.chain_id,
        "token": token,
        "block": receipt["blockNumber"],
        "tx": Web3.to_hex(receipt["transactionHash"]),
        "admin": admin,
        "guardian": guardian,
    }
    if mint:
        print(f"Minting {mint:,} GTT to the admin wallet for testing...", flush=True)
        call_tx(w3, admin_key, token_contract(w3, token).functions.mint(admin, mint * GTT))
    return info


def main(argv=None):
    ap = argparse.ArgumentParser(description="Deploy GuardedToken to a public test network")
    ap.add_argument("--max-supply", type=int, default=10_000_000)
    ap.add_argument("--limit", type=int, default=10_000, help="hourly transfer limit per wallet (GTT)")
    ap.add_argument("--mint", type=int, default=100_000, help="GTT minted to the admin for testing")
    ap.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    ap.add_argument("--new", action="store_true", help="deploy again even if a deployment exists")
    args = ap.parse_args(argv)

    w3 = connect()
    network = check_network(w3)
    for name in ("admin", "guardian"):
        if not key_path(name).exists():
            raise SystemExit(cmd(f"Missing {key_path(name)}. Create it with: python -m tools.keys new {name}"))
    admin_addr, guardian_addr = address_of("admin"), address_of("guardian")
    if admin_addr == guardian_addr:
        raise SystemExit("Admin and guardian must be different wallets.")

    try:
        existing = load_deployment()
        if not args.new and existing.get("chain_id") == w3.eth.chain_id and w3.eth.get_code(existing["token"]):
            print(f"Already deployed on {network}: {existing['token']}")
            print(explorer(w3, "address", existing["token"]))
            print("Use --new to deploy a second token.")
            return
    except SystemExit:
        pass

    print(f"Network:  {network} (chain id {w3.eth.chain_id})")
    print(f"Admin:    {admin_addr}  ({eth(w3, admin_addr):.4f} ETH)")
    print(f"Guardian: {guardian_addr}  ({eth(w3, guardian_addr):.4f} ETH)")
    print(f"Token:    max supply {args.max_supply:,} GTT, hourly limit {args.limit:,} GTT, "
          f"{args.mint:,} GTT minted to admin")
    if eth(w3, admin_addr) < 0.005:
        raise SystemExit("The admin wallet needs Sepolia ETH to pay for deployment (0.01 ETH is plenty). "
                         "Use a faucet, then try again.")
    if eth(w3, guardian_addr) < 0.002:
        print(cmd("Note: the guardian has little or no ETH; it will need some to act. "
                  "Fund it later with: python -m tools.admin fund guardian 0.01"))
    if not args.yes and input("Deploy now? Type yes: ").strip().lower() != "yes":
        raise SystemExit("Cancelled; nothing was sent.")

    admin_key = load_key("admin")
    from .chain import DATA_DIR
    (DATA_DIR / "token_address").unlink(missing_ok=True)  # the monitor waits until the new one is written
    info = deploy(w3, admin_key, guardian_addr, args.max_supply, args.limit, args.mint)
    save_deployment(info)
    print(f"\nDeployed! Token address: {info['token']} (block {info['block']})")
    print(f"View it:  {explorer(w3, 'address', info['token'])}")
    print("Saved to deployment/. Next: start the monitor (see README, 'Going public').")


if __name__ == "__main__":
    main()
