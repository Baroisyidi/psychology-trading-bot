"""
A live fire drill on the public test network: pretend a wallet's key was
stolen and watch the monitor and guardian stop the thief, in real time.

    python -m tools.sepolia_drill              # asks before sending anything
    python -m tools.sepolia_drill --yes

What it does:
  1. makes a throwaway "victim" wallet (saved in secrets/drill_victim.key so
     no test ETH is lost if something goes wrong) and gives it a little
     Sepolia ETH and some GTT from the admin wallet
  2. the "thief" then pays the GTT out to brand-new wallets, one transfer
     at a time, the way a real drain or fan-out looks
  3. it stops as soon as the guardian blocklists the victim or pauses the
     token, and reports how much got out and how long it took
  4. leftover ETH goes back to the admin wallet

The monitor (with GUARDIAN_MODE=on) must be running against the same token,
and the guardian wallet needs a little Sepolia ETH. Uses only test ETH:
about 0.003 ETH per drill.
"""

from __future__ import annotations

import argparse
import os
import random
import secrets
import time

from eth_account import Account

from .chain import GTT, call_tx, check_network, connect, eth, explorer, load_deployment, send, token_contract
from . import cmd
from .keystore import KEYS_DIR, address_of, load_key

VICTIM_FILE = KEYS_DIR / "drill_victim.key"


def victim_key() -> str:
    """Reuse the throwaway victim if a previous drill left one with ETH; otherwise make one."""
    if VICTIM_FILE.exists():
        return VICTIM_FILE.read_text().strip()
    key = "0x" + secrets.token_hex(32)
    KEYS_DIR.mkdir(parents=True, exist_ok=True)
    VICTIM_FILE.write_text(key)
    try:
        os.chmod(VICTIM_FILE, 0o600)
    except OSError:
        pass
    return key


def stopped(token, victim: str) -> str | None:
    f = token.functions
    if f.paused().call():
        return "token paused"
    if f.blocklisted(victim).call():
        return "victim wallet blocklisted"
    return None


def sweep_eth(w3, key: str, to: str) -> float:
    """Send the victim's leftover ETH back (balance minus the fee)."""
    me = Account.from_key(key).address
    bal = w3.eth.get_balance(me)
    base = w3.eth.get_block("latest").get("baseFeePerGas")
    price = (base * 2 + w3.to_wei(1, "gwei")) if base is not None else w3.eth.gas_price
    fee = 21_000 * price
    if bal <= fee * 2:
        return 0.0
    tx = {"to": to, "value": bal - fee, "gas": 21_000}
    if base is not None:
        tx.update(maxFeePerGas=price, maxPriorityFeePerGas=min(w3.to_wei(1, "gwei"), price))
    else:
        tx["gasPrice"] = price
    send(w3, key, tx)
    return (bal - fee) / 10**18


def main(argv=None):
    ap = argparse.ArgumentParser(description="Live attack drill on Sepolia")
    ap.add_argument("--gtt", type=float, default=4_000, help="GTT given to the victim (keep under the hourly limit)")
    ap.add_argument("--payments", type=int, default=10, help="how many payments the thief tries")
    ap.add_argument("--eth", type=float, default=0.003, help="ETH given to the victim for fees")
    ap.add_argument("--wait", type=int, default=30,
                    help="seconds between funding and the attack (lets the monitor see the funding)")
    ap.add_argument("--yes", action="store_true")
    args = ap.parse_args(argv)

    w3 = connect()
    network = check_network(w3)
    dep = load_deployment()
    token = token_contract(w3, dep["token"])
    f = token.functions
    admin = address_of("admin")
    guardian = f.guardian().call()

    if f.paused().call():
        raise SystemExit(cmd("The token is paused. Unpause it first: python -m tools.admin unpause"))
    if eth(w3, guardian) < 0.001:
        raise SystemExit(cmd("The guardian has almost no ETH, so it could not act. Fund it first: "
                             "python -m tools.admin fund guardian 0.01"))
    limit = f.transfer_limit().call() / GTT
    if args.gtt > limit:
        raise SystemExit(f"--gtt {args.gtt:,.0f} is above the hourly limit ({limit:,.0f}); the contract "
                         "itself would stop most of it. Use a smaller amount to test the guardian.")

    vkey = victim_key()
    victim = Account.from_key(vkey).address
    if f.blocklisted(victim).call():  # left blocked by an earlier drill: start a fresh victim
        sweep_eth(w3, vkey, admin)
        VICTIM_FILE.unlink()
        vkey = victim_key()
        victim = Account.from_key(vkey).address

    print(f"Network: {network}. Token {dep['token']}")
    print(f"Victim (throwaway): {victim}")
    print(f"Plan: give it {args.eth} ETH + {args.gtt:,.0f} GTT, wait {args.wait}s, then the 'thief' "
          f"tries {args.payments} payments to brand-new wallets.")
    print("Make sure the monitor is running with GUARDIAN_MODE=on and watch Telegram.")
    if not args.yes and input("Start the drill? Type yes: ").strip().lower() != "yes":
        raise SystemExit("Cancelled; nothing was sent.")

    akey = load_key("admin")
    if eth(w3, victim) < args.eth / 2:
        send(w3, akey, {"to": victim, "value": w3.to_wei(args.eth, "ether")})
    have = f.balanceOf(victim).call() / GTT
    if have < args.gtt:
        call_tx(w3, akey, f.transfer(victim, int((args.gtt - have) * GTT)))
    print(f"Victim funded: {eth(w3, victim):.4f} ETH, {f.balanceOf(victim).call() / GTT:,.0f} GTT")
    time.sleep(args.wait)

    rng = random.Random()
    start = time.time()
    sent = escaped = 0.0
    reason = None
    print("\nAttack starting...")
    for i in range(1, args.payments + 1):
        reason = stopped(token, victim)
        if reason:
            break
        bal = f.balanceOf(victim).call() / GTT
        amount = round(min(bal, args.gtt / args.payments * rng.uniform(0.85, 1.15)), 2)
        if amount <= 0:
            break
        mule = Account.create().address  # a brand-new wallet nobody has the key to: test GTT is burned
        try:
            r = call_tx(w3, vkey, f.transfer(mule, int(amount * GTT)))
            escaped += amount
            print(f"  payment {i}: {amount:,.2f} GTT -> {mule[:10]}...  (block {r['blockNumber']})", flush=True)
        except BaseException as e:  # reverted (blocked/paused) or failed gas estimate
            if isinstance(e, KeyboardInterrupt):
                raise
            reason = stopped(token, victim) or f"transfer refused ({str(e)[:80]})"
            break
        sent += 1
    else:
        reason = stopped(token, victim)
    secs = time.time() - start

    print()
    if reason:
        print(f"STOPPED after {int(sent)} payment(s), {secs:.0f}s: {reason}.")
    else:
        print(f"NOT STOPPED: all {int(sent)} payments went through in {secs:.0f}s. Check the monitor logs "
              "and that GUARDIAN_MODE=on.")
    print(f"GTT that got out: {escaped:,.2f} of {args.gtt:,.0f}; still in the victim wallet: "
          f"{f.balanceOf(victim).call() / GTT:,.2f}")
    print(f"Victim on Etherscan: {explorer(w3, 'address', victim)}")
    back = sweep_eth(w3, vkey, admin)
    if back:
        print(f"Returned {back:.5f} ETH to the admin wallet.")
    if f.blocklisted(victim).call():
        VICTIM_FILE.unlink(missing_ok=True)  # done with it; next drill uses a fresh victim
    if f.paused().call():
        print(cmd("The token is paused. When you are done: python -m tools.admin unpause"))


if __name__ == "__main__":
    main()
