"""
Day-to-day admin tasks for the public deployment.

    python -m tools.admin status                    # roles, balances, pause, limit, timelocks
    python -m tools.admin fund guardian 0.01        # send Sepolia ETH from admin to a wallet
    python -m tools.admin mint 0xABC... 1000        # mint GTT (admin only)
    python -m tools.admin send 0xABC... 250         # transfer GTT from the admin wallet
    python -m tools.admin check 0xABC...            # is this address blocklisted? balance?
    python -m tools.admin unblock 0xABC...          # undo a guardian block (admin only)
    python -m tools.admin unpause                   # undo a guardian pause (admin only)
    python -m tools.admin propose-limit 10000       # start the 48h timelock to restore the limit
    python -m tools.admin execute-limit             # finish it after 48h
    python -m tools.admin revoke-guardian           # emergency: remove a compromised guardian
    python -m tools.admin cancel                    # cancel all pending timelocked changes

"guardian" or "admin" can be used in place of an address.
"""

from __future__ import annotations

import sys
import time

from web3 import Web3

from .chain import GTT, call_tx, connect, check_network, eth, explorer, load_deployment, revert_reason, send, token_contract
from . import cmd as show
from .keystore import address_of, load_key

ZERO = "0x0000000000000000000000000000000000000000"


def resolve(who: str) -> str:
    if who.lower() in ("admin", "guardian") or who.lower().endswith(".json"):
        return address_of(who.lower())
    if not Web3.is_address(who):
        raise SystemExit(f"'{who}' is not an address (expected 0x... or admin/guardian).")
    return Web3.to_checksum_address(who)


def _eta(ts: int) -> str:
    if not ts:
        return "-"
    left = ts - int(time.time())
    return "ready now" if left <= 0 else f"ready in {left // 3600}h {left % 3600 // 60}m"


def _wait_text(seconds: int) -> str:
    return f"{seconds // 3600}h {seconds % 3600 // 60}m"


def precheck(w3, f, cmd: str, rest: list[str]) -> None:
    """Refuse early, with a clear reason, when the contract would refuse anyway."""
    now = w3.eth.get_block("latest")["timestamp"]  # chain time, which the timelock uses
    if cmd == "execute-limit":
        eta = f.pending_limit_eta().call()
        if not eta:
            raise SystemExit(show("Nothing to execute: no limit change is pending. "
                                  "Start one with: python -m tools.admin propose-limit 10000"))
        if eta > now:
            raise SystemExit(f"Not yet: the 48-hour timelock ends in {_wait_text(eta - now)} "
                             f"(change to {f.pending_limit().call() / GTT:,.0f} GTT per hour). Try again then.")
    elif cmd == "propose-limit" and f.pending_limit_eta().call():
        eta = f.pending_limit_eta().call()
        print(f"Note: a change to {f.pending_limit().call() / GTT:,.0f} GTT is already pending "
              f"({'ready now' if eta <= now else 'ready in ' + _wait_text(eta - now)}). "
              "Proposing again replaces it and restarts the 48-hour wait.")
        if input("Replace it? Type yes: ").strip().lower() != "yes":
            raise SystemExit("Kept the pending change; nothing was sent.")
    elif cmd == "unpause" and not f.paused().call():
        raise SystemExit("The token is not paused; nothing to do.")
    elif cmd == "unblock" and rest and not f.blocklisted(resolve(rest[0])).call():
        raise SystemExit(f"{resolve(rest[0])} is not blocklisted; nothing to do.")


def status(w3, token) -> None:
    f = token.functions
    dep = load_deployment()
    admin, guardian = f.admin().call(), f.guardian().call()
    print(f"Network:  {check_network(w3)}   latest block {w3.eth.block_number}")
    print(f"Token:    {f.name().call()} ({f.symbol().call()})  {token.address}")
    print(f"          {explorer(w3, 'address', token.address)}")
    print(f"Supply:   {f.totalSupply().call() / GTT:,.2f} of max {f.MAX_SUPPLY().call() / GTT:,.0f} GTT")
    print(f"Paused:   {'YES' if f.paused().call() else 'no'}")
    print(f"Limit:    {f.transfer_limit().call() / GTT:,.2f} GTT per wallet per hour")
    print(f"Admin:    {admin}  {eth(w3, admin):.4f} ETH  {f.balanceOf(admin).call() / GTT:,.2f} GTT")
    g = "(revoked)" if guardian == ZERO else f"{guardian}  {eth(w3, guardian):.4f} ETH"
    print(f"Guardian: {g}")
    if eth(w3, guardian) < 0.002 and guardian != ZERO:
        print(show("          !! guardian is low on ETH and may not be able to act: python -m tools.admin fund guardian 0.01"))
    pending = []
    if f.pending_limit_eta().call():
        pending.append(f"limit -> {f.pending_limit().call() / GTT:,.2f} GTT ({_eta(f.pending_limit_eta().call())})")
    if f.pending_guardian_eta().call():
        pending.append(f"guardian -> {f.pending_guardian().call()} ({_eta(f.pending_guardian_eta().call())})")
    if f.pending_admin_eta().call():
        pending.append(f"admin -> {f.pending_admin().call()} ({_eta(f.pending_admin_eta().call())})")
    print("Pending:  " + ("; ".join(pending) if pending else "nothing"))
    print(f"Deployed: block {dep['block']}")


def main(argv=None):
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        print(show(__doc__))
        raise SystemExit(2)
    cmd, rest = args[0], args[1:]
    w3 = connect()
    check_network(w3)
    token = token_contract(w3, load_deployment()["token"])
    f = token.functions

    def need(n, usage):
        if len(rest) < n:
            raise SystemExit(show(f"Usage: python -m tools.admin {cmd} {usage}"))

    if cmd == "status":
        return status(w3, token)
    if cmd == "check":
        need(1, "<address>")
        a = resolve(rest[0])
        print(f"{a}: blocklisted={f.blocklisted(a).call()}  balance={f.balanceOf(a).call() / GTT:,.2f} GTT  "
              f"{eth(w3, a):.5f} ETH")
        return

    # Checks that need no password: say "not yet" before asking for one.
    precheck(w3, f, cmd, rest)

    # Everything below sends a transaction from the admin wallet.
    key = load_key("admin")
    if cmd == "fund":
        need(2, "<address|guardian> <ETH amount>")
        to, amount = resolve(rest[0]), float(rest[1])
        if amount > 0.5:
            raise SystemExit("That is a lot of test ETH for one wallet; use 0.5 or less.")
        r = send(w3, key, {"to": to, "value": w3.to_wei(amount, "ether")})
        print(f"Sent {amount} ETH to {to}: {explorer(w3, 'tx', Web3.to_hex(r['transactionHash']))}")
        return
    if cmd == "mint":
        need(2, "<address> <GTT amount>")
        fn = f.mint(resolve(rest[0]), int(float(rest[1]) * GTT))
    elif cmd == "send":
        need(2, "<address> <GTT amount>")
        fn = f.transfer(resolve(rest[0]), int(float(rest[1]) * GTT))
    elif cmd == "unblock":
        need(1, "<address>")
        fn = f.unblock_address(resolve(rest[0]))
    elif cmd == "unpause":
        fn = f.unpause()
    elif cmd == "propose-limit":
        need(1, "<GTT per hour>")
        fn = f.propose_limit(int(float(rest[0]) * GTT))
    elif cmd == "execute-limit":
        fn = f.execute_limit()
    elif cmd == "revoke-guardian":
        fn = f.revoke_guardian()
    elif cmd == "cancel":
        fn = f.cancel_pending()
    else:
        print(show(__doc__))
        raise SystemExit(f"Unknown command: {cmd}")
    try:
        r = call_tx(w3, key, fn)
    except Exception as e:  # e.g. a revert reason from the contract
        raise SystemExit(f"{cmd} failed: {revert_reason(e)}") from None
    print(f"{cmd}: done in block {r['blockNumber']}  {explorer(w3, 'tx', Web3.to_hex(r['transactionHash']))}")


if __name__ == "__main__":
    main()
