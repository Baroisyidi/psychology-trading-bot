"""
Phase 4: the guardian acts on incidents automatically.

Escalation ladder, per incident:
  1. BLOCKLIST  incident reaches ALERT (70+) with corroboration (two alerts, or one
                at 90+): blocklist the wallets involved, so they can no longer send.
  2. TIGHTEN    incident reaches CRITICAL (90+): lower the hourly transfer limit for
                everyone (default: to 10% of the starting limit).
  3. PAUSE      the attack keeps going after steps 1-2 (new flagged transfers from
                wallets not yet blocklisted), or the blocklist budget runs out:
                pause the whole token.

Every step is protective and reversible, matching what the contract lets the
guardian do. Only the admin can undo it: unblock_address(), unpause(), and
propose_limit() + a 48-hour timelock to raise the limit again.

Safety limits, so a bug or false alarm cannot run wild:
  - protected wallets (admin, guardian, anything in PROTECTED_WALLETS) are never touched
  - at most MAX_BLOCKS_PER_HOUR blocklistings per hour, and MAX_WALLETS_PER_INCIDENT
  - GUARDIAN_MODE=dry-run reports what it WOULD do without sending anything
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field

from eth_account import Account
from web3 import Web3

from .alerts import fmt_tokens, short
from .chain import load_abi
from .incidents import Incident

TEST_MNEMONIC = "test test test test test test test test test test test junk"
LOCAL_CHAIN_IDS = {1337, 31337}


@dataclass
class Policy:
    block_at: int = 70  # incident peak needed to blocklist ...
    corroborate: int = 2  # ... with at least this many alerts at that level ...
    act_alone: int = 90  # ... unless one alert is this strong
    tighten_at: int = 90
    tighten_to: float = 0.10  # new limit as a share of the starting limit
    pause_after: int = 3  # further flagged transfers after tightening
    max_blocks_per_hour: int = 30
    max_wallets_per_incident: int = 25


def local_test_key(index: int = 1) -> str:
    """The guardian key of the public test mnemonic. Local chains ONLY."""
    Account.enable_unaudited_hdwallet_features()
    return Account.from_mnemonic(TEST_MNEMONIC, account_path=f"m/44'/60'/0'/0/{index}").key.hex()


class Executor:
    """Signs and sends the guardian's transactions."""

    def __init__(self, w3: Web3, token_address: str, private_key: str):
        self.w3 = w3
        self.account = Account.from_key(private_key)
        self.token = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=load_abi())
        self.pending: list[str] = []
        self._nonce: int | None = None

    @property
    def address(self) -> str:
        return self.account.address

    def _send(self, fn) -> str:
        """Sign and broadcast. Receipts are collected later by wait_all(), so several
        actions go out in the same block instead of one per block."""
        if self._nonce is None:
            self._nonce = self.w3.eth.get_transaction_count(self.address, "pending")
        tx = fn.build_transaction({"from": self.address, "nonce": self._nonce, "chainId": self.w3.eth.chain_id})
        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
        self._nonce += 1
        h = tx_hash.hex()
        h = h if h.startswith("0x") else "0x" + h
        self.pending.append(h)
        return h

    def wait_all(self, timeout: int = 180) -> None:
        """Wait until every sent transaction is mined; raise if any reverted."""
        pending, self.pending, self._nonce = self.pending, [], None
        failed = []
        for h in pending:
            receipt = self.w3.eth.wait_for_transaction_receipt(h, timeout=timeout)
            if receipt["status"] != 1:
                failed.append(h)
        if failed:
            raise RuntimeError(f"{len(failed)} guardian transaction(s) reverted: {', '.join(failed)}")

    # reads
    def is_blocked(self, addr) -> bool:
        return self.token.functions.blocklisted(Web3.to_checksum_address(addr)).call()

    def paused(self) -> bool:
        return self.token.functions.paused().call()

    def limit(self) -> int:
        return self.token.functions.transfer_limit().call()

    def roles(self) -> tuple[str, str]:
        f = self.token.functions
        return f.admin().call(), f.guardian().call()

    # writes
    def block(self, addr) -> str:
        return self._send(self.token.functions.block_address(Web3.to_checksum_address(addr)))

    def tighten(self, new_limit: int) -> str:
        return self._send(self.token.functions.tighten_limit(new_limit))

    def pause(self) -> str:
        return self._send(self.token.functions.pause())


@dataclass
class IncidentState:
    blocked: set = field(default_factory=set)
    tightened: bool = False
    alerts_at_tighten: int = 0
    paused: bool = False
    budget_exhausted: bool = False


class Responder:
    def __init__(self, executor: Executor, notifier, mode: str = "dry-run",
                 protected: set[str] | None = None, policy: Policy | None = None):
        if mode not in ("on", "dry-run"):
            raise ValueError("mode must be 'on' or 'dry-run'")
        self.ex = executor
        self.notifier = notifier
        self.mode = mode
        self.policy = policy or Policy()
        admin, guardian = executor.roles()
        if guardian.lower() != executor.address.lower():
            raise ValueError(f"key is for {executor.address}, but the token's guardian is {guardian}")
        self.protected = {a.lower() for a in (protected or set())} | {admin.lower(), guardian.lower()}
        self.start_limit = executor.limit()
        self.state: dict[int, IncidentState] = {}
        self.block_times: deque = deque()
        self.own_txs: set[str] = set()
        self.actions_taken: list[dict] = []
        notifier.quiet_txs = self.own_txs  # our own actions are reported once, not re-alerted

    # ------------------------------------------------------------ helpers

    def _do(self, what: str, fn, *args):
        if self.mode == "dry-run":
            return "dry-run"
        tx = fn(*args)
        self.own_txs.add(tx.lower())
        return tx

    def _blocks_left(self, now: float) -> int:
        while self.block_times and self.block_times[0] <= now - 3600:
            self.block_times.popleft()
        return self.policy.max_blocks_per_hour - len(self.block_times)

    def _strong(self, inc: Incident) -> bool:
        p = self.policy
        if inc.peak < p.block_at:
            return False
        strong_alerts = sum(1 for a in inc.alerts if a.score >= p.block_at)
        return inc.peak >= p.act_alone or strong_alerts >= p.corroborate

    # ------------------------------------------------------------ main entry

    def review(self, incidents: list[Incident]) -> None:
        """Called after every poll with the open incidents."""
        for inc in incidents:
            try:
                self._review_one(inc)
                if self.mode == "on":
                    self.ex.wait_all()
            except Exception as err:  # never let a failed action stop the monitor
                self.ex.pending, self.ex._nonce = [], None
                self.notifier.send_status(
                    f"[ACTION FAILED] Incident #{inc.number}: {type(err).__name__}: {err}\n"
                    "The guardian could not act. Investigate manually."
                )

    def _review_one(self, inc: Incident) -> None:
        if not self._strong(inc):
            return
        p = self.policy
        st = self.state.setdefault(inc.number, IncidentState())
        lines: list[str] = []
        now = time.time()

        # 1. Blocklist the wallets involved.
        if not st.paused:
            candidates = [
                w for w in sorted(inc.wallets)
                if w.lower() not in self.protected and w not in st.blocked
            ]
            room = min(self._blocks_left(now), p.max_wallets_per_incident - len(st.blocked))
            done = []
            for w in candidates:
                if room <= 0:
                    st.budget_exhausted = True
                    break
                if self.mode == "on" and self.ex.is_blocked(w):
                    st.blocked.add(w)
                    continue
                self._do("block", self.ex.block, w)
                st.blocked.add(w)
                self.block_times.append(now)
                done.append(w)
                room -= 1
            if done:
                lines.append(f"blocklisted {len(done)} wallet(s): " + ", ".join(short(w) for w in done))
                self.actions_taken.append({"incident": inc.number, "action": "block", "wallets": done})
            if st.budget_exhausted:
                lines.append("blocklist budget used up for now; escalating")

        # 2. Tighten the hourly limit for everyone.
        if inc.peak >= p.tighten_at and not st.tightened:
            target = int(self.start_limit * p.tighten_to)
            current = self.ex.limit() if self.mode == "on" else self.start_limit
            if target > 0 and (current == 0 or target < current):
                self._do("tighten", self.ex.tighten, target)
                lines.append(f"tightened hourly limit: {fmt_tokens(current)} -> {fmt_tokens(target)}")
                self.actions_taken.append({"incident": inc.number, "action": "tighten", "limit": target})
            st.tightened = True
            st.alerts_at_tighten = len(inc.alerts)

        # 3. Pause if the attack keeps going.
        if st.tightened and not st.paused:
            # Blocklisted wallets cannot send, so any flagged transfer after the
            # first response means the attack is still finding new wallets.
            later = inc.alerts[st.alerts_at_tighten:]
            still_going = sum(1 for a in later if a.score >= p.block_at)
            if still_going >= p.pause_after or (st.budget_exhausted and inc.peak >= p.tighten_at):
                if self.mode == "dry-run" or not self.ex.paused():
                    self._do("pause", self.ex.pause)
                    lines.append("PAUSED the token: the attack continued after blocklisting")
                    self.actions_taken.append({"incident": inc.number, "action": "pause"})
                st.paused = True

        if lines:
            head = f"[ACTION] Incident #{inc.number}: guardian responded"
            if self.mode == "dry-run":
                head += " (DRY RUN - nothing was sent on-chain)"
            undo = "Undo (admin only): unblock_address(...), unpause(), propose_limit(...) + 48h timelock"
            self.notifier.send_status("\n".join([head] + [f"- {l}" for l in lines] + [undo]))
