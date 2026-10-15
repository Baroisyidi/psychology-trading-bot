"""
The chain monitor: polls for new blocks, decodes the token's events, runs
them through the detector and hands alerts to the notifier.

Works with any web3 provider: Ape's in-process local chain, a local node
such as Ganache at http://127.0.0.1:8545, or a public RPC (Sepolia).

Public-network care:
  - eth_getLogs block ranges shrink automatically if the provider refuses them
  - few calls per poll: one eth_blockNumber, plus one eth_getLogs only when a
    block arrived; the pause state comes from events, not a call per block
  - RPC errors back off instead of hammering the provider
  - an optional signed heartbeat is pushed to a remote watchdog (e.g. on Render)
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import requests

from eth_utils import event_abi_to_log_topic
from web3 import Web3

from .alerts import Alert
from .notify import Notifier
from .redact import format_exc, redact
from .rules import Context, Detector, Meta

ABI_PATH = Path(__file__).with_name("GuardedToken.abi.json")
MAX_BLOCK_RANGE = int(os.getenv("LOG_BLOCK_RANGE", "500"))  # shrinks automatically if refused


CHUNK_PAUSE = float(os.getenv("LOG_CHUNK_PAUSE", "0.25"))  # between log queries when catching up


def is_rate_limited(err: Exception) -> bool:
    text = f"{type(err).__name__} {err}".lower()
    return any(s in text for s in ("429", "too many requests", "rate limit", "per second", "capacity"))


def is_connection_error(err: Exception) -> bool:
    if isinstance(err, (requests.ConnectionError, requests.Timeout)):
        return True
    text = f"{type(err).__name__} {err}".lower()
    return any(s in text for s in ("name resolution", "failed to resolve", "connection refused",
                                    "connection aborted", "timed out", "max retries exceeded"))


def load_abi() -> list:
    return json.loads(ABI_PATH.read_text())


class ChainMonitor:
    def __init__(
        self,
        w3: Web3,
        token_address: str,
        notifier: Notifier,
        detector: Detector | None = None,
        start_block: int | None = None,
        heartbeat_path: str | Path | None = "monitor_heartbeat.json",
        heartbeat_every: int = 600,
        responder=None,
        threat_path: str | Path | None = None,
        remote_watchdog_url: str | None = None,
        push_every: int = 30,
    ):
        self.responder = responder
        self.threat_path = threat_path
        self.remote_watchdog_url = remote_watchdog_url
        self.push_every = push_every
        self._last_push = 0.0
        self._push_failures = 0
        self.block_range = MAX_BLOCK_RANGE
        self._paused: bool | None = None
        self.w3 = w3
        abi = load_abi()
        self.token = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=abi)
        self.notifier = notifier
        self.detector = detector or Detector()
        self.heartbeat_path = Path(heartbeat_path) if heartbeat_path else None
        self.heartbeat_every = heartbeat_every
        self._last_heartbeat_msg = time.time()
        self._alerts_since_heartbeat = 0

        # topic hash -> event name, to decode logs from a single get_logs call
        self._topics = {
            "0x" + event_abi_to_log_topic(e).hex(): e["name"]
            for e in abi
            if e["type"] == "event"
        }
        latest = w3.eth.block_number
        self.last_block = latest if start_block is None else start_block - 1

    # ------------------------------------------------------------ reading the chain

    def _context(self) -> Context:
        t = self.token.functions
        return Context(
            transfer_limit=t.transfer_limit().call(),
            admin=t.admin().call(),
            guardian=t.guardian().call(),
            total_supply=t.totalSupply().call(),
            max_supply=t.MAX_SUPPLY().call(),
        )

    def _decode(self, log) -> tuple[str, dict] | None:
        topic0 = log["topics"][0]
        topic0 = topic0.hex() if hasattr(topic0, "hex") else str(topic0)
        if not topic0.startswith("0x"):
            topic0 = "0x" + topic0
        name = self._topics.get(topic0)
        if not name:
            return None
        decoded = getattr(self.token.events, name)().process_log(log)
        return name, dict(decoded["args"])

    def _get_logs(self, start: int, end: int) -> tuple[list, int]:
        """Fetch logs, shrinking the block range if the provider refuses it.
        Returns (logs, last block actually covered)."""
        waits = 0
        while True:
            try:
                logs = self.w3.eth.get_logs(
                    {"address": self.token.address, "fromBlock": start, "toBlock": end}
                )
                return logs, end
            except Exception as err:
                if is_rate_limited(err) and waits < 6:  # "slow down", not "range too big": wait, same range
                    waits += 1
                    time.sleep(min(30, 2 ** waits))
                    continue
                if end <= start or is_connection_error(err):  # network trouble: not the range's fault
                    raise
                self.block_range = max(1, (end - start + 1) // 2)
                print(f"Provider refused a {end - start + 1}-block log query ({type(err).__name__}); "
                      f"using {self.block_range} blocks per query.")
                end = start + self.block_range - 1

    def warm_up(self, from_block: int = 0) -> int:
        """
        Read the token's history from `from_block` WITHOUT alerting or acting, so
        the detector knows which wallets are established before going live.
        Without this, a freshly (re)started monitor sees every wallet as brand
        new, and the guardian could blocklist innocent users.
        Returns the number of events read.
        """
        self.last_block = from_block - 1
        self._quiet = True
        try:
            seen = len(self.poll_once())
        finally:
            self._quiet = False
        return seen

    def poll_once(self) -> list[Alert]:
        """Process any blocks mined since the last poll. Returns all alerts."""
        quiet = getattr(self, "_quiet", False)
        latest = self.w3.eth.block_number
        produced: list[Alert] = []
        if latest < self.last_block:
            # The local chain was restarted from scratch: start over from block 0.
            print(f"Chain reset detected (block {self.last_block} -> {latest}); rescanning.")
            self.last_block = -1
        while self.last_block < latest:
            start = self.last_block + 1
            logs, end = self._get_logs(start, min(latest, start + self.block_range - 1))
            ctx = self._context() if logs else None
            timestamps: dict[int, int] = {}
            for log in sorted(logs, key=lambda l: (l["blockNumber"], l["logIndex"])):
                decoded = self._decode(log)
                if not decoded:
                    continue
                name, args = decoded
                if name in ("Paused", "Unpaused"):  # track it from events: no extra RPC call per block
                    self._paused = name == "Paused"
                n = log["blockNumber"]
                if n not in timestamps:
                    timestamps[n] = self.w3.eth.get_block(n)["timestamp"]
                tx = log["transactionHash"]
                tx_hex = tx.hex() if hasattr(tx, "hex") else str(tx)
                if not tx_hex.startswith("0x"):
                    tx_hex = "0x" + tx_hex
                meta = Meta(block=n, timestamp=timestamps[n], tx_hash=tx_hex)
                for alert in self.detector.observe(name, args, meta, ctx):
                    produced.append(alert)
                    if not quiet and self.notifier.handle(alert):
                        self._alerts_since_heartbeat += 1
            self.last_block = end
            if self.last_block < latest and CHUNK_PAUSE:
                time.sleep(CHUNK_PAUSE)  # catching up in many pieces: stay under free-plan rate limits
        if quiet:
            return produced
        if self.notifier.open_incidents:
            self.notifier.tick(self.w3.eth.get_block(latest)["timestamp"])
        if self.responder is not None and self.notifier.tracker is not None:
            self.responder.review(self.notifier.tracker.open)
        if self.threat_path:
            from defense.gate import write_threat

            incidents = self.notifier.tracker.open if self.notifier.tracker else []
            peak = max((i.peak for i in incidents), default=0)
            if self._paused is None:  # asked once; afterwards Paused/Unpaused events keep it current
                self._paused = self.token.functions.paused().call()
            write_threat(self.threat_path, len(incidents), peak, self._paused)
        self._write_heartbeat(latest)
        return produced

    # ------------------------------------------------------------ heartbeat

    def _write_heartbeat(self, block: int) -> None:
        if self.heartbeat_path:
            tmp = self.heartbeat_path.with_suffix(".tmp")
            tmp.write_text(json.dumps({"time": time.time(), "block": block}))
            tmp.replace(self.heartbeat_path)  # atomic, so the watchdog never reads half a file

    def _maybe_push_heartbeat(self) -> None:
        """Send a signed 'I am alive' to a remote watchdog, so someone notices if
        this whole machine goes down (the local watchdog would go down with it)."""
        if not self.remote_watchdog_url or time.time() - self._last_push < self.push_every:
            return
        self._last_push = time.time()
        body = json.dumps({"time": int(time.time()), "block": self.last_block,
                           "token": self.token.address}, sort_keys=True)
        try:
            r = requests.post(self.remote_watchdog_url, timeout=10,
                              json={"body": body, "sig": self.notifier.signer.sign_hex(body.encode())})
            problem = None if r.ok else f"refused the heartbeat ({r.status_code} {r.text[:80]})"
            if r.status_code == 403:
                problem += (f". Its MONITOR_PUBLIC_KEY must be this monitor's key: "
                            f"{self.notifier.signer.public_key_hex}")
        except requests.RequestException as err:
            problem = f"unreachable ({type(err).__name__})"
        if problem and (self._push_failures == 0 or self._push_failures % 20 == 0):
            print(f"Remote watchdog {problem}; will keep trying.", flush=True)
        elif not problem and self._push_failures:
            print("Remote watchdog reachable again.", flush=True)
        self._push_failures = self._push_failures + 1 if problem else 0

    def _maybe_send_heartbeat(self) -> None:
        if not self.heartbeat_every:
            return
        if time.time() - self._last_heartbeat_msg >= self.heartbeat_every:
            self.notifier.send_status(
                f"[HEARTBEAT] Monitor alive\n"
                f"Block {self.last_block} | alerts sent since last heartbeat: "
                f"{self._alerts_since_heartbeat} | open incidents: {self.notifier.open_incidents}"
            )
            self._last_heartbeat_msg = time.time()
            self._alerts_since_heartbeat = 0

    # ------------------------------------------------------------ main loop

    def run(self, interval: float = 1.0) -> None:
        self.notifier.send_status(
            f"[ONLINE] Monitor started\n"
            f"Watching token {self.token.address}\n"
            f"From block {self.last_block + 1}"
        )
        errors = 0
        while True:
            started = time.time()
            try:
                self.poll_once()
                self._maybe_send_heartbeat()
                self._maybe_push_heartbeat()
                errors = 0
            except KeyboardInterrupt:
                raise
            except Exception as err:  # keep running; one bad RPC response should not stop the guard
                errors += 1
                if is_connection_error(err):
                    if errors == 1 or errors % 20 == 0:
                        print(f"Cannot reach the RPC provider ({type(err).__name__}); retrying. "
                              "Check this computer's internet / DNS.", flush=True)
                elif errors == 1:
                    print(format_exc(), flush=True)
                else:
                    print(f"Poll failed again ({type(err).__name__}: {redact(err)[:300]}); backing off.", flush=True)
                time.sleep(min(60, interval * 2 ** min(errors, 6)))
                continue
            time.sleep(max(0.0, interval - (time.time() - started)))
