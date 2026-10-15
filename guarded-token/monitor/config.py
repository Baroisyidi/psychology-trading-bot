"""Settings, read from environment variables or a .env file."""

from __future__ import annotations

import os
from pathlib import Path
from dataclasses import dataclass

from dotenv import load_dotenv

from .notify import ConsoleSender, Notifier, TelegramSender
from .signing import Signer


@dataclass
class Settings:
    rpc_url: str
    token_address: str
    telegram_bot_token: str
    telegram_chat_ids: list[str]
    min_score: int
    heartbeat_every: int
    signing_key_path: str
    heartbeat_path: str
    watchdog_stale_after: int
    detector: str
    model_path: str
    alert_log_path: str
    guardian_mode: str
    guardian_private_key: str
    protected_wallets: list[str]
    threat_path: str
    guardian_keystore: str
    guardian_keystore_password: str
    poll_seconds: float
    remote_watchdog_url: str

    @classmethod
    def load(cls) -> "Settings":
        load_dotenv()
        chat_ids = [c.strip() for c in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]
        return cls(
            rpc_url=os.getenv("RPC_URL", "http://127.0.0.1:8545"),
            token_address=os.getenv("TOKEN_ADDRESS", ""),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_ids=chat_ids,
            min_score=int(os.getenv("MIN_ALERT_SCORE", "40")),
            heartbeat_every=int(os.getenv("HEARTBEAT_SECONDS", "600")),
            signing_key_path=os.getenv("SIGNING_KEY_PATH", "keys/monitor_ed25519.key"),
            heartbeat_path=os.getenv("HEARTBEAT_PATH", "monitor_heartbeat.json"),
            watchdog_stale_after=int(os.getenv("WATCHDOG_STALE_SECONDS", "30")),
            detector=os.getenv("DETECTOR", "auto").lower(),
            model_path=os.getenv("MODEL_PATH", "models/guard_model.joblib"),
            alert_log_path=os.getenv("ALERT_LOG_PATH", "alerts.jsonl"),
            guardian_mode=(os.getenv("GUARDIAN_MODE") or "dry-run").lower(),
            guardian_private_key=os.getenv("GUARDIAN_PRIVATE_KEY", ""),
            protected_wallets=[w.strip() for w in os.getenv("PROTECTED_WALLETS", "").split(",") if w.strip()],
            threat_path=os.getenv("THREAT_PATH", "threat.json"),
            guardian_keystore=os.getenv("GUARDIAN_KEYSTORE", ""),
            guardian_keystore_password=os.getenv("GUARDIAN_KEYSTORE_PASSWORD", ""),
            poll_seconds=float(os.getenv("POLL_SECONDS", "1")),
            remote_watchdog_url=os.getenv("REMOTE_WATCHDOG_URL", ""),
        )

    def make_responder(self, w3, token_address: str, notifier):
        """The Phase 4 guardian, or None when GUARDIAN_MODE=off."""
        from .guardian import LOCAL_CHAIN_IDS, Executor, Responder, local_test_key

        if self.guardian_mode == "off":
            print("Guardian: off (alerts only).")
            return None
        key = self.guardian_private_key
        if not key and self.guardian_keystore:
            import sys

            from tools.keystore import load_key

            if not self.guardian_keystore_password and not sys.stdin.isatty():
                raise SystemExit("GUARDIAN_KEYSTORE is set but GUARDIAN_KEYSTORE_PASSWORD is not, and there is "
                                 "no keyboard to type it on. Add GUARDIAN_KEYSTORE_PASSWORD to your env file.")
            key = load_key(self.guardian_keystore, self.guardian_keystore_password)
        if not key:
            if w3.eth.chain_id not in LOCAL_CHAIN_IDS:
                raise SystemExit(
                    "No guardian key. Set GUARDIAN_KEYSTORE (+ GUARDIAN_KEYSTORE_PASSWORD) "
                    "or GUARDIAN_PRIVATE_KEY. The public test key is never used on a public network."
                )
            key = local_test_key(1)
        responder = Responder(Executor(w3, token_address, key), notifier, mode=self.guardian_mode,
                              protected=set(self.protected_wallets))
        print(f"Guardian: {self.guardian_mode} (acts as {responder.ex.address}).")
        if self.guardian_mode == "on":
            for problem in guardian_problems(w3, responder.ex):
                print(f"WARNING: {problem}")
                notifier.send_status(f"[WARNING] Guardian problem: {problem}")
        return responder

    def make_detector(self):
        """auto: trained models if a model file exists, else Phase 2 rules."""
        from .rules import Detector

        if self.detector == "rules":
            print("Detector: Phase 2 rules (DETECTOR=rules).")
            return Detector()
        path = Path(self.model_path)
        if not path.exists():
            if self.detector == "ml":
                raise SystemExit(f"DETECTOR=ml but no model at {path}. Run: python -m ml.train")
            print(f"Detector: Phase 2 rules (no model at {path}; run `python -m ml.train` to create one).")
            return Detector()
        try:
            from .ml_detector import GuardDetector

            det = GuardDetector.load(path)
        except Exception as err:  # a broken or incompatible model must not stop the guard
            if self.detector == "ml":
                raise
            print(f"Detector: Phase 2 rules (could not load model: {type(err).__name__}).")
            return Detector()
        print(f"Detector: Phase 3 models ({det.description}) + Phase 2 rules for admin events.")
        return det

    def make_sender(self):
        if self.telegram_bot_token and self.telegram_chat_ids:
            return TelegramSender(self.telegram_bot_token, self.telegram_chat_ids)
        print("No TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_IDS set: printing alerts to the console.")
        return ConsoleSender()

    def make_notifier(self, key_path: str | None = None, log_path="default") -> Notifier:
        if log_path == "default":
            log_path = self.alert_log_path
        signer = Signer.from_file(key_path or self.signing_key_path)
        print(f"Alert signing public key: {signer.public_key_hex}")
        return Notifier(self.make_sender(), signer, min_score=self.min_score, log_path=log_path)


def guardian_problems(w3, executor, min_eth: float = 0.002) -> list[str]:
    """Things that would make the guardian's actions fail when it matters most."""
    from tools import cmd

    problems = []
    admin, guardian = executor.roles()
    if executor.address not in (admin, guardian):
        problems.append(f"{executor.address} is neither the token's guardian ({guardian}) nor its admin, "
                        "so every action would be rejected. Check GUARDIAN_KEYSTORE.")
    eth = w3.eth.get_balance(executor.address) / 10**18
    if eth < min_eth:
        problems.append(f"the guardian wallet has only {eth:.4f} ETH for fees and may be unable to act. "
                        "Send it some test ETH: " + cmd("python -m tools.admin fund guardian 0.01"))
    return problems
