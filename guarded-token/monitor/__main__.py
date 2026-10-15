"""
Run the monitor against a node with an HTTP RPC endpoint.

    python -m monitor --rpc http://127.0.0.1:8545 --token 0xYourToken

Settings can also come from a .env file (see .env.example).
"""

import argparse
import os

from web3 import Web3

from .chain import ChainMonitor
from .config import Settings
from .redact import install as hide_secrets_in_errors


def main():
    hide_secrets_in_errors()
    settings = Settings.load()
    parser = argparse.ArgumentParser(description="Guarded token monitor")
    parser.add_argument("--rpc", default=settings.rpc_url)
    parser.add_argument("--token", default=settings.token_address)
    parser.add_argument("--interval", type=float, default=settings.poll_seconds, help="seconds between polls")
    args = parser.parse_args()
    if not args.token:
        parser.error("set --token or TOKEN_ADDRESS")

    w3 = Web3(Web3.HTTPProvider(args.rpc))
    if not w3.is_connected():
        parser.error(f"cannot reach RPC at {args.rpc}")

    notifier = settings.make_notifier()
    monitor = ChainMonitor(
        w3,
        args.token,
        notifier,
        detector=settings.make_detector(),
        heartbeat_path=settings.heartbeat_path,
        heartbeat_every=settings.heartbeat_every,
        responder=settings.make_responder(w3, args.token, notifier),
        threat_path=settings.threat_path,
        remote_watchdog_url=settings.remote_watchdog_url or None,
    )
    start = int(os.getenv("START_BLOCK", "0"))
    behind = w3.eth.block_number - start
    if behind > 2000:
        print(f"Reading the token's history: {behind:,} blocks since block {start}. On a free RPC plan "
              "this can take a few minutes...", flush=True)
    events = monitor.warm_up(start)
    print(f"Learned the token's history from block {start} ({events} scored events, no alerts sent).")
    print(f"Monitoring {args.token} every {args.interval}s. Ctrl+C to stop.")
    try:
        monitor.run(interval=args.interval)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")


if __name__ == "__main__":
    main()
