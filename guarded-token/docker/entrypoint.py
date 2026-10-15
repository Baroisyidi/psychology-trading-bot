"""
Container entry point. docker-compose.yml starts each service with a role:

    deploy    wait for the local chain, deploy the token (or reuse it)
    monitor   wait for the token, then run the 1-second monitor
    watchdog  run the independent "monitor down" alarm
    attack    play an attack against the running chain (drain or fanout)
    api       the puzzle-protected public API (Phase 4)

Public test network (docker-compose.sepolia.yml):
    public-monitor   wait for deployment/token_address, then monitor Sepolia
    public-watchdog  the local "monitor down" alarm, with public-network timing
    public-api       the puzzle-protected API, reading Sepolia
    keys | deploy-public | admin | drill   the tools in tools/ (see README)

Anything else is run as a normal command, e.g. `ape test`.
"""

import os
import sys
import time
from pathlib import Path

NODE_URI = os.environ.get("NODE_URI", "http://node:8545")
ADDRESS_FILE = Path(os.environ.get("TOKEN_ADDRESS_FILE", "/data/token_address"))
APE_NETWORK = f"ethereum:local:{NODE_URI}"


def say(msg):
    print(f"[entrypoint] {msg}", flush=True)


def web3():
    from web3 import Web3

    return Web3(Web3.HTTPProvider(NODE_URI, request_kwargs={"timeout": 3}))


def wait_for_node(timeout=90):
    w3 = web3()
    say(f"waiting for the local chain at {NODE_URI} ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if w3.is_connected():
                say(f"chain is up (block {w3.eth.block_number})")
                return w3
        except Exception:
            pass
        time.sleep(1)
    sys.exit(f"Local chain not reachable at {NODE_URI} after {timeout}s.")


def deployed_address(w3, timeout):
    """Wait until the address file names a contract that exists on the chain."""
    deadline = time.time() + timeout
    while True:
        if ADDRESS_FILE.exists():
            address = ADDRESS_FILE.read_text().strip()
            if address and w3.eth.get_code(w3.to_checksum_address(address)):
                return address
        if time.time() >= deadline:
            sys.exit(
                "No deployed token found. Start the stack first with:\n"
                "    docker compose up -d"
            )
        time.sleep(1)


def run(cmd, env_updates=None):
    os.environ.update(env_updates or {})
    say("running: " + " ".join(cmd))
    sys.stdout.flush()
    try:
        os.execvp(cmd[0], cmd)
    except FileNotFoundError:
        hint = ""
        if cmd[0] == "docker":
            hint = (
                "\nDocker commands run in PowerShell, not inside a container."
                "\nIf you pasted two commands on one line, run them one at a time."
            )
        sys.exit(f"Command not found inside the container: {cmd[0]}{hint}")


DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/deployment"))
TOOLS = {"keys": "tools.keys", "deploy-public": "tools.deploy_public", "admin": "tools.admin",
         "drill": "tools.sepolia_drill"}


def public_rpc():
    url = os.environ.get("SEPOLIA_RPC_URL", "").strip()
    if not url or "YOUR" in url.upper():
        sys.exit("Set SEPOLIA_RPC_URL in .env.sepolia (your Alchemy or Infura Sepolia URL).")
    return url


def public_deployment(rpc):
    """The token address and start block written by `deploy-public`. Waits until
    they exist AND the contract is really on this network (a leftover file from
    another network or a wiped chain must not be monitored)."""
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 20}))
    address_file, start_file, told = DATA_DIR / "token_address", DATA_DIR / "start_block", ""
    while True:
        why = "no token deployed yet"
        try:
            if address_file.exists():
                address = Web3.to_checksum_address(address_file.read_text().strip())
                if w3.eth.get_code(address):
                    start = start_file.read_text().strip() if start_file.exists() else "0"
                    return address, start
                why = f"no contract at {address} on this network (chain id {w3.eth.chain_id})"
        except Exception as err:  # RPC hiccup or a half-written file: try again
            why = f"cannot check the deployment yet ({type(err).__name__})"
        if why != told:
            say(f"{why}; waiting. Deploy the token with:\n"
                "    docker compose -f docker-compose.sepolia.yml run --rm tools deploy-public")
            told = why
        time.sleep(10)


# Defaults for a public network, used when .env.sepolia leaves a setting empty.
PUBLIC_DEFAULTS = {"GUARDIAN_MODE": "on", "POLL_SECONDS": "6", "LOG_BLOCK_RANGE": "10",
                   "HEARTBEAT_SECONDS": "3600", "WATCHDOG_STALE_SECONDS": "120"}


def public_defaults():
    return {k: os.environ.get(k) or v for k, v in PUBLIC_DEFAULTS.items()}


def public_env():
    rpc = public_rpc()
    address, start = public_deployment(rpc)
    env = {"RPC_URL": rpc, "TOKEN_ADDRESS": address, "START_BLOCK": os.environ.get("START_BLOCK") or start}
    env.update(public_defaults())
    keystore = Path(os.environ.get("KEYS_DIR", "/app/secrets")) / "guardian.json"
    if not os.environ.get("GUARDIAN_PRIVATE_KEY") and not os.environ.get("GUARDIAN_KEYSTORE") and keystore.exists():
        env["GUARDIAN_KEYSTORE"] = str(keystore)
    return env


def main():
    args = sys.argv[1:] or ["ape", "test"]
    role = args[0]

    if role in TOOLS:
        env = {"RPC_URL": public_rpc()} if role != "keys" else {}
        run(["python", "-m", TOOLS[role], *args[1:]], env)

    elif role == "public-monitor":
        run(["python", "-m", "monitor", *args[1:]], public_env())

    elif role == "public-watchdog":
        run(["python", "-m", "monitor.watchdog", *args[1:]], public_defaults())

    elif role == "public-api":
        run(["python", "-m", "defense.api", *args[1:]], public_env())

    elif role == "tools":
        print(__doc__)
        sys.exit("Name a tool, e.g.: docker compose -f docker-compose.sepolia.yml run --rm tools keys new admin")

    elif role == "deploy":
        wait_for_node()
        run(["ape", "run", "deploy", "--network", APE_NETWORK])

    elif role == "monitor":
        w3 = wait_for_node()
        address = deployed_address(w3, timeout=120)
        run(
            ["python", "-m", "monitor", *args[1:]],
            {"RPC_URL": NODE_URI, "TOKEN_ADDRESS": address},
        )

    elif role == "api":
        w3 = wait_for_node()
        address = deployed_address(w3, timeout=120)
        run(["python", "-m", "defense.api", *args[1:]], {"RPC_URL": NODE_URI, "TOKEN_ADDRESS": address})

    elif role == "watchdog":
        run(["python", "-m", "monitor.watchdog", *args[1:]])

    elif role in ("attack", "drain", "fanout"):
        # `docker compose run --rm attack fanout` replaces the command with ["fanout"].
        w3 = wait_for_node(timeout=15)
        address = deployed_address(w3, timeout=5)
        scenario = role if role != "attack" else (args[1] if len(args) > 1 else "drain")
        run(
            ["ape", "run", "attack_sim", "--network", APE_NETWORK],
            {"TOKEN_ADDRESS": address, "ATTACK": scenario},
        )

    else:
        run(args)


if __name__ == "__main__":
    main()
