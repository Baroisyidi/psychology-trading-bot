"""
A small public API protected by the puzzle gate.

    python -m defense.api --port 8080

Endpoints:
  GET /status             threat level, and whether YOU are marked suspicious (free)
  GET /challenge          the puzzle you must solve right now (or null)
  GET /balance/<address>  token balance; send the solved puzzle in two headers:
                            X-Puzzle:   the puzzle JSON, base64-encoded
                            X-Solution: your answer
  Use `python -m defense.client` to do all of this automatically.

Responses: 200 ok, 403 puzzle missing/wrong, 404 unknown path.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .gate import Gate


def make_handler(gate: Gate, balance_of):
    class Handler(BaseHTTPRequestHandler):
        def _reply(self, code: int, body: dict):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, fmt, *args):  # one short line per request
            print(f"[api] {self.client_address[0]} {fmt % args}")

        def do_GET(self):
            client = self.client_address[0]
            path = self.path.split("?")[0].rstrip("/")
            if path == "/status":
                return self._reply(200, {"threat": gate.level(), "you_are_suspicious": gate.is_suspicious(client)})
            if path == "/challenge":
                return self._reply(200, {"threat": gate.level(), "puzzle": gate.challenge(client)})
            if path.startswith("/balance/"):
                puzzle = None
                if self.headers.get("X-Puzzle"):
                    try:
                        puzzle = json.loads(base64.b64decode(self.headers["X-Puzzle"]))
                    except ValueError:
                        return self._reply(403, {"error": "X-Puzzle is not valid base64 JSON"})
                ok, reason = gate.admit(client, puzzle, self.headers.get("X-Solution"))
                if not ok:
                    return self._reply(403, {"error": reason, "threat": gate.level()})
                address = path.rsplit("/", 1)[-1]
                try:
                    return self._reply(200, {"address": address, "balance_gtt": balance_of(address)})
                except Exception as err:
                    return self._reply(400, {"error": f"could not read balance: {err}"})
            return self._reply(404, {"error": "unknown path", "paths": ["/status", "/challenge", "/balance/<address>"]})

    return Handler


def chain_balance_reader(rpc_url: str, token_address: str):
    from web3 import Web3

    from monitor.chain import load_abi

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    token = w3.eth.contract(address=Web3.to_checksum_address(token_address), abi=load_abi())
    return lambda a: token.functions.balanceOf(Web3.to_checksum_address(a)).call() / 10**18


def serve(port: int, gate: Gate, balance_of) -> ThreadingHTTPServer:
    return ThreadingHTTPServer(("0.0.0.0", port), make_handler(gate, balance_of))


def main():
    ap = argparse.ArgumentParser(description="Puzzle-protected token API")
    ap.add_argument("--port", type=int, default=int(os.getenv("API_PORT", "8080")))
    args = ap.parse_args()
    token = os.getenv("TOKEN_ADDRESS", "")
    if not token and os.getenv("TOKEN_ADDRESS_FILE") and Path(os.environ["TOKEN_ADDRESS_FILE"]).exists():
        token = Path(os.environ["TOKEN_ADDRESS_FILE"]).read_text().strip()
    if not token:
        raise SystemExit("Set TOKEN_ADDRESS (or TOKEN_ADDRESS_FILE).")
    gate = Gate(threat_path=os.getenv("THREAT_PATH", "threat.json"))
    print(f"Puzzle gate ready ({gate.issuer.squarings_per_second:,.0f} squarings/s measured for time-locks).")
    server = serve(args.port, gate, chain_balance_reader(os.getenv("RPC_URL", "http://127.0.0.1:8545"), token))
    print(f"API on http://0.0.0.0:{args.port}  (try /status)")
    server.serve_forever()


if __name__ == "__main__":
    main()
