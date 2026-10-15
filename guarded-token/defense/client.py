"""
Client for the puzzle-protected API: fetches a puzzle, solves it, asks.

    python -m defense.client http://127.0.0.1:8080 0xSomeAddress [--times 3]
"""

from __future__ import annotations

import argparse
import base64
import json
import time

import requests

from .puzzles import solve


def fetch_balance(base: str, address: str, session=None) -> tuple[dict, float, str]:
    """Returns (response json, seconds spent solving, puzzle description)."""
    s = session or requests.Session()
    puzzle = s.get(f"{base}/challenge", timeout=10).json()["puzzle"]
    headers, spent, what = {}, 0.0, "none"
    if puzzle:
        what = f"proof of work, {puzzle['bits']} bits" if puzzle["type"] == "pow" else f"time-lock, {puzzle['t']:,} squarings"
        start = time.perf_counter()
        solution = solve(puzzle)
        spent = time.perf_counter() - start
        headers = {"X-Puzzle": base64.b64encode(json.dumps(puzzle).encode()).decode(), "X-Solution": solution}
    r = s.get(f"{base}/balance/{address}", headers=headers, timeout=30)
    return r.json() | {"http": r.status_code}, spent, what


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("base", help="e.g. http://127.0.0.1:8080")
    ap.add_argument("address")
    ap.add_argument("--times", type=int, default=1)
    args = ap.parse_args()
    base = args.base.rstrip("/")
    print("status:", requests.get(f"{base}/status", timeout=10).json())
    for i in range(args.times):
        body, spent, what = fetch_balance(base, args.address)
        print(f"request {i + 1}: puzzle {what}, solved in {spent:.2f}s -> HTTP {body.pop('http')} {body}")


if __name__ == "__main__":
    main()
