"""
Measure what the puzzles cost an honest user vs a flooding attacker.

    python -m defense.flood_demo

For each threat level, an honest client makes an occasional request and an
attacker floods the API. The attacker quickly gets marked suspicious and must
solve much harder puzzles. One puzzle per case is really solved and checked
by the gate. Costs are EXPECTED values from this machine's measured speed:
proof-of-work solve times vary a lot from puzzle to puzzle (it is luck), so a
handful of samples would mislead; 2**bits / hashes-per-second does not.
"""

from __future__ import annotations

import hashlib
import time

from .gate import LEVELS, RATE_PER_MIN, Gate
from .puzzles import solve


def hash_rate(seconds: float = 1.0) -> float:
    """SHA-256 attempts per second, done the same way the solver does them."""
    n, start = 0, time.perf_counter()
    while time.perf_counter() - start < seconds:
        for i in range(10_000):
            hashlib.sha256(b"challenge" + str(i).encode()).digest()
        n += 10_000
    return n / (time.perf_counter() - start)


def cost(gate: Gate, client: str, rate: float) -> tuple[str, float, float]:
    """(label, expected seconds per request, measured seconds for one real solve)."""
    p = gate.challenge(client)
    if p is None:
        return "none", 0.0, 0.0
    start = time.perf_counter()
    answer = solve(p)
    measured = time.perf_counter() - start
    ok, reason = gate.admit(client, p, answer)
    assert ok, reason
    if p["type"] == "pow":
        return f"proof of work {p['bits']} bits", 2 ** p["bits"] / rate, measured
    return f"time-lock ({p['t']:,} squarings)", measured, measured


def human(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 120:
        return f"{seconds:.1f} s"
    if seconds < 7200:
        return f"{seconds / 60:.0f} min"
    if seconds < 172_800:
        return f"{seconds / 3600:.1f} h"
    return f"{seconds / 86_400:.1f} days"


def main():
    print("Setting up the puzzle gate (generating a 2048-bit RSA modulus)...")
    gate = Gate()
    rate = hash_rate()
    print(f"This machine: {rate:,.0f} SHA-256 hashes/s and "
          f"{gate.issuer.squarings_per_second:,.0f} modular squarings/s.\n")
    print(f"  {'level':9s} {'honest user':42s} {'flooding attacker':44s}")
    rows = []
    for level in LEVELS:
        gate.level_override = level
        honest, attacker = f"honest-{level}", f"attacker-{level}"
        for _ in range(RATE_PER_MIN + 1):  # the attacker floods
            gate.note_request(attacker)
        h_label, h_cost, _ = cost(gate, honest, rate)
        a_label, a_cost, _ = cost(gate, attacker, rate)
        rows.append((level, h_label, h_cost, a_label, a_cost))
        print(f"  {level:9s} {h_label:30s} {human(h_cost):>9s}   {a_label:33s} {human(a_cost):>9s}")

    print("\nExpected cost of 10,000 requests on one CPU core:")
    print(f"  {'threat level':12s} {'honest user':>14s} {'flooding attacker':>20s}")
    for level, _, h, _, a in rows:
        print(f"  {level:12s} {human(h * 10_000):>14s} {human(a * 10_000):>20s}")
    print(
        "\nNotes: proof of work can be parallelised (100 cores = ~100x faster), so the\n"
        "time-lock puzzle is used against suspicious clients at the critical level:\n"
        "its squarings must run one after another, so extra machines do not help.\n"
        "Optimised code (e.g. GMP in C) can be several times faster than this Python."
    )


if __name__ == "__main__":
    main()
