"""
Computational puzzles: make attackers pay in computing time.

Two kinds, both cheap for the SERVER to check:

1. Proof of work (Hashcash). Find a nonce so that SHA-256(challenge + nonce)
   starts with `bits` zero bits. Expected work: 2**bits hashes. Checking: one
   hash. Every extra bit doubles the cost. Weakness: it parallelises, so an
   attacker with many cores or GPUs solves it faster.

2. Time-lock puzzle (Rivest, Shamir & Wagner, 1996). Compute
   y = x ** (2 ** t) mod N, where N = p * q is an RSA modulus. Without knowing
   p and q, the only known way is t squarings ONE AFTER ANOTHER, so extra
   computers do not help: it costs wall-clock time. The server knows
   phi(N) = (p-1)(q-1), so it checks the answer instantly with
   y == x ** (2**t mod phi(N)) mod N.

Puzzles are stateless: the server signs each one with HMAC-SHA256 and an
expiry time, so it only has to remember the IDs of puzzles already used
(to stop a solved puzzle being replayed).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time

POW = "pow"
TIMELOCK = "timelock"


# ------------------------------------------------------------ number theory


def _is_probable_prime(n: int, rounds: int = 40) -> bool:
    if n < 2:
        return False
    for p in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37):
        if n % p == 0:
            return n == p
    d, r = n - 1, 0
    while d % 2 == 0:
        d //= 2
        r += 1
    for _ in range(rounds):  # Miller-Rabin
        a = secrets.randbelow(n - 3) + 2
        x = pow(a, d, n)
        if x in (1, n - 1):
            continue
        for _ in range(r - 1):
            x = pow(x, 2, n)
            if x == n - 1:
                break
        else:
            return False
    return True


def random_prime(bits: int) -> int:
    while True:
        n = secrets.randbits(bits) | (1 << (bits - 1)) | 1
        if _is_probable_prime(n):
            return n


# ------------------------------------------------------------ solving (client side)


def leading_zero_bits(digest: bytes) -> int:
    n = int.from_bytes(digest, "big")
    return len(digest) * 8 - n.bit_length()


def solve_pow(challenge: str, bits: int) -> str:
    prefix = challenge.encode()
    nonce = 0
    while True:
        if leading_zero_bits(hashlib.sha256(prefix + str(nonce).encode()).digest()) >= bits:
            return str(nonce)
        nonce += 1


def solve_timelock(x: int, t: int, n: int) -> str:
    y = x
    for _ in range(t):  # strictly sequential
        y = y * y % n
    return str(y)


def solve(puzzle: dict) -> str:
    if puzzle["type"] == POW:
        return solve_pow(puzzle["challenge"], puzzle["bits"])
    if puzzle["type"] == TIMELOCK:
        return solve_timelock(int(puzzle["x"]), puzzle["t"], int(puzzle["n"]))
    raise ValueError(f"unknown puzzle type {puzzle['type']}")


# ------------------------------------------------------------ issuing and checking (server side)


class PuzzleIssuer:
    def __init__(self, secret: bytes | None = None, ttl: int = 300, modulus_bits: int = 2048):
        self.secret = secret or secrets.token_bytes(32)
        self.ttl = ttl
        half = modulus_bits // 2
        p, q = random_prime(half), random_prime(half)
        while q == p:
            q = random_prime(half)
        self.n = p * q
        self._phi = (p - 1) * (q - 1)
        self._used: dict[str, float] = {}
        self.squarings_per_second = self._measure()

    def _measure(self) -> float:
        """How fast this machine squares mod N (to size time-lock puzzles)."""
        x, count = secrets.randbelow(self.n), 20_000
        start = time.perf_counter()
        for _ in range(count):
            x = x * x % self.n
        return count / (time.perf_counter() - start)

    def _sign(self, puzzle: dict) -> str:
        body = json.dumps({k: v for k, v in puzzle.items() if k != "sig"}, sort_keys=True)
        return hmac.new(self.secret, body.encode(), hashlib.sha256).hexdigest()

    def issue_pow(self, bits: int) -> dict:
        p = {
            "type": POW,
            "id": secrets.token_hex(8),
            "challenge": secrets.token_hex(16),
            "bits": bits,
            "expires": int(time.time()) + self.ttl,
        }
        p["sig"] = self._sign(p)
        return p

    def issue_timelock(self, seconds: float) -> dict:
        t = max(1, int(seconds * self.squarings_per_second))
        p = {
            "type": TIMELOCK,
            "id": secrets.token_hex(8),
            "n": str(self.n),
            "x": str(secrets.randbelow(self.n - 3) + 2),
            "t": t,
            "expires": int(time.time()) + self.ttl + int(seconds) * 3,
        }
        p["sig"] = self._sign(p)
        return p

    def verify(self, puzzle: dict, solution: str) -> tuple[bool, str]:
        """Returns (ok, reason)."""
        now = time.time()
        self._used = {k: v for k, v in self._used.items() if v > now}
        try:
            if not hmac.compare_digest(puzzle.get("sig", ""), self._sign(puzzle)):
                return False, "puzzle was not issued by this server"
            if puzzle["expires"] < now:
                return False, "puzzle expired"
            if puzzle["id"] in self._used:
                return False, "puzzle already used"
            if puzzle["type"] == POW:
                digest = hashlib.sha256((puzzle["challenge"] + str(solution)).encode()).digest()
                ok = leading_zero_bits(digest) >= puzzle["bits"]
            elif puzzle["type"] == TIMELOCK:
                n, x, t = int(puzzle["n"]), int(puzzle["x"]), puzzle["t"]
                ok = pow(x, pow(2, t, self._phi), n) == int(solution)  # instant shortcut
            else:
                return False, "unknown puzzle type"
        except (KeyError, ValueError, TypeError):
            return False, "malformed puzzle or solution"
        if not ok:
            return False, "wrong solution"
        self._used[puzzle["id"]] = puzzle["expires"]
        return True, "ok"
