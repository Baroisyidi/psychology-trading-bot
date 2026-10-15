"""
The gate in front of the public API: decides who must solve which puzzle.

Difficulty depends on two things:
  - the THREAT LEVEL, written by the monitor from its open incidents
  - whether THIS CLIENT looks suspicious (too many requests, failed puzzles)

| Threat level | Normal client | Suspicious client |
|--------------|---------------|-------------------|
| normal       | no puzzle     | proof of work, 16 bits |
| elevated     | PoW 12 bits   | PoW 20 bits |
| high         | PoW 16 bits   | PoW 22 bits |
| critical     | PoW 18 bits   | time-lock, 10 seconds |
"""

from __future__ import annotations

import json
import time
from collections import defaultdict, deque
from pathlib import Path

from .puzzles import PuzzleIssuer

LEVELS = ["normal", "elevated", "high", "critical"]

POLICY = {
    # level: (normal client, suspicious client); ("pow", bits) or ("timelock", seconds)
    "normal": (None, ("pow", 16)),
    "elevated": (("pow", 12), ("pow", 20)),
    "high": (("pow", 16), ("pow", 22)),
    "critical": (("pow", 18), ("timelock", 10)),
}

RATE_PER_MIN = 30  # more requests than this per minute marks a client suspicious
SUSPICIOUS_FOR = 600  # seconds
MAX_FAILURES = 3


def threat_from_peak(peak: int, paused: bool = False) -> str:
    if paused or peak >= 90:
        return "critical"
    if peak >= 70:
        return "high"
    if peak >= 40:
        return "elevated"
    return "normal"


def write_threat(path: str | Path, open_incidents: int, peak: int, paused: bool = False) -> None:
    """Called by the monitor after each poll."""
    path = Path(path)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({
        "level": threat_from_peak(peak, paused),
        "open_incidents": open_incidents,
        "peak": peak,
        "paused": bool(paused),
        "time": time.time(),
    }))
    tmp.replace(path)


class Gate:
    def __init__(self, issuer: PuzzleIssuer | None = None, threat_path: str | Path | None = None,
                 policy: dict | None = None):
        self.issuer = issuer or PuzzleIssuer()
        self.threat_path = Path(threat_path) if threat_path else None
        self.policy = policy or POLICY
        self.level_override: str | None = None
        self.requests: dict[str, deque] = defaultdict(deque)
        self.failures: dict[str, int] = defaultdict(int)
        self.suspicious_until: dict[str, float] = {}

    # ------------------------------------------------------------ state

    def level(self) -> str:
        if self.level_override:
            return self.level_override
        if self.threat_path and self.threat_path.exists():
            try:
                data = json.loads(self.threat_path.read_text())
                if time.time() - data.get("time", 0) < 120:
                    return data["level"]
                return "high"  # monitor silent for 2 minutes: assume the worst
            except (ValueError, KeyError):
                return "high"
        return "normal"

    def is_suspicious(self, client: str) -> bool:
        return self.suspicious_until.get(client, 0) > time.time()

    def _mark(self, client: str):
        self.suspicious_until[client] = time.time() + SUSPICIOUS_FOR

    def note_request(self, client: str) -> None:
        now = time.time()
        q = self.requests[client]
        q.append(now)
        while q and q[0] <= now - 60:
            q.popleft()
        if len(q) > RATE_PER_MIN:
            self._mark(client)

    # ------------------------------------------------------------ puzzles

    def requirement(self, client: str):
        normal, suspicious = self.policy[self.level()]
        return suspicious if self.is_suspicious(client) else normal

    def challenge(self, client: str) -> dict | None:
        req = self.requirement(client)
        if req is None:
            return None
        kind, amount = req
        return self.issuer.issue_pow(amount) if kind == "pow" else self.issuer.issue_timelock(amount)

    def admit(self, client: str, puzzle: dict | None, solution: str | None) -> tuple[bool, str]:
        """Should this request be served? Counts the request first."""
        self.note_request(client)
        req = self.requirement(client)
        if req is None:
            return True, "no puzzle needed"
        if not puzzle or solution is None:
            return False, "puzzle required: GET /challenge first"
        kind, amount = req
        # The puzzle must be at least as hard as what is required right now.
        if puzzle.get("type") == "pow" and kind == "pow" and puzzle.get("bits", 0) < amount:
            return False, "puzzle too easy for the current threat level"
        if kind == "timelock" and puzzle.get("type") != "timelock":
            return False, "a time-lock puzzle is required"
        ok, reason = self.issuer.verify(puzzle, solution)
        if not ok:
            self.failures[client] += 1
            if self.failures[client] >= MAX_FAILURES:
                self._mark(client)
        return ok, reason
