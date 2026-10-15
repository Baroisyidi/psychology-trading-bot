import base64
import json
import threading
import time

import pytest
import requests

from defense.api import serve
from defense.gate import RATE_PER_MIN, Gate, threat_from_peak, write_threat
from defense.puzzles import PuzzleIssuer, solve

EASY = {  # tiny puzzles so tests run fast
    "normal": (None, ("pow", 4)),
    "elevated": (("pow", 2), ("pow", 6)),
    "high": (("pow", 4), ("pow", 8)),
    "critical": (("pow", 6), ("timelock", 0.01)),
}


@pytest.fixture(scope="module")
def issuer():
    return PuzzleIssuer(modulus_bits=512)  # small modulus: fast to generate in tests


def test_pow_roundtrip_and_tamper(issuer):
    p = issuer.issue_pow(8)
    answer = solve(p)
    assert issuer.verify(p, answer) == (True, "ok")
    assert issuer.verify(p, answer) == (False, "puzzle already used")  # no replays
    q = issuer.issue_pow(8)
    q["bits"] = 1  # make it easier: signature no longer matches
    assert issuer.verify(q, solve(q))[0] is False


def test_timelock_is_checked_with_the_shortcut(issuer):
    p = issuer.issue_timelock(0.01)
    assert p["t"] > 0
    assert issuer.verify(p, solve(p)) == (True, "ok")
    p2 = issuer.issue_timelock(0.01)
    assert issuer.verify(p2, "12345") == (False, "wrong solution")


def test_expired_puzzle_rejected(issuer):
    p = issuer.issue_pow(2)
    p["expires"] = int(time.time()) - 1
    p["sig"] = issuer._sign(p)
    assert issuer.verify(p, solve(p)) == (False, "puzzle expired")


def test_threat_levels(tmp_path):
    assert [threat_from_peak(s) for s in (0, 40, 70, 90)] == ["normal", "elevated", "high", "critical"]
    assert threat_from_peak(0, paused=True) == "critical"
    path = tmp_path / "threat.json"
    write_threat(path, 2, 75)
    gate = Gate(issuer=PuzzleIssuer(modulus_bits=512), threat_path=path, policy=EASY)
    assert gate.level() == "high"


def test_flooding_client_gets_harder_puzzles(issuer):
    gate = Gate(issuer=issuer, policy=EASY)
    gate.level_override = "critical"
    assert gate.requirement("honest") == ("pow", 6)
    for _ in range(RATE_PER_MIN + 1):
        gate.note_request("flooder")
    assert gate.requirement("flooder") == ("timelock", 0.01)
    p = gate.challenge("flooder")
    assert gate.admit("flooder", p, solve(p))[0]
    easy = gate.issuer.issue_pow(6)  # an easy puzzle is not accepted from a suspicious client
    assert gate.admit("flooder", easy, solve(easy)) == (False, "a time-lock puzzle is required")


def test_failed_solutions_mark_client_suspicious(issuer):
    # 30-bit puzzles: a guessed answer is right with probability 2**-30 (never solved here).
    gate = Gate(issuer=issuer, policy=EASY | {"high": (("pow", 30), ("pow", 30))})
    gate.level_override = "high"
    for _ in range(3):
        gate.admit("guesser", gate.challenge("guesser"), "0")
    assert gate.is_suspicious("guesser")


def test_http_api_end_to_end(issuer):
    gate = Gate(issuer=issuer, policy=EASY)
    gate.level_override = "elevated"
    server = serve(0, gate, lambda a: 42.0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        assert requests.get(f"{base}/status").json()["threat"] == "elevated"
        assert requests.get(f"{base}/balance/0xabc").status_code == 403  # no puzzle
        p = requests.get(f"{base}/challenge").json()["puzzle"]
        r = requests.get(f"{base}/balance/0xabc", headers={
            "X-Puzzle": base64.b64encode(json.dumps(p).encode()).decode(),
            "X-Solution": solve(p),
        })
        assert r.status_code == 200 and r.json()["balance_gtt"] == 42.0
    finally:
        server.shutdown()
