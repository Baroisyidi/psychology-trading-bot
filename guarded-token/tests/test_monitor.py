import json

import pytest
from nacl.signing import SigningKey

from conftest import HOURLY_LIMIT, TOKEN
from monitor.alerts import Alert, severity
from monitor.chain import ChainMonitor
from monitor.notify import ConsoleSender, Notifier, TelegramSender
from monitor.signing import Signer, verify_text
from monitor.watchdog import Watchdog


def advance(chain, seconds):
    chain.pending_timestamp += seconds
    chain.mine()


@pytest.fixture
def signer():
    return Signer(SigningKey.generate())


@pytest.fixture
def sender():
    return ConsoleSender()


@pytest.fixture
def monitor(chain, token, sender, signer, tmp_path):
    notifier = Notifier(sender, signer, log_path=tmp_path / "alerts.jsonl")
    return ChainMonitor(
        chain.provider.web3,
        token.address,
        notifier,
        heartbeat_path=tmp_path / "hb.json",
        heartbeat_every=0,
    )


def make_alert(score=50, rules=("burst",), subject="0xabc", ts=1000):
    return Alert(
        score=score, title="t", details=[], rules=list(rules),
        block=1, timestamp=ts, subject=subject,
    )


# ------------------------------------------------------------- detection on chain


def test_no_new_blocks_no_alerts(monitor):
    assert monitor.poll_once() == []


def test_normal_activity_is_quiet(monitor, token, alice, bob, chain, sender):
    for _ in range(3):
        advance(chain, 300)
        token.transfer(bob, 100 * TOKEN, sender=alice)
    monitor.poll_once()
    assert sender.sent == []


def test_drain_at_hourly_limit_alerts(monitor, token, alice, bob, sender):
    token.transfer(bob, int(HOURLY_LIMIT * 0.9), sender=alice)
    alerts = monitor.poll_once()
    assert len(alerts) == 1
    a = alerts[0]
    assert "drain_pace" in a.rules and a.score >= 70
    assert a.subject == alice.address
    assert len(sender.sent) == 1


def test_fresh_wallet_dump_escalates(monitor, token, alice, attacker, bob, chain):
    token.transfer(attacker, 9_000 * TOKEN, sender=alice)
    monitor.poll_once()
    scores = []
    for amount in (4_000, 3_000, 1_500):
        advance(chain, 10)
        token.transfer(bob, amount * TOKEN, sender=attacker)
        scores += [a.score for a in monitor.poll_once() if a.subject == attacker.address]
    assert scores[-1] >= 80
    assert scores == sorted(scores)  # gets worse as the dump continues


def test_pause_is_critical(monitor, token, guardian):
    token.pause(sender=guardian)
    (alert,) = monitor.poll_once()
    assert alert.severity == "CRITICAL"
    assert alert.rules == ["admin_event:Paused"]
    assert any("(guardian)" in d for d in alert.details)


def test_proposal_to_remove_limit_is_flagged(monitor, token, admin):
    token.propose_limit(0, sender=admin)
    (alert,) = monitor.poll_once()
    assert alert.score == 70 and "REMOVE" in alert.title


def test_admin_minting_far_from_cap_is_only_logged(monitor, token, admin, bob, sender):
    token.mint(bob, 1_000 * TOKEN, sender=admin)
    (alert,) = monitor.poll_once()
    assert alert.score < 40 and sender.sent == []


def test_heartbeat_file_written(monitor, tmp_path):
    monitor.poll_once()
    data = json.loads((tmp_path / "hb.json").read_text())
    assert data["block"] >= 0 and data["time"] > 0


# ------------------------------------------------------------- severity and grouping


def test_severity_bands():
    assert [severity(s) for s in (10, 40, 69, 70, 89, 90, 100)] == [
        "LOG", "WARNING", "WARNING", "ALERT", "ALERT", "CRITICAL", "CRITICAL",
    ]


def test_repeats_are_grouped(sender, signer, tmp_path):
    n = Notifier(sender, signer, group_seconds=300, log_path=tmp_path / "a.jsonl")
    assert n.handle(make_alert(ts=1000))
    assert not n.handle(make_alert(ts=1010))  # same alert, same window
    assert not n.handle(make_alert(ts=1020))
    assert n.handle(make_alert(score=75, ts=1030))  # got worse: send anyway
    assert "(+2 similar alerts grouped)" in sender.sent[-1]
    assert n.handle(make_alert(ts=1400))  # new window
    lines = (tmp_path / "a.jsonl").read_text().splitlines()
    assert len(lines) == 5  # everything is logged, sent or not


def test_low_scores_not_sent(sender, signer):
    n = Notifier(sender, signer, log_path=None)
    assert not n.handle(make_alert(score=39))
    assert sender.sent == []


# ------------------------------------------------------------- signing


def test_signature_roundtrip(signer):
    signed = signer.sign_text("[ALERT 80] Something happened\nBlock 5")
    assert verify_text(signed, signer.public_key_hex)


def test_tampered_message_fails(signer):
    signed = signer.sign_text("[CRITICAL 92] Token paused")
    forged = signed.replace("CRITICAL 92", "LOG 5")
    assert not verify_text(forged, signer.public_key_hex)


def test_wrong_key_fails(signer):
    other = Signer(SigningKey.generate())
    assert not verify_text(other.sign_text("[HEARTBEAT] all clear"), signer.public_key_hex)


def test_key_file_created_private(tmp_path):
    path = tmp_path / "keys" / "k.key"
    s1 = Signer.from_file(path)
    assert (path.stat().st_mode & 0o777) == 0o600
    assert Signer.from_file(path).public_key_hex == s1.public_key_hex


# ------------------------------------------------------------- telegram


class FakeResponse:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body or {}
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, json=None, timeout=None):
        self.calls.append((url, json))
        return self.responses.pop(0) if self.responses else FakeResponse(200)


def test_telegram_sends_to_each_allowlisted_chat():
    session = FakeSession([])
    TelegramSender("123:ABC", ["111", "222"], session=session).send("hello")
    assert [c[1]["chat_id"] for c in session.calls] == ["111", "222"]
    assert session.calls[0][0] == "https://api.telegram.org/bot123:ABC/sendMessage"
    assert session.calls[0][1]["text"] == "hello"


def test_telegram_retries_after_rate_limit(monkeypatch):
    monkeypatch.setattr("monitor.notify.time.sleep", lambda s: None)
    session = FakeSession([FakeResponse(429, {"parameters": {"retry_after": 1}}), FakeResponse(200)])
    TelegramSender("123:ABC", ["111"], session=session).send("hi")
    assert len(session.calls) == 2


def test_telegram_error_never_prints_token(capsys):
    session = FakeSession([FakeResponse(401, {"description": "Unauthorized"})])
    TelegramSender("123:SECRET", ["111"], session=session).send("hi")
    assert "SECRET" not in capsys.readouterr().out


def test_telegram_requires_config():
    with pytest.raises(ValueError):
        TelegramSender("", ["1"])


# ------------------------------------------------------------- watchdog


def test_watchdog_down_then_recovered(sender, signer, tmp_path):
    hb = tmp_path / "hb.json"
    dog = Watchdog(Notifier(sender, signer, log_path=None), hb, stale_after=30, started_at=950)

    assert dog.check(now=970) == "ok"  # grace period while the monitor starts
    assert dog.check(now=1000) == "down"  # still no heartbeat file after 50s
    assert dog.check(now=1001) == "ok"  # does not repeat the alarm
    hb.write_text(json.dumps({"time": 1100, "block": 7}))
    assert dog.check(now=1105) == "recovered"
    assert dog.check(now=1140) == "down"  # 40s old: stale again
    assert "MONITOR DOWN" in sender.sent[0]
    assert verify_text(sender.sent[0], signer.public_key_hex)


def test_chain_reset_is_handled(monitor, token, alice, bob, chain):
    snap = chain.snapshot()
    for _ in range(3):
        chain.mine()
    monitor.poll_once()
    high = monitor.last_block
    chain.restore(snap)  # simulate the local node restarting
    assert chain.blocks.height < high
    token.transfer(bob, int(HOURLY_LIMIT * 0.9), sender=alice)
    alerts = monitor.poll_once()
    assert any("drain_pace" in a.rules for a in alerts)


def test_telegram_success_is_echoed_and_network_errors_survive(monkeypatch, capsys):
    import requests

    monkeypatch.setattr("monitor.notify.time.sleep", lambda s: None)

    class FlakySession(FakeSession):
        def post(self, url, json=None, timeout=None):
            self.calls.append((url, json))
            if len(self.calls) == 1:
                raise requests.ConnectionError("boom 123:SECRET")
            return FakeResponse(200)

    session = FlakySession([])
    TelegramSender("123:SECRET", ["111"], session=session).send("[ALERT 80] Drain\nmore")
    out = capsys.readouterr().out
    assert "sent to Telegram chat 111: [ALERT 80] Drain" in out
    assert "SECRET" not in out
