from nacl.signing import SigningKey

from monitor.alerts import Alert
from monitor.incidents import IncidentTracker
from monitor.notify import ConsoleSender, Notifier
from monitor.signing import Signer, verify_text

VICTIM, M1, M2, M3, COLLECTOR, EXCHANGE, OTHER = (f"0x{i:040x}" for i in range(1, 8))
GTT = 10**18
MIN = 60


def alert(t, sender, related, score=95, value=400):
    return Alert(score=score, title="Transfer matches attack patterns", details=["why: test"],
                 rules=["ml_fraud"], block=1, timestamp=t, subject=sender,
                 related=related, value=value * GTT)


def test_one_attack_is_one_incident():
    tr = IncidentTracker()
    assert "Incident #1 opened" in tr.add(alert(0, VICTIM, [VICTIM, M1]))
    assert tr.add(alert(3 * MIN, VICTIM, [VICTIM, M2])) is None  # grouped silently
    msg = tr.add(alert(11 * MIN, VICTIM, [VICTIM, M3]))  # 10 minutes passed: update
    assert "Incident #1 update" in msg and "3 suspicious transfer(s)" in msg
    # The mules later forward to a collector: still the same incident.
    assert tr.add(alert(15 * MIN, M1, [M1, COLLECTOR])) is None
    assert len(tr.open) == 1 and tr.open[0].wallets == {VICTIM, M1, M2, M3, COLLECTOR}


def test_severity_rise_triggers_update():
    tr = IncidentTracker()
    tr.add(alert(0, VICTIM, [VICTIM], score=50))
    msg = tr.add(alert(60, VICTIM, [VICTIM], score=75))
    assert msg.startswith("[ALERT 75] Incident #1 update (severity raised)")


def test_close_then_reopen_then_expire():
    tr = IncidentTracker()
    tr.add(alert(0, VICTIM, [VICTIM, M1]))
    assert tr.tick(1 * 3600) == []
    (closed,) = tr.tick(2 * 3600 + 1)
    assert closed.startswith("[CLOSED] Incident #1")
    msg = tr.add(alert(5 * 3600, M1, [M1, COLLECTOR]))  # patient attacker comes back
    assert "Incident #1 REOPENED" in msg
    tr.tick(8 * 3600)
    msg = tr.add(alert(40 * 3600, M1, [M1]))  # more than a day later: a new incident
    assert "Incident #2 opened" in msg


def test_exchange_does_not_glue_separate_attacks():
    # Exchanges are established wallets, so detectors never list them in `related`.
    tr = IncidentTracker()
    tr.add(alert(0, VICTIM, [VICTIM]))
    tr.add(alert(60, OTHER, [OTHER]))
    assert len(tr.open) == 2


def test_alert_linking_two_incidents_merges_them():
    tr = IncidentTracker()
    tr.add(alert(0, VICTIM, [VICTIM, M1]))
    tr.add(alert(60, OTHER, [OTHER, M2]))
    tr.add(alert(120, M1, [M1, M2]))
    assert len(tr.open) == 1
    msg = tr.add(alert(20 * MIN, M1, [M1]))
    assert "merged incident(s): #2" in msg


def test_notifier_groups_transfers_but_not_admin_events(tmp_path):
    sender, signer = ConsoleSender(), Signer(SigningKey.generate())
    n = Notifier(sender, signer, log_path=tmp_path / "a.jsonl")
    assert n.handle(alert(0, VICTIM, [VICTIM, M1]))
    assert not n.handle(alert(60, VICTIM, [VICTIM, M2]))
    pause = Alert(score=92, title="Token paused", details=[], rules=["admin_event:Paused"], block=2, timestamp=90)
    assert n.handle(pause)  # admin events always go straight out
    assert n.open_incidents == 1
    n.tick(3 * 3600)
    assert sender.sent[-1].startswith("[CLOSED] Incident #1")
    assert all(verify_text(m, signer.public_key_hex) for m in sender.sent)
    logged = (tmp_path / "a.jsonl").read_text().splitlines()
    assert len(logged) == 3  # every alert is logged, grouped or not
    assert '"incident": 1' in logged[1]
