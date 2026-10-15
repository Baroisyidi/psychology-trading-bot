import os
from collections import defaultdict, deque

import pytest
from nacl.signing import SigningKey

from conftest import TOKEN
from ml.features import FEATURES, FeatureExtractor, explain
from ml.simulate import ATTACK_KINDS, LIMIT, ZERO, simulate
from monitor.chain import ChainMonitor
from monitor.config import Settings
from monitor.ml_detector import GuardDetector
from monitor.notify import ConsoleSender, Notifier
from monitor.rules import Detector
from monitor.signing import Signer

A, B, C, D = (f"0x{i:040x}" for i in range(1, 5))
FULL_MODEL = os.path.join(os.path.dirname(__file__), "..", "models", "guard_model.joblib")


def f(x, name):
    return x[FEATURES.index(name)]


# ------------------------------------------------------------- simulator


def test_simulation_is_deterministic_and_valid():
    a = simulate(5, days=4, n_users=80, incidents_per_kind=2)
    b = simulate(5, days=4, n_users=80, incidents_per_kind=2)
    assert [(e.t, e.sender, e.value) for e in a] == [(e.t, e.sender, e.value) for e in b]
    assert {e.kind for e in a if e.label} <= set(ATTACK_KINDS)
    bal, win = defaultdict(float), defaultdict(deque)
    for e in a:
        if e.sender != ZERO:
            bal[e.sender] -= e.value
            q = win[e.sender]
            q.append((e.t, e.value))
            while q[0][0] <= e.t - 3600:
                q.popleft()
            assert sum(v for _, v in q) <= LIMIT + 0.05  # contract limit respected
            assert bal[e.sender] >= -0.05  # no overdrafts
        bal[e.receiver] += e.value


# ------------------------------------------------------------- features


def test_features_use_only_the_past():
    fx = FeatureExtractor()
    assert fx.observe(0, ZERO, A, 1000) is None  # mint: no score, but state updated
    x, _, _ = fx.observe(100, A, B, 10)
    assert len(x) == len(FEATURES)
    assert f(x, "receiver_new") == 1 and f(x, "sender_new_recv_1h") == 1
    x, _, _ = fx.observe(200, A, C, 10)
    assert f(x, "sender_new_recv_1h") == 2 and f(x, "sender_tx_1h") == 2
    x, _, _ = fx.observe(300, A, B, 10)
    assert f(x, "receiver_new") == 0 and f(x, "repeat_pair_24h") > 0


def test_cycle_features():
    fx = FeatureExtractor()
    fx.observe(0, ZERO, A, 1000)
    fx.observe(10, A, B, 100)
    x, _, _ = fx.observe(20, B, A, 50)
    assert f(x, "reciprocal_24h") == 1
    fx.observe(30, B, C, 20)
    x, _, _ = fx.observe(40, C, A, 10)  # A -> B -> C -> A
    assert f(x, "two_hop_cycle_24h") == 1


def test_admin_sends_are_not_scored():
    fx = FeatureExtractor(admin=A)
    fx.observe(0, ZERO, A, 1000)
    assert fx.observe(10, A, B, 100) is None


def test_explanations_are_plain_english():
    fx = FeatureExtractor()
    fx.observe(0, ZERO, A, 5000)
    for i, r in enumerate([B, C, D]):
        x, _, _ = fx.observe(100 + i, A, r, 300)
    reasons = explain(x, {name: 0.0 for name in FEATURES})
    assert reasons and all(isinstance(r, str) for r in reasons)


# ------------------------------------------------------------- training + loading


@pytest.fixture(scope="session")
def quick_model(tmp_path_factory):
    from ml.train import main

    d = tmp_path_factory.mktemp("model")
    path = d / "m.joblib"
    main(["--quick", "--model", str(path), "--report", str(d / "r.md"), "--metrics", str(d / "m.json")])
    assert (d / "r.md").read_text().startswith("# Phase 3 detector scorecard")
    return path


def test_model_loads_and_scores(quick_model):
    det = GuardDetector.load(quick_model)
    fraud, anomaly = det.score([0.0] * len(FEATURES))
    assert 0 <= fraud <= 100 and 0 <= anomaly <= 100


def test_wrong_features_rejected(quick_model):
    import joblib

    bundle = joblib.load(quick_model)
    bundle["features"] = bundle["features"][:-1]
    with pytest.raises(ValueError):
        GuardDetector(bundle)


def test_config_falls_back_to_rules(tmp_path, monkeypatch, quick_model):
    monkeypatch.setenv("MODEL_PATH", str(tmp_path / "missing.joblib"))
    monkeypatch.setenv("DETECTOR", "auto")
    assert isinstance(Settings.load().make_detector(), Detector)
    monkeypatch.setenv("DETECTOR", "rules")
    monkeypatch.setenv("MODEL_PATH", str(quick_model))
    assert isinstance(Settings.load().make_detector(), Detector)
    monkeypatch.setenv("DETECTOR", "auto")
    assert isinstance(Settings.load().make_detector(), GuardDetector)
    (tmp_path / "broken.joblib").write_text("not a model")
    monkeypatch.setenv("MODEL_PATH", str(tmp_path / "broken.joblib"))
    assert isinstance(Settings.load().make_detector(), Detector)  # broken model: keep guarding


# ------------------------------------------------------------- live monitor


def make_monitor(chain, token, detector, tmp_path):
    notifier = Notifier(ConsoleSender(), Signer(SigningKey.generate()), log_path=tmp_path / "a.jsonl")
    return ChainMonitor(chain.provider.web3, token.address, notifier, detector=detector,
                        heartbeat_path=None, heartbeat_every=0)


def test_admin_events_still_use_rule_scores(chain, token, guardian, quick_model, tmp_path):
    mon = make_monitor(chain, token, GuardDetector.load(quick_model), tmp_path)
    token.pause(sender=guardian)
    (alert,) = mon.poll_once()
    assert alert.score == 92 and alert.rules == ["admin_event:Paused"]


def test_transfers_get_model_alerts(chain, token, alice, bob, quick_model, tmp_path):
    mon = make_monitor(chain, token, GuardDetector.load(quick_model), tmp_path)
    token.transfer(bob, 9_000 * TOKEN, sender=alice)
    for a in mon.poll_once():
        assert a.rules[0] in ("ml_fraud", "ml_anomaly")
        assert any(d.startswith("fraud model") for d in a.details)


@pytest.mark.skipif(not os.path.exists(FULL_MODEL), reason="run `python -m ml.train` first")
def test_full_model_catches_fan_out_the_rules_miss(chain, token, accounts, admin, tmp_path):
    victim, mules = accounts[5], accounts[10:20]
    ml_mon = make_monitor(chain, token, GuardDetector.load(FULL_MODEL), tmp_path)
    rules_mon = make_monitor(chain, token, Detector(), tmp_path)
    token.mint(victim, 8_000 * TOKEN, sender=admin)
    chain.pending_timestamp += 3 * 86_400  # victim has held the tokens for days
    chain.mine()
    ml_alerts, rule_alerts = [], []
    for m in mules:
        chain.pending_timestamp += 180
        chain.mine()
        token.transfer(m, 400 * TOKEN, sender=victim)
        ml_alerts += ml_mon.poll_once()
        rule_alerts += rules_mon.poll_once()
    assert max((a.score for a in rule_alerts if a.subject == victim.address), default=0) < 40
    assert max(a.score for a in ml_alerts if a.subject == victim.address) >= 70
