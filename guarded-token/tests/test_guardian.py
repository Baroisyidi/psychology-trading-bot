import os

import ape
import pytest
from nacl.signing import SigningKey

from conftest import TOKEN
from monitor.alerts import Alert
from monitor.chain import ChainMonitor
from monitor.guardian import Executor, Policy, Responder, local_test_key
from monitor.notify import ConsoleSender, Notifier
from monitor.signing import Signer

FULL_MODEL = os.path.join(os.path.dirname(__file__), "..", "models", "guard_model.joblib")


@pytest.fixture
def notifier():
    return Notifier(ConsoleSender(), Signer(SigningKey.generate()), log_path=None)


def make_responder(chain, token, notifier, **kw):
    ex = Executor(chain.provider.web3, token.address, local_test_key(1))
    return Responder(ex, notifier, mode=kw.pop("mode", "on"), protected=kw.pop("protected", None),
                     policy=Policy(**kw))


def feed(notifier, t, sender, related, score):
    notifier.handle(Alert(score=score, title="t", details=[], rules=["ml_fraud"], block=1,
                          timestamp=t, subject=sender, related=related, value=TOKEN))


def test_key_must_belong_to_the_guardian(chain, token, notifier):
    ex = Executor(chain.provider.web3, token.address, local_test_key(5))
    with pytest.raises(ValueError):
        Responder(ex, notifier)


def test_strong_incident_blocklists_and_tightens(chain, token, notifier, accounts, alice, bob):
    r = make_responder(chain, token, notifier)
    victim, mule = accounts[6], accounts[7]
    feed(notifier, 0, victim.address, [victim.address, mule.address], 95)
    r.review(notifier.tracker.open)
    assert token.blocklisted(victim) and token.blocklisted(mule)
    assert token.transfer_limit() == 1_000 * TOKEN
    assert "[ACTION] Incident #1" in notifier.sender.sent[-1]
    assert not token.blocklisted(alice)  # bystanders untouched
    token.transfer(bob, 10 * TOKEN, sender=alice)


def test_single_medium_alert_needs_corroboration(chain, token, notifier, accounts):
    r = make_responder(chain, token, notifier)
    w = accounts[6]
    feed(notifier, 0, w.address, [w.address], 75)
    r.review(notifier.tracker.open)
    assert not token.blocklisted(w)
    feed(notifier, 60, w.address, [w.address], 75)
    r.review(notifier.tracker.open)
    assert token.blocklisted(w)
    assert token.transfer_limit() == 10_000 * TOKEN  # only ALERT level: no tightening


def test_protected_and_admin_wallets_never_blocked(chain, token, notifier, accounts, admin):
    exchange = accounts[8]
    r = make_responder(chain, token, notifier, protected={exchange.address})
    feed(notifier, 0, exchange.address, [exchange.address, admin.address], 99)
    r.review(notifier.tracker.open)
    assert not token.blocklisted(exchange) and not token.blocklisted(admin)


def test_dry_run_changes_nothing(chain, token, notifier, accounts):
    r = make_responder(chain, token, notifier, mode="dry-run")
    w = accounts[6]
    feed(notifier, 0, w.address, [w.address], 99)
    r.review(notifier.tracker.open)
    assert not token.blocklisted(w) and token.transfer_limit() == 10_000 * TOKEN
    assert "DRY RUN" in notifier.sender.sent[-1]


def test_pause_when_attack_continues(chain, token, notifier, accounts):
    r = make_responder(chain, token, notifier)
    first = accounts[6]
    feed(notifier, 0, first.address, [first.address], 95)
    r.review(notifier.tracker.open)
    assert not token.paused()
    # Three more flagged transfers from wallets that were not blocklisted yet, same incident.
    for i, w in enumerate(accounts[10:13]):
        feed(notifier, 60 + i, w.address, [w.address, first.address], 95)
    r.review(notifier.tracker.open)
    assert token.paused()
    assert "PAUSED" in notifier.sender.sent[-1]


def test_blocklist_budget_escalates_to_pause(chain, token, notifier, accounts):
    r = make_responder(chain, token, notifier, max_blocks_per_hour=2)
    wallets = [a.address for a in accounts[10:15]]
    feed(notifier, 0, wallets[0], wallets, 95)
    r.review(notifier.tracker.open)
    assert sum(token.blocklisted(w) for w in wallets) == 2
    assert token.paused()


def test_own_actions_are_not_realerted(chain, token, notifier, accounts):
    r = make_responder(chain, token, notifier)
    mon = ChainMonitor(chain.provider.web3, token.address, notifier, heartbeat_path=None,
                       heartbeat_every=0, responder=r)
    w = accounts[6]
    feed(notifier, chain.pending_timestamp, w.address, [w.address], 95)
    before = len(notifier.sender.sent)
    mon.poll_once()  # responder acts here
    mon.poll_once()  # sees the Blocklisted / LimitChanged events
    new = notifier.sender.sent[before:]
    assert len(new) == 1 and new[0].startswith("[ACTION]")


def test_failed_action_is_reported_not_fatal(chain, token, notifier, accounts):
    r = make_responder(chain, token, notifier)

    def boom(addr):
        raise RuntimeError("node unreachable")

    r.ex.block = boom
    feed(notifier, 0, accounts[6].address, [accounts[6].address], 95)
    r.review(notifier.tracker.open)
    assert notifier.sender.sent[-1].startswith("[ACTION FAILED]")


@pytest.mark.skipif(not os.path.exists(FULL_MODEL), reason="run `python -m ml.train` first")
def test_full_system_stops_a_fan_out(chain, token, accounts, admin, notifier):
    from monitor.ml_detector import GuardDetector

    victim, mules = accounts[5], accounts[10:20]
    r = make_responder(chain, token, notifier)
    mon = ChainMonitor(chain.provider.web3, token.address, notifier,
                       detector=GuardDetector.load(FULL_MODEL), heartbeat_path=None,
                       heartbeat_every=0, responder=r)
    token.mint(victim, 8_000 * TOKEN, sender=admin)
    chain.pending_timestamp += 3 * 86_400
    chain.mine()
    sent = 0
    for m in mules:
        chain.pending_timestamp += 180
        chain.mine()
        try:
            token.transfer(m, 400 * TOKEN, sender=victim)
            sent += 1
        except ape.exceptions.ContractLogicError:
            pass
        mon.poll_once()
    assert token.blocklisted(victim)
    assert sent < len(mules) // 2  # stopped well before the fan-out finished


@pytest.mark.skipif(not os.path.exists(FULL_MODEL), reason="run `python -m ml.train` first")
def test_restarted_monitor_does_not_blocklist_known_wallets(chain, token, accounts, notifier, alice, bob):
    """A monitor started after wallets were funded must learn the history first."""
    from monitor.ml_detector import GuardDetector

    thief = accounts[9]
    r = make_responder(chain, token, notifier)
    mon = ChainMonitor(chain.provider.web3, token.address, notifier,
                       detector=GuardDetector.load(FULL_MODEL), heartbeat_path=None,
                       heartbeat_every=0, responder=r)
    token.mint(bob, 1_000 * TOKEN, sender=accounts[0])  # bob funded BEFORE the monitor goes live
    mon.warm_up(0)
    assert notifier.sender.sent == []  # history is read silently
    token.transfer(bob, 50 * TOKEN, sender=alice)  # ordinary payment to a known wallet
    mon.poll_once()
    token.transfer(thief, 9_000 * TOKEN, sender=alice)
    mon.poll_once()
    token.transfer(accounts[10], 4_000 * TOKEN, sender=thief)
    mon.poll_once()
    assert token.blocklisted(thief)
    assert not token.blocklisted(bob)
