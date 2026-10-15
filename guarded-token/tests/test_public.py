"""Tests for the public-network pieces: remote watchdog, key files, deploy/admin
helpers and the monitor's adaptive log queries."""

import json
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest
from nacl.signing import SigningKey

from conftest import TOKEN
from monitor.chain import ChainMonitor
from monitor.notify import ConsoleSender, Notifier
from monitor.signing import Signer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "render_watchdog"))
from app import Watchdog, make_handler  # noqa: E402


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def beat(signer, ts, block=100, token="0xToken", sig=None):
    body = json.dumps({"time": int(ts), "block": block, "token": token}, sort_keys=True)
    return {"body": body, "sig": sig or signer.sign_hex(body.encode())}


@pytest.fixture
def dog_parts():
    signer = Signer(SigningKey.generate())
    clock, said = Clock(), []
    dog = Watchdog(signer.public_key_hex, said.append, stale_after=120, stall_after=600, clock=clock)
    return signer, clock, said, dog


# --- remote watchdog --------------------------------------------------------
def test_watchdog_online_down_recovered(dog_parts):
    signer, clock, said, dog = dog_parts
    assert dog.receive(beat(signer, clock()))[0] == 200
    assert said[-1].startswith("[ONLINE]")
    clock.t += 100
    dog.check()
    assert len(said) == 1  # not stale yet
    clock.t += 30
    dog.check()
    dog.check()  # only one alert, not one per check
    assert said[-1].startswith("[MONITOR DOWN]") and len(said) == 2
    assert dog.status()["healthy"] is False
    assert dog.receive(beat(signer, clock(), block=101))[0] == 200
    assert said[-1].startswith("[RECOVERED]") and dog.status()["healthy"] is True


def test_watchdog_alerts_if_no_heartbeat_ever_arrives(dog_parts):
    _, clock, said, dog = dog_parts
    clock.t += 121
    dog.check()
    assert "since this watchdog started" in said[-1]


def test_watchdog_rejects_forged_replayed_and_old_heartbeats(dog_parts):
    signer, clock, said, dog = dog_parts
    impostor = Signer(SigningKey.generate())
    assert dog.receive(beat(impostor, clock()))[0] == 403  # wrong key
    good = beat(signer, clock())
    tampered = dict(good, body=good["body"].replace("100", "999"))
    assert dog.receive(tampered)[0] == 403
    assert dog.receive({"nonsense": 1})[0] == 403
    assert dog.receive(beat(signer, clock() - 3600))[0] == 403  # too old
    assert dog.receive(good)[0] == 200
    assert dog.receive(good)[0] == 409  # replay
    assert dog.status()["rejected"] == 5
    # an attacker replaying old heartbeats cannot keep the watchdog quiet
    clock.t += 200
    dog.receive(good)
    dog.check()
    assert said[-1].startswith("[MONITOR DOWN]")


def test_watchdog_warns_when_blocks_stop_moving(dog_parts):
    signer, clock, said, dog = dog_parts
    for _ in range(25):  # alive every 30s, but always block 100
        dog.receive(beat(signer, clock(), block=100))
        clock.t += 30
        dog.check()
    assert any(m.startswith("[WARNING]") and "stuck at block 100" in m for m in said)
    dog.receive(beat(signer, clock(), block=101))
    assert said[-1].startswith("[RECOVERED]") and "new blocks" in said[-1]


def test_monitor_pushes_heartbeats_to_remote_watchdog(chain, token, tmp_path):
    signer = Signer(SigningKey.generate())
    said = []
    dog = Watchdog(signer.public_key_hex, said.append)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(dog))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/heartbeat"
        mon = ChainMonitor(chain.provider.web3, token.address,
                           Notifier(ConsoleSender(), signer, log_path=tmp_path / "a.jsonl"),
                           heartbeat_path=None, heartbeat_every=0, remote_watchdog_url=url)
        mon.poll_once()
        mon._maybe_push_heartbeat()
        status = dog.status()
        assert status["healthy"] and status["token"] == token.address
        assert status["block"] == mon.last_block
        assert said[-1].startswith("[ONLINE]")
    finally:
        server.shutdown()


# --- monitor on a public RPC --------------------------------------------------
def test_log_range_shrinks_when_provider_refuses(chain, token, alice, accounts, tmp_path, monkeypatch):
    w3 = chain.provider.web3
    mon = ChainMonitor(w3, token.address,
                       Notifier(ConsoleSender(), Signer(SigningKey.generate()), log_path=tmp_path / "a.jsonl"),
                       heartbeat_path=None, heartbeat_every=0)
    mon.poll_once()
    for i in range(12):
        token.transfer(accounts[10 + i], 10 * TOKEN, sender=alice)
    real = w3.eth.get_logs
    asked = []

    def picky(params):  # like a free RPC plan: at most 5 blocks per query
        asked.append(params["toBlock"] - params["fromBlock"] + 1)
        if asked[-1] > 5:
            raise ValueError("query exceeds max block range 5")
        return real(params)

    monkeypatch.setattr(w3.eth, "get_logs", picky)
    observed = []
    real_observe = mon.detector.observe
    mon.detector.observe = lambda name, *a: (observed.append(name), real_observe(name, *a))[1]
    mon.poll_once()  # one poll catches up, in small pieces
    assert mon.last_block == chain.blocks.head.number
    assert observed.count("Transfer") == 12  # and no transfer was skipped
    assert mon.block_range <= 5
    assert asked[:2] == [12, 6] and max(asked[2:]) <= 5  # refused twice, then small queries only


# --- key files ----------------------------------------------------------------
def test_keystore_round_trip(tmp_path, monkeypatch):
    from tools import keystore

    monkeypatch.setattr(keystore, "KEYS_DIR", tmp_path)
    monkeypatch.setenv("GUARDIAN_KEYSTORE_PASSWORD", "correct horse")
    key = "0x" + "11" * 32
    addr = keystore.save("guardian", key)
    raw = (tmp_path / "guardian.json").read_text()
    assert "11" * 32 not in raw  # the key is encrypted, not stored in plain text
    assert keystore.address_of("guardian") == addr
    assert keystore.load_key("guardian") in (key, key[2:])
    with pytest.raises(SystemExit):
        keystore.save("guardian", key)  # never silently overwrite a wallet
    with pytest.raises(SystemExit, match="Wrong password"):
        keystore.load_key("guardian", password="wrong password")


# --- deploy / admin helpers -----------------------------------------------------
def test_refuses_main_networks():
    from tools.chain import check_network

    for cid in (1, 137, 8453):
        with pytest.raises(SystemExit, match="MAIN network"):
            check_network(SimpleNamespace(eth=SimpleNamespace(chain_id=cid)))
    assert check_network(SimpleNamespace(eth=SimpleNamespace(chain_id=11155111))) == "Sepolia"


def test_revert_reason_is_readable():
    from tools.chain import revert_reason

    ganache = ValueError("execution reverted: VM Exception while processing transaction: revert timelock active",
                         {"reason": "timelock active"})
    assert revert_reason(ganache) == "timelock active"
    geth = Exception("execution reverted: not paused")
    assert revert_reason(geth) == "not paused"


def test_public_deploy_helper(chain, accounts, tmp_path, monkeypatch):
    from tools import chain as tchain
    from tools.deploy_public import deploy

    w3 = chain.provider.web3
    admin, guardian = accounts[5], accounts[6]
    info = deploy(w3, admin.private_key, guardian.address, max_supply=1_000, limit=100, mint=500)
    t = tchain.token_contract(w3, info["token"])
    assert t.functions.admin().call() == admin.address
    assert t.functions.guardian().call() == guardian.address
    assert t.functions.balanceOf(admin.address).call() == 500 * TOKEN
    assert t.functions.transfer_limit().call() == 100 * TOKEN
    monkeypatch.setattr(tchain, "DATA_DIR", tmp_path)
    tchain.save_deployment(info)
    assert (tmp_path / "token_address").read_text() == info["token"]
    assert int((tmp_path / "start_block").read_text()) == info["block"]
    assert tchain.load_deployment()["guardian"] == guardian.address


def test_idle_monitor_makes_few_rpc_calls(chain, token, admin, tmp_path):
    """Free RPC plans are metered: an idle block must cost one eth_blockNumber + one eth_getLogs."""
    w3 = chain.provider.web3
    mon = ChainMonitor(w3, token.address,
                       Notifier(ConsoleSender(), Signer(SigningKey.generate()), log_path=tmp_path / "a.jsonl"),
                       heartbeat_path=tmp_path / "hb.json", heartbeat_every=0, threat_path=tmp_path / "t.json")
    mon.poll_once()
    calls = []
    real = w3.manager._make_request

    def counting(method, params, *a, **k):
        calls.append(method)
        return real(method, params, *a, **k)

    w3.manager._make_request = counting
    try:
        for _ in range(5):
            chain.mine()
            mon.poll_once()
        mon.poll_once()  # nothing new: only the block number
    finally:
        del w3.manager._make_request
    assert calls.count("eth_blockNumber") == 6
    assert calls.count("eth_getLogs") == 5
    assert len(calls) == 11, calls
    # the pause state still follows the chain, via events
    token.pause(sender=admin)
    mon.poll_once()
    assert json.loads((tmp_path / "t.json").read_text())["paused"] is True
    token.unpause(sender=admin)
    mon.poll_once()
    assert json.loads((tmp_path / "t.json").read_text())["paused"] is False


def test_rate_limit_waits_instead_of_shrinking(chain, token, alice, accounts, tmp_path, monkeypatch):
    import monitor.chain as mchain

    w3 = chain.provider.web3
    mon = ChainMonitor(w3, token.address,
                       Notifier(ConsoleSender(), Signer(SigningKey.generate()), log_path=tmp_path / "a.jsonl"),
                       heartbeat_path=None, heartbeat_every=0)
    mon.poll_once()
    token.transfer(accounts[10], 10 * TOKEN, sender=alice)
    real, fails, slept = w3.eth.get_logs, [2], []

    def busy(params):
        if fails[0]:
            fails[0] -= 1
            raise RuntimeError("429 Client Error: Too Many Requests: exceeded its compute units per second capacity")
        return real(params)

    monkeypatch.setattr(w3.eth, "get_logs", busy)
    monkeypatch.setattr(mchain.time, "sleep", slept.append)
    before = mon.block_range
    mon.poll_once()
    assert mon.block_range == before  # not mistaken for "range too big"
    assert slept == [2, 4] and mon.last_block == chain.blocks.head.number


def test_watchdog_down_message_hints_at_wrong_key(dog_parts):
    _, clock, said, dog = dog_parts
    impostor = Signer(SigningKey.generate())  # e.g. Render configured with an old key
    dog.receive(beat(impostor, clock()))
    clock.t += 121
    dog.check()
    assert "MONITOR_PUBLIC_KEY" in said[-1]


# --- secrets never reach logs --------------------------------------------------
def test_redact_hides_rpc_keys_and_bot_tokens():
    from monitor.redact import redact

    text = ("HTTPSConnectionPool(host='eth-sepolia.g.alchemy.com'): Max retries exceeded with url: "
            "/v2/alch_FAKEexampleKEY_0000 (Caused by NameResolutionError) and "
            "https://mainnet.infura.io/v3/0123456789abcdef0123456789abcdef and "
            "https://api.telegram.org/bot123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw/sendMessage")
    out = redact(text)
    for secret in ("FAKEexampleKEY", "0123456789abcdef0123", "AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw"):
        assert secret not in out
    assert "/v2/***" in out and "/v3/***" in out and "bot***" in out
    assert "eth-sepolia.g.alchemy.com" in out  # the useful part stays


def test_network_errors_do_not_shrink_the_log_range(chain, token, tmp_path, monkeypatch):
    import requests

    w3 = chain.provider.web3
    mon = ChainMonitor(w3, token.address,
                       Notifier(ConsoleSender(), Signer(SigningKey.generate()), log_path=tmp_path / "a.jsonl"),
                       heartbeat_path=None, heartbeat_every=0)
    mon.poll_once()
    chain.mine(3)

    def offline(params):
        raise requests.ConnectionError("Failed to resolve 'eth-sepolia.g.alchemy.com' (name resolution)")

    monkeypatch.setattr(w3.eth, "get_logs", offline)
    before = mon.block_range
    with pytest.raises(requests.ConnectionError):
        mon.poll_once()
    assert mon.block_range == before  # DNS outage is not "range too big"


def test_execute_limit_says_how_long_to_wait_before_asking_password(chain, token, admin, monkeypatch):
    from tools import admin as tadmin

    w3 = chain.provider.web3
    f = w3.eth.contract(address=token.address, abi=token.contract_type.model_dump()["abi"]).functions
    with pytest.raises(SystemExit, match="Nothing to execute"):
        tadmin.precheck(w3, f, "execute-limit", [])
    token.propose_limit(5_000 * TOKEN, sender=admin)
    chain.mine()
    with pytest.raises(SystemExit, match=r"Not yet: the 48-hour timelock ends in 4[78]h"):
        tadmin.precheck(w3, f, "execute-limit", [])
    with pytest.raises(SystemExit, match="not paused"):
        tadmin.precheck(w3, f, "unpause", [])


def test_watchdog_attached_to_an_existing_aiohttp_bot(monkeypatch):
    """The way it runs inside a Telegram bot on Render: same aiohttp app, async send."""
    import asyncio
    import time as _time

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from app import attach_guard

    signer = Signer(SigningKey.generate())
    monkeypatch.setenv("MONITOR_PUBLIC_KEY", signer.public_key_hex)
    sent = []

    async def send_message(text):  # stands in for app.bot.send_message(chat_id=..., text=...)
        sent.append(text)

    async def scenario():
        bot_app = web.Application()

        async def webhook(request):  # the bot's own route keeps working
            return web.Response(text="OK")

        bot_app.router.add_post("/webhook", webhook)
        dog = attach_guard(bot_app, send_message)
        async with TestClient(TestServer(bot_app)) as client:
            assert (await client.post("/webhook", json={})).status == 200
            ok = await client.post("/heartbeat", json=beat(signer, _time.time()))
            assert ok.status == 200
            bad = await client.post("/heartbeat", json=beat(Signer(SigningKey.generate()), _time.time() + 1))
            assert bad.status == 403
            assert (await client.post("/heartbeat", data="nope")).status == 400
            status = await (await client.get("/guard")).json()
            assert status["healthy"] and status["rejected"] == 1
            await asyncio.sleep(0.05)  # let the alert task run
            assert sent and sent[0].startswith("[ONLINE]")
            dog.last_seen -= 500  # pretend the monitor went silent
            dog.check()
            await asyncio.sleep(0.05)
            assert sent[-1].startswith("[MONITOR DOWN]")

    asyncio.run(scenario())


def test_attach_guard_is_off_without_a_key(monkeypatch):
    from aiohttp import web
    from app import attach_guard

    monkeypatch.delenv("MONITOR_PUBLIC_KEY", raising=False)
    bot_app = web.Application()
    assert attach_guard(bot_app, None) is None
    assert not [r for r in bot_app.router.routes() if r.resource.canonical == "/heartbeat"]
