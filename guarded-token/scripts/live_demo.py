"""
Phase 2 demo: the monitor watching a local chain while normal users and an
attacker act on it.

    ape run live_demo

If TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS are set (in .env), alerts go to
your Telegram chat. Otherwise they print here.
"""

from ape.logging import LogLevel, logger

logger.set_level(LogLevel.SUCCESS)  # QUIET: hide 'INFO: Confirmed ...' lines
from ape import accounts, chain, project

from monitor.chain import ChainMonitor
from monitor.config import Settings

TOKEN = 10**18


def wait(seconds):
    chain.pending_timestamp += seconds
    chain.mine()


def main():
    admin, guardian, alice, bob, carol, attacker, mule = accounts.test_accounts[:7]

    token = admin.deploy(
        project.GuardedToken,
        "Guarded Test Token",
        "GTT",
        1_000_000 * TOKEN,
        admin,
        guardian,
        10_000 * TOKEN,
    )

    settings = Settings.load()
    monitor = ChainMonitor(
        chain.provider.web3,
        token.address,
        settings.make_notifier(),
        detector=settings.make_detector(),
        heartbeat_path=settings.heartbeat_path,
        heartbeat_every=0,  # heartbeats are for the long-running monitor
    )
    monitor.notifier.send_status(f"[ONLINE] Demo monitor watching {token.address}")

    def step(title):
        print(f"\n######## {title}")

    def poll():
        alerts = monitor.poll_once()
        quiet = [a for a in alerts if a.score < monitor.notifier.min_score]
        if quiet:
            print(f"  ({len(quiet)} low-score event(s) logged, not sent)")

    step("Admin distributes tokens (expected, no alerts)")
    for user, amount in [(alice, 40_000), (bob, 20_000), (carol, 15_000)]:
        token.mint(user, amount * TOKEN, sender=admin)
    poll()

    step("Normal users trade small amounts, minutes apart (no alerts)")
    for sender, receiver, amount in [(alice, bob, 120), (bob, carol, 75), (carol, alice, 300)]:
        wait(240)
        token.transfer(receiver, amount * TOKEN, sender=sender)
    poll()

    step("Attack: a compromised wallet funds a fresh address, which dumps fast")
    wait(300)
    token.transfer(attacker, 9_500 * TOKEN, sender=alice)  # alice's key is stolen
    poll()
    wait(20)
    token.transfer(mule, 4_000 * TOKEN, sender=attacker)
    poll()
    wait(10)
    token.transfer(mule, 3_000 * TOKEN, sender=attacker)
    poll()
    wait(10)
    token.transfer(mule, 1_500 * TOKEN, sender=attacker)
    poll()

    step("Guardian responds: tighten limit, blocklist, pause")
    token.tighten_limit(1_000 * TOKEN, sender=guardian)
    token.block_address(attacker, sender=guardian)
    token.pause(sender=guardian)
    poll()

    step("Admin recovers after review")
    wait(1800)
    token.unpause(sender=admin)
    poll()

    step("Someone proposes removing the transfer limit")
    token.propose_limit(0, sender=admin)
    poll()

    print(f"\nEvery alert (sent or grouped) is logged in {settings.alert_log_path}")
