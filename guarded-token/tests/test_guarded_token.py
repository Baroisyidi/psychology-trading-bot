import ape
from conftest import HOURLY_LIMIT, MAX_SUPPLY, TOKEN

DAY = 24 * 3600


def advance(chain, seconds):
    """Move the local chain's clock forward."""
    chain.pending_timestamp += seconds
    chain.mine()


# ------------------------------------------------------------- basic ERC-20


def test_metadata(token, admin, guardian):
    assert token.name() == "Guarded Test Token"
    assert token.symbol() == "GTT"
    assert token.decimals() == 18
    assert token.MAX_SUPPLY() == MAX_SUPPLY
    assert token.admin() == admin
    assert token.guardian() == guardian


def test_transfer(token, alice, bob):
    tx = token.transfer(bob, 100 * TOKEN, sender=alice)
    assert token.balanceOf(bob) == 100 * TOKEN
    assert token.balanceOf(alice) == 49_900 * TOKEN
    log = list(tx.decode_logs(token.Transfer))[0]
    assert log.sender == alice and log.receiver == bob and log.value == 100 * TOKEN


def test_transfer_insufficient_balance(token, bob, alice):
    with ape.reverts("insufficient balance"):
        token.transfer(alice, 1, sender=bob)


def test_approve_and_transfer_from(token, alice, bob):
    token.approve(bob, 500 * TOKEN, sender=alice)
    token.transferFrom(alice, bob, 200 * TOKEN, sender=bob)
    assert token.balanceOf(bob) == 200 * TOKEN
    assert token.allowance(alice, bob) == 300 * TOKEN
    with ape.reverts("insufficient allowance"):
        token.transferFrom(alice, bob, 400 * TOKEN, sender=bob)


# ------------------------------------------------------------- supply


def test_only_admin_mints(token, guardian, attacker):
    with ape.reverts("admin only"):
        token.mint(attacker, 1, sender=guardian)
    with ape.reverts("admin only"):
        token.mint(attacker, 1, sender=attacker)


def test_mint_cap(token, admin, bob):
    remaining = MAX_SUPPLY - token.totalSupply()
    token.mint(bob, remaining, sender=admin)
    assert token.totalSupply() == MAX_SUPPLY
    with ape.reverts("max supply exceeded"):
        token.mint(bob, 1, sender=admin)


def test_burn(token, alice):
    supply = token.totalSupply()
    token.burn(1_000 * TOKEN, sender=alice)
    assert token.totalSupply() == supply - 1_000 * TOKEN


# ------------------------------------------------------------- pause


def test_guardian_can_pause_but_not_unpause(token, guardian, admin, alice, bob):
    tx = token.pause(sender=guardian)
    assert token.paused()
    assert list(tx.decode_logs(token.Paused))[0].by == guardian

    with ape.reverts("paused"):
        token.transfer(bob, 1, sender=alice)
    with ape.reverts("paused"):
        token.mint(bob, 1, sender=admin)
    with ape.reverts("admin only"):
        token.unpause(sender=guardian)

    token.unpause(sender=admin)
    token.transfer(bob, 1, sender=alice)  # works again


def test_strangers_cannot_pause(token, attacker):
    with ape.reverts("admin or guardian only"):
        token.pause(sender=attacker)


# ------------------------------------------------------------- blocklist


def test_blocklist_stops_sending(token, guardian, admin, alice, attacker):
    token.transfer(attacker, 100 * TOKEN, sender=alice)
    token.block_address(attacker, sender=guardian)
    with ape.reverts("sender blocklisted"):
        token.transfer(alice, 1, sender=attacker)

    # Guardian cannot undo it; only the admin can.
    with ape.reverts("admin only"):
        token.unblock_address(attacker, sender=guardian)
    token.unblock_address(attacker, sender=admin)
    token.transfer(alice, 1, sender=attacker)


def test_blocklisted_spender_cannot_use_allowance(token, guardian, alice, attacker):
    token.approve(attacker, 100 * TOKEN, sender=alice)
    token.block_address(attacker, sender=guardian)
    with ape.reverts("spender blocklisted"):
        token.transferFrom(alice, attacker, 100 * TOKEN, sender=attacker)


def test_guardian_cannot_block_admin(token, guardian, admin):
    with ape.reverts("cannot block admin"):
        token.block_address(admin, sender=guardian)


# ------------------------------------------------------------- hourly transfer limit


def test_hourly_limit(token, alice, bob, chain):
    token.transfer(bob, HOURLY_LIMIT - TOKEN, sender=alice)
    token.transfer(bob, TOKEN, sender=alice)  # exactly at the limit
    with ape.reverts("hourly limit exceeded"):
        token.transfer(bob, 1, sender=alice)

    advance(chain, 3601)  # new window
    token.transfer(bob, TOKEN, sender=alice)


def test_admin_exempt_from_limit(token, admin, bob):
    token.mint(admin, HOURLY_LIMIT * 3, sender=admin)
    token.transfer(bob, HOURLY_LIMIT * 3, sender=admin)
    assert token.balanceOf(bob) == HOURLY_LIMIT * 3


def test_guardian_can_only_tighten_limit(token, guardian, alice, bob):
    token.tighten_limit(100 * TOKEN, sender=guardian)
    assert token.transfer_limit() == 100 * TOKEN
    with ape.reverts("hourly limit exceeded"):
        token.transfer(bob, 101 * TOKEN, sender=alice)
    with ape.reverts("not tighter"):
        token.tighten_limit(1_000 * TOKEN, sender=guardian)
    with ape.reverts("use propose_limit to remove the limit"):
        token.tighten_limit(0, sender=guardian)


# ------------------------------------------------------------- timelocks


def test_raising_limit_needs_timelock(token, admin, guardian, chain):
    token.propose_limit(50_000 * TOKEN, sender=admin)
    with ape.reverts("timelock active"):
        token.execute_limit(sender=admin)
    advance(chain, 2 * DAY)
    token.execute_limit(sender=admin)
    assert token.transfer_limit() == 50_000 * TOKEN
    with ape.reverts("admin only"):
        token.propose_limit(0, sender=guardian)


def test_changing_guardian_needs_timelock(token, admin, bob, chain):
    token.propose_guardian(bob, sender=admin)
    advance(chain, DAY)
    with ape.reverts("timelock active"):
        token.execute_guardian(sender=admin)
    advance(chain, DAY)
    token.execute_guardian(sender=admin)
    assert token.guardian() == bob


def test_cancel_pending(token, admin, bob):
    token.propose_guardian(bob, sender=admin)
    token.cancel_pending(sender=admin)
    with ape.reverts("nothing pending"):
        token.execute_guardian(sender=admin)


def test_admin_handover(token, admin, bob, chain):
    token.propose_admin(bob, sender=admin)
    with ape.reverts("timelock active"):
        token.accept_admin(sender=bob)
    advance(chain, 2 * DAY)
    with ape.reverts("not pending admin"):
        token.accept_admin(sender=admin)
    token.accept_admin(sender=bob)
    assert token.admin() == bob


def test_revoke_stolen_guardian_instantly(token, admin, guardian):
    token.revoke_guardian(sender=admin)
    assert token.guardian() == "0x0000000000000000000000000000000000000000"
    with ape.reverts("admin or guardian only"):
        token.pause(sender=guardian)
