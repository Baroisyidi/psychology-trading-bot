# pragma version ~=0.4.3
# pragma evm-version shanghai
"""
@title  GuardedToken
@notice Learning-prototype ERC-20 token with emergency controls that an
        off-chain AI "guardian" service can trigger.
@dev    Two roles:
          admin    - full control (in a real launch: a 2-of-3 multisig).
          guardian - the AI security service. It can only take PROTECTIVE,
                     reversible actions: pause, blocklist, tighten the limit.
                     It can never mint, move funds, unpause or loosen limits.
        Loosening changes (new guardian, higher limit, new admin) wait behind
        a 48-hour timelock so they can be reviewed before they take effect.
        Not audited. Testnet / local chain only.
"""

from ethereum.ercs import IERC20

implements: IERC20


# ---------------------------------------------------------------- events

event Transfer:
    sender: indexed(address)
    receiver: indexed(address)
    value: uint256

event Approval:
    owner: indexed(address)
    spender: indexed(address)
    value: uint256

event Paused:
    by: indexed(address)

event Unpaused:
    by: indexed(address)

event Blocklisted:
    account: indexed(address)
    by: indexed(address)

event Unblocked:
    account: indexed(address)
    by: indexed(address)

event LimitChanged:
    old_limit: uint256
    new_limit: uint256
    by: indexed(address)

event GuardianProposed:
    new_guardian: indexed(address)
    eta: uint256

event GuardianChanged:
    old_guardian: indexed(address)
    new_guardian: indexed(address)

event GuardianRevoked:
    old_guardian: indexed(address)

event LimitProposed:
    new_limit: uint256
    eta: uint256

event AdminProposed:
    new_admin: indexed(address)
    eta: uint256

event AdminChanged:
    old_admin: indexed(address)
    new_admin: indexed(address)

event PendingCancelled:
    by: indexed(address)


# ---------------------------------------------------------------- ERC-20 state

name: public(String[32])
symbol: public(String[8])
decimals: public(constant(uint8)) = 18
totalSupply: public(uint256)
balanceOf: public(HashMap[address, uint256])
allowance: public(HashMap[address, HashMap[address, uint256]])

# Hard ceiling on supply, fixed forever at deployment.
MAX_SUPPLY: public(immutable(uint256))


# ---------------------------------------------------------------- security state

admin: public(address)
guardian: public(address)
paused: public(bool)
blocklisted: public(HashMap[address, bool])

# Max tokens any non-admin wallet may send per rolling window. 0 = no limit.
transfer_limit: public(uint256)
LIMIT_WINDOW: public(constant(uint256)) = 3600  # 1 hour
window_start: public(HashMap[address, uint256])
sent_in_window: public(HashMap[address, uint256])

# Timelock for loosening changes.
TIMELOCK_DELAY: public(constant(uint256)) = 48 * 3600  # 48 hours
pending_guardian: public(address)
pending_guardian_eta: public(uint256)
pending_limit: public(uint256)
pending_limit_eta: public(uint256)
pending_admin: public(address)
pending_admin_eta: public(uint256)


# ---------------------------------------------------------------- constructor

@deploy
def __init__(
    _name: String[32],
    _symbol: String[8],
    _max_supply: uint256,
    _admin: address,
    _guardian: address,
    _transfer_limit: uint256,
):
    assert _admin != empty(address), "admin required"
    assert _max_supply > 0, "max supply required"
    self.name = _name
    self.symbol = _symbol
    MAX_SUPPLY = _max_supply
    self.admin = _admin
    self.guardian = _guardian
    self.transfer_limit = _transfer_limit


# ---------------------------------------------------------------- internal helpers

@internal
def _only_admin():
    assert msg.sender == self.admin, "admin only"


@internal
def _only_protector():
    # Admin or guardian: used for protective actions only.
    assert msg.sender == self.admin or (
        msg.sender == self.guardian and self.guardian != empty(address)
    ), "admin or guardian only"


@internal
def _check_limit(sender: address, amount: uint256):
    # The admin is exempt so it can distribute tokens.
    if self.transfer_limit == 0 or sender == self.admin:
        return
    if block.timestamp >= self.window_start[sender] + LIMIT_WINDOW:
        self.window_start[sender] = block.timestamp
        self.sent_in_window[sender] = 0
    new_total: uint256 = self.sent_in_window[sender] + amount
    assert new_total <= self.transfer_limit, "hourly limit exceeded"
    self.sent_in_window[sender] = new_total


@internal
def _transfer(sender: address, receiver: address, amount: uint256):
    assert not self.paused, "paused"
    assert not self.blocklisted[sender], "sender blocklisted"
    assert receiver != empty(address), "zero receiver"
    assert self.balanceOf[sender] >= amount, "insufficient balance"
    self._check_limit(sender, amount)
    self.balanceOf[sender] -= amount
    self.balanceOf[receiver] += amount
    log Transfer(sender=sender, receiver=receiver, value=amount)


# ---------------------------------------------------------------- ERC-20

@external
def transfer(receiver: address, amount: uint256) -> bool:
    self._transfer(msg.sender, receiver, amount)
    return True


@external
def transferFrom(sender: address, receiver: address, amount: uint256) -> bool:
    assert not self.blocklisted[msg.sender], "spender blocklisted"
    allowed: uint256 = self.allowance[sender][msg.sender]
    if allowed != max_value(uint256):
        assert allowed >= amount, "insufficient allowance"
        self.allowance[sender][msg.sender] = allowed - amount
    self._transfer(sender, receiver, amount)
    return True


@external
def approve(spender: address, amount: uint256) -> bool:
    self.allowance[msg.sender][spender] = amount
    log Approval(owner=msg.sender, spender=spender, value=amount)
    return True


# ---------------------------------------------------------------- supply

@external
def mint(receiver: address, amount: uint256):
    self._only_admin()
    assert not self.paused, "paused"
    assert receiver != empty(address), "zero receiver"
    assert self.totalSupply + amount <= MAX_SUPPLY, "max supply exceeded"
    self.totalSupply += amount
    self.balanceOf[receiver] += amount
    log Transfer(sender=empty(address), receiver=receiver, value=amount)


@external
def burn(amount: uint256):
    assert not self.paused, "paused"
    assert self.balanceOf[msg.sender] >= amount, "insufficient balance"
    self.balanceOf[msg.sender] -= amount
    self.totalSupply -= amount
    log Transfer(sender=msg.sender, receiver=empty(address), value=amount)


# ---------------------------------------------------------------- protective actions (admin or guardian, instant)

@external
def pause():
    self._only_protector()
    assert not self.paused, "already paused"
    self.paused = True
    log Paused(by=msg.sender)


@external
def block_address(account: address):
    self._only_protector()
    assert account != self.admin, "cannot block admin"
    assert not self.blocklisted[account], "already blocklisted"
    self.blocklisted[account] = True
    log Blocklisted(account=account, by=msg.sender)


@external
def tighten_limit(new_limit: uint256):
    # Lowering the limit is protective, so it takes effect immediately.
    self._only_protector()
    assert new_limit > 0, "use propose_limit to remove the limit"
    old: uint256 = self.transfer_limit
    assert old == 0 or new_limit < old, "not tighter"
    self.transfer_limit = new_limit
    log LimitChanged(old_limit=old, new_limit=new_limit, by=msg.sender)


# ---------------------------------------------------------------- recovery actions (admin only, instant)

@external
def unpause():
    self._only_admin()
    assert self.paused, "not paused"
    self.paused = False
    log Unpaused(by=msg.sender)


@external
def unblock_address(account: address):
    self._only_admin()
    assert self.blocklisted[account], "not blocklisted"
    self.blocklisted[account] = False
    log Unblocked(account=account, by=msg.sender)


@external
def revoke_guardian():
    # Emergency: if the guardian key is stolen, cut it off immediately.
    self._only_admin()
    old: address = self.guardian
    assert old != empty(address), "no guardian"
    self.guardian = empty(address)
    log GuardianRevoked(old_guardian=old)


# ---------------------------------------------------------------- timelocked changes (admin only, 48h delay)

@external
def propose_guardian(new_guardian: address):
    self._only_admin()
    self.pending_guardian = new_guardian
    self.pending_guardian_eta = block.timestamp + TIMELOCK_DELAY
    log GuardianProposed(new_guardian=new_guardian, eta=self.pending_guardian_eta)


@external
def execute_guardian():
    self._only_admin()
    eta: uint256 = self.pending_guardian_eta
    assert eta != 0, "nothing pending"
    assert block.timestamp >= eta, "timelock active"
    old: address = self.guardian
    self.guardian = self.pending_guardian
    self.pending_guardian = empty(address)
    self.pending_guardian_eta = 0
    log GuardianChanged(old_guardian=old, new_guardian=self.guardian)


@external
def propose_limit(new_limit: uint256):
    self._only_admin()
    self.pending_limit = new_limit
    self.pending_limit_eta = block.timestamp + TIMELOCK_DELAY
    log LimitProposed(new_limit=new_limit, eta=self.pending_limit_eta)


@external
def execute_limit():
    self._only_admin()
    eta: uint256 = self.pending_limit_eta
    assert eta != 0, "nothing pending"
    assert block.timestamp >= eta, "timelock active"
    old: uint256 = self.transfer_limit
    self.transfer_limit = self.pending_limit
    self.pending_limit = 0
    self.pending_limit_eta = 0
    log LimitChanged(old_limit=old, new_limit=self.transfer_limit, by=msg.sender)


@external
def propose_admin(new_admin: address):
    self._only_admin()
    assert new_admin != empty(address), "zero admin"
    self.pending_admin = new_admin
    self.pending_admin_eta = block.timestamp + TIMELOCK_DELAY
    log AdminProposed(new_admin=new_admin, eta=self.pending_admin_eta)


@external
def accept_admin():
    # The new admin must accept, proving the address is controlled.
    assert msg.sender == self.pending_admin, "not pending admin"
    eta: uint256 = self.pending_admin_eta
    assert block.timestamp >= eta, "timelock active"
    old: address = self.admin
    self.admin = msg.sender
    self.pending_admin = empty(address)
    self.pending_admin_eta = 0
    log AdminChanged(old_admin=old, new_admin=msg.sender)


@external
def cancel_pending():
    self._only_admin()
    self.pending_guardian = empty(address)
    self.pending_guardian_eta = 0
    self.pending_limit = 0
    self.pending_limit_eta = 0
    self.pending_admin = empty(address)
    self.pending_admin_eta = 0
    log PendingCancelled(by=msg.sender)
