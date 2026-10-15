import pytest

TOKEN = 10**18  # 1 token with 18 decimals
MAX_SUPPLY = 1_000_000 * TOKEN
HOURLY_LIMIT = 10_000 * TOKEN


@pytest.fixture(scope="session")
def admin(accounts):
    return accounts[0]


@pytest.fixture(scope="session")
def guardian(accounts):
    return accounts[1]


@pytest.fixture(scope="session")
def alice(accounts):
    return accounts[2]


@pytest.fixture(scope="session")
def bob(accounts):
    return accounts[3]


@pytest.fixture(scope="session")
def attacker(accounts):
    return accounts[4]


@pytest.fixture
def token(project, admin, guardian, alice):
    t = admin.deploy(
        project.GuardedToken,
        "Guarded Test Token",
        "GTT",
        MAX_SUPPLY,
        admin,
        guardian,
        HOURLY_LIMIT,
    )
    # Give Alice some tokens to work with.
    t.mint(alice, 50_000 * TOKEN, sender=admin)
    return t
