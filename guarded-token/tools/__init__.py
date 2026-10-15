"""Tools for running the token on a public test network (Sepolia)."""

import os
import re

from monitor.redact import install as _hide_secrets

_hide_secrets()  # uncaught errors from any tool never print RPC keys or bot tokens

_DOCKER_NAMES = {"keys": "keys", "deploy_public": "deploy-public", "admin": "admin", "sepolia_drill": "drill"}
_DOCKER_PREFIX = "docker compose -f docker-compose.sepolia.yml run --rm tools "


def cmd(text: str) -> str:
    """Show commands the way the user runs them: `python -m tools.admin ...` on the
    command line, or the docker compose equivalent inside the Sepolia containers."""
    if not os.getenv("GT_DOCKER"):
        return text
    return re.sub(r"python -m tools\.(\w+)", lambda m: _DOCKER_PREFIX + _DOCKER_NAMES.get(m[1], m[1]), text)
