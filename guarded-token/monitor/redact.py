"""
Keep secrets out of logs and error messages.

RPC URLs carry the provider key in their path (Alchemy .../v2/KEY, Infura
.../v3/KEY) and Telegram URLs carry the bot token (.../botTOKEN/...). Python
errors happily print full URLs, so everything that prints an error passes it
through redact() first, and install() makes uncaught errors do the same.
"""

from __future__ import annotations

import re
import sys
import traceback

_PATTERNS = [
    (re.compile(r"(/v[23]/)[A-Za-z0-9_\-]{8,}"), r"\1***"),           # Alchemy / Infura keys
    (re.compile(r"(\bbot)\d{5,}:[A-Za-z0-9_\-]{20,}"), r"\1***"),       # Telegram bot token
    (re.compile(r"\balch_[A-Za-z0-9_\-]{8,}"), "alch_***"),             # bare Alchemy key
    (re.compile(r"(api[_-]?key=)[A-Za-z0-9_\-]{8,}", re.I), r"\1***"),  # ?apikey=...
]


def redact(text: object) -> str:
    s = str(text)
    for pattern, repl in _PATTERNS:
        s = pattern.sub(repl, s)
    return s


def format_exc() -> str:
    return redact(traceback.format_exc())


def _hook(exc_type, exc, tb):
    if issubclass(exc_type, KeyboardInterrupt):
        return sys.__excepthook__(exc_type, exc, tb)
    sys.stderr.write(redact("".join(traceback.format_exception(exc_type, exc, tb))))


def install() -> None:
    """Uncaught errors are printed with secrets removed."""
    sys.excepthook = _hook
