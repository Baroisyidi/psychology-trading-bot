"""
Ed25519 signing for alerts, so a forged "all clear" message can be spotted.

Every message ends with a line `sig: <hex>`: an Ed25519 signature over all
the text above that line. Anyone with the monitor's public key can check it
with:  python -m monitor.verify
"""

from __future__ import annotations

import os
from pathlib import Path

from nacl.exceptions import BadSignatureError
from nacl.signing import SigningKey, VerifyKey

SIG_PREFIX = "sig: "


class Signer:
    def __init__(self, signing_key: SigningKey):
        self._key = signing_key

    @classmethod
    def from_file(cls, path: str | Path) -> "Signer":
        """Load the key, creating a new one (readable only by you) if missing."""
        path = Path(path)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            key = SigningKey.generate()
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(key.encode().hex())
            return cls(key)
        return cls(SigningKey(bytes.fromhex(path.read_text().strip())))

    @property
    def public_key_hex(self) -> str:
        return self._key.verify_key.encode().hex()

    def sign_hex(self, data: bytes) -> str:
        """Raw Ed25519 signature over bytes, hex-encoded (used for heartbeats)."""
        return self._key.sign(data).signature.hex()

    def sign_text(self, body: str) -> str:
        """Return the body with a signature line appended."""
        sig = self._key.sign(body.encode()).signature.hex()
        return f"{body}\n{SIG_PREFIX}{sig}"


def verify_hex(data: bytes, sig_hex: str, public_key_hex: str) -> bool:
    try:
        VerifyKey(bytes.fromhex(public_key_hex)).verify(data, bytes.fromhex(sig_hex))
        return True
    except (BadSignatureError, ValueError):
        return False


def verify_text(signed: str, public_key_hex: str) -> bool:
    """True if the last line is a valid signature over the rest."""
    signed = signed.strip().replace("\r\n", "\n")
    body, sep, last = signed.rpartition("\n")
    if not sep or not last.startswith(SIG_PREFIX):
        return False
    try:
        sig = bytes.fromhex(last[len(SIG_PREFIX):].strip())
        VerifyKey(bytes.fromhex(public_key_hex)).verify(body.encode(), sig)
        return True
    except (BadSignatureError, ValueError):
        return False
