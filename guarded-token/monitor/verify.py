"""
Check that an alert really came from your monitor.

    python -m monitor.verify <public_key_hex>

Then paste the whole Telegram message (including the `sig:` line) and
press Ctrl+D (Ctrl+Z then Enter on Windows).
"""

import sys

from .signing import verify_text


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    print("Paste the alert, then Ctrl+D:")
    text = sys.stdin.read()
    if verify_text(text, sys.argv[1]):
        print("VALID: signed by this monitor's key.")
    else:
        print("INVALID: do not trust this message.")
        sys.exit(1)


if __name__ == "__main__":
    main()
