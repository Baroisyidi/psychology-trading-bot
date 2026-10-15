"""
Remote watchdog: a tiny web service (made for Render) that tells you on
Telegram when your monitor goes quiet.

The local watchdog runs on the same computer as the monitor, so it cannot
warn you if that whole computer switches off or loses internet. This one
lives elsewhere. The monitor POSTs a signed heartbeat every 30 seconds; if
none arrives for STALE_SECONDS, or the block number stops moving, you get
an alert, and another when things recover.

Heartbeats are signed with the monitor's Ed25519 key, so nobody else can
keep the watchdog quiet by sending fake "I'm alive" messages.

Only the Python standard library + PyNaCl. Environment variables:
  TELEGRAM_BOT_TOKEN   the same bot token the monitor uses
  TELEGRAM_CHAT_IDS    comma-separated chat ids
  MONITOR_PUBLIC_KEY   printed by the monitor at startup ("Alert signing public key")
  STALE_SECONDS        default 120
  STALL_SECONDS        default 600 (block number not moving)
  PORT                 set by Render
This service only SENDS messages with the bot; it never touches your bot's
webhook, so your existing bot keeps working.
"""

from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

MAX_BODY = 2048
MAX_CLOCK_SKEW = 300  # seconds


def short(addr: str) -> str:
    return f"{addr[:6]}...{addr[-4:]}" if addr and len(addr) > 12 else str(addr)


class Watchdog:
    def __init__(self, public_key_hex: str, send, stale_after: int = 120, stall_after: int = 600,
                 clock=time.time):
        self.key = VerifyKey(bytes.fromhex(public_key_hex))
        self.send = send
        self.stale_after = stale_after
        self.stall_after = stall_after
        self.clock = clock
        self.started = clock()
        self.lock = threading.Lock()
        self.last_seen: float | None = None   # when we received the last good heartbeat
        self.last_time = 0                    # the monitor's own timestamp in it (replay guard)
        self.block = None
        self.block_changed = clock()
        self.token = ""
        self.down = False      # alert sent: no heartbeats
        self.stalled = False   # alert sent: block number not moving
        self.rejected = 0

    # --- heartbeats -----------------------------------------------------
    def receive(self, payload: dict) -> tuple[int, str]:
        """Check one heartbeat. Returns (HTTP status, reason)."""
        try:
            body, sig = payload["body"], payload["sig"]
            self.key.verify(body.encode(), bytes.fromhex(sig))
            data = json.loads(body)
            ts, block, token = int(data["time"]), data.get("block"), str(data.get("token", ""))
        except (KeyError, TypeError, ValueError, BadSignatureError):
            self.rejected += 1
            return 403, "bad signature or format"
        now = self.clock()
        with self.lock:
            if abs(now - ts) > MAX_CLOCK_SKEW:
                self.rejected += 1
                return 403, "timestamp too far from now"
            if ts <= self.last_time:
                self.rejected += 1
                return 409, "replayed or out-of-order heartbeat"
            first = self.last_seen is None
            self.last_time, self.last_seen, self.token = ts, now, token
            if block != self.block:
                self.block, self.block_changed = block, now
                if self.stalled:
                    self.stalled = False
                    self._say(f"[RECOVERED] Monitor is seeing new blocks again (block {block}).")
            if self.down:
                self.down = False
                self._say(f"[RECOVERED] Remote watchdog: monitor heartbeats are back.\n"
                          f"Token {short(token)} | block {block}")
            elif first:
                self._say(f"[ONLINE] Remote watchdog receiving heartbeats.\nToken {short(token)} | block {block}")
        return 200, "ok"

    # --- periodic check -------------------------------------------------
    def check(self) -> None:
        now = self.clock()
        with self.lock:
            since = self.last_seen if self.last_seen is not None else self.started
            if not self.down and now - since > self.stale_after:
                self.down = True
                what = ("No heartbeat since this watchdog started"
                        if self.last_seen is None else f"No heartbeat for {int(now - since)}s")
                hint = (f"\n{self.rejected} heartbeat(s) were rejected: if the monitor is running, check that "
                        "MONITOR_PUBLIC_KEY matches the key it prints at startup." if self.rejected else "")
                self._say(f"[MONITOR DOWN] {what}.\n"
                          "The monitor computer may be off, offline or crashed: the token is NOT being "
                          f"watched and the guardian cannot act. Check it now.{hint}")
            if (self.last_seen is not None and not self.down and not self.stalled
                    and now - self.block_changed > self.stall_after):
                self.stalled = True
                self._say(f"[WARNING] Monitor is alive but stuck at block {self.block} for "
                          f"{int(now - self.block_changed)}s. Its RPC provider may be down or rate-limiting.")

    def status(self) -> dict:
        now = self.clock()
        with self.lock:
            return {
                "healthy": self.last_seen is not None and not self.down and not self.stalled,
                "last_heartbeat_age": None if self.last_seen is None else round(now - self.last_seen),
                "block": self.block,
                "token": self.token,
                "rejected": self.rejected,
            }

    def _say(self, text: str) -> None:
        try:
            self.send(text)
        except Exception as err:  # never let Telegram trouble kill the watchdog
            print(f"could not send alert: {err}", flush=True)


def telegram_sender(bot_token: str, chat_ids: list[str]):
    def send(text: str) -> None:
        print(text, flush=True)
        for chat in chat_ids:
            for attempt in range(3):
                try:
                    r = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                                      json={"chat_id": chat, "text": text}, timeout=10)
                    if r.ok:
                        break
                    print(f"Telegram refused ({r.status_code}): {r.text[:200]}", flush=True)
                except requests.RequestException as err:
                    print(f"Telegram unreachable: {type(err).__name__}", flush=True)
                time.sleep(2 * (attempt + 1))
    return send


def make_handler(dog: Watchdog):
    class Handler(BaseHTTPRequestHandler):
        def _reply(self, code: int, obj: dict) -> None:
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):  # health check for Render, and a quick look for you
            self._reply(200, dog.status())

        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

        def do_POST(self):
            if self.path.rstrip("/") not in ("", "/heartbeat"):
                return self._reply(404, {"error": "not found"})
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > MAX_BODY:
                return self._reply(413, {"error": "body too large or empty"})
            try:
                payload = json.loads(self.rfile.read(length))
            except ValueError:
                return self._reply(400, {"error": "not JSON"})
            code, reason = dog.receive(payload if isinstance(payload, dict) else {})
            self._reply(code, {"result": reason})

        def log_message(self, fmt, *args):  # keep Render logs quiet: only alerts are printed
            pass

    return Handler


def attach_guard(aio_app, send_async):
    """
    Run the watchdog inside an EXISTING aiohttp web app, e.g. a Telegram bot
    already hosted on Render, instead of as its own service:

        from heartbeat_watchdog import attach_guard
        attach_guard(aio_app, lambda text: app.bot.send_message(chat_id=CHAT_ID, text=text))

    Call it after creating the aiohttp app and before runner.setup(). Adds
    POST /heartbeat (for the monitor) and GET /guard (status). Alerts are sent
    with the bot's own async send function, so nothing blocks the bot. Does
    nothing if MONITOR_PUBLIC_KEY is not set.
    """
    import asyncio

    from aiohttp import web

    pub = os.getenv("MONITOR_PUBLIC_KEY", "").strip()
    if not pub:
        print("[GUARD] MONITOR_PUBLIC_KEY not set: guard watchdog disabled.", flush=True)
        return None
    pending: set = set()

    def say(text: str) -> None:
        async def deliver():
            try:
                await send_async(text)
            except Exception as err:  # Telegram trouble must never break the host app
                print(f"[GUARD] could not send alert ({type(err).__name__})", flush=True)
        task = asyncio.get_running_loop().create_task(deliver())
        pending.add(task)
        task.add_done_callback(pending.discard)

    stale = max(75, int(os.getenv("STALE_SECONDS", "120")))
    dog = Watchdog(pub, say, stale, int(os.getenv("STALL_SECONDS", "600")))
    loop_task = {}

    async def heartbeat(request):
        if (request.content_length or 0) > MAX_BODY:
            return web.json_response({"error": "body too large"}, status=413)
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "not JSON"}, status=400)
        code, reason = dog.receive(payload if isinstance(payload, dict) else {})
        return web.json_response({"result": reason}, status=code)

    async def status(request):
        return web.json_response(dog.status())

    async def check_forever():
        while True:
            await asyncio.sleep(5)
            dog.check()

    async def on_startup(_app):
        loop_task["t"] = asyncio.get_running_loop().create_task(check_forever())

    async def on_cleanup(_app):
        if loop_task.get("t"):
            loop_task["t"].cancel()

    aio_app.router.add_post("/heartbeat", heartbeat)
    aio_app.router.add_get("/guard", status)
    aio_app.on_startup.append(on_startup)
    aio_app.on_cleanup.append(on_cleanup)
    print(f"[GUARD] watchdog active: POST /heartbeat, status at /guard (alert after {stale}s)", flush=True)
    return dog


def main() -> None:
    pub = os.getenv("MONITOR_PUBLIC_KEY", "").strip()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chats = [c.strip() for c in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]
    if not pub:
        raise SystemExit("Set MONITOR_PUBLIC_KEY (the monitor prints it at startup).")
    send = telegram_sender(token, chats) if token and chats else (lambda text: print(text, flush=True))
    stale = int(os.getenv("STALE_SECONDS", "120"))
    if stale < 75:  # the monitor pushes every 30s; anything shorter raises false alarms
        print(f"STALE_SECONDS={stale} is too short for 30-second heartbeats; using 75.", flush=True)
        stale = 75
    dog = Watchdog(pub, send, stale, int(os.getenv("STALL_SECONDS", "600")))

    def loop():
        while True:
            time.sleep(5)
            dog.check()

    threading.Thread(target=loop, daemon=True).start()
    port = int(os.getenv("PORT", "10000"))
    print(f"Remote watchdog listening on port {port}; alerting after {dog.stale_after}s of silence.", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), make_handler(dog)).serve_forever()


if __name__ == "__main__":
    main()
