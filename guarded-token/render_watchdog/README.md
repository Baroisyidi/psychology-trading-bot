# Remote watchdog (for Render)

A small web service that alerts you on Telegram if your monitor stops
sending heartbeats, for example because your computer is off or offline.
The full walkthrough is in the main README, under "Going public", step 7.

It only *sends* messages through your bot. It never changes the bot's
webhook, so your existing bot keeps working.

## A. As its own Render web service

| Setting        | Value                             |
|----------------|-----------------------------------|
| Root directory | `render_watchdog`                 |
| Build command  | `pip install -r requirements.txt` |
| Start command  | `python app.py`                   |
| Health check   | `/`                               |

Environment variables:
- Required: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_IDS`, and
  `MONITOR_PUBLIC_KEY` (the monitor prints it at startup as "Alert signing
  public key").
- Optional: `STALE_SECONDS` (default 120) and `STALL_SECONDS` (default 600).

Then set `REMOTE_WATCHDOG_URL=https://<your-service>.onrender.com/heartbeat`
in `.env.sepolia`.

Free plan note: a service that receives a heartbeat every 30 seconds never
sleeps, so it uses about 744 of the 750 free hours a month on its own. If
your bot is also on the free plan, use option B.

## B. Inside your existing Python bot (no extra Render hours)

Copy `app.py` into your bot's project as `heartbeat_watchdog.py`, and add
`pynacl` to its requirements. Add `MONITOR_PUBLIC_KEY` to the bot's
environment on Render (it already has the bot token). Then add a
`/heartbeat` route next to your webhook route. With Flask:

```python
import os, threading, time
from flask import request, jsonify
from heartbeat_watchdog import Watchdog, telegram_sender

dog = Watchdog(os.environ["MONITOR_PUBLIC_KEY"],
               telegram_sender(os.environ["TELEGRAM_BOT_TOKEN"], ["YOUR_CHAT_ID"]))

def _check_forever():
    while True:
        time.sleep(5)
        dog.check()

threading.Thread(target=_check_forever, daemon=True).start()

@app.post("/heartbeat")
def heartbeat():
    code, reason = dog.receive(request.get_json(silent=True) or {})
    return jsonify(result=reason), code

@app.get("/watchdog")
def watchdog_status():
    return jsonify(dog.status())
```

With FastAPI the route is the same idea:
`code, reason = dog.receive(await request.json())`, then return a
`JSONResponse({"result": reason}, status_code=code)`.

Then set `REMOTE_WATCHDOG_URL=https://<your-bot>.onrender.com/heartbeat`.

If your bot runs several worker processes (for example gunicorn
`--workers 3`), use one worker, or each worker will keep its own watchdog
and you may get duplicate alerts.
