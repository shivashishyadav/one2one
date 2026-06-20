import os
import sqlite3
import json
import time
import threading

ON_RENDER = os.environ.get("RENDER") == "true"

if ON_RENDER:
    import eventlet
    eventlet.monkey_patch()
    ASYNC_MODE = "eventlet"
else:
    ASYNC_MODE = "threading"

from flask import Flask, render_template, request, jsonify, g
from flask_socketio import SocketIO, emit, join_room, leave_room
from pywebpush import webpush, WebPushException

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "static-private-room-key-2024")

socketio = SocketIO(
    app,
    async_mode=ASYNC_MODE,
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
    ping_timeout=60,
    ping_interval=25,
)

# ── CONFIG ────────────────────────────────────
MESSAGE_EXPIRY_HOURS = 2
REJOIN_GRACE_SECONDS = 15   # slot held this long after disconnect

if ON_RENDER:
    DB_PATH = "/tmp/chat.db"
else:
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chat.db")

VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "fPwUlH8aYzlxm1tKYDgs9V72qagoao1GvPe-ckXFw0s")
VAPID_PUBLIC_KEY  = os.environ.get("VAPID_PUBLIC_KEY",  "BHv0EC77zNTvegemwvsGM-sRGI1Ow0ayj5RrieaBiZZERfblzTXUz8EJzxvDnE_D93Kj4AVx17aWp2KOzGErzTQ")
VAPID_CLAIMS      = {"sub": os.environ.get("VAPID_EMAIL", "mailto:you@example.com")}

# ── In-memory state ───────────────────────────
# active_rooms[room] = { sid: slot }   ← currently connected
active_rooms = {}

# ghost_slots[room][slot] = timestamp of disconnect
# Holds a slot for REJOIN_GRACE_SECONDS so a page refresh doesn't lock out the user
ghost_slots = {}

push_subscriptions = {}
_lock = threading.Lock()

# ── Database ──────────────────────────────────
def init_db():
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                room    TEXT    NOT NULL,
                sender  TEXT    NOT NULL,
                text    TEXT    NOT NULL,
                ts      REAL    NOT NULL,
                seen    INTEGER NOT NULL DEFAULT 0
            )
        """)
        db.execute("CREATE INDEX IF NOT EXISTS idx_room_ts ON messages(room, ts)")
        db.commit()

def save_message(room, sender, text):
    with sqlite3.connect(DB_PATH) as db:
        cur = db.execute(
            "INSERT INTO messages (room, sender, text, ts, seen) VALUES (?,?,?,?,0)",
            (room, sender, text, time.time()),
        )
        db.commit()
        return cur.lastrowid

def load_recent_messages(room):
    cutoff = time.time() - (MESSAGE_EXPIRY_HOURS * 3600)
    with sqlite3.connect(DB_PATH) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT id, sender, text, ts, seen FROM messages WHERE room=? AND ts>? ORDER BY ts ASC",
            (room, cutoff),
        ).fetchall()
    return [dict(r) for r in rows]

def mark_seen(msg_ids):
    if not msg_ids:
        return
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            f'UPDATE messages SET seen=1 WHERE id IN ({",".join("?"*len(msg_ids))})',
            msg_ids,
        )
        db.commit()

def purge_expired():
    cutoff = time.time() - (MESSAGE_EXPIRY_HOURS * 3600)
    with sqlite3.connect(DB_PATH) as db:
        db.execute("DELETE FROM messages WHERE ts < ?", (cutoff,))
        db.commit()

def purge_loop():
    while True:
        time.sleep(600)
        try:
            purge_expired()
        except Exception:
            pass

# ── Ghost slot helpers ────────────────────────
def get_live_slot_count(room_name):
    """
    Count of slots that are ACTUALLY blocking the room:
    - currently connected sids
    - ghost slots still within grace period
    Returns (count, ghost_slot_available)
    ghost_slot_available = a slot held by a recent disconnect that a rejoiner can reclaim
    """
    now = time.time()
    sessions = active_rooms.get(room_name, {})
    live_slots = set(sessions.values())

    ghosts = ghost_slots.get(room_name, {})
    # expire old ghosts
    expired = [s for s, t in ghosts.items() if now - t > REJOIN_GRACE_SECONDS]
    for s in expired:
        ghosts.pop(s, None)

    active_ghost_slots = set(ghosts.keys()) - live_slots  # ghosts not yet reclaimed

    return live_slots, active_ghost_slots

def release_ghost(room_name, slot):
    """Immediately release a ghost slot (called when someone reclaims it)."""
    ghost_slots.get(room_name, {}).pop(slot, None)

# ── Push notification ─────────────────────────
def send_push(room, slot, title, body):
    sub = push_subscriptions.get(room, {}).get(slot)
    if not sub:
        return
    try:
        webpush(
            subscription_info=sub,
            data=json.dumps({"title": title, "body": body}),
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims=VAPID_CLAIMS,
        )
    except WebPushException as e:
        if e.response and e.response.status_code in (404, 410):
            push_subscriptions.get(room, {}).pop(slot, None)
    except Exception:
        pass

# ── Routes ────────────────────────────────────
@app.route("/")
def home():
    return render_template("home.html")

@app.route("/room/<room_name>")
def room(room_name):
    clean = "".join(c for c in room_name.lower() if c.isalnum() or c == "-")[:40]
    if not clean:
        return "Invalid room name.", 400
    return render_template("index.html", vapid_public_key=VAPID_PUBLIC_KEY, room_name=clean)

@app.route("/save-subscription", methods=["POST"])
def save_subscription():
    data = request.get_json()
    room = data.get("room")
    slot = data.get("slot")
    sub  = data.get("subscription")

    if not room or slot not in ("user_a", "user_b"):
        return jsonify({"ok": False})

    if room not in push_subscriptions:
        push_subscriptions[room] = {}

    if sub is None:
        push_subscriptions[room].pop(slot, None)
    else:
        push_subscriptions[room][slot] = sub

    return jsonify({"ok": True})

# ── Socket events ─────────────────────────────
@socketio.on("connect")
def on_connect():
    pass

@socketio.on("join")
def handle_join(data):
    sid            = request.sid
    room_name      = data.get("room", "").strip().lower()[:40]
    preferred_slot = data.get("preferred_slot")

    if not room_name:
        return

    with _lock:
        if room_name not in active_rooms:
            active_rooms[room_name] = {}
        if room_name not in ghost_slots:
            ghost_slots[room_name] = {}

        sessions = active_rooms[room_name]

        # Remove this sid if it's already in the room (rejoining)
        sessions.pop(sid, None)

        live_slots, active_ghost_slots = get_live_slot_count(room_name)

        # Total "occupied" slots = live + ghost
        total_occupied = live_slots | active_ghost_slots

        # ── Can this person join? ──────────────────
        # KEY RULE: only reject if BOTH slots have LIVE connections.
        # Ghosts (recent disconnects) do NOT block new joins.
        slot = None

        # Case 1: preferred slot is free (not even a live connection on it)
        if preferred_slot in ("user_a", "user_b") and preferred_slot not in live_slots:
            slot = preferred_slot
            release_ghost(room_name, slot)

        # Case 2: no preference or preferred is live → take any slot not live
        elif len(live_slots) < 2:
            # Prefer a slot that matches preferred if possible
            candidates = ["user_a", "user_b"]
            if preferred_slot in candidates:
                candidates = [preferred_slot] + [c for c in candidates if c != preferred_slot]
            for candidate in candidates:
                if candidate not in live_slots:
                    slot = candidate
                    release_ghost(room_name, slot)
                    break

        # Case 3: both slots are LIVE right now → room actually full
        if slot is None:
            emit("rejected", {"reason": "Room is full. Try again later."})
            return

        sessions[sid] = slot
        online_count = len(sessions)

    join_room(room_name)

    messages = load_recent_messages(room_name)
    emit("history", {"messages": messages, "your_slot": slot, "room": room_name})

    other_slot = "user_b" if slot == "user_a" else "user_a"
    unseen_ids = [m["id"] for m in messages if m["sender"] == other_slot and not m["seen"]]
    if unseen_ids:
        mark_seen(unseen_ids)
        emit("messages-seen", {"ids": unseen_ids}, to=room_name, include_self=False)

    emit("status", {
        "type": "waiting" if online_count == 1 else "connected",
        "text": "Other person is offline — they will get a notification." if online_count == 1
                else "Both online.",
    })
    if online_count == 2:
        emit("status", {"type": "connected", "text": "The other person just came online."},
             to=room_name, include_self=False)


@socketio.on("disconnect")
def handle_disconnect():
    sid = request.sid

    with _lock:
        for room_name, sessions in list(active_rooms.items()):
            if sid in sessions:
                slot = sessions.pop(sid)
                leave_room(room_name)

                # ── KEY FIX: hold the slot as a ghost for grace period ──
                # This means a page refresh won't make the room appear full
                # The slot fully clears after REJOIN_GRACE_SECONDS
                if room_name not in ghost_slots:
                    ghost_slots[room_name] = {}
                ghost_slots[room_name][slot] = time.time()

                if not sessions:
                    active_rooms.pop(room_name, None)
                else:
                    emit("status", {
                        "type": "waiting",
                        "text": "Other person went offline. Your next message will notify them.",
                    }, to=room_name)
                break


@socketio.on("chat-message")
def handle_message(data):
    sid       = request.sid
    room_name = data.get("room", "")

    with _lock:
        sessions = dict(active_rooms.get(room_name, {}))

    if sid not in sessions:
        return

    sender = sessions[sid]
    text   = data.get("text", "").strip()
    if not text:
        return

    msg_id  = save_message(room_name, sender, text)
    payload = {"id": msg_id, "sender": sender, "text": text, "ts": time.time(), "seen": 0}

    emit("message-sent", payload)

    recipient_online = any(s != sender for s in sessions.values())
    if recipient_online:
        emit("chat-message", payload, to=room_name, include_self=False)
    else:
        other_slot = "user_b" if sender == "user_a" else "user_a"
        send_push(room_name, other_slot, "New message 💬", text[:80])


@socketio.on("mark-seen")
def handle_mark_seen(data):
    room_name = data.get("room", "")
    ids       = data.get("ids", [])
    if not ids:
        return
    mark_seen(ids)
    emit("messages-seen", {"ids": ids}, to=room_name, include_self=False)


# ── Startup ───────────────────────────────────
init_db()
threading.Thread(target=purge_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n  Running on http://localhost:{port}")
    print(f"  Mode: {ASYNC_MODE}\n")
    socketio.run(app, host="0.0.0.0", port=port,
                 debug=False, use_reloader=False, allow_unsafe_werkzeug=True)