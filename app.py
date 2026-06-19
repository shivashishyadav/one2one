import os
import sqlite3
import json
import time
import threading
from flask import Flask, render_template, request, jsonify, g
from flask_socketio import SocketIO, emit, join_room, leave_room
from pywebpush import webpush, WebPushException

app = Flask(__name__)

# Reads the secret key from environment, falls back to a default if not found
app.config["SECRET_KEY"] = os.environ.get(
    "FLASK_SECRET_KEY", "static-private-room-key-2024"
)

socketio = SocketIO(
    app,
    async_mode="threading",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
)

# ─────────────────────────────────────────────
#  CONFIG (Loads dynamically from system environment)
# ─────────────────────────────────────────────
MESSAGE_EXPIRY_HOURS = 2
DB_PATH = "chat.db"

# os.environ.get("KEY", "FALLBACK_VALUE")
VAPID_PRIVATE_KEY = os.environ.get(
    "VAPID_PRIVATE_KEY", "fPwUlH8aYzlxm1tKYDgs9V72qagoao1GvPe-ckXFw0s"
)
VAPID_PUBLIC_KEY = os.environ.get(
    "VAPID_PUBLIC_KEY",
    "BHv0EC77zNTvegemwvsGM-sRGI1Ow0ayj5RrieaBiZZERfblzTXUz8EJzxvDnE_D93Kj4AVx17aWp2KOzGErzTQ",
)

vapid_email = os.environ.get("VAPID_EMAIL", "mailto:you@example.com")
VAPID_CLAIMS = {"sub": vapid_email}
# ─────────────────────────────────────────────

# { room_name: {sid: slot} }
active_rooms = {}

# { room_name: {slot: push_subscription} }
push_subscriptions = {}
# ── Database ──────────────────────────────────


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(e=None):
    db = g.pop("db", None)
    if db:
        db.close()


def init_db():
    print("DB PATH:", os.path.abspath(DB_PATH))
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
    print("Database initialized.")


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


threading.Thread(target=purge_loop, daemon=True).start()


init_db()

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
    """Landing page — enter a room name to get started."""
    return render_template("home.html")


@app.route("/room/<room_name>")
def room(room_name):
    # Only allow alphanumeric + hyphens, max 40 chars
    clean = "".join(c for c in room_name.lower() if c.isalnum() or c == "-")[:40]
    if not clean:
        return "Invalid room name.", 400
    return render_template(
        "index.html", vapid_public_key=VAPID_PUBLIC_KEY, room_name=clean
    )


@app.route("/save-subscription", methods=["POST"])
def save_subscription():
    data = request.get_json()
    room = data.get("room")
    slot = data.get("slot")
    sub = data.get("subscription")  # None means disable

    if not room or slot not in ("user_a", "user_b"):
        return jsonify({"ok": False})

    if room not in push_subscriptions:
        push_subscriptions[room] = {}

    if sub is None:
        # User disabled notifications — remove subscription
        push_subscriptions[room].pop(slot, None)
    else:
        push_subscriptions[room][slot] = sub

    return jsonify({"ok": True})


# ── Socket events ─────────────────────────────


@socketio.on("join")
def handle_join(data):
    sid = request.sid
    room_name = data.get("room", "").strip().lower()[:40]
    preferred_slot = data.get("preferred_slot")  # client's remembered slot
    if not room_name:
        return

    # Init room tracking
    if room_name not in active_rooms:
        active_rooms[room_name] = {}

    room_sessions = active_rooms[room_name]

    # Block third person
    if len(room_sessions) >= 2:
        emit("rejected", {"reason": "Room is full. Try again later."})
        return

    slots_used = set(room_sessions.values())

    # Try to honour the client's remembered slot first
    # (prevents both users getting user_a after a server restart)
    if preferred_slot in ("user_a", "user_b") and preferred_slot not in slots_used:
        slot = preferred_slot
    else:
        # Fall back: assign whichever slot is free
        slot = "user_a" if "user_a" not in slots_used else "user_b"

    room_sessions[sid] = slot

    # Join the socket.io room so broadcast is scoped
    join_room(room_name)

    # Send history
    messages = load_recent_messages(room_name)
    emit("history", {"messages": messages, "your_slot": slot, "room": room_name})

    # Mark other person's unseen messages as seen
    other_slot = "user_b" if slot == "user_a" else "user_a"
    unseen_ids = [
        m["id"] for m in messages if m["sender"] == other_slot and not m["seen"]
    ]
    if unseen_ids:
        mark_seen(unseen_ids)
        emit("messages-seen", {"ids": unseen_ids}, to=room_name, include_self=False)

    online_count = len(room_sessions)
    emit(
        "status",
        {
            "type": "waiting" if online_count == 1 else "connected",
            "text": (
                "Other person is offline — they will get a notification."
                if online_count == 1
                else "Both online."
            ),
        },
    )
    if online_count == 2:
        emit(
            "status",
            {"type": "connected", "text": "The other person just came online."},
            to=room_name,
            include_self=False,
        )


@socketio.on("disconnect")
def handle_disconnect():
    sid = request.sid
    # Find which room this sid belongs to
    for room_name, sessions in list(active_rooms.items()):
        if sid in sessions:
            sessions.pop(sid)
            leave_room(room_name)
            if not sessions:
                # Room empty — clean up
                active_rooms.pop(room_name, None)
            else:
                emit(
                    "status",
                    {
                        "type": "waiting",
                        "text": "Other person went offline. Your next message will notify them.",
                    },
                    to=room_name,
                )
            break


@socketio.on("chat-message")
def handle_message(data):
    sid = request.sid
    room_name = data.get("room", "")
    sessions = active_rooms.get(room_name, {})

    if sid not in sessions:
        return

    sender = sessions[sid]
    text = data.get("text", "").strip()
    if not text:
        return

    msg_id = save_message(room_name, sender, text)
    ts = time.time()
    payload = {"id": msg_id, "sender": sender, "text": text, "ts": ts, "seen": 0}

    # Confirm to sender
    emit("message-sent", payload)

    recipient_online = any(s != sender for s in sessions.values())
    if recipient_online:
        emit("chat-message", payload, to=room_name, include_self=False)
    else:
        other_slot = "user_b" if sender == "user_a" else "user_a"
        send_push(room_name, other_slot, "New message", text[:80])


@socketio.on("mark-seen")
def handle_mark_seen(data):
    sid = request.sid
    room_name = data.get("room", "")
    ids = data.get("ids", [])
    if not ids:
        return
    mark_seen(ids)
    emit("messages-seen", {"ids": ids}, to=room_name, include_self=False)


if __name__ == "__main__":
    # init_db()
    port = int(os.environ.get("PORT", 5000))
    socketio.run(
        app,
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
        allow_unsafe_werkzeug=True,
    )
