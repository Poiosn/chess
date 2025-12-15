# app.py
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room, leave_room
import chess
import time
import random
import secrets
import threading

app = Flask(__name__)
app.config["SECRET_KEY"] = secrets.token_hex(16)

# async_mode threading works without extra deps; eventlet/gevent may be used but not required.
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# In-memory games store
games = {}

# Stockfish OFF by user choice B — bot will play random legal moves
engine = None
print("ℹ Stockfish disabled — bot will play random moves")

@app.route("/")
def index():
    return render_template("ui.html")


def board_to_matrix(board: chess.Board):
    """Convert python-chess board to 8x8 matrix for the UI."""
    grid = [["." for _ in range(8)] for _ in range(8)]
    for sq, piece in board.piece_map().items():
        file = chess.square_file(sq)
        rank = chess.square_rank(sq)
        row = 7 - rank
        col = file
        grid[row][col] = piece.symbol()
    return grid

def format_seconds(sec: float) -> str:
    """Format seconds into M:SS (e.g. 5:00, 4:07)."""
    total = int(max(0, round(sec)))
    m = total // 60
    s = total % 60
    return f"{m}:{s:02d}"

def export_state(room):
    """Export current game state for a room."""
    g = games[room]
    b: chess.Board = g["board"]
    return {
        "board": board_to_matrix(b),
        "turn": "white" if b.turn else "black",
        "check": b.is_check(),
        "winner": g["winner"],
        "reason": g["reason"],
        # Keep numeric seconds (client doesn't require, but useful)
        "whiteTime": int(g["whiteTime"]),
        "blackTime": int(g["blackTime"]),
        # Add formatted strings expected by UI
        "whiteTimeFormatted": format_seconds(g["whiteTime"]),
        "blackTimeFormatted": format_seconds(g["blackTime"]),
    }

def move_to_notation(board: chess.Board, move):
    """Convert move to algebraic notation (SAN)."""
    try:
        return board.san(move)
    except Exception:
        from_sq = chess.square_name(move.from_square)
        to_sq = chess.square_name(move.to_square)
        return f"{from_sq}{to_sq}"

def send_game_update(room, move_obj=None):
    """Send full game state + lastMove (from/to) to all clients in room."""
    last_move = None
    move_notation = None

    if move_obj:
        from_row = 7 - chess.square_rank(move_obj.from_square)
        from_col = chess.square_file(move_obj.from_square)
        to_row = 7 - chess.square_rank(move_obj.to_square)
        to_col = chess.square_file(move_obj.to_square)
        last_move = {
            "from": {"row": from_row, "col": from_col},
            "to": {"row": to_row, "col": to_col},
        }

        # notation: get SAN by undoing last move on a copy
        g = games[room]
        board_copy = g["board"].copy()
        board_copy.pop()
        move_notation = move_to_notation(board_copy, move_obj)

    socketio.emit(
        "game_update",
        {
            "state": export_state(room),
            "lastMove": last_move,
            "moveNotation": move_notation
        },
        room=room,
    )

@socketio.on("connect")
def on_connect():
    print(f"✅ Client connected: {request.sid}")

@socketio.on("disconnect")
def on_disconnect():
    print(f"❌ Client disconnected: {request.sid}")

@socketio.on("create_room")
def on_create_room(data):
    room = data["room"]
    is_bot = data.get("bot", False)
    time_control = data.get("timeControl", 300)

    print(f"🚪 Creating room '{room}', bot={is_bot}, time={time_control}s, sid={request.sid}")

    board = chess.Board()
    games[room] = {
        "board": board,
        "players": {"white": request.sid, "black": None},
        "whiteTime": float(time_control),
        "blackTime": float(time_control),
        "lastUpdate": time.time(),
        "winner": None,
        "reason": None,
        "bot": is_bot,
        "bot_color": "black" if is_bot else None,
        "draw_offer": None,
        "lock": threading.Lock(),  # ← FIXED: Add thread safety
    }

    join_room(room)

    emit(
        "room_created",
        {
            "room": room,
            "color": "white",
            "state": export_state(room),
            "bot": is_bot,
        },
    )

    print(f"✅ Room '{room}' created successfully")

@socketio.on("join_room")
def on_join_room(data):
    room = data["room"]
    print(f"➡️ Attempting to join room '{room}', sid={request.sid}")

    if room not in games:
        emit("error", {"message": "Room does not exist"})
        print(f"❌ Room '{room}' does not exist")
        return

    g = games[room]

    if g["bot"]:
        emit("error", {"message": "Cannot join bot game"})
        print(f"❌ Cannot join bot game '{room}'")
        return

    if g["players"]["black"] is not None:
        emit("error", {"message": "Room is full"})
        print(f"❌ Room '{room}' is full")
        return

    g["players"]["black"] = request.sid
    g["lastUpdate"] = time.time()
    join_room(room)

    emit(
        "room_joined",
        {
            "room": room,
            "color": "black",
            "state": export_state(room),
            "bot": False,
        },
    )

    socketio.emit("game_start", {"state": export_state(room)}, room=room)
    print(f"✅ Player joined room '{room}' as BLACK. Game starting!")

@socketio.on("leave_room")
def on_leave_room(data):
    room = data["room"]
    if room in games:
        print(f"🚪 Deleting room '{room}'")
        del games[room]
    leave_room(room)

@socketio.on("get_possible_moves")
def on_get_possible_moves(data):
    room = data["room"]
    from_pos = data["from"]

    if room not in games:
        return

    board: chess.Board = games[room]["board"]
    from_sq = chess.square(from_pos["col"], 7 - from_pos["row"])

    moves = []
    for mv in board.legal_moves:
        if mv.from_square == from_sq:
            r = 7 - chess.square_rank(mv.to_square)
            c = chess.square_file(mv.to_square)
            moves.append({"row": r, "col": c})

    emit("possible_moves", {"moves": moves})

@socketio.on("get_time")
def on_get_time(data):
    room = data["room"]
    if room not in games:
        return

    g = games[room]
    board: chess.Board = g["board"]

    if g["winner"]:
        emit(
            "time_update",
            {
                "whiteTime": int(g["whiteTime"]),
                "blackTime": int(g["blackTime"]),
                "whiteTimeFormatted": format_seconds(g["whiteTime"]),
                "blackTimeFormatted": format_seconds(g["blackTime"]),
            },
        )
        return

    # ← FIXED: Thread-safe time updates
    with g["lock"]:
        now = time.time()
        elapsed = now - g["lastUpdate"]
        g["lastUpdate"] = now

        turn = "white" if board.turn else "black"

        if turn == "white":
            g["whiteTime"] = max(0.0, g["whiteTime"] - elapsed)
            if g["whiteTime"] == 0.0 and not g["winner"]:
                g["winner"] = "black"
                g["reason"] = "timeout"
                print(f"⏱️ WHITE timed out in room '{room}'")
                send_game_update(room)
        else:
            g["blackTime"] = max(0.0, g["blackTime"] - elapsed)
            if g["blackTime"] == 0.0 and not g["winner"]:
                g["winner"] = "white"
                g["reason"] = "timeout"
                print(f"⏱️ BLACK timed out in room '{room}'")
                send_game_update(room)

    emit(
        "time_update",
        {
            "whiteTime": int(g["whiteTime"]),
            "blackTime": int(g["blackTime"]),
            "whiteTimeFormatted": format_seconds(g["whiteTime"]),
            "blackTimeFormatted": format_seconds(g["blackTime"]),
        },
    )

# ← FIXED: New thread-safe time update function
def update_time_before_move(g):
    """Thread-safe time update with timeout detection."""
    now = time.time()
    elapsed = now - g["lastUpdate"]
    board = g["board"]
    
    # Subtract time from current player
    if board.turn:  # White's turn
        g["whiteTime"] = max(0.0, g["whiteTime"] - elapsed)
        if g["whiteTime"] == 0.0 and not g["winner"]:
            g["winner"] = "black"
            g["reason"] = "timeout"
    else:  # Black's turn
        g["blackTime"] = max(0.0, g["blackTime"] - elapsed)
        if g["blackTime"] == 0.0 and not g["winner"]:
            g["winner"] = "white" 
            g["reason"] = "timeout"
    
    g["lastUpdate"] = now

def handle_checkmate_and_draw(g, room):
    """Check for game-ending conditions."""
    board: chess.Board = g["board"]
    if board.is_checkmate():
        g["winner"] = "white" if not board.turn else "black"
        g["reason"] = "checkmate"
        print(f"👑 Checkmate! {g['winner'].upper()} wins in room '{room}'")
    elif (
        board.is_stalemate()
        or board.is_insufficient_material()
        or board.can_claim_threefold_repetition()
        or board.is_fifty_moves()
    ):
        g["winner"] = "draw"
        g["reason"] = "draw"
        print(f"🤝 Draw in room '{room}'")

@socketio.on("move")
def on_move(data):
    room = data["room"]
    if room not in games:
        return

    g = games[room]
    board: chess.Board = g["board"]

    # ← FIXED: Thread-safe time update and validation
    with g["lock"]:
        update_time_before_move(g)
        
        if g["winner"]:
            emit("error", {"message": "Game already finished"})
            return

        from_pos = data["from"]
        to_pos = data["to"]
        promotion_piece = data.get("promotion")

        from_sq = chess.square(from_pos["col"], 7 - from_pos["row"])
        to_sq = chess.square(to_pos["col"], 7 - to_pos["row"])

        if promotion_piece:
            prom = {
                "q": chess.QUEEN,
                "r": chess.ROOK,
                "b": chess.BISHOP,
                "n": chess.KNIGHT,
            }.get(promotion_piece.lower(), chess.QUEEN)
            mv = chess.Move(from_sq, to_sq, promotion=prom)
        else:
            mv = chess.Move(from_sq, to_sq)

        if mv not in board.legal_moves:
            emit("error", {"message": "Illegal move"})
            print(f"❌ Illegal move attempted in room '{room}'")
            return

        board.push(mv)
        g["draw_offer"] = None

    print(f"♟ Move made in room '{room}': {mv}")

    handle_checkmate_and_draw(g, room)
    send_game_update(room, mv)

    # Bot move (random) if applicable
    if (
        g["bot"]
        and not g["winner"]
        and g["bot_color"] == ("white" if board.turn else "black")
    ):
        socketio.start_background_task(bot_move, room)

# ← FIXED: Fair bot timing
def bot_move(room):
    """Background task: bot with fair timing."""
    if room not in games:
        return

    g = games[room]
    
    # Update bot's clock for thinking time
    with g["lock"]:
        update_time_before_move(g)
        if g["winner"]:  # Check if timeout occurred
            send_game_update(room)
            return
            
    # Bot thinks outside the lock
    time.sleep(1.0)
    
    # Make the move atomically
    with g["lock"]:
        board = g["board"]
        if board.is_game_over() or g["winner"]:
            return

        try:
            mv = random.choice(list(board.legal_moves))
            board.push(mv)
            g["lastUpdate"] = time.time()
            print(f"🤖 Bot move in room '{room}': {mv}")
        except Exception as e:
            print(f"⚠ Bot failed: {e}")
            return

    handle_checkmate_and_draw(g, room)
    send_game_update(room, mv)

# ← FIXED: Background timeout watcher for reliable timeout detection
def timeout_watcher():
    """Background thread to detect timeouts reliably."""
    while True:
        time.sleep(0.5)  # Check every 500ms
        
        for room, g in list(games.items()):
            try:
                with g["lock"]:
                    if g["winner"]:
                        continue
                        
                    now = time.time()
                    elapsed = now - g["lastUpdate"]
                    board = g["board"]
                    
                    if board.turn:  # White's turn
                        new_time = max(0.0, g["whiteTime"] - elapsed)
                        if new_time == 0.0 and g["whiteTime"] > 0:
                            g["whiteTime"] = 0.0
                            g["winner"] = "black"
                            g["reason"] = "timeout"
                            print(f"⏰ WHITE timeout in '{room}'")
                            send_game_update(room)
                            continue
                        g["whiteTime"] = new_time
                    else:  # Black's turn
                        new_time = max(0.0, g["blackTime"] - elapsed) 
                        if new_time == 0.0 and g["blackTime"] > 0:
                            g["blackTime"] = 0.0
                            g["winner"] = "white"
                            g["reason"] = "timeout"
                            print(f"⏰ BLACK timeout in '{room}'")
                            send_game_update(room)
                            continue
                        g["blackTime"] = new_time
                    
                    g["lastUpdate"] = now
            except Exception as e:
                print(f"⚠ Timeout watcher error for room '{room}': {e}")

@socketio.on("resign")
def on_resign(data):
    room = data["room"]
    color = data["color"]
    if room not in games:
        return

    g = games[room]
    if g["winner"]:
        return

    g["winner"] = "black" if color == "white" else "white"
    g["reason"] = "resign"
    print(f"🏳️ {color.upper()} resigned in room '{room}'")
    send_game_update(room)

@socketio.on("offer_draw")
def on_offer_draw(data):
    room = data["room"]
    from_color = data["color"]

    if room not in games:
        return

    g = games[room]
    if g["winner"]:
        return

    g["draw_offer"] = from_color
    print(f"🤝 {from_color.upper()} offered draw in room '{room}'")

    socketio.emit(
        "draw_offered",
        {"fromColor": from_color},
        room=room,
        skip_sid=request.sid
    )

@socketio.on("respond_draw")
def on_respond_draw(data):
    room = data["room"]
    accept = data["accept"]

    if room not in games:
        return

    g = games[room]

    if accept:
        g["winner"] = "draw"
        g["reason"] = "agreement"
        print(f"🤝 Draw accepted in room '{room}'")
        send_game_update(room)
    else:
        g["draw_offer"] = None
        print(f"❌ Draw declined in room '{room}'")
        socketio.emit("draw_declined", {}, room=room)

@socketio.on("reset_game")
def on_reset_game(data):
    room = data["room"]
    if room not in games:
        return

    print(f"🔄 Resetting game in room '{room}'")
    g = games[room]
    time_control = g.get("whiteTime", 300.0)

    board = chess.Board()
    g["board"] = board
    g["whiteTime"] = time_control
    g["blackTime"] = time_control
    g["lastUpdate"] = time.time()
    g["winner"] = None
    g["reason"] = None
    g["draw_offer"] = None

    send_game_update(room)

# ---------------------------
# Chat + Typing Handlers
# ---------------------------
@socketio.on("send_message")
def on_send_message(data):
    """
    data: { room: str, sender: 'white'|'black'|'spectator', message: str }
    Broadcasts chat_message to the room.
    """
    room = data.get("room")
    msg = data.get("message", "").strip()
    sender = data.get("sender", "spectator")
    if not room or not msg:
        return
    socketio.emit("chat_message", {"sender": sender, "message": msg}, room=room)

@socketio.on("typing")
def on_typing(data):
    room = data.get("room")
    sender = data.get("sender")
    if not room or not sender:
        return
    # broadcast to other clients in room
    socketio.emit("user_typing", {"sender": sender}, room=room, skip_sid=request.sid)

@socketio.on("stop_typing")
def on_stop_typing(data):
    room = data.get("room")
    sender = data.get("sender")
    if not room or not sender:
        return
    socketio.emit("user_stop_typing", {"sender": sender}, room=room, skip_sid=request.sid)

if __name__ == "__main__":
    print("=" * 50)
    print("🎮 Chess Master Server Starting (Stockfish OFF)...")
    print("⏰ Starting timeout watcher...")
    # ← FIXED: Start background timeout watcher
    threading.Thread(target=timeout_watcher, daemon=True).start()
    print("=" * 50)
    print("🔗 Open http://localhost:5000 in your browser")
    print("=" * 50)
    socketio.run(app)
