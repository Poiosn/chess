"""
Simplified Single-Table Database Module for Chess Game Tracking
Everything stored in one 'games' table
"""

import os
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from contextlib import contextmanager

# Get database URL from environment variable
DATABASE_URL = os.environ.get('DATABASE_URL')

# Fix for Railway's postgres:// vs postgresql://
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)


# ===========================
# DATABASE CONNECTION
# ===========================

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Database error: {e}")
        raise e
    finally:
        if conn:
            conn.close()


def init_db():
    """Initialize database table"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Create games table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS games (
                        id SERIAL PRIMARY KEY,
                        room_name VARCHAR(100) NOT NULL,
                        game_status VARCHAR(20) DEFAULT 'active',
                        white_player_name VARCHAR(100),
                        black_player_name VARCHAR(100),
                        winner VARCHAR(10),
                        game_result VARCHAR(20),
                        total_moves INTEGER DEFAULT 0,
                        time_control INTEGER,
                        game_duration INTEGER,
                        is_bot_game BOOLEAN DEFAULT FALSE,
                        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        ended_at TIMESTAMP,
                        last_moves JSONB,
                        chat_history JSONB
                    )
                """)
                
                # Create indexes
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_games_room_name ON games(room_name);
                    CREATE INDEX IF NOT EXISTS idx_games_started_at ON games(started_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_games_status ON games(game_status);
                    CREATE INDEX IF NOT EXISTS idx_games_white_player ON games(white_player_name);
                    CREATE INDEX IF NOT EXISTS idx_games_black_player ON games(black_player_name);
                """)
                
        print("✅ Database table created successfully!")
        return True
    except Exception as e:
        print(f"❌ Error creating database table: {e}")
        return False


def test_connection():
    """Test database connection"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        print("✅ Database connection successful!")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


# ===========================
# GAME FUNCTIONS
# ===========================

def create_game(room_name, white_name, black_name, time_control, is_bot=False):
    """Create a new game record"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO games 
                    (room_name, white_player_name, black_player_name, 
                     time_control, is_bot_game, last_moves, chat_history)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (room_name, white_name, black_name, time_control, is_bot, 
                      Json([]), Json([])))
                
                game_id = cur.fetchone()[0]
                print(f"✅ Created game #{game_id}: {white_name} vs {black_name}")
                return game_id
    except Exception as e:
        print(f"❌ Error creating game: {e}")
        return None


def update_game_move(game_id, move_notation, move_number):
    """Update game with new move"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get current last_moves
                cur.execute("SELECT last_moves FROM games WHERE id = %s", (game_id,))
                result = cur.fetchone()
                
                if result:
                    moves = result[0] or []
                    moves.append({
                        'number': move_number,
                        'notation': move_notation,
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    # Keep only last 20 moves to save space
                    if len(moves) > 20:
                        moves = moves[-20:]
                    
                    # Update game
                    cur.execute("""
                        UPDATE games 
                        SET total_moves = %s, last_moves = %s
                        WHERE id = %s
                    """, (move_number, Json(moves), game_id))
                    
                    return True
        return False
    except Exception as e:
        print(f"❌ Error updating move: {e}")
        return False


def add_chat_message(game_id, player_name, player_color, message):
    """Add chat message to game"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get current chat history
                cur.execute("SELECT chat_history FROM games WHERE id = %s", (game_id,))
                result = cur.fetchone()
                
                if result:
                    chat = result[0] or []
                    chat.append({
                        'player': player_name,
                        'color': player_color,
                        'message': message,
                        'timestamp': datetime.now().isoformat()
                    })
                    
                    # Update game
                    cur.execute("""
                        UPDATE games 
                        SET chat_history = %s
                        WHERE id = %s
                    """, (Json(chat), game_id))
                    
                    return True
        return False
    except Exception as e:
        print(f"❌ Error adding chat: {e}")
        return False


def end_game(game_id, winner, result):
    """Mark game as completed"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get start time to calculate duration
                cur.execute("SELECT started_at FROM games WHERE id = %s", (game_id,))
                result_row = cur.fetchone()
                
                if result_row:
                    started_at = result_row[0]
                    ended_at = datetime.now()
                    duration = int((ended_at - started_at).total_seconds())
                    
                    # Update game
                    cur.execute("""
                        UPDATE games 
                        SET game_status = 'completed',
                            winner = %s,
                            game_result = %s,
                            ended_at = %s,
                            game_duration = %s
                        WHERE id = %s
                    """, (winner, result, ended_at, duration, game_id))
                    
                    print(f"✅ Game #{game_id} ended: {winner} wins by {result}")
                    return True
        return False
    except Exception as e:
        print(f"❌ Error ending game: {e}")
        return False


def abandon_game(game_id):
    """Mark game as abandoned"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE games 
                    SET game_status = 'abandoned'
                    WHERE id = %s
                """, (game_id,))
                return True
    except Exception as e:
        print(f"❌ Error abandoning game: {e}")
        return False


# ===========================
# QUERY FUNCTIONS
# ===========================

def get_game_by_id(game_id):
    """Get game details by ID"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM games WHERE id = %s
                """, (game_id,))
                return cur.fetchone()
    except Exception as e:
        print(f"❌ Error fetching game: {e}")
        return None


def get_active_game_by_room(room_name):
    """Get active game by room name"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM games 
                    WHERE room_name = %s AND game_status = 'active'
                    ORDER BY started_at DESC
                    LIMIT 1
                """, (room_name,))
                return cur.fetchone()
    except Exception as e:
        print(f"❌ Error fetching game: {e}")
        return None


def get_player_stats(player_name):
    """Get statistics for a player (aggregated from all games)"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_games,
                        SUM(CASE WHEN winner = player_color THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN winner != player_color AND winner != 'draw' THEN 1 ELSE 0 END) as losses,
                        SUM(CASE WHEN winner = 'draw' THEN 1 ELSE 0 END) as draws,
                        MAX(ended_at) as last_played
                    FROM (
                        SELECT 
                            'white' as player_color, 
                            winner, 
                            ended_at
                        FROM games 
                        WHERE white_player_name = %s AND game_status = 'completed'
                        
                        UNION ALL
                        
                        SELECT 
                            'black' as player_color, 
                            winner, 
                            ended_at
                        FROM games 
                        WHERE black_player_name = %s AND game_status = 'completed'
                    ) as player_games
                """, (player_name, player_name))
                
                stats = cur.fetchone()
                
                if stats and stats['total_games'] > 0:
                    win_rate = (stats['wins'] / stats['total_games']) * 100
                    return {
                        'player_name': player_name,
                        'total_games': stats['total_games'],
                        'wins': stats['wins'],
                        'losses': stats['losses'],
                        'draws': stats['draws'],
                        'win_rate': round(win_rate, 2),
                        'last_played': stats['last_played'].isoformat() if stats['last_played'] else None
                    }
                else:
                    return {
                        'player_name': player_name,
                        'total_games': 0,
                        'wins': 0,
                        'losses': 0,
                        'draws': 0,
                        'win_rate': 0,
                        'last_played': None
                    }
    except Exception as e:
        print(f"❌ Error fetching player stats: {e}")
        return None


def get_player_games(player_name, limit=20):
    """Get all games for a specific player"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        id, room_name, white_player_name, black_player_name,
                        winner, game_result, total_moves, game_duration,
                        started_at, ended_at
                    FROM games
                    WHERE (white_player_name = %s OR black_player_name = %s)
                    AND game_status = 'completed'
                    ORDER BY ended_at DESC
                    LIMIT %s
                """, (player_name, player_name, limit))
                
                games = cur.fetchall()
                return [dict(game) for game in games]
    except Exception as e:
        print(f"❌ Error fetching player games: {e}")
        return []


def get_recent_games(limit=20):
    """Get recent completed games"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        id, room_name, white_player_name, black_player_name,
                        winner, game_result, total_moves, game_duration,
                        started_at, ended_at, is_bot_game
                    FROM games
                    WHERE game_status = 'completed'
                    ORDER BY ended_at DESC
                    LIMIT %s
                """, (limit,))
                
                games = cur.fetchall()
                return [dict(game) for game in games]
    except Exception as e:
        print(f"❌ Error fetching recent games: {e}")
        return []


def get_leaderboard(limit=10):
    """Get top players by wins"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        player_name,
                        COUNT(*) as total_games,
                        SUM(CASE WHEN winner = player_color THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN winner != player_color AND winner != 'draw' THEN 1 ELSE 0 END) as losses,
                        SUM(CASE WHEN winner = 'draw' THEN 1 ELSE 0 END) as draws,
                        ROUND(
                            (SUM(CASE WHEN winner = player_color THEN 1 ELSE 0 END)::DECIMAL / 
                             COUNT(*)) * 100, 2
                        ) as win_rate
                    FROM (
                        SELECT white_player_name as player_name, 'white' as player_color, winner
                        FROM games WHERE game_status = 'completed' AND white_player_name IS NOT NULL
                        
                        UNION ALL
                        
                        SELECT black_player_name as player_name, 'black' as player_color, winner
                        FROM games WHERE game_status = 'completed' AND black_player_name IS NOT NULL
                    ) as all_games
                    GROUP BY player_name
                    HAVING COUNT(*) > 0
                    ORDER BY wins DESC, win_rate DESC
                    LIMIT %s
                """, (limit,))
                
                leaderboard = cur.fetchall()
                return [dict(player) for player in leaderboard]
    except Exception as e:
        print(f"❌ Error fetching leaderboard: {e}")
        return []


def get_today_stats():
    """Get statistics for today"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_games,
                        COUNT(DISTINCT white_player_name) + COUNT(DISTINCT black_player_name) as unique_players,
                        AVG(game_duration) as avg_duration,
                        AVG(total_moves) as avg_moves
                    FROM games
                    WHERE DATE(started_at) = CURRENT_DATE
                    AND game_status = 'completed'
                """)
                
                return dict(cur.fetchone())
    except Exception as e:
        print(f"❌ Error fetching today's stats: {e}")
        return None


# ===========================
# MAIN (for testing)
# ===========================

if __name__ == "__main__":
    print("🗄️ Testing Database Connection...")
    
    if test_connection():
        print("\n📊 Initializing Database...")
        init_db()
        
        print("\n✅ Database ready!")
        print("\nAvailable functions:")
        print("- create_game(room, white_name, black_name, time_control, is_bot)")
        print("- update_game_move(game_id, move_notation, move_number)")
        print("- add_chat_message(game_id, player_name, player_color, message)")
        print("- end_game(game_id, winner, result)")
        print("- get_player_stats(player_name)")
        print("- get_recent_games(limit)")
        print("- get_leaderboard(limit)")
    else:
        print("\n❌ Database connection failed!")
