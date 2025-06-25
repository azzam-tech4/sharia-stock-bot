import sqlite3
import json
import time
import math
import logging
import os

logger = logging.getLogger(__name__)

# --- *** تم تعديل هذا الجزء ليتوافق مع أي سيرفر *** ---
# سيستخدم المسار المحدد في متغير البيئة DATA_PATH، وإذا لم يكن موجوداً، سيستخدم المجلد الحالي
DATA_PATH = os.environ.get('DATA_PATH', '.')
DB_FILE = os.path.join(DATA_PATH, "sharia_stock_bot.db")

# التأكد من وجود المجلد قبل إنشاء الاتصال
os.makedirs(DATA_PATH, exist_ok=True)
# --- نهاية التعديل ---

try:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()
    logger.info(f"Successfully connected to database: {DB_FILE}")
except sqlite3.Error as e:
    logger.error(f"Database connection error: {e}")
    exit()

def initialize_database():
    try:
        cursor.execute("PRAGMA user_version")
        current_version = cursor.fetchone()[0]
        logger.info(f"Database current version: {current_version}")
        if current_version < 1:
            logger.info("Migrating database to version 1...")
            # --- *** تم التعديل هنا لحذف القيمة الافتراضية للغة *** ---
            cursor.execute('''CREATE TABLE IF NOT EXISTS users (chat_id INTEGER PRIMARY KEY, language TEXT, last_request_time REAL DEFAULT 0, join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            try: cursor.execute("ALTER TABLE users ADD COLUMN first_name TEXT")
            except sqlite3.OperationalError: logger.warning("Column 'first_name' already exists in 'users'.")
            try: cursor.execute("ALTER TABLE users ADD COLUMN username TEXT")
            except sqlite3.OperationalError: logger.warning("Column 'username' already exists in 'users'.")

            cursor.execute('''CREATE TABLE IF NOT EXISTS searches (search_id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, symbol_searched TEXT NOT NULL, search_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (chat_id) REFERENCES users (chat_id) ON DELETE CASCADE)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS user_states (chat_id INTEGER PRIMARY KEY, state_data TEXT NOT NULL, FOREIGN KEY (chat_id) REFERENCES users (chat_id) ON DELETE CASCADE)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS stock_cache (symbol TEXT PRIMARY KEY, data TEXT NOT NULL, timestamp REAL NOT NULL)''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS report_cache (chat_id INTEGER PRIMARY KEY, symbol TEXT NOT NULL, data TEXT NOT NULL, FOREIGN KEY (chat_id) REFERENCES users (chat_id) ON DELETE CASCADE)''')

            cursor.execute("PRAGMA user_version = 1")
            conn.commit()
            logger.info("Database successfully migrated to version 1.")
    except sqlite3.Error as e:
        logger.error(f"Error during database initialization/migration: {e}")

def _clean_for_json(data):
    if isinstance(data, dict): return {k: _clean_for_json(v) for k, v in data.items()}
    if isinstance(data, (list, tuple)): return [_clean_for_json(i) for i in data]
    if isinstance(data, float) and math.isnan(data): return None
    return data

# --- *** بداية التعديل على دالة add_user_if_not_exists *** ---
def add_user_if_not_exists(chat_id: int, first_name: str, username: str) -> bool:
    """Returns True if a new user was created, False otherwise."""
    try:
        cursor.execute("INSERT OR IGNORE INTO users (chat_id, first_name, username) VALUES (?, ?, ?)", (chat_id, first_name, username))
        is_new_user = cursor.rowcount > 0  # .rowcount تكون 1 إذا تمت إضافة صف جديد، و 0 إذا لم يحدث شيء
        
        # نقوم بتحديث بيانات المستخدم القديم فقط إذا لم يكن جديداً
        if not is_new_user:
            cursor.execute("UPDATE users SET first_name = ?, username = ? WHERE chat_id = ?", (first_name, username, chat_id))
        
        conn.commit()
        return is_new_user
    except sqlite3.Error as e: 
        logger.error(f"Error adding/updating user {chat_id}: {e}")
        return False
# --- *** نهاية التعديل على دالة add_user_if_not_exists *** ---

def remove_user(chat_id: int):
    try:
        cursor.execute("DELETE FROM users WHERE chat_id = ?", (chat_id,))
        conn.commit()
        logger.info(f"Removed user {chat_id} from the database.")
    except sqlite3.Error as e: logger.error(f"Error removing user {chat_id}: {e}")

def get_all_user_chat_ids() -> list[int]:
    try:
        cursor.execute("SELECT chat_id FROM users")
        return [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Error fetching all user IDs: {e}")
        return []

def get_user_setting(chat_id: int, setting: str, default=None):
    try:
        if setting not in ['language', 'last_request_time', 'first_name']: raise ValueError("Invalid setting requested")
        cursor.execute(f"SELECT {setting} FROM users WHERE chat_id = ?", (chat_id,))
        result = cursor.fetchone()
        return result[0] if result else default
    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Error getting setting '{setting}' for user {chat_id}: {e}")
        return default

def set_user_setting(chat_id: int, setting: str, value):
    try:
        if setting not in ['language', 'last_request_time']: raise ValueError("Invalid setting requested")
        cursor.execute(f"UPDATE users SET {setting} = ? WHERE chat_id = ?", (value, chat_id))
        conn.commit()
    except (sqlite3.Error, ValueError) as e: logger.error(f"Error setting '{setting}' for user {chat_id}: {e}")

def get_user_state(chat_id: int) -> dict | None:
    try:
        cursor.execute("SELECT state_data FROM user_states WHERE chat_id = ?", (chat_id,))
        result = cursor.fetchone()
        return json.loads(result[0]) if result else None
    except (sqlite3.Error, json.JSONDecodeError) as e:
        logger.error(f"Error getting state for user {chat_id}: {e}")
        return None

def set_user_state(chat_id: int, state_data: dict):
    try:
        json_data = json.dumps(_clean_for_json(state_data))
        cursor.execute("INSERT OR REPLACE INTO user_states (chat_id, state_data) VALUES (?, ?)", (chat_id, json_data))
        conn.commit()
    except (sqlite3.Error, TypeError) as e: logger.error(f"Error setting state for user {chat_id}: {e}")

def clear_user_state(chat_id: int):
    try:
        cursor.execute("DELETE FROM user_states WHERE chat_id = ?", (chat_id,))
        conn.commit()
    except sqlite3.Error as e: logger.error(f"Error clearing state for user {chat_id}: {e}")

def get_cached_stock(symbol: str, ttl: int) -> tuple | None:
    try:
        cursor.execute("SELECT data, timestamp FROM stock_cache WHERE symbol = ?", (symbol,))
        result = cursor.fetchone()
        if result:
            data_json, timestamp = result
            if time.time() - timestamp < ttl: return tuple(json.loads(data_json))
        return None
    except (sqlite3.Error, json.JSONDecodeError) as e:
        logger.error(f"Error getting cache for symbol {symbol}: {e}")
        return None

def cache_stock(symbol: str, data: tuple):
    try:
        json_data = json.dumps(_clean_for_json(data))
        cursor.execute("INSERT OR REPLACE INTO stock_cache (symbol, data, timestamp) VALUES (?, ?, ?)", (symbol, json_data, time.time()))
        conn.commit()
    except (sqlite3.Error, TypeError) as e: logger.error(f"Error setting cache for symbol {symbol}: {e}")

def get_report_data(chat_id: int, symbol: str) -> dict | None:
    try:
        cursor.execute("SELECT data FROM report_cache WHERE chat_id = ? AND symbol = ?", (chat_id, symbol))
        result = cursor.fetchone()
        return json.loads(result[0]) if result else None
    except (sqlite3.Error, json.JSONDecodeError) as e:
        logger.error(f"Error getting report cache for user {chat_id}, symbol {symbol}: {e}")
        return None

def set_report_data(chat_id: int, symbol: str, data: dict):
    try:
        json_data = json.dumps(_clean_for_json(data))
        cursor.execute("INSERT OR REPLACE INTO report_cache (chat_id, symbol, data) VALUES (?, ?, ?)", (chat_id, symbol, json_data))
        conn.commit()
    except (sqlite3.Error, TypeError) as e: logger.error(f"Error setting report cache for user {chat_id}: {e}")

def log_search(chat_id: int, symbol: str):
    try:
        cursor.execute("INSERT INTO searches (chat_id, symbol_searched) VALUES (?, ?)", (chat_id, symbol))
        conn.commit()
    except sqlite3.Error as e: logger.error(f"Error logging search for user {chat_id}: {e}")

def get_bot_stats() -> dict:
    stats = {
        'total_users': 0, 'new_users_today': 0, 'new_users_week': 0, 'new_users_month': 0,
        'total_searches': 0, 'searches_today': 0, 'searches_yesterday': 0,
        'searches_this_week': 0, 'searches_last_week': 0, 'searches_this_month': 0,
        'searches_last_month': 0, 'searches_this_year': 0, 'searches_last_year': 0,
        'active_users_today': 0, 'active_users_week': 0, 'active_users_month': 0,
        'language_distribution': {}, 'top_stocks_overall': [], 'top_stocks_month': [],
        'top_stocks_week': [], 'top_stocks_day': []
    }
    try:
        # User stats
        cursor.execute("SELECT COUNT(*) FROM users"); stats['total_users'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE date(join_date) = date('now')"); stats['new_users_today'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE strftime('%Y-%W', join_date) = strftime('%Y-%W', 'now')"); stats['new_users_week'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM users WHERE strftime('%Y-%m', join_date) = strftime('%Y-%m', 'now')"); stats['new_users_month'] = cursor.fetchone()[0]

        # Search stats
        cursor.execute("SELECT COUNT(*) FROM searches"); stats['total_searches'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM searches WHERE date(search_time) = date('now')"); stats['searches_today'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM searches WHERE date(search_time) = date('now', '-1 day')"); stats['searches_yesterday'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM searches WHERE strftime('%Y-%W', search_time) = strftime('%Y-%W', 'now')"); stats['searches_this_week'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM searches WHERE strftime('%Y-%W', search_time) = strftime('%Y-%W', 'now', '-7 days')"); stats['searches_last_week'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM searches WHERE strftime('%Y-%m', search_time) = strftime('%Y-%m', 'now')"); stats['searches_this_month'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM searches WHERE strftime('%Y-%m', search_time) = strftime('%Y-%m', 'now', '-1 month')"); stats['searches_last_month'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM searches WHERE strftime('%Y', search_time) = strftime('%Y', 'now')"); stats['searches_this_year'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM searches WHERE strftime('%Y', search_time) = strftime('%Y', 'now', '-1 year')"); stats['searches_last_year'] = cursor.fetchone()[0]

        # Active users stats
        cursor.execute("SELECT COUNT(DISTINCT chat_id) FROM searches WHERE date(search_time) = date('now')"); stats['active_users_today'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT chat_id) FROM searches WHERE strftime('%Y-%W', search_time) = strftime('%Y-%W', 'now')"); stats['active_users_week'] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT chat_id) FROM searches WHERE strftime('%Y-%m', search_time) = strftime('%Y-%m', 'now')"); stats['active_users_month'] = cursor.fetchone()[0]

        # Language distribution
        cursor.execute("SELECT language, COUNT(*) FROM users GROUP BY language"); stats['language_distribution'] = {lang: count for lang, count in cursor.fetchall()}

        # Top stocks
        base_query = "SELECT symbol_searched, COUNT(*) as c FROM searches WHERE {} GROUP BY symbol_searched ORDER BY c DESC LIMIT 5"
        stats['top_stocks_overall'] = cursor.execute(base_query.format("1=1")).fetchall()
        stats['top_stocks_month'] = cursor.execute(base_query.format("strftime('%Y-%m', search_time) = strftime('%Y-%m', 'now')")).fetchall()
        stats['top_stocks_week'] = cursor.execute(base_query.format("strftime('%Y-%W', search_time) = strftime('%Y-%W', 'now')")).fetchall()
        stats['top_stocks_day'] = cursor.execute(base_query.format("date(search_time) = date('now')")).fetchall()
    except sqlite3.Error as e: logger.error(f"Error getting bot stats: {e}")
    return stats