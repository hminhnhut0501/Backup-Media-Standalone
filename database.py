import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("TELEVAULT_DB_PATH") or Path(__file__).parent.resolve() / "televault_v2.db")

def get_db_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn

def init_db():
    conn = get_db_connection()
    conn.execute('''CREATE TABLE IF NOT EXISTS saved_targets (link TEXT PRIMARY KEY)''')
    conn.commit()
    conn.close()

# Tự động chạy khởi tạo DB
init_db()
