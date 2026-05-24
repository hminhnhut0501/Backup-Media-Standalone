import configparser
import asyncio
import os
import sqlite3
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession

try:
    import cryptg
    CRYPTG_AVAILABLE = True
except ImportError:
    CRYPTG_AVAILABLE = False

ROOT = Path(__file__).parent.resolve()
CONFIG_PATH = ROOT / "config_filter.ini"
DB_PATH = Path(os.getenv("TELEVAULT_DB_PATH") or ROOT / "televault_v2.db")

client_instances = {}
client_locks = {}

def ensure_accounts_db():
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS tg_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            api_id INTEGER,
            api_hash TEXT,
            phone TEXT,
            session_path TEXT UNIQUE,
            is_active INTEGER DEFAULT 0,
            status TEXT DEFAULT 'Chưa kiểm tra',
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )''')
        conn.commit()

def default_account_from_config():
    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")
    session = os.getenv("TG_SESSION_PATH", "sessions/filter")
    if api_id and api_hash:
        return {
            "id": 0,
            "name": "Env mặc định",
            "api_id": int(api_id),
            "api_hash": api_hash.strip(),
            "phone": "",
            "session_path": session.strip(),
            "is_active": 1,
            "status": "Từ biến môi trường",
        }

    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_PATH)
    return {
        "id": 0,
        "name": "Config mặc định",
        "api_id": int(cfg["Access"]["id"]),
        "api_hash": cfg["Access"]["hash"].strip(),
        "phone": "",
        "session_path": cfg["Access"].get("session", "sessions/filter").strip(),
        "is_active": 1,
        "status": "Từ config_filter.ini",
    }

def get_active_account():
    ensure_accounts_db()
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM tg_accounts WHERE is_active = 1 ORDER BY id DESC LIMIT 1").fetchone()
        if row:
            return dict(row)
    return default_account_from_config()

async def get_tg_client(account_id=None):
    string_session = os.getenv("TG_STRING_SESSION")
    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")
    if string_session and api_id and api_hash:
        key = "env:string_session"
        client = client_instances.get(key)
        if client and client.is_connected():
            return client

        lock = client_locks.setdefault(key, asyncio.Lock())
        async with lock:
            client = client_instances.get(key)
            if client and client.is_connected():
                return client
            client = TelegramClient(
                StringSession(string_session.strip()),
                int(api_id),
                api_hash.strip(),
                connection_retries=None,
                auto_reconnect=True,
                flood_sleep_threshold=60,
            )
            await client.start()
            client_instances[key] = client
            if CRYPTG_AVAILABLE:
                print("⚡ CRYPTG OK: MTProto download/upload acceleration enabled")
            else:
                print("⚠️ cryptg chưa có, download/upload sẽ chậm hơn")
            return client

    ensure_accounts_db()
    if account_id is None:
        acc = get_active_account()
    elif int(account_id) == 0:
        acc = default_account_from_config()
    else:
        with sqlite3.connect(str(DB_PATH)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM tg_accounts WHERE id = ?", (account_id,)).fetchone()
            if not row:
                raise RuntimeError("Không tìm thấy Telegram account")
            acc = dict(row)

    key = str(acc["session_path"])
    client = client_instances.get(key)
    if client and client.is_connected():
        return client

    lock = client_locks.setdefault(key, asyncio.Lock())
    async with lock:
        client = client_instances.get(key)
        if client and client.is_connected():
            return client

        session_path = acc["session_path"]
        session_dir = os.path.dirname(session_path)
        if session_dir and not os.path.exists(session_dir):
            os.makedirs(session_dir, exist_ok=True)

        last_error = None
        for attempt in range(6):
            client = TelegramClient(
                session_path,
                int(acc["api_id"]),
                acc["api_hash"].strip(),
                connection_retries=None,
                auto_reconnect=True,
                flood_sleep_threshold=60,
            )
            try:
                await client.start()
                last_error = None
                break
            except sqlite3.OperationalError as e:
                last_error = e
                try:
                    await client.disconnect()
                except Exception:
                    pass
                if "database is locked" not in str(e).lower() or attempt >= 5:
                    raise
                await asyncio.sleep(0.5 + attempt * 0.5)
        if last_error:
            raise last_error
        client_instances[key] = client

        if CRYPTG_AVAILABLE:
            print("⚡ CRYPTG OK: MTProto download/upload acceleration enabled")
        else:
            print("⚠️ cryptg chưa có, download/upload sẽ chậm hơn")
        return client
