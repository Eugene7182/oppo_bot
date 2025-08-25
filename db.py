import os
import sqlite3
import threading
from datetime import datetime
from difflib import SequenceMatcher
from typing import Optional, Tuple, List, Dict

DB_PATH = os.getenv("DB_PATH", "db.sqlite3")

_lock = threading.Lock()

def _connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.row_factory = sqlite3.Row
    return conn

def init():
    with _lock, _connect() as cx:
        cx.executescript(
            """
            CREATE TABLE IF NOT EXISTS admins(
                username TEXT PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS user_networks(
                username TEXT PRIMARY KEY,
                network  TEXT NOT NULL DEFAULT '-'
            );

            CREATE TABLE IF NOT EXISTS stocks(
                username TEXT NOT NULL,
                item     TEXT NOT NULL,
                qty      INTEGER NOT NULL DEFAULT 0,
                network  TEXT NOT NULL DEFAULT '-',
                PRIMARY KEY(username, item, network)
            );

            CREATE TABLE IF NOT EXISTS sales(
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                model    TEXT NOT NULL,   -- normalized model: 'reno11f5g'
                memory   TEXT NOT NULL,   -- '128'
                qty      INTEGER NOT NULL,
                network  TEXT NOT NULL DEFAULT '-',
                ts       TEXT NOT NULL    -- ISO timestamp UTC-like (we keep local Asia/Almaty textual time ok)
            );

            CREATE INDEX IF NOT EXISTS idx_sales_user_month ON sales(username, ts);
            CREATE INDEX IF NOT EXISTS idx_sales_model_mem ON sales(model, memory);

            CREATE TABLE IF NOT EXISTS plans(
                username TEXT NOT NULL,
                ym       TEXT NOT NULL,   -- 'YYYY-MM'
                plan     INTEGER NOT NULL,
                PRIMARY KEY(username, ym)
            );
            """
        )

# ================
# --- Админы ---
# ================
def is_admin(username: str) -> bool:
    username = (username or "").lstrip("@")
    with _lock, _connect() as cx:
        r = cx.execute("SELECT 1 FROM admins WHERE username=?", (username,)).fetchone()
        return r is not None

def add_admin(username: str):
    username = username.lstrip("@")
    with _lock, _connect() as cx:
        cx.execute("INSERT OR IGNORE INTO admins(username) VALUES (?)", (username,))

def remove_admin(username: str):
    username = username.lstrip("@")
    with _lock, _connect() as cx:
        cx.execute("DELETE FROM admins WHERE username=?", (username,))

def list_admins() -> List[str]:
    with _lock, _connect() as cx:
        rows = cx.execute("SELECT username FROM admins ORDER BY username").fetchall()
        return [r["username"] for r in rows]

# ==================
# --- Сети (bind)
# ==================
def set_network(username: str, network: str):
    username = username.lstrip("@")
    net = network if network else "-"
    with _lock, _connect() as cx:
        cx.execute(
            "INSERT INTO user_networks(username, network) VALUES(?, ?) "
            "ON CONFLICT(username) DO UPDATE SET network=excluded.network",
            (username, net)
        )

def get_network(username: str) -> Optional[str]:
    username = username.lstrip("@")
    with _lock, _connect() as cx:
        r = cx.execute("SELECT network FROM user_networks WHERE username=?", (username,)).fetchone()
        return r["network"] if r else None

# ==================
# --- Стоки
# ==================
def update_stock(username: str, item: str, qty: int, network: str = "-"):
    username = username.lstrip("@")
    net = network if network else "-"
    with _lock, _connect() as cx:
        cx.execute(
            "INSERT INTO stocks(username, item, qty, network) VALUES(?, ?, ?, ?) "
            "ON CONFLICT(username, item, network) DO UPDATE SET qty=excluded.qty",
            (username, item, int(qty), net)
        )

def get_stocks(username: Optional[str] = None, network: Optional[str] = None) -> List[tuple]:
    # Возвращаем как [(username, item, qty, network), ...]
    q = "SELECT username, item, qty, network FROM stocks"
    cond = []
    args = []
    if username:
        cond.append("username=?")
        args.append(username.lstrip("@"))
    if network:
        cond.append("network=?")
        args.append(network)
    if cond:
        q += " WHERE " + " AND ".join(cond)
    q += " ORDER BY username, item"
    with _lock, _connect() as cx:
        rows = cx.execute(q, args).fetchall()
        return [(r["username"], r["item"], r["qty"], r["network"]) for r in rows]

def decrease_stock(username: str, item: str, qty: int, network: str = "-"):
    username = username.lstrip("@")
    net = network if network else "-"
    with _lock, _connect() as cx:
        r = cx.execute(
            "SELECT qty FROM stocks WHERE username=? AND item=? AND network=?",
            (username, item, net)
        ).fetchone()
        if not r:
            return
        newq = max(0, int(r["qty"]) - int(qty))
        cx.execute(
            "UPDATE stocks SET qty=? WHERE username=? AND item=? AND network=?",
            (newq, username, item, net)
        )

def _normalize(s: str) -> str:
    return "".join(re.findall(r"[a-z0-9]+", s.lower()))

def find_stock_like(username: str, model_norm: str, memory: str, network: str = "-") -> Tuple[Optional[str], int]:
    """
    Ищем лучший матч среди стоков пользователя по близости к model_norm + совпадение памяти.
    Возвращаем (item_name, qty) или (None, 0)
    """
    username = username.lstrip("@")
    net = network if network else "-"
    candidates = get_stocks(username=username, network=net)
    best_item = None
    best_score = 0.0
    best_qty = 0
    for _, item, qty, _ in candidates:
        # item типа "Reno 11F 5G 128"
        mem_match = re.search(r"(\d{2,4})", item)
        if not mem_match:
            continue
        mem = mem_match.group(1)
        if mem != str(memory):
            continue
        item_key = _normalize(item)
        score = SequenceMatcher(None, item_key, model_norm).ratio()
        if score > best_score:
            best_score = score
            best_item = item
            best_qty = int(qty)
    if best_item and best_score >= 0.55:  # порог разумный
        return best_item, best_qty
    return None, 0

# ==================
# --- Продажи
# ==================
def add_sale(username: str, model_norm: str, memory: str, qty: int, network: str = "-"):
    username = username.lstrip("@")
    net = network if network else "-"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _lock, _connect() as cx:
        cx.execute(
            "INSERT INTO sales(username, model, memory, qty, network, ts) VALUES(?, ?, ?, ?, ?, ?)",
            (username, model_norm, str(memory), int(qty), net, ts)
        )

def _month_bounds(year: int, month: int) -> tuple[str, str]:
    from calendar import monthrange
    start = f"{year:04d}-{month:02d}-01 00:00:00"
    last_day = monthrange(year, month)[1]
    end = f"{year:04d}-{month:02d}-{last_day:02d} 23:59:59"
    return start, end

def month_sales(year: int, month: int, username: Optional[str] = None) -> tuple[int, Dict[str, int]]:
    start, end = _month_bounds(year, month)
    q = "SELECT username, model, memory, SUM(qty) s FROM sales WHERE ts BETWEEN ? AND ?"
    args = [start, end]
    if username:
        q += " AND username=?"
        args.append(username.lstrip("@"))
    q += " GROUP BY username, model, memory"
    total = 0
    by_model: Dict[str, int] = {}
    with _lock, _connect() as cx:
        rows = cx.execute(q, args).fetchall()
        for r in rows:
            s = int(r["s"])
            total += s
            key = f"{r['model']} {r['memory']}"
            by_model[key] = by_model.get(key, 0) + s
    return total, by_model

def get_last_sale_time(username: str) -> Optional[datetime]:
    username = username.lstrip("@")
    with _lock, _connect() as cx:
        r = cx.execute(
            "SELECT ts FROM sales WHERE username=? ORDER BY ts DESC LIMIT 1",
            (username,)
        ).fetchone()
        if not r:
            return None
        # ts сохранён локальным текстом "YYYY-MM-DD HH:MM:SS"
        try:
            return datetime.strptime(r["ts"], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

def get_all_known_users() -> List[str]:
    with _lock, _connect() as cx:
        r1 = cx.execute("SELECT DISTINCT username FROM stocks").fetchall()
        r2 = cx.execute("SELECT DISTINCT username FROM sales").fetchall()
        names = {row["username"] for row in r1} | {row["username"] for row in r2}
        return sorted(names)

# ===========
# --- Планы
# ===========
def set_plan(username: str, ym: str, plan: int):
    username = username.lstrip("@")
    ym = ym[:7]
    with _lock, _connect() as cx:
        cx.execute(
            "INSERT INTO plans(username, ym, plan) VALUES(?, ?, ?) "
            "ON CONFLICT(username, ym) DO UPDATE SET plan=excluded.plan",
            (username, ym, int(plan))
        )

def get_plan(username: str, ym: str) -> Optional[int]:
    username = username.lstrip("@")
    ym = ym[:7]
    with _lock, _connect() as cx:
        r = cx.execute("SELECT plan FROM plans WHERE username=? AND ym=?", (username, ym)).fetchone()
        return int(r["plan"]) if r else None

def get_all_plans(ym: str) -> Dict[str, int]:
    ym = ym[:7]
    with _lock, _connect() as cx:
        rows = cx.execute("SELECT username, plan FROM plans WHERE ym=?", (ym,)).fetchall()
        return {r["username"]: int(r["plan"]) for r in rows}
