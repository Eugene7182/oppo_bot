import sqlite3
import re
from difflib import SequenceMatcher
from datetime import datetime
from pytz import timezone

DB_NAME = "sales.db"
tz = timezone("Asia/Almaty")

# =========================
# --- ИНИЦИАЛИЗАЦИЯ/МИГРАЦИИ
# =========================
def init():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # sales
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            model TEXT,
            memory TEXT,
            qty INTEGER,
            network TEXT,
            date TEXT
        )
    """)

    # stocks
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            username TEXT,
            item TEXT,
            qty INTEGER,
            network TEXT,
            updated_at TEXT,
            PRIMARY KEY (username, item, network)
        )
    """)

    # admins
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            username TEXT PRIMARY KEY
        )
    """)

    # networks
    cur.execute("""
        CREATE TABLE IF NOT EXISTS networks (
            username TEXT PRIMARY KEY,
            network TEXT
        )
    """)

    # plans v2: с ключом YM
    cur.execute("""
        CREATE TABLE IF NOT EXISTS plans (
            username TEXT,
            ym TEXT,
            plan INTEGER,
            PRIMARY KEY (username, ym)
        )
    """)

    # --- Миграция со старой схемы plans (username, plan) ---
    # если обнаружится старая таблица без ym, попробуем её распознать и перенести
    cur.execute("PRAGMA table_info(plans)")
    cols = [r[1] for r in cur.fetchall()]
    if cols == ["username", "plan"]:  # старая схема
        # создаём новую
        cur.execute("""
            CREATE TABLE IF NOT EXISTS plans_v2 (
                username TEXT,
                ym TEXT,
                plan INTEGER,
                PRIMARY KEY (username, ym)
            )
        """)
        # переносим в текущий месяц
        ym = datetime.now(tz).strftime("%Y-%m")
        cur.execute("INSERT OR IGNORE INTO plans_v2 (username, ym, plan) SELECT username, ?, plan FROM plans", (ym,))
        # подменяем
        cur.execute("DROP TABLE plans")
        cur.execute("ALTER TABLE plans_v2 RENAME TO plans")

    conn.commit()
    conn.close()

# =========================
# --- АДМИНЫ ---
# =========================
def add_admin(username: str):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("INSERT OR IGNORE INTO admins (username) VALUES (?)", (username,))

def remove_admin(username: str):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("DELETE FROM admins WHERE username=?", (username,))

def is_admin(username: str) -> bool:
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute("SELECT 1 FROM admins WHERE username=?", (username,))
        return bool(cur.fetchone())

def list_admins():
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute("SELECT username FROM admins")
        return [r[0] for r in cur.fetchall()]

# back-compat
def get_admins():
    return list_admins()

# =========================
# --- СЕТИ ---
# =========================
def set_network(username: str, network: str):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            INSERT INTO networks (username, network)
            VALUES (?, ?)
            ON CONFLICT(username) DO UPDATE SET network=excluded.network
        """, (username, network))

def get_network(username: str) -> str:
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute("SELECT network FROM networks WHERE username=?", (username,))
        row = cur.fetchone()
        return row[0] if row else "-"

# =========================
# --- ПЛАНЫ ---
# =========================
def set_plan(username: str, ym: str, plan: int):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            INSERT INTO plans (username, ym, plan)
            VALUES (?, ?, ?)
            ON CONFLICT(username, ym) DO UPDATE SET plan=excluded.plan
        """, (username, ym, plan))

def get_plan(username: str, ym: str) -> int:
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute("SELECT plan FROM plans WHERE username=? AND ym=?", (username, ym))
        row = cur.fetchone()
        return int(row[0]) if row else 0

def get_all_plans(ym: str) -> dict:
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute("SELECT username, plan FROM plans WHERE ym=?", (ym,))
        return {u: int(p) for u, p in cur.fetchall()}

# =========================
# --- ПРОДАЖИ ---
# =========================
def _today() -> str:
    return datetime.now(tz).strftime("%Y-%m-%d")

def add_sale(username: str, model: str, memory: str, qty: int, network: str):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            INSERT INTO sales (username, model, memory, qty, network, date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (username, model, memory, qty, network, _today()))

def month_sales(year: int, month: int, username: str | None = None):
    ym = f"{year:04d}-{month:02d}"
    params = [ym + "%"]
    where = "date LIKE ?"
    if username:
        where += " AND username=?"
        params.append(username)
    by_model = {}
    total = 0
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute(f"""
            SELECT model, SUM(qty)
            FROM sales
            WHERE {where}
            GROUP BY model
        """, params)
        for m, s in cur.fetchall():
            s = int(s or 0)
            by_model[m] = s
            total += s
    return total, by_model

def get_last_sale(username: str):
    """Back-compat: вернуть дату последней продажи строкой YYYY-MM-DD."""
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute("SELECT date FROM sales WHERE username=? ORDER BY date DESC LIMIT 1", (username,))
        row = cur.fetchone()
        return row[0] if row else None

def get_last_sale_time(username: str):
    """Современный вариант: вернуть datetime с TZ."""
    s = get_last_sale(username)
    if not s:
        return None
    return tz.localize(datetime.strptime(s, "%Y-%m-%d"))

def reset_monthly_sales():
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("DELETE FROM sales")

# =========================
# --- СТОКИ ---
# =========================
def update_stock(username: str, item: str, qty: int, network: str):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("""
            INSERT INTO stocks (username, item, qty, network, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(username, item, network)
            DO UPDATE SET qty=excluded.qty, updated_at=excluded.updated_at
        """, (username, item, qty, network, datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")))

def get_stock_qty(username: str, item: str, network: str):
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute("""
            SELECT qty FROM stocks
            WHERE username=? AND item=? AND network=?
            ORDER BY updated_at DESC
            LIMIT 1
        """, (username, item, network))
        row = cur.fetchone()
        return int(row[0]) if row else None

def decrease_stock(username: str, item: str, qty: int, network: str):
    with sqlite3.connect(DB_NAME) as conn:
        # не уходим в минус
        cur = conn.execute("""
            SELECT qty FROM stocks WHERE username=? AND item=? AND network=?
        """, (username, item, network))
        row = cur.fetchone()
        if not row:
            return
        new_qty = max(0, int(row[0]) - qty)
        conn.execute("""
            UPDATE stocks SET qty=?, updated_at=? WHERE username=? AND item=? AND network=?
        """, (new_qty, datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S"), username, item, network))

def get_stocks(username: str | None = None, network: str | None = None):
    """
    Если передан username (и/или network) — вернуть [(username, item, qty, network)].
    Если не передано — вернуть полный список с updated_at для универсальных проверок.
    """
    with sqlite3.connect(DB_NAME) as conn:
        if username or network:
            where = []
            params = []
            if username:
                where.append("username=?")
                params.append(username)
            if network:
                where.append("network=?")
                params.append(network)
            where_sql = "WHERE " + " AND ".join(where) if where else ""
            cur = conn.execute(f"""
                SELECT username, item, qty, network
                FROM stocks
                {where_sql}
                ORDER BY updated_at DESC
            """, params)
            return [(u, i, int(q), n) for (u, i, q, n) in cur.fetchall()]
        else:
            cur = conn.execute("""
                SELECT username, item, qty, network, updated_at
                FROM stocks
                ORDER BY updated_at DESC
            """)
            return [(u, i, int(q), n, t) for (u, i, q, n, t) in cur.fetchall()]

# =========================
# --- ФУЗЗИ ПОИСК СТОКА ---
# =========================
_norm_rx = re.compile(r"[a-z0-9]+")

def _normalize(s: str) -> str:
    return "".join(_norm_rx.findall(s.lower()))

def find_stock_like(username: str, model_norm: str, memory: str, network: str = "-"):
    """
    Ищем лучший матч среди стоков пользователя:
    - сравниваем нормализованную модель со стоковым item
    - память (128/256/512...) должна совпасть по числу
    - выбираем максимум по similarity ratio
    - порог 0.55, чтобы не хватать мусор
    Возвращаем (item_name, qty) или (None, 0).
    """
    candidates = get_stocks(username=username, network=network)
    best_item = None
    best_score = 0.0
    best_qty = 0
    for _, item, qty, _net in candidates:
        mem_match = re.search(r"(\d{2,4})", item)
        if not mem_match or mem_match.group(1) != str(memory):
            continue
        item_key = _normalize(item)
        score = SequenceMatcher(None, item_key, model_norm).ratio()
        if score > best_score:
            best_score = score
            best_item = item
            best_qty = int(qty)
    if best_item and best_score >= 0.55:
        return best_item, best_qty
    return None, 0

# =========================
# --- ПРОЧЕЕ ---
# =========================
def get_all_known_users():
    with sqlite3.connect(DB_NAME) as conn:
        cur = conn.execute("""
            SELECT username FROM sales
            UNION
            SELECT username FROM stocks
            UNION
            SELECT username FROM plans
            UNION
            SELECT username FROM networks
            UNION
            SELECT username FROM admins
        """)
        return [r[0] for r in cur.fetchall()]
