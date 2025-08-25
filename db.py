import os
import re
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pytz import timezone

DB_NAME = os.getenv("DB_NAME", "sales.db")
TZ = timezone("Asia/Almaty")

# -----------------------
# Вспомогательные утилиты
# -----------------------
def _conn():
    return sqlite3.connect(DB_NAME)

def _now_date() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")

def _now_dt_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def _normalize_model(s: str) -> str:
    # "Reno 11F 5G" -> "reno11f5g"
    return re.sub(r"[^a-z0-9]", "", s.lower())

def _normalize_item(item: str) -> Tuple[str, str]:
    # "Reno 11F 5G 128" -> ("reno11f5g", "128")
    item = re.sub(r"\s+", " ", item.strip())
    parts = item.split(" ")
    if not parts:
        return "", ""
    memory = ""
    if parts and re.fullmatch(r"\d{2,4}", parts[-1]):
        memory = parts[-1]
        parts = parts[:-1]
    model_norm = _normalize_model("".join(parts))
    return model_norm, memory

def _month_prefix(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"

# -------------
# Инициализация
# -------------
def init():
    conn = _conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            model TEXT NOT NULL,
            memory TEXT NOT NULL,
            qty INTEGER NOT NULL,
            network TEXT,
            date TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            username TEXT NOT NULL,
            item TEXT NOT NULL,
            qty INTEGER NOT NULL,
            network TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (username, item, network)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            username TEXT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS plans (
            username TEXT NOT NULL,
            ym TEXT NOT NULL,
            plan INTEGER NOT NULL,
            PRIMARY KEY (username, ym)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS networks (
            username TEXT PRIMARY KEY,
            network TEXT NOT NULL
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_user_date ON sales(username, date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stocks_user_net ON stocks(username, network)")

    conn.commit()
    conn.close()

# -------------
#   Админы
# -------------
def add_admin(username: str):
    conn = _conn(); cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO admins (username) VALUES (?)", (username.lstrip("@"),))
    conn.commit(); conn.close()

def remove_admin(username: str):
    conn = _conn(); cur = conn.cursor()
    cur.execute("DELETE FROM admins WHERE username=?", (username.lstrip("@"),))
    conn.commit(); conn.close()

# Ретро-алиас
def del_admin(username: str):
    remove_admin(username)

def is_admin(username: str) -> bool:
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM admins WHERE username=?", (username.lstrip("@"),))
    row = cur.fetchone(); conn.close()
    return bool(row)

def get_admins() -> List[str]:
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT username FROM admins")
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows

def list_admins() -> List[str]:
    return get_admins()

# -------------
#   Сети
# -------------
def set_network(username: str, network: str):
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO networks (username, network)
        VALUES (?, ?)
        ON CONFLICT(username) DO UPDATE SET network=excluded.network
    """, (username, network))
    conn.commit(); conn.close()

def get_network(username: str) -> str:
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT network FROM networks WHERE username=?", (username,))
    row = cur.fetchone(); conn.close()
    return row[0] if row else "-"

def find_users_by_network(network: str) -> list[str]:
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT username FROM networks WHERE network=?", (network,))
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows

def get_user_by_network(network: str) -> Optional[str]:
    users = find_users_by_network(network)
    if len(users) == 1:
        return users[0]
    return None

# -------------
#   Планы
# -------------
def set_plan(username: str, ym: str, plan: int):
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO plans (username, ym, plan)
        VALUES (?, ?, ?)
        ON CONFLICT(username, ym) DO UPDATE SET plan=excluded.plan
    """, (username, ym, int(plan)))
    conn.commit(); conn.close()

def get_plan(username: str, ym: str) -> Optional[int]:
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT plan FROM plans WHERE username=? AND ym=?", (username, ym))
    row = cur.fetchone(); conn.close()
    return int(row[0]) if row else None

def get_all_plans(ym: str) -> Dict[str, int]:
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT username, plan FROM plans WHERE ym=?", (ym,))
    res = {u: int(p) for u, p in cur.fetchall()}
    conn.close()
    return res

# -------------
#   Продажи
# -------------
def add_sale(username: str, model: str, memory: str, qty: int, network: str):
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO sales (username, model, memory, qty, network, date, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (username, _normalize_model(model), str(memory), int(qty), network, _now_date(), _now_dt_str()))
    conn.commit(); conn.close()

def month_sales(year: int, month: int, username: Optional[str] = None) -> Tuple[int, Dict[str, int]]:
    ym = f"{year:04d}-{month:02d}"
    conn = _conn(); cur = conn.cursor()
    if username:
        username = username.lstrip("@")
        cur.execute("""
            SELECT model, SUM(qty)
            FROM sales
            WHERE username=? AND date LIKE ? || '%'
            GROUP BY model
        """, (username, ym))
    else:
        cur.execute("""
            SELECT model, SUM(qty)
            FROM sales
            WHERE date LIKE ? || '%'
            GROUP BY model
        """, (ym,))
    rows = cur.fetchall()
    by_model = {m: int(s) for m, s in rows}
    total = sum(by_model.values())
    conn.close()
    return total, by_model

def get_last_sale(username: str) -> Optional[str]:
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT date FROM sales WHERE username=? ORDER BY date DESC LIMIT 1", (username,))
    row = cur.fetchone(); conn.close()
    return row[0] if row else None

def get_last_sale_time(username: str) -> Optional[datetime]:
    s = get_last_sale(username)
    if not s:
        return None
    dt = datetime.strptime(s, "%Y-%m-%d")
    return TZ.localize(dt)

def reset_monthly_sales():
    """
    Авто-очистка продаж (как просил заказчик).
    Если нужно хранить историю — заменим на удаление только прошлых месяцев.
    """
    conn = _conn(); cur = conn.cursor()
    cur.execute("DELETE FROM sales")
    conn.commit(); conn.close()

def set_monthly_fact(username: str, ym: str, qty: int, network: str = "-"):
    """
    Жёстко задаёт факт за месяц: удаляет ВСЕ продажи пользователя в этом месяце
    и вставляет одну 'manual' запись.
    """
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("DELETE FROM sales WHERE username=? AND date LIKE ? || '%'", (username, ym))
    cur.execute("""
        INSERT INTO sales (username, model, memory, qty, network, date, created_at)
        VALUES (?, 'manual', '-', ?, ?, ?, ?)
    """, (username, int(qty), network, f"{ym}-01", _now_dt_str()))
    conn.commit(); conn.close()

def add_monthly_fact(username: str, ym: str, qty: int, network: str = "-"):
    """
    Добавляет к факту за месяц: вставляет дополнительную 'manual' запись.
    """
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO sales (username, model, memory, qty, network, date, created_at)
        VALUES (?, 'manual', '-', ?, ?, ?, ?)
    """, (username, int(qty), network, f"{ym}-01", _now_dt_str()))
    conn.commit(); conn.close()

# -------------
#   Стоки
# -------------
def update_stock(username: str, item: str, qty: int, network: str):
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO stocks (username, item, qty, network, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(username, item, network)
        DO UPDATE SET qty=excluded.qty, updated_at=excluded.updated_at
    """, (username, item.strip(), int(qty), network or "-", _now_dt_str()))
    conn.commit(); conn.close()

def get_stock_qty(username: str, item: str, network: str) -> Optional[int]:
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("""
        SELECT qty FROM stocks
        WHERE username=? AND item=? AND network=?
        ORDER BY updated_at DESC
        LIMIT 1
    """, (username, item.strip(), network or "-"))
    row = cur.fetchone(); conn.close()
    return int(row[0]) if row else None

def decrease_stock(username: str, item: str, qty: int, network: str):
    username = username.lstrip("@")
    conn = _conn(); cur = conn.cursor()
    cur.execute("""
        UPDATE stocks
        SET qty = CASE WHEN qty - ? < 0 THEN 0 ELSE qty - ? END
        WHERE username=? AND item=? AND network=?
    """, (int(qty), int(qty), username, item.strip(), network or "-"))
    conn.commit(); conn.close()

def get_stocks(username: Optional[str] = None, network: Optional[str] = None) -> List[Tuple[str, str, int, str, str]]:
    """
    Возвращает: (username, item, qty, network, updated_at), отсортирован по updated_at DESC.
    """
    conn = _conn(); cur = conn.cursor()
    if username and network:
        cur.execute("""
            SELECT username, item, qty, network, updated_at
            FROM stocks WHERE username=? AND network=?
            ORDER BY updated_at DESC
        """, (username.lstrip("@"), network))
    elif username:
        cur.execute("""
            SELECT username, item, qty, network, updated_at
            FROM stocks WHERE username=?
            ORDER BY updated_at DESC
        """, (username.lstrip("@"),))
    elif network:
        cur.execute("""
            SELECT username, item, qty, network, updated_at
            FROM stocks WHERE network=?
            ORDER BY updated_at DESC
        """, (network,))
    else:
        cur.execute("""
            SELECT username, item, qty, network, updated_at
            FROM stocks
            ORDER BY updated_at DESC
        """)
    rows = cur.fetchall()
    conn.close()
    return rows

def find_stock_like(username: str, model_norm: str, memory: str, network: str) -> Tuple[Optional[str], int]:
    """
    Находит лучшую позицию стока для списания:
    - фильтруем по username+network
    - сравниваем нормализованные модели
    - память: должна совпадать, но если в стоке память пустая — считаем совпадением
    """
    username = username.lstrip("@")
    rows = get_stocks(username=username, network=network)
    best_item = None
    best_qty = 0
    best_score = -1

    for _, item, qty, net, _ in rows:
        it_model_norm, it_mem = _normalize_item(item)
        if memory and it_mem and it_mem != str(memory):
            continue
        if not it_model_norm:
            continue
        a = model_norm
        b = it_model_norm
        if a in b or b in a:
            score = min(len(a), len(b))
            if score > best_score:
                best_score = score
                best_item = item
                best_qty = int(qty)

    return (best_item, best_qty) if best_item else (None, 0)

# --------------------------
# Прочее / агрегирующие вещи
# --------------------------
def get_all_known_users() -> List[str]:
    conn = _conn(); cur = conn.cursor()
    users = set()

    cur.execute("SELECT DISTINCT username FROM sales")
    users.update(u for (u,) in cur.fetchall() if u)

    cur.execute("SELECT DISTINCT username FROM stocks")
    users.update(u for (u,) in cur.fetchall() if u)

    cur.execute("SELECT DISTINCT username FROM plans")
    users.update(u for (u,) in cur.fetchall() if u)

    cur.execute("SELECT DISTINCT username FROM networks")
    users.update(u for (u,) in cur.fetchall() if u)

    conn.close()
    return sorted(u for u in users if u)
