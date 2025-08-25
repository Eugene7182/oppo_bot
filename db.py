# db.py
import sqlite3
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pytz import timezone

DB_NAME = "sales.db"
TZ = timezone("Asia/Almaty")


# =========================
# ВСПОМОГАТЕЛЬНЫЕ УТИЛИТЫ
# =========================
def _conn():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def _today_str() -> str:
    # локальная дата Asia/Almaty
    return datetime.now(TZ).strftime("%Y-%m-%d")

def _month_bounds(ym: str) -> Tuple[str, str]:
    """ym='YYYY-MM' -> (start_date_inclusive, end_date_exclusive)"""
    year, month = map(int, ym.split("-"))
    start = datetime(year, month, 1, tzinfo=TZ)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=TZ)
    else:
        end = datetime(year, month + 1, 1, tzinfo=TZ)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def _norm(s: str) -> str:
    """Нормализация модели: только [a-z0-9], без пробелов и знаков"""
    return re.sub(r"[^a-z0-9]+", "", s.lower())

def _split_item_model_mem(item: str) -> Tuple[str, Optional[str]]:
    """
    Из строки стока ('Reno 11F 5G 128') получить:
    - нормализованную модель без памяти ('reno11f5g')
    - память ('128') или None
    """
    full = _norm(item)  # 'reno11f5g128'
    m = re.match(r"^(.*?)(\d{2,4})$", full)
    if not m:
        return full, None
    return m.group(1), m.group(2)


# =========================
# ИНИЦИАЛИЗАЦИЯ СХЕМЫ
# =========================
def init():
    conn = _conn()
    cur = conn.cursor()

    # Продажи
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            model    TEXT,
            memory   TEXT,
            qty      INTEGER,
            network  TEXT,
            date     TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_user_date ON sales(username, date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date)")

    # Стоки
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            username   TEXT,
            item       TEXT,
            qty        INTEGER,
            network    TEXT,
            updated_at TEXT,
            PRIMARY KEY (username, item, network)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stocks_user_net ON stocks(username, network)")

    # Админы
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            username TEXT PRIMARY KEY
        )
    """)

    # Привязка сети к пользователю
    cur.execute("""
        CREATE TABLE IF NOT EXISTS networks (
            username TEXT PRIMARY KEY,
            network  TEXT
        )
    """)

    # Планы ПО МЕСЯЦАМ
    cur.execute("""
        CREATE TABLE IF NOT EXISTS plans_monthly (
            username TEXT,
            ym       TEXT,   -- 'YYYY-MM'
            plan     INTEGER,
            PRIMARY KEY (username, ym)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_plans_monthly_ym ON plans_monthly(ym)")

    conn.commit()
    conn.close()


# =========================
# АДМИНЫ
# =========================
def add_admin(username: str):
    conn = _conn()
    conn.execute("INSERT OR IGNORE INTO admins(username) VALUES (?)", (username.lstrip("@"),))
    conn.commit(); conn.close()

def remove_admin(username: str):
    conn = _conn()
    conn.execute("DELETE FROM admins WHERE username=?", (username.lstrip("@"),))
    conn.commit(); conn.close()

def is_admin(username: str) -> bool:
    conn = _conn()
    cur = conn.execute("SELECT 1 FROM admins WHERE username=?", (username.lstrip("@"),))
    ok = cur.fetchone() is not None
    conn.close()
    return ok

def list_admins() -> List[str]:
    conn = _conn()
    cur = conn.execute("SELECT username FROM admins")
    res = [r[0] for r in cur.fetchall()]
    conn.close()
    return res


# =========================
# СЕТИ
# =========================
def set_network(username: str, network: str):
    username = username.lstrip("@")
    conn = _conn()
    conn.execute("""
        INSERT INTO networks(username, network)
        VALUES(?, ?)
        ON CONFLICT(username) DO UPDATE SET network=excluded.network
    """, (username, network))
    conn.commit(); conn.close()

def get_network(username: str) -> str:
    username = username.lstrip("@")
    conn = _conn()
    cur = conn.execute("SELECT network FROM networks WHERE username=?", (username,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else "-"

def find_users_by_network(network_name: str) -> List[str]:
    """Ищет пользователей, у кого привязана ТАКАЯ сеть (регистронезависимо)"""
    conn = _conn()
    cur = conn.execute(
        "SELECT username FROM networks WHERE lower(network)=lower(?)",
        (network_name,))
    res = [r[0] for r in cur.fetchall()]
    conn.close()
    return res


# =========================
# СТОКИ
# =========================
def update_stock(username: str, item: str, qty: int, network: str):
    conn = _conn()
    conn.execute("""
        INSERT INTO stocks(username, item, qty, network, updated_at)
        VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(username, item, network)
        DO UPDATE SET qty=excluded.qty, updated_at=excluded.updated_at
    """, (username, item, qty, network, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit(); conn.close()

def get_stocks(username: Optional[str] = None, network: Optional[str] = None) -> List[Tuple[str, str, int, str]]:
    """
    Возвращает [(username, item, qty, network), ...]
    Можно фильтровать по username и/или network.
    """
    conn = _conn()
    if username and network:
        cur = conn.execute(
            "SELECT username, item, qty, network FROM stocks WHERE username=? AND network=? ORDER BY updated_at DESC",
            (username.lstrip("@"), network))
    elif username:
        cur = conn.execute(
            "SELECT username, item, qty, network FROM stocks WHERE username=? ORDER BY updated_at DESC",
            (username.lstrip("@"),))
    elif network:
        cur = conn.execute(
            "SELECT username, item, qty, network FROM stocks WHERE network=? ORDER BY updated_at DESC",
            (network,))
    else:
        cur = conn.execute(
            "SELECT username, item, qty, network FROM stocks ORDER BY updated_at DESC")
    rows = cur.fetchall()
    conn.close()
    return rows

def decrease_stock(username: str, item: str, qty: int, network: str):
    conn = _conn()
    conn.execute("""
        UPDATE stocks
           SET qty = CASE WHEN qty-? < 0 THEN 0 ELSE qty-? END
         WHERE username=? AND item=? AND network=?
    """, (qty, qty, username.lstrip("@"), item, network))
    conn.commit(); conn.close()

def find_stock_like(username: str, model_norm: str, memory: str, network: Optional[str]) -> Tuple[Optional[str], Optional[int]]:
    """
    Находит позицию стока пользователя, «похожую» на model_norm (без пробелов) и с нужной памятью.
    Возвращает (item_name, qty) или (None, None)
    """
    username = username.lstrip("@")
    stocks = get_stocks(username=username, network=network) if network else get_stocks(username=username)

    best_item = None
    best_qty = None

    for _, item, qty, net in stocks:
        base, mem = _split_item_model_mem(item)  # ('reno11f5g', '128')
        if mem is None:
            continue
        if mem != str(memory):
            continue

        # Фази-логика: точное совпадение основы, либо подстрока в обе стороны
        if base == model_norm or base in model_norm or model_norm in base:
            best_item = item
            best_qty = qty
            break

    return (best_item, best_qty) if best_item is not None else (None, None)


# =========================
# ПРОДАЖИ
# =========================
def add_sale(username: str, model: str, memory: str, qty: int, network: str):
    conn = _conn()
    conn.execute("""
        INSERT INTO sales(username, model, memory, qty, network, date)
        VALUES(?, ?, ?, ?, ?, ?)
    """, (username.lstrip("@"), model, memory, int(qty), network, _today_str()))
    conn.commit(); conn.close()

def month_sales(year: int, month: int, username: Optional[str] = None) -> Tuple[int, Dict[str, int]]:
    """
    Возвращает (итого_за_месяц, { 'model memory': qty, ... })
    """
    start = datetime(year, month, 1, tzinfo=TZ).strftime("%Y-%m-%d")
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=TZ).strftime("%Y-%m-%d")
    else:
        end = datetime(year, month + 1, 1, tzinfo=TZ).strftime("%Y-%m-%d")

    conn = _conn()
    if username:
        cur = conn.execute("""
            SELECT model, memory, SUM(qty)
              FROM sales
             WHERE username=? AND date>=? AND date<?
             GROUP BY model, memory
        """, (username.lstrip("@"), start, end))
    else:
        cur = conn.execute("""
            SELECT model, memory, SUM(qty)
              FROM sales
             WHERE date>=? AND date<?
             GROUP BY model, memory
        """, (start, end))
    rows = cur.fetchall()
    conn.close()

    by_model: Dict[str, int] = {}
    total = 0
    for m, mem, s in rows:
        key = f"{m} {mem}"
        by_model[key] = int(s)
        total += int(s)
    return total, by_model

def get_last_sale_time(username: str) -> Optional[datetime]:
    """Последняя дата продажи пользователя как aware datetime (Asia/Almaty, 00:00)"""
    conn = _conn()
    cur = conn.execute("""
        SELECT MAX(date) FROM sales WHERE username=?
    """, (username.lstrip("@"),))
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        return None
    # делаем aware datetime на полночь локального дня
    d = datetime.strptime(row[0], "%Y-%m-%d")
    return TZ.localize(datetime(d.year, d.month, d.day, 0, 0, 0))


def reset_monthly_sales():
    """Полный сброс таблицы продаж (как вы и хотели запускать 1-го числа)."""
    conn = _conn()
    conn.execute("DELETE FROM sales")
    conn.commit(); conn.close()


# =========================
# ПЛАНЫ (ПО МЕСЯЦАМ)
# =========================
def set_plan(username: str, ym: str, plan: int):
    conn = _conn()
    conn.execute("""
        INSERT INTO plans_monthly(username, ym, plan)
        VALUES(?, ?, ?)
        ON CONFLICT(username, ym) DO UPDATE SET plan=excluded.plan
    """, (username.lstrip("@"), ym, int(plan)))
    conn.commit(); conn.close()

def get_plan(username: str, ym: str) -> int:
    conn = _conn()
    cur = conn.execute("""
        SELECT plan FROM plans_monthly WHERE username=? AND ym=?
    """, (username.lstrip("@"), ym))
    row = cur.fetchone()
    conn.close()
    return int(row[0]) if row else 0

def get_all_plans(ym: str) -> Dict[str, int]:
    conn = _conn()
    cur = conn.execute("SELECT username, plan FROM plans_monthly WHERE ym=?", (ym,))
    res = {r[0]: int(r[1]) for r in cur.fetchall()}
    conn.close()
    return res


# =========================
# РУЧНОЕ КОРРЕКТИРОВАНИЕ ФАКТА
# =========================
def set_monthly_fact(username: str, ym: str, qty: int, network: Optional[str] = "-"):
    """
    Жёсткая установка факта за месяц:
    - удаляет ВСЕ продажи пользователя за указанный месяц
    - вставляет одну запись 'manual'
    """
    username = username.lstrip("@")
    start, end = _month_bounds(ym)
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM sales WHERE username=? AND date>=? AND date<?", (username, start, end))
    # ставим запись на 1-е число месяца
    cur.execute("""
        INSERT INTO sales(username, model, memory, qty, network, date)
        VALUES(?, 'manual', '-', ?, ?, ?)
    """, (username, int(qty), network or "-", start))
    conn.commit(); conn.close()

def add_monthly_fact(username: str, ym: str, qty: int, network: Optional[str] = "-"):
    """
    Добавить к факту за месяц (добавляется еще одна запись 'manual').
    """
    username = username.lstrip("@")
    start, _ = _month_bounds(ym)
    conn = _conn()
    conn.execute("""
        INSERT INTO sales(username, model, memory, qty, network, date)
        VALUES(?, 'manual', '-', ?, ?, ?)
    """, (username, int(qty), network or "-", start))
    conn.commit(); conn.close()


# =========================
# ПРОЧЕЕ
# =========================
def get_all_known_users() -> List[str]:
    """
    Собирает известных пользователей из всех таблиц.
    """
    conn = _conn()
    users: set = set()

    for sql in [
        "SELECT DISTINCT username FROM networks",
        "SELECT DISTINCT username FROM stocks",
        "SELECT DISTINCT username FROM sales",
        "SELECT DISTINCT username FROM admins",
        "SELECT DISTINCT username FROM plans_monthly",
    ]:
        cur = conn.execute(sql)
        users.update(r[0] for r in cur.fetchall() if r and r[0])

    conn.close()
    # Нормализуем и сортируем
    return sorted({str(u).lstrip("@") for u in users if u})
