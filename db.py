import sqlite3
from datetime import datetime, timedelta

DB_NAME = "sales.db"


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            username TEXT PRIMARY KEY
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS plans (
            username TEXT PRIMARY KEY,
            plan INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS networks (
            username TEXT PRIMARY KEY,
            network TEXT
        )
    """)

    conn.commit()
    conn.close()


# -------------------- Админы --------------------
def add_admin(username: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO admins (username) VALUES (?)", (username,))
    conn.commit()
    conn.close()


def del_admin(username: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM admins WHERE username=?", (username,))
    conn.commit()
    conn.close()


def is_admin(username: str) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM admins WHERE username=?", (username,))
    row = cur.fetchone()
    conn.close()
    return bool(row)


def get_admins():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT username FROM admins")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


# -------------------- Планы --------------------
def set_plan(username: str, plan: int):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO plans (username, plan)
        VALUES (?, ?)
        ON CONFLICT(username) DO UPDATE SET plan=excluded.plan
    """, (username, plan))
    conn.commit()
    conn.close()


def get_plan(username: str) -> int:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT plan FROM plans WHERE username=?", (username,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0


# -------------------- Сети --------------------
def set_network(username: str, network: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO networks (username, network)
        VALUES (?, ?)
        ON CONFLICT(username) DO UPDATE SET network=excluded.network
    """, (username, network))
    conn.commit()
    conn.close()


def get_network(username: str) -> str:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT network FROM networks WHERE username=?", (username,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else "-"


# -------------------- Продажи --------------------
def add_sale(username: str, model: str, memory: str, qty: int, network: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sales (username, model, memory, qty, network, date)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (username, model, memory, qty, network, datetime.now().strftime("%Y-%m-%d")))
    conn.commit()
    conn.close()


def get_sales_all(date: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT username, SUM(qty), (SELECT plan FROM plans WHERE username=s.username), s.network
        FROM sales s
        WHERE date=?
        GROUP BY username, network
    """, (date,))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_sales_month():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT username, SUM(qty), (SELECT plan FROM plans WHERE username=s.username), s.network
        FROM sales s
        GROUP BY username, network
    """)
    rows = cur.fetchall()
    conn.close()
    return rows


def set_sales(username: str, qty: int):
    """Установить продажи вручную (перезаписывает для текущего месяца)"""
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM sales WHERE username=? AND date>=?", (username, month_start))
    cur.execute("""
        INSERT INTO sales (username, model, memory, qty, network, date)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (username, "manual", "-", qty, "-", datetime.now().strftime("%Y-%m-%d")))
    conn.commit()
    conn.close()


def reset_monthly_sales():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("DELETE FROM sales")
    conn.commit()
    conn.close()


def get_last_sale(username: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT date FROM sales WHERE username=? ORDER BY date DESC LIMIT 1", (username,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


# -------------------- Стоки --------------------
def update_stock(username: str, item: str, qty: int, network: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO stocks (username, item, qty, network, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(username, item, network)
        DO UPDATE SET qty=excluded.qty, updated_at=excluded.updated_at
    """, (username, item, qty, network, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()


def get_stock_qty(username: str, item: str, network: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT qty FROM stocks
        WHERE username=? AND item=? AND network=?
        ORDER BY updated_at DESC
        LIMIT 1
    """, (username, item, network))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def decrease_stock(username: str, item: str, qty: int, network: str):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        UPDATE stocks
        SET qty = qty - ?
        WHERE username=? AND item=? AND network=?
    """, (qty, username, item, network))
    conn.commit()
    conn.close()


def get_stocks():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        SELECT username, item, qty, network, updated_at
        FROM stocks
        ORDER BY updated_at DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows
