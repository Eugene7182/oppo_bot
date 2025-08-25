import sqlite3
from datetime import datetime

# Один файл БД, потокобезопасно для aiogram/webhook
conn = sqlite3.connect("sales.db", check_same_thread=False)
cursor = conn.cursor()

# --- Таблицы ---
cursor.execute("""
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

cursor.execute("""
CREATE TABLE IF NOT EXISTS stok (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    item TEXT,
    qty INTEGER,
    network TEXT,
    last_update TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    plan INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS admins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_networks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    network TEXT
)
""")

conn.commit()

# --- Продажи ---
def add_sale(username, model, memory, qty, network):
    cursor.execute(
        "INSERT INTO sales (username, model, memory, qty, network, date) VALUES (?, ?, ?, ?, ?, ?)",
        (username, model, memory, qty, network, datetime.now().strftime("%Y-%m-%d"))
    )
    conn.commit()

def get_sales_month():
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT username, SUM(qty),
               (SELECT plan FROM plans WHERE plans.username = sales.username),
               network
        FROM sales
        WHERE date >= ?
        GROUP BY username, network
    """, (month_start,))
    return cursor.fetchall()

def get_sales_all(date_str):
    cursor.execute("""
        SELECT username, SUM(qty),
               (SELECT plan FROM plans WHERE plans.username = sales.username),
               network
        FROM sales
        WHERE date = ?
        GROUP BY username, network
    """, (date_str,))
    return cursor.fetchall()

def reset_monthly_sales():
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM sales WHERE date >= ?", (month_start,))
    conn.commit()

def get_sales_by_network(network):
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT username, SUM(qty),
               (SELECT plan FROM plans WHERE plans.username = sales.username)
        FROM sales
        WHERE date >= ? AND network = ?
        GROUP BY username
    """, (month_start, network))
    return cursor.fetchall()

# --- Ручная установка продаж за месяц ---
def set_sales(username, qty):
    """Перезаписывает продажи пользователя в текущем месяце заданным числом."""
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM sales WHERE username=? AND date>=?", (username, month_start))
    cursor.execute(
        "INSERT INTO sales (username, model, memory, qty, date, network) VALUES (?, ?, ?, ?, ?, ?)",
        (username, "manual", "-", qty, datetime.now().strftime("%Y-%m-%d"), "-")
    )
    conn.commit()

# --- Стоки (храним только актуальные значения) ---
def update_stock(username, item, qty, network):
    cursor.execute(
        "SELECT qty FROM stok WHERE username=? AND item=? AND network=?",
        (username, item, network)
    )
    row = cursor.fetchone()
    if row:
        cursor.execute(
            "UPDATE stok SET qty=?, last_update=? WHERE username=? AND item=? AND network=?",
            (qty, datetime.now().strftime("%Y-%m-%d %H:%M"), username, item, network)
        )
    else:
        cursor.execute(
            "INSERT INTO stok (username, item, qty, network, last_update) VALUES (?, ?, ?, ?, ?)",
            (username, item, qty, network, datetime.now().strftime("%Y-%m-%d %H:%M"))
        )
    conn.commit()

def get_stock_qty(username, item, network):
    cursor.execute(
        "SELECT qty FROM stok WHERE username=? AND item=? AND network=?",
        (username, item, network)
    )
    row = cursor.fetchone()
    return row[0] if row else None

def decrease_stock(username, item, qty, network):
    cursor.execute(
        "SELECT qty FROM stok WHERE username=? AND item=? AND network=?",
        (username, item, network)
    )
    row = cursor.fetchone()
    if not row:
        return
    new_qty = max(0, row[0] - qty)
    cursor.execute(
        "UPDATE stok SET qty=?, last_update=? WHERE username=? AND item=? AND network=?",
        (new_qty, datetime.now().strftime("%Y-%m-%d %H:%M"), username, item, network)
    )
    conn.commit()

def get_stocks():
    cursor.execute("SELECT username, item, qty, network, last_update FROM stok")
    return cursor.fetchall()

# --- «Похожие» совпадения модели в стоках (модель + память) ---
def find_stock_like(username, model, memory, network):
    """
    Сравниваем без пробелов/регистра:
      - model (например 'reno14f' или 'a3x')
      - memory ('256', '512', '1024'; 'тб/tb' игнорятся на стороне бота)
    """
    cursor.execute(
        "SELECT item, qty FROM stok WHERE username=? AND network=?",
        (username, network)
    )
    rows = cursor.fetchall()

    model_low = (model or "").lower().replace(" ", "")
    memory_low = str(memory).lower().replace(" ", "")

    for item, qty in rows:
        item_low = (item or "").lower().replace(" ", "")
        if model_low in item_low and memory_low in item_low:
            return item, qty
    return None, None

# --- Планы ---
def set_plan(username, plan):
    cursor.execute("INSERT OR REPLACE INTO plans (username, plan) VALUES (?, ?)", (username, plan))
    conn.commit()

# --- Админы ---
def is_admin(username):
    cursor.execute("SELECT 1 FROM admins WHERE username=?", (username,))
    return cursor.fetchone() is not None

def add_admin(username):
    cursor.execute("INSERT OR IGNORE INTO admins (username) VALUES (?)", (username,))
    conn.commit()

def del_admin(username):
    cursor.execute("DELETE FROM admins WHERE username=?", (username,))
    conn.commit()

def get_admins():
    cursor.execute("SELECT username FROM admins")
    return [row[0] for row in cursor.fetchall()]

# --- Привязка сети к промоутеру ---
def set_network(username, network):
    cursor.execute("INSERT OR REPLACE INTO user_networks (username, network) VALUES (?, ?)", (username, network))
    conn.commit()

def get_network(username):
    cursor.execute("SELECT network FROM user_networks WHERE username=?", (username,))
    row = cursor.fetchone()
    return row[0] if row else "-"

# --- Для напоминаний о неактивных ---
def get_all_usernames():
    users = set()

    cursor.execute("SELECT DISTINCT username FROM sales")
    users.update([r[0] for r in cursor.fetchall() if r[0]])

    cursor.execute("SELECT DISTINCT username FROM stok")
    users.update([r[0] for r in cursor.fetchall() if r[0]])

    cursor.execute("SELECT DISTINCT username FROM user_networks")
    users.update([r[0] for r in cursor.fetchall() if r[0]])

    cursor.execute("SELECT DISTINCT username FROM plans")
    users.update([r[0] for r in cursor.fetchall() if r[0]])

    return sorted(users)

def get_last_sale_date(username):
    cursor.execute("SELECT MAX(date) FROM sales WHERE username=?", (username,))
    row = cursor.fetchone()
    return row[0] if row and row[0] else None
import sqlite3
from datetime import datetime

# Один файл БД, потокобезопасно для aiogram/webhook
conn = sqlite3.connect("sales.db", check_same_thread=False)
cursor = conn.cursor()

# --- Таблицы ---
cursor.execute("""
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

cursor.execute("""
CREATE TABLE IF NOT EXISTS stok (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    item TEXT,
    qty INTEGER,
    network TEXT,
    last_update TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    plan INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS admins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_networks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    network TEXT
)
""")

conn.commit()

# --- Продажи ---
def add_sale(username, model, memory, qty, network):
    cursor.execute(
        "INSERT INTO sales (username, model, memory, qty, network, date) VALUES (?, ?, ?, ?, ?, ?)",
        (username, model, memory, qty, network, datetime.now().strftime("%Y-%m-%d"))
    )
    conn.commit()

def get_sales_month():
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT username, SUM(qty),
               (SELECT plan FROM plans WHERE plans.username = sales.username),
               network
        FROM sales
        WHERE date >= ?
        GROUP BY username, network
    """, (month_start,))
    return cursor.fetchall()

def get_sales_all(date_str):
    cursor.execute("""
        SELECT username, SUM(qty),
               (SELECT plan FROM plans WHERE plans.username = sales.username),
               network
        FROM sales
        WHERE date = ?
        GROUP BY username, network
    """, (date_str,))
    return cursor.fetchall()

def reset_monthly_sales():
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM sales WHERE date >= ?", (month_start,))
    conn.commit()

def get_sales_by_network(network):
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT username, SUM(qty),
               (SELECT plan FROM plans WHERE plans.username = sales.username)
        FROM sales
        WHERE date >= ? AND network = ?
        GROUP BY username
    """, (month_start, network))
    return cursor.fetchall()

# --- Ручная установка продаж за месяц ---
def set_sales(username, qty):
    """Перезаписывает продажи пользователя в текущем месяце заданным числом."""
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM sales WHERE username=? AND date>=?", (username, month_start))
    cursor.execute(
        "INSERT INTO sales (username, model, memory, qty, date, network) VALUES (?, ?, ?, ?, ?, ?)",
        (username, "manual", "-", qty, datetime.now().strftime("%Y-%m-%d"), "-")
    )
    conn.commit()

# --- Стоки (храним только актуальные значения) ---
def update_stock(username, item, qty, network):
    cursor.execute(
        "SELECT qty FROM stok WHERE username=? AND item=? AND network=?",
        (username, item, network)
    )
    row = cursor.fetchone()
    if row:
        cursor.execute(
            "UPDATE stok SET qty=?, last_update=? WHERE username=? AND item=? AND network=?",
            (qty, datetime.now().strftime("%Y-%m-%d %H:%M"), username, item, network)
        )
    else:
        cursor.execute(
            "INSERT INTO stok (username, item, qty, network, last_update) VALUES (?, ?, ?, ?, ?)",
            (username, item, qty, network, datetime.now().strftime("%Y-%m-%d %H:%M"))
        )
    conn.commit()

def get_stock_qty(username, item, network):
    cursor.execute(
        "SELECT qty FROM stok WHERE username=? AND item=? AND network=?",
        (username, item, network)
    )
    row = cursor.fetchone()
    return row[0] if row else None

def decrease_stock(username, item, qty, network):
    cursor.execute(
        "SELECT qty FROM stok WHERE username=? AND item=? AND network=?",
        (username, item, network)
    )
    row = cursor.fetchone()
    if not row:
        return
    new_qty = max(0, row[0] - qty)
    cursor.execute(
        "UPDATE stok SET qty=?, last_update=? WHERE username=? AND item=? AND network=?",
        (new_qty, datetime.now().strftime("%Y-%m-%d %H:%M"), username, item, network)
    )
    conn.commit()

def get_stocks():
    cursor.execute("SELECT username, item, qty, network, last_update FROM stok")
    return cursor.fetchall()

# --- «Похожие» совпадения модели в стоках (модель + память) ---
def find_stock_like(username, model, memory, network):
    """
    Сравниваем без пробелов/регистра:
      - model (например 'reno14f' или 'a3x')
      - memory ('256', '512', '1024'; 'тб/tb' игнорятся на стороне бота)
    """
    cursor.execute(
        "SELECT item, qty FROM stok WHERE username=? AND network=?",
        (username, network)
    )
    rows = cursor.fetchall()

    model_low = (model or "").lower().replace(" ", "")
    memory_low = str(memory).lower().replace(" ", "")

    for item, qty in rows:
        item_low = (item or "").lower().replace(" ", "")
        if model_low in item_low and memory_low in item_low:
            return item, qty
    return None, None

# --- Планы ---
def set_plan(username, plan):
    cursor.execute("INSERT OR REPLACE INTO plans (username, plan) VALUES (?, ?)", (username, plan))
    conn.commit()

# --- Админы ---
def is_admin(username):
    cursor.execute("SELECT 1 FROM admins WHERE username=?", (username,))
    return cursor.fetchone() is not None

def add_admin(username):
    cursor.execute("INSERT OR IGNORE INTO admins (username) VALUES (?)", (username,))
    conn.commit()

def del_admin(username):
    cursor.execute("DELETE FROM admins WHERE username=?", (username,))
    conn.commit()

def get_admins():
    cursor.execute("SELECT username FROM admins")
    return [row[0] for row in cursor.fetchall()]

# --- Привязка сети к промоутеру ---
def set_network(username, network):
    cursor.execute("INSERT OR REPLACE INTO user_networks (username, network) VALUES (?, ?)", (username, network))
    conn.commit()

def get_network(username):
    cursor.execute("SELECT network FROM user_networks WHERE username=?", (username,))
    row = cursor.fetchone()
    return row[0] if row else "-"

# --- Для напоминаний о неактивных ---
def get_all_usernames():
    users = set()

    cursor.execute("SELECT DISTINCT username FROM sales")
    users.update([r[0] for r in cursor.fetchall() if r[0]])

    cursor.execute("SELECT DISTINCT username FROM stok")
    users.update([r[0] for r in cursor.fetchall() if r[0]])

    cursor.execute("SELECT DISTINCT username FROM user_networks")
    users.update([r[0] for r in cursor.fetchall() if r[0]])

    cursor.execute("SELECT DISTINCT username FROM plans")
    users.update([r[0] for r in cursor.fetchall() if r[0]])

    return sorted(users)

def get_last_sale_date(username):
    cursor.execute("SELECT MAX(date) FROM sales WHERE username=?", (username,))
    row = cursor.fetchone()
    return row[0] if row and row[0] else None
