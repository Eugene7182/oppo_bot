import sqlite3
from datetime import datetime

conn = sqlite3.connect("sales.db")
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

conn.commit()

# --- Продажи ---
def add_sale(username, model, memory, qty, network="-"):
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
        COALESCE(network, '-')
        FROM sales
        WHERE date>=?
        GROUP BY username, network
    """, (month_start,))
    return cursor.fetchall()

def get_sales_all(date):
    cursor.execute("""
        SELECT username, SUM(qty),
        (SELECT plan FROM plans WHERE plans.username = sales.username),
        COALESCE(network, '-')
        FROM sales
        WHERE date=?
        GROUP BY username, network
    """, (date,))
    return cursor.fetchall()

def get_sales_by_network(network):
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT username, SUM(qty),
        (SELECT plan FROM plans WHERE plans.username = sales.username)
        FROM sales
        WHERE date>=? AND network=?
        GROUP BY username
    """, (month_start, network))
    return cursor.fetchall()

def reset_monthly_sales():
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM sales WHERE date>=?", (month_start,))
    conn.commit()

# --- Планы ---
def set_plan(username, plan):
    cursor.execute("INSERT OR REPLACE INTO plans (username, plan) VALUES (?, ?)", (username, plan))
    conn.commit()

# --- Стоки ---
def update_stock(username, item, qty, network="-"):
    cursor.execute("SELECT qty FROM stok WHERE username=? AND item=? AND network=?", (username, item, network))
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

def get_stocks(username=None, network=None):
    query = "SELECT username, item, qty, network, last_update FROM stok WHERE 1=1"
    params = []
    if username:
        query += " AND username=?"
        params.append(username)
    if network:
        query += " AND network=?"
        params.append(network)
    cursor.execute(query, tuple(params))
    return cursor.fetchall()

def get_stock_qty(username, item, network="-"):
    cursor.execute("SELECT qty FROM stok WHERE username=? AND item=? AND network=?", (username, item, network))
    row = cursor.fetchone()
    return row[0] if row else None

def decrease_stock(username, item, qty, network="-"):
    cursor.execute("SELECT qty FROM stok WHERE username=? AND item=? AND network=?", (username, item, network))
    row = cursor.fetchone()
    if row:
        new_qty = max(0, row[0] - qty)
        cursor.execute(
            "UPDATE stok SET qty=?, last_update=? WHERE username=? AND item=? AND network=?",
            (new_qty, datetime.now().strftime("%Y-%m-%d %H:%M"), username, item, network)
        )
        conn.commit()

# --- Админы ---
def add_admin(username):
    cursor.execute("INSERT OR IGNORE INTO admins (username) VALUES (?)", (username,))
    conn.commit()

def del_admin(username):
    cursor.execute("DELETE FROM admins WHERE username=?", (username,))
    conn.commit()

def get_admins():
    cursor.execute("SELECT username FROM admins")
    return [row[0] for row in cursor.fetchall()]

def is_admin(username):
    cursor.execute("SELECT 1 FROM admins WHERE username=?", (username,))
    return cursor.fetchone() is not None
