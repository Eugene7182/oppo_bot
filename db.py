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
    date TEXT,
    network TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS stok (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    item TEXT,
    qty INTEGER,
    last_update TEXT,
    network TEXT
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
CREATE TABLE IF NOT EXISTS user_network (
    username TEXT PRIMARY KEY,
    network TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS admins (
    username TEXT PRIMARY KEY
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS admin_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin TEXT,
    action TEXT,
    ts TEXT
)
""")

conn.commit()

# --- Админы ---
def seed_admins_from_env(usernames):
    for u in usernames:
        cursor.execute("INSERT OR IGNORE INTO admins (username) VALUES (?)", (u.lower(),))
    conn.commit()

def is_admin(username):
    cursor.execute("SELECT 1 FROM admins WHERE username=?", (username.lower(),))
    return cursor.fetchone() is not None

def add_admin(username):
    cursor.execute("INSERT OR IGNORE INTO admins (username) VALUES (?)", (username.lower(),))
    conn.commit()

def del_admin(username):
    cursor.execute("DELETE FROM admins WHERE username=?", (username.lower(),))
    conn.commit()

def list_admins():
    cursor.execute("SELECT username FROM admins ORDER BY username")
    return [r[0] for r in cursor.fetchall()]

def log_admin_action(admin, action):
    cursor.execute(
        "INSERT INTO admin_logs (admin, action, ts) VALUES (?, ?, ?)",
        (admin, action, datetime.now().strftime("%Y-%m-%d %H:%M"))
    )
    conn.commit()

# --- Пользовательские сети ---
def set_user_network(username, network):
    cursor.execute("INSERT OR REPLACE INTO user_network (username, network) VALUES (?, ?)", (username, network))
    conn.commit()

def get_user_network(username):
    cursor.execute("SELECT network FROM user_network WHERE username=?", (username,))
    row = cursor.fetchone()
    return row[0] if row and row[0] else None

# --- Продажи ---
def add_sale(username, model, memory, qty, network=None):
    cursor.execute(
        "INSERT INTO sales (username, model, memory, qty, date, network) VALUES (?, ?, ?, ?, ?, ?)",
        (username, model, memory, qty, datetime.now().strftime("%Y-%m-%d"), network)
    )
    conn.commit()

def get_sales_month():
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT username, SUM(qty),
               (SELECT plan FROM plans WHERE plans.username = sales.username)
        FROM sales
        WHERE date>=?
        GROUP BY username
    """, (month_start,))
    return cursor.fetchall()

def get_sales_month_by_network(network):
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT username, SUM(qty),
               (SELECT plan FROM plans WHERE plans.username = sales.username)
        FROM sales
        WHERE date>=? AND network=?
        GROUP BY username
    """, (month_start, network))
    return cursor.fetchall()

def get_sales_all(date):
    cursor.execute("""
        SELECT username, SUM(qty),
               (SELECT plan FROM plans WHERE plans.username = sales.username)
        FROM sales
        WHERE date=?
        GROUP BY username
    """, (date,))
    return cursor.fetchall()

def get_top_sellers(limit=3):
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT username, SUM(qty) AS total_qty
        FROM sales
        WHERE date>=?
        GROUP BY username
        ORDER BY total_qty DESC
        LIMIT ?
    """, (month_start, limit))
    return cursor.fetchall()

# --- Планы ---
def set_plan(username, plan):
    cursor.execute("INSERT OR REPLACE INTO plans (username, plan) VALUES (?, ?)", (username, plan))
    conn.commit()

# --- Стоки ---
def update_stock(username, item, qty, network=None):
    cursor.execute("""
        SELECT qty FROM stok
        WHERE username=? AND item=? AND IFNULL(network,'')=IFNULL(?, '')
    """, (username, item, network))
    row = cursor.fetchone()
    if row:
        cursor.execute(
            "UPDATE stok SET qty=?, last_update=?, network=? WHERE username=? AND item=?",
            (qty, datetime.now().strftime("%Y-%m-%d %H:%M"), network, username, item)
        )
    else:
        cursor.execute(
            "INSERT INTO stok (username, item, qty, last_update, network) VALUES (?, ?, ?, ?, ?)",
            (username, item, qty, datetime.now().strftime("%Y-%m-%d %H:%M"), network)
        )
    conn.commit()

def get_stocks_filtered(username=None, network=None):
    q = "SELECT username, item, qty, last_update, network FROM stok WHERE 1=1"
    args = []
    if username:
        q += " AND username=?"
        args.append(username)
    if network:
        q += " AND network=?"
        args.append(network)
    q += " ORDER BY last_update DESC"
    cursor.execute(q, tuple(args))
    return cursor.fetchall()

# --- Итог месяца ---
def reset_monthly_sales():
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM sales WHERE date>=?", (month_start,))
    conn.commit()
