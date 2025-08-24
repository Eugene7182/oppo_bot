import sqlite3
from datetime import datetime, timedelta

# --- Подключение к базе ---
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
    date TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS stok (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    item TEXT,
    qty INTEGER,
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
CREATE TABLE IF NOT EXISTS photos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    date TEXT
)
""")

conn.commit()

# --- Продажи ---
def add_sale(username, model, memory, qty):
    cursor.execute(
        "INSERT INTO sales (username, model, memory, qty, date) VALUES (?, ?, ?, ?, ?)",
        (username, model, memory, qty, datetime.now().strftime("%Y-%m-%d"))
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

def get_sales_all(date):
    cursor.execute("""
        SELECT username, SUM(qty), 
        (SELECT plan FROM plans WHERE plans.username = sales.username) 
        FROM sales 
        WHERE date=? 
        GROUP BY username
    """, (date,))
    return cursor.fetchall()

def set_sales(username, qty):
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM sales WHERE username=? AND date>=?", (username, month_start))
    cursor.execute(
        "INSERT INTO sales (username, model, memory, qty, date) VALUES (?, ?, ?, ?, ?)",
        (username, "manual", "-", qty, datetime.now().strftime("%Y-%m-%d"))
    )
    conn.commit()

# --- Планы ---
def set_plan(username, plan):
    cursor.execute("INSERT OR REPLACE INTO plans (username, plan) VALUES (?, ?)", (username, plan))
    conn.commit()

# --- Стоки ---
def update_stock(username, item, qty):
    cursor.execute("SELECT qty FROM stok WHERE username=? AND item=?", (username, item))
    row = cursor.fetchone()
    if row:
        cursor.execute(
            "UPDATE stok SET qty=?, last_update=? WHERE username=? AND item=?",
            (qty, datetime.now().strftime("%Y-%m-%d %H:%M"), username, item)
        )
    else:
        cursor.execute(
            "INSERT INTO stok (username, item, qty, last_update) VALUES (?, ?, ?, ?)",
            (username, item, qty, datetime.now().strftime("%Y-%m-%d %H:%M"))
        )
    conn.commit()

def get_all_stocks():
    cursor.execute("SELECT username, item, qty, last_update FROM stok")
    return cursor.fetchall()

# --- Фото ---
def add_photo(username):
    cursor.execute(
        "INSERT INTO photos (username, date) VALUES (?, ?)",
        (username, datetime.now().strftime("%Y-%m-%d"))
    )
    conn.commit()

def get_photos_week():
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    cursor.execute("SELECT username, COUNT(*) FROM photos WHERE date>=? GROUP BY username", (week_ago,))
    return cursor.fetchall()

# --- Итог месяца ---
def reset_monthly_sales():
    month_start = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM sales WHERE date>=?", (month_start,))
    conn.commit()
