# -*- coding: utf-8 -*-
# db.py — SQLite-репозиторий под senior bot.py

import os
import re
import sqlite3
import asyncio
from types import SimpleNamespace
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple
from pytz import timezone

# =========================
# НАСТРОЙКИ
# =========================
DB_NAME = os.getenv("DB_NAME", "sales.db")
TZ = timezone(os.getenv("TZ", "Asia/Almaty"))
AGG_USER = "__NET__"  # спец-пользователь для сетевого агрегированного стока

# =========================
# БАЗОВЫЕ УТИЛИТЫ
# =========================
def _conn():
    conn = sqlite3.connect(DB_NAME, isolation_level=None)  # autocommit
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn

def _today_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower()).strip()

def _split_item_model_mem(item: str) -> Tuple[str, Optional[int]]:
    full = _norm(item)  # 'reno11f5g128'
    m = re.match(r"^(.*?)(\d{2,4})$", full)
    if not m:
        return full, None
    try:
        return m.group(1), int(m.group(2))
    except:
        return m.group(1), None

def _compose_item(product_id: str, mem: int) -> str:
    # item для стока: "<canonical> <mem>" если mem>0, иначе просто canonical
    product_id = str(product_id).strip()
    return f"{product_id} {mem}".strip() if mem and int(mem) > 0 else product_id

def _to_thread(fn, *a, **kw):
    return asyncio.to_thread(fn, *a, **kw)

# =========================
# ИНИЦИАЛИЗАЦИЯ СХЕМЫ
# =========================
def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = [r["name"] for r in cur.fetchall()]
    return col in cols

def init():
    conn = _conn()
    cur = conn.cursor()

    # Продажи
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            username           TEXT,
            model              TEXT,
            memory             INTEGER,
            qty                INTEGER,
            network            TEXT,
            date               TEXT,
            source_update_id   INTEGER
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_user_date ON sales(username, date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_network_date ON sales(network, date)")
    # На старых БД могло не быть колонки source_update_id — добавим тихо
    if not _column_exists(conn, "sales", "source_update_id"):
        try:
            conn.execute("ALTER TABLE sales ADD COLUMN source_update_id INTEGER")
        except Exception:
            pass

    # Приходы (лог)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shipments (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            network    TEXT,
            product    TEXT,
            memory     INTEGER,
            qty        INTEGER,
            occurred_at TEXT
        )
    """)

    # Стоки (перс. и сетевой агрегат; сетевой держим на username='__NET__')
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

    # Привязки "пользователь → сеть"
    cur.execute("""
        CREATE TABLE IF NOT EXISTS networks (
            username TEXT PRIMARY KEY,
            network  TEXT
        )
    """)

    # Админы (не используется новым ботом, но оставим совместимость)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admins (
            username TEXT PRIMARY KEY
        )
    """)

    # Планы по месяцам (историческая таблица)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS plans_monthly (
            username TEXT,
            ym       TEXT,   -- 'YYYY-MM'
            plan     INTEGER,
            PRIMARY KEY (username, ym)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_plans_monthly_ym ON plans_monthly(ym)")

    # Антидубль ретраев Telegram
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_updates (
            update_id    INTEGER PRIMARY KEY,
            processed_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # Флаги "сегодня уже просили" (анти-спам)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS network_prompt_flags (
            network TEXT,
            day     TEXT,   -- 'YYYY-MM-DD'
            kind    TEXT,   -- 'negative' | 'init' | 'decrease'
            PRIMARY KEY (network, day, kind)
        )
    """)

    # Метаданные сети (инициализирован ли сток; город/адрес опционально)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS network_meta (
            network     TEXT PRIMARY KEY,
            city        TEXT,
            address     TEXT,
            initialized INTEGER NOT NULL DEFAULT 0
        )
    """)

    # Последняя продажа (для напоминаний 4 дня)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS people_last_sale (
            username     TEXT PRIMARY KEY,
            last_sale_at TEXT
        )
    """)

    conn.commit()
    conn.close()

# Инициализируем схему при импорте
init()

# =========================
# НИЖЕ — АДАПТЕР / РЕПО ДЛЯ bot.py
# =========================

# --- Вспомогательные сетевые операции поверх нашей схемы ---

def _ensure_network_meta(network: str, city: Optional[str] = None, address: Optional[str] = None):
    conn = _conn()
    try:
        conn.execute("""
            INSERT INTO network_meta(network, city, address, initialized)
            VALUES(?, ?, ?, COALESCE((SELECT initialized FROM network_meta WHERE network=?), 0))
            ON CONFLICT(network) DO UPDATE SET
                city=COALESCE(excluded.city, network_meta.city),
                address=COALESCE(excluded.address, network_meta.address)
        """, (network, city, address, network))
    finally:
        conn.close()

def _get_network_initialized(network: str) -> bool:
    conn = _conn()
    try:
        cur = conn.execute("SELECT initialized FROM network_meta WHERE network=?", (network,))
        row = cur.fetchone()
        if row is None:
            # если меты нет — считаем по наличию снапшота
            c2 = conn.execute("SELECT 1 FROM stocks WHERE username=? AND network=? LIMIT 1", (AGG_USER, network)).fetchone()
            return c2 is not None
        return bool(int(row["initialized"]))
    finally:
        conn.close()

def _set_network_initialized(network: str, value: bool):
    conn = _conn()
    try:
        conn.execute("""
            INSERT INTO network_meta(network, initialized)
            VALUES(?, ?)
            ON CONFLICT(network) DO UPDATE SET initialized=excluded.initialized
        """, (network, 1 if value else 0))
    finally:
        conn.close()

def _clear_prompt_flags(network: str):
    conn = _conn()
    try:
        conn.execute("DELETE FROM network_prompt_flags WHERE network=? AND day=?", (network, _today_str()))
    finally:
        conn.close()

def _prompt_needed_today(network: str, kind: str) -> bool:
    conn = _conn()
    try:
        conn.execute("""
            INSERT INTO network_prompt_flags(network, day, kind)
            VALUES(?, ?, ?)
        """, (network, _today_str(), kind))
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def _mark_and_check_update(update_id: int) -> bool:
    conn = _conn()
    try:
        conn.execute("INSERT INTO processed_updates(update_id) VALUES (?)", (int(update_id),))
        return False  # не видели
    except sqlite3.IntegrityError:
        return True   # уже видели
    finally:
        conn.close()

def _net_clear_stock(network: str):
    conn = _conn()
    try:
        conn.execute("DELETE FROM stocks WHERE username=? AND network=?", (AGG_USER, network))
    finally:
        conn.close()

def _net_items(network: Optional[str] = None) -> List[str]:
    conn = _conn()
    try:
        if network:
            cur = conn.execute(
                "SELECT DISTINCT item FROM stocks WHERE username=? AND network=? ORDER BY item",
                (AGG_USER, network)
            )
        else:
            cur = conn.execute(
                "SELECT DISTINCT item FROM stocks WHERE username=? ORDER BY item",
                (AGG_USER,)
            )
        return [r["item"] for r in cur.fetchall()]
    finally:
        conn.close()

def _net_get_qty(network: str, item: str) -> int:
    conn = _conn()
    try:
        cur = conn.execute(
            "SELECT qty FROM stocks WHERE username=? AND network=? AND item=?",
            (AGG_USER, network, item)
        )
        row = cur.fetchone()
        return int(row["qty"]) if row else 0
    finally:
        conn.close()

def _net_set_qty(network: str, item: str, qty: int):
    conn = _conn()
    try:
        conn.execute("""
            INSERT INTO stocks(username, item, qty, network, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(username, item, network)
            DO UPDATE SET qty=excluded.qty, updated_at=excluded.updated_at
        """, (AGG_USER, item, int(qty), network, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")))
    finally:
        conn.close()

def _net_add_delta(network: str, item: str, delta: int) -> int:
    current = _net_get_qty(network, item)
    new_qty = current + int(delta)
    _net_set_qty(network, item, new_qty)
    return new_qty

# =========================
# Класс Repo — интерфейс для bot.py
# =========================
class Repo:
    # транзакция-заглушка для совместимости (SQLite автокоммит)
    class _Tx:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
    async def tx(self): return Repo._Tx()

    def __init__(self):
        # гарантируем, что схема создана
        init()

    # ---------- users / networks ----------
    async def get_person_by_tg(self, tg_user_id: int):
        # Мы храним username как строковый tg_id; этого достаточно
        return SimpleNamespace(id=str(tg_user_id))

    async def bind_by_tgid(self, tg_user_id: int, network_name: str):
        uname = str(tg_user_id)
        conn = _conn()
        try:
            conn.execute("""
                INSERT INTO networks(username, network)
                VALUES(?, ?)
                ON CONFLICT(username) DO UPDATE SET network=excluded.network
            """, (uname, network_name))
            _ensure_network_meta(network_name)
        finally:
            conn.close()

    async def bind_by_username(self, username: str, network_name: str):
        uname = username.lstrip("@")
        conn = _conn()
        try:
            conn.execute("""
                INSERT INTO networks(username, network)
                VALUES(?, ?)
                ON CONFLICT(username) DO UPDATE SET network=excluded.network
            """, (uname, network_name))
            _ensure_network_meta(network_name)
        finally:
            conn.close()

    async def ensure_network(self, name: str, city: Optional[str] = None, address: Optional[str] = None):
        _ensure_network_meta(name, city, address)

    async def get_primary_network_for_person(self, person_id: Any) -> Optional[str]:
        uname = str(person_id).lstrip("@")
        conn = _conn()
        try:
            cur = conn.execute("SELECT network FROM networks WHERE username=?", (uname,))
            row = cur.fetchone()
            return row["network"] if row else None
        finally:
            conn.close()

    async def get_network(self, network_id: str):
        # Вернём объект с полем initialized (и, на всякий — названем)
        return SimpleNamespace(id=network_id, name=network_id, initialized=_get_network_initialized(network_id))

    # ---------- планы ----------
    async def set_plan(self, network_name: str, y: int, m: int, qty: int):
        # Пишем в твою таблицу plans_monthly на ключ username=network_name (чтобы не ломать старые отчеты)
        ym = f"{y:04d}-{m:02d}"
        conn = _conn()
        try:
            conn.execute("""
                INSERT INTO plans_monthly(username, ym, plan)
                VALUES(?, ?, ?)
                ON CONFLICT(username, ym) DO UPDATE SET plan=excluded.plan
            """, (network_name, ym, int(qty)))
        finally:
            conn.close()

    # ---------- продажи / приход / сток ----------
    async def insert_sale(self, occurred_at: datetime, day: date, person_id: Any,
                          network_id: str, product_id: str, memory_gb: int, qty: int,
                          source_update_id: Optional[int]):
        uname = str(person_id)
        d = day.strftime("%Y-%m-%d")
        conn = _conn()
        try:
            conn.execute("""
                INSERT INTO sales(username, model, memory, qty, network, date, source_update_id)
                VALUES(?, ?, ?, ?, ?, ?, ?)
            """, (uname, str(product_id), int(memory_gb or 0), int(qty), str(network_id), d, int(source_update_id) if source_update_id else None))
        finally:
            conn.close()

    async def insert_shipment(self, occurred_at: datetime, day: date,
                              network_id: str, product_id: str, memory_gb: int, qty: int):
        conn = _conn()
        try:
            conn.execute("""
                INSERT INTO shipments(network, product, memory, qty, occurred_at)
                VALUES(?, ?, ?, ?, ?)
            """, (str(network_id), str(product_id), int(memory_gb or 0), int(qty),
                  occurred_at.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")))
        finally:
            conn.close()

    async def add_stock(self, network_id: str, product_id: str, memory_gb: int, delta_qty: int) -> int:
        item = _compose_item(str(product_id), int(memory_gb or 0))
        new_qty = _net_add_delta(str(network_id), item, int(delta_qty))
        return new_qty

    async def replace_stock_snapshot(self, network_id: str, rows: List[Tuple[str, int, int]]):
        # rows: [(product_id, mem, qty), ...]
        net = str(network_id)
        _net_clear_stock(net)
        for pid, mem, qty in rows:
            item = _compose_item(str(pid), int(mem or 0))
            _net_set_qty(net, item, int(qty))

    async def set_network_initialized(self, network_id: str, value: bool):
        _set_network_initialized(str(network_id), bool(value))

    async def clear_prompt_flags(self, network_id: str):
        _clear_prompt_flags(str(network_id))

    async def prompt_needed_today(self, network_id: str, kind: str) -> bool:
        return _prompt_needed_today(str(network_id), kind)

    async def touch_last_sale(self, person_id: Any):
        uname = str(person_id)
        conn = _conn()
        try:
            conn.execute("""
                INSERT INTO people_last_sale(username, last_sale_at)
                VALUES(?, ?)
                ON CONFLICT(username) DO UPDATE SET last_sale_at=excluded.last_sale_at
            """, (uname, datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")))
        finally:
            conn.close()

    # ---------- fuzzy кандидаты ----------
    async def get_network_stock_candidates(self, network_id: str) -> List[Tuple[str, str]]:
        items = _net_items(str(network_id))
        # product_id = item (строка), display = item
        return [(it, it) for it in items]

    async def get_product_candidates_with_aliases(self) -> List[Tuple[str, str]]:
        # Возвращаем все уникальные items из сетевых стоков + модели из продаж
        items = set(_net_items(None))
        conn = _conn()
        try:
            cur = conn.execute("SELECT DISTINCT model, memory FROM sales")
            for r in cur.fetchall():
                m = (r["model"] or "").strip()
                mem = r["memory"]
                if not m:
                    continue
                items.add(_compose_item(m, int(mem or 0)))
        finally:
            conn.close()
        items = sorted(items)
        return [(it, it) for it in items]

    # ---------- агрегаты ----------
    async def get_sales_by_network_day(self, day: date, network: Optional[str]) -> List[Tuple[str, int]]:
        d0 = day.strftime("%Y-%m-%d")
        d1 = (day + timedelta(days=1)).strftime("%Y-%m-%d")
        conn = _conn()
        try:
            if network:
                cur = conn.execute("""
                    SELECT network, COALESCE(SUM(qty),0) AS s
                      FROM sales
                     WHERE network=? AND date>=? AND date<?
                     GROUP BY network
                     ORDER BY network
                """, (network, d0, d1))
            else:
                cur = conn.execute("""
                    SELECT network, COALESCE(SUM(qty),0) AS s
                      FROM sales
                     WHERE date>=? AND date<?
                     GROUP BY network
                     ORDER BY network
                """, (d0, d1))
            return [(r["network"], int(r["s"])) for r in cur.fetchall()]
        finally:
            conn.close()

    async def get_sales_by_network_week(self, day: date, network: Optional[str]) -> List[Tuple[str, int]]:
        start = (datetime(day.year, day.month, day.day, tzinfo=TZ) - timedelta(days=day.weekday())).date()
        end = start + timedelta(days=7)
        return await self.get_sales_by_network_between(start, end, network)

    async def get_sales_by_network_between(self, start: date, end: date, network: Optional[str]) -> List[Tuple[str, int]]:
        d0 = start.strftime("%Y-%m-%d")
        d1 = end.strftime("%Y-%m-%d")
        conn = _conn()
        try:
            if network:
                cur = conn.execute("""
                    SELECT network, COALESCE(SUM(qty),0) AS s
                      FROM sales
                     WHERE network=? AND date>=? AND date<?
                     GROUP BY network
                     ORDER BY network
                """, (network, d0, d1))
            else:
                cur = conn.execute("""
                    SELECT network, COALESCE(SUM(qty),0) AS s
                      FROM sales
                     WHERE date>=? AND date<?
                     GROUP BY network
                     ORDER BY network
                """, (d0, d1))
            return [(r["network"], int(r["s"])) for r in cur.fetchall()]
        finally:
            conn.close()

    async def get_sales_by_network_month(self, y: int, m: int, network: Optional[str]) -> List[Tuple[str, int]]:
        d0 = date(y, m, 1)
        d1 = (date(y+1,1,1) if m==12 else date(y, m+1, 1))
        return await self.get_sales_by_network_between(d0, d1, network)

    async def get_stock_table(self, network: Optional[str]) -> List[Tuple[str, int, int]]:
        """
        Возвращает [(canonical_name, mem_gb, qty)]
        """
        conn = _conn()
        try:
            if not network:
                # без сети — пусто, т.к. таблица агрегированная по сети
                return []
            cur = conn.execute("""
                SELECT item, qty FROM stocks
                 WHERE username=? AND network=?
                 ORDER BY item
            """, (AGG_USER, network))
            out: List[Tuple[str, int, int]] = []
            for r in cur.fetchall():
                item = r["item"]
                qty = int(r["qty"])
                # вытащим память из item (если была)
                base, mem = _split_item_model_mem(item)
                # восстановим canonical (без нормализации — лучше показывать как есть)
                # простая эвристика: canonical = item без последнего числа, если оно память
                canonical = item
                if mem is not None:
                    # убрать суффикс " {mem}" при наличии
                    tail = f" {mem}"
                    if item.endswith(tail):
                        canonical = item[: -len(tail)]
                out.append((canonical, int(mem or 0), qty))
            return out
        finally:
            conn.close()

    # ---------- 4 дня без продаж ----------
    async def get_stale_people_by_network(self, days: int) -> Dict[str, List[str]]:
        """
        {network: ['@user1', '@user2', ...]} у кого не было продаж >= days.
        Смотрим на таблицу networks + sales.
        """
        edge = datetime.now(TZ).date() - timedelta(days=days)
        conn = _conn()
        out: Dict[str, List[str]] = {}
        try:
            cur = conn.execute("SELECT username, network FROM networks")
            pairs = cur.fetchall()
            for r in pairs:
                uname, net = r["username"], r["network"]
                c2 = conn.execute("SELECT MAX(date) AS d FROM sales WHERE username=?", (uname,)).fetchone()
                last = c2["d"]
                if not last or last <= edge.strftime("%Y-%m-%d"):
                    out.setdefault(net, []).append(f"@{uname}")
        finally:
            conn.close()
        return out

    # ---------- идемпотентность ----------
    async def mark_and_check_update(self, update_id: int) -> bool:
        return _mark_and_check_update(int(update_id))
