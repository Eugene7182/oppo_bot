# db.py — SQLite Repo для нового bot.py
import os, sqlite3, contextlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

DB_PATH = os.getenv("DB_PATH", "sales.db")

def _conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def _today_str() -> str:
    return date.today().strftime("%Y-%m-%d")

@dataclass
class Person:
    id: str           # используем строку tgid
    username: str|None

class Repo:
    def __init__(self):
        self.conn = _conn()
        self._init_schema()

    # ---------- schema ----------
    def _init_schema(self):
        c = self.conn.cursor()

        # люди
        c.execute("""
        CREATE TABLE IF NOT EXISTS people(
            tgid      TEXT PRIMARY KEY,
            username  TEXT,
            last_sale TEXT
        )""")

        # сети
        c.execute("""
        CREATE TABLE IF NOT EXISTS networks(
            name       TEXT PRIMARY KEY,
            city       TEXT,
            address    TEXT,
            initialized INTEGER DEFAULT 0
        )""")

        # привязки
        c.execute("""
        CREATE TABLE IF NOT EXISTS person_network(
            tgid    TEXT PRIMARY KEY REFERENCES people(tgid) ON DELETE CASCADE,
            network TEXT REFERENCES networks(name) ON DELETE SET NULL
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS username_network(
            username TEXT PRIMARY KEY,
            network  TEXT REFERENCES networks(name) ON DELETE SET NULL
        )""")

        # продукты и алиасы
        c.execute("""
        CREATE TABLE IF NOT EXISTS products(
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS aliases(
            alias TEXT PRIMARY KEY,
            product_id INTEGER REFERENCES products(id) ON DELETE CASCADE
        )""")

        # стоки
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock(
            network   TEXT REFERENCES networks(name) ON DELETE CASCADE,
            product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
            memory_gb INTEGER NOT NULL DEFAULT 0,
            qty       INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT,
            PRIMARY KEY(network, product_id, memory_gb)
        )""")

        # продажи
        c.execute("""
        CREATE TABLE IF NOT EXISTS sales(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at TEXT,
            day TEXT,
            tgid TEXT REFERENCES people(tgid),
            network TEXT REFERENCES networks(name),
            product_id INTEGER REFERENCES products(id),
            memory_gb INTEGER,
            qty INTEGER,
            source_update_id INTEGER
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_sales_day ON sales(day)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_sales_net ON sales(network)")

        # поставки/приход
        c.execute("""
        CREATE TABLE IF NOT EXISTS shipments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at TEXT,
            day TEXT,
            network TEXT REFERENCES networks(name),
            product_id INTEGER REFERENCES products(id),
            memory_gb INTEGER,
            qty INTEGER
        )""")

        # планы по сети
        c.execute("""
        CREATE TABLE IF NOT EXISTS plans(
            network TEXT,
            year INTEGER,
            month INTEGER,
            plan INTEGER,
            PRIMARY KEY(network, year, month)
        )""")

        # флаги напоминаний (чтоб не спамить)
        c.execute("""
        CREATE TABLE IF NOT EXISTS prompts(
            network TEXT,
            kind TEXT,
            last_date TEXT,
            PRIMARY KEY(network, kind)
        )""")

        # антидубль апдейтов
        c.execute("""
        CREATE TABLE IF NOT EXISTS processed_updates(
            update_id INTEGER PRIMARY KEY
        )""")

        self.conn.commit()

    # ---------- утилиты ----------
    @contextlib.asynccontextmanager
    async def tx(self):
        try:
            self.conn.execute("BEGIN")
            yield
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    # ---------- люди / привязки ----------
    async def get_person_by_tg(self, tgid: int) -> Person:
        tgid = str(tgid)
        cur = self.conn.execute("SELECT tgid, username FROM people WHERE tgid=?", (tgid,))
        row = cur.fetchone()
        if not row:
            self.conn.execute("INSERT INTO people(tgid) VALUES(?)", (tgid,))
            self.conn.commit()
            return Person(id=tgid, username=None)
        return Person(id=row["tgid"], username=row["username"])

    async def bind_by_tgid(self, tgid: int, network: str):
        tgid = str(tgid)
        self.conn.execute("INSERT OR IGNORE INTO people(tgid) VALUES(?)", (tgid,))
        self.conn.execute("INSERT INTO networks(name) VALUES(?) ON CONFLICT(name) DO NOTHING", (network,))
        self.conn.execute("""
            INSERT INTO person_network(tgid, network) VALUES(?,?)
            ON CONFLICT(tgid) DO UPDATE SET network=excluded.network
        """, (tgid, network))
        self.conn.commit()

    async def bind_by_username(self, username: str, network: str):
        u = (username or "").lstrip("@")
        self.conn.execute("INSERT INTO networks(name) VALUES(?) ON CONFLICT(name) DO NOTHING", (network,))
        self.conn.execute("""
            INSERT INTO username_network(username, network) VALUES(?,?)
            ON CONFLICT(username) DO UPDATE SET network=excluded.network
        """, (u, network))
        self.conn.commit()

    async def get_network_by_username(self, username: str) -> Optional[str]:
        u = (username or "").lstrip("@")
        cur = self.conn.execute("SELECT network FROM username_network WHERE username=?", (u,))
        r = cur.fetchone()
        return r["network"] if r else None

    async def get_primary_network_for_person(self, person_id: str) -> Optional[str]:
        cur = self.conn.execute("SELECT network FROM person_network WHERE tgid=?", (str(person_id),))
        r = cur.fetchone()
        return r["network"] if r else None

    async def ensure_network(self, name: str, city: Optional[str]=None, address: Optional[str]=None):
        self.conn.execute("""
            INSERT INTO networks(name, city, address) VALUES(?,?,?)
            ON CONFLICT(name) DO UPDATE SET
                city=COALESCE(excluded.city, city),
                address=COALESCE(excluded.address, address)
        """, (name, city, address))
        self.conn.commit()

    async def get_network(self, name: str) -> Dict[str, Any]:
        cur = self.conn.execute("SELECT * FROM networks WHERE name=?", (name,))
        r = cur.fetchone()
        return dict(r) if r else {"name": name, "city": None, "address": None, "initialized": 0}

    # ---------- продукты/алиасы ----------
    async def get_product_candidates_with_aliases(self) -> List[Tuple[int, str]]:
        rows = []
        rows += [(r["id"], r["name"]) for r in self.conn.execute("SELECT id,name FROM products").fetchall()]
        rows += [(r["product_id"], r["alias"]) for r in self.conn.execute("SELECT product_id,alias FROM aliases").fetchall()]
        return rows

    async def get_network_stock_candidates(self, network: str) -> List[Tuple[int, str]]:
        cur = self.conn.execute("""
            SELECT s.product_id, p.name
            FROM stock s JOIN products p ON p.id=s.product_id
            WHERE s.network=?
            GROUP BY s.product_id
        """, (network,))
        return [(r["product_id"], r["name"]) for r in cur.fetchall()]

    # (вдруг пригодится) завести продукт и алиас
    async def ensure_product(self, canonical_name: str, alias: Optional[str]=None) -> int:
        cur = self.conn.execute("INSERT INTO products(name) VALUES(?) ON CONFLICT(name) DO NOTHING", (canonical_name,))
        if cur.rowcount == 0:
            cur = self.conn.execute("SELECT id FROM products WHERE name=?", (canonical_name,))
        else:
            cur = self.conn.execute("SELECT last_insert_rowid() AS id")
        pid = int(cur.fetchone()["id"])
        if alias:
            self.conn.execute("INSERT INTO aliases(alias, product_id) VALUES(?,?) ON CONFLICT(alias) DO UPDATE SET product_id=excluded.product_id", (alias, pid))
        self.conn.commit()
        return pid

    # ---------- сток ----------
    async def add_stock(self, network: str, product_id: int, memory_gb: int, delta: int) -> int:
        # upsert
        cur = self.conn.execute("""
            SELECT qty FROM stock WHERE network=? AND product_id=? AND memory_gb=?
        """, (network, product_id, memory_gb or 0))
        row = cur.fetchone()
        if row:
            new_qty = int(row["qty"]) + int(delta)
            self.conn.execute("""
                UPDATE stock SET qty=?, updated_at=datetime('now','localtime')
                WHERE network=? AND product_id=? AND memory_gb=?
            """, (new_qty, network, product_id, memory_gb or 0))
        else:
            new_qty = int(delta)
            self.conn.execute("""
                INSERT INTO stock(network,product_id,memory_gb,qty,updated_at)
                VALUES(?,?,?,?,datetime('now','localtime'))
            """, (network, product_id, memory_gb or 0, new_qty))
        self.conn.commit()
        return new_qty

    async def replace_stock_snapshot(self, network: str, rows: List[Tuple[int,int,int]]):
        # rows: [(product_id, mem, qty)]
        self.conn.execute("DELETE FROM stock WHERE network=?", (network,))
        for pid, mem, qty in rows:
            self.conn.execute("""
                INSERT INTO stock(network,product_id,memory_gb,qty,updated_at)
                VALUES(?,?,?,?,datetime('now','localtime'))
            """, (network, pid, mem or 0, int(qty)))
        self.conn.commit()

    async def set_network_initialized(self, network: str, flag: bool):
        self.conn.execute("UPDATE networks SET initialized=? WHERE name=?", (1 if flag else 0, network))
        self.conn.commit()

    async def clear_prompt_flags(self, network: str):
        self.conn.execute("DELETE FROM prompts WHERE network=?", (network,))
        self.conn.commit()

    async def get_stock_table(self, network: Optional[str]) -> List[Tuple[str, Optional[int], int]]:
        if not network:
            return []
        cur = self.conn.execute("""
            SELECT p.name AS name, s.memory_gb AS mem, s.qty AS qty
            FROM stock s JOIN products p ON p.id=s.product_id
            WHERE s.network=?
            ORDER BY p.name, s.memory_gb
        """, (network,))
        return [(r["name"], r["mem"], r["qty"]) for r in cur.fetchall()]

    # ---------- продажи/поставки ----------
    async def insert_sale(self, occurred_at: datetime, day: date, person_id: str,
                          network_id: str, product_id: int, memory_gb: int, qty: int,
                          source_update_id: int):
        self.conn.execute("""
            INSERT INTO sales(occurred_at,day,tgid,network,product_id,memory_gb,qty,source_update_id)
            VALUES(?,?,?,?,?,?,?,?)
        """, (occurred_at.isoformat(), day.strftime("%Y-%m-%d"), str(person_id),
              network_id, product_id, memory_gb or 0, int(qty), int(source_update_id)))
        # обновим last_sale у человека
        self.conn.execute("UPDATE people SET last_sale=? WHERE tgid=?", (day.strftime("%Y-%m-%d"), str(person_id)))
        self.conn.commit()

    async def insert_shipment(self, occurred_at: datetime, day: date,
                              network_id: str, product_id: int, memory_gb: int, qty: int):
        self.conn.execute("""
            INSERT INTO shipments(occurred_at,day,network,product_id,memory_gb,qty)
            VALUES(?,?,?,?,?,?)
        """, (occurred_at.isoformat(), day.strftime("%Y-%m-%d"),
              network_id, product_id, memory_gb or 0, int(qty)))
        self.conn.commit()

    async def touch_last_sale(self, person_id: str):
        self.conn.execute("UPDATE people SET last_sale=? WHERE tgid=?", (_today_str(), str(person_id)))
        self.conn.commit()

    # ---------- отчёты ----------
    async def get_sales_by_network_day(self, d: date, only_network: Optional[str]) -> List[Tuple[str,int]]:
        sql = "SELECT network, SUM(qty) s FROM sales WHERE day=?"
        args = [d.strftime("%Y-%m-%d")]
        if only_network:
            sql += " AND network=?"
            args.append(only_network)
        sql += " GROUP BY network ORDER BY s DESC"
        cur = self.conn.execute(sql, args)
        return [(r["network"], int(r["s"])) for r in cur.fetchall()]

    async def get_sales_by_network_week(self, today: date, only_network: Optional[str]) -> List[Tuple[str,int]]:
        # ISO: понедельник — воскресенье
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=7)
        sql = "SELECT network, SUM(qty) s FROM sales WHERE day>=? AND day<?"
        args = [start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")]
        if only_network:
            sql += " AND network=?"
            args.append(only_network)
        sql += " GROUP BY network ORDER BY s DESC"
        cur = self.conn.execute(sql, args)
        return [(r["network"], int(r["s"])) for r in cur.fetchall()]

    async def get_sales_by_network_month(self, y: int, m: int, only_network: Optional[str]) -> List[Tuple[str,int]]:
        start = date(y, m, 1)
        end = date(y+1,1,1) if m==12 else date(y, m+1, 1)
        sql = "SELECT network, SUM(qty) s FROM sales WHERE day>=? AND day<?"
        args = [start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")]
        if only_network:
            sql += " AND network=?"
            args.append(only_network)
        sql += " GROUP BY network ORDER BY s DESC"
        cur = self.conn.execute(sql, args)
        return [(r["network"], int(r["s"])) for r in cur.fetchall()]

    async def set_plan(self, network: str, y: int, m: int, plan: int):
        self.conn.execute("""
            INSERT INTO plans(network,year,month,plan) VALUES(?,?,?,?)
            ON CONFLICT(network,year,month) DO UPDATE SET plan=excluded.plan
        """, (network, y, m, int(plan)))
        self.conn.commit()

    async def get_stale_people_by_network(self, days: int=4) -> Dict[str, List[str]]:
        cutoff = date.today() - timedelta(days=days)
        cur = self.conn.execute("""
            SELECT pn.network, p.username, p.tgid, p.last_sale
            FROM person_network pn
            JOIN people p ON p.tgid=pn.tgid
        """)
        res: Dict[str, List[str]] = {}
        for r in cur.fetchall():
            d = r["last_sale"]
            stale = (not d) or (d < cutoff.strftime("%Y-%m-%d"))
            if stale:
                shown = f"@{r['username']}" if r["username"] else r["tgid"]
                res.setdefault(r["network"], []).append(shown)
        return res

    # ---------- напоминания ----------
    async def prompt_needed_today(self, network: str, kind: str="negative") -> bool:
        today = _today_str()
        cur = self.conn.execute("SELECT last_date FROM prompts WHERE network=? AND kind=?", (network, kind))
        r = cur.fetchone()
        if r and r["last_date"] == today:
            return False
        self.conn.execute("""
            INSERT INTO prompts(network,kind,last_date) VALUES(?,?,?)
            ON CONFLICT(network,kind) DO UPDATE SET last_date=excluded.last_date
        """, (network, kind, today))
        self.conn.commit()
        return True

    # ---------- антидубль ----------
    async def mark_and_check_update(self, update_id: int) -> bool:
        try:
            self.conn.execute("INSERT INTO processed_updates(update_id) VALUES(?)", (int(update_id),))
            self.conn.commit()
            # простой трим старья
            self.conn.execute("DELETE FROM processed_updates WHERE update_id < (SELECT MAX(update_id)-50000 FROM processed_updates)")
            self.conn.commit()
            return False  # еще не было
        except sqlite3.IntegrityError:
            return True   # уже видели
