# -*- coding: utf-8 -*-
"""
Production bot.py (aiogram v3, webhook, keep-alive, APScheduler)
Зависимости (requirements.txt):
aiogram>=3.5
aiohttp>=3.9
apscheduler>=3.10
pytz
rapidfuzz>=3.6
SQLAlchemy>=2.0       # если используешь jobstore/sqlalchemy в APS
python-dateutil       # опционально
"""

import os
import re
import asyncio
import logging
import calendar
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from pytz import timezone
from aiohttp import web

from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiogram import BaseMiddleware

from apscheduler.schedulers.asyncio import AsyncIOScheduler
try:
    from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
    HAS_SQLA = True
except Exception:
    HAS_SQLA = False

# Твой модуль работы с БД
import db  # ОЖИДАЕМ db.Repo(...)

# =============================================================================
# Конфиг
# =============================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me")
CRON_KEY = os.getenv("CRON_KEY", "change-me")

# Групповой чат для отчётов/напоминаний (публичные сообщения)
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "0"))

# Единственный админ (ты)
ADMIN_TG_ID = int(os.getenv("ADMIN_TG_ID", "0"))  # задай свой Telegram user id

# Таймзона и планировщик
TZ = timezone("Asia/Almaty")
DATABASE_URL = os.getenv("DATABASE_URL", "")  # если задашь, jobstore будет персистентным

# Keep-alive
KEEPALIVE_ENABLED = os.getenv("KEEPALIVE_ENABLED", "1") == "1"
KEEPALIVE_PATH = os.getenv("KEEPALIVE_PATH", "/")
KEEPALIVE_INTERVAL_MIN = int(os.getenv("KEEPALIVE_INTERVAL_MIN", "4"))

# Поведение по стоку/тише
SILENT_UNBOUND = True  # если нет привязки сети у автора — полная тишина
STRICT_STOCK_PROMPT = True  # просим обновить сток редко и не чаще 1 раза/день/сеть

# =============================================================================
# Логирование
# =============================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bot")

# =============================================================================
# Помощники и парсинг
# =============================================================================

# Игнор всего сообщения, если встречается слово:
IGNORE_WHOLE_MSG_IF_CONTAINS = ("доля",)

SALE_MARKERS = ("продал", "продажа", "прод.", "sale", "шт", "шт.")
STOCK_INC_MARKERS = ("приход", "поступил", "получили", "привезли")
STOCK_SNAPSHOT_PREFIXES = ("сток:", "остаток:", "новый сток:")

MEM_TB_TOKENS = ("1тб", "1tb", "1 tb", "1 тб")
VALID_MEM = {"64", "128", "256", "512", "1024"}

SPACE_RE = re.compile(r"\s+")
PRICE_RE = re.compile(r"(\d[\d\s]{3,})\s*(?:тг|тенге|₸|kzt)", re.IGNORECASE)
RAM_ROM_RE = re.compile(r"\b(\d{1,2})\s*/\s*(\d{2,4})\b")
GB_SUFFIX_RE = re.compile(r"\b(gb|гб)\b", re.IGNORECASE)

def now_local() -> datetime:
    return datetime.now(TZ)

def today_local() -> date:
    return now_local().date()

def _norm(s: str) -> str:
    s = s.lower().replace("ё", "е")
    s = s.replace("×", "x").replace("х", "x")
    s = SPACE_RE.sub(" ", s).strip()
    return s

def contains_ignored_word(s: str) -> bool:
    L = _norm(s)
    return any(w in L for w in IGNORE_WHOLE_MSG_IF_CONTAINS)

def _strip_prices(s: str) -> str:
    return PRICE_RE.sub("", s)

def _extract_qty(s: str) -> Optional[int]:
    s = _norm(s)
    s_wo_gb = GB_SUFFIX_RE.sub("", s)
    for tb in MEM_TB_TOKENS:
        s_wo_gb = s_wo_gb.replace(tb, "")
    m = re.search(r'(?:[-—:]\s*(\d+)\s*$)|(?:x\s*(\d+)\s*$)|(?:\b(\d+)\s*шт\.?\s*$)|(?:\s(\d+)\s*$)', s_wo_gb)
    if m:
        for g in m.groups():
            if g:
                try:
                    return int(g)
                except ValueError:
                    pass
    return None

def _extract_mem(s: str) -> Optional[int]:
    L = _norm(s)
    if any(tok in L.replace(" ", "") for tok in ("1tb", "1тб")):
        return 1024
    m = RAM_ROM_RE.search(L)
    if m:
        mem = m.group(2)
        if mem in VALID_MEM:
            return int(mem)
    m2 = re.search(r'\b(64|128|256|512|1024)\b', L)
    if m2:
        return int(m2.group(1))
    return None

def _clean_model_fragment(s: str) -> str:
    s = _strip_prices(s)
    s = _norm(s)
    s = RAM_ROM_RE.sub(" ", s)
    s = GB_SUFFIX_RE.sub(" ", s)
    for tb in MEM_TB_TOKENS:
        s = s.replace(tb, " ")
    s = re.sub(r'\b(64|128|256|512|1024)\b', " ", s)
    s = re.sub(r'[-—:]\s*\d+\s*$', " ", s)
    s = re.sub(r'\bx\s*\d+\s*$', " ", s)
    s = re.sub(r'\b\d+\s*шт\.?\s*$', " ", s)
    for mk in SALE_MARKERS:
        s = s.replace(mk, " ")
    s = re.sub(r"[^\w\s\-+]", " ", s)
    s = SPACE_RE.sub(" ", s).strip()
    s = re.sub(r'\b(\d+)\s+g\b', r'\1g', s)
    s = re.sub(r'\b(\d+)\s+f\b', r'\1f', s)
    return s

def _has_qty(s: str) -> bool:
    return _extract_qty(s) is not None

def _has_mem_token(s: str) -> bool:
    return _extract_mem(s) is not None

def _looks_like_sale_line(s: str) -> bool:
    L = _norm(s)
    return _has_qty(L) and (any(mk in L for mk in SALE_MARKERS) or _has_mem_token(L))

def parse_sale_line(line: str) -> Optional[Dict[str, Any]]:
    if not line or not line.strip():
        return None
    if contains_ignored_word(line):
        return None
    if not _looks_like_sale_line(line):
        return None
    qty = _extract_qty(line) or 1
    mem = _extract_mem(line)  # может быть None
    model_raw = _clean_model_fragment(line)
    if not model_raw or len(model_raw) < 2:
        return None
    return {"model_raw": model_raw, "mem_gb": mem, "qty": qty}

def parse_sales_message(text: str) -> List[Dict[str, Any]]:
    if not text:
        return []
    if contains_ignored_word(text):
        return []
    out: List[Dict[str, Any]] = []
    raw_lines: List[str] = []
    for raw in text.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        if ';' in raw and len(raw) > 10:
            raw_lines.extend([p.strip() for p in raw.split(';') if p.strip()])
        else:
            raw_lines.append(raw)
    for line in raw_lines:
        item = parse_sale_line(line)
        if item:
            out.append(item)
    return out

def classify_message(text: str) -> str:
    t = (text or "").strip()
    L = t.lower()
    if contains_ignored_word(L):
        return "ignore"
    if any(L.startswith(p) for p in STOCK_SNAPSHOT_PREFIXES):
        return "stock_snapshot"
    if any(k in L for k in STOCK_INC_MARKERS) and _has_qty(L):
        return "stock_inc"
    if _looks_like_sale_line(L):
        return "sale"
    return "ignore"

# =============================================================================
# Repo middleware (внедряем repo в хэндлеры)
# =============================================================================

class RepoMiddleware(BaseMiddleware):
    def __init__(self, repo):
        super().__init__()
        self.repo = repo
    async def __call__(self, handler, event, data):
        data["repo"] = self.repo
        return await handler(event, data)

# =============================================================================
# Бот/диспетчер/роутер
# =============================================================================

bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()
router = Router()
dp.include_router(router)

def is_admin(user_id: int) -> bool:
    return ADMIN_TG_ID and user_id == ADMIN_TG_ID

# =============================================================================
# Вспомогательные отправки с бэкоффом
# =============================================================================

async def safe_send(chat_id: int, text: str):
    delay = 0.5
    for _ in range(5):
        try:
            await bot.send_message(chat_id, text)
            return
        except Exception as e:
            s = str(e).lower()
            if "too many requests" in s or "429" in s:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)
            else:
                log.warning("send failed: %s", e)
                await asyncio.sleep(delay)
    # сдаёмся молча

# =============================================================================
# Хэндлеры сообщений
# =============================================================================

@router.message(F.text.startswith("сеть:"))
async def bind_network(m: Message, repo: db.Repo):
    """
    Пользователь сам может привязаться:
    Формат: "сеть: <название>, <город>[, <адрес>]"
    Для Павлодара адрес обязателен. Для Аксу/Экибастуза нужен город.
    """
    try:
        raw = m.text.split(":", 1)[1].strip()
    except Exception:
        return
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    name = parts[0] if parts else None
    city = parts[1] if len(parts) > 1 else None
    address = parts[2] if len(parts) > 2 else None

    if not name:
        return
    # Валидация по городам
    if city is None and any(x in name.lower() for x in ("аксу", "экибастуз", "ekibastuz", "aksu")):
        city = "Аксу" if "аксу" in name.lower() else "Экибастуз"
    # Павлодар → обязателен адрес
    if city and "павлодар" in city.lower() and not address:
        await m.answer("Для Павлодара укажите адрес: сеть: <название>, Павлодар, <адрес>")
        return

    await repo.ensure_network(name=name, city=city, address=address)
    await repo.bind_by_tgid(m.from_user.id, name)
    # По твоим правилам — без лишних сообщений. Можно ответить коротко:
    await m.answer(f"Привязал к сети: {name}")

@router.message(Command("set_network"))
async def cmd_set_network(m: Message, repo: db.Repo):
    if not is_admin(m.from_user.id):
        return
    # /set_network <@username|tgid> <сеть>
    try:
        _, ident, net = m.text.strip().split(maxsplit=2)
    except Exception:
        await m.answer("Формат: /set_network <@username|tgid> <сеть>")
        return
    await repo.ensure_network(name=net)
    if ident.startswith("@"):
        await repo.bind_by_username(ident[1:], net)
    else:
        try:
            await repo.bind_by_tgid(int(ident), net)
        except Exception:
            await m.answer("tgid должен быть числом")
            return
    await m.answer(f"🔗 {net} → привязка сохранена")

@router.message(Command("set_netinfo"))
async def cmd_set_netinfo(m: Message, repo: db.Repo):
    if not is_admin(m.from_user.id):
        return
    # /set_netinfo <сеть> city=... [address=...]
    text = m.text.strip()
    try:
        _, rest = text.split(" ", 1)
    except Exception:
        await m.answer("Формат: /set_netinfo <сеть> city=<город> [address=<адрес>]")
        return
    parts = rest.split()
    name = parts[0]
    kv = {}
    for p in parts[1:]:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.lower()] = v
    city = kv.get("city")
    address = kv.get("address")
    if city and "павлодар" in city.lower() and not address:
        await m.answer("Для Павлодара нужен address=<адрес>")
        return
    await repo.ensure_network(name=name, city=city, address=address)
    await m.answer("OK")

@router.message(Command("plan"))
async def cmd_plan(m: Message, repo: db.Repo):
    if not is_admin(m.from_user.id):
        return
    # /plan <сеть> <число> [город=...] [адрес=...]
    parts = m.text.strip().split()
    if len(parts) < 3:
        await m.answer("Формат: /plan <сеть> <число> [город=...] [адрес=...]")
        return
    _, net, qty, *rest = parts
    try:
        qty = int(qty)
    except Exception:
        await m.answer("Число плана должно быть int")
        return
    kv = {}
    for p in rest:
        if "=" in p:
            k, v = p.split("=", 1)
            kv[k.lower()] = v
    city = kv.get("город") or kv.get("city")
    address = kv.get("адрес") or kv.get("address")
    if city and "павлодар" in city.lower() and not address:
        await m.answer("Для Павлодара нужен адрес")
        return
    await repo.ensure_network(name=net, city=city, address=address)
    y, mth = today_local().year, today_local().month
    await repo.set_plan(net, y, mth, qty)
    await m.answer(f"План для {net} на {mth:02d}.{y}: {qty}")

@router.message(Command("sales"))
async def cmd_sales(m: Message, repo: db.Repo):
    if not is_admin(m.from_user.id):
        return
    # /sales [day|week|month] [сеть?]
    parts = m.text.strip().split()
    scope = "day"
    net = None
    if len(parts) >= 2:
        if parts[1] in ("day", "week", "month"):
            scope = parts[1]
            if len(parts) >= 3:
                net = parts[2]
        else:
            net = parts[1]
    if scope == "day":
        data = await repo.get_sales_by_network_day(today_local(), net)
        title = f"Сегодня {today_local().strftime('%d.%m.%Y')}"
    elif scope == "week":
        data = await repo.get_sales_by_network_week(today_local(), net)
        title = "Текущая неделя"
    else:
        data = await repo.get_sales_by_network_month(today_local().year, today_local().month, net)
        title = f"Месяц {today_local().month:02d}.{today_local().year}"
    if not data:
        await m.answer(f"{title}: продаж нет")
        return
    lines = [f"📊 {title}:"]
    for name, qty in data:  # [(network, qty)]
        lines.append(f"• {name}: {qty}")
    # Проекция для месяца
    if scope == "month":
        dom = today_local().day
        days_in_month = calendar.monthrange(today_local().year, today_local().month)[1]
        lines.append("")
        lines.append("🔭 Проекция на месяц:")
        for name, qty in data:
            pace = qty / max(dom, 1)
            proj = round(pace * days_in_month)
            lines.append(f"• {name}: MTD {qty} → ~{proj} к {days_in_month}.{today_local().month}")
    await m.answer("\n".join(lines))

@router.message(Command("stocks"))
async def cmd_stocks(m: Message, repo: db.Repo):
    if not is_admin(m.from_user.id):
        return
    # /stocks [сеть?]
    parts = m.text.strip().split()
    net = parts[1] if len(parts) >= 2 else None
    rows = await repo.get_stock_table(net)
    if not rows:
        await m.answer("Нужно обновить сток.")
        return
    lines = ["📦 Текущий сток:"]
    for name, mem, qty in rows:  # [(canonical, mem, qty)]
        tail = f" {mem}ГБ" if mem else ""
        lines.append(f"• {name}{tail} — {qty}")
    await m.answer("\n".join(lines))

@router.message(Command("ask_stocks"))
async def cmd_ask_stocks(m: Message):
    if not is_admin(m.from_user.id):
        return
    # Публичная просьба в общий чат
    if GROUP_CHAT_ID:
        await safe_send(GROUP_CHAT_ID,
            "Коллеги, пришлите, пожалуйста, актуальный сток в формате:\n"
            "сток:\nМодель Память — Количество\nПример:\nReno 11F 5G 128 — 3\nA38 128 — 7\nGalaxy A15 — 5"
        )

@router.message(F.text)
async def on_text(m: Message, repo: db.Repo):
    # Дедуп по update_id (ретраи Телеграма)
    if await repo.mark_and_check_update(m.update_id):
        return

    kind = classify_message(m.text or "")
    if kind == "ignore":
        return

    # Привязка автора к сети обязательна (но мы не подсказываем — полная тишина)
    person = await repo.get_person_by_tg(m.from_user.id)
    if not person:
        if SILENT_UNBOUND:
            return
        else:
            await m.answer("Сделайте привязку: сеть: <название>, <город>[, <адрес>]")
            return
    network_id = await repo.get_primary_network_for_person(person.id)
    if not network_id:
        if SILENT_UNBOUND:
            return
        else:
            await m.answer("Сделайте привязку: сеть: <название>, <город>[, <адрес>]")
            return

    net = await repo.get_network(network_id)

    if kind == "stock_snapshot":
        await handle_stock_snapshot(m, repo, network_id)
        return

    if kind == "stock_inc":
        await handle_stock_inc(m, repo, network_id)
        return

    if kind == "sale":
        await handle_sale(m, repo, network_id, net)
        return

# =============================================================================
# Реализация бизнес-логики
# =============================================================================

async def resolve_product_from_stock_first(repo: db.Repo, network_id: int, raw_model: str) -> Tuple[Optional[int], str]:
    """
    1) кандидаты из стока сети (порог мягче),
    2) кандидаты из всего каталога/алиасов (порог строже).
    repo должен реализовать:
      - get_network_stock_candidates(network_id) -> List[Tuple[int, str]]
      - get_product_candidates_with_aliases() -> List[Tuple[int, str]]
    """
    from rapidfuzz import process, fuzz

    q = _norm(raw_model)

    stock_candidates = await repo.get_network_stock_candidates(network_id)
    if stock_candidates:
        names = [name for _, name in stock_candidates]
        match = process.extractOne(q, names, scorer=fuzz.WRatio)
        if match:
            _, score, idx = match
            if score >= 82:
                return stock_candidates[idx][0], names[idx]

    all_candidates = await repo.get_product_candidates_with_aliases()
    if all_candidates:
        names = [name for _, name in all_candidates]
        match = process.extractOne(q, names, scorer=fuzz.WRatio)
        if match:
            _, score, idx = match
            if score >= 90:
                return all_candidates[idx][0], names[idx]

    return None, raw_model

async def handle_sale(m: Message, repo: db.Repo, network_id: int, net: Any):
    wrote_any = False
    prompted_today = False
    items = parse_sales_message(m.text or "")
    for it in items:
        pid, canonical = await resolve_product_from_stock_first(repo, network_id, it["model_raw"])
        if not pid:
            # строго игнорим нераспознанную модель
            continue

        mem = it["mem_gb"] or 0
        qty = it["qty"]

        await repo.insert_sale(
            occurred_at=now_local(),
            day=today_local(),
            person_id=(await repo.get_person_by_tg(m.from_user.id)).id,
            network_id=network_id,
            product_id=pid,
            memory_gb=mem,
            qty=qty,
            source_update_id=m.update_id,
        )
        wrote_any = True

        new_qty = await repo.add_stock(network_id, pid, mem, -qty)  # может стать отрицательным

        # Просить обновить: только по делу и не чаще 1р/день/сеть
        if new_qty < 0:
            if STRICT_STOCK_PROMPT and await repo.prompt_needed_today(network_id, kind="negative"):
                await safe_send(m.chat.id, "Остаток ушёл в минус, обновите сток.")
                prompted_today = True
        else:
            # Если сеть инициализирована, но модели не было в стоке (создали позицию впервые) — new_qty мог быть >=0.
            # Просьбу в этом случае **не шлём**, пока не будет реального минуса (по твоим правилам).
            pass

    if wrote_any:
        await repo.touch_last_sale((await repo.get_person_by_tg(m.from_user.id)).id)
        # Если сеть не инициализирована — НЕ просим сток, пока не будет минуса. (как договорились)

async def handle_stock_inc(m: Message, repo: db.Repo, network_id: int):
    # инкремент построчно
    for line in (l for l in (m.text or "").splitlines() if l.strip()):
        if classify_message(line) != "stock_inc":
            continue
        qty = _extract_qty(line)
        mem = _extract_mem(line)
        pid, _ = await resolve_product_from_stock_first(repo, network_id, _clean_model_fragment(line))
        if not pid or not qty:
            continue
        await repo.insert_shipment(
            occurred_at=now_local(),
            day=today_local(),
            network_id=network_id,
            product_id=pid,
            memory_gb=mem or 0,
            qty=qty,
        )
        await repo.add_stock(network_id, pid, mem or 0, +qty)
    # никаких просьб «сверить» — ждём явный снапшот

async def handle_stock_snapshot(m: Message, repo: db.Repo, network_id: int):
    # строки после заголовка
    rows: List[Tuple[int, int, int]] = []
    for line in (m.text or "").splitlines()[1:]:
        l = line.strip()
        if not l:
            continue
        # пытаемся извлечь qty: «— 3», «- 7», «: 5»
        qty = _extract_qty(l)
        # память может быть, а может нет
        mem = _extract_mem(l)
        pid, _ = await resolve_product_from_stock_first(repo, network_id, _clean_model_fragment(l))
        if pid and qty is not None:
            rows.append((pid, mem or 0, qty))

    # атомарная замена стока + помечаем сеть как инициализированную
    async with repo.tx():
        await repo.replace_stock_snapshot(network_id, rows)
        await repo.set_network_initialized(network_id, True)
        await repo.clear_prompt_flags(network_id)

    await safe_send(m.chat.id, "Обновил сток, спасибо.")

# =============================================================================
# Ежедневные задачи
# =============================================================================

async def daily_summary_and_projection(repo: db.Repo):
    if not GROUP_CHAT_ID:
        return
    y, mth, dom = today_local().year, today_local().month, today_local().day
    days_in_month = calendar.monthrange(y, mth)[1]

    per_network_today = await repo.get_sales_by_network_day(today_local(), None)
    per_network_mtd   = await repo.get_sales_by_network_month(y, mth, None)

    lines = [f"🗓 Отчёт за {today_local().strftime('%d.%m.%Y')}"]
    if not per_network_today:
        lines.append("Сегодня продаж пока нет.")
    else:
        lines.append("Сегодня:")
        for n, qty in per_network_today:
            lines.append(f"• {n}: {qty} шт")

    if per_network_mtd:
        lines.append("")
        lines.append("🔭 Проекция на месяц:")
        for n, qty_mtd in per_network_mtd:
            pace = qty_mtd / max(dom, 1)
            proj = round(pace * days_in_month)
            lines.append(f"• {n}: MTD {qty_mtd} → ~{proj} к {days_in_month}.{mth}")

    await safe_send(GROUP_CHAT_ID, "\n".join(lines))

async def remind_no_sales_4d(repo: db.Repo):
    if not GROUP_CHAT_ID:
        return
    groups = await repo.get_stale_people_by_network(days=4)  # {network_name: ['@user1','@user2',...]}
    if not groups:
        return
    lines = ["Нет продаж 4 дня:"]
    for net, users in groups.items():
        if not users:
            continue
        lines.append(f"• {net}: " + ", ".join(users))
    await safe_send(GROUP_CHAT_ID, "\n".join(lines))

# =============================================================================
# Keep-alive и веб-сервер
# =============================================================================

async def health(_):
    return web.json_response({"ok": True, "ts": datetime.utcnow().isoformat()})

async def cron_daily_report(request: web.Request):
    if request.query.get("key") != CRON_KEY:
        return web.Response(status=401, text="unauthorized")
    app = request.app
    repo: db.Repo = app["repo"]
    await daily_summary_and_projection(repo)
    return web.Response(text="ok")

async def keepalive_ping():
    if not RENDER_EXTERNAL_URL or not KEEPALIVE_ENABLED:
        return
    import aiohttp, random
    url = f"{RENDER_EXTERNAL_URL}{KEEPALIVE_PATH}"
    await asyncio.sleep(0.2 + random.random() * 0.6)
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        headers = {"User-Agent": "keepalive-bot/1.0"}
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.head(url, headers=headers) as r:
                if r.status >= 400:
                    async with s.get(url, headers=headers) as rr:
                        pass
    except Exception as e:
        log.debug("keepalive error: %s", e)

async def on_startup(app: web.Application):
    # Repo из твоего db.py (не ломаем твои данные/привязки)
    app["repo"] = db.Repo()
    dp.message.middleware(RepoMiddleware(app["repo"]))

    # Снести старые хуки и поставить новый — исключает «двух ботов»
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    if RENDER_EXTERNAL_URL:
        url = f"{RENDER_EXTERNAL_URL}/webhook"
        await bot.set_webhook(
            url=url,
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
            allowed_updates=["message", "edited_message"]
        )
        log.info("Webhook set to %s", url)
    else:
        log.warning("RENDER_EXTERNAL_URL пуст — вебхук не поставлен")

    # Планировщик
    jobstores = {"default": SQLAlchemyJobStore(url=DATABASE_URL)} if (DATABASE_URL and HAS_SQLA) else None
    scheduler = AsyncIOScheduler(timezone=str(TZ), jobstores=jobstores)
    # Свод в 20:00
    scheduler.add_job(lambda: daily_summary_and_projection(app["repo"]), "cron",
                      hour=20, minute=0, misfire_grace_time=3600, id="daily_report")
    # Напоминания в 10:00
    scheduler.add_job(lambda: remind_no_sales_4d(app["repo"]), "cron",
                      hour=10, minute=0, misfire_grace_time=3600, id="no_sales_4d")
    # Keep-alive каждые 4 минуты
    if KEEPALIVE_ENABLED:
        scheduler.add_job(keepalive_ping, "interval", minutes=KEEPALIVE_INTERVAL_MIN,
                          id="keepalive", next_run_time=now_local() + timedelta(seconds=10))
    scheduler.start()
    app["scheduler"] = scheduler
    log.info("Scheduler started")

async def on_cleanup(app: web.Application):
    scheduler = app.get("scheduler")
    if scheduler:
        scheduler.shutdown(wait=False)

def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_head("/", health)
    app.router.add_post("/cron/daily_report", cron_daily_report)
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET).register(app, path="/webhook")
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

# =============================================================================
# Entry
# =============================================================================

if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("Set BOT_TOKEN environment variable")
    web.run_app(build_app(), port=PORT)
