import asyncio
import calendar
import logging
import os
import re
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from aiohttp import web

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiogram.filters import Command

import db

# =========================
# --- Конфиг окружения ---
# =========================
TOKEN = os.getenv("BOT_TOKEN", "")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-1000000000000"))
ADMINS_ENV = [s.strip().lstrip("@") for s in os.getenv("ADMINS", "").split(",")] if os.getenv("ADMINS") else []
tz = timezone("Asia/Almaty")

WEBHOOK_URL = f"{os.getenv('RENDER_EXTERNAL_URL')}/webhook"
PORT = int(os.getenv("PORT", 10000))

# ==============
# --- Telegram
# ==============
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

logging.basicConfig(level=logging.INFO)

# =========================
# --- Утилиты форматирования
# =========================
def now_ala():
    return datetime.now(tz)

def yyyymm(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def parse_yyyymm(s: str | None) -> tuple[int, int]:
    if not s:
        d = now_ala()
        return d.year, d.month
    s = s.strip()
    if re.fullmatch(r"\d{4}-\d{2}", s):
        year, month = s.split("-")
        return int(year), int(month)
    if re.fullmatch(r"\d{6}", s):
        return int(s[:4]), int(s[4:])
    raise ValueError("Неверный формат месяца. Жду YYYY-MM, например 2025-08.")

def pct(a: int, b: int) -> str:
    if b <= 0:
        return "—"
    return f"{round(a * 100 / b)}%"

def bold(s: str) -> str:
    return f"<b>{s}</b>"

def code(s: str) -> str:
    return f"<code>{s}</code>"

def eusername(message: Message) -> str:
    return (message.from_user.username or str(message.from_user.id)).lstrip("@")

def extract_mentioned_username(text: str) -> str | None:
    m = re.search(r"@([A-Za-z0-9_]+)", text or "")
    return m.group(1) if m else None

# NEW: поддержка @username ИЛИ числового ID
def extract_user_ref(text: str) -> str | None:
    if not text:
        return None
    m = re.search(r"@([A-Za-z0-9_]+)", text)
    if m:
        return m.group(1)
    m2 = re.search(r"\b\d{6,12}\b", text)  # телеграм id обычно 7-10 цифр
    if m2:
        return m2.group(0)
    return None

def human_network(net: str) -> str:
    return net if net and net != "-" else "—"

# -------------------------
# Обёртки для совместимости
# -------------------------
def list_admins_safe():
    if hasattr(db, "list_admins"):
        try:
            return db.list_admins()
        except Exception:
            pass
    if hasattr(db, "get_admins"):
        try:
            return db.get_admins()
        except Exception:
            pass
    return []

def get_last_sale_dt(username: str):
    if hasattr(db, "get_last_sale_time"):
        try:
            return db.get_last_sale_time(username)
        except Exception:
            return None
    if hasattr(db, "get_last_sale"):
        try:
            s = db.get_last_sale(username)
            if not s:
                return None
            return tz.localize(datetime.strptime(s, "%Y-%m-%d"))
        except Exception:
            return None
    return None

# ======================
# --- Определение сети
# ======================
def extract_network(username: str, text: str | None) -> str:
    bind = db.get_network(username)
    if bind and bind != "-":
        return bind
    t = (text or "").lower()
    for key in ["mechta", "beeline", "sulpak", "sulpka", "td"]:
        if key in t:
            return key.capitalize()
    return "-"

# ===============
# --- Админы ---
# ===============
def is_admin(username: str) -> bool:
    if not username:
        return False
    if username.lstrip("@") in ADMINS_ENV:
        return True
    return db.is_admin(username.lstrip("@"))

async def admin_guard(message: Message) -> bool:
    if not is_admin(eusername(message)):
        await message.reply("⛔ Только для админов.")
        return False
    return True

# ============================
# --- Регулярки парсинга ---
# ============================
# Примеры: "Reno 11F 5G 128 - 2", "11 128 1", "reno 12 256", "11f 5g 128-3"
SALE_RE = re.compile(
    r"((?:reno\s*)?\d{1,2}\s*(?:f)?\s*(?:5\s*g)?)\s*(\d{1,4})(?:тб|tb)?\s*[-—: ]?\s*(\d+)?",
    re.IGNORECASE
)

# Для обновления стоков строками: "Reno 11F 5G 128 - 3"
STOCK_RE = re.compile(
    r"([a-zа-яё0-9\+\-\s]+?)\s*(?:\(?\d+\s*/\s*\)?)?\s*(\d{1,4})(?:тб|tb)?\s*[-—: ]?\s*(\d+)?",
    re.IGNORECASE
)

# ==================================
# ЕДИНЫЙ ОБРАБОТЧИК ТЕКСТОВ (без /)
# ==================================
@router.message(F.text & ~F.text.startswith("/"))
async def handle_message(message: Message):
    if message.chat.id != GROUP_CHAT_ID:
        return

    text = message.text.strip()
    if not text or text.startswith("/") or "доля" in text.lower():
        return

    user = eusername(message)
    network = extract_network(user, text)

    try:
        # --- СТОКИ ---
        if any(w in text.lower() for w in ["сток", "остаток", "stock", "stocks", "приход", "приехал", "поступил", "остатки"]):
            rows = text.splitlines()
            updated = []
            for row in rows:
                m = STOCK_RE.search(row)
                if not m:
                    continue
                model = re.sub(r"\s+", " ", m.group(1)).strip()
                memory = m.group(2)
                qty = int(m.group(3)) if m.group(3) else 0
                item_name = f"{model} {memory}"
                db.update_stock(user, item_name, qty, network)
                updated.append(f"{item_name} = {qty}")
            if updated:
                await message.reply("📦 Обновлено:\n" + "\n".join(updated) + f"\nСеть: {human_network(network)}")
            return

        # --- ПРОДАЖИ ---
        matches = SALE_RE.findall(text)
        if not matches:
            return

        # Есть ли у юзера стоки (чтобы уметь списывать)
        user_stocks = [row for row in db.get_stocks() if row[0] == user]

        for model_raw, memory, qty_raw in matches:
            model_norm = re.sub(r"\s+", "", model_raw).lower()
            qty = int(qty_raw) if qty_raw else 1

            # Запись продажи
            db.add_sale(user, model_norm, str(memory), qty, network)

            # Стоки
            if not user_stocks:
                continue

            stock_item, stock_qty = db.find_stock_like(user, model_norm, str(memory), network)
            if stock_item is None:
                await message.reply(f"⚠️ Остаток для {model_norm} {memory} не найден. @{user}, обновите сток!")
            elif stock_qty < qty:
                await message.reply(f"⚠️ У @{user} не хватает стока для {stock_item} (продажа {qty}, остаток {stock_qty}).")
            else:
                db.decrease_stock(user, stock_item, qty, network)

        if user_stocks:
            await message.reply(f"✅ Продажи учтены. Сеть: {human_network(network)}")

    except Exception as e:
        logging.exception("handle_message error")
        await message.reply(f"⚠️ Ошибка: {e}")

# ============================
# --- Команды админа/пользы ---
# ============================

@router.message(Command("ping"))
async def cmd_ping(message: Message):
    await message.reply("🏓 Готов к работе.")

@router.message(Command("help"))
async def cmd_help(message: Message):
    txt = [
        bold("Команды:"),
        "/help — это меню",
        "/stocks [@user] [network] — показать стоки (по умолчанию твои)",
        "/sales_month [YYYY-MM] [@user] — продажи за месяц",
        "/set_network @user network или '-' — привязать сеть пользователю (можно ID вместо @user)",
        "/set_sales @user model memory qty [network] — добавить продажу",
        "/set_plan @user|all PLAN [YYYY-MM] — задать план",
        "/plan_show [YYYY-MM] — показать планы",
        "/admins_show — список админов",
        "/admin_add @user — выдать админа",
        "/admin_remove @user — забрать админа",
    ]
    await message.reply("\n".join(txt))

@router.message(Command("admins_show"))
async def cmd_admins_show(message: Message):
    if not await admin_guard(message):
        return
    env = ", ".join(f"@{u}" for u in ADMINS_ENV) if ADMINS_ENV else "—"
    dbs = ", ".join(f"@{u}" for u in list_admins_safe()) or "—"
    await message.reply(f"👮 ENV: {env}\n👮 DB: {dbs}")

@router.message(Command("admin_add"))
async def cmd_admin_add(message: Message):
    if not await admin_guard(message):
        return
    u = extract_mentioned_username(message.text)
    if not u:
        await message.reply("Формат: /admin_add @user")
        return
    db.add_admin(u)
    await message.reply(f"✅ @{u} теперь админ.")

@router.message(Command("admin_remove"))
async def cmd_admin_remove(message: Message):
    if not await admin_guard(message):
        return
    u = extract_mentioned_username(message.text)
    if not u:
        await message.reply("Формат: /admin_remove @user")
        return
    db.remove_admin(u)
    await message.reply(f"🗑️ @{u} удалён из админов.")

@router.message(Command("set_network"))
async def cmd_set_network(message: Message):
    if not await admin_guard(message):
        return
    parts = message.text.split()
    # ПРИНИМАЕМ @username ИЛИ ID
    target = extract_user_ref(message.text)
    if not target or len(parts) < 3:
        await message.reply("Формат: /set_network @user network   или   /set_network user_id network   или   /set_network @user -")
        return
    network = parts[-1]
    if network == "@"+str(target):  # если слиплось
        await message.reply("Формат: /set_network @user network   или   /set_network user_id network   или   /set_network @user -")
        return
    if network == "-":
        db.set_network(target, "-")
        await message.reply(f"❌ Сеть для {target} удалена")
        return
    db.set_network(target, network)
    await message.reply(f"🔗 {target} → сеть: {human_network(network)}")

@router.message(Command("stocks"))
async def cmd_stocks(message: Message):
    # /stocks [@user] [network]
    parts = (message.text or "").split()
    u = extract_mentioned_username(message.text)
    net = None
    if len(parts) >= 2 and not u:
        net = parts[1] if parts[1] != "-" else None
    if len(parts) >= 3 and u:
        net = parts[2] if parts[2] != "-" else None
    # доступ: админ видит всех, обычный — только себя
    viewer = eusername(message)
    target = u or viewer
    if target != viewer and not is_admin(viewer):
        await message.reply("⛔ Можно смотреть только свои стоки (или быть админом).")
        return
    rows = db.get_stocks(username=target, network=net)
    if not rows:
        await message.reply(f"📦 Стоков нет. @{target} сеть: {human_network(net or db.get_network(target))}")
        return
    lines = [bold(f"📦 Стоки @{target} (сеть: {human_network(net or db.get_network(target))})")]
    for _, item, qty, network in rows:
        lines.append(f"{item} — {qty}")
    await message.reply("\n".join(lines))

@router.message(Command("sales_month")))
async def cmd_sales_month(message: Message):
    # /sales_month [YYYY-MM] [@user]
    txt = (message.text or "").strip()
    u = extract_mentioned_username(txt)
    arg_month = None
    for token in txt.split():
        if re.fullmatch(r"\d{4}-\d{2}", token) or re.fullmatch(r"\d{6}", token):
            arg_month = token
            break
    year, month = parse_yyyymm(arg_month)
    viewer = eusername(message)
    target = u or viewer
    if target != viewer and not is_admin(viewer):
        await message.reply("⛔ Можно смотреть только свои продажи (или быть админом).")
        return
    total, by_model = db.month_sales(year, month, username=target)
    plan = db.get_plan(target, f"{year:04d}-{month:02d}") or 0
    k = pct(total, plan) if plan else "—"
    # В отчёте показываем сеть, если задана
    name_for_report = db.get_network(target)
    display_name = name_for_report if name_for_report and name_for_report != "-" else f"@{target}"
    hdr = bold(f"📈 Продажи {display_name} за {year:04d}-{month:02d}") + f"\nПлан: {plan} | Факт: {total} | Вып: {k}"
    lines = [hdr]
    if by_model:
        for m, s in sorted(by_model.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"• {m} — {s}")
    await message.reply("\n".join(lines))

@router.message(Command("set_sales"))
async def cmd_set_sales(message: Message):
    if not await admin_guard(message):
        return
    # /set_sales @user model memory qty [network]
    # Пример: /set_sales @vasya reno11f5g 128 2 Mechta
    parts = (message.text or "").split()
    u = extract_mentioned_username(message.text)
    if not u:
        await message.reply("Формат: /set_sales @user model memory qty [network]")
        return
    try:
        # убираем имя и команду
        rest = [p for p in parts if not p.startswith("/") and not p.startswith("@")]
        if len(rest) < 3:
            await message.reply("Формат: /set_sales @user model memory qty [network]")
            return
        model = re.sub(r"\s+", "", rest[0]).lower()
        memory = str(int(rest[1]))
        qty = int(rest[2])
        net = rest[3] if len(rest) >= 4 else db.get_network(u) or "-"
        db.add_sale(u, model, memory, qty, net)
        await message.reply(f"✅ Добавлено: @{u} {model} {memory} x{qty} (сеть: {human_network(net)})")
    except Exception as e:
        await message.reply(f"⚠️ Ошибка: {e}")

@router.message(Command("set_plan"))
async def cmd_set_plan(message: Message):
    if not await admin_guard(message):
        return
    # /set_plan @user|all PLAN [YYYY-MM]
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.reply("Формат: /set_plan @user|all PLAN [YYYY-MM]")
        return
    tgt_user = extract_mentioned_username(message.text)
    target_all = (parts[1].lower() == "all")
    plan_val = None
    month_s = None
    # вычленяем число и опциональный месяц
    nums = [p for p in parts if re.fullmatch(r"\d+", p)]
    ym = [p for p in parts if re.fullmatch(r"\d{4}-\d{2}", p) or re.fullmatch(r"\d{6}", p)]
    if not nums:
        await message.reply("Укажи план, например 120.")
        return
    plan_val = int(nums[0])
    month_s = ym[0] if ym else None
    y, m = parse_yyyymm(month_s)
    ym_key = f"{y:04d}-{m:02d}"
    if target_all:
        users = db.get_all_known_users()
        for u in users:
            db.set_plan(u, ym_key, plan_val)
        await message.reply(f"✅ План {plan_val} проставлен всем на {ym_key}.")
        return
    if not tgt_user:
        await message.reply("Укажи @user или all. Пример: /set_plan @vasya 120 2025-08")
        return
    db.set_plan(tgt_user, ym_key, plan_val)
    await message.reply(f"✅ План @{tgt_user}: {plan_val} на {ym_key}")

@router.message(Command("plan_show"))
async def cmd_plan_show(message: Message):
    # /plan_show [YYYY-MM]
    parts = (message.text or "").split()
    ym = None
    for p in parts[1:]:
        if re.fullmatch(r"\d{4}-\d{2}", p) or re.fullmatch(r"\d{6}", p):
            ym = p
            break
    y, m = parse_yyyymm(ym)
    ym_key = f"{y:04d}-{m:02d}"
    plans = db.get_all_plans(ym_key)
    if not plans:
        await message.reply(f"Планы на {ym_key} не заданы.")
        return
    lines = [bold(f"📋 Планы на {ym_key}")]
    for u, p in sorted(plans.items()):
        total, _ = db.month_sales(y, m, username=u)
        # Отображаем сеть, если есть
        display_user = db.get_network(u)
        display = display_user if display_user and display_user != "-" else f"@{u}"
        lines.append(f"{display}: план {p} | факт {total} | {pct(total, p)}")
    await message.reply("\n".join(lines))

# ============================
# --- Отчёты и напоминания ---
# ============================
async def daily_report():
    d = now_ala()
    y, m = d.year, d.month
    days_in_month = calendar.monthrange(y, m)[1]
    day = d.day
    ym_key = f"{y:04d}-{m:02d}"

    users = db.get_all_known_users()
    if not users:
        return

    lines = [bold(f"🗓️ Ежедневный отчёт {ym_key} (на {d.strftime('%d.%m %H:%M')})")]
    for u in sorted(users):
        fact, _ = db.month_sales(y, m, username=u)
        plan = db.get_plan(u, ym_key) or 0
        pr = pct(fact, plan) if plan else "—"
        # проекция
        pace = (fact / day) if day > 0 else 0
        proj = round(pace * days_in_month)
        # Показываем сеть вместо ID/ника, если есть
        display = db.get_network(u)
        display_name = display if display and display != "-" else f"@{u}"
        lines.append(f"{display_name}: факт {fact} / план {plan} ({pr}), проекция {proj}")
    await bot.send_message(GROUP_CHAT_ID, "\n".join(lines))

async def weekly_projection():
    d = now_ala()
    y, m = d.year, d.month
    days_in_month = calendar.monthrange(y, m)[1]
    day = d.day
    ym_key = f"{y:04d}-{m:02d}"

    users = db.get_all_known_users()
    if not users:
        return

    lines = [bold(f"📈 Недельная проекция {ym_key}")]
    for u in sorted(users):
        fact, _ = db.month_sales(y, m, username=u)
        plan = db.get_plan(u, ym_key) or 0
        pace = (fact / day) if day > 0 else 0
        proj = round(pace * days_in_month)
        display = db.get_network(u)
        display_name = display if display and display != "-" else f"@{u}"
        lines.append(f"{display_name}: факт {fact}, план {plan}, проекция {proj}")
    await bot.send_message(GROUP_CHAT_ID, "\n".join(lines))

async def weekly_stock_reminder():
    users = db.get_all_known_users()
    if not users:
        return
    mentions = " ".join(f"@{u}" for u in users)
    txt = bold("📦 Напоминание о стоках") + "\n" + \
          "Обновите остатки (модель память — количество). " + mentions
    await bot.send_message(GROUP_CHAT_ID, txt)

async def monthly_report():
    # Финал месяца: сводка по всем
    d = now_ala()
    y, m = d.year, d.month
    ym_key = f"{y:04d}-{m:02d}"
    users = db.get_all_known_users()
    if not users:
        return

    lines = [bold(f"🏁 Итоговый отчёт за {ym_key}")]
    total_all = 0
    plan_all = 0
    for u in sorted(users):
        fact, _ = db.month_sales(y, m, username=u)
        plan = db.get_plan(u, ym_key) or 0
        total_all += fact
        plan_all += plan
        display = db.get_network(u)
        display_name = display if display and display != "-" else f"@{u}"
        lines.append(f"{display_name}: {fact} / {plan} ({pct(fact, plan)})")
    lines.append(bold(f"ИТОГО: {total_all} / {plan_all} ({pct(total_all, plan_all)})"))
    await bot.send_message(GROUP_CHAT_ID, "\n".join(lines))

async def inactive_promoters_reminder():
    # Пинать тех, у кого нет продаж последние 2 дня
    cutoff = now_ala() - timedelta(days=2)
    users = db.get_all_known_users()
    lazy = []
    for u in users:
        last = get_last_sale_dt(u)
        if last is None or last < cutoff:
            lazy.append(u)
    if not lazy:
        return
    txt = bold("🔔 Напоминание") + "\n" + \
          "Нет продаж за последние 48 часов: " + ", ".join(f"@{u}" for u in lazy)
    await bot.send_message(GROUP_CHAT_ID, txt)

# =========
#   MAIN
# =========
async def main():
    # Гарантируем, что БД подготовлена
    db.init()

    # Вебхук
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)

    # Планировщик
    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_report, "cron", hour=21, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_projection, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_stock_reminder, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(monthly_report, "cron", day="last", hour=20, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(inactive_promoters_reminder, "cron", hour=20, minute=30, timezone="Asia/Almaty")
    # Автосброс продаж в начале месяца (как у тебя было)
    scheduler.add_job(db.reset_monthly_sales, "cron", day=1, hour=0, minute=5, timezone="Asia/Almaty")
    scheduler.start()

    # AIOHTTP
    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")

    # health-check рут
    async def health(request):
        return web.Response(text="OK")
    app.add_routes([web.get("/", health)])

    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT); await site.start()

    print(f"🚀 Webhook бот запущен на {WEBHOOK_URL}")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
