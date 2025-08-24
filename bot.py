import os
import re
import asyncio
import calendar
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone

from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from aiohttp import web  # HTTP для Render

import db

# --- Настройки ---
TOKEN = os.getenv("TOKEN")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-1000000000000"))
tz = timezone("Asia/Almaty")

# Сети (просто метки в тексте)
RAW_NETWORKS = {"mechta", "beeline", "sulpak", "sulpka", "td"}
def normalize_network(tok: str | None) -> str | None:
    if not tok:
        return None
    t = tok.lower()
    if t not in RAW_NETWORKS:
        return None
    return "Sulpak" if t == "sulpka" else t.capitalize()

def extract_network(text: str) -> str | None:
    words = re.findall(r"[A-Za-zА-Яа-я]+", text)
    for w in words:
        nw = normalize_network(w)
        if nw:
            return nw
    return None

def strip_network_from_text(text: str) -> str:
    for raw in RAW_NETWORKS:
        text = re.sub(rf"\b{raw}\b", "", text, flags=re.IGNORECASE)
    return " ".join(text.split())

# Админы
ENV_ADMINS = {u.strip().lower() for u in os.getenv("ADMINS", "").split(",") if u.strip()}
def is_admin_user(message: Message) -> bool:
    u = (message.from_user.username or str(message.from_user.id)).lower()
    return (u in ENV_ADMINS) or db.is_admin(u)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# --------------------------
# ХЕНДЛЕРЫ ПРОДАЖ И СТОКОВ
# --------------------------
SALE_RE = re.compile(r"([a-zа-я]+[\s]?\d+\w*)\s*(\d{2,4}(?:tb|тб)?)(?:\s*[-x]?\s*(\d+))?", re.IGNORECASE)

@router.message()
async def sales_handler(message: Message):
    if message.chat.id != GROUP_CHAT_ID or not message.text or message.text.startswith("/"):
        return
    text = message.text.strip()
    if "доля" in text.lower():
        return

    network = extract_network(text)
    clean = strip_network_from_text(text)

    try:
        matches = SALE_RE.findall(clean)
        if not matches:
            return
        user = message.from_user.username or str(message.from_user.id)
        if not network:
            network = db.get_user_network(user)
        for model_raw, memory, qty_raw in matches:
            model = model_raw.replace(" ", "")
            qty = int(qty_raw) if qty_raw else 1
            db.add_sale(user, model, memory, qty, network)
        tag = f" (сеть: {network})" if network else ""
        await message.reply(f"✅ Продажи учтены{tag}")
    except Exception as e:
        await message.reply(f"⚠ Ошибка: {e}")

@router.message()
async def stock_handler(message: Message):
    if message.chat.id != GROUP_CHAT_ID or not message.text or message.text.startswith("/"):
        return
    low = message.text.lower()
    if not any(w in low for w in ["приход", "приехал", "остаток", "сток"]):
        return
    try:
        network = extract_network(message.text)
        clean = strip_network_from_text(message.text)
        parts = clean.split()
        if len(parts) < 2:
            return
        item, qty = parts[0], int(parts[1])
        user = message.from_user.username or str(message.from_user.id)
        if not network:
            network = db.get_user_network(user)
        db.update_stock(user, item, qty, network)
        tag = f" (сеть: {network})" if network else ""
        await message.reply(f"📦 Сток обновлён: {item} = {qty}{tag}")
    except Exception as e:
        await message.reply(f"⚠ Ошибка стока: {e}")

# --------------------------
# КОМАНДЫ
# --------------------------
@router.message()
async def cmd_plan(message: Message):
    if not (message.text and message.text.startswith("/plan")):
        return
    if not is_admin_user(message):
        await message.reply("⛔ У вас нет прав на эту команду.")
        return
    parts = message.text.split()
    if len(parts) != 3:
        await message.reply("Использование: /plan @username число")
        return
    username = parts[1].lstrip("@")
    plan = int(parts[2])
    db.set_plan(username, plan)
    who = message.from_user.username or str(message.from_user.id)
    db.log_admin_action(who, f"set_plan {username} -> {plan}")
    await message.reply(f"✅ План для @{username} установлен: {plan} (назначил @{who})")

@router.message()
async def cmd_sales_month(message: Message):
    if not (message.text and message.text.startswith("/sales_month")):
        return
    data = db.get_sales_month()
    if not data:
        await message.reply("📊 Продажи с начала месяца: пока нет данных.")
        return
    report = "📊 Продажи с начала месяца:\n"
    for username, qty, plan in data:
        percent = int(qty / plan * 100) if plan else 0
        report += f"@{username}: {qty} / план {plan or '-'} → {percent}%\n"
    await message.reply(report)

@router.message()
async def cmd_stocks(message: Message):
    if not (message.text and message.text.startswith("/stocks")):
        return
    args = message.text.split()[1:]
    username = None
    network = None
    for a in args:
        if a.startswith("@"):
            username = a.lstrip("@")
        else:
            n = normalize_network(a)
            if n:
                network = n
    rows = db.get_stocks_filtered(username=username, network=network)
    if not rows:
        await message.reply("📦 Пока нет данных по остаткам.")
        return
    title = "📦 Остатки"
    if username: title += f" по @{username}"
    if network: title += f" | сеть: {network}"
    report = title + ":\n"
    for u, item, qty, lu, net in rows:
        tag = f" | {net}" if net else ""
        report += f"@{u}: {item} = {qty} (обновлено {lu}){tag}\n"
    await message.reply(report)

# --- Ручные отчёты ---
@router.message()
async def cmd_daily(message: Message):
    if message.text and message.text.startswith("/daily_report"):
        if not is_admin_user(message):
            await message.reply("⛔ У вас нет прав на эту команду.")
            return
        await daily_report()

@router.message()
async def cmd_weekly(message: Message):
    if message.text and message.text.startswith("/weekly_projection"):
        if not is_admin_user(message):
            await message.reply("⛔ У вас нет прав на эту команду.")
            return
        await weekly_projection()

@router.message()
async def cmd_monthly(message: Message):
    if message.text and message.text.startswith("/monthly_report"):
        if not is_admin_user(message):
            await message.reply("⛔ У вас нет прав на эту команду.")
            return
        await monthly_report()

@router.message()
async def cmd_top(message: Message):
    if not (message.text and message.text.startswith("/top_sellers")):
        return
    parts = message.text.split()
    limit = 3
    if len(parts) == 2 and parts[1].isdigit():
        limit = max(1, min(20, int(parts[1])))
    rows = db.get_top_sellers(limit=limit)
    if not rows:
        await message.reply("🏆 Пока нет продаж за месяц.")
        return
    txt = "🏆 Топ продавцов (месяц):\n"
    for i, (u, qty) in enumerate(rows, start=1):
        txt += f"{i}) @{u} — {qty}\n"
    await message.reply(txt)

@router.message()
async def cmd_by_network(message: Message):
    if not (message.text and message.text.startswith("/by_network")):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Использование: /by_network <Mechta|Beeline|Sulpak|TD>")
        return
    net = normalize_network(parts[1])
    if not net:
        await message.reply("Неизвестная сеть. Используй: Mechta, Beeline, Sulpak, TD")
        return
    rows = db.get_sales_month_by_network(net)
    if not rows:
        await message.reply(f"📊 По сети {net} пока нет данных.")
        return
    txt = f"📊 Продажи за месяц (сеть {net}):\n"
    for u, qty, plan in rows:
        pct = int(qty / plan * 100) if plan else 0
        txt += f"@{u}: {qty} / план {plan or '-'} → {pct}%\n"
    await message.reply(txt)

# --------------------------
# ОТЧЁТЫ (scheduler)
# --------------------------
async def daily_report():
    today = datetime.now(tz).strftime("%Y-%m-%d")
    sales = db.get_sales_all(today)
    if not sales:
        report = f"📊 Продажи за {today}: пока нет данных."
    else:
        report = f"📊 Продажи за {today}:\n"
        for username, qty, plan in sales:
            percent = int(qty / plan * 100) if plan else 0
            report += f"@{username}: {qty} / план {plan or '-'} → {percent}%\n"
    await bot.send_message(GROUP_CHAT_ID, report)

async def weekly_projection():
    today = datetime.now(tz)
    day_of_month = today.day
    total_days = calendar.monthrange(today.year, today.month)[1]
    data = db.get_sales_month()
    if not data:
        return
    report = "🔮 Прогноз выполнения плана:\n"
    for username, qty, plan in data:
        if not plan or plan == 0:
            continue
        avg = qty / day_of_month
        forecast = int(avg * total_days)
        percent = int(forecast / plan * 100)
        report += f"@{username}: сейчас {qty}/{plan}, прогноз {forecast} ({percent}%)\n"
    await bot.send_message(GROUP_CHAT_ID, report)

async def monthly_report():
    today = datetime.now(tz)
    data = db.get_sales_month()
    report = f"📊 Итог продаж за {today.strftime('%B %Y')}:\n"
    for username, qty, plan in data:
        percent = int(qty / plan * 100) if plan else 0
        report += f"@{username}: {qty} / план {plan or '-'} → {percent}%\n"
    await bot.send_message(GROUP_CHAT_ID, report)
    db.reset_monthly_sales()

async def weekly_stock_reminder():
    await bot.send_message(GROUP_CHAT_ID, "📦 Напомните актуальные остатки, пожалуйста.")

async def weekly_top_broadcast():
    rows = db.get_top_sellers(limit=5)
    if not rows:
        return
    txt = "🏆 Еженедельный топ продавцов:\n"
    for i, (u, qty) in enumerate(rows, start=1):
        txt += f"{i}) @{u} — {qty}\n"
    await bot.send_message(GROUP_CHAT_ID, txt)

# --------------------------
# HTTP-сервер для Render
# --------------------------
async def handle_root(request):
    return web.Response(text="Bot is running 24/7!")

async def start_web():
    app = web.Application()
    app.router.add_get("/", handle_root)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 5000)))
    await site.start()

# --------------------------
# MAIN
# --------------------------
async def main():
    if ENV_ADMINS:
        db.seed_admins_from_env(list(ENV_ADMINS))

    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_report, "cron", hour=21, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_projection, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_stock_reminder, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(monthly_report, "cron", day="last", hour=20, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_top_broadcast, "cron", day_of_week="mon", hour=10, minute=0, timezone="Asia/Almaty")
    scheduler.start()

    await start_web()

    print("🚀 Бот запущен 24/7 ✅")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
