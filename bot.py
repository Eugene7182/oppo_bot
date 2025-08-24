import asyncio
import calendar
import logging
import os
import re
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from aiohttp import web

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

import db

# --------------------------
# НАСТРОЙКИ
# --------------------------
TOKEN = os.getenv("BOT_TOKEN", "")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-1002663663535"))
ADMINS_ENV = os.getenv("ADMINS", "").split(",") if os.getenv("ADMINS") else []
tz = timezone("Asia/Almaty")

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "https://oppo-bot-k2d2.onrender.com")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

logging.basicConfig(level=logging.INFO)

# --------------------------
# СЕТИ
# --------------------------
NETWORKS = ["mechta", "beeline", "sulpak", "sulpka", "td"]

def extract_network(text: str) -> str:
    for net in NETWORKS:
        if net in text.lower():
            return net.capitalize()
    return "-"

# --------------------------
# АДМИНЫ
# --------------------------
def is_admin(username: str) -> bool:
    if not username:
        return False
    if username in ADMINS_ENV:
        return True
    return db.is_admin(username)

# --------------------------
# РЕГУЛЯРКИ
# --------------------------
SALE_RE = re.compile(r"([a-zа-яё0-9\-\+]+)\s+(\d{2,4}(?:tb|тб)?)\s*[-—: ]?\s*(\d+)?", re.IGNORECASE)
STOCK_RE = re.compile(
    r"([a-zа-яё0-9\+\-\s]+?)\s*(?:\(?\d+\/)?(\d{2,4})(?:тб|tb)?\)?\s*[-—: ]?\s*(\d+)?",
    re.IGNORECASE
)

# --------------------------
# ПРОДАЖИ
# --------------------------
@router.message()
async def sales_handler(message: Message):
    if message.chat.id != GROUP_CHAT_ID or not message.text or message.text.startswith("/"):
        return
    if "доля" in message.text.lower():
        return

    text = message.text.strip()
    network = extract_network(text)

    try:
        matches = SALE_RE.findall(text)
        if not matches:
            return
        user = message.from_user.username or str(message.from_user.id)

        for model_raw, memory, qty_raw in matches:
            model = model_raw.strip()
            qty = int(qty_raw) if qty_raw else 1
            item = f"{model} {memory}"

            # 1) Запись продажи
            db.add_sale(user, model, memory, qty, network)

            # 2) Проверка стока
            stock_qty = db.get_stock_qty(user, item, network)
            if stock_qty is None:
                await message.reply(f"⚠ Остаток для {item} не найден. @{user}, обновите сток!")
            elif stock_qty < qty:
                await message.reply(
                    f"⚠ У @{user} не хватает стока для {item} (продажа {qty}, остаток {stock_qty}).\n"
                    f"👉 Обновите остатки!"
                )
            else:
                db.decrease_stock(user, item, qty, network)

        await message.reply(f"✅ Продажи учтены (сеть: {network})")
    except Exception as e:
        await message.reply(f"⚠ Ошибка: {e}")

# --------------------------
# СТОКИ
# --------------------------
@router.message()
async def stock_handler(message: Message):
    if message.chat.id != GROUP_CHAT_ID or not message.text:
        return
    text = message.text.strip()
    if not any(w in text.lower() for w in ["сток", "остаток", "stock", "stocks", "приход", "приехал"]):
        return

    try:
        rows = text.splitlines()
        user = message.from_user.username or str(message.from_user.id)
        network = extract_network(text)

        updated_items = []
        for row in rows:
            match = STOCK_RE.search(row)
            if not match:
                continue
            model = match.group(1).strip().replace("  ", " ")
            memory = match.group(2)
            qty = int(match.group(3)) if match.group(3) else 0
            item_name = f"{model} {memory}"
            db.update_stock(user, item_name, qty, network)
            updated_items.append(f"{item_name} = {qty}")

        if updated_items:
            await message.reply("📦 Обновлено:\n" + "\n".join(updated_items) + f"\n(сеть: {network})")
        else:
            await message.reply("⚠ Не удалось распознать позиции.")
    except Exception as e:
        await message.reply(f"⚠ Ошибка стока: {e}")

# --------------------------
# АДМИН-КОМАНДЫ
# --------------------------
@router.message(F.text.startswith("/admins"))
async def cmd_admins(message: Message):
    if not is_admin(message.from_user.username): return
    admins_env = ", ".join(ADMINS_ENV) if ADMINS_ENV else "-"
    admins_db = ", ".join(db.get_admins()) or "-"
    await message.reply(f"👑 ENV: {admins_env}\n👤 DB: {admins_db}")

@router.message(F.text.startswith("/add_admin"))
async def cmd_add_admin(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 2: 
        await message.reply("Использование: /add_admin @username")
        return
    db.add_admin(parts[1].lstrip("@"))
    await message.reply(f"✅ {parts[1]} добавлен")

@router.message(F.text.startswith("/del_admin"))
async def cmd_del_admin(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 2: 
        await message.reply("Использование: /del_admin @username")
        return
    db.del_admin(parts[1].lstrip("@"))
    await message.reply(f"❌ {parts[1]} удалён")

@router.message(F.text.startswith("/plan"))
async def cmd_plan(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 3:
        await message.reply("Использование: /plan @username число")
        return
    db.set_plan(parts[1].lstrip("@"), int(parts[2]))
    await message.reply(f"✅ План для {parts[1]} = {parts[2]}")

@router.message(F.text.startswith("/sales_month"))
async def cmd_sales_month(message: Message):
    if not is_admin(message.from_user.username): return
    data = db.get_sales_month()
    if not data: 
        await message.reply("📊 Нет данных.")
        return
    txt = "📊 Продажи месяца:\n"
    for username, qty, plan, net in data:
        percent = int(qty / plan * 100) if plan else 0
        txt += f"@{username} ({net}): {qty}/{plan or '-'} ({percent}%)\n"
    await message.reply(txt)

# --------------------------
# ОТЧЁТЫ (scheduler)
# --------------------------
async def daily_report():
    today = datetime.now(tz).strftime("%Y-%m-%d")
    sales = db.get_sales_all(today)
    txt = f"📊 Продажи за {today}:\n" if sales else f"📊 {today}: нет данных."
    if sales:
        for u, qty, plan, net in sales:
            percent = int(qty / plan * 100) if plan else 0
            txt += f"@{u} ({net}): {qty}/{plan or '-'} ({percent}%)\n"
    await bot.send_message(GROUP_CHAT_ID, txt)

async def weekly_projection():
    today = datetime.now(tz)
    day = today.day
    total_days = calendar.monthrange(today.year, today.month)[1]
    data = db.get_sales_month()
    if not data: return
    txt = "🔮 Прогноз:\n"
    for u, qty, plan, net in data:
        if not plan: continue
        forecast = int((qty / day) * total_days)
        percent = int(forecast / plan * 100)
        txt += f"@{u} ({net}): {qty}/{plan}, прогноз {forecast} ({percent}%)\n"
    await bot.send_message(GROUP_CHAT_ID, txt)

async def monthly_report():
    today = datetime.now(tz)
    data = db.get_sales_month()
    txt = f"📊 Итог {today.strftime('%B %Y')}:\n"
    for u, qty, plan, net in data:
        percent = int(qty / plan * 100) if plan else 0
        txt += f"@{u} ({net}): {qty}/{plan or '-'} ({percent}%)\n"
    await bot.send_message(GROUP_CHAT_ID, txt)
    db.reset_monthly_sales()

async def weekly_stock_reminder():
    await bot.send_message(GROUP_CHAT_ID, "📦 Обновите стоки!")

# --------------------------
# MAIN
# --------------------------
async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_report, "cron", hour=21, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_projection, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_stock_reminder, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(monthly_report, "cron", day="last", hour=20, minute=0, timezone="Asia/Almaty")
    scheduler.start()

    # --- Сброс старого вебхука и установка нового ---
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)

    # aiohttp web server
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, dp.webhook_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 10000)))
    await site.start()

    print(f"🚀 Webhook бот запущен на {WEBHOOK_URL}")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
