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

from aiohttp import web   # HTTP-сервер для Render

import db

# --- Настройки ---
TOKEN = os.getenv("TOKEN")  # секретный токен в Render → Environment
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-1000000000000"))
tz = timezone("Asia/Almaty")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# --- Продажи ---
@router.message()
async def sales_handler(message: Message):
    if message.chat.id != GROUP_CHAT_ID:
        return
    text = message.text.strip().lower()
    if "доля" in text:
        return
    try:
        pattern = re.compile(r"([a-zа-я]+[\s]?\d+\w*)\s*(\d{2,4}(?:tb|тб)?)(?:\s*[-x]?\s*(\d+))?", re.IGNORECASE)
        matches = pattern.findall(text)
        if not matches:
            return
        for match in matches:
            model = match[0].replace(" ", "")
            memory = match[1]
            qty = int(match[2]) if match[2] else 1
            user = message.from_user.username or str(message.from_user.id)
            db.add_sale(user, model, memory, qty)
        await message.reply("✅ Продажи учтены")
    except Exception as e:
        await message.reply(f"⚠ Ошибка: {e}")

# --- Стоки ---
@router.message()
async def stock_handler(message: Message):
    if message.chat.id != GROUP_CHAT_ID:
        return
    text = message.text.strip().lower()
    if any(word in text for word in ["приход", "приехал", "остаток", "сток"]):
        try:
            parts = message.text.split()
            if len(parts) < 2:
                return
            item, qty = parts[0], int(parts[1])
            user = message.from_user.username or str(message.from_user.id)
            db.update_stock(user, item, qty)
            await message.reply(f"📦 Сток обновлён: {item} = {qty}")
        except Exception as e:
            await message.reply(f"⚠ Ошибка стока: {e}")

# --- Фото ---
@router.message()
async def photo_handler(message: Message):
    if message.chat.id != GROUP_CHAT_ID:
        return
    if message.photo:
        user = message.from_user.username or str(message.from_user.id)
        db.add_photo(user)
        await message.reply("📸 Фото учтено")

# --- Команды ---
@router.message()
async def cmd_plan(message: Message):
    if message.text.startswith("/plan"):
        parts = message.text.split()
        if len(parts) != 3:
            await message.reply("Использование: /plan @username число")
            return
        username = parts[1].lstrip("@")
        plan = int(parts[2])
        db.set_plan(username, plan)
        await message.reply(f"✅ План для @{username} установлен: {plan}")

@router.message()
async def cmd_sales_month(message: Message):
    if message.text.startswith("/sales_month"):
        data = db.get_sales_month()
        if not data:
            await message.reply("📊 Продажи с начала месяца: пока нет данных.")
            return
        report = "📊 Продажи с начала месяца:\n"
        for username, qty, plan in data:
            percent = int(qty / plan * 100) if plan else 0
            report += f"@{username}: {qty} / план {plan or '-'} → {percent}%\n"
        await message.reply(report)

# --- Автоотчёты ---
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

# --- HTTP-сервер для Render ---
async def handle(request):
    return web.Response(text="Bot is running!")

async def start_web():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 5000)))
    await site.start()

# --- Цикл работы бота (08:00–24:00) ---
async def run_bot_limited():
    while True:
        now = datetime.now(tz).time()
        if 8 <= now.hour < 24:  # активность
            print("⏰ Время работы (08:00–24:00) — бот запущен ✅")
            try:
                await dp.start_polling(bot)
            except Exception as e:
                print(f"⚠ Ошибка polling: {e}")
                await asyncio.sleep(30)
        else:
            print("😴 Ночь — бот отдыхает до 08:00")
            await asyncio.sleep(300)  # проверяем каждые 5 мин

# --- Главная ---
async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_report, "cron", hour=21, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_projection, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_stock_reminder, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(monthly_report, "cron", day="last", hour=20, minute=0, timezone="Asia/Almaty")
    scheduler.start()

    # HTTP-сервер для Render
    await start_web()

    # Ограничение по времени
    await run_bot_limited()

if __name__ == "__main__":
    asyncio.run(main())
