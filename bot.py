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
TOKEN = os.getenv("TOKEN")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-1000000000000"))
tz = timezone("Asia/Almaty")

# Список сетей
NETWORKS = ["mechta", "beeline", "sulpak", "td"]

# Админы (через переменные окружения в Render)
ADMINS = {u.strip().lower() for u in os.getenv("ADMINS", "").split(",") if u.strip()}

def is_admin(message: Message) -> bool:
    if not ADMINS:
        return True
    u = (message.from_user.username or str(message.from_user.id)).lower()
    return u in ADMINS

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# --- Продажи ---
@router.message()
async def sales_handler(message: Message):
    if message.chat.id != GROUP_CHAT_ID or not message.text or message.text.startswith("/"):
        return
    text = message.text.strip().lower()
    if "доля" in text:
        return
    try:
        # Поиск сети
        network = None
        for net in NETWORKS:
            if net in text:
                network = net.capitalize()
                text = text.replace(net, "")
                break

        # Поиск модели, памяти, qty
        pattern = re.compile(r"([a-zа-я]+[\s]?\d+\w*)\s*(\d{2,4}(?:tb|тб)?)(?:\s*[-x]?\s*(\d+))?", re.IGNORECASE)
        matches = pattern.findall(text)
        if not matches:
            return
        for match in matches:
            model = match[0].replace(" ", "")
            memory = match[1]
            qty = int(match[2]) if match[2] else 1
            user = message.from_user.username or str(message.from_user.id)
            db.add_sale(user, model, memory, qty, network)
        net_note = f" (сеть: {network})" if network else ""
        await message.reply(f"✅ Продажи учтены{net_note}")
    except Exception as e:
        await message.reply(f"⚠ Ошибка: {e}")

# --- Стоки ---
@router.message()
async def stock_handler(message: Message):
    if message.chat.id != GROUP_CHAT_ID or not message.text or message.text.startswith("/"):
        return
    text = message.text.strip().lower()
    if any(word in text for word in ["приход", "приехал", "остаток", "сток"]):
        try:
            network = None
            for net in NETWORKS:
                if net in text:
                    network = net.capitalize()
                    text = text.replace(net, "")
                    break
            parts = text.split()
            if len(parts) < 2:
                return
            item, qty = parts[0], int(parts[1])
            user = message.from_user.username or str(message.from_user.id)
            db.update_stock(user, item, qty, network)
            net_note = f" (сеть: {network})" if network else ""
            await message.reply(f"📦 Сток обновлён: {item} = {qty}{net_note}")
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

# =========================
# КОМАНДЫ (с админ-проверкой)
# =========================

# /plan @username 100
@router.message()
async def cmd_plan(message: Message):
    if not message.text.startswith("/plan"):
        return
    if not is_admin(message):
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
    await message.reply(f"✅ План для @{username} установлен: {plan} (назначил @{who})")

# /sales_month
@router.message()
async def cmd_sales_month(message: Message):
    if not message.text.startswith("/sales_month"):
        return
    data = db.get_sales_month()
    if not data:
        await message.reply("📊 Продажи с начала месяца: пока нет данных.")
        return
    report = "📊 Продажи с начала месяца:\n"
    for username, qty, plan, network in data:
        percent = int(qty / plan * 100) if plan else 0
        net_note = f" ({network})" if network else ""
        report += f"@{username}: {qty} / план {plan or '-'} → {percent}%{net_note}\n"
    await message.reply(report)

# /stocks
@router.message()
async def cmd_stocks(message: Message):
    if not message.text.startswith("/stocks"):
        return
    data = db.get_all_stocks()
    if not data:
        await message.reply("📦 Пока нет данных по остаткам.")
        return
    report = "📦 Актуальные остатки:\n"
    for username, item, qty, last_update, network in data:
        net_note = f" ({network})" if network else ""
        report += f"@{username}: {item} = {qty} (обновлено {last_update}){net_note}\n"
    await message.reply(report)

# /daily_report
@router.message()
async def cmd_daily(message: Message):
    if not message.text.startswith("/daily_report"):
        return
    if not is_admin(message):
        await message.reply("⛔ У вас нет прав на эту команду.")
        return
    await daily_report()

# /weekly_projection
@router.message()
async def cmd_weekly(message: Message):
    if not message.text.startswith("/weekly_projection"):
        return
    if not is_admin(message):
        await message.reply("⛔ У вас нет прав на эту команду.")
        return
    await weekly_projection()

# /monthly_report
@router.message()
async def cmd_monthly(message: Message):
    if not message.text.startswith("/monthly_report"):
        return
    if not is_admin(message):
        await message.reply("⛔ У вас нет прав на эту команду.")
        return
    await monthly_report()

# /top_sellers
@router.message()
async def cmd_top(message: Message):
    if not message.text.startswith("/top_sellers"):
        return
    data = db.get_top_sellers()
    if not data:
        await message.reply("📊 Пока нет данных по продажам.")
        return
    report = "🏆 ТОП продавцов:\n"
    for username, qty, network in data:
        net_note = f" ({network})" if network else ""
        report += f"@{username}: {qty}{net_note}\n"
    await message.reply(report)

# /under_plan
@router.message()
async def cmd_under(message: Message):
    if not message.text.startswith("/under_plan"):
        return
    data = db.get_under_plan()
    if not data:
        await message.reply("Все выполняют план 👍")
        return
    report = "⚠ Отстающие:\n"
    for username, qty, plan, network in data:
        percent = int(qty / plan * 100) if plan else 0
        net_note = f" ({network})" if network else ""
        report += f"@{username}: {qty}/{plan} → {percent}%{net_note}\n"
    await message.reply(report)

# /by_store Sulpak
@router.message()
async def cmd_by_store(message: Message):
    if not message.text.startswith("/by_store"):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Использование: /by_store <сеть>")
        return
    network = parts[1].capitalize()
    data = db.get_sales_by_network(network)
    if not data:
        await message.reply(f"Нет данных по сети {network}.")
        return
    report = f"📊 Продажи по сети {network}:\n"
    for username, qty, plan in data:
        percent = int(qty / plan * 100) if plan else 0
        report += f"@{username}: {qty}/{plan or '-'} → {percent}%\n"
    await message.reply(report)

# --- Автоотчёты ---
async def daily_report():
    today = datetime.now(tz).strftime("%Y-%m-%d")
    sales = db.get_sales_all(today)
    if not sales:
        report = f"📊 Продажи за {today}: пока нет данных."
    else:
        report = f"📊 Продажи за {today}:\n"
        for username, qty, plan, network in sales:
            percent = int(qty / plan * 100) if plan else 0
            net_note = f" ({network})" if network else ""
            report += f"@{username}: {qty} / план {plan or '-'} → {percent}%{net_note}\n"
    await bot.send_message(GROUP_CHAT_ID, report)

async def weekly_projection():
    today = datetime.now(tz)
    day_of_month = today.day
    total_days = calendar.monthrange(today.year, today.month)[1]
    data = db.get_sales_month()
    if not data:
        return
    report = "🔮 Прогноз выполнения плана:\n"
    for username, qty, plan, network in data:
        if not plan:
            continue
        avg = qty / day_of_month
        forecast = int(avg * total_days)
        percent = int(forecast / plan * 100)
        net_note = f" ({network})" if network else ""
        report += f"@{username}: {qty}/{plan}, прогноз {forecast} ({percent}%){net_note}\n"
    await bot.send_message(GROUP_CHAT_ID, report)

async def monthly_report():
    today = datetime.now(tz)
    data = db.get_sales_month()
    report = f"📊 Итог продаж за {today.strftime('%B %Y')}:\n"
    for username, qty, plan, network in data:
        percent = int(qty / plan * 100) if plan else 0
        net_note = f" ({network})" if network else ""
        report += f"@{username}: {qty}/{plan or '-'} → {percent}%{net_note}\n"
    await bot.send_message(GROUP_CHAT_ID, report)
    db.reset_monthly_sales()

async def weekly_stock_reminder():
    await bot.send_message(GROUP_CHAT_ID, "📦 Напомните актуальные остатки, пожалуйста.")

# --- HTTP-сервер для Render ---
async def handle(request):
    return web.Response(text="Bot is running 24/7!")

async def start_web():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 5000)))
    await site.start()

# --- Главная ---
async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_report, "cron", hour=21, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_projection, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_stock_reminder, "cron", day_of_week="sun", hour=12, minute=5, timezone="Asia/Almaty")
    scheduler.add_job(monthly_report, "cron", day="last", hour=20, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(cmd_top, "cron", day_of_week="mon", hour=10, minute=0, timezone="Asia/Almaty")
    scheduler.start()

    await start_web()

    print("🚀 Бот запущен 24/7 ✅")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
