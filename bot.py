import asyncio
import calendar
import re
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

import db
import os

# --- Настройки ---
TOKEN = os.getenv("BOT_TOKEN", "ТОКЕН_ТУТ")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-1002663663535"))
ADMINS_ENV = os.getenv("ADMINS", "").split(",") if os.getenv("ADMINS") else []
tz = timezone("Asia/Almaty")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# --- Сети ---
NETWORKS = ["mechta", "beeline", "sulpak", "sulpka", "td"]

# --- Хелперы ---
def is_admin(username: str) -> bool:
    if not username:
        return False
    if username in ADMINS_ENV:
        return True
    return db.is_admin(username)

def extract_network(text: str) -> str:
    for net in NETWORKS:
        if net in text.lower():
            return net.capitalize()
    return "-"

# --- Регулярки ---
SALE_RE = re.compile(r"([a-zа-яё0-9\-\+]+)\s+(\d{2,4}(?:tb|тб)?)\s*(\d+)?", re.IGNORECASE)
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
    text = message.text.strip()
    if "доля" in text.lower():
        return

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

            # 1) Сохраняем продажу
            db.add_sale(user, model, memory, qty, network)

            # 2) Проверяем остатки
            stock_qty = db.get_stock_qty(user, item, network)
            if stock_qty is None:
                await message.reply(f"⚠ Остаток для {item} не найден. @{user}, обновите сток!")
            elif stock_qty < qty:
                await message.reply(
                    f"⚠ У @{user} не хватает стока для {item} (продажа {qty}, остаток {stock_qty}).\n"
                    f"👉 Пожалуйста, обновите остатки!"
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
    if message.chat.id != GROUP_CHAT_ID:
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
            await message.reply("⚠ Не удалось распознать позиции в сообщении.")
    except Exception as e:
        await message.reply(f"⚠ Ошибка стока: {e}")

# --------------------------
# КОМАНДЫ
# --------------------------
@router.message(F.text.startswith("/admins"))
async def cmd_admins(message: Message):
    if not is_admin(message.from_user.username):
        return
    admins_env = ", ".join(ADMINS_ENV) if ADMINS_ENV else "-"
    admins_db = ", ".join(db.get_admins()) or "-"
    await message.reply(f"👑 Админы (ENV): {admins_env}\n👤 Админы (DB): {admins_db}")

@router.message(F.text.startswith("/add_admin"))
async def cmd_add_admin(message: Message):
    if not is_admin(message.from_user.username):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Использование: /add_admin @username")
        return
    username = parts[1].lstrip("@")
    db.add_admin(username)
    await message.reply(f"✅ @{username} добавлен в админы")

@router.message(F.text.startswith("/del_admin"))
async def cmd_del_admin(message: Message):
    if not is_admin(message.from_user.username):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Использование: /del_admin @username")
        return
    username = parts[1].lstrip("@")
    db.del_admin(username)
    await message.reply(f"❌ @{username} удалён из админов")

@router.message(F.text.startswith("/plan"))
async def cmd_plan(message: Message):
    if not is_admin(message.from_user.username):
        return
    parts = message.text.split()
    if len(parts) != 3:
        await message.reply("Использование: /plan @username число")
        return
    username = parts[1].lstrip("@")
    plan = int(parts[2])
    db.set_plan(username, plan)
    await message.reply(f"✅ План для @{username} установлен: {plan}")

@router.message(F.text.startswith("/sales_month"))
async def cmd_sales_month(message: Message):
    if not is_admin(message.from_user.username):
        return
    data = db.get_sales_month()
    if not data:
        await message.reply("📊 Продажи с начала месяца: пока нет данных.")
        return
    txt = "📊 Продажи с начала месяца:\n"
    for username, qty, plan, net in data:
        percent = int(qty / plan * 100) if plan else 0
        txt += f"@{username} ({net}): {qty} / план {plan or '-'} → {percent}%\n"
    await message.reply(txt)

@router.message(F.text.startswith("/daily_report"))
async def cmd_daily(message: Message):
    if not is_admin(message.from_user.username):
        return
    await daily_report()

@router.message(F.text.startswith("/weekly_projection"))
async def cmd_projection(message: Message):
    if not is_admin(message.from_user.username):
        return
    await weekly_projection()

@router.message(F.text.startswith("/monthly_report"))
async def cmd_monthly(message: Message):
    if not is_admin(message.from_user.username):
        return
    await monthly_report()

@router.message(F.text.startswith("/stocks"))
async def cmd_stocks(message: Message):
    if not is_admin(message.from_user.username):
        return
    rows = db.get_stocks()
    if not rows:
        await message.reply("📦 Нет данных по стокам.")
        return
    txt = "📦 Стоки:\n"
    for u, item, qty, net, upd in rows:
        txt += f"@{u} {item}: {qty} (сеть: {net}, {upd})\n"
    await message.reply(txt)

@router.message(F.text.startswith("/by_network"))
async def cmd_by_network(message: Message):
    if not is_admin(message.from_user.username):
        return
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply("Использование: /by_network Mechta")
        return
    net = parts[1].capitalize()
    rows = db.get_sales_by_network(net)
    if not rows:
        await message.reply(f"📊 Продажи по сети {net}: нет данных.")
        return
    txt = f"📊 Продажи по сети {net}:\n"
    for u, qty, plan in rows:
        percent = int(qty / plan * 100) if plan else 0
        txt += f"@{u}: {qty} / {plan or '-'} ({percent}%)\n"
    await message.reply(txt)

# --------------------------
# ОТЧЁТЫ (scheduler)
# --------------------------
async def daily_report():
    today = datetime.now(tz).strftime("%Y-%m-%d")
    sales = db.get_sales_all(today)
    if not sales:
        txt = f"📊 Продажи за {today}: пока нет данных."
    else:
        txt = f"📊 Продажи за {today}:\n"
        for u, qty, plan, net in sales:
            percent = int(qty / plan * 100) if plan else 0
            txt += f"@{u} ({net}): {qty} / {plan or '-'} ({percent}%)\n"
    await bot.send_message(GROUP_CHAT_ID, txt)

async def weekly_projection():
    today = datetime.now(tz)
    day_of_month = today.day
    total_days = calendar.monthrange(today.year, today.month)[1]
    data = db.get_sales_month()
    if not data:
        return
    txt = "🔮 Прогноз выполнения плана:\n"
    for u, qty, plan, net in data:
        if not plan or plan == 0:
            continue
        avg = qty / day_of_month
        forecast = int(avg * total_days)
        percent = int(forecast / plan * 100)
        txt += f"@{u} ({net}): {qty}/{plan}, прогноз {forecast} ({percent}%)\n"
    await bot.send_message(GROUP_CHAT_ID, txt)

async def monthly_report():
    today = datetime.now(tz)
    data = db.get_sales_month()
    txt = f"📊 Итог продаж за {today.strftime('%B %Y')}:\n"
    for u, qty, plan, net in data:
        percent = int(qty / plan * 100) if plan else 0
        txt += f"@{u} ({net}): {qty} / {plan or '-'} ({percent}%)\n"
    await bot.send_message(GROUP_CHAT_ID, txt)
    db.reset_monthly_sales()

async def weekly_stock_reminder():
    await bot.send_message(GROUP_CHAT_ID, "📦 Напомните актуальные остатки, пожалуйста.")

# --- Главный цикл ---
async def main():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_report, "cron", hour=21, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_projection, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_stock_reminder, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(monthly_report, "cron", day="last", hour=20, minute=0, timezone="Asia/Almaty")
    scheduler.start()

    print("🚀 Бот запущен 24/7 ✅")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
