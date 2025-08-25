import asyncio, calendar, logging, os, re
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from aiohttp import web

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler

import db

# --- Конфиг окружения ---
TOKEN = os.getenv("BOT_TOKEN", "")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-1002663663535"))
ADMINS_ENV = os.getenv("ADMINS", "").split(",") if os.getenv("ADMINS") else []
tz = timezone("Asia/Almaty")

# Render сам даёт URL в переменной RENDER_EXTERNAL_URL
WEBHOOK_URL = f"{os.getenv('RENDER_EXTERNAL_URL')}/webhook"
PORT = int(os.getenv("PORT", 10000))

# --- Telegram ---
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

logging.basicConfig(level=logging.INFO)

# --- Определение сети ---
def extract_network(username, text: str) -> str:
    bind = db.get_network(username)
    if bind and bind != "-":
        return bind
    t = (text or "").lower()
    for key in ["mechta", "beeline", "sulpak", "sulpka", "td"]:
        if key in t:
            return key.capitalize()
    return "-"

# --- Админы ---
def is_admin(username: str) -> bool:
    if not username:
        return False
    if username in ADMINS_ENV:
        return True
    return db.is_admin(username)

# --- Регулярки ---
# Продажи: гибкая модель (reno 14 / 14f / 14 5g / 14f 5g),
# память 64..1024 (+тб/tb), количество опционально через -, —, :, пробел
SALE_RE = re.compile(
    r"((?:reno\s*)?\d{1,2}\s*(?:f)?\s*(?:5\s*g)?)\s*"
    r"(\d{1,4})(?:тб|tb)?\s*"
    r"[-—: ]?\s*(\d+)?",
    re.IGNORECASE
)

# Стоки: свободный формат; поддержка (8/256) перед памятью
STOCK_RE = re.compile(
    r"([a-zа-яё0-9\+\-\s]+?)\s*"
    r"(?:\(?\d+\s*\/\s*\)?)?\s*"
    r"(\d{1,4})(?:тб|tb)?\s*"
    r"[-—: ]?\s*(\d+)?",
    re.IGNORECASE
)

# --------------------------
# ЕДИНЫЙ ОБРАБОТЧИК
# --------------------------
@router.message()
async def handle_message(message: Message):
    if message.chat.id != GROUP_CHAT_ID or not message.text or message.text.startswith("/"):
        return

    text = message.text.strip()
    if "доля" in text.lower():
        return

    user = message.from_user.username or str(message.from_user.id)
    network = extract_network(user, text)

    try:
        # --- СТОКИ ---
        if any(w in text.lower() for w in ["сток", "остаток", "stock", "stocks", "приход", "приехал"]):
            rows = text.splitlines()
            updated = []
            for row in rows:
                m = STOCK_RE.search(row)
                if not m:
                    continue
                model = m.group(1).strip().replace("  ", " ")
                memory = m.group(2)
                qty = int(m.group(3)) if m.group(3) else 0
                item_name = f"{model} {memory}"
                db.update_stock(user, item_name, qty, network)
                updated.append(f"{item_name} = {qty}")
            if updated:
                await message.reply("📦 Обновлено:\n" + "\n".join(updated) + f"\n(сеть: {network})")
            return

        # --- ПРОДАЖИ ---
        matches = SALE_RE.findall(text)
        if not matches:
            return

        for model_raw, memory, qty_raw in matches:
            model_norm = re.sub(r"\s+", "", model_raw).lower()
            qty = int(qty_raw) if qty_raw else 1

            # Запись продажи
            db.add_sale(user, model_norm, str(memory), qty, network)

            # Сопоставление с остатками (приближённо)
            stock_item, stock_qty = db.find_stock_like(user, model_norm, str(memory), network)
            if stock_item is None:
                await message.reply(f"⚠ Остаток для {model_norm} {memory} не найден. @{user}, обновите сток!")
            elif stock_qty < qty:
                await message.reply(
                    f"⚠ У @{user} не хватает стока для {stock_item} (продажа {qty}, остаток {stock_qty})."
                )
            else:
                db.decrease_stock(user, stock_item, qty, network)

        await message.reply(f"✅ Продажи учтены (сеть: {network})")

    except Exception as e:
        await message.reply(f"⚠ Ошибка: {e}")

# --------------------------
# КОМАНДЫ АДМИНА
# --------------------------
@router.message(F.text.startswith("/set_network"))
async def cmd_set_network(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 3:
        return await message.reply("Использование: /set_network @username NetworkName")
    username = parts[1].lstrip("@")
    network = parts[2].capitalize()
    db.set_network(username, network)
    await message.reply(f"✅ @{username} закреплён за сетью {network}")

@router.message(F.text.startswith("/stocks"))
async def cmd_stocks(message: Message):
    if not is_admin(message.from_user.username): return
    rows = db.get_stocks()
    if not rows:
        return await message.reply("📦 Нет данных по стокам.")
    txt = "📦 Актуальные стоки:\n"
    for u, item, qty, net, upd in rows:
        txt += f"@{u} {item}: {qty} (сеть: {net}, {upd})\n"
    await message.reply(txt)

@router.message(F.text.startswith("/sales_month"))
async def cmd_sales_month(message: Message):
    if not is_admin(message.from_user.username): return
    data = db.get_sales_month()
    if not data:
        return await message.reply("📊 Нет данных за месяц.")
    txt = "📊 Продажи месяца:\n"
    for u, qty, plan, net in data:
        percent = int(qty / plan * 100) if plan else 0
        txt += f"@{u} ({net}): {qty}/{plan or '-'} ({percent}%)\n"
    await message.reply(txt)

@router.message(F.text.startswith("/by_network"))
async def cmd_by_network(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2:
        return await message.reply("Использование: /by_network Mechta")
    net = parts[1].capitalize()
    rows = db.get_sales_by_network(net)
    if not rows:
        return await message.reply(f"📊 По сети {net} нет данных.")
    txt = f"📊 Продажи по сети {net}:\n"
    for u, qty, plan in rows:
        percent = int(qty / plan * 100) if plan else 0
        txt += f"@{u}: {qty}/{plan or '-'} ({percent}%)\n"
    await message.reply(txt)

@router.message(F.text.startswith("/admins"))
async def cmd_admins(message: Message):
    if not is_admin(message.from_user.username): return
    envs = ", ".join(ADMINS_ENV) if ADMINS_ENV else "-"
    dbs = ", ".join(db.get_admins()) or "-"
    await message.reply(f"👑 ENV: {envs}\n👤 DB: {dbs}")

@router.message(F.text.startswith("/add_admin"))
async def cmd_add_admin(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 2:
        return await message.reply("Использование: /add_admin @username")
    db.add_admin(parts[1].lstrip("@"))
    await message.reply(f"✅ {parts[1]} добавлен")

@router.message(F.text.startswith("/del_admin"))
async def cmd_del_admin(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 2:
        return await message.reply("Использование: /del_admin @username")
    db.del_admin(parts[1].lstrip("@"))
    await message.reply(f"❌ {parts[1]} удалён")

@router.message(F.text.startswith("/plan"))
async def cmd_plan(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 3:
        return await message.reply("Использование: /plan @username число")
    db.set_plan(parts[1].lstrip("@"), int(parts[2]))
    await message.reply(f"✅ План для {parts[1]} = {parts[2]}")

@router.message(F.text.startswith("/set_sales"))
async def cmd_set_sales(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 3:
        return await message.reply("Использование: /set_sales @username число")
    username = parts[1].lstrip("@")
    try:
        qty = int(parts[2])
    except:
        return await message.reply("⚠ Количество должно быть числом")
    db.set_sales(username, qty)
    await message.reply(f"✅ Продажи для @{username} установлены вручную = {qty}")

# --------------------------
# ОТЧЁТЫ + НАПОМИНАНИЯ
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
    d = max(1, today.day)
    total_days = calendar.monthrange(today.year, today.month)[1]
    data = db.get_sales_month()
    if not data:
        return
    txt = "🔮 Прогноз выполнения плана:\n"
    for u, qty, plan, net in data:
        if not plan:
            continue
        forecast = int((qty / d) * total_days)
        percent = int(forecast / plan * 100)
        txt += f"@{u} ({net}): {qty}/{plan}, прогноз {forecast} ({percent}%)\n"
    await bot.send_message(GROUP_CHAT_ID, txt)

async def monthly_report():
    today = datetime.now(tz)
    data = db.get_sales_month()
    txt = f"📊 Итог продаж за {today.strftime('%B %Y')}:\n"
    for u, qty, plan, net in data:
        percent = int(qty / plan * 100) if plan else 0
        txt += f"@{u} ({net}): {qty}/{plan or '-'} ({percent}%)\n"
    await bot.send_message(GROUP_CHAT_ID, txt)
    db.reset_monthly_sales()

async def weekly_stock_reminder():
    await bot.send_message(GROUP_CHAT_ID, "📦 Напомните актуальные остатки!")

async def inactive_promoters_reminder(days_threshold=3):
    """
    Ежедневно в 20:30: если с последней продажи прошло >= 3 дней —
    пингуем в общий чат. После 3-го дня напоминаем КАЖДЫЙ день, пока не отпишется.
    """
    today = datetime.now(tz).date()
    users = db.get_all_usernames()
    if not users:
        return

    for u in users:
        last_date = db.get_last_sale_date(u)
        if not last_date:
            await bot.send_message(
                GROUP_CHAT_ID,
                f"⚠ @{u}, у вас ещё нет продаж в этом месяце. Обновите актуальные данные!"
            )
            continue
        try:
            last_dt = datetime.strptime(last_date, "%Y-%m-%d").date()
        except:
            continue
        days_passed = (today - last_dt).days
        if days_passed >= days_threshold:
            await bot.send_message(
                GROUP_CHAT_ID,
                f"⚠ @{u}, вы не писали продажи {days_passed} дней. Пожалуйста, обновите актуальные продажи!"
            )

# --------------------------
# MAIN (webhook + scheduler)
# --------------------------
async def main():
    # Сбросим старый вебхук и поставим новый (исключаем конфликты)
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)

    # Планировщик
    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_report, "cron", hour=21, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_projection, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(weekly_stock_reminder, "cron", day_of_week="sun", hour=12, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(monthly_report, "cron", day="last", hour=20, minute=0, timezone="Asia/Almaty")
    scheduler.add_job(inactive_promoters_reminder, "cron", hour=20, minute=30, timezone="Asia/Almaty")
    scheduler.start()

    # HTTP-сервер под webhook
    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT); await site.start()

    print(f"🚀 Webhook бот запущен на {WEBHOOK_URL}")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
