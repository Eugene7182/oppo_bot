import asyncio
import calendar
import logging
import os
import re
from datetime import datetime
from difflib import get_close_matches

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from aiohttp import web

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.webhook.aiohttp_server import SimpleRequestHandler

import db

# =========================================================
# НАСТРОЙКИ / ОКРУЖЕНИЕ
# =========================================================
TOKEN = os.getenv("BOT_TOKEN", "")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "-1002663663535"))
ADMINS_ENV = os.getenv("ADMINS", "").split(",") if os.getenv("ADMINS") else []

# Render прокидывает внешний адрес сюда
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
WEBHOOK_URL = f"{RENDER_EXTERNAL_URL}/webhook" if RENDER_EXTERNAL_URL else ""
PORT = int(os.getenv("PORT", 10000))

tz = timezone("Asia/Almaty")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# =========================================================
# ХЕЛПЕРЫ
# =========================================================
def is_admin(username: str) -> bool:
    """Проверка админа: ENV + БД"""
    if not username:
        return False
    if username in ADMINS_ENV:
        return True
    return db.is_admin(username)

def normalize_text(text: str) -> str:
    """Склейки и чистка: '14 F'→'14F', убрать лишние тире/пробелы."""
    if not text:
        return ""
    t = text

    # Склеиваем цифры и следующую букву: "14 F" -> "14F"
    t = re.sub(r"(\d+)\s*([a-zа-яё])", r"\1\2", t, flags=re.IGNORECASE)
    # Убираем тире-переводы в пробел
    t = re.sub(r"[–—\-]+", " ", t)
    # Одинарные пробелы
    t = re.sub(r"\s+", " ", t).strip()
    return t

def extract_network(username: str, text: str) -> str:
    """
    Сеть для пользователя: сначала привязка /set_network,
    если нет — пробуем вытащить из текста (mechta, beeline, sulpak, td),
    иначе '-'.
    """
    bind = db.get_network(username)
    if bind and bind != "-":
        return bind
    t = (text or "").lower()
    for key in ["mechta", "beeline", "sulpak", "sulpka", "td"]:
        if key in t:
            return key.capitalize()
    return "-"

def closest_match(item: str, candidates: list[str], cutoff: float = 0.6) -> str | None:
    """Нечёткое сравнение: вернуть ближайшую строку-кандидата."""
    m = get_close_matches(item, candidates, n=1, cutoff=cutoff)
    return m[0] if m else None

# =========================================================
# РЕГУЛЯРКИ (гибкие)
# =========================================================
# Продажи:
#   - модель = одна "склейка" из букв/цифр/+,-
#   - память = 2..4 цифры или TB/тб
#   - количество опционально через - : пробел
SALE_RE = re.compile(
    r"([a-zа-яё0-9\-\+]+)\s+(\d{2,4}(?:tb|тб)?)\s*[-: ]?\s*(\d+)?",
    re.IGNORECASE
)

# Стоки: строка → (модель) (память) [-|:|пробел] qty?
# допускаем "(8/256)" (игнорируем 8/)
STOCK_RE = re.compile(
    r"([a-zа-яё0-9\+\-\s]+?)\s*(?:\(?\d+\s*/\s*)?(\d{2,4})(?:тб|tb)?\)?\s*[-: ]?\s*(\d+)?",
    re.IGNORECASE
)

# =========================================================
# ЕДИНЫЙ ОБРАБОТЧИК СООБЩЕНИЙ
# =========================================================
@router.message()
async def handle_message(message: Message):
    # Только наш групповой чат, игнор /commands, игнор "доля"
    if message.chat.id != GROUP_CHAT_ID or not message.text or message.text.startswith("/"):
        return
    if "доля" in message.text.lower():
        return

    raw_text = message.text
    text = normalize_text(raw_text)

    user = message.from_user.username or str(message.from_user.id)
    network = extract_network(user, text)

    try:
        # -------------------------------------------------
        # ЧАСТЬ 1: СТОКИ (ключевые слова)
        # -------------------------------------------------
        if any(w in text.lower() for w in ["сток", "остаток", "stock", "stocks", "приход", "приехал"]):
            rows = raw_text.splitlines()
            updated = []
            # Было ли у пользователя хоть что-то в стоках ДО обновления?
            before = [r for r in db.get_stocks() if r[0] == user]

            for row in rows:
                m = STOCK_RE.search(normalize_text(row))
                if not m:
                    continue
                model = m.group(1).strip()
                memory = m.group(2)
                qty = int(m.group(3)) if m.group(3) else 0
                item_name = f"{model} {memory}"
                db.update_stock(user, item_name, qty, network)
                updated.append(f"{item_name} = {qty}")

            if updated:
                # Если это ПЕРВЫЕ стоки у пользователя — сообщаем, что теперь включён контроль
                if not before:
                    await message.reply(
                        "📦 Стоки сохранены. Начиная с этого момента продажи будут сверяться с остатками.\n"
                        "Если модель не найдена — бот попросит обновить позицию."
                    )
                # Общая квитанция по обновлению
                await message.reply("📦 Обновлено:\n" + "\n".join(updated) + (f"\n(сеть: {network})" if network != "-" else ""))
            else:
                await message.reply("⚠ Не удалось распознать позиции в сообщении.")
            return

        # -------------------------------------------------
        # ЧАСТЬ 2: ПРОДАЖИ
        # -------------------------------------------------
        matches = SALE_RE.findall(text)
        if not matches:
            return

        # Все стоки (для поиска/фильтрации)
        all_stocks = db.get_stocks()
        # Только стоки этого пользователя и (если есть) сети
        def filter_user_stocks(rows):
            if network == "-":
                return [r for r in rows if r[0] == user]
            return [r for r in rows if r[0] == user and (r[3] == network)]

        user_stocks = filter_user_stocks(all_stocks)

        for model_raw, memory, qty_raw in matches:
            # Нормализуем модель (убираем пробелы внутри)
            model_norm = re.sub(r"\s+", "", model_raw).lower()
            mem_norm = memory.lower()
            qty = int(qty_raw) if qty_raw else 1

            # Записываем продажу в любом случае (тихая запись поддерживается)
            db.add_sale(user, model_norm, mem_norm, qty, network)

            # Если у пользователя НЕТ стоков — просто молчим и не пытаемся сверять
            if not user_stocks:
                continue

            # Ключ из продажи
            sale_key = f"{model_norm} {mem_norm}".strip()

            # Кандидаты для fuzzy — ТОЛЬКО его стоки (и его сеть)
            stock_items = [row[1] for row in user_stocks]  # item string из stocks
            candidate = closest_match(sale_key, stock_items, cutoff=0.6)

            if not candidate:
                await message.reply(f"⚠ Модель «{sale_key}» не найдена в ваших остатках. Обновите сток!")
                continue

            # Сверяем количество по candidate
            current_qty = db.get_stock_qty(user, candidate, network)
            if current_qty is None:
                await message.reply(f"⚠ Остаток для «{candidate}» не найден. Обновите сток!")
                continue

            if current_qty < qty:
                await message.reply(
                    f"⚠ Не хватает остатка для «{candidate}»: продажа {qty}, остаток {current_qty}. "
                    "Обновите сток!"
                )
                continue

            # Всё ок — уменьшаем
            db.decrease_stock(user, candidate, qty, network)

        # Сообщение об успехе — только если у пользователя уже были стоки
        if user_stocks:
            await message.reply(f"✅ Продажи учтены (сеть: {network})")

    except Exception as e:
        await message.reply(f"⚠ Ошибка: {e}")

# =========================================================
# КОМАНДЫ АДМИНА
# =========================================================
@router.message(F.text.startswith("/admins"))
async def cmd_admins(message: Message):
    if not is_admin(message.from_user.username): return
    envs = ", ".join(ADMINS_ENV) if ADMINS_ENV else "-"
    dbs = ", ".join(db.get_admins()) or "-"
    await message.reply(f"👑 Админы (ENV): {envs}\n👤 Админы (DB): {dbs}")

@router.message(F.text.startswith("/add_admin"))
async def cmd_add_admin(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 2:
        return await message.reply("Использование: /add_admin @username")
    db.add_admin(parts[1].lstrip("@"))
    await message.reply(f"✅ {parts[1]} добавлен в админы")

@router.message(F.text.startswith("/del_admin"))
async def cmd_del_admin(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 2:
        return await message.reply("Использование: /del_admin @username")
    db.del_admin(parts[1].lstrip("@"))
    await message.reply(f"❌ {parts[1]} удалён из админов")

@router.message(F.text.startswith("/plan"))
async def cmd_plan(message: Message):
    if not is_admin(message.from_user.username): return
    parts = message.text.split()
    if len(parts) != 3:
        return await message.reply("Использование: /plan @username число")
    username = parts[1].lstrip("@")
    try:
        plan = int(parts[2])
    except:
        return await message.reply("⚠ План должен быть числом")
    db.set_plan(username, plan)
    await message.reply(f"✅ План для @{username} = {plan}")

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
    pick = parts[1].capitalize()

    # Берём общую сводку и фильтруем по сети здесь
    data = db.get_sales_month()
    data = [row for row in data if (row[3] or "-") == pick]
    if not data:
        return await message.reply(f"📊 По сети {pick} нет данных.")
    txt = f"📊 Продажи по сети {pick}:\n"
    for u, qty, plan, _net in data:
        percent = int(qty / plan * 100) if plan else 0
        txt += f"@{u}: {qty}/{plan or '-'} ({percent}%)\n"
    await message.reply(txt)

# =========================================================
# АВТО-ОТЧЁТЫ И НАПОМИНАНИЯ
# =========================================================
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
    day = max(1, today.day)
    total_days = calendar.monthrange(today.year, today.month)[1]
    data = db.get_sales_month()
    if not data:
        return
    txt = "🔮 Прогноз выполнения плана:\n"
    for u, qty, plan, net in data:
        if not plan:
            continue
        forecast = int((qty / day) * total_days)
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

async def inactive_promoters_reminder(days_threshold: int = 3):
    """
    Каждый день в 20:30: если с последней продажи >= 3 дней — пингуем.
    Напоминаем ежедневно, пока не отпишутся.
    """
    all_stocks = db.get_stocks()
    users = sorted({row[0] for row in all_stocks})  # только те, кто вообще имеет стоки
    for u in users:
        last_date = db.get_last_sale(u)  # "YYYY-MM-DD" или None
        if not last_date:
            await bot.send_message(
                GROUP_CHAT_ID,
                f"⚠ @{u}, у вас ещё нет продаж в этом месяце. Обновите актуальные данные!"
            )
            continue
        try:
            days = (datetime.now(tz).date() - datetime.strptime(last_date, "%Y-%m-%d").date()).days
        except Exception:
            continue
        if days >= days_threshold:
            await bot.send_message(
                GROUP_CHAT_ID,
                f"⚠ @{u}, вы не писали продажи {days} дней. Пожалуйста, обновите актуальные продажи!"
            )

# =========================================================
# ЗАПУСК: webhook + scheduler
# =========================================================
async def main():
    # Инициализация БД (на всякий)
    db.init_db()

    # Сбросить старый webhook и поставить новый (защита от конфликтов)
    await bot.delete_webhook(drop_pending_updates=True)
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL)
    else:
        logging.warning("WEBHOOK_URL пуст. Проверь RENDER_EXTERNAL_URL в переменных окружения.")

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

    # healthcheck (чтобы Render не сыпал 404 по /)
    async def health(_):
        return web.Response(text="OK")
    app.router.add_get("/", health)

    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path="/webhook")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    print(f"🚀 Webhook бот запущен на {WEBHOOK_URL or '(URL не задан)'}")
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
