import requests
import calendar
import json
import asyncio
import re
from datetime import datetime, timedelta
import logging
import html

from telegram import InlineKeyboardButton
from telegram.helpers import escape_markdown  # Використовуємо вбудовану функцію
from telegram.helpers import escape_markdown as tg_escape_markdown
from telegram import CallbackQuery
from fpdf import FPDF






# Telegram API imports
from telegram import (
    Update,
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    InputMediaPhoto,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackContext,
    MessageHandler,
    filters,
    CallbackQueryHandler,
)
from telegram.error import BadRequest, Forbidden
from telegram.helpers import escape_markdown

# Firebase imports
import firebase_admin
from firebase_admin import credentials, firestore

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

print("Bot is starting...")

# ========= FIREBASE INITIALIZATION =========
with open(r"C:\Users\reset\Downloads\ton-not-firebase-adminsdk-5lvba-39b1a0ff34.json", encoding="utf-8") as f:
    cred_data = json.load(f)
cred = credentials.Certificate(cred_data)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ========= TELEGRAM BOT TOKEN =========
TELEGRAM_TOKEN = "7667202033:AAE6wmv967u_-rq64XREq8ea4bRIE1mFGcI"

# ========= ADMIN & GLOBAL VARIABLES =========
ADMIN_USER_IDS = [5523891091, 6359919561]  # Заміна на ваші ID
user_selection = {}
booked_slots = {}
pending_confirmations = {}


# === КЕШ ДЛЯ ПОСЛУГ (завантажується з Firestore) ===
services_cache = {}  # { doc_id: {"name": str, "price": str, "duration": int, "active": bool} }

# Інфо про барбера (для розділу "Про нас")
BARBER_NAME = "NAZAR BARBER"
PHONE_NUMBER = "+380 63 338 87 38"

# Режим розсилки (для адміністраторів): {admin_id: bool}
admin_broadcast_mode = {}

# Стан для адміністраторських налаштувань:
ADMIN_STATE = {}  # {admin_id: state_string}

# ========= НОВЕ: ЗБЕРІГАННЯ РОБОЧОГО ГРАФІКА В БД =========
# "default_schedule" лежить у документі schedule_config/default_schedule (назва колекції чи документа може бути будь-яка).
# Ключі: 0..6 (Mon..Sun)
# Значення: {"start": "HH:MM", "end": "HH:MM", "off": bool}
# Якщо немає документа — створимо з таким стартовим:
INITIAL_DEFAULT_SCHEDULE = {
    0: {"start": "10:00", "end": "21:30", "off": False},  # Monday
    1: {"start": "10:00", "end": "21:30", "off": False},  # Tuesday
    2: {"start": "10:00", "end": "21:30", "off": False},  # Wednesday
    3: {"start": "10:00", "end": "21:30", "off": False},  # Thursday
    4: {"start": "10:00", "end": "21:30", "off": False},  # Friday
    5: {"start": "10:00", "end": "21:30", "off": False},  # Saturday
    6: {"start": "10:00", "end": "21:30", "off": True},  # Sunday (тепер працює)
}


# Локальний кеш завантаженого дефолтного розкладу
DEFAULT_WEEK_SCHEDULE = {}  # буде підвантажений із БД нижче

working_hours_mode = "default"
working_hours_default = {
    "default": [("10:00", "21:30")]
}
working_hours_kurs = {
    "default": [("10:00", "12:30"), ("18:00", "21:30")],
}

# ========= 1. ЗАГРУЗКА ВЖЕ ПІДТВЕРДЖЕНИХ ЗАПИСІВ (booked_slots) =========
def load_booked_slots():
    global booked_slots
    # Завантажуємо записи, статус яких або "pending", або "confirmed"
    bookings = db.collection("bookings").where("status", "in", ["pending", "confirmed"]).stream()
    for booking in bookings:
        data = booking.to_dict()
        date = data.get("date")
        time = data.get("time")
        if date and time:
            booked_for_date = booked_slots.get(date, [])
            duration = data.get("duration", 30)  # fallback, якщо немає
            slots_needed = duration // 30
            start_time = datetime.strptime(time, "%H:%M")
            for i in range(slots_needed):
                time_str = (start_time + timedelta(minutes=30 * i)).strftime("%H:%M")
                booked_for_date.append(time_str)
            booked_slots[date] = booked_for_date

def is_blacklisted(user_id: int) -> bool:
    doc = db.collection("users").document(str(user_id)).get()
    if doc.exists:
        return doc.to_dict().get("blacklisted", False)
    return False

def blacklist_protected(func):
    async def wrapper(update: Update, context: CallbackContext):
        user_id = update.effective_user.id if update.effective_user else None
        if user_id and is_blacklisted(user_id):
            if update.message:
                await update.message.reply_text("🚫 Ви перебуваєте у чорному списку та не можете користуватися ботом.")
            elif update.callback_query:
                await safe_edit_message_text(update.callback_query, "🚫 Ви перебуваєте у чорному списку та не можете користуватися ботом.")
            return  # Не викликаємо основну функцію
        return await func(update, context)
    return wrapper


# ========= 2. ЗАГРУЗКА/ОНОВЛЕННЯ СПИСКУ ПОСЛУГ (services_cache) =========
def refresh_services_cache():
    global services_cache
    services_cache.clear()
    services_ref = db.collection("services").stream()
    for doc in services_ref:
        data = doc.to_dict()
        services_cache[doc.id] = data

# ========= ЗАГРУЗКА ДЕФОЛТНОГО РОЗКЛАДУ ІЗ БД =========
def load_default_schedule_from_db():
    """
    Завантажує розклад із Firestore. Якщо документа немає, створює його з DEFAULT_WEEK_SCHEDULE.
    """
    global DEFAULT_WEEK_SCHEDULE
    schedule_config_ref = db.collection("schedule_config").document("default_schedule")
    doc_ = schedule_config_ref.get()

    if doc_.exists:
        # Якщо документ існує, підвантажуємо його в new_schedule
        saved_data = doc_.to_dict()
        new_schedule = {}

        for k, v in saved_data.items():
            try:
                idx = int(k)  # Переконуємось, що ключ є цілим числом
                if not isinstance(v, dict):
                    print(f"⚠️ Warning: Invalid schedule data for {k}: {v}. Skipping...")
                    continue
                
                # Перевіряємо, чи є всі потрібні ключі
                start = v.get("start", "").strip()
                end = v.get("end", "").strip()
                off = v.get("off", None)

                if not start or not end or off is None:
                    print(f"⚠️ Warning: Incomplete schedule for day {k}: {v}. Using default...")
                    new_schedule[idx] = INITIAL_DEFAULT_SCHEDULE[idx]
                else:
                    new_schedule[idx] = {
                        "start": start,
                        "end": end,
                        "off": off
                    }
            except ValueError:
                print(f"⚠️ Error: Invalid schedule key '{k}' in Firestore. Skipping...")

        # Переконуємось, що всі 7 днів є в розкладі (на випадок пошкоджених даних)
        for i in range(7):
            if i not in new_schedule:
                print(f"⚠️ Warning: Missing schedule for day {i}. Using default value.")
                new_schedule[i] = INITIAL_DEFAULT_SCHEDULE[i]

        DEFAULT_WEEK_SCHEDULE = new_schedule
        print("✅ Default schedule successfully loaded from Firestore.")
    
    else:
        # Якщо документа немає — створюємо його у Firestore
        formatted_schedule = {str(k): v for k, v in INITIAL_DEFAULT_SCHEDULE.items()}
        schedule_config_ref.set(formatted_schedule)
        DEFAULT_WEEK_SCHEDULE = INITIAL_DEFAULT_SCHEDULE.copy()
        print("🆕 No schedule found in Firestore. Initialized with default schedule.")

def save_default_schedule_to_db():
    """
    Зберігає поточний DEFAULT_WEEK_SCHEDULE до Firestore:
    schedule_config/default_schedule
    """
    schedule_config_ref = db.collection("schedule_config").document("default_schedule")
    to_save = {}

    for i in range(7):
        day_info = DEFAULT_WEEK_SCHEDULE.get(i, {})

        # Validate fields
        start = day_info.get("start", "").strip()
        end = day_info.get("end", "").strip()
        off = day_info.get("off", None)

        if not start or not end or off is None:
            print(f"⚠️ Warning: Skipping invalid schedule entry for day {i}: {day_info}")
            continue  # Skip if any field is invalid

        to_save[str(i)] = {
            "start": start,
            "end": end,
            "off": off
        }

    if to_save:
        schedule_config_ref.set(to_save)
        print("✅ Default schedule successfully saved to Firestore.")
    else:
        print("⚠️ No valid schedule data to save.")

# ========= ДОПОМІЖНІ ФУНКЦІЇ =========
def escape_markdown(text, version=1):
    """Екранування спецсимволів у Markdown."""
    escape_chars = r'_*[]()~>#+-=|{}.!'

    if version == 2:
        escape_chars = r'_*[]()~>#+-=|{}.!'
    return re.sub(r'([{}])'.format(re.escape(escape_chars)), r'\\\1', text)

def safe_strptime(time_str, fmt):
    """
    Безпечне перетворення рядка на datetime.time (або datetime) за форматом fmt.
    Повертає None, якщо помилка.
    """
    try:
        return datetime.strptime(time_str, fmt)
    except ValueError:
        return None

async def safe_edit_message_text(query, text, reply_markup=None, parse_mode="MarkdownV2"):
    try:
        # Використовуємо вбудовану функцію для екранювання
        if parse_mode in ["Markdown", "MarkdownV2"]:
            text = tg_escape_markdown(text, version=2)

        message = query.message

        # Якщо повідомлення містить медіа — видаляємо і надсилаємо нове
        if message.photo or message.video or message.document or message.audio:
            await message.delete()
            await query.message.chat.send_message(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
            return

        # Якщо текст та кнопки не змінилися — видаляємо і надсилаємо нове
        if message.text.strip() == text.strip() and message.reply_markup == reply_markup:
            logging.info("⚠️ Повідомлення не змінилося, видаляємо і надсилаємо нове.")
            await query.message.delete()
            await query.message.chat.send_message(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
            return

        # Редагуємо текстове повідомлення
        await query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )

    except BadRequest as e:
        error_message = str(e)
        logging.warning(f"⚠️ Telegram BadRequest Error: {error_message}")

        if "message to edit not found" in error_message or "Message can't be edited" in error_message:
            try:
                await query.message.delete()
                await query.message.chat.send_message(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
            except Exception as e2:
                logging.error(f"❌ Не вдалося надіслати нове повідомлення: {e2}")
        elif "Message is not modified" in error_message:
            logging.info("⚠️ Повідомлення вже має такий самий вміст, редагування скасовано.")
            await query.message.delete()
            await query.message.chat.send_message(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        elif "can't find end of the entity" in error_message:
            logging.warning("⚠️ Некоректний Markdown, пробуємо без форматування.")
            await query.edit_message_text(text=text, reply_markup=reply_markup)
        else:
            raise e

def get_custom_schedule_for_date(date_str):
    doc_ref = db.collection("custom_schedule").document(date_str)
    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict()
    return None

def update_day_in_db(date_str, off, start, end):
    doc_ref = db.collection("custom_schedule").document(date_str)
    doc_ref.set(
        {
            "off": off,
            "start": start,
            "end": end
        },
        merge=True
    )

def get_weekday_name_ua(weekday_index: int) -> str:
    """Повертає назву дня тижня українською за індексом (0=понеділок, 6=неділя)."""
    weekdays_uk = ["Понеділок", "Вівторок", "Середа", "Четвер", "П’ятниця", "Субота", "Неділя"]
    return weekdays_uk[weekday_index]

def get_working_hours(date: datetime.date, service_name: str):
    """
    Повертає список кортежів [(start, end), ...] для заданої дати (з custom_schedule чи дефолту).
    Якщо день вихідний — повертає пустий список.
    """
    # 1) Якщо режим 'kurs', то для прикладу свої години:
    global working_hours_mode
    if working_hours_mode == "kurs":
        weekday = date.weekday()  # Monday=0..Sunday=6
        if weekday == 5:  # Субота
            return [("10:00", "21:00")]
        else:
            return working_hours_kurs.get("default", [])

    # 2) Перевіряємо custom_schedule
    date_str = date.strftime("%d.%m.%Y")
    custom_data = get_custom_schedule_for_date(date_str)
    if custom_data:
        if custom_data["off"]:
            return []
        else:
            return [(custom_data["start"], custom_data["end"])]

    # 3) Якщо немає custom_data, дивимось DEFAULT_WEEK_SCHEDULE з БД
    weekday = date.weekday()
    day_info = DEFAULT_WEEK_SCHEDULE.get(weekday, {})
    if day_info.get("off"):
        return []
    else:
        return [(day_info["start"], day_info["end"])]

# ====== Telegram меню (користувач / адмін) ======
async def show_user_menu(update: Update):
    keyboard = [
        ["Записатися на послугу"],
        ["КОСМЕТИКА", "ІСТОРІЯ", "Про нас"],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
    welcome_message = (
        f"Привіт {update.effective_user.mention_html()} 👋\n"
        f"Ласкаво просимо до нашого барбершопу 💈"
    )
    if update.message:
        await update.message.reply_html(welcome_message, reply_markup=reply_markup)
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.delete()
        await update.effective_chat.send_message(
            text=welcome_message,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )

async def show_admin_menu(update: Update):
    """
    Головне меню для адміністратора.
    """
    keyboard = [
        ["📅 Записи", "👤 Клієнти"],
        ["📊 Статистика", "💇‍♂️ Налаштування послуг"],
        ["📆 Розклад", "✉️ Повідомлення"],
        ["⚙️ Налаштування бота"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)
    welcome_message = (
        f"Привіт {update.effective_user.mention_html()} 👋\n"
        f"Ласкаво просимо до панелі адміністратора 💼"
    )
    if update.message:
        await update.message.reply_html(welcome_message, reply_markup=reply_markup)
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.delete()
        await update.effective_chat.send_message(
            text=welcome_message,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )

@blacklist_protected
async def start(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        await show_admin_menu(update)
    else:
        await show_user_menu(update)

async def kurs_command(update: Update, context: CallbackContext) -> None:
    if update.effective_user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("У вас немає прав для виконання цієї команди.")
        return
    global working_hours_mode
    working_hours_mode = "kurs"
    await update.message.reply_text(
        "Робочий час змінено на 'kurs' режим.\n\n"
        "Приклад: 10:00-12:30 та 18:00-21:30, Субота 10:00-21:00."
    )

async def all_command(update: Update, context: CallbackContext) -> None:
    if update.effective_user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("У вас немає прав для виконання цієї команди.")
        return
    global working_hours_mode
    working_hours_mode = "default"
    await update.message.reply_text(
        "Робочий час відновлено на дефолтний (неділя вихідний)."
    )

async def cancel_command(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        if admin_broadcast_mode.get(user_id, False):
            admin_broadcast_mode[user_id] = False
            await update.message.reply_text("Розсилку скасовано.")
        if user_id in ADMIN_STATE:
            ADMIN_STATE[user_id] = None
            await update.message.reply_text("Поточну операцію скасовано.")
    else:
        await update.message.reply_text("Нема поточної операції для скасування.")

# ========= Меню користувача =========
async def about_us(update: Update, context: CallbackContext) -> None:
    """Відправляє інформацію про барбершоп з актуальним тижневим графіком роботи українською мовою (без конкретних дат)."""

    # Сформуємо текст з дефолтного розкладу (7 днів)
    # Увага: це лише "день тижня" + години. Custom для конкретних дат тут не враховуємо.
    week_schedule_text = ""
    for i in range(7):
        weekday_str = get_weekday_name_ua(i)
        day_info = DEFAULT_WEEK_SCHEDULE.get(i, {"start": "??:??", "end": "??:??", "off": True})
        if day_info["off"]:
            hours = "❌ Вихідний"
        else:
            hours = f"{day_info['start']} - {day_info['end']}"
        week_schedule_text += f"📅 *{weekday_str}*: {hours}\n"

    text = f"""✂️ *Вітаю! Я — {BARBER_NAME}, майстер-барбер, який допомагає клієнтам виглядати стильно та доглянуто.* 💈

📌 *Послуги:*
    • 💇‍♂️ *Чоловічі стрижки* — класика і сучасність
    • 🧔 *Догляд за бородою* — обрізка та формування
    • 🪒 *Традиційне гоління* — гладко та комфортно
    • 🌟 *Стилізація* — вибір образу, що підходить вам
    • 👶 *Дитячі стрижки* — з турботою про комфорт дитини

💎 *Чому обирають мене:*
    • 👤 *Індивідуальний підхід* до кожного клієнта
    • ✅ *Висока якість* і увага до деталей
    • ✨ *Стиль*, що підкреслює вашу унікальність

📍 *Наші дані:*
    📞 *Телефон:* {PHONE_NUMBER}
    ⏰ *Графік роботи на тиждень:*
{week_schedule_text}
    📲 *Telegram:* [@shvetsnazar_barber](https://t.me/shvetsnazar_barber)

Завжди радий бачити нових і постійних клієнтів! Дозвольте мені допомогти вам виглядати на всі 100%! 🌟
"""

    text = escape_markdown(text, version=2)

    keyboard = [
        [
            InlineKeyboardButton("Instagram", url="https://www.instagram.com/shvetsnazar_barber/"),
            InlineKeyboardButton("WhatsApp", url="https://www.instagram.com/shvetsnazar_barber/c")
        ],
        [InlineKeyboardButton("НАЗАД", callback_data="back_to_main_menu_text")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.message:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="MarkdownV2")
    else:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode="MarkdownV2"
        )

async def cosmetics(update: Update, context: CallbackContext) -> None:
    photo_url = "https://i.ibb.co/7W1nTm1/361fdbc957e0.jpg"
    caption = (
        "Uppercut Deluxe Matte Pomade (100g) — це універсальний засіб для укладки волосся, "
        "який надає матовий ефект і середню фіксацію. Підходить для всіх типів волосся "
        "та легко змивається водою."
    )

    caption = escape_markdown(caption, version=2)

    keyboard = [
        [InlineKeyboardButton("ЗАМОВИТИ", callback_data="order_cosmetics")],
        [InlineKeyboardButton("НАЗАД", callback_data="back_to_main_menu_text")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.message:
        await update.message.reply_photo(
            photo=photo_url,
            caption=caption,
            reply_markup=reply_markup,
            parse_mode="MarkdownV2"
        )
    else:
        query = update.callback_query
        await query.answer()
        await query.message.delete()
        await update.effective_chat.send_photo(
            photo=photo_url,
            caption=caption,
            reply_markup=reply_markup,
            parse_mode="MarkdownV2"
        )
@blacklist_protected
async def book_service(update: Update, context: CallbackContext) -> None:
    query = None
    if update.callback_query:
        query = update.callback_query
        await query.answer()

    active_services = [
        (doc_id, data)
        for doc_id, data in services_cache.items()
        if data.get("active")
    ]
    if not active_services:
        text = "💈 На жаль, зараз немає доступних послуг. ✂️ Спробуйте пізніше!"
        back_button = [[InlineKeyboardButton("НАЗАД", callback_data="back_to_main_menu_text")]]
        reply_markup = InlineKeyboardMarkup(back_button)
        if query:
            await safe_edit_message_text(query, text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
        return

    keyboard = []
    for doc_id, data in active_services:
        name = data.get("name", "Без назви")
        price = data.get("price", "0 євро")
        btn_text = f"{name} - {price}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"book_service_{doc_id}")])

    keyboard.append([InlineKeyboardButton("НАЗАД", callback_data="back_to_main_menu_text")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "Оберіть послугу:"
    if query:
        await safe_edit_message_text(query, text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

# ========= ВИБІР ДАТИ (для запису) =========
async def select_date(update: Update, context: CallbackContext) -> None:
    """Функція для вибору дати запису (через callback_data в button_handler)."""
    query = update.callback_query
    await query.answer()

    data = query.data
    offset = 0

    if data.startswith("more_dates_"):
        parts = data.split("_")
        if len(parts) == 3:
            offset = int(parts[2])
        offset += 14

    elif data.startswith("previous_dates_"):
        parts = data.split("_")
        if len(parts) == 3:
            old_offset = int(parts[2])
            offset = max(0, old_offset - 14)

    today = datetime.now().date() + timedelta(days=offset)
    dates = []

    for i in range(14):
        current_date = today + timedelta(days=i)
        if current_date.weekday() == 6:  # Якщо це неділя
            custom_data = get_custom_schedule_for_date(current_date.strftime("%d.%m.%Y"))
            # Якщо є дані та вказано, що вихідний, то пропускаємо неділю.
            if custom_data is not None and custom_data.get("off"):
                continue
        dates.append(current_date.strftime("%d.%m.%Y"))

    if not dates:
        keyboard = [[InlineKeyboardButton("НАЗАД", callback_data="back_to_services")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_edit_message_text(query, "Немає доступних дат.", reply_markup=reply_markup)
        return

    keyboard = []
    for i in range(0, len(dates), 2):
        row = [InlineKeyboardButton(dates[i], callback_data=f"date_{dates[i]}")]
        if i + 1 < len(dates):
            row.append(InlineKeyboardButton(dates[i + 1], callback_data=f"date_{dates[i + 1]}"))
        keyboard.append(row)

    nav_row = []
    if offset > 0:
        nav_row.append(InlineKeyboardButton("🔙 Назад", callback_data=f"previous_dates_{offset}"))
    else:
        nav_row.append(InlineKeyboardButton("🔙 Назад", callback_data="back_to_services"))
    
    nav_row.append(InlineKeyboardButton("Ще Дата", callback_data=f"more_dates_{offset}"))
    keyboard.append(nav_row)

    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(query, "📅 Оберіть дату запису:", reply_markup=reply_markup)
    
def round_up_to_next_slot(dt: datetime) -> datetime:
    dt = dt.replace(second=0, microsecond=0)
    remainder = dt.minute % 30
    if remainder != 0:
        dt += timedelta(minutes=(30 - remainder))
    return dt

async def select_time(update: Update, context: CallbackContext, selected_date_str: str = None) -> None:
    """Функція для вибору часу після вибору дати."""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id

    if not selected_date_str:
        # data: "date_ДД.ММ.РРРР"
        selected_date_str = query.data.split("_", 1)[1]

    if user_id not in user_selection or "service_id" not in user_selection[user_id]:
        logging.error(f"🚨 ПОМИЛКА! У користувача {user_id} немає вибраної послуги! user_selection: {user_selection}")
        await safe_edit_message_text(query, "⚠️ Послуга не вибрана. Спробуйте ще раз.")
        return

    service_id = user_selection[user_id]["service_id"]
    service_data = services_cache.get(service_id, {})
    service_name = service_data.get("name", "Послуга")
    service_duration = service_data.get("duration", 30)

    user_selection[user_id]["date"] = selected_date_str
    date_obj = datetime.strptime(selected_date_str, "%d.%m.%Y").date()
    service_working_hours = get_working_hours(date_obj, service_name)

    if not service_working_hours:
        keyboard = [[InlineKeyboardButton("Обрати іншу дату 📅", callback_data="select_date")]]
        await safe_edit_message_text(
            query,
            "⛔ На цю дату вихідний. Оберіть іншу дату!",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    times = []
    booked_for_date = booked_slots.get(selected_date_str, [])
    slots_needed = service_duration // 30

    # Якщо бронювання на сьогодні, встановлюємо буфер 2 години і округлюємо до наступного 30-хвилинного слоту
    now = datetime.now()
    is_today = (date_obj == now.date())
    if is_today:
        min_booking_dt = round_up_to_next_slot(now + timedelta(hours=2))
    else:
        min_booking_dt = None

    for interval in service_working_hours:
        interval_start = safe_strptime(interval[0], "%H:%M")
        interval_end = safe_strptime(interval[1], "%H:%M")
        if not interval_start or not interval_end:
            continue

        # Створюємо datetime для даної дати
        interval_start_dt = datetime.combine(date_obj, interval_start.time())
        interval_end_dt = datetime.combine(date_obj, interval_end.time())

        if is_today:
            current_interval_start = max(interval_start_dt, min_booking_dt)
        else:
            current_interval_start = interval_start_dt

        adjusted_end = interval_end_dt - timedelta(minutes=(slots_needed - 1) * 30)
        current_time = current_interval_start
        while current_time <= adjusted_end:
            time_str = current_time.strftime("%H:%M")
            slot_times = [
                (current_time + timedelta(minutes=30 * i)).strftime("%H:%M")
                for i in range(slots_needed)
            ]
            if all(t not in booked_for_date for t in slot_times):
                times.append(time_str)
            current_time += timedelta(minutes=30)

    if not times:
        await safe_edit_message_text(query, "⛔ На цю дату немає вільного часу. Оберіть іншу дату!")
        return

    times.sort()
    keyboard = []
    for i in range(0, len(times), 2):
        row = [InlineKeyboardButton(times[i], callback_data=f"time_{selected_date_str}_{times[i]}")]
        if i + 1 < len(times):
            row.append(InlineKeyboardButton(times[i+1], callback_data=f"time_{selected_date_str}_{times[i+1]}"))
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="select_date")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await safe_edit_message_text(query, text="🕒 Оберіть час:", reply_markup=reply_markup)

# ========= ПІДТВЕРДЖЕННЯ ВИБОРУ (для запису) =========
async def confirm_selection(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    _, selected_date, selected_time = query.data.split("_", 2)

    service_id = user_selection[user_id].get("service_id", None)
    date = user_selection[user_id].get("date", None)
    if not service_id or not date:
        await query.answer()
        await safe_edit_message_text(query, "Виникла помилка, спробуйте ще раз.")
        return

    service_data = services_cache.get(service_id, {})
    service_name = service_data.get("name", "Послуга")
    price = service_data.get("price", "0 євро")
    duration = service_data.get("duration", 30)

    user_selection[user_id]["time"] = selected_time

    message = (
        f"Ви вибрали:\n"
        f"------------------------------------\n"
        f"Послуга: {service_name}\n"
        f"Дата: {date}\n"
        f"Час: {selected_time}\n"
        f"Загальна вартість: {price}\n"
        f"------------------------------------\n"
        f"Якщо все правильно, натисніть «ПІДТВЕРДИТИ»"
    )

    keyboard = [
        [
            InlineKeyboardButton("ПІДТВЕРДИТИ", callback_data="proceed"),
            InlineKeyboardButton("ВІДМІНИТИ", callback_data="cancel"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.answer()
    await safe_edit_message_text(query, text=message, reply_markup=reply_markup)


async def show_count_periods(update: Update, context: CallbackContext):
    keyboard = [
        [
            InlineKeyboardButton("14 днів", callback_data="stat_count_14"),
            InlineKeyboardButton("1 місяць", callback_data="stat_count_30")
        ],
        [
            InlineKeyboardButton("3 місяці", callback_data="stat_count_90"),
            InlineKeyboardButton("6 місяців", callback_data="stat_count_180")
        ],
        [
            InlineKeyboardButton("9 місяці", callback_data="stat_count_270"),
            InlineKeyboardButton("12 місяці", callback_data="stat_count_365")
        ],
        [
            InlineKeyboardButton("Custom", callback_data="stat_count_custom"),
            InlineKeyboardButton("Назад", callback_data="stat_menu")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("Оберіть період для звіту *Кількість записів*:", reply_markup=reply_markup, parse_mode="MarkdownV2")
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, "Оберіть період для звіту *Кількість записів*:", reply_markup=reply_markup, parse_mode="MarkdownV2")
        
async def show_avg_periods(update: Update, context: CallbackContext):
    keyboard = [
        [
            InlineKeyboardButton("14 днів", callback_data="stat_avg_14"),
            InlineKeyboardButton("1 місяць", callback_data="stat_avg_30")
        ],
        [
            InlineKeyboardButton("3 місяці", callback_data="stat_avg_90"),
            InlineKeyboardButton("6 місяці", callback_data="stat_avg_180")
        ],
        [
            InlineKeyboardButton("9 місяці", callback_data="stat_avg_270"),
            InlineKeyboardButton("12 місяці", callback_data="stat_avg_365")
        ],
        [
            InlineKeyboardButton("Custom", callback_data="stat_avg_custom"),
            InlineKeyboardButton("Назад", callback_data="stat_menu")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("Оберіть період для звіту *Середній чек*:", reply_markup=reply_markup, parse_mode="MarkdownV2")
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, "Оберіть період для звіту *Середній чек*:", reply_markup=reply_markup, parse_mode="MarkdownV2")



async def send_user_orders(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    orders_ref = (
        db.collection("orders")
        .where("user_id", "==", user_id)
        .where("status", "==", "confirmed")
    )

    orders = [doc.to_dict() for doc in orders_ref.stream()]
    if not orders:
        await safe_edit_message_text(
            query,
            "У вас немає підтверджених замовлень.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("НАЗАД", callback_data="back_to_history_menu")]]
            ),
        )
        return

    message = "Це всі ваші підтверджені замовлення:"
    for data in orders:
        product = data.get("product", "Товар не вказаний")
        price = data.get("price", "Ціна не вказана")
        order_info = (
            f"\n------------------------------------\n"
            f"Товар: {product}\n"
            f"Ціна: {price}\n"
        )
        if len(message) + len(order_info) > 3500:
            await safe_edit_message_text(
                query,
                message,
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("НАЗАД", callback_data="back_to_history_menu")]]
                ),
            )
            message = order_info
            await context.bot.send_message(chat_id=user_id, text=message)
        else:
            message += order_info

    keyboard = [[InlineKeyboardButton("НАЗАД", callback_data="back_to_history_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(query, message, reply_markup=reply_markup)

# ========= АДМІН ІСТОРІЯ =========

async def show_clients(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("У вас немає прав для цього розділу.")
        return

    users_collection = db.collection("users").stream()
    users_data = list(users_collection)
    if not users_data:
        await update.message.reply_text("Наразі немає зареєстрованих користувачів.")
        return

    message = "Список клієнтів:\n"
    for doc in users_data:
        user_info = doc.to_dict()
        username = user_info.get("username", f"UserID_{user_info.get('user_id')}")
        full_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip()
        display_name = f"{username} ({full_name})" if full_name else username

        message += f"- {display_name}\n"
        if len(message) > 3500:
            await update.message.reply_text(message)
            message = ""

    if message:
        await update.message.reply_text(message)


# ========= 3. РОЗКЛАД (📆 Розклад) =========
async def show_schedule_menu(update: Update, context: CallbackContext):
    query = None
    if update.callback_query:
        query = update.callback_query
        if update.effective_user.id not in ADMIN_USER_IDS:
            await query.answer("У вас немає прав для цього розділу.", show_alert=True)
            return
        await query.answer()
    else:
        if update.effective_user.id not in ADMIN_USER_IDS:
            await update.message.reply_text("У вас немає прав для цього розділу.")
            return

    keyboard = [
        [InlineKeyboardButton("📅 Однаковий графік", callback_data="schedule_same_for_week")],
        [InlineKeyboardButton("📆 Графік по днях", callback_data="schedule_daily")],
        [InlineKeyboardButton("🛑 Вихідні", callback_data="schedule_days_off")],
        [InlineKeyboardButton("🕒 Дата й час", callback_data="schedule_edit_date_range")],
        [InlineKeyboardButton("👀 Розклад", callback_data="schedule_view")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_menu")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "Налаштування розкладу:"
    if query:
        await safe_edit_message_text(query, text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)

def validate_time_range(time_range_str):
    pattern = r"^\d{2}:\d{2}-\d{2}:\d{2}$"
    if re.match(pattern, time_range_str):
        start, end = time_range_str.split("-")
        try:
            datetime.strptime(start, "%H:%M")
            datetime.strptime(end, "%H:%M")
            return True
        except ValueError:
            return False
    return False

def validate_date_range_input(text: str):
    """
    Приклад: "12.07.2025-15.07.2025 10:00-20:00" або "12.07.2025-15.07.2025 вихідний".
    """
    parts = text.split(" ")
    if len(parts) not in [1, 2]:
        return None

    date_range_part = parts[0]
    if "-" not in date_range_part:
        return None

    start_date_str, end_date_str = date_range_part.split("-")
    try:
        start_date = datetime.strptime(start_date_str.strip(), "%d.%m.%Y").date()
        end_date = datetime.strptime(end_date_str.strip(), "%d.%m.%Y").date()
    except ValueError:
        return None

    if start_date > end_date:
        return None

    if len(parts) == 1:
        return None  # немає інфи про час чи "вихідний"

    time_part = parts[1].lower().strip()
    if time_part == "вихідний":
        return (start_date, end_date, True, None, None)
    else:
        if validate_time_range(time_part):
            s, e = time_part.split("-")
            return (start_date, end_date, False, s, e)
        else:
            return None

async def schedule_set_same_for_week(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    ADMIN_STATE[query.from_user.id] = "WAITING_FOR_SAME_SCHEDULE"
    text = (
        "Вкажіть час роботи для ВСЬОГО тижня у форматі 09:00-18:00\n"
        "або введіть 'вихідний', якщо всі дні мають бути вихідними."
    )
    await safe_edit_message_text(query, text)

async def schedule_set_daily(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    ADMIN_STATE[query.from_user.id] = "DAILY_SCHEDULE_SETUP"

    buttons = []
    for i in range(7):
        current_info = DEFAULT_WEEK_SCHEDULE[i]
        status = "✅" if not current_info["off"] else "❌"
        label = f"{status} {get_weekday_name_ua(i)} ({current_info['start']}-{current_info['end']})"
        buttons.append([InlineKeyboardButton(label, callback_data=f"daily_schedule_day_{i}")])

    buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_schedule_main")])
    reply_markup = InlineKeyboardMarkup(buttons)

    await safe_edit_message_text(query, "Оберіть день, який хочете змінити:", reply_markup=reply_markup)

async def schedule_set_days_off(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    ADMIN_STATE[query.from_user.id] = "DAYS_OFF_SETUP"
    await show_days_off_menu(query, context)

def build_days_off_keyboard():
    buttons = []
    for i in range(7):
        off = DEFAULT_WEEK_SCHEDULE[i]["off"]
        day_str = get_weekday_name_ua(i)
        prefix = "✅" if off else "❌"
        btn_label = f"{prefix} {day_str}"
        buttons.append([InlineKeyboardButton(btn_label, callback_data=f"toggle_day_off_{i}")])
    buttons.append([InlineKeyboardButton("Зберегти зміни", callback_data="finish_set_days_off")])
    buttons.append([InlineKeyboardButton("Назад", callback_data="back_to_schedule_main")])
    return InlineKeyboardMarkup(buttons)

async def show_days_off_menu(query, context):
    text = "Оберіть дні, які будуть вихідними (дефолт). Натисніть, щоб змінити статус."
    reply_markup = build_days_off_keyboard()
    await safe_edit_message_text(query, text, reply_markup=reply_markup)

async def schedule_edit_date_range(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    ADMIN_STATE[query.from_user.id] = "EDIT_DATE_RANGE"
    await safe_edit_message_text(
        query,
        "Введіть діапазон дат та час у форматі DD.MM.YYYY-DD.MM.YYYY 09:00-18:00\n"
        "або DD.MM.YYYY-DD.MM.YYYY вихідний.\n\n"
        "Приклад: 12.07.2025-15.07.2025 10:00-20:00"
    )

async def schedule_view(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    today = datetime.now().date()
    text = "📅 *Поточний графік (найближчі 10 днів):*\n\n"

    keyboard = []
    for i in range(10):
        d = today + timedelta(days=i)
        date_str = d.strftime("%d.%m.%Y")
        weekday_str = get_weekday_name_ua(d.weekday())

        custom_data = get_custom_schedule_for_date(date_str)
        if custom_data:
            if custom_data["off"]:
                hours = "❌ Вихідний"
            else:
                hours = f"{custom_data['start']} - {custom_data['end']}"
        else:
            default_info = DEFAULT_WEEK_SCHEDULE[d.weekday()]
            if default_info["off"]:
                hours = "❌ Вихідний"
            else:
                hours = f"{default_info['start']} - {default_info['end']}"

        button_text = f"📅 {date_str} | {weekday_str}: {hours}"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_schedule_{date_str}")])

    keyboard.append([InlineKeyboardButton("🏠 Головне меню", callback_data="back_to_admin_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await safe_edit_message_text(query, text, reply_markup=reply_markup)

# ========= НАЛАШТУВАННЯ ПОСЛУГ =========
async def show_services_settings_menu(update: Update, context: CallbackContext):
    if update.message:
        if update.effective_user.id not in ADMIN_USER_IDS:
            await update.message.reply_text("У вас немає прав для цього розділу.")
            return
        text = "Налаштування послуг:"
        keyboard = [
            [InlineKeyboardButton("➕ Додати/редагувати послугу", callback_data="service_add_edit_main")],
            [InlineKeyboardButton("💰 Змінити вартість послуги", callback_data="service_change_price")],
            [InlineKeyboardButton("✅ Доступність послуг", callback_data="service_toggle_active")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(text, reply_markup=reply_markup)
    else:
        query = update.callback_query
        await query.answer()
        text = "Налаштування послуг:"
        keyboard = [
            [InlineKeyboardButton("➕ Додати/редагувати послугу", callback_data="service_add_edit_main")],
            [InlineKeyboardButton("💰 Змінити вартість послуги", callback_data="service_change_price")],
            [InlineKeyboardButton("✅ Доступність послуг", callback_data="service_toggle_active")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_edit_message_text(query, text, reply_markup=reply_markup)

async def service_add_edit_main(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    services_list = list(services_cache.items())  # (doc_id, data)
    services_list.sort(key=lambda x: x[1].get("name", ""))

    keyboard = []
    for doc_id, data in services_list:
        service_name = data.get("name", "Без назви")
        keyboard.append(
            [InlineKeyboardButton(service_name, callback_data=f"service_edit_select_{doc_id}")]
        )

    keyboard.append([InlineKeyboardButton("➕ Додати нову послугу", callback_data="service_add_new")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_services_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "Виберіть послугу для редагування або додайте нову:"
    await safe_edit_message_text(query, text, reply_markup=reply_markup)

async def service_add_new(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    ADMIN_STATE[query.from_user.id] = "SERVICE_ADD_NEW_NAME"
    await safe_edit_message_text(query, "Введіть назву нової послуги:")

async def service_edit_select(update: Update, context: CallbackContext, doc_id: str):
    query = update.callback_query
    await query.answer()
    context.user_data["edit_service_id"] = doc_id
    ADMIN_STATE[query.from_user.id] = "SERVICE_EDIT_NAME"
    service_data = services_cache.get(doc_id, {})
    current_name = service_data.get("name", "Без назви")
    current_price = service_data.get("price", "0 євро")
    current_duration = service_data.get("duration", 30)
    text = (
        f"Поточна назва: {current_name}\n"
        f"Поточна ціна: {current_price}\n"
        f"Поточна тривалість: {current_duration} хв\n\n"
        f"Введіть **нову назву** послуги (або залиште порожнім, щоб не змінювати):"
    )
    await safe_edit_message_text(query, text, parse_mode="Markdown")

async def service_change_price_main(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    services_list = list(services_cache.items())
    services_list.sort(key=lambda x: x[1].get("name", ""))

    keyboard = []
    for doc_id, data in services_list:
        service_name = data.get("name", "Без назви")
        current_price = data.get("price", "0 євро")
        btn_text = f"{service_name} – {current_price}"
        keyboard.append(
            [InlineKeyboardButton(btn_text, callback_data=f"service_price_select_{doc_id}")]
        )

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_services_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(query, "Оберіть послугу для зміни ціни:", reply_markup=reply_markup)

async def service_toggle_active_main(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    services_list = list(services_cache.items())
    services_list.sort(key=lambda x: x[1].get("name", ""))

    keyboard = []
    for doc_id, data in services_list:
        name = data.get("name", "Без назви")
        active = data.get("active", False)
        btn_text = f"{'✅' if active else '❌'} {name}"
        keyboard.append(
            [InlineKeyboardButton(btn_text, callback_data=f"service_toggle_active_{doc_id}")]
        )

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_services_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "Натисніть, щоб змінити статус активності:"
    await safe_edit_message_text(query, text, reply_markup=reply_markup)

def create_service_in_db(name: str, price: str, duration: int):
    doc_ref = db.collection("services").document()
    doc_id = doc_ref.id
    doc_ref.set({
        "name": name,
        "price": price,
        "duration": duration,
        "active": True
    })
    refresh_services_cache()
    return doc_id

def update_service_in_db(doc_id: str, new_data: dict):
    doc_ref = db.collection("services").document(doc_id)
    doc_ref.update(new_data)
    refresh_services_cache()


# ========= НАГАДУВАННЯ (JOB QUEUE) =========
async def send_reminders(context: CallbackContext):
    now = datetime.now()
    bookings_ref = db.collection("bookings").where("status", "==", "confirmed")
    bookings = bookings_ref.stream()

    for booking in bookings:
        data = booking.to_dict()
        user_id_ = data.get("user_id")
        date_str = data.get("date")
        time_str = data.get("time")
        service = data.get("service", "Послуга")
        price = data.get("price", "Ціна")

        appointment_datetime = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
        time_until_appointment = appointment_datetime - now

        # Нагадування користувачу (24 год і 2 год)
        if 23.5 * 3600 <= time_until_appointment.total_seconds() <= 24.5 * 3600:
            message = (
                f"⏰Нагадування: Ваш запис на завтра!\n"
                f"------------------------------------\n"
                f"Послуга: {service}\n"
                f"Дата: {date_str}\n"
                f"Час: {time_str}\n"
                f"Ціна: {price}\n"
            )
            try:
                await context.bot.send_message(chat_id=user_id_, text=message)
            except Exception as e:
                logger.error(f"Не вдалося надіслати нагадування (24 години) користувачу {user_id_}: {e}")

        elif 1.5 * 3600 <= time_until_appointment.total_seconds() <= 2.5 * 3600:
            message = (
                f"⏰Нагадування: Ваш запис через 2 години!\n"
                f"------------------------------------\n"
                f"Послуга: {service}\n"
                f"Дата: {date_str}\n"
                f"Час: {time_str}\n"
                f"Ціна: {price}\n"
            )
            try:
                await context.bot.send_message(chat_id=user_id_, text=message)
            except Exception as e:
                logger.error(f"Не вдалося надіслати нагадування (2 години) користувачу {user_id_}: {e}")

        # Нагадування адмінам (5 год і 2 год)
        for admin_id in ADMIN_USER_IDS:
            if 4.5 * 3600 <= time_until_appointment.total_seconds() <= 5.5 * 3600:
                admin_msg = (
                    f"⏰Нагадування: Запис через 5 годин!\n"
                    f"------------------------------------\n"
                    f"Послуга: {service}\n"
                    f"Дата: {date_str}\n"
                    f"Час: {time_str}\n"
                    f"Користувач: {data.get('username', 'Користувач')}\n"
                )
                try:
                    await context.bot.send_message(chat_id=admin_id, text=admin_msg)
                except Exception as e:
                    logger.error(f"Не вдалося надіслати нагадування (5 годин) адміну {admin_id}: {e}")

            elif 1.5 * 3600 <= time_until_appointment.total_seconds() <= 2.5 * 3600:
                admin_msg = (
                    f"⏰Нагадування: Запис через 2 години!\n"
                    f"------------------------------------\n"
                    f"Послуга: {service}\n"
                    f"Дата: {date_str}\n"
                    f"Час: {time_str}\n"
                    f"Користувач: {data.get('username', 'Користувач')}\n"
                )
                try:
                    await context.bot.send_message(chat_id=admin_id, text=admin_msg)
                except Exception as e:
                    logger.error(f"Не вдалося надіслати нагадування (2 години) адміну {admin_id}: {e}")


# ========= МЕНЮ «✉️ Повідомлення» (розсилка) =========
async def show_messages(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("У вас немає прав для цього розділу.")
        return

    keyboard = [
        [
            InlineKeyboardButton(
                "Надіслати повідомлення всім клієнтам (розсилка)",
                callback_data="broadcast_message"
            )
        ],
        [InlineKeyboardButton("НАЗАД", callback_data="back_to_admin_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Меню повідомлень:", reply_markup=reply_markup)

async def bot_settings(update: Update, context: CallbackContext):
    if update.effective_user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("У вас немає прав для цього розділу.")
        return

    keyboard = [[InlineKeyboardButton("НАЗАД", callback_data="back_to_admin_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Налаштування бота:", reply_markup=reply_markup)

async def broadcast_message_button(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if user_id in ADMIN_USER_IDS:
        admin_broadcast_mode[user_id] = True
        await safe_edit_message_text(
            query,
            "Введіть повідомлення для розсилки або /cancel для відміни."
        )
    else:
        await safe_edit_message_text(query, "У вас немає прав для цього.")

# ========== НОВИЙ ФУНКЦІОНАЛ: ПІДМЕНЮ "Записи" ДЛЯ АДМІНА =========
async def admin_bookings_menu(update: Update, context: CallbackContext):
    query = None
    if update.callback_query:
        query = update.callback_query
        await query.answer()
    if update.message:
        if update.effective_user.id not in ADMIN_USER_IDS:
            await update.message.reply_text("У вас немає прав для цього розділу.")
            return
        keyboard = [
            [InlineKeyboardButton("🗓️ Сьогоднішні записи", callback_data="admin_today_bookings")],
            [InlineKeyboardButton("📆 Записи на обрану дату", callback_data="admin_pick_date_for_bookings")],
            [InlineKeyboardButton("❌ Скасування запису", callback_data="admin_cancel_booking_main")],
            [InlineKeyboardButton("📜 Історія записів", callback_data="admin_records_menu")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Меню «Записи»:", reply_markup=reply_markup)
    else:
        keyboard = [
            [InlineKeyboardButton("🗓️ Сьогоднішні записи", callback_data="admin_today_bookings")],
            [InlineKeyboardButton("📆 Записи на обрану дату", callback_data="admin_pick_date_for_bookings")],
            [InlineKeyboardButton("❌ Скасування запису", callback_data="admin_cancel_booking_main")],
            [InlineKeyboardButton("📜 Історія записів", callback_data="admin_records_menu")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_edit_message_text(query, "Меню «Записи»:", reply_markup=reply_markup)

async def admin_today_bookings(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    today_str = datetime.now().strftime("%d.%m.%Y")
    bookings_ref = db.collection("bookings").where("date","==", today_str).stream()
    bookings_list = []
    for doc in bookings_ref:
        data = doc.to_dict()
        status = data.get("status")
        if status in ["pending","confirmed"]:
            bookings_list.append((doc.id, data))

    if not bookings_list:
        await safe_edit_message_text(
            query,
            "Сьогодні немає підтверджених/в процесі записів.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")]
            ])
        )
        return

    # (// CHANGE) зберігаємо список у context для пагінації
    context.user_data["current_records"] = bookings_list
    # Викликаємо нашу пагінацію
    await display_records_list(
        update,
        context,
        bookings_list,
        "admin_bookings_main",
        page=0,
        page_size=10
    )



    keyboard = []
    text_header = "📅 *Сьогоднішні записи*:\n"
    for booking_id, data in bookings_list:
        username = data.get("username", "Невідомо")
        time_ = data.get("time", "--:--")
        service_ = data.get("service", "Послуга")
        btn_text = f"{username} - {time_} - {service_}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"admin_booking_details_{booking_id}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(query, text_header, reply_markup=reply_markup, parse_mode="Markdown")

async def admin_pick_date_for_bookings(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    ADMIN_STATE[query.from_user.id] = "ADMIN_WAITING_DATE_FOR_BOOKINGS"
    await safe_edit_message_text(
        query,
        "Введіть дату у форматі ДД.ММ.РРРР, щоб переглянути записи:"
    )

async def admin_show_bookings_for_date(date_str: str, update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    query = None
    if update.callback_query:
        query = update.callback_query

    bookings_ref = db.collection("bookings").where("date", "==", date_str).stream()
    bookings_list = [...]  # Відібрані за датою
    await display_records_list(update, context, bookings_list, return_callback="admin_bookings_main", page=0, page_size=10)
    now = datetime.now()

    for doc in bookings_ref:
        data = doc.to_dict()
        status = data.get("status")
        if status not in ["pending", "confirmed"]:
            continue
        dt_str = f"{data.get('date')} {data.get('time')}"
        try:
            dt_obj = datetime.strptime(dt_str, "%d.%m.%Y %H:%M")
            if dt_obj >= now:
                bookings_list.append((doc.id, data))
        except:
            pass

    if not bookings_list:
        text = f"На {date_str} немає (pending/confirmed) актуальних записів."
        if query:
            await safe_edit_message_text(
                query,
                text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")]
                ])
            )
        else:
            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")]
                ])
            )
        return

    keyboard = []
    text_header = f"📅 Записи на {date_str}:\n"
    for booking_id, data in bookings_list:
        username = data.get("username", "Невідомо")
        time_ = data.get("time", "--:--")
        service_ = data.get("service", "Послуга")
        btn_text = f"{username} - {time_} - {service_}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"admin_booking_details_{booking_id}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    if query:
        await safe_edit_message_text(query, text_header, reply_markup=reply_markup)
    else:
        await context.bot.send_message(chat_id=user_id, text=text_header, reply_markup=reply_markup)

async def admin_booking_details(update: Update, context: CallbackContext, booking_id: str):
    query = update.callback_query
    await query.answer()

    doc_ref = db.collection("bookings").document(booking_id)
    doc = doc_ref.get()
    if not doc.exists:
        await safe_edit_message_text(query, "Запис не знайдено або видалено.")
        return

    data = doc.to_dict()
    user_name = data.get("username", "Невідомий")
    service_ = data.get("service", "Послуга")
    date_ = data.get("date", "--.--.----")
    time_ = data.get("time", "--:--")
    price_ = data.get("price", "N/A")
    status_ = data.get("status", "pending/confirmed?")

    detail_text = (
        f"👤 *Користувач:* {user_name}\n"
        f"💇 *Послуга:* {service_}\n"
        f"📅 *Дата:* {date_}\n"
        f"⏰ *Час:* {time_}\n"
        f"💵 *Ціна:* {price_}\n"
        f"📌 *Статус:* {status_}"
    )

    keyboard = [
        [InlineKeyboardButton("📩 Надіслати сповіщення", callback_data=f"admin_notify_booking_{booking_id}")],
        [InlineKeyboardButton("❌ Скасувати запис", callback_data=f"admin_cancel_booking_{booking_id}")],
        [InlineKeyboardButton("🔙 Назад", callback_data=f"admin_show_bookings_for_date_{date_}")]

    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(query, detail_text, reply_markup=reply_markup, parse_mode="Markdown")

async def admin_notify_booking(update: Update, context: CallbackContext, booking_id: str):
    query = update.callback_query
    await query.answer()
    ADMIN_STATE[query.from_user.id] = f"ADMIN_SEND_NOTIFICATION_{booking_id}"
    await safe_edit_message_text(
        query,
        "Введіть текст сповіщення для клієнта або /cancel щоб скасувати:"
    )

async def process_admin_send_notification(user_id: int, text: str, context: CallbackContext):
    state = ADMIN_STATE.get(user_id, "")
    if not state.startswith("ADMIN_SEND_NOTIFICATION_"):
        return

    booking_id = state.split("_")[-1]
    booking_ref = db.collection("bookings").document(booking_id)
    doc = booking_ref.get()
    if not doc.exists:
        return

    booking_data = doc.to_dict()
    client_id = booking_data.get("user_id")
    if not client_id:
        return

    try:
        await context.bot.send_message(chat_id=client_id, text=f"📩 Повідомлення від адміністратора:\n{text}")
    except Exception as e:
        logger.error(f"Не вдалося відправити повідомлення користувачу {client_id}: {e}")

    ADMIN_STATE[user_id] = None

async def admin_cancel_booking(update: Update, context: CallbackContext, booking_id: str):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [
            InlineKeyboardButton("✅ Так", callback_data=f"admin_confirm_cancel_{booking_id}"),
            InlineKeyboardButton("❌ Ні", callback_data="admin_bookings_main")
        ]
    ]
    text = "Чи дійсно ви хочете скасувати цей запис?"
    await safe_edit_message_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_confirm_cancel(update: Update, context: CallbackContext, booking_id: str):
    query = update.callback_query
    await query.answer()
    ADMIN_STATE[query.from_user.id] = f"ADMIN_CANCEL_BOOKING_REASON_{booking_id}"
    await safe_edit_message_text(query, "Введіть причину скасування (або /cancel для відміни):")

async def process_admin_cancel_reason(user_id: int, reason_text: str, context: CallbackContext):
    state = ADMIN_STATE.get(user_id, "")
    if not state.startswith("ADMIN_CANCEL_BOOKING_REASON_"):
        return

    booking_id = state.split("_")[-1]
    booking_ref = db.collection("bookings").document(booking_id)
    doc = booking_ref.get()
    if not doc.exists:
        return

    booking_data = doc.to_dict()
    date_ = booking_data.get("date")
    time_ = booking_data.get("time")
    duration_ = booking_data.get("duration", 30)
    booked_for_date = booked_slots.get(date_, [])

    # Звільняємо слоти
    start_time = datetime.strptime(time_, "%H:%M")
    slots_needed = duration_ // 30
    for i in range(slots_needed):
        t_str = (start_time + timedelta(minutes=30*i)).strftime("%H:%M")
        if t_str in booked_for_date:
            booked_for_date.remove(t_str)
    booked_slots[date_] = booked_for_date

    booking_ref.update({"status": "canceled"})

    client_id = booking_data.get("user_id")
    service_ = booking_data.get("service", "Послуга")
    msg = (
        f"❌ Ваш запис було *скасовано* адміністратором!\n"
        f"------------------------------------\n"
        f"Послуга: {service_}\n"
        f"Дата: {date_}\n"
        f"Час: {time_}\n"
        f"Причина: {reason_text}"
    )
    try:
        await context.bot.send_message(chat_id=client_id, text=msg, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Не вдалося відправити повідомлення про скасування користувачу {client_id}: {e}")

    ADMIN_STATE[user_id] = None

async def admin_cancel_booking_main(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    now = datetime.now()
    upper_limit = now + timedelta(days=30)
    bookings_ref = db.collection("bookings").stream()

    upcoming_bookings = []
    for doc in bookings_ref:
        data = doc.to_dict()
        status = data.get("status")
        if status not in ["pending", "confirmed"]:
            continue
        date_str = data.get("date")
        time_str = data.get("time")
        try:
            dt = datetime.strptime(date_str + " " + time_str, "%d.%m.%Y %H:%M")
            if now <= dt <= upper_limit:
                upcoming_bookings.append((doc.id, data))
        except:
            pass

    if not upcoming_bookings:
        await safe_edit_message_text(
            query,
            "Немає найближчих (підтверджених/в процесі) записів на 30 днів уперед.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")]
            ])
        )
        return

    keyboard = []
    text_header = "Оберіть запис для скасування:"
    for booking_id, data in upcoming_bookings:
        user_ = data.get("username", "??")
        date_ = data.get("date", "--.--.----")
        time_ = data.get("time", "--:--")
        serv_ = data.get("service", "Послуга")
        btn_text = f"{date_} {time_} | {user_} | {serv_}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"admin_cancel_booking_{booking_id}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")])
    await safe_edit_message_text(
        query,
        text_header,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
async def service_details_menu(update: Update, context: CallbackContext, doc_id: str):
    # Якщо виклик із callback, використовуємо його, інакше - повідомлення
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        send_method = query.edit_message_text
    else:
        send_method = update.message.reply_text

    service_data = services_cache.get(doc_id, {})
    name = service_data.get("name", "Без назви")
    price = service_data.get("price", "0 євро")
    duration = service_data.get("duration", "N/A")
    text = (
        f"Детальна інформація про послугу:\n\n"
        f"Назва: {name}\n"
        f"Ціна: {price}\n"
        f"Тривалість: {duration} хв"
    )
    keyboard = [
        [InlineKeyboardButton("Редагувати", callback_data=f"service_edit_{doc_id}")],
        [InlineKeyboardButton("Видалити", callback_data=f"service_delete_{doc_id}")],
        [InlineKeyboardButton("Назад", callback_data="service_add_edit_main")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await send_method(text=text, reply_markup=reply_markup)

    
async def service_edit_menu(update: Update, context: CallbackContext, doc_id: str):
    """
    Відображає меню редагування з кнопками для зміни назви, ціни або тривалості.
    """
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("Назва", callback_data=f"edit_name_{doc_id}")],
        [InlineKeyboardButton("Ціна", callback_data=f"edit_price_{doc_id}")],
        [InlineKeyboardButton("Тривалість", callback_data=f"edit_duration_{doc_id}")],
        [InlineKeyboardButton("Назад", callback_data=f"service_edit_select_{doc_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(query, "Оберіть, що редагувати:", reply_markup=reply_markup)

    
async def service_delete_confirm(update: Update, context: CallbackContext, doc_id: str):
    """
    Відображає підтвердження видалення послуги із детальною інформацією.
    """
    query = update.callback_query
    await query.answer()
    service_data = services_cache.get(doc_id, {})
    name = service_data.get("name", "Без назви")
    price = service_data.get("price", "0 євро")
    duration = service_data.get("duration", "N/A")
    text = f"Ви дійсно хочете видалити наступну послугу?\n\n" \
           f"Назва: {name}\nЦіна: {price}\nТривалість: {duration} хв"
    keyboard = [
        [InlineKeyboardButton("Так", callback_data=f"confirm_delete_{doc_id}")],
        [InlineKeyboardButton("Ні", callback_data=f"service_edit_select_{doc_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(query, text, reply_markup=reply_markup)
    
async def service_add_edit_main(update: Update, context: CallbackContext):
    """
    Відображає список послуг для редагування або додавання нової.
    """
    query = update.callback_query
    await query.answer()
    services_list = list(services_cache.items())  # (doc_id, data)
    services_list.sort(key=lambda x: x[1].get("name", ""))
    keyboard = []
    for doc_id, data in services_list:
        service_name = data.get("name", "Без назви")
        # При натисканні передаємо doc_id в callback_data
        keyboard.append([InlineKeyboardButton(service_name, callback_data=f"service_edit_select_{doc_id}")])
    keyboard.append([InlineKeyboardButton("➕ Додати нову послугу", callback_data="service_add_new")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_services_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "Виберіть послугу для редагування або додайте нову:"
    await safe_edit_message_text(query, text, reply_markup=reply_markup)


# 1. Головне меню "Записи"# 1. Головне меню "Записи" (Records Menu)
async def show_records_menu(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        
    
        # Меню для адміністратора
        keyboard = [
            [InlineKeyboardButton("Підтверджені", callback_data="records_confirmed")],
            [InlineKeyboardButton("Відхилені", callback_data="records_rejected")],
            [InlineKeyboardButton("Минулі", callback_data="records_past")],
        ]
    else:
        # Меню для клієнтів
        keyboard = [
            [InlineKeyboardButton("ПІДТВЕРДЖЕНІ", callback_data="client_confirmed_bookings")],
            [InlineKeyboardButton("В ПРОЦЕСІ", callback_data="client_pending_bookings")],
            [InlineKeyboardButton("МИНУЛІ", callback_data="client_past_bookings")],
            [InlineKeyboardButton("НАЗАД", callback_data="back_to_main_menu_text")]
        ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("Оберіть тип історії записів:", reply_markup=reply_markup)
    else:
        await safe_edit_message_text(update.callback_query, "Оберіть тип історії записів:", reply_markup=reply_markup)

# 2. Функції для отримання записів із бази даних

async def show_records(update: Update, context: CallbackContext, status: str = None, date: str = None):
    query = update.callback_query
    user_id = query.from_user.id
    
    data = query.data
    if status is None and data.startswith("records_"):
        status_from_data = data.split("_", 1)[1]  # "confirmed", "rejected" або "past"
        # Далі можна підмінити локальну змінну:
        status = status_from_data

    if status:

        if status:
            bookings = db.collection("bookings").where("status", "==", status).order_by("date", direction=firestore.Query.DESCENDING).limit(10).stream()
        elif date:
            bookings = db.collection("bookings").where("date", "==", date).order_by("time", direction=firestore.Query.DESCENDING).limit(10).stream()
        else:
            await query.answer("Невірний запит.")
            return

    records = [doc.to_dict() for doc in bookings]

    if not records:
        await query.edit_message_text("Немає записів для відображення.")
        return

    message = "Записи:\n"
    message += "ID".ljust(15) + "Користувач".ljust(20) + "Послуга".ljust(25) + "Дата".ljust(15) + "Час".ljust(10) + "Ціна".ljust(10) + "Статус\n"
    message += "=" * 95 + "\n"
    for record in records:
        message += f"{record['booking_id'][:14].ljust(15)}{record['username'][:19].ljust(20)}{record['service'][:24].ljust(25)}{record['date'].ljust(15)}{record['time'].ljust(10)}{str(record['price']).ljust(10)}{record['status']}\n"

    keyboard = [
        [InlineKeyboardButton("Назад", callback_data="back_to_records_menu")],
        InlineKeyboardButton("Всі записи", callback_data=f"all_records_{status if status else (date if date else 'all')}")
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(message, reply_markup=reply_markup)
    
from fpdf import FPDF
import os
import time
import logging

from fpdf import FPDF
import os
import time
import logging

logger = logging.getLogger(__name__)

async def send_filtered_records_pdf(update, context, status_filter=None):
    query = update.callback_query
    if status_filter is None:
        data = query.data.split("_")
        if len(data) > 2:
            status_filter = data[2]
        else:
            await query.answer("Невірний формат запиту!", show_alert=True)
            return


@blacklist_protected
async def send_filtered_records_pdf(update: Update, context: CallbackContext, status_filter=None):

    # Дістаємо status_filter з callback_data
    query = update.callback_query
    data = query.data  # типово, наприклад: "all_records_confirmed"

    # Тепер парсимо статус (припустімо, формат "all_records_назваСтатусу"):
    parts = data.split("_", 2)  # ["all", "records", "confirmed"]
    if len(parts) < 3:
        await query.answer("Невірний callback_data (немає статусу)!")
        return

    status_filter = parts[2]  # "confirmed" / "past" / "rejected" / будь‐що інше
    ...
    # решта логіки для PDF


    status_filter = parts[2]  # "confirmed", "rejected", "past" тощо

    # Далі йде логіка генерації PDF:
    # ---------------------------------------------------------------
    CACHE_FILE = f"bookings_{status_filter}.pdf"
    CACHE_DURATION = 600  # 10 хвилин

    # Визначаємо chat_id
    chat_id = None
    if update.message and update.message.chat_id:
        chat_id = update.message.chat_id
    elif update.callback_query and update.callback_query.message:
        chat_id = update.callback_query.message.chat_id

    if chat_id is None:
        logger.error("❌ Не вдалося визначити chat_id, PDF не буде відправлено.")
        return

    # Перевіряємо кеш (не старший за CACHE_DURATION секунд)
    import os, time
    if os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE)) < CACHE_DURATION:
        try:
            await context.bot.send_document(
                chat_id=chat_id,
                document=open(CACHE_FILE, "rb"),
                filename=CACHE_FILE,
                caption=f"📄 Записи зі статусом {status_filter} (кешований PDF)"
            )
            return
        except Exception as e:
            logger.error(f"❌ Не вдалося відправити кешований PDF: {e}")

    # Якщо кешу або він застарів – збираємо дані та формуємо PDF
    from fpdf import FPDF
    if status_filter == "past":
        # Наприклад, "past" – це всі дати до сьогодні
        today = datetime.today().strftime("%d.%m.%Y")
        bookings_cursor = db.collection("bookings").where("date", "<", today).stream()
    else:
        # Інакше фільтруємо просто за полем "status"
        bookings_cursor = db.collection("bookings").where("status", "==", status_filter).stream()

    records = [doc.to_dict() for doc in bookings_cursor]
    if not records:
        await context.bot.send_message(chat_id, f"⚠️ Немає записів зі статусом: {status_filter}.")
        return

    # Генеруємо PDF
    pdf = FPDF()
    pdf.add_page()
    font_path = r"C:\Users\reset\OneDrive\Робочий стіл\vps server\dejavu-fonts-ttf-2.37\ttf\DejaVuSansCondensed.ttf"
    pdf.add_font("DejaVu", "", "./DejaVuSansCondensed.ttf", uni=True)
    pdf.set_font("DejaVu", "", 10)


    pdf.cell(0, 10, f"Записи зі статусом {status_filter}", ln=True, align="C")
    pdf.ln(5)

    headers = ["Ім'я", "Послуга", "Дата", "Час", "Ціна"]
    col_widths = [40, 50, 25, 20, 20]
    pdf.set_font("DejaVu", "", 9)
    # Рядок заголовків
    for i, header in enumerate(headers):
        pdf.cell(col_widths[i], 8, header, border=1, align="C")
    pdf.ln()

    # Заповнюємо таблицю
    for b in records:
        row = [
            str(b.get("username", "-")),
            str(b.get("service", "-")),
            str(b.get("date", "--.--.----")),
            str(b.get("time", "--:--")),
            str(b.get("price", "-"))
        ]
        for i, item in enumerate(row):
            pdf.cell(col_widths[i], 8, item, border=1, align="C")
        pdf.ln()

    pdf.output(CACHE_FILE, "F")

    # Відправляємо документ
    try:
        await context.bot.send_document(
            chat_id=chat_id,
            document=open(CACHE_FILE, "rb"),
            filename=CACHE_FILE,
            caption=f"📄 Записи зі статусом {status_filter}"
        )
    except Exception as e:
        logger.error(f"❌ Не вдалося відправити PDF: {e}")
    # ---------------------------------------------------------------



async def handle_date_input(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if update.message:
        text = update.message.text.strip()
    else:
        text = update.callback_query.data.split("_")[-1]

    try:
        datetime.strptime(text, "%d.%m.%Y")
        await show_records(update, context, date=text)
    except ValueError:
        await update.message.reply_text("Невірний формат дати. Спробуйте ще раз у форматі ДД.ММ.РРРР.")

async def show_records_menu(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("Підтверджені", callback_data="records_confirmed")],
        [InlineKeyboardButton("Відхилені", callback_data="records_rejected")],
        [InlineKeyboardButton("Минулі", callback_data="records_past")],
        [InlineKeyboardButton("За датою", callback_data="records_by_date")],
        [InlineKeyboardButton("Назад", callback_data="back_to_main_menu_text")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "Історія записів:"

    if update.message:
        await update.message.reply_text(text, reply_markup=reply_markup)
    elif update.callback_query:
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except Exception as e:
            print(f"❌ Помилка редагування повідомлення: {e}")

# Для клієнта – фільтруємо по user_id
# Для адміністратора (фільтруємо всі записи за статусом)
async def get_admin_records(status_filter: str, past: bool) -> list:
    now = datetime.now()
    records = []
    if status:
        bookings = db.collection("bookings").where("status", "==", status).order_by("date", direction=firestore.Query.DESCENDING).limit(10).stream()
        for doc in db.collection("bookings").stream():
            data = doc.to_dict()
            try:
                record_dt = datetime.strptime(f"{data.get('date')} {data.get('time')}", "%d.%m.%Y %H:%M")
            except Exception:
                continue
            if status_filter in ["confirmed", "pending", "rejected"]:
                if data.get("status", "").lower() != status_filter:
                    continue
            if past:
                if record_dt < now:
                    records.append((doc.id, data))
            else:
                if record_dt >= now:
                    records.append((doc.id, data))
                return records

## Для адміністратора (фільтруємо всі записи за статусом)
async def get_admin_records(status_filter: str, past: bool) -> list:
    now = datetime.now()
    records = []
    for doc in db.collection("bookings").stream():
        data = doc.to_dict()
        try:
            record_dt = datetime.strptime(f"{data.get('date')} {data.get('time')}", "%d.%m.%Y %H:%M")
        except Exception:
            continue
        if status_filter in ["confirmed", "pending", "rejected"]:
            if data.get("status", "").lower() != status_filter:
                continue
        if past:
            if record_dt < now:
                records.append((doc.id, data))
        else:
            if record_dt >= now:
                records.append((doc.id, data))
    return records

# 3. Виведення списку записів із кнопками "ДЕТАЛІ" та "НАЗАД"from telegram import InlineKeyboardButton, InlineKeyboardMarkup



async def show_bookings_by_status(update: Update, context: CallbackContext, status: str = None):
    query = update.callback_query
    await query.answer()

    logger.info(f"🔍 Showing bookings with status: {status}")

    # Отримуємо всі записи з вибраним статусом
    bookings_ref = db.collection("bookings").where("status", "==", status).stream()
    bookings = [(doc.id, doc.to_dict()) for doc in bookings_ref]

    if not bookings:
        await safe_edit_message_text(query, f"😕 Немає записів зі статусом '{status}'.")
        return

    # Формуємо список кнопок
    keyboard = []
    text_header = f"📂 Записи зі статусом '{status}':\n"

    for doc_id, data in bookings:
        time_ = data.get("time", "--:--")
        date_ = data.get("date", "--.--.----")
        user_ = data.get("username", "???")
        service_ = data.get("service", "Послуга")

        btn_text = f"{date_} | {time_} | {user_} | {service_}"
        keyboard.append([
            InlineKeyboardButton(
                btn_text,
                callback_data=f"record_details_{doc_id}_status_{status}"
            )
        ])

    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await safe_edit_message_text(query, text_header, reply_markup=reply_markup)



async def send_admin_bookings(update: Update, context: CallbackContext, status: str, past: bool = False):
    """
    Shows the administrator a list of bookings with the given status and timeframe.
    :param update: The Telegram Update object.
    :param context: The Telegram CallbackContext object.
    :param status: The booking status to filter by (e.g., "confirmed", "rejected").
    :param past: If True, fetches past bookings; otherwise, fetches future bookings.
    """
    query = update.callback_query
    if query:
        await query.answer()
    else:
        # If invoked via message (rare), handle it accordingly
        pass

    # Fetch all bookings from Firestore
# Fetch all bookings from Firestore
    now = datetime.now()
    all_bookings = db.collection("bookings").stream()

    # Initialize the list outside the loop
    bookings = []

    for doc in all_bookings:
        data = doc.to_dict()
        # Filter based on the provided status (for example)
        if data.get("status", "").lower() == status.lower():
            try:
                record_dt = datetime.strptime(f"{data.get('date', '')} {data.get('time', '')}", "%d.%m.%Y %H:%M")
            except Exception:
                continue

            # Filter by timeframe (past or future)
            if past:
                if record_dt < now:
                    bookings.append((doc.id, data))
            else:
                if record_dt >= now:
                    bookings.append((doc.id, data))


    if not bookings:
        text = f"Немає записів зі статусом: {status}"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if query:
            await safe_edit_message_text(query, text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
        return

    # Build the list of buttons for each booking
    keyboard = []
    text_header = f"Записи зі статусом '{status}':\n"
    for doc_id, data in bookings:
        time_ = data.get("time", "--:--")
        date_ = data.get("date", "--.--.----")
        user_ = data.get("username", "???")
        service_ = data.get("service", "Послуга")
        btn_text = f"{date_} | {time_} | {user_} | {service_}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"view_record_details_{doc_id}_admin_bookings_main")])


    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query:
        await safe_edit_message_text(query, text_header, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text_header, reply_markup=reply_markup)

async def service_delete_service(update: Update, context: CallbackContext, doc_id: str):
    """
    Видаляє послугу з бази даних, оновлює кеш та повертає список послуг.
    """
    query = update.callback_query
    await query.answer()
    db.collection("services").document(doc_id).delete()
    refresh_services_cache()
    await safe_edit_message_text(query, "Послугу видалено.")
    await service_add_edit_main(update, context)

# ========= CALLBACK QUERY HANDLER =========
@blacklist_protected
async def button_handler(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    status = None  
    logger.info(f"⚡ Button clicked: {data}")
    
    if data.startswith("view_record_details_"):
        await view_record_details(update, context)
    elif data.startswith("record_details_"):  # Додаємо підтримку record_details_
        await view_record_details(update, context)
    elif data == "admin_bookings_main":
        await admin_bookings_menu(update, context)
        
        # -- Додано: обробка статистики ---
    if data.startswith("stat_") or data.startswith("stat_menu"):
        await statistics_callback_handler(update, context)
        return
    
    if data == "client_menu":
        await show_client_menu(update, context)
        return
    
        # --- ІСТОРІЯ адміна ---
    if data == "admin_records_menu":
        await show_records_menu(update, context)
        return
    
        # Обробка натискання "Отримати всі записи"
    if data == "get_all_records":
        # Показуємо меню вибору формату: TXT, PDF, або "Назад"
        keyboard = [
            [
                InlineKeyboardButton("TXT", callback_data="export_all_txt"),
                InlineKeyboardButton("PDF", callback_data="export_all_pdf"),
            ],
            [InlineKeyboardButton("Назад", callback_data="back_to_records_menu")]  # або куди треба повертати
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.answer()
        await safe_edit_message_text(query, "Оберіть формат файлу:", reply_markup=reply_markup)
        return

    # Якщо користувач вибрав TXT
    if data == "export_all_txt":
        await query.answer()
        await send_all_records_in_txt(query, context)
        return

    # Якщо користувач вибрав PDF
    if data == "export_all_pdf":
        await query.answer()
        await send_all_records_in_pdf(query, context)
        return

    # Якщо користувач тисне "Назад" на тій клавіатурі,
    # ви можете просто повернути його до "display_records_list" або в "admin_bookings_menu",
    # залежно від вашої логіки:
    if data == "back_to_records_menu":
        await admin_bookings_menu(update, context)  # або show_records_menu(update, context)
        return

    
    if data == "client_search_results":
        await show_client_search_results(update, context)
        return

    if data == "client_search":
        await client_search_prompt(update, context)
        return

    if data == "client_history_menu":
        await client_history_prompt(update, context)
        return

    if data == "client_blacklist_menu":
        await client_blacklist_prompt(update, context)
        return

    if data.startswith("client_details_"):
        client_id = data.split("_")[-1]
        await client_details(update, context, client_id)
        return

    if data.startswith("client_history_"):
        client_id = data.split("_")[-1]
        await client_history_details(update, context, client_id)
        return

    if data.startswith("client_blacklist_details_"):
        client_id = data.split("_")[-1]
        await client_blacklist_details(update, context, client_id)
        return
    
    if data == "admin_records_menu":
        await show_records_menu(update, context)
        return


    if data.startswith("client_blacklist_confirm_") or \
    data.startswith("client_blacklist_remove_") or \
    data.startswith("client_blacklist_confirm_yes_") or \
    data.startswith("client_blacklist_confirm_no_") or \
    data.startswith("client_blacklist_remove_yes_") or \
    data.startswith("client_blacklist_remove_no_"):
        await handle_client_blacklist_callback(update, context)
        return


    if data.startswith("client_blacklist_confirm_") or data.startswith("client_blacklist_cancel_"):
        await handle_client_blacklist_callback(update, context)
        return

  
    # --- ПІДТВЕРДЖЕННЯ НАДСИЛАННЯ СПОВІЩЕННЯ ---
    if data.startswith("confirm_send_notification_yes_"):
        booking_id = data.split("_")[-1]
        notification_text = context.user_data.get("notification_text", "")
        booking_ref = db.collection("bookings").document(booking_id)
        booking_doc = booking_ref.get()
        if booking_doc.exists:
            booking_data = booking_doc.to_dict()
            client_username = booking_data.get("username", "Невідомий")
            client_id = booking_data.get("user_id")
            try:
                # Надсилаємо повідомлення клієнту у форматі HTML
                await context.bot.send_message(
                    chat_id=client_id,
                    text=f"<b>Повідомлення від адміністратора:</b>\n\n{html.escape(notification_text)}",
                    parse_mode="HTML"
                )
                # Інформуємо адміністратора, кому надіслали повідомлення, із деталями
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=(
                        f"<b>Повідомлення було надіслано користувачу:</b> {client_username}\n\n"
                        f"<b>Текст повідомлення:</b>\n<pre>{html.escape(notification_text)}</pre>"
                    ),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Не вдалося відправити повідомлення користувачу {client_id}: {e}")
        ADMIN_STATE[user_id] = None
        context.user_data.pop("notification_text", None)
        # Видаляємо повідомлення з клавіатурою підтвердження
        try:
            await query.message.delete()
        except Exception as e:
            logger.error(f"Не вдалося видалити підтверджуюче повідомлення: {e}")
        return

    if data.startswith("confirm_send_notification_no_"):
        booking_id = data.split("_")[-1]
        try:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="Надсилання повідомлення скасовано."
            )
            await query.message.delete()
        except Exception as e:
            logger.error(f"Не вдалося видалити підтверджуюче повідомлення: {e}")
        ADMIN_STATE[user_id] = None
        context.user_data.pop("notification_text", None)
        return

    # --- ПІДТВЕРДЖЕННЯ СКАСУВАННЯ ЗАПИСУ ---
    if data.startswith("confirm_cancel_booking_yes_"):
        booking_id = data.split("_")[-1]
        booking_ref = db.collection("bookings").document(booking_id)
        booking_doc = booking_ref.get()
        if booking_doc.exists:
            booking_data = booking_doc.to_dict()
            date_ = booking_data.get("date")
            time_ = booking_data.get("time")
            duration_ = booking_data.get("duration", 30)
            booked_for_date = booked_slots.get(date_, [])
            start_time = datetime.strptime(time_, "%H:%M")
            slots_needed = duration_ // 30
            for i in range(slots_needed):
                t_str = (start_time + timedelta(minutes=30 * i)).strftime("%H:%M")
                if t_str in booked_for_date:
                    booked_for_date.remove(t_str)
            booked_slots[date_] = booked_for_date
            booking_ref.update({"status": "canceled"})
            cancel_reason = context.user_data.get("cancel_reason", "")
            cancellation_details = (
                f"<b>Запис скасовано!</b>\n"
                f"------------------------------------\n"
                f"<b>Користувач:</b> {booking_data.get('username', 'Невідомий')}\n"
                f"<b>Послуга:</b> {booking_data.get('service', 'Послуга')}\n"
                f"<b>Дата:</b> {date_}\n"
                f"<b>Час:</b> {time_}\n"
                f"<b>Ціна:</b> {booking_data.get('price', 'N/A')}\n"
                f"<b>Причина скасування:</b> {html.escape(cancel_reason)}"
            )
            try:
                await context.bot.send_message(
                    chat_id=booking_data.get("user_id"),
                    text=cancellation_details,
                    parse_mode="HTML"
                )
                await context.bot.send_message(
                    chat_id=query.message.chat_id,
                    text=f"Скасування запису виконано:\n\n{cancellation_details}",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Не вдалося відправити повідомлення користувачу {booking_data.get('user_id')}: {e}")
        ADMIN_STATE[user_id] = None
        context.user_data.pop("cancel_reason", None)
        try:
            await query.message.delete()
        except Exception as e:
            logger.error(f"Не вдалося видалити повідомлення з клавіатурою: {e}")
        return

    if data.startswith("confirm_cancel_booking_no_"):
        booking_id = data.split("_")[-1]
        try:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="Скасування запису скасовано."
            )
            await query.message.delete()
        except Exception as e:
            logger.error(f"Не вдалося видалити повідомлення з клавіатурою: {e}")
        ADMIN_STATE[user_id] = None
        context.user_data.pop("cancel_reason", None)
        return


    # --- Кнопки повернення до меню ---
    if data == "back_to_main_menu_text":
        if user_id in ADMIN_USER_IDS:
            await show_admin_menu(update)
        else:
            await show_user_menu(update)
        return

    if data == "back_to_services":
        await book_service(update, context)
        return

    if data == "back_to_history_menu":
        await booking_history(update, context)
        return


    if data == "back_to_admin_menu":
        await show_admin_menu(update)
        return

    if data == "back_to_schedule_main":
        await show_schedule_menu(update, context)
        return

    if data == "back_to_services_menu":
        await show_services_settings_menu(update, context)
        return

    # --- ІСТОРІЯ користувача ---
    if data == "confirmed_bookings":
        await send_user_bookings(update, context, status="confirmed", past=False)
        return
    if data == "pending_bookings":
        await send_user_bookings(update, context, status="pending")
        return
    if data == "past_bookings":
        await send_user_bookings(update, context, status="confirmed", past=True)
        return
    if data == "user_orders":
        await send_user_orders(update, context)
        return
    
    if data == "admin_rejected_bookings":
        # 1) забираємо з БД
        all_docs = db.collection("bookings").where("status","in",["rejected","canceled"]).stream()

        bookings = []
        now = datetime.now()
        for doc_ in all_docs:
            data_ = doc_.to_dict()
            try:
                dt_ = datetime.strptime(
                    data_.get("date","")+" "+data_.get("time",""), "%d.%m.%Y %H:%M"
                )
            except:
                continue
            # При потребі фільтруєте по минулому/майбутньому, якщо хочете
            bookings.append((doc_.id, data_))

        if not bookings:
            await safe_edit_message_text(
                query,
                "Немає відхилених записів (rejected/canceled).",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_bookings_main")]
                ])
            )
            return
        
        # 2) Зберігаємо у context, щоб пагінація працювала
        context.user_data["current_records"] = bookings
        context.user_data["return_callback"] = "admin_bookings_main"

        # 3) Викликаємо display_records_list
        await display_records_list(update, context, bookings, "admin_bookings_main", page=0, page_size=10)
        return


    # --- ІСТОРІЯ адміна ---
    if data == "admin_confirmed_bookings":
        records = await get_admin_records("confirmed", past=False)
        await display_records_list(update, context, records, "show_records_menu")
        return
    if status == "rejected":
        # хочемо "rejected" + "canceled"
        bookings_ref = db.collection("bookings").where("status", "in", ["rejected","canceled"])
    else:
        bookings_ref = db.collection("bookings").where("status", "==", status)

    if data == "admin_past_bookings":
        # було past=False, виправляємо:
        await send_admin_bookings(update, context, status="confirmed", past=True)
        return
    
    
    status = None  # Default value
    if "status_" in data:
            status = data.split("_")[1]  # Example of extracting status from callback data

    print(f"Extracted status: {status}")  # Debugging

    if status == "rejected":
            await query.answer("The booking has been rejected.")

    if data == "admin_pick_date_for_bookings":
        await admin_pick_date_for_bookings(update, context)    
    if data == "admin_analytics":
        await admin_analytics(update, context)
        return
    if data == "admin_history":
        await admin_history(update, context)
        return

    # --- Меню "Записи" (адмін) ---
    if data == "admin_bookings_main":
        await admin_bookings_menu(update, context)
        return
    if data == "admin_today_bookings":
        await admin_today_bookings(update, context)
        return
    if data == "admin_pick_date_for_bookings":
        await admin_pick_date_for_bookings(update, context)
        return
    if data == "admin_cancel_booking_main":
        await admin_cancel_booking_main(update, context)
        return
    
    if data.startswith("service_edit_select_"):
        doc_id = data.split("_")[-1]
        await service_details_menu(update, context, doc_id)
        return

    # Якщо користувач вибрав редагування:
    if data.startswith("service_edit_"):
        doc_id = data.split("_")[-1]
        await service_edit_menu(update, context, doc_id)
        return

    # Обробка вибору поля для редагування:
    if data.startswith("edit_name_"):
        doc_id = data.split("_")[-1]
        ADMIN_STATE[user_id] = f"SERVICE_EDIT_NAME_{doc_id}"
        await query.answer()
        await safe_edit_message_text(query, "Введіть нову назву послуги:")
        return
    if data.startswith("edit_price_"):
        doc_id = data.split("_")[-1]
        ADMIN_STATE[user_id] = f"SERVICE_EDIT_PRICE_{doc_id}"
        await query.answer()
        await safe_edit_message_text(query, "Введіть нову ціну (лише число, напр. 450):")
        return
    if data.startswith("edit_duration_"):
        doc_id = data.split("_")[-1]
        ADMIN_STATE[user_id] = f"SERVICE_EDIT_DURATION_{doc_id}"
        await query.answer()
        await safe_edit_message_text(query, "Введіть нову тривалість послуги (хвилин, напр. 45):")
        return

    # Видалення послуги: спочатку підтвердження
    if data.startswith("service_delete_"):
        doc_id = data.split("_")[-1]
        await service_delete_confirm(update, context, doc_id)
        return
    if data.startswith("confirm_delete_"):
        doc_id = data.split("_")[-1]
        await service_delete_service(update, context, doc_id)
        return

    
        # ...
    # Обробка підтвердження розсилки
    if data == "confirm_broadcast_yes":
        user_id = query.from_user.id
        broadcast_text = context.user_data.get("broadcast_message")
        if not broadcast_text:
            await safe_edit_message_text(query, "Немає повідомлення для розсилки.")
            return
        users_collection = db.collection("users").stream()
        count_sent = 0
        for user_doc in users_collection:
            user_data = user_doc.to_dict()
            chat_id = user_data.get("user_id")
            if chat_id:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=broadcast_text)
                    count_sent += 1
                except Exception as e:
                    logger.error(f"Не вдалося надіслати повідомлення користувачу {chat_id}: {e}")
        admin_broadcast_mode[user_id] = False
        ADMIN_STATE[user_id] = None
        await safe_edit_message_text(query, f"Розсилку завершено. Повідомлення надіслано {count_sent} користувачам.")
        return

    if data == "confirm_broadcast_no":
        user_id = query.from_user.id
        admin_broadcast_mode[user_id] = False
        ADMIN_STATE[user_id] = None
        await safe_edit_message_text(query, "Розсилку скасовано.")
        return
    # ...


    # --- Деталі записів ---
    if data.startswith("admin_booking_details_"):
        booking_id = data.split("_")[-1]
        await admin_booking_details(update, context, booking_id)
        return
    
    if data.startswith("admin_show_bookings_for_date_"):
        date_str = data.split("_")[-1]
        await admin_show_bookings_for_date(date_str, update, context)
        return


    # --- Надіслати сповіщення ---
    if data.startswith("admin_notify_booking_"):
        booking_id = data.split("_")[-1]
        await admin_notify_booking(update, context, booking_id)
        return

    # --- Скасувати запис ---
    if data.startswith("admin_cancel_booking_"):
        booking_id = data.split("_")[-1]
        await admin_cancel_booking(update, context, booking_id)
        return

    if data.startswith("admin_confirm_cancel_"):
        booking_id = data.split("_")[-1]
        await admin_confirm_cancel(update, context, booking_id)
        return

    # --- Запис користувача: вибір послуги, дати, часу ---
    if data.startswith("book_service_"):
        doc_id = data.split("_", 2)[2]
        user_selection[user_id] = {"service_id": doc_id}
        await select_date(update, context)
        return

    if data == "select_date":
        await select_date(update, context)
        return

    if data.startswith("more_dates_") or data.startswith("previous_dates_"):
        await select_date(update, context)
        return

    if data.startswith("date_"):
        await select_time(update, context)
        return

    if data.startswith("time_"):
        await confirm_selection(update, context)
        return

    if data == "cancel":
        user_selection.pop(user_id, None)
        await safe_edit_message_text(query, "Ваш запис було відмінено.")
        return

    if data == "proceed":
        user_data = user_selection.get(user_id, {})
        service_id = user_data.get("service_id")
        date = user_data.get("date")
        time_ = user_data.get("time")

        if not (service_id and date and time_):
            await safe_edit_message_text(query, "Помилка у бронюванні. Спробуйте ще раз.")
            return

        service_data = services_cache.get(service_id, {})
        service_name = service_data.get("name", "Послуга")
        price = service_data.get("price", "0 євро")
        duration = service_data.get("duration", 30)

        booked_for_date = booked_slots.get(date, [])
        slots_needed = duration // 30
        start_time = datetime.strptime(time_, "%H:%M")

        # Перевіряємо, чи ще вільний слот
        slot_unavailable = False
        for i in range(slots_needed):
            time_str = (start_time + timedelta(minutes=30*i)).strftime("%H:%M")
            if time_str in booked_for_date:
                slot_unavailable = True
                break

        if slot_unavailable:
            await safe_edit_message_text(query, "Вибачте, цей час уже зайнятий. Будь ласка, оберіть інший.")
            await select_time(update, context, selected_date_str=date)
            return

        # Бронюємо
        for i in range(slots_needed):
            time_str = (start_time + timedelta(minutes=30 * i)).strftime("%H:%M")
            booked_for_date.append(time_str)
        booked_slots[date] = booked_for_date

        # Зберігаємо в БД
        booking_ref = db.collection("bookings").document()
        booking_id = booking_ref.id

        username = query.from_user.username
        if not username:
            full_name = " ".join(filter(None, [query.from_user.first_name, query.from_user.last_name]))
            username = full_name if full_name.strip() else str(user_id)

        booking_data = {
            "username": username,
            "user_id": user_id,
            "service_id": service_id,
            "service": service_name,
            "date": date,
            "time": time_,
            "price": price,
            "duration": duration,
            "status": "pending",
            "booking_id": booking_id,
        }
        booking_ref.set(booking_data)

        # Зберігаємо користувача, якщо не існує
        user_ref = db.collection("users").document(str(user_id))
        if not user_ref.get().exists:
            user_ref.set({
                "username": username,
                "user_id": user_id,
                "first_name": query.from_user.first_name,
                "last_name": query.from_user.last_name,
            })

        # Сповіщення адмінам
        admin_message = (
            f"❗️Новий запис:❗️\n"
            f"------------------------------------\n"
            f"Послуга: {service_name}\n"
            f"Дата: {date}\n"
            f"Час: {time_}\n"
            f"Користувач: {username}\n"
            f"------------------------------------\n"
            f"❔Підтвердити чи відхилити?"
        )
        admin_keyboard = [
            [
                InlineKeyboardButton("ПРИЙНЯТИ", callback_data=f"accept_{booking_id}"),
                InlineKeyboardButton("ВІДХИЛИТИ", callback_data=f"reject_{booking_id}"),
            ]
        ]
        admin_reply_markup = InlineKeyboardMarkup(admin_keyboard)

        for admin_chat_id in ADMIN_USER_IDS:
            try:
                await context.bot.send_message(chat_id=admin_chat_id, text=admin_message, reply_markup=admin_reply_markup)
            except Forbidden:
                logger.warning(f"Не вдалося відправити повідомлення адміністратору {admin_chat_id}")

        # Повідомлення користувачу
        await safe_edit_message_text(
            query,
            "Все готово! ✅\n\n‼️ Запис набуде чинності після підтвердження адміністратором. Очікуйте! ‼️"
        )
        return

    if data.startswith("accept_"):
        booking_id = data.split("_")[-1]
        # Якщо для цього запису вже є підтвердження – не надсилаємо нове
        if booking_id in pending_confirmations:
            await context.bot.send_message(chat_id=query.message.chat_id, 
                text="Підтверджуюче повідомлення вже надіслано. Будь ласка, оберіть одну з опцій.")
            return
        first_message_id = query.message.message_id  # ID першого повідомлення
        booking_ref = db.collection("bookings").document(booking_id)
        booking_doc = booking_ref.get()
        if booking_doc.exists:
            booking_data = booking_doc.to_dict()
            user_name = booking_data.get("username", "Невідомий")
            service_ = booking_data.get("service", "Послуга")
            date_ = booking_data.get("date", "Дата")
            time_ = booking_data.get("time", "Час")
            price_ = booking_data.get("price", "Ціна")
            confirmation_text = (
                f"Ви дійсно хочете прийняти цей запис?\n"
                f"------------------------------------\n"
                f"Користувач: {user_name}\n"
                f"Послуга: {service_}\n"
                f"Дата: {date_}\n"
                f"Час: {time_}\n"
                f"Ціна: {price_}\n"
            )
            keyboard = [
                [
                    InlineKeyboardButton("✅ Так", callback_data=f"confirm_accept_yes_{booking_id}"),
                    InlineKeyboardButton("❌ Ні", callback_data=f"confirm_accept_no_{booking_id}")
                ] 
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            sent_message = await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=confirmation_text,
                reply_markup=reply_markup
            )
            pending_confirmations[booking_id] = {
                "first": first_message_id,
                "confirmation": sent_message.message_id
            }
        else:
            await safe_edit_message_text(query, "Запис не знайдено або вже опрацьовано.")
        return

    if data.startswith("confirm_accept_yes_"):
        booking_id = data.split("_")[-1]
        # Видаляємо обидва повідомлення, якщо вони збережені
        if booking_id in pending_confirmations:
            ids = pending_confirmations.pop(booking_id)
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=ids["first"])
            except Exception as e:
                logger.error(f"Помилка видалення першого повідомлення: {e}")
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=ids["confirmation"])
            except Exception as e:
                logger.error(f"Помилка видалення підтверджуючого повідомлення: {e}")
        booking_ref = db.collection("bookings").document(booking_id)
        booking_doc = booking_ref.get()
        if booking_doc.exists:
            booking_data = booking_doc.to_dict()
            user_id_ = booking_data.get("user_id")
            service_name = booking_data.get("service", "Послуга")
            date_ = booking_data.get("date", "Дата")
            time_ = booking_data.get("time", "Час")
            price = booking_data.get("price", "Ціна")
            booking_ref.update({"status": "confirmed"})
            confirmation_message = (
                f"✅ Ваше бронювання підтверджено! ✅\n"
                f"------------------------------------\n"
                f"Послуга: {service_name}\n"
                f"Дата: {date_}\n"
                f"Час: {time_}\n"
                f"Ціна: {price}\n"
                f"------------------------------------\n"
                f"Чекаємо вас у нашому барбершопі!"
            )
            try:
                await context.bot.send_message(chat_id=user_id_, text=confirmation_message)
            except Forbidden:
                logger.warning(f"Не вдалося відправити повідомлення користувачу {user_id_}")
            await context.bot.send_message(chat_id=query.message.chat_id, text="✅ Бронювання підтверджено!")
        else:
            await context.bot.send_message(chat_id=query.message.chat_id, text="Запис не знайдено або вже опрацьовано.")
        return
    
    if data.startswith("confirm_accept_no_"):
        booking_id = data.split("_")[-1]
        # Видаляємо лише підтверджуюче повідомлення (з кнопками «Так»/«Ні»)
        if booking_id in pending_confirmations:
            ids = pending_confirmations.pop(booking_id)
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=ids["confirmation"])
            except Exception as e:
                logger.error(f"Помилка видалення підтверджуючого повідомлення: {e}")
        await context.bot.send_message(chat_id=query.message.chat_id, text="Бронювання залишено без змін.")
        return


    if data.startswith("reject_"):
        booking_id = data[len("reject_"):]
        if booking_id in pending_confirmations:
            await context.bot.send_message(chat_id=query.message.chat_id, 
                text="Підтверджуюче повідомлення вже надіслано. Будь ласка, оберіть одну з опцій.")
            return
        first_message_id = query.message.message_id
        booking_ref = db.collection("bookings").document(booking_id)
        booking_doc = booking_ref.get()
        if booking_doc.exists:
            booking_data = booking_doc.to_dict()
            user_name = booking_data.get("username", "Невідомий")
            service_ = booking_data.get("service", "Послуга")
            date_ = booking_data.get("date", "Дата")
            time_ = booking_data.get("time", "Час")
            price_ = booking_data.get("price", "Ціна")
            confirmation_text = (
                f"Ви дійсно хочете відмінити цей запис?\n"
                f"------------------------------------\n"
                f"Користувач: {user_name}\n"
                f"Послуга: {service_}\n"
                f"Дата: {date_}\n"
                f"Час: {time_}\n"
                f"Ціна: {price_}\n"
            )
            keyboard = [
                [
                    InlineKeyboardButton("✅ Так", callback_data=f"confirm_reject_yes_{booking_id}"),
                    InlineKeyboardButton("❌ Ні", callback_data=f"confirm_reject_no_{booking_id}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            sent_message = await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=confirmation_text,
                reply_markup=reply_markup
            )
            pending_confirmations[booking_id] = {
                "first": first_message_id,
                "confirmation": sent_message.message_id
            }
        else:
            await safe_edit_message_text(query, "Запис не знайдено або вже опрацьовано.")
        return

        
    if data.startswith("confirm_reject_yes_"):
        booking_id = data.split("_")[-1]
        if booking_id in pending_confirmations:
            ids = pending_confirmations.pop(booking_id)
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=ids["first"])
            except Exception as e:
                logger.error(f"Помилка видалення першого повідомлення: {e}")
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=ids["confirmation"])
            except Exception as e:
                logger.error(f"Помилка видалення підтверджуючого повідомлення: {e}")
        booking_ref = db.collection("bookings").document(booking_id)
        booking_doc = booking_ref.get()
        if booking_doc.exists:
            booking_data = booking_doc.to_dict()
            user_id_ = booking_data.get("user_id")
            service_name = booking_data.get("service", "Послуга")
            date_ = booking_data.get("date", "Дата")
            time_ = booking_data.get("time", "Час")
            price = booking_data.get("price", "Ціна")
            duration_ = booking_data.get("duration", 30)  # Отримуємо тривалість запису
            # Видаляємо заброньовані слоти:
            booked_for_date = booked_slots.get(date_, [])
            start_time = datetime.strptime(time_, "%H:%M")
            slots_needed = duration_ // 30
            for i in range(slots_needed):
                t_str = (start_time + timedelta(minutes=30 * i)).strftime("%H:%M")
                if t_str in booked_for_date:
                    booked_for_date.remove(t_str)
            booked_slots[date_] = booked_for_date

            booking_ref.update({"status": "rejected"})
            rejection_message = (
                f"❌ Ваше бронювання відхилено ❌\n"
                f"------------------------------------\n"
                f"Послуга: {service_name}\n"
                f"Дата: {date_}\n"
                f"Час: {time_}\n"
                f"Ціна: {price}\n"
                f"------------------------------------\n"
                f"Спробуйте обрати інший час або зв'яжіться з адміністратором."
            )
            try:
                await context.bot.send_message(chat_id=user_id_, text=rejection_message)
            except Forbidden:
                logger.warning(f"Не вдалося відправити повідомлення користувачу {user_id_}")
            await context.bot.send_message(chat_id=query.message.chat_id, text="❌ Бронювання відхилено!")
        else:
            await context.bot.send_message(chat_id=query.message.chat_id, text="Запис не знайдено або вже опрацьовано.")
        return




    if data.startswith("confirm_reject_no_"):
        booking_id = data.split("_")[-1]
        if booking_id in pending_confirmations:
            ids = pending_confirmations.pop(booking_id)
            try:
                await context.bot.delete_message(chat_id=query.message.chat_id, message_id=ids["confirmation"])
            except Exception as e:
                logger.error(f"Помилка видалення підтверджуючого повідомлення: {e}")
        await context.bot.send_message(chat_id=query.message.chat_id, text="Бронювання залишено без змін.")
        return



    # Користувач натиснув "Замовити" косметику
    if data == "order_cosmetics":
        message = (
            "Зверніться, будь ласка, до адміністратора @shvetsnazar_barber "
            "для оформлення замовлення. Дякуємо! 😊✂️"
        )
        await safe_edit_message_text(query, message)
        return

    # --- Розклад ---
    if data == "schedule_same_for_week":
        await schedule_set_same_for_week(update, context)
        return
    if data == "schedule_daily":
        await schedule_set_daily(update, context)
        return
    if data == "schedule_days_off":
        await schedule_set_days_off(update, context)
        return
    if data == "schedule_edit_date_range":
        await schedule_edit_date_range(update, context)
        return
    if data == "schedule_view":
        await schedule_view(update, context)
        return
    if data == "daily_schedule_save":
        await safe_edit_message_text(query, "✅ Всі зміни вже автоматично збережені!", reply_markup=None)
        return
    
    if data.startswith("daily_schedule_day_"):
        day_index = int(data.split("_")[-1])
        ADMIN_STATE[user_id] = f"DAILY_SCHEDULE_DAY_{day_index}"
        await safe_edit_message_text(
            query,
            f"Введіть час роботи для {get_weekday_name_ua(day_index)} у форматі 09:00-18:00 або 'вихідний':"
        )
        return

    if data.startswith("toggle_day_off_"):
        day_index = int(data.split("_")[-1])
        current_off = DEFAULT_WEEK_SCHEDULE[day_index]["off"]
        DEFAULT_WEEK_SCHEDULE[day_index]["off"] = not current_off
        # Оновлюємо локально й перевідкриваємо меню
        await show_days_off_menu(query, context)
        return

    if data == "finish_set_days_off":
        # Зберігаємо до БД
        save_default_schedule_to_db()
        ADMIN_STATE[user_id] = None
        await safe_edit_message_text(
            query,
            "Вихідні дні (дефолт) оновлено! ✅",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Змінити ще раз", callback_data="schedule_days_off")],
                [InlineKeyboardButton("Переглянути графік", callback_data="schedule_view")],
                [InlineKeyboardButton("Головне меню", callback_data="back_to_admin_menu")]
            ])
        )
        return

    # --- Налаштування послуг ---
    if data == "service_add_edit_main":
        await service_add_edit_main(update, context)
        return
    if data == "service_add_new":
        await service_add_new(update, context)
        return
    if data.startswith("service_edit_select_"):
        doc_id = data.split("_")[-1]
        await service_edit_select(update, context, doc_id)
        return
    if data == "service_change_price":
        await service_change_price_main(update, context)
        return
    if data == "service_toggle_active":
        await service_toggle_active_main(update, context)
        return
    if data.startswith("service_price_select_"):
        doc_id = data.split("_")[-1]
        await query.answer()
        context.user_data["change_price_service_id"] = doc_id
        ADMIN_STATE[user_id] = "SERVICE_CHANGE_PRICE"
        service_data = services_cache.get(doc_id, {})
        current_price = service_data.get("price", "0 євро")
        await safe_edit_message_text(
            query,
            f"Поточна ціна: {current_price}\nВведіть нову ціну у євро (наприклад, '15'):"
        )
        return

    if data.startswith("service_toggle_active_"):
        doc_id = data.split("_")[-1]
        service_data = services_cache.get(doc_id, {})
        if not service_data:
            await query.answer("Послуга не знайдена", show_alert=True)
            return
        new_status = not service_data.get("active", False)
        update_service_in_db(doc_id, {"active": new_status})
        await service_toggle_active_main(update, context)
        return

    # Якщо нічого не підходить
    await query.answer()
    
    

    # Редагування назви послуги
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

async def show_top_services_periods(update: Update, context: CallbackContext):
    keyboard = [
        [
            InlineKeyboardButton("14 днів", callback_data="stat_top_14"),
            InlineKeyboardButton("1 місяць", callback_data="stat_top_30")
        ],
        [
            InlineKeyboardButton("3 місяці", callback_data="stat_top_90"),
            InlineKeyboardButton("6 місяців", callback_data="stat_top_180")
        ],
        [
            InlineKeyboardButton("9 місяців", callback_data="stat_top_270"),
            InlineKeyboardButton("12 місяців", callback_data="stat_top_365")
        ],
        [
            InlineKeyboardButton("Custom", callback_data="stat_top_custom"),
            InlineKeyboardButton("Назад", callback_data="stat_menu")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("Оберіть період для звіту «Топ послуг»:", reply_markup=reply_markup)
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, "Оберіть період для звіту «Топ послуг»:", reply_markup=reply_markup)


async def handle_admin_text_states(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if not update.message or not update.message.text:
        return  # Захист від помилок, якщо update.message відсутнє

    text = update.message.text.strip()
    current_state = ADMIN_STATE.get(user_id, None)  # Ініціалізація current_state

    # Перевіряємо, чи вірний імпорт та доступність класу InlineKeyboardButton
    assert InlineKeyboardButton, "InlineKeyboardButton is not imported correctly!"
    # Якщо користувач знаходиться в одному з нових станів для клієнтів
    if current_state in ["CLIENT_SEARCH_INPUT", "CLIENT_HISTORY_INPUT", "CLIENT_BLACKLIST_INPUT"]:
        await handle_client_text_states(update, context)
        return

    # Якщо користувач вводить кастомний період для статистики
    if current_state == "STAT_COUNT_INPUT":
        period = parse_date_range(text)
        if not period:
            await update.message.reply_text("Невірний формат періоду. Спробуйте ще раз у форматі DD.MM.YYYY-DD.MM.YYYY.")
            return
        start_date, end_date = period
        await process_stat_count_input_with_dates(update, context, start_date, end_date)
        ADMIN_STATE[user_id] = None
        return

    if current_state == "STAT_TOP_INPUT":
        period = parse_date_range(text)
        if not period:
            await update.message.reply_text("Невірний формат періоду. Спробуйте ще раз у форматі DD.MM.YYYY-DD.MM.YYYY.")
            return
        start_date, end_date = period
        await process_stat_top_input_with_dates(update, context, start_date, end_date)
        ADMIN_STATE[user_id] = None
        return

    if current_state == "STAT_AVG_INPUT":
        period = parse_date_range(text)
        if not period:
            await update.message.reply_text("Невірний формат періоду. Спробуйте ще раз у форматі DD.MM.YYYY-DD.MM.YYYY.")
            return
        start_date, end_date = period
        await process_stat_avg_input_with_dates(update, context, start_date, end_date)
        ADMIN_STATE[user_id] = None
        return

    # Редагування назви послуги
    if current_state and current_state.startswith("SERVICE_EDIT_NAME_"):
        doc_id = current_state.split("_")[-1]
        update_service_in_db(doc_id, {"name": text})
        ADMIN_STATE[user_id] = None
        refresh_services_cache()
        await service_details_menu(update, context, doc_id)  # Переходимо без додаткового повідомлення
        return

    # --- ОБРОБКА ВВЕДЕНОГО ТЕКСТУ ДЛЯ СПОВІЩЕННЯ ---
    if current_state and current_state.startswith("ADMIN_SEND_NOTIFICATION_"):
        if not current_state.startswith("ADMIN_SEND_NOTIFICATION_CONFIRM_"):
            booking_id = current_state.split("_")[-1]
            context.user_data["notification_text"] = text  # Зберігаємо повідомлення
            
            ADMIN_STATE[user_id] = f"ADMIN_SEND_NOTIFICATION_CONFIRM_{booking_id}"
            
            keyboard = [
                [
                    InlineKeyboardButton("✅ Так", callback_data=f"confirm_send_notification_yes_{booking_id}"),
                    InlineKeyboardButton("❌ Ні", callback_data=f"confirm_send_notification_no_{booking_id}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"Ви ввели наступне повідомлення для надсилання клієнту:\n\n{text}\n\n"
                f"Ви дійсно хочете його надіслати?",
                reply_markup=reply_markup
            )
            return

    # --- ОБРОБКА ВВЕДЕНОГО ТЕКСТУ ДЛЯ СКАСУВАННЯ ЗАПИСУ ---
    if current_state and current_state.startswith("ADMIN_CANCEL_BOOKING_REASON_"):
        booking_id = current_state.split("_")[-1]
        context.user_data["cancel_reason"] = text  # Зберігаємо причину скасування

        ADMIN_STATE[user_id] = f"ADMIN_CANCEL_BOOKING_CONFIRM_{booking_id}"

        booking_ref = db.collection("bookings").document(booking_id)
        booking_doc = booking_ref.get()
        if booking_doc.exists:
            booking_data = booking_doc.to_dict()
            detail_text = (
                f"👤 Користувач: {booking_data.get('username', 'Невідомий')}\n"
                f"💇 Послуга: {booking_data.get('service', 'Послуга')}\n"
                f"📅 Дата: {booking_data.get('date', '--.--.----')}\n"
                f"⏰ Час: {booking_data.get('time', '--:--')}\n"
                f"💵 Ціна: {booking_data.get('price', 'N/A')}\n"
                f"------------------------------------\n"
                f"Причина скасування: {text}\n"
            )
            
            # 🛠 Фікс: Переконайтеся, що кнопки створюються ПРАВИЛЬНО:
            keyboard = [
                [InlineKeyboardButton("✅ Так", callback_data=f"confirm_cancel_booking_yes_{booking_id}")],
                [InlineKeyboardButton("❌ Ні", callback_data=f"confirm_cancel_booking_no_{booking_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"Ви дійсно хочете скасувати цей запис?\n\n{detail_text}",
                reply_markup=reply_markup
            )
            return

    # Редагування ціни послуги
    if current_state and current_state.startswith("SERVICE_EDIT_PRICE_"):
        doc_id = current_state.split("_")[-1]
        if not text.isdigit():
            await update.message.reply_text("Будь ласка, введіть число (без додаткових символів).")
            return
        new_price = f"{text} євро"
        update_service_in_db(doc_id, {"price": new_price})
        ADMIN_STATE[user_id] = None
        refresh_services_cache()
        await service_details_menu(update, context, doc_id)
        return

    # Редагування тривалості послуги
    if current_state and current_state.startswith("SERVICE_EDIT_DURATION_"):
        doc_id = current_state.split("_")[-1]
        if not text.isdigit():
            await update.message.reply_text("Будь ласка, введіть число (хвилин).")
            return
        new_duration = int(text)
        update_service_in_db(doc_id, {"duration": new_duration})
        ADMIN_STATE[user_id] = None
        refresh_services_cache()
        await service_details_menu(update, context, doc_id)
        return

    # Якщо користувач не адмін, перевіряємо, чи він у режимі розсилки
    if user_id not in ADMIN_USER_IDS:
        if admin_broadcast_mode.get(user_id, False):
            await handle_broadcast_message(update, context)
        return

    # Якщо в режимі розсилки
    if admin_broadcast_mode.get(user_id, False):
        await handle_broadcast_message(update, context)
        return

    # ====== РОЗКЛАД (Однаковий на весь тиждень) ======
    if current_state == "WAITING_FOR_SAME_SCHEDULE":
        if text.lower() == "вихідний":
            for i in range(7):
                DEFAULT_WEEK_SCHEDULE[i]["off"] = True
                DEFAULT_WEEK_SCHEDULE[i]["start"] = "00:00"
                DEFAULT_WEEK_SCHEDULE[i]["end"] = "00:00"
            save_default_schedule_to_db()
            await update.message.reply_text("✅ Увесь тиждень встановлено вихідним!")
        else:
            if validate_time_range(text):
                start, end = text.split("-")
                for i in range(7):
                    # Якщо день не позначено як вихідний, оновлюємо години
                    if not DEFAULT_WEEK_SCHEDULE[i]["off"]:
                        DEFAULT_WEEK_SCHEDULE[i]["start"] = start
                        DEFAULT_WEEK_SCHEDULE[i]["end"] = end
                save_default_schedule_to_db()
                await update.message.reply_text(f"✅ Графік оновлено: {start}-{end} для робочих днів!")
            else:
                    await update.message.reply_text("❌ Невірний формат! Спробуйте ще раз (09:00-18:00) або 'вихідний'.")
                    return
        ADMIN_STATE[user_id] = None
        return

    # ====== РОЗКЛАД (Графік по днях) ======
    if current_state and current_state.startswith("DAILY_SCHEDULE_DAY_"):
        day_index = int(current_state.split("_")[-1])

        if text.lower() == "вихідний":
            DEFAULT_WEEK_SCHEDULE[day_index] = {
                "off": True,
                "start": "00:00",
                "end": "00:00"
            }
            message = f"✅ Для {get_weekday_name_ua(day_index)} встановлено вихідний день."
        else:
            if validate_time_range(text):
                start, end = text.split("-")
                DEFAULT_WEEK_SCHEDULE[day_index] = {
                    "off": False,
                    "start": start,
                    "end": end
                }
                message = f"✅ Для {get_weekday_name_ua(day_index)} встановлено час: {start}-{end}."
            else:
                await update.message.reply_text("❌ Невірний формат! Введіть, наприклад: 09:00-18:00 або 'вихідний'.")
                return

        # 🔹 Автоматично зберігаємо зміни в Firestore
        save_default_schedule_to_db()
        ADMIN_STATE[user_id] = "DAILY_SCHEDULE_SETUP"

        await update.message.reply_text(
            message + "\n🔄 Розклад оновлено! Введіть час для іншого дня або вийдіть в меню."
        )
        return

    # ====== ГРАФІК НА КОНКРЕТНИЙ ПЕРІОД (Діапазон дат) ======
    if current_state == "EDIT_DATE_RANGE":
        parsed = validate_date_range_input(text)
        if not parsed:
            await update.message.reply_text(
                "❌ Невірний формат! Приклад: 12.07.2025-15.07.2025 10:00-20:00 або 12.07.2025-15.07.2025 вихідний."
            )
            return
        

        (start_date, end_date, is_off, start_time, end_time) = parsed
        current_day = start_date
        while current_day <= end_date:
            date_str = current_day.strftime("%d.%m.%Y")
            if is_off:
                update_day_in_db(date_str, off=True, start="00:00", end="00:00")
            else:
                update_day_in_db(date_str, off=False, start=start_time, end=end_time)
            current_day += timedelta(days=1)

        await update.message.reply_text("✅ Розклад на вказаний період оновлено в базі даних!")
        ADMIN_STATE[user_id] = None
        return
    
    


    # ====== МЕНЮ «ЗАПИСИ»: введення дати для "Записи на обрану дату" ======
    if current_state == "ADMIN_WAITING_DATE_FOR_BOOKINGS":
        date_pattern = r"^\d{2}\.\d{2}\.\d{4}$"
        if not re.match(date_pattern, text):
            await update.message.reply_text("Невірний формат дати. Спробуйте ще раз (ДД.ММ.РРРР).")
            return
        ADMIN_STATE[user_id] = None
        await admin_show_bookings_for_date(text, update, context)
        return

    # ====== СПОВІЩЕННЯ КЛІЄНТУ (admin_notify_booking) ======
    if current_state and current_state.startswith("ADMIN_SEND_NOTIFICATION_"):
        await process_admin_send_notification(user_id, text, context)
        ADMIN_STATE[user_id] = None
        await update.message.reply_text("Повідомлення надіслано клієнту.")
        return

    # ====== СКАСУВАННЯ ЗАПИСУ (reason) ======
    if current_state and current_state.startswith("ADMIN_CANCEL_BOOKING_REASON_"):
        await process_admin_cancel_reason(user_id, text, context)
        await update.message.reply_text("Запис скасовано та клієнт повідомлений.")
        return

    # ====== НАЛАШТУВАННЯ ПОСЛУГ ======
    if current_state == "SERVICE_ADD_NEW_NAME":
        context.user_data["new_service_name"] = text
        ADMIN_STATE[user_id] = "SERVICE_ADD_NEW_PRICE"
        await update.message.reply_text("Введіть вартість послуги (у євро), напр. '500':")
        return

    if current_state == "SERVICE_ADD_NEW_PRICE":
        if not text.isdigit():
            await update.message.reply_text("Будь ласка, введіть ціле число (у євро).")
            return
        context.user_data["new_service_price"] = f"{text} євро"
        ADMIN_STATE[user_id] = "SERVICE_ADD_NEW_DURATION"
        await update.message.reply_text("Введіть тривалість послуги у хвилинах, напр. '45':")
        return

    if current_state == "SERVICE_ADD_NEW_DURATION":
        if not text.isdigit():
            await update.message.reply_text("Будь ласка, введіть число (тривалість у хвилинах).")
            return
        duration = int(text)
        new_name = context.user_data.get("new_service_name")
        new_price = context.user_data.get("new_service_price")

        create_service_in_db(new_name, new_price, duration)

        context.user_data.pop("new_service_name", None)
        context.user_data.pop("new_service_price", None)
        ADMIN_STATE[user_id] = None
        await update.message.reply_text(
            f"Нова послуга «{new_name}» створена!\nЦіна: {new_price}, тривалість: {duration} хв."
        )
        return

    if current_state == "SERVICE_EDIT_NAME":
        doc_id = context.user_data.get("edit_service_id")
        if not doc_id:
            await update.message.reply_text("Помилка: не знайдено ID послуги.")
            ADMIN_STATE[user_id] = None
            return

        service_data = services_cache.get(doc_id, {})
        new_name = text
        if not new_name:
            new_name = service_data.get("name", "Без назви")
        context.user_data["edit_service_new_name"] = new_name
        ADMIN_STATE[user_id] = "SERVICE_EDIT_PRICE"
        current_price = service_data.get("price", "0 євро")
        await update.message.reply_text(
            f"ОК. Нова назва: {new_name}\n"
            f"Поточна ціна: {current_price}\n"
            f"Введіть нову ціну (у євро) або залиште порожнім, щоб не змінювати:"
        )
        return

    if current_state == "SERVICE_EDIT_PRICE":
        doc_id = context.user_data.get("edit_service_id")
        if not doc_id:
            await update.message.reply_text("Помилка: не знайдено ID послуги.")
            ADMIN_STATE[user_id] = None
            return

        service_data = services_cache.get(doc_id, {})
        new_price_str = text
        if not new_price_str:
            new_price_str = service_data.get("price", "0 євро")
        else:
            if not new_price_str.isdigit():
                clean_str = new_price_str.replace("євро", "").strip()
                if clean_str.isdigit():
                    new_price_str = clean_str
                else:
                    await update.message.reply_text("Невірний формат ціни. Введіть лише число, напр. '10'")
                    return
            new_price_str = f"{new_price_str} євро"

        context.user_data["edit_service_new_price"] = new_price_str
        ADMIN_STATE[user_id] = "SERVICE_EDIT_DURATION"
        current_duration = service_data.get("duration", 30)
        await update.message.reply_text(
            f"ОК. Нова ціна: {new_price_str}\n"
            f"Поточна тривалість: {current_duration} хв\n"
            f"Введіть нову тривалість у хвилинах або залиште порожнім, щоб не змінювати:"
        )
        return

    if current_state == "SERVICE_EDIT_DURATION":
        doc_id = context.user_data.get("edit_service_id")
        if not doc_id:
            await update.message.reply_text("Помилка: не знайдено ID послуги.")
            ADMIN_STATE[user_id] = None
            return

        service_data = services_cache.get(doc_id, {})
        new_name = context.user_data.get("edit_service_new_name", service_data.get("name", "Без назви"))
        new_price = context.user_data.get("edit_service_new_price", service_data.get("price", "0 євро"))

        new_duration_str = text.strip()
        if not new_duration_str:
            new_duration = service_data.get("duration", 30)
        else:
            if not new_duration_str.isdigit():
                await update.message.reply_text("Невірний формат тривалості. Введіть число.")
                return
            new_duration = int(new_duration_str)

        update_service_in_db(doc_id, {
            "name": new_name,
            "price": new_price,
            "duration": new_duration
        })

        context.user_data.pop("edit_service_id", None)
        context.user_data.pop("edit_service_new_name", None)
        context.user_data.pop("edit_service_new_price", None)
        ADMIN_STATE[user_id] = None

        await update.message.reply_text(
            f"Послугу оновлено!\n"
            f"Назва: {new_name}\nЦіна: {new_price}\nТривалість: {new_duration} хв."
        )
        return

    if current_state == "SERVICE_CHANGE_PRICE":
        doc_id = context.user_data.get("change_price_service_id")
        if not doc_id:
            await update.message.reply_text("Помилка: не знайдено ID послуги.")
            ADMIN_STATE[user_id] = None
            return

        if not text.isdigit():
            clean_str = text.replace("євро", "").strip()
            if not clean_str.isdigit():
                await update.message.reply_text("Будь ласка, введіть коректну суму (лише цифри).")
                return
            text = clean_str

        new_price = f"{text} євро"
        update_service_in_db(doc_id, {"price": new_price})
        ADMIN_STATE[user_id] = None
        context.user_data.pop("change_price_service_id", None)
        await update.message.reply_text(f"Ціна оновлена: {new_price}")
        return

    # Якщо нічого з адмін-станів не спрацювало — можливо, розсилка?
    await handle_broadcast_message(update, context)
    
async def send_all_records_in_txt(query: CallbackQuery, context: CallbackContext):
    # 1) Збираємо ВСІ записи
    all_bookings = db.collection("bookings").stream()
    records = []
    for doc in all_bookings:
        data = doc.to_dict()
        records.append(data)

    # 2) Формуємо заголовок + текстове форматування
    txt_content = "ID".ljust(15) + "Користувач".ljust(20) + "Послуга".ljust(25) + "Дата".ljust(15) + "Час".ljust(10) + "Ціна".ljust(10) + "Статус\n"
    txt_content += "=" * 100 + "\n"  # Лінія розділення

    # 3) Формуємо рядки у таблицю
    for b in records:
        rec_id = b.get("booking_id", "<no_id>").ljust(15)
        username = b.get("username", "<no_user>").ljust(20)
        service = b.get("service", "<no_service>").ljust(25)
        date_ = b.get("date", "--.--.----").ljust(15)
        time_ = b.get("time", "--:--").ljust(10)
        price = str(b.get("price", "0")).ljust(10)
        status = b.get("status", "unknown")

        txt_content += f"{rec_id}{username}{service}{date_}{time_}{price}{status}\n"

    # 4) Записуємо у тимчасовий файл
    file_name = "all_bookings.txt"
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(txt_content)

    # 5) Відправляємо як документ:
    try:
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=open(file_name, "rb"),
            filename=file_name,
            caption="Ось всі записи у форматі TXT"
        )
    except Exception as e:
        logger.error(f"Не вдалося відправити TXT-файл: {e}")

    # 6) Видаляємо файл після надсилання
    if os.path.exists(file_name):
        os.remove(file_name)
    # Повернемося до попереднього меню (за потреби):
    # await context.bot.send_message(chat_id=query.message.chat_id, text="Оберіть наступну дію...")
from fpdf import FPDF
import os
import time

async def send_filtered_records_pdf(update, context, status_filter):
    CACHE_FILE = f"bookings_{status_filter}.pdf"
    CACHE_DURATION = 600  # 10 хвилин

    # Визначаємо chat_id правильно
    chat_id = None
    if update.message and update.message.chat_id:
        chat_id = update.message.chat_id
    elif update.callback_query and update.callback_query.message:
        chat_id = update.callback_query.message.chat_id

    # Перевіряємо, чи визначено chat_id
    if chat_id is None:
        logger.error("❌ Не вдалося визначити chat_id, PDF не буде відправлено.")
        return  # Вихід із функції, щоб уникнути помилки

    # Використовуємо кеш, якщо файл не старіший за 10 хвилин
    if os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE)) < CACHE_DURATION:
        try:
            await context.bot.send_document(
                chat_id=chat_id,
                document=open(CACHE_FILE, "rb"),
                filename=CACHE_FILE,
                caption=f"Ось всі {status_filter} записи у форматі PDF (кешовано)"
            )
            return
        except Exception as e:
            logger.error(f"❌ Не вдалося відправити кешований PDF: {e}")

    # Отримуємо записи з Firestore
    all_bookings = db.collection("bookings").where("status", "==", status_filter).stream()
    records = [doc.to_dict() for doc in all_bookings]

    if not records:
        await context.bot.send_message(chat_id, f"⚠️ Немає записів зі статусом {status_filter}.")
        return

    # Генерація PDF
    pdf = FPDF()
    pdf.add_page()

    # Вказуємо шлях до шрифту
    font_path = r"C:\Users\reset\OneDrive\Робочий стіл\vps server\dejavu-fonts-ttf-2.37\ttf\DejaVuSansCondensed.ttf"
    if not os.path.exists(font_path):
        logger.error(f"⚠️ Шрифт не знайдено за шляхом: {font_path}")
        await context.bot.send_message(chat_id, "⚠️ Помилка: файл шрифту не знайдено.")
        return

    # Додаємо шрифт
    pdf.add_font("DejaVu", "", font_path, uni=True)
    pdf.set_font("DejaVu", "", 10)

    # Заголовок
    pdf.cell(0, 10, f"Записи зі статусом: {status_filter}", ln=True, align="C")
    pdf.ln(5)

    # Таблиця
    headers = ["Ім'я", "Послуга", "Дата", "Час", "Ціна"]
    col_widths = [40, 50, 25, 20, 20]

    pdf.set_font("DejaVu", "", 9)
    for i, header in enumerate(headers):
        pdf.cell(col_widths[i], 8, header, border=1, align="C")
    pdf.ln()

    # Заповнення таблиці
    for b in records:
        row = [
            str(b.get("username", "-")),
            str(b.get("service", "-")),
            str(b.get("date", "--.--.----")),
            str(b.get("time", "--:--")),
            str(b.get("price", "-"))
        ]
        for i, item in enumerate(row):
            pdf.cell(col_widths[i], 8, item, border=1, align="C")
        pdf.ln()

    # Збереження PDF
    pdf.output(CACHE_FILE, "F")

    # Відправка у Telegram
    try:
        await context.bot.send_document(
            chat_id=chat_id,
            document=open(CACHE_FILE, "rb"),
            filename=CACHE_FILE,
            caption=f"Ось всі {status_filter} записи у форматі PDF"
        )
    except Exception as e:
        logger.error(f"❌ Не вдалося відправити PDF: {e}")
        
        

    
# 1. Головне меню "Клієнти"
async def show_client_menu(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("🔍 Пошук клієнта", callback_data="client_search")],
        [InlineKeyboardButton("📖 Перегляд історії візитів", callback_data="client_history_menu")],
        [InlineKeyboardButton("🚫 Додати до чорного списку", callback_data="client_blacklist_menu")],
        [InlineKeyboardButton("🔙 Назад до Головного Меню", callback_data="back_to_main_menu_text")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("👤 Меню клієнтів:", reply_markup=reply_markup)
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, "👤 Меню клієнтів:", reply_markup=reply_markup)

# 2. Функції-підказки для пошуку, історії та чорного списку
async def client_search_prompt(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    ADMIN_STATE[user_id] = "CLIENT_SEARCH_INPUT"
    if update.message:
        await update.message.reply_text("🔎 Введіть ім'я або номер телефону клієнта для пошуку:")
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, "🔎 Введіть ім'я або номер телефону клієнта для пошуку:")

async def client_history_prompt(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    ADMIN_STATE[user_id] = "CLIENT_HISTORY_INPUT"
    if update.message:
        await update.message.reply_text("🔎 Введіть ім'я або номер телефону клієнта для перегляду історії візитів:")
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, "🔎 Введіть ім'я або номер телефону клієнта для перегляду історії візитів:")

async def client_blacklist_prompt(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    ADMIN_STATE[user_id] = "CLIENT_BLACKLIST_INPUT"
    if update.message:
        await update.message.reply_text("🔎 Введіть ім'я або номер телефону клієнта для додавання до чорного списку:")
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, "🔎 Введіть ім'я або номер телефону клієнта для додавання до чорного списку:")

# 3. Обробка текстового вводу для пошуку/історії/чорного списку
async def handle_client_text_states(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    current_state = ADMIN_STATE.get(user_id)

    # 3.1 Пошук клієнта
    if current_state == "CLIENT_SEARCH_INPUT":
        query_text = text.lower()
        users = list(db.collection("users").stream())
        matched_users = []
        for doc in users:
            data = doc.to_dict()
            fields = []
            if "username" in data:
                fields.append(data["username"])
            if "first_name" in data:
                fields.append(data["first_name"])
            if "last_name" in data:
                fields.append(data["last_name"])
            if "phone" in data:
                fields.append(data["phone"])
            combined = " ".join(x for x in fields if x is not None).lower()
            if query_text in combined:
                matched_users.append((doc.id, data))
        if not matched_users:
            await update.message.reply_text("❌ Клієнта не знайдено. Спробуйте ще раз.")
            # НЕ скидайте стан пошуку – залиште його, щоб користувач міг повторити запит
            return
        # Зберігаємо результати пошуку
        context.user_data["last_search_results"] = matched_users

        keyboard = []
        for idx, (client_id, data) in enumerate(matched_users, start=1):
            display_name = data.get("username") or (data.get("first_name", "") + " " + data.get("last_name", ""))
            phone = data.get("phone", "Немає")
            if data.get("blacklisted", False):
                display_name = "⚠️ " + display_name
            btn_text = f"{idx}. {display_name} — {phone}"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"client_details_{client_id}")])
        # Якщо потрібно, можете додати кнопку для повернення до головного меню
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="client_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("👥 Знайдені клієнти:", reply_markup=reply_markup)
        ADMIN_STATE[user_id] = None
        return

    # 3.2 Перегляд історії клієнта
    if current_state == "CLIENT_HISTORY_INPUT":
        query_text = text.lower()
        users = list(db.collection("users").stream())
        matched_users = []

        for doc in users:
            data = doc.to_dict()
            fields = []
            if "username" in data:
                fields.append(data["username"])
            if "first_name" in data:
                fields.append(data["first_name"])
            if "last_name" in data:
                fields.append(data["last_name"])
            if "phone" in data:
                fields.append(data["phone"])
            combined = " ".join(x for x in fields if x).lower()

            if query_text in combined:
                matched_users.append((doc.id, data))

        if not matched_users:
            await update.message.reply_text("❌ Клієнта не знайдено. Спробуйте ще раз.")
            # ВАЖЛИВО: не скидаємо ADMIN_STATE[user_id], щоби користувач міг знову ввести
            return

        # Якщо ми дійшли сюди – є збіги, можна скинути стан, щоб пошук більше не повторювався
        ADMIN_STATE[user_id] = None

        # Формуємо список кнопок
        keyboard = []
        for idx, (client_id, data) in enumerate(matched_users, start=1):
            display_name = data.get("username") or (data.get("first_name", "") + " " + data.get("last_name", ""))
            phone = data.get("phone", "Немає")
            if data.get("blacklisted", False):
                display_name = "⚠️ " + display_name
            btn_text = f"{idx}. {display_name} — {phone}"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"client_history_{client_id}")])

        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="client_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("👥 Знайдені клієнти для перегляду історії:", reply_markup=reply_markup)


    # 3.3 Пошук клієнта для додавання до чорного списку
    if current_state == "CLIENT_BLACKLIST_INPUT":
        query_text = text.lower()
        users = list(db.collection("users").stream())
        matched_users = []
        for doc in users:
            data = doc.to_dict()
            fields = []
            if "username" in data:
                fields.append(data["username"])
            if "first_name" in data:
                fields.append(data["first_name"])
            if "last_name" in data:
                fields.append(data["last_name"])
            if "phone" in data:
                fields.append(data["phone"])
            combined = " ".join(x for x in fields if x is not None).lower()
            if query_text in combined:
                matched_users.append((doc.id, data))
        if not matched_users:
            await update.message.reply_text("❌ Клієнта не знайдено. Спробуйте ще раз.")
            return
        # ... далі формування клавіатури та відправлення результатів

        # Зберігаємо результати пошуку
        keyboard = []
        for idx, (client_id, data) in enumerate(matched_users, start=1):
            display_name = data.get("username") or (data.get("first_name", "") + " " + data.get("last_name", ""))
            phone = data.get("phone", "Немає")
            if data.get("blacklisted", False):
                display_name = "⚠️ " + display_name
            btn_text = f"{idx}. {display_name} — {phone}"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"client_blacklist_details_{client_id}")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="client_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("👥 Знайдені клієнти для додавання до чорного списку:", reply_markup=reply_markup)
        ADMIN_STATE[user_id] = None
        return


# 4. Відображення деталей клієнта з меню
async def client_details(update: Update, context: CallbackContext, client_id: str):
    doc_ref = db.collection("users").document(client_id)
    doc = doc_ref.get()
    if not doc.exists:
        await safe_edit_message_text(update.callback_query, "Клієнт не знайдено.")
        return
    data = doc.to_dict()
    display_name = data.get("username") or (data.get("first_name", "") + " " + data.get("last_name", ""))
    phone = data.get("phone", "Немає")
    blacklisted = data.get("blacklisted", False)
    
    message = f"Детальна інформація про клієнта:\n\nІм'я: {display_name}\nТелефон: {phone}\n"
    
    keyboard = [
        [InlineKeyboardButton("📖 Перегляд історії", callback_data=f"client_history_{client_id}")]
    ]
    if blacklisted:
        keyboard.append([InlineKeyboardButton("Видалити з чорного списку", callback_data=f"client_blacklist_remove_{client_id}")])
    else:
        keyboard.append([InlineKeyboardButton("Додати до чорного списку", callback_data=f"client_blacklist_confirm_{client_id}")])
    
    # Якщо в context.user_data збережено результати пошуку – повертаємося до них, інакше до головного меню
    back_callback = "client_search_results" if context.user_data.get("last_search_results") else "client_menu"
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=back_callback)])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(update.callback_query, message, reply_markup=reply_markup)

async def show_client_search_results(update: Update, context: CallbackContext):
    query = update.callback_query
    results = context.user_data.get("last_search_results")
    if not results:
        await query.edit_message_text("Результати пошуку відсутні. Будь ласка, виконайте пошук знову.")
        ADMIN_STATE[update.effective_user.id] = "CLIENT_SEARCH_INPUT"
        return
    
    doc = db.collection("bookings").document(rec_id).get()
    if not doc.exists:
        logger.warning(f"Booking document not found: {rec_id}")
        await safe_edit_message_text(query, "Запис не знайдено або видалено.")
        return


    keyboard = []
    for idx, (client_id, data) in enumerate(results, start=1):
        display_name = data.get("username") or (data.get("first_name", "") + " " + data.get("last_name", ""))
        phone = data.get("phone", "Немає")
        if data.get("blacklisted", False):
            display_name = "⚠️ " + display_name
        btn_text = f"{idx}. {display_name} — {phone}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"client_details_{client_id}")])
    # Додаємо кнопку "🔙 Назад", яка повертає до головного меню клієнтів
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="client_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text("👥 Результати пошуку:", reply_markup=reply_markup, parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error editing message: {e}")
        # Якщо редагування не вдалося, можна надіслати нове повідомлення як резервний варіант
        await query.message.reply_text("👥 Результати пошуку:", reply_markup=reply_markup, parse_mode="MarkdownV2")

# 5. Перегляд історії візитів клієнта
async def client_history_details(update: Update, context: CallbackContext, client_id: str):
    doc_ref = db.collection("users").document(client_id)
    doc = doc_ref.get()
    if not doc.exists:
        await safe_edit_message_text(update.callback_query, "Клієнт не знайдено.")
        return
    client_data = doc.to_dict()
    display_name = client_data.get("username") or (client_data.get("first_name", "") + " " + client_data.get("last_name", ""))
    user_id_val = client_data.get("user_id")
    bookings = []
    for booking in db.collection("bookings").where("user_id", "==", user_id_val).stream():
        bookings.append(booking.to_dict())
    if not bookings:
        message = f"ℹ️ Історія візитів для {display_name} відсутня."
    else:
        message = f"📖 Історія візитів {display_name}:\n"
        total_visits = 0
        total_spent = 0
        for b in bookings:
            message += f"✅ {b.get('date', '??.??.????')} — {b.get('service', 'Послуга')}\n"
            total_visits += 1
            price_str = str(b.get("price", "0"))
            price_num = ''.join(filter(str.isdigit, price_str))
            try:
                total_spent += int(price_num)
            except:
                pass
        message += f"\n📊 Загальна кількість візитів: {total_visits}\n💰 Загальна сума витрат: {total_spent} грн"
    # Повертаємо користувача до головного меню
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="client_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(update.callback_query, message, reply_markup=reply_markup)

    

# 6. Перевірка та підтвердження додавання клієнта до чорного списку
async def client_blacklist_details(update: Update, context: CallbackContext, client_id: str):
    doc_ref = db.collection("users").document(client_id)
    doc = doc_ref.get()
    if not doc.exists:
        await safe_edit_message_text(update.callback_query, "Клієнт не знайдено.")
        return
    data = doc.to_dict()
    display_name = data.get("username") or (data.get("first_name", "") + " " + data.get("last_name", ""))
    phone = data.get("phone", "Немає")
    blacklisted = data.get("blacklisted", False)
    message = f"Детальна інформація про клієнта:\n\nІм'я: {display_name}\nТелефон: {phone}\n"
    if blacklisted:
        keyboard = [
            [InlineKeyboardButton("Видалити з чорного списку", callback_data=f"client_blacklist_remove_{client_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="client_menu")]
        ]
    else:
        keyboard = [
            [InlineKeyboardButton("✅ Так", callback_data=f"client_blacklist_confirm_{client_id}"),
             InlineKeyboardButton("❌ Ні", callback_data=f"client_blacklist_cancel_{client_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="client_menu")]
        ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_edit_message_text(update.callback_query, message, reply_markup=reply_markup)

# 7. Callback для підтвердження/скасування додавання до чорного списку
async def handle_client_blacklist_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    data = query.data
    # Отримуємо ID клієнта з callback_data (останній елемент)
    client_id = data.split("_")[-1]
    # Завантажуємо дані клієнта
    doc_ref = db.collection("users").document(client_id)
    doc = doc_ref.get()
    if not doc.exists:
        await safe_edit_message_text(query, "Клієнт не знайдено.")
        return
    client_data = doc.to_dict()
    display_name = client_data.get("username") or (client_data.get("first_name", "") + " " + client_data.get("last_name", ""))
    
    # Якщо користувач хоче додати клієнта до чорного списку
    if data.startswith("client_blacklist_confirm_") and not data.startswith("client_blacklist_confirm_yes_"):
        message = f"Ви дійсно хочете додати {display_name} до чорного списку?"
        keyboard = [
            [InlineKeyboardButton("✅ Так", callback_data=f"client_blacklist_confirm_yes_{client_id}"),
             InlineKeyboardButton("❌ Ні", callback_data=f"client_blacklist_confirm_no_{client_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_edit_message_text(query, message, reply_markup=reply_markup)
        return

    if data.startswith("client_blacklist_confirm_yes_"):
        doc_ref.update({"blacklisted": True})
        try:
            await query.message.delete()
        except Exception as e:
            logger.error(f"Error deleting confirmation message: {e}")
        detail_message = f"{display_name} успішно додано до чорного списку."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"client_details_{client_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(chat_id=query.message.chat_id, text=detail_message, reply_markup=reply_markup)
        return
    
    if data.startswith("client_blacklist_confirm_no_"):
        client_id = data.split("_")[-1]
        try:
            await query.message.delete()  # Видаляємо клавіатуру підтвердження
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
        # Негайно повертаємо користувача до деталей клієнта
        await client_details(update, context, client_id)
    


        # Надсилаємо повідомлення про скасування з поверненням до деталей користувача
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="❌ Операцію скасовано.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data=f"client_details_{client_id}")]
            ])
        )
        return

# Якщо користувач хоче видалити клієнта з чорного списку
    if data.startswith("client_blacklist_remove_") and not data.startswith("client_blacklist_remove_yes_"):
        message = f"Ви дійсно хочете видалити {display_name} з чорного списку?"
        keyboard = [
            [InlineKeyboardButton("✅ Так", callback_data=f"client_blacklist_remove_yes_{client_id}"),
            InlineKeyboardButton("❌ Ні", callback_data=f"client_blacklist_remove_no_{client_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_edit_message_text(query, message, reply_markup=reply_markup)
        return


    if data.startswith("client_blacklist_remove_yes_"):
        doc_ref.update({"blacklisted": False})
        try:
            await query.message.delete()
        except Exception as e:
            logger.error(f"Error deleting confirmation message: {e}")
        detail_message = f"{display_name} успішно видалено з чорного списку."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"client_details_{client_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(chat_id=query.message.chat_id, text=detail_message, reply_markup=reply_markup)
        return

    if data.startswith("client_blacklist_remove_no_"):
        client_id = data.split("_")[-1]
        try:
            await query.message.delete()  # Видаляємо повідомлення з підтвердженням
        except Exception as e:
            logger.error(f"Error deleting confirmation message: {e}")
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="❌ Операцію скасовано.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data=f"client_details_{client_id}")]
            ])
        )
        return

        
    # Якщо користувач хоче видалити клієнта з чорного списку
    if data.startswith("client_blacklist_remove_") and not data.startswith("client_blacklist_remove_yes_"):
        message = f"Ви дійсно хочете видалити {display_name} з чорного списку?"
        keyboard = [
            [InlineKeyboardButton("✅ Так", callback_data=f"client_blacklist_remove_yes_{client_id}"),
             InlineKeyboardButton("❌ Ні", callback_data=f"client_blacklist_remove_no_{client_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_edit_message_text(query, message, reply_markup=reply_markup)
        return

    if data.startswith("client_blacklist_remove_yes_"):
        doc_ref.update({"blacklisted": False})
        try:
            await query.message.delete()
        except Exception as e:
            logger.error(f"Error deleting confirmation message: {e}")
        detail_message = f"{display_name} успішно видалено з чорного списку."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"client_details_{client_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await context.bot.send_message(chat_id=query.message.chat_id, text=detail_message, reply_markup=reply_markup)
        return

    if data.startswith("client_blacklist_remove_no_"):
        client_id = data.split("_")[-1]
        try:
            await query.message.delete()  # Видаляємо повідомлення з підтвердженням
        except Exception as e:
            logger.error(f"Error deleting confirmation message: {e}")
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="❌ Операцію скасовано.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Назад", callback_data=f"client_details_{client_id}")]
            ])
        )
        return

        # Надсилаємо нове повідомлення
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=cancel_message,
            reply_markup=reply_markup
        )
        return


# ========= РОЗСИЛКА =========
async def handle_broadcast_message(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS and admin_broadcast_mode.get(user_id, False):
        broadcast_text = update.message.text

        # Зберігаємо текст розсилки для подальшого використання
        context.user_data["broadcast_message"] = broadcast_text
        ADMIN_STATE[user_id] = "BROADCAST_CONFIRM"

        keyboard = [
            [
                InlineKeyboardButton("✅ Так", callback_data="confirm_broadcast_yes"),
                InlineKeyboardButton("❌ Ні", callback_data="confirm_broadcast_no")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"Все правильно, можна розсилати:\n\n{broadcast_text}",
            reply_markup=reply_markup
        )
        
        # ------------------ ФУНКЦІЇ ДЛЯ СТАТИСТИКИ ------------------

def parse_date_range(text: str):
    # Розбиваємо рядок за дефісом, допускаючи пробіли навколо нього
    parts = re.split(r'\s*-\s*', text)
    if len(parts) != 2:
        return None
    try:
        start_date = datetime.strptime(parts[0].strip(), "%d.%m.%Y").date()
        end_date = datetime.strptime(parts[1].strip(), "%d.%m.%Y").date()
        return start_date, end_date
    except Exception:
        return None


# Меню статистики – додано окремі варіанти для кастомного вибору кожного звіту
async def show_statistics_menu(update: Update, context: CallbackContext):
    keyboard = [
        [InlineKeyboardButton("✅ Кількість записів", callback_data="stat_count")],
        [InlineKeyboardButton("📌 Топ послуг", callback_data="stat_top")],
        [InlineKeyboardButton("💰 Середній чек", callback_data="stat_avg")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("Виберіть звіт:", reply_markup=reply_markup)
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, "Виберіть звіт:", reply_markup=reply_markup)


# Спочатку перевіряємо, чи це Custom
    if data == "stat_top_custom":
        ADMIN_STATE[user_id] = "STAT_TOP_INPUT"
        await safe_edit_message_text(
            query,
            "Введіть період для звіту *ТОП послуг* у форматі `DD.MM.YYYY-DD.MM.YYYY`",
            parse_mode="MarkdownV2"
        )
        return
    
    if data == "stat_count_custom":
        ADMIN_STATE[user_id] = "STAT_COUNT_INPUT"
        await safe_edit_message_text(query, "Введіть період для звіту *Кількість записів* у форматі `DD.MM.YYYY-DD.MM.YYYY`", parse_mode="MarkdownV2")
        return

    if data == "stat_avg_custom":
        ADMIN_STATE[user_id] = "STAT_AVG_INPUT"
        await safe_edit_message_text(query, "Введіть період для звіту *Середній чек* у форматі `DD.MM.YYYY-DD.MM.YYYY`", parse_mode="MarkdownV2")
        return


    if data == "stat_top_custom":
        # 1) Ставимо стан, щоб бот чекав текстового введення періоду
        ADMIN_STATE[user_id] = "STAT_TOP_INPUT"
        await safe_edit_message_text(
            query,
            "Введіть період для звіту *ТОП послуг* у форматі `DD.MM.YYYY-DD.MM.YYYY`",
            parse_mode="MarkdownV2"
        )
        return


async def statistics_callback_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    
    

    if data == "stat_menu":
        await show_statistics_menu(update, context)
        return
    if data == "stat_count":
        # Показ preset-періодів для "Кількість записів"
        await show_count_periods(update, context)
        return
    if data == "stat_avg":
        # Показ preset-періодів для "Середній чек"
        await show_avg_periods(update, context)
        return
    if data == "stat_top":
        await show_top_services_periods(update, context)
        return

    # Обробка Custom для ТОП послуг, Кількість записів та Середнього чеку:
    if data == "stat_top_custom":
        ADMIN_STATE[user_id] = "STAT_TOP_INPUT"
        await safe_edit_message_text(
            query,
            "Введіть період для звіту *ТОП послуг* у форматі `DD.MM.YYYY-DD.MM.YYYY`",
            parse_mode="MarkdownV2"
        )
        return

    if data == "stat_count_custom":
        ADMIN_STATE[user_id] = "STAT_COUNT_INPUT"
        await safe_edit_message_text(
            query,
            "Введіть період для звіту *Кількість записів* у форматі `DD.MM.YYYY-DD.MM.YYYY`",
            parse_mode="MarkdownV2"
        )
        return

    if data == "stat_avg_custom":
        ADMIN_STATE[user_id] = "STAT_AVG_INPUT"
        await safe_edit_message_text(
            query,
            "Введіть період для звіту *Середній чек* у форматі `DD.MM.YYYY-DD.MM.YYYY`",
            parse_mode="MarkdownV2"
        )
        return

    # Обробка preset-періодів:
    if data.startswith("stat_top_"):
        try:
            days = int(data.split("_")[-1])
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            await process_stat_top_input_with_dates(update, context, start_date, end_date)
            return
        except ValueError:
            await query.answer("Невірний формат періоду.")
            return

    if data.startswith("stat_count_"):
        try:
            days = int(data.split("_")[-1])
        except ValueError:
            await query.answer("Невірний формат періоду.")
            return
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        await process_stat_count_input_with_dates(update, context, start_date, end_date)
        return

    if data.startswith("stat_avg_"):
        try:
            days = int(data.split("_")[-1])
        except ValueError:
            await query.answer("Невірний формат періоду.")
            return
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        await process_stat_avg_input_with_dates(update, context, start_date, end_date)
        return
    
    # ============ ПАГІНАЦІЯ ЗАПИСІВ (ОСНОВНА ЗМІНА!) ============
    # (// CHANGE) Тут не звертаємось у БД, а беремо раніше збережений список
    # Якщо натиснули «Ще записи» / «Попередня»
    if data.startswith("records_page_"):
        try:
            _, page_and_return = data.split("records_page_", 1)
            page_str, return_callback = page_and_return.split("|", 1)
            page = int(page_str)
        except ValueError:
            await query.answer("⚠️ Помилка формату пагінації")
            return

        # Отримуємо список записів, який збережено в контексті
        records = context.user_data.get("current_records", [])

        if not records:
            await query.answer("⚠️ Немає записів для відображення.")
            return

        # Викликаємо функцію відображення потрібної сторінки
        await display_records_list(update, context, records, return_callback, page=page, page_size=10)



    # Якщо інші колбеки – додайте тут

    await query.answer("Невідомий callback_data!")

    if data.startswith("record_details_"):
        await view_record_details(update, context)
        return

    # Обробка записів для адміністратора
    if data == "admin_records_confirmed":
        records = await get_admin_records_cached(context, "confirmed", past=False)
        await display_records_list(update, context, records, "admin_bookings_menu")
        return

    if data == "admin_records_rejected":
        records = await get_admin_records_cached(context, "rejected", past=False)
        await display_records_list(update, context, records, "admin_bookings_menu")
        return

    if data == "admin_records_past":
        records = await get_admin_records_cached(context, "confirmed", past=True)
        await display_records_list(update, context, records, "admin_bookings_menu")
        return
    # Обробка записів для клієнтів
    if data == "client_records_confirmed":
        records = await get_client_records_cached(context, user_id, "confirmed", past=False)
        await display_records_list(update, context, records, "client_menu", page=0, page_size=10)
        return
    if data == "client_records_pending":
        records = await get_client_records_cached(context, user_id, "pending", past=False)
        await display_records_list(update, context, records, "client_menu", page=0, page_size=10)
        return
    if data == "client_records_past":
        records = await get_client_records_cached(context, user_id, "confirmed", past=True)
        await display_records_list(update, context, records, "client_menu", page=0, page_size=10)
        return

    # Обробка вибору деталей запису
    if data.startswith("record_details_"):
        await view_record_details(update, context)
        return

    # Якщо натиснуто "За датою" (адміністратор)
    if data == "admin_pick_date_for_bookings":
        ADMIN_STATE[user_id] = "ADMIN_WAITING_DATE_FOR_RECORDS"
        await safe_edit_message_text(query, "Введіть дату у форматі ДД.ММ.РРРР для перегляду записів:")
        return

    # Обробка для введення дати адміністратором (callback текстом)
    if data.startswith("admin_show_records_for_date_"):
        # Формат callback: "admin_show_records_for_date_{date}"
        date_str = data.split("_")[-1]
        records = await get_admin_records("confirmed", past=False)
        # Фільтруємо за датою:
        filtered = [(rid, rec) for rid, rec in records if rec.get("date") == date_str]
        await display_records_list(update, context, filtered, "admin_bookings_menu")
        return

    # Кнопка "НАЗАД" для повернення до головного меню записів
    if data in ["back_to_records_menu"]:
        await show_records_menu(update, context)
        return

    # Якщо не підходить жодна умова – повертаємо повідомлення
    await query.answer("Невідома команда, спробуйте ще раз.")
    
    # Для обробки текстового вводу адміністратором при виборі дати записів
async def handle_admin_records_text(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    current_state = ADMIN_STATE.get(user_id)
    if current_state == "ADMIN_WAITING_DATE_FOR_RECORDS":
        # Очікуємо дату у форматі ДД.ММ.РРРР
        date_pattern = r"^\d{2}\.\d{2}\.\d{4}$"
        if not re.match(date_pattern, text):
            await update.message.reply_text("Невірний формат дати. Спробуйте ще раз (ДД.ММ.РРРР).")
            return
        ADMIN_STATE[user_id] = None
        records = await get_admin_records("confirmed", past=False)
        filtered = [(rid, rec) for rid, rec in records if rec.get("date") == text]
        await display_records_list(update, context, filtered, "admin_bookings_menu")
        return
    


    # Обробка Custom для "Кількість записів"
    if data == "stat_count_custom":
        ADMIN_STATE[user_id] = "STAT_COUNT_INPUT"
        await safe_edit_message_text(query, "Введіть період для звіту *Кількість записів* у форматі `DD.MM.YYYY-DD.MM.YYYY`", parse_mode="MarkdownV2")
        return

    # Обробка Custom для "Середній чек"
    if data == "stat_avg_custom":
        ADMIN_STATE[user_id] = "STAT_AVG_INPUT"
        await safe_edit_message_text(query, "Введіть період для звіту *Середній чек* у форматі `DD.MM.YYYY-DD.MM.YYYY`", parse_mode="MarkdownV2")
        return


async def process_stat_count_input_with_dates(update: Update, context: CallbackContext, start_date: datetime.date, end_date: datetime.date):
    all_bookings = [doc.to_dict() for doc in db.collection("bookings").stream()]
    count_confirmed = 0
    count_canceled = 0
    user_min_date = {}
    for booking in all_bookings:
        try:
            b_date = datetime.strptime(booking.get("date", ""), "%d.%m.%Y").date()
        except Exception:
            continue
        if start_date <= b_date <= end_date:
            status = booking.get("status", "").lower()
            if status == "confirmed":
                count_confirmed += 1
            if status in ["canceled", "rejected"]:
                count_canceled += 1
            uid = booking.get("user_id")
            if uid:
                if uid in user_min_date:
                    if b_date < user_min_date[uid]:
                        user_min_date[uid] = b_date
                else:
                    user_min_date[uid] = b_date
    new_clients = sum(1 for uid, min_date in user_min_date.items() if min_date >= start_date)

    message = (
        f"📅 *Звіт за період* {start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}\n\n"
        f"🔹 *Записи:* {count_confirmed}\n"
        f"❌ *Скасовані візити:* {count_canceled}\n"
        f"🆕 *Нові клієнти:* {new_clients}"
    )
    message_escaped = escape_markdown(message, version=2)
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="stat_menu")],
        [InlineKeyboardButton("📆 Обрати інший період", callback_data="stat_count")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text(message_escaped, reply_markup=reply_markup, parse_mode="MarkdownV2")
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, message_escaped, reply_markup=reply_markup, parse_mode="MarkdownV2")


async def process_stat_avg_input_with_dates(update: Update, context: CallbackContext, start_date: datetime.date, end_date: datetime.date):
    all_bookings = [doc.to_dict() for doc in db.collection("bookings").stream()]
    total_revenue = 0
    count = 0
    for booking in all_bookings:
        try:
            b_date = datetime.strptime(booking.get("date", ""), "%d.%m.%Y").date()
        except Exception:
            continue
        if start_date <= b_date <= end_date and booking.get("status", "").lower() == "confirmed":
            price_str = booking.get("price", "0")
            digits = "".join(ch for ch in price_str if ch.isdigit())
            if digits:
                total_revenue += int(digits)
                count += 1
    avg = total_revenue / count if count > 0 else 0

    message = (
        f"💰 Середній чек за період {start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}:\n\n"
        f"📊 Дохід: {total_revenue} євро\n"
        f"📝 Кількість записів: {count}\n"
        f"📈 Середній чек: {avg:.2f} євро"
    )
    message_escaped = escape_markdown(message, version=2)
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="stat_menu")],
        [InlineKeyboardButton("📆 Обрати інший період", callback_data="stat_avg")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text(message_escaped, reply_markup=reply_markup, parse_mode="MarkdownV2")
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, message_escaped, reply_markup=reply_markup, parse_mode="MarkdownV2")


async def process_stat_top_input_with_dates(update: Update, context: CallbackContext, start_date: datetime.date, end_date: datetime.date):
    all_bookings = [doc.to_dict() for doc in db.collection("bookings").stream()]
    service_counts = {}
    for booking in all_bookings:
        try:
            b_date = datetime.strptime(booking.get("date", ""), "%d.%m.%Y").date()
        except Exception:
            continue
        if start_date <= b_date <= end_date and booking.get("status", "").lower() == "confirmed":
            service_name = booking.get("service", "Невідома послуга")
            service_counts[service_name] = service_counts.get(service_name, 0) + 1

    top_services = sorted(service_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    message = f"🏆 *ТОП популярних послуг* за період {start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}:\n\n"
    if not top_services:
        message += "Немає даних."
    else:
        rank_emojis = ["1️⃣", "2️⃣", "3️⃣"]
        for i, (service, cnt) in enumerate(top_services):
            message += f"{rank_emojis[i]} {service} – {cnt} записів\n"

    # Екрануємо повідомлення для MarkdownV2
    message_escaped = escape_markdown(message, version=2)
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="stat_menu")],
        [InlineKeyboardButton("📆 Обрати інший період", callback_data="stat_top")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.message:
        await update.message.reply_text(message_escaped, reply_markup=reply_markup, parse_mode="MarkdownV2")
    elif update.callback_query:
        await safe_edit_message_text(update.callback_query, message_escaped, reply_markup=reply_markup, parse_mode="MarkdownV2")


async def process_stat_avg_input(update: Update, context: CallbackContext):
    """
    Обробляє введення періоду для звіту "Середній чек". Завантажує confirmed записи, сумує доход,
    підраховує кількість та обчислює середній чек.
    """
    text_input = update.message.text.strip()
    period = parse_date_range(text_input)
    if not period:
        await update.message.reply_text("Невірний формат періоду. Спробуйте ще раз у форматі DD.MM.YYYY-DD.MM.YYYY.")
        return
    start_date, end_date = period

    all_bookings = [doc.to_dict() for doc in db.collection("bookings").stream()]
    total_revenue = 0
    count = 0
    for booking in all_bookings:
        try:
            b_date = datetime.strptime(booking.get("date", ""), "%d.%m.%Y").date()
        except Exception:
            continue
        if start_date <= b_date <= end_date and booking.get("status", "").lower() == "confirmed":
            price_str = booking.get("price", "0")
            digits = "".join(ch for ch in price_str if ch.isdigit())
            if digits:
                total_revenue += int(digits)
                count += 1

    avg = total_revenue / count if count > 0 else 0
    message = (
        f"💰 *Середній чек* за період {start_date.strftime('%d.%m.%Y')} – {end_date.strftime('%d.%m.%Y')}:\n\n"
        f"📊 *Дохід:* {total_revenue} євро\n"
        f"📝 *Кількість записів:* {count}\n"
        f"📈 *Середній чек:* {avg:.2f} євро"
    )
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="stat_menu")],
        [InlineKeyboardButton("📆 Обрати інший період", callback_data="stat_avg")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(message, reply_markup=reply_markup, parse_mode="MarkdownV2")


# ========= ГОЛОВНА ФУНКЦІЯ ЗАПУСКУ БОТА =========
def main():
    # 1) Завантажимо дефолтний розклад з БД (або створимо, якщо немає)
    load_default_schedule_from_db()
    # 2) Завантажимо заброньовані слоти
    load_booked_slots()
    # 3) Завантажимо (обновимо) кеш послуг
    refresh_services_cache()
    
        # Завантаження налаштувань, розкладу, бронювань, кешу послуг
    load_default_schedule_from_db()
    load_booked_slots()
    refresh_services_cache()
    
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Команди
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("kurs", kurs_command))
    application.add_handler(CommandHandler("all", all_command))
    application.add_handler(CommandHandler("cancel", cancel_command))

    # Меню користувача
    application.add_handler(MessageHandler(filters.Text(["Записатися на послугу"]), book_service))
    application.add_handler(MessageHandler(filters.Text(["КОСМЕТИКА"]), cosmetics))
    application.add_handler(MessageHandler(filters.Text(["ІСТОРІЯ"]), show_records_menu))
    application.add_handler(MessageHandler(filters.Text(["Про нас"]), about_us))
    

    
    

    # Меню адміна
    application.add_handler(MessageHandler(filters.Text(["📅 Записи"]), admin_bookings_menu))
    application.add_handler(MessageHandler(filters.Text(["👤 Клієнти"]), show_client_menu))
    # Для прикладу зробимо, що кнопка "📊 Статистика" відкриває admin_analytics (або show_clients — як було)
    application.add_handler(MessageHandler(filters.Text(["📊 Статистика"]), show_statistics_menu))
    application.add_handler(MessageHandler(filters.Text(["💇‍♂️ Налаштування послуг"]), show_services_settings_menu))
    application.add_handler(MessageHandler(filters.Text(["📆 Розклад"]), show_schedule_menu))
    application.add_handler(MessageHandler(filters.Text(["✉️ Повідомлення"]), show_messages))
    application.add_handler(MessageHandler(filters.Text(["⚙️ Налаштування бота"]), bot_settings))
    application.add_handler(CallbackQueryHandler(show_records, pattern=r"^records_(confirmed|rejected|past)$"))
    application.add_handler(CallbackQueryHandler(lambda update, context: send_filtered_records_pdf(update, context, update.callback_query.data.split("_")[2]), pattern=r"^all_records_.*$"))
    application.add_handler(CallbackQueryHandler(lambda update, context: handle_date_input(update, context), pattern=r"^records_by_date$"))
    application.add_handler(MessageHandler(filters.Regex(r"^\d{2}\.\d{2}\.\d{4}$"), handle_date_input))

    # Окремо хендлер на натискання кнопки "broadcast_message"
    application.add_handler(CallbackQueryHandler(broadcast_message_button, pattern="^broadcast_message$"))
    # Основний хендлер колбеків (усі інші callback_data)
    application.add_handler(CallbackQueryHandler(button_handler))

    # Текст (адмін-стейти + розсилка)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_admin_text_states))

    # JobQueue для нагадувань (щогодини)
    job_queue = application.job_queue
    job_queue.run_repeating(send_reminders, interval=3600, first=0)

    application.run_polling(timeout=60)


if __name__ == "__main__":
    main()
