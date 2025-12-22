from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List, Any

# --- Основные Reply-клавиатуры ---

user_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="⚙️ Баланс")],
        [KeyboardButton(text="❓ Помощь")]
    ],
    resize_keyboard=True
)

admin_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="⚙️ Баланс и Тариф")],
        [KeyboardButton(text="👤 Управление пользователями")],
        [KeyboardButton(text="👨‍💼 Управление рекрутерами")],
        [KeyboardButton(text="❓ Помощь")]
    ],
    resize_keyboard=True,
    input_field_placeholder="Выберите действие:"
)

# --- Inline-клавиатуры для Статистики ---

# Главное меню статистики
stats_main_menu_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="👁️ Посмотреть за 7 дней (текст)", callback_data="view_stats_7days")],
        [InlineKeyboardButton(text="📥 Выгрузить в Excel (выбор дат)", callback_data="export_excel_start")]
    ]
)

# Быстрые диапазоны для Excel
export_date_options_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="📅 Последние 7 дней", callback_data="export_range_7")],
        [InlineKeyboardButton(text="📅 Последние 14 дней", callback_data="export_range_14")],
        [InlineKeyboardButton(text="📅 Последние 30 дней", callback_data="export_range_30")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_fsm")]
    ]
)

# Функция-заглушка (чтобы common.py не ругался на импорт), если вдруг она где-то вызывается
def create_stats_export_keyboard(period: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📥 Скачать Excel", callback_data=f"export_stats_{period}")]
        ]
    )

# --- Остальные Inline-клавиатуры ---

cancel_fsm_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_fsm")]]
)

role_choice_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Пользователь 🧑‍💻", callback_data="set_role_user"),
            InlineKeyboardButton(text="Администратор ✨", callback_data="set_role_admin")
        ]
    ]
)

limits_menu_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Установить баланс", callback_data="set_limit")],
        [InlineKeyboardButton(text="💰 Установить тарифы", callback_data="set_tariff")]
    ]
)

limit_options_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="1000"), KeyboardButton(text="5000"), KeyboardButton(text="10000")],
        [KeyboardButton(text="❌ Отмена")]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

# --- Универсальная клавиатура управления ---

BUTTON_TEXTS = {
    "add_user": "➕ Добавить пользователя",
    "del_user": "➖ Удалить пользователя",
    "add_recruiter": "➕ Добавить рекрутера",
    "del_recruiter": "➖ Удалить рекрутера",
    "update_recruiter": "🔄 Обновить рекрутера",
}

def create_management_keyboard(items: List[Any], *actions: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for action in actions:
        text = BUTTON_TEXTS.get(action, action.replace('_', ' ').capitalize())
        builder.button(text=text, callback_data=action)
    builder.adjust(2)
    return builder.as_markup()