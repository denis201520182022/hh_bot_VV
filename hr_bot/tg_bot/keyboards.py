from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List, Any

# --- Основные Reply-клавиатуры ---

# Клавиатура для обычного пользователя
user_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="⚙️ Баланс")],
        [KeyboardButton(text="❓ Помощь")]
    ],
    resize_keyboard=True
)

# Клавиатура для администратора
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

# --- Inline-клавиатуры ---

def create_stats_export_keyboard(period: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру для отчета со статистикой с кнопкой экспорта."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📥 Выгрузить в Excel", callback_data=f"export_stats_{period}")]
        ]
    )

# Клавиатура для отмены любого FSM-действия
cancel_fsm_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_fsm")]
    ]
)

# Клавиатура для выбора роли
role_choice_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Пользователь 🧑‍💻", callback_data="set_role_user"),
            InlineKeyboardButton(text="Администратор ✨", callback_data="set_role_admin")
        ]
    ]
)

# Клавиатура для меню управления балансом
limits_menu_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Установить баланс", callback_data="set_limit")],
        [InlineKeyboardButton(text="💰 Установить тарифы", callback_data="set_tariff")]
    ]
)

# Клавиатура с готовыми вариантами СУММ для баланса
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