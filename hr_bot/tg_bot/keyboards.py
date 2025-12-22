# hr_bot/tg_bot/keyboards.py

from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton
)
# --- ИЗМЕНЕНО: Добавляем InlineKeyboardBuilder для гибкости ---
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List, Any

# --- Основные Reply-клавиатуры (остаются без изменений) ---

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
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="⚙️ Баланс и Тариф")], # <--- Должно быть так
        [KeyboardButton(text="👤 Управление пользователями")],
        [KeyboardButton(text="👨‍💼 Управление рекрутерами")],
        [KeyboardButton(text="❓ Помощь")]
    ],
    resize_keyboard=True,
    input_field_placeholder="Выберите действие:"
)


# --- Inline-клавиатуры (остаются без изменений) ---

# Клавиатура для первоначального выбора периода статистики
stats_period_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Последние 7 дней", callback_data="stats_today"),
            InlineKeyboardButton(text="🗓️ За всё время", callback_data="stats_all_time")
        ]
    ]
)

def create_stats_export_keyboard(period: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру для отчета со статистикой, включая кнопку для экспорта."""
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

# Клавиатура для выбора роли при добавлении пользователя через FSM
role_choice_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Пользователь 🧑‍💻", callback_data="set_role_user"),
            InlineKeyboardButton(text="Администратор ✨", callback_data="set_role_admin")
        ]
    ]
)

# Клавиатура для меню управления лимитами (только для админов)
limits_menu_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Установить баланс", callback_data="set_limit")],
        [InlineKeyboardButton(text="💰 Установить тариф", callback_data="set_tariff")]
    ]
)

# Клавиатура с готовыми вариантами лимитов
limit_options_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="50"), KeyboardButton(text="100"), KeyboardButton(text="150")],
        [KeyboardButton(text="❌ Отмена")]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

# --- ИЗМЕНЕНО: Полностью заменяем старую функцию на новую, гибкую ---

# Словарь для красивых текстов кнопок. Легко добавлять новые.
BUTTON_TEXTS = {
    "add_user": "➕ Добавить пользователя",
    "del_user": "➖ Удалить пользователя",
    "add_recruiter": "➕ Добавить рекрутера",
    "del_recruiter": "➖ Удалить рекрутера",
    "update_recruiter": "🔄 Обновить рекрутера",  # <-- Наша новая кнопка
}

def create_management_keyboard(items: List[Any], *actions: str) -> InlineKeyboardMarkup:
    """
    Создает гибкое inline-меню для управления.
    Принимает список 'items' (пока не используется) и переменное количество
    строк callback_data ('actions') для создания кнопок.
    """
    builder = InlineKeyboardBuilder()

    # Проходим по всем переданным названиям действий ('add_recruiter', 'del_recruiter' и т.д.)
    for action in actions:
        # Ищем текст для кнопки в словаре. Если не находим, используем само название действия.
        text = BUTTON_TEXTS.get(action, action.replace('_', ' ').capitalize())
        builder.button(text=text, callback_data=action)
        
    # Располагаем кнопки по 2 в ряд, если их больше двух.
    builder.adjust(2)
        
    return builder.as_markup()

# --- КОНЕЦ ИЗМЕНЕНИЯ ---