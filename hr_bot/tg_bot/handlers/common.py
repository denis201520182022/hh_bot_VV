# hr_bot/tg_bot/handlers/common.py

import logging
import io
import pandas as pd
from datetime import date
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.filters import CommandStart, Command
from sqlalchemy import func
from sqlalchemy.orm import Session
# --- ПРАВИЛЬНЫЙ ИМПОРТ: Убираем все лишнее, оставляем только конструктор ---
from aiogram.utils.formatting import Text, Bold, Italic
from hr_bot.db.models import TelegramUser, Statistic, Vacancy, AppSettings
from hr_bot.tg_bot.keyboards import (
    user_keyboard, 
    admin_keyboard, 
    stats_period_keyboard, 
    create_stats_export_keyboard
)

from datetime import date, timedelta
from sqlalchemy import func, cast, Date
from hr_bot.db.models import (
    TelegramUser, Statistic, Vacancy, AppSettings, 
    InactiveNotificationQueue, RejectedNotificationQueue, NotificationQueue
)

logger = logging.getLogger(__name__)
router = Router()



@router.message(CommandStart())
async def handle_start(message: Message, db_session: Session):
    user_id = str(message.from_user.id)
    user = db_session.query(TelegramUser).filter(TelegramUser.telegram_id == user_id).first()
    if not user:
        await message.answer("❌ У вас нет доступа к этому боту.")
        return

    if user.role == 'admin':
        keyboard = admin_keyboard
        role_name = "Администратор ✨"
    else:
        keyboard = user_keyboard
        role_name = "Пользователь 🧑‍💻"
    
    # Конструктор Bold() сам позаботится об экранировании имени пользователя
    content = Text(
        "👋 Здравствуйте, ", Bold(message.from_user.first_name or "Пользователь"), "!\n\n",
        "Я бот для управления HR-статистикой.\n",
        Bold("Ваша роль:"), f" {role_name}"
    )
    await message.answer(**content.as_kwargs(), reply_markup=keyboard)



@router.message(F.text == "❓ Помощь")
@router.message(Command("help"))
async def handle_help(message: Message, db_session: Session):
    user = db_session.query(TelegramUser).filter(TelegramUser.telegram_id == str(message.from_user.id)).first()
    if not user: return
    if user.role == 'admin':
        help_text = (
            "*Руководство для Администратора:*\n\n"
            "Кнопки на клавиатуре предоставляют доступ ко всему функционалу:\n"
            "• *Статистика* - Просмотр статистики.\n"
            "• *Лимиты и Тариф* - Просмотр и управление лимитами.\n"
            "• *Управление пользователями* - Добавление/удаление пользователей бота.\n"
            "• *Управление вакансиями* - Добавление/удаление отслеживаемых вакансий hh.ru.\n"
            "• *Управление рекрутерами* - Добавление/удаление рекрутеров hh.ru и их токенов."
        )
    else:
        help_text = (
            "*Руководство для Пользователя:*\n\n"
            "• *Статистика* - Просмотр статистики за сегодня или за всё время.\n"
            "• *Лимиты* - Просмотр оставшихся лимитов.\n\n"
            "Вам автоматически будут приходить уведомления о новых кандидатах."
        )
    # help_text безопасен, так как не содержит пользовательского ввода
    await message.answer(help_text)




def _build_7day_stats_content(db_session: Session) -> Text:
    """Собирает отчет за последние 7 дней посуточно."""
    content_parts = [Bold("📅 Статистика за последние 7 дней:"), "\n\n"]
    
    # Генерируем список дат (от сегодня и назад)
    days = [date.today() - timedelta(days=i) for i in range(7)]
    
    has_any_data = False

    for day in days:
        # 1. Считаем отклики (из таблицы статистики)
        responses = db_session.query(func.sum(Statistic.responses_count))\
            .filter(Statistic.date == day).scalar() or 0
        
        # 2. Считаем молчунов (из очереди уведомлений о неактивности)
        # Используем cast, так как в очередях поле created_at — это DateTime, а нам нужен Date
        silents = db_session.query(func.count(InactiveNotificationQueue.id))\
            .filter(cast(InactiveNotificationQueue.created_at, Date) == day).scalar() or 0
            
        # 3. Считаем отказников (из очереди отказов)
        rejects = db_session.query(func.count(RejectedNotificationQueue.id))\
            .filter(cast(RejectedNotificationQueue.created_at, Date) == day).scalar() or 0
            
        # 4. Считаем подошедших (из основной очереди уведомлений)
        qualified = db_session.query(func.count(NotificationQueue.id))\
            .filter(cast(NotificationQueue.created_at, Date) == day).scalar() or 0

        # Если за день есть хоть какая-то активность, выводим его
        if any([responses, silents, rejects, qualified]):
            has_any_data = True
            day_str = day.strftime('%d.%m (%a)') # Например: 22.10 (Пн)
            content_parts.extend([
                Bold(f"🗓 {day_str}"), "\n",
                "  📥 Откликов: ", Bold(responses), "\n",
                "  🟢 Подошло: ", Bold(qualified), "\n",
                "  🔴 Отказов: ", Bold(rejects), "\n",
                "  😶 Молчунов: ", Bold(silents), "\n",
                "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            ])

    if not has_any_data:
        return Text("📊 За последние 7 дней данных пока нет.")

    return Text(*content_parts)

# --- Обновленный хендлер кнопки статистики ---
@router.callback_query(F.data == "stats_today") # Оставляем старый callback или меняем в клавиатуре
async def process_stats_7days(callback: CallbackQuery, db_session: Session):
    content = _build_7day_stats_content(db_session)
    # Используем edit_text для обновления меню
    await callback.message.edit_text(
        **content.as_kwargs(), 
        reply_markup=create_stats_export_keyboard(period="7days")
    )
    await callback.answer()