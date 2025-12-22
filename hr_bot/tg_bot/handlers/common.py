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

logger = logging.getLogger(__name__)
router = Router()

def _build_stats_content(stats_query, period_text: str) -> Text:
    """Собирает форматированный отчет с помощью конструктора aiogram."""
    if not stats_query:
        return Text("📊 Статистика ", Italic(period_text), " пока пуста.")
    
    total_responses, total_dialogs, total_qualified = 0, 0, 0
    content_parts = [Bold(f"📊 Статистика {period_text}"), "\n\n"]
    
    for stat in stats_query:
        total_responses += stat.total_responses or 0
        total_dialogs += stat.total_dialogs or 0
        total_qualified += stat.total_qualified or 0
        
        # Конструктор Bold() сам позаботится об экранировании спецсимволов в названии
        content_parts.extend([
            Bold(stat.title), ":\n",
            "  - Откликов: ", Bold(stat.total_responses or 0), "\n",
            "  - Диалогов: ", Bold(stat.total_dialogs or 0), "\n",
            "  - Квалифицировано: ", Bold(stat.total_qualified or 0), "\n\n"
        ])
    
    content_parts.extend([
        Bold("Итого по всем вакансиям:"), "\n",
        "  - Откликов: ", Bold(total_responses), "\n",
        "  - Диалогов: ", Bold(total_dialogs), "\n",
        "  - Квалифицировано: ", Bold(total_qualified)
    ])
    return Text(*content_parts)


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


@router.message(F.text == "📊 Статистика")
@router.message(Command("stats"))
async def handle_stats_command(message: Message, db_session: Session):
    if not db_session.query(TelegramUser).filter(TelegramUser.telegram_id == str(message.from_user.id)).first():
        return
    await message.answer("Выберите период для просмотра статистики:", reply_markup=stats_period_keyboard)


@router.callback_query(F.data == "stats_today")
async def process_stats_today(callback: CallbackQuery, db_session: Session):
    today = date.today()
    stats_query = db_session.query(
        Vacancy.title,
        func.sum(Statistic.responses_count).label('total_responses'),
        func.sum(Statistic.started_dialogs_count).label('total_dialogs'),
        func.sum(Statistic.qualified_count).label('total_qualified')
    ).join(Statistic).filter(Statistic.date == today).group_by(Vacancy.title).all()
    content = _build_stats_content(stats_query, f"за {today.strftime('%d.%m.%Y')}")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=create_stats_export_keyboard(period="today"))
    await callback.answer()


@router.callback_query(F.data == "stats_all_time")
async def process_stats_all_time(callback: CallbackQuery, db_session: Session):
    stats_query = db_session.query(
        Vacancy.title,
        func.sum(Statistic.responses_count).label('total_responses'),
        func.sum(Statistic.started_dialogs_count).label('total_dialogs'),
        func.sum(Statistic.qualified_count).label('total_qualified')
    ).join(Statistic).group_by(Vacancy.title).all()
    content = _build_stats_content(stats_query, "за всё время")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=create_stats_export_keyboard(period="all_time"))
    await callback.answer()


@router.callback_query(F.data.startswith("export_stats_"))
async def export_stats_to_excel(callback: CallbackQuery, db_session: Session):
    await callback.answer("Готовлю Excel-отчет...", show_alert=False)
    period = callback.data.split("_")[-1]
    today = date.today()
    query_builder = db_session.query(
        Vacancy.title.label('Вакансия'),
        func.sum(Statistic.responses_count).label('Количество откликов'),
        func.sum(Statistic.started_dialogs_count).label('Начато диалогов'),
        func.sum(Statistic.qualified_count).label('Прошли квалификацию')
    ).join(Statistic)
    if period == "today":
        query_builder = query_builder.filter(Statistic.date == today)
        filename = f"hr_stats_{today.strftime('%Y-%m-%d')}.xlsx"
    else:
        filename = "hr_stats_all_time.xlsx"
    stats_data = query_builder.group_by(Vacancy.title).all()
    if not stats_data:
        await callback.message.answer("Нет данных для экспорта.")
        return
    df = pd.DataFrame(stats_data)
    output_buffer = io.BytesIO()
    df.to_excel(output_buffer, index=False, sheet_name='Статистика')
    output_buffer.seek(0)
    file_to_send = BufferedInputFile(output_buffer.read(), filename=filename)
    
    # Конструктор Italic() сам позаботится об экранировании символов '_' в имени файла
    content = Text("Ваш отчет ", Italic(filename))
    await callback.message.answer_document(file_to_send, **content.as_kwargs())


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