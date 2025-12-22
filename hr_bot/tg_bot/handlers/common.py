import logging
import io
import pandas as pd
from datetime import date, timedelta
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.filters import CommandStart, Command
from sqlalchemy import func, cast, Date
from sqlalchemy.orm import Session
from aiogram.utils.formatting import Text, Bold, Italic, Code

from hr_bot.db.models import (
    TelegramUser, Statistic, Vacancy, AppSettings, 
    InactiveNotificationQueue, RejectedNotificationQueue, NotificationQueue
)
from hr_bot.tg_bot.keyboards import (
    user_keyboard, 
    admin_keyboard, 
    create_stats_export_keyboard
)

logger = logging.getLogger(__name__)
router = Router()

def _build_7day_stats_content(db_session: Session) -> Text:
    """Собирает отчет за последние 7 дней посуточно по категориям."""
    content_parts = [Bold("📅 Статистика за последние 7 дней:"), "\n\n"]
    
    days = [date.today() - timedelta(days=i) for i in range(7)]
    has_any_data = False

    for day in days:
        # 1. Отклики
        responses = db_session.query(func.sum(Statistic.responses_count))\
            .filter(Statistic.date == day).scalar() or 0
        
        # 2. Молчуны
        silents = db_session.query(func.count(InactiveNotificationQueue.id))\
            .filter(cast(InactiveNotificationQueue.created_at, Date) == day).scalar() or 0
            
        # 3. Отказники
        rejects = db_session.query(func.count(RejectedNotificationQueue.id))\
            .filter(cast(RejectedNotificationQueue.created_at, Date) == day).scalar() or 0
            
        # 4. Подошедшие
        qualified = db_session.query(func.count(NotificationQueue.id))\
            .filter(cast(NotificationQueue.created_at, Date) == day).scalar() or 0

        if any([responses, silents, rejects, qualified]):
            has_any_data = True
            day_str = day.strftime('%d.%m (%a)')
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

@router.message(CommandStart())
async def handle_start(message: Message, db_session: Session):
    user_id = str(message.from_user.id)
    user = db_session.query(TelegramUser).filter(TelegramUser.telegram_id == user_id).first()
    if not user:
        await message.answer("❌ У вас нет доступа к этому боту.")
        return

    keyboard = admin_keyboard if user.role == 'admin' else user_keyboard
    role_name = "Администратор ✨" if user.role == 'admin' else "Пользователь 🧑‍💻"
    
    content = Text(
        "👋 Здравствуйте, ", Bold(message.from_user.first_name or "Пользователь"), "!\n\n",
        "Я бот для управления HR-процессами.\n",
        Bold("Ваша роль:"), f" {role_name}"
    )
    await message.answer(**content.as_kwargs(), reply_markup=keyboard)

@router.message(F.text == "📊 Статистика")
@router.message(Command("stats"))
async def handle_stats_command(message: Message, db_session: Session):
    user = db_session.query(TelegramUser).filter(TelegramUser.telegram_id == str(message.from_user.id)).first()
    if not user: return

    content = _build_7day_stats_content(db_session)
    await message.answer(
        **content.as_kwargs(), 
        reply_markup=create_stats_export_keyboard(period="7days")
    )

@router.callback_query(F.data == "stats_today")
async def process_stats_refresh(callback: CallbackQuery, db_session: Session):
    """Хендлер для обновления статистики (если кнопка будет в инлайне)"""
    content = _build_7day_stats_content(db_session)
    await callback.message.edit_text(
        **content.as_kwargs(), 
        reply_markup=create_stats_export_keyboard(period="7days")
    )
    await callback.answer()

@router.callback_query(F.data.startswith("export_stats_"))
async def export_stats_to_excel(callback: CallbackQuery, db_session: Session):
    await callback.answer("Готовлю Excel-отчет за неделю...", show_alert=False)
    
    data_for_excel = []
    days = [date.today() - timedelta(days=i) for i in range(7)]
    
    for day in days:
        responses = db_session.query(func.sum(Statistic.responses_count)).filter(Statistic.date == day).scalar() or 0
        silents = db_session.query(func.count(InactiveNotificationQueue.id)).filter(cast(InactiveNotificationQueue.created_at, Date) == day).scalar() or 0
        rejects = db_session.query(func.count(RejectedNotificationQueue.id)).filter(cast(RejectedNotificationQueue.created_at, Date) == day).scalar() or 0
        qualified = db_session.query(func.count(NotificationQueue.id)).filter(cast(NotificationQueue.created_at, Date) == day).scalar() or 0
        
        data_for_excel.append({
            "Дата": day.strftime('%d.%m.%Y'),
            "Отклики": responses,
            "Подошло": qualified,
            "Отказы": rejects,
            "Молчуны": silents
        })

    df = pd.DataFrame(data_for_excel)
    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Статистика 7 дней')
    
    output_buffer.seek(0)
    filename = f"hr_stats_7days_{date.today()}.xlsx"
    file_to_send = BufferedInputFile(output_buffer.read(), filename=filename)
    
    await callback.message.answer_document(file_to_send, caption="📊 Детальный отчет за последние 7 дней")

@router.message(F.text == "❓ Помощь")
@router.message(Command("help"))
async def handle_help(message: Message, db_session: Session):
    user = db_session.query(TelegramUser).filter(TelegramUser.telegram_id == str(message.from_user.id)).first()
    if not user: return
    
    if user.role == 'admin':
        help_text = (
            "<b>Руководство для Администратора:</b>\n\n"
            "• <b>Статистика</b> - Посуточный отчет за 7 дней.\n"
            "• <b>Баланс и Тариф</b> - Управление бюджетом и ценами.\n"
            "• <b>Управление пользователями</b> - Права доступа.\n"
            "• <b>Управление рекрутерами</b> - Настройка токенов и чатов."
        )
    else:
        help_text = (
            "<b>Руководство для Пользователя:</b>\n\n"
            "• <b>Статистика</b> - Ваша активность за неделю.\n"
            "• <b>Баланс</b> - Информация о средствах в системе.\n\n"
            "Уведомления о новых кандидатах приходят автоматически в рабочие чаты."
        )
    await message.answer(help_text, parse_mode="HTML")