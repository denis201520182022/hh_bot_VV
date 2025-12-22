import logging
import io
import pandas as pd
from datetime import date, datetime, timedelta
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sqlalchemy import func, cast, Date, select
from sqlalchemy.orm import Session
from aiogram.utils.formatting import Text, Bold, Italic

from hr_bot.db.models import (
    TelegramUser, Statistic, Vacancy, TrackedRecruiter,
    InactiveNotificationQueue, RejectedNotificationQueue, NotificationQueue, Dialogue
)
from hr_bot.tg_bot.keyboards import (
    user_keyboard, admin_keyboard, 
    stats_main_menu_keyboard, export_date_options_keyboard, cancel_fsm_keyboard
)

logger = logging.getLogger(__name__)
router = Router()

class ExportStates(StatesGroup):
    waiting_for_range = State()

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ТЕКСТОВОЙ СТАТИСТИКИ ---
def _build_7day_stats_content(db_session: Session) -> Text:
    content_parts = [Bold("📅 Статистика за последние 7 дней:"), "\n\n"]
    days = [date.today() - timedelta(days=i) for i in range(7)]
    has_any_data = False
    for day in days:
        res = db_session.query(func.sum(Statistic.responses_count)).filter(Statistic.date == day).scalar() or 0
        sil = db_session.query(func.count(InactiveNotificationQueue.id)).filter(cast(InactiveNotificationQueue.created_at, Date) == day).scalar() or 0
        rej = db_session.query(func.count(RejectedNotificationQueue.id)).filter(cast(RejectedNotificationQueue.created_at, Date) == day).scalar() or 0
        qual = db_session.query(func.count(NotificationQueue.id)).filter(cast(NotificationQueue.created_at, Date) == day).scalar() or 0
        if any([res, sil, rej, qual]):
            has_any_data = True
            content_parts.extend([Bold(f"🗓 {day.strftime('%d.%m')}:"), f" 📥{res} | 🟢{qual} | 🔴{rej} | 😶{sil}\n"])
    return Text(*content_parts) if has_any_data else Text("📊 Данных за 7 дней нет.")

# --- ОБРАБОТЧИКИ ---

@router.message(CommandStart())
async def handle_start(message: Message, db_session: Session):
    user = db_session.query(TelegramUser).filter(TelegramUser.telegram_id == str(message.from_user.id)).first()
    if not user:
        await message.answer("❌ Нет доступа.")
        return
    kb = admin_keyboard if user.role == 'admin' else user_keyboard
    await message.answer(f"👋 Привет, {message.from_user.first_name}!", reply_markup=kb)

@router.message(F.text == "📊 Статистика")
async def stats_main_menu(message: Message):
    await message.answer("Выберите режим работы со статистикой:", reply_markup=stats_main_menu_keyboard)

@router.callback_query(F.data == "view_stats_7days")
async def view_text_stats(callback: CallbackQuery, db_session: Session):
    content = _build_7day_stats_content(db_session)
    await callback.message.answer(**content.as_kwargs())
    await callback.answer()

@router.callback_query(F.data == "export_excel_start")
async def export_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(ExportStates.waiting_for_range)
    await callback.message.answer(
        "За какой период выгрузить данные?\n\n"
        "Можно выбрать готовую кнопку или прислать диапазон вручную в формате:\n"
        "<code>01.12.2025 - 15.12.2025</code>\n\n"
        "⚠️ Максимальный период — 30 дней.",
        reply_markup=export_date_options_keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(ExportStates.waiting_for_range, F.data.startswith("export_range_"))
async def export_range_quick(callback: CallbackQuery, state: FSMContext, db_session: Session):
    days_count = int(callback.data.split("_")[-1])
    end_date = date.today()
    start_date = end_date - timedelta(days=days_count-1)
    await generate_and_send_excel(callback.message, start_date, end_date, db_session, state)
    await callback.answer()

@router.message(ExportStates.waiting_for_range)
async def export_range_manual(message: Message, state: FSMContext, db_session: Session):
    try:
        parts = message.text.split("-")
        start_date = datetime.strptime(parts[0].strip(), "%d.%m.%Y").date()
        end_date = datetime.strptime(parts[1].strip(), "%d.%m.%Y").date()
        
        if (end_date - start_date).days > 30:
            await message.answer("❌ Ошибка: период не может превышать 30 дней.")
            return
        if start_date > end_date:
            await message.answer("❌ Ошибка: дата начала больше даты конца.")
            return
            
        await generate_and_send_excel(message, start_date, end_date, db_session, state)
    except Exception:
        await message.answer("❌ Неверный формат. Пришлите даты так: <code>01.12.2025 - 10.12.2025</code>", parse_mode="HTML")

async def generate_and_send_excel(message: Message, start_date: date, end_date: date, db: Session, state: FSMContext):
    msg_wait = await message.answer("⏳ Собираю данные и формирую Excel, подождите...")
    
    # 1. Собираем данные
    data = []
    current_day = start_date
    while current_day <= end_date:
        # Сложный запрос: группируем по рекрутеру, городу и вакансии
        results = db.query(
            TrackedRecruiter.name.label("recruiter"),
            Vacancy.city.label("city"),
            Vacancy.title.label("vacancy"),
            Vacancy.id.label("v_id"),
            TrackedRecruiter.id.label("r_id")
        ).join(Vacancy, Vacancy.recruiter_id == TrackedRecruiter.id).all()

        for row in results:
            # Кол-во новых диалогов (из Statistic)
            resp = db.query(Statistic.responses_count).filter(Statistic.vacancy_id == row.v_id, Statistic.date == current_day).scalar() or 0
            
            # Молчуны (Inactive)
            sil = db.query(func.count(InactiveNotificationQueue.id)).join(Dialogue).filter(
                Dialogue.vacancy_id == row.v_id, 
                Dialogue.recruiter_id == row.r_id,
                cast(InactiveNotificationQueue.created_at, Date) == current_day
            ).scalar() or 0
            
            # Отказы (Rejected)
            rej = db.query(func.count(RejectedNotificationQueue.id)).join(Dialogue).filter(
                Dialogue.vacancy_id == row.v_id, 
                Dialogue.recruiter_id == row.r_id,
                cast(RejectedNotificationQueue.created_at, Date) == current_day
            ).scalar() or 0
            
            # Собеседования (Qualified)
            qual = db.query(func.count(NotificationQueue.id)).join(Dialogue, Dialogue.candidate_id == NotificationQueue.candidate_id).filter(
                Dialogue.vacancy_id == row.v_id, 
                Dialogue.recruiter_id == row.r_id,
                cast(NotificationQueue.created_at, Date) == current_day
            ).scalar() or 0

            if any([resp, sil, rej, qual]):
                data.append({
                    "Дата": current_day.strftime("%d.%m.%Y"),
                    "Рекрутер": row.recruiter,
                    "Город": row.city,
                    "Вакансия": row.vacancy,
                    "Всего откликов": resp,
                    "Подошло (Собес)": qual,
                    "Отказы": rej,
                    "Молчуны": sil
                })
        current_day += timedelta(days=1)

    if not data:
        await msg_wait.edit_text("🤷 За выбранный период данных не найдено.")
        await state.clear()
        return

    # 2. Создаем Excel
    df = pd.DataFrame(data)
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Отчет')
        workbook  = writer.book
        worksheet = writer.sheets['Отчет']

        # Настройки стиля
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        
        # Включаем автофильтр на все колонки
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        
        # Закрепляем верхнюю строку
        worksheet.freeze_panes(1, 0)
        
        # Устанавливаем ширину колонок
        for i, col in enumerate(df.columns):
            column_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
            worksheet.set_column(i, i, column_len)

    output.seek(0)
    filename = f"HR_Report_{start_date}_{end_date}.xlsx"
    await message.answer_document(
        BufferedInputFile(output.read(), filename=filename),
        caption=f"📊 Отчет за период:\n{start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}"
    )
    await msg_wait.delete()
    await state.clear()

@router.message(F.text == "❓ Помощь")
async def handle_help(message: Message, db_session: Session):
    await message.answer("Используйте кнопки меню для управления ботом.")