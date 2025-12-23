import logging
import io
import pandas as pd
from datetime import date, datetime, timedelta
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sqlalchemy import func, cast, Date
from sqlalchemy.orm import Session
from aiogram.utils.formatting import Text, Bold, Italic

from hr_bot.db.models import (
    TelegramUser, Statistic, Vacancy, TrackedRecruiter,
    InactiveNotificationQueue, RejectedNotificationQueue, NotificationQueue, Dialogue
)
from hr_bot.tg_bot.keyboards import (
    user_keyboard, admin_keyboard, 
    stats_main_menu_keyboard, export_date_options_keyboard, 
    cancel_fsm_keyboard, create_stats_export_keyboard
)

logger = logging.getLogger(__name__)
router = Router()

class ExportStates(StatesGroup):
    waiting_for_range = State()

def _build_7day_stats_content(db_session: Session) -> Text:
    content_parts = [Bold("📊 Статистика за последние 7 дней:"), "\n\n"]
    days = [date.today() - timedelta(days=i) for i in range(7)]
    has_any_data = False
    for day in days:
        res = db_session.query(func.sum(Statistic.responses_count)).filter(Statistic.date == day).scalar() or 0
        sil = db_session.query(func.count(InactiveNotificationQueue.id)).filter(cast(InactiveNotificationQueue.created_at, Date) == day).scalar() or 0
        rej = db_session.query(func.count(RejectedNotificationQueue.id)).filter(cast(RejectedNotificationQueue.created_at, Date) == day).scalar() or 0
        qual = db_session.query(func.count(NotificationQueue.id)).filter(cast(NotificationQueue.created_at, Date) == day).scalar() or 0
        if any([res, sil, rej, qual]):
            has_any_data = True
            content_parts.extend([
                Bold(f"📅 {day.strftime('%d.%m (%a)')}"), "\n",
                "  📩 Откликов: ", Bold(res), "\n",
                "   Подошло: ", Bold(qual), "\n",
                "   Отказов: ", Bold(rej), "\n",
                "   Молчунов: ", Bold(sil), "\n",
                "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            ])
    return Text(*content_parts) if has_any_data else Text("📊 Данных за 7 дней нет.")

@router.message(CommandStart())
async def handle_start(message: Message, db_session: Session):
    user = db_session.query(TelegramUser).filter(TelegramUser.telegram_id == str(message.from_user.id)).first()
    if not user:
        await message.answer("❌ Нет доступа.")
        return
    kb = admin_keyboard if user.role == 'admin' else user_keyboard
    await message.answer(f"👋 Привет, {message.from_user.first_name or 'HR'}!", reply_markup=kb)

@router.message(F.text == "📊 Статистика")
async def stats_main_menu(message: Message):
    await message.answer("Выберите режим работы со статистикой:", reply_markup=stats_main_menu_keyboard)

@router.callback_query(F.data == "view_stats_7days")
async def view_text_stats(callback: CallbackQuery, db_session: Session):
    content = _build_7day_stats_content(db_session)
    await callback.message.edit_text(**content.as_kwargs())
    await callback.answer()

@router.callback_query(F.data == "export_excel_start")
async def export_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(ExportStates.waiting_for_range)
    await callback.message.answer(
        "За какой период выгрузить данные?\n\n"
        "Выберите кнопку или пришлите диапазон:\n<code>01.12.2025 - 15.12.2025</code>",
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
        await generate_and_send_excel(message, start_date, end_date, db_session, state)
    except Exception:
        await message.answer("❌ Неверный формат. Пример: 01.12.2025 - 10.12.2025")


async def generate_and_send_excel(message: Message, start_date: date, end_date: date, db: Session, state: FSMContext):
    msg_wait = await message.answer("⏳ Собираю когортный отчет (лист 'Отчет')...")
    
    # 1. Получаем все диалоги, где дата ОТКЛИКА (response_created_at) попадает в диапазон
    # Нам нужны сразу связи с вакансией, рекрутером и всеми очередями уведомлений
    query = db.query(Dialogue).filter(
        cast(Dialogue.response_created_at, Date) >= start_date,
        cast(Dialogue.response_created_at, Date) <= end_date
    )
    dialogues = query.all()

    if not dialogues:
        await msg_wait.edit_text("🤷 За этот период откликов не найдено.")
        await state.clear()
        return

    # Структура для агрегации данных: {(дата, рекрутер, город, вакансия): {метрики}}
    report_data = {}

    for d in dialogues:
        # Определяем ключи группировки
        dt = d.response_created_at.strftime("%d.%m.%Y")
        recruiter_name = d.recruiter.name if d.recruiter else "Не указан"
        city = d.vacancy.city if d.vacancy else "Не указан"
        vacancy_title = d.vacancy.title if d.vacancy else "Не указана"
        key = (dt, recruiter_name, city, vacancy_title)

        if key not in report_data:
            report_data[key] = {
                "отклики": 0, "начали_диалог": 0, "собес": 0, 
                "отказался_кд": 0, "отказали_мы": 0, "молчуны": 0
            }

        # Метрика 1: Всего откликов
        report_data[key]["отклики"] += 1

        # Метрика 2: Начали диалог (проверка истории)
        # Логика: есть роль 'user' и в контенте НЕТ '[SYSTEM COMMAND]'
        history = d.history or []
        started = any(
            isinstance(m, dict) and 
            m.get('role') == 'user' and 
            '[SYSTEM COMMAND]' not in m.get('content', '') 
            for m in history
        )
        if started:
            report_data[key]["начали_диалог"] += 1

        # Метрика 3: Собес (проверка наличия в NotificationQueue)
        # У диалога есть связь NotificationQueue (загружаем через d.candidate.notification_queue или напрямую)
        if d.status == 'qualified':
             report_data[key]["собес"] += 1

        # Метрика 4: Отказался КД (dialogue_state == 'declined_vacancy')
        if d.dialogue_state == 'declined_vacancy':
            report_data[key]["отказался_кд"] += 1

        # Метрика 5: Отказали мы (status == 'qualification_failed')
        if d.dialogue_state == 'qualification_failed':
            report_data[key]["отказали_мы"] += 1

        # Метрика 6: Молчуны (диалог когда-либо попадал в InactiveNotificationQueue)
        if d.inactive_alerts:
            report_data[key]["молчуны"] += 1

    # Преобразуем словарь в плоский список для DataFrame
    final_rows = []
    for (dt, rec, cit, vac), m in report_data.items():
        total_rejects = m["отказался_кд"] + m["отказали_мы"]
        final_rows.append({
            "Дата": dt,
            "Рекрутер": rec,
            "Город": cit,
            "Вакансия": vac,
            "Отклики": m["отклики"],
            "начали диалог": m["начали_диалог"],
            "Собес": m["собес"],
            "Отказался КД": m["отказался_кд"],
            "Отказали мы": m["отказали_мы"],
            "Молчуны": m["молчуны"],
            "Отказы": total_rejects
        })

    # Сортируем данные: сначала дата, потом рекрутер
    df = pd.DataFrame(final_rows)
    df['dt_obj'] = pd.to_datetime(df['Дата'], format='%d.%m.%Y')
    df = df.sort_values(by=['dt_obj', 'Рекрутер'], ascending=[True, True]).drop(columns=['dt_obj'])

    # 2. Создаем Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Отчет')
        workbook  = writer.book
        worksheet = writer.sheets['Отчет']

        # Стилизация под шаблон
        header_format = workbook.add_format({
            'bold': True, 
            'bg_color': '#4F81BD', # Синий цвет как на скрине
            'font_color': 'white',
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })
        cell_format = workbook.add_format({'border': 1, 'align': 'center'})
        
        # Применяем шапку
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
            
        # Устанавливаем ширину и границы для всех данных
        worksheet.set_column(0, 0, 12, cell_format) # Дата
        worksheet.set_column(1, 1, 20, cell_format) # Рекрутер
        worksheet.set_column(2, 3, 25, cell_format) # Город, Вакансия
        worksheet.set_column(4, 10, 15, cell_format) # Цифры

        worksheet.freeze_panes(1, 0) # Закрепить шапку

    output.seek(0)
    filename = f"HR_Complex_Report_{date.today()}.xlsx"
    await message.answer_document(
        BufferedInputFile(output.read(), filename=filename),
        caption="✅ Лист 'Отчет' сформирован по когортной модели (на основе даты отклика)."
    )
    await msg_wait.delete()
    await state.clear()