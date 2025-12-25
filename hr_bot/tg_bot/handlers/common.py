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

from sqlalchemy import cast, Date, func
from datetime import date, timedelta
# Предполагается, что Bold, Italic, Text импортированы из aiogram.utils.formatting или аналогичной библиотеки
from aiogram.utils.formatting import Bold, Italic, Text

import io
import pandas as pd
from datetime import date
from sqlalchemy import cast, Date
from sqlalchemy.orm import Session
from aiogram.types import Message, BufferedInputFile
from aiogram.fsm.context import FSMContext

from hr_bot.db.models import Dialogue, InactiveNotificationQueue

logger = logging.getLogger(__name__)
router = Router()

class ExportStates(StatesGroup):
    waiting_for_range = State()


def _build_7day_stats_content(db_session: Session) -> Text:
    content_parts = [Bold("📊 Статистика за последние 7 дней:"), "\n", Italic("(по дате создания диалога)"), "\n\n"]
    
    # Генерируем список последних 7 дней (от сегодня назад)
    days = [date.today() - timedelta(days=i) for i in range(7)]
    has_any_data = False

    for day in days:
        # 1. ОТКЛИКИ (Всего диалогов, созданных в этот день)
        res = db_session.query(func.count(Dialogue.id)).filter(
            cast(Dialogue.created_at, Date) == day
        ).scalar() or 0

        # 2. ПОДОШЛО (Диалоги за этот день со статусом qualified)
        qual = db_session.query(func.count(Dialogue.id)).filter(
            cast(Dialogue.created_at, Date) == day,
            Dialogue.status == 'qualified'
        ).scalar() or 0

        # 3. ОТКАЗОВ (Диалоги за этот день со статусом rejected)
        rej = db_session.query(func.count(Dialogue.id)).filter(
            cast(Dialogue.created_at, Date) == day,
            Dialogue.status == 'rejected'
        ).scalar() or 0

        # 4. МОЛЧУНЫ (Диалоги за этот день, которые попали в таблицу молчунов)
        sil = db_session.query(func.count(Dialogue.id)).join(
            InactiveNotificationQueue, Dialogue.id == InactiveNotificationQueue.dialogue_id
        ).filter(
            cast(Dialogue.created_at, Date) == day
        ).scalar() or 0

        if res > 0: # Выводим день, только если были отклики
            has_any_data = True
            day_str = day.strftime('%d.%m (%a)')
            content_parts.extend([
                Bold(f"📅 {day_str}"), "\n",
                "   Откликов: ", Bold(str(res)), "\n",
                "   - Подошло: ", Bold(str(qual)), "\n",
                "   - Отказов: ", Bold(str(rej)), "\n",
                "   - Молчунов: ", Bold(str(sil)), "\n",
                "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            ])

    if not has_any_data:
        return Text("📊 Данных за последние 7 дней не найдено.")

    return Text(*content_parts)

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
    msg_wait = await message.answer("⏳ Собираю данные и формирую детальный отчет по новым правилам...")
    
    # 1. СБОР ДАННЫХ ИЗ БД
    # Используем created_at для фильтрации по дате создания диалога в системе
    query = db.query(Dialogue).filter(
        cast(Dialogue.created_at, Date) >= start_date,
        cast(Dialogue.created_at, Date) <= end_date
    )
    dialogues = query.all()

    if not dialogues:
        await msg_wait.edit_text("🤷 За этот период откликов не найдено.")
        await state.clear()
        return

    report_map = {}
    
    for d in dialogues:
        # Группировка: Дата | Рекрутер | Город | Вакансия
        dt = d.created_at.strftime("%d.%m.%Y")
        rec = d.recruiter.name if d.recruiter else "Не указан"
        cit = d.vacancy.city if d.vacancy else "Не указан"
        vac = d.vacancy.title if d.vacancy else "Не указана"
        key = (dt, rec, cit, vac)

        if key not in report_map:
            report_map[key] = {
                "отклики_всего": 0,
                "начали_диалог_счетчик": 0,
                "собес_счетчик": 0,
                "отказался_кд_счетчик": 0,
                "отказали_мы_счетчик": 0,
                "молчуны_в_очереди_счетчик": 0
            }

        m = report_map[key]
        
        # А) Отклики (считаем всё)
        m["отклики_всего"] += 1
        
        # Б) Проверка: Начал ли пользователь диалог?
        # Ищем хотя бы одно сообщение от 'user', которое не является системной командой
        history = d.history or []
        user_started = False
        for h in history:
            if isinstance(h, dict) and h.get('role') == 'user':
                content = h.get('content', '')
                if content and not content.startswith("[SYSTEM COMMAND]"):
                    user_started = True
                    break
        
        if user_started:
            m["начали_диалог_счетчик"] += 1

        # В) Собеседования (Status Qualified)
        if d.status == 'qualified':
            m["собес_счетчик"] += 1
            
        # Г) Отказался КД (Status Rejected + State Declined)
        if d.status == 'rejected' and d.dialogue_state == 'declined_vacancy':
            m["отказался_кд_счетчик"] += 1
            
        # Д) Отказали мы (State Qualification Failed)
        if d.dialogue_state == 'qualification_failed':
            m["отказали_мы_счетчик"] += 1
            
        # Е) Молчуны (наличие записи в таблице молчунов)
        if d.inactive_alerts: # Связь uselist=False в моделях
            m["молчуны_в_очереди_счетчик"] += 1

    # 2. ФОРМИРОВАНИЕ СТРОК ДЛЯ EXCEL С ВЫЧИСЛЕНИЯМИ
    rows = []
    for (dt, rec, cit, vac), m in report_map.items():
        # Математика по твоим правилам:
        отклики = m["отклики_всего"]
        начали_диалог = m["начали_диалог_счетчик"]
        не_вступили = отклики - начали_диалог
        
        собес = m["собес_счетчик"]
        отказался_кд = m["отказался_кд_счетчик"]
        отказали_мы = m["отказали_мы_счетчик"]
        отказы_всего = отказался_кд + отказали_мы
        
        # Молчуны = (Все кто в таблице молчунов) - (Те, кто даже не вступил в диалог)
        молчуны = max(0, m["молчуны_в_очереди_счетчик"] - не_вступили)

        rows.append({
            "Дата": dt, 
            "Рекрутер": rec, 
            "Город": cit, 
            "Вакансия": vac,
            "Отклики": отклики, 
            "Не вступили": не_вступили,
            "Начали диалог": начали_диалог, 
            "Собес": собес,
            "Отказался КД": отказался_кд, 
            "Отказали мы": отказали_мы, 
            "Молчуны": молчуны,
            "Отказы всего": отказы_всего
        })

    # Создание DataFrame
    df_base = pd.DataFrame(rows)
    # Сортировка по дате (внутренняя)
    df_base['dt_obj'] = pd.to_datetime(df_base['Дата'], format='%d.%m.%Y')
    df_base = df_base.sort_values(['dt_obj', 'Рекрутер']).drop(columns=['dt_obj'])

    # 3. ФУНКЦИЯ ДЛЯ СВОДНЫХ ТАБЛИЦ (с расчетом конверсий)
    def create_summary_df(groupby_col):
        summary = df_base.groupby(groupby_col).agg({
            'Отклики': 'sum', 'Не вступили': 'sum', 'Начали диалог': 'sum', 'Собес': 'sum',
            'Отказался КД': 'sum', 'Отказали мы': 'sum', 'Молчуны': 'sum', 'Отказы всего': 'sum'
        }).reset_index()

        s = summary
        # Конверсии
        s['Собес/отклик %'] = (s['Собес'] / s['Отклики']).fillna(0)
        s['Молчуны/Диалог %'] = (s['Молчуны'] / s['Начали диалог']).fillna(0)
        s['Отказы/Диалог %'] = (s['Отказы всего'] / s['Начали диалог']).fillna(0)

        # Итоговая строка
        total = s.sum(numeric_only=True)
        total[groupby_col] = 'ИТОГО'
        
        # Пересчет конверсий для строки ИТОГО
        t_resp = total['Отклики'] if total['Отклики'] > 0 else 1
        t_dial = total['Начали диалог'] if total['Начали диалог'] > 0 else 1
        total['Собес/отклик %'] = total['Собес'] / t_resp
        total['Молчуны/Диалог %'] = total['Молчуны'] / t_dial
        total['Отказы/Диалог %'] = total['Отказы всего'] / t_dial
        
        return pd.concat([s, pd.DataFrame([total])], ignore_index=True)

    # Генерация листов
    df_date = create_summary_df('Дата')
    df_rec = create_summary_df('Рекрутер')
    df_city = create_summary_df('Город')
    df_vac = create_summary_df('Вакансия')

    # 4. СОХРАНЕНИЕ В EXCEL (с форматированием)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_date.to_excel(writer, index=False, sheet_name='Свод по датам')
        df_rec.to_excel(writer, index=False, sheet_name='Свод по рекрутерам')
        df_city.to_excel(writer, index=False, sheet_name='Свод по городам')
        df_vac.to_excel(writer, index=False, sheet_name='Свод по вакансиям')
        df_base.to_excel(writer, index=False, sheet_name='Общий отчет')

        workbook = writer.book
        # Форматы
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9EAD3', 'border': 1})
        perc_fmt = workbook.add_format({'num_format': '0%', 'border': 1})
        num_fmt = workbook.add_format({'border': 1})
        total_fmt = workbook.add_format({'bold': True, 'bg_color': '#F4CCCC', 'border': 1})

        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            ws.set_column('A:Z', 15, num_fmt)
            # Применяем процентный формат к колонкам с % в названии
            current_df = df_date if sheet_name == 'Свод по датам' else df_base # упрощенно
            for i, col in enumerate(df_date.columns):
                if '%' in col:
                    ws.set_column(i, i, 18, perc_fmt)

    output.seek(0)
    await message.answer_document(
        BufferedInputFile(output.read(), filename=f"Detail_Report_{date.today()}.xlsx"),
        caption=f"📊 Расширенный отчет ({start_date} - {end_date})"
    )
    await msg_wait.delete()
    await state.clear()