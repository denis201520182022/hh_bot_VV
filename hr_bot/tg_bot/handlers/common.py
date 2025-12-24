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
    content_parts = [Bold("📊 Статистика за последние 7 дней:"), "\n", Italic("(по дате отклика)"), "\n\n"]
    
    # Генерируем список последних 7 дней
    days = [date.today() - timedelta(days=i) for i in range(7)]
    has_any_data = False

    for day in days:
        # 1. ВСЕГО ОТКЛИКОВ (берем из Dialogue, чтобы база была единой с остальными цифрами)
        res = db_session.query(func.count(Dialogue.id)).filter(
            cast(Dialogue.response_created_at, Date) == day
        ).scalar() or 0

        # 2. МОЛЧУНЫ (считаем записи в InactiveQueue, привязанные к диалогам за этот день)
        sil = db_session.query(func.count(InactiveNotificationQueue.id)).join(
            Dialogue, InactiveNotificationQueue.dialogue_id == Dialogue.id
        ).filter(
            cast(Dialogue.response_created_at, Date) == day
        ).scalar() or 0

        # 3. ОТКАЗНИКИ (считаем записи в RejectedQueue, привязанные к диалогам за этот день)
        rej = db_session.query(func.count(RejectedNotificationQueue.id)).join(
            Dialogue, RejectedNotificationQueue.dialogue_id == Dialogue.id
        ).filter(
            cast(Dialogue.response_created_at, Date) == day
        ).scalar() or 0

        # 4. ПОДОШЕДШИЕ / СОБЕСЫ (считаем записи в NotificationQueue через Candidate)
        qual = db_session.query(func.count(NotificationQueue.id)).join(
            Dialogue, NotificationQueue.candidate_id == Dialogue.candidate_id
        ).filter(
            cast(Dialogue.response_created_at, Date) == day
        ).scalar() or 0

        if any([res, sil, rej, qual]):
            has_any_data = True
            day_str = day.strftime('%d.%m (%a)')
            content_parts.extend([
                Bold(f"📅 {day_str}"), "\n",
                "   Откликов: ", Bold(res), "\n",
                "   - Подошло: ", Bold(qual), "\n",
                "   - Отказов: ", Bold(rej), "\n",
                "   - Молчунов: ", Bold(sil), "\n",
                "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
            ])

    if not has_any_data:
        return Text("📊 Данных за 7 дней пока нет.")

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
    msg_wait = await message.answer("⏳ Собираю данные и формирую детальный отчет...")
    
    # 1. СБОР СЫРЫХ ДАННЫХ
    query = db.query(Dialogue).filter(
        cast(Dialogue.response_created_at, Date) >= start_date,
        cast(Dialogue.response_created_at, Date) <= end_date
    )
    dialogues = query.all()

    if not dialogues:
        await msg_wait.edit_text("🤷 За этот период откликов не найдено.")
        await state.clear()
        return

    report_map = {}
    for d in dialogues:
        dt = d.response_created_at.strftime("%d.%m.%Y")
        rec = d.recruiter.name if d.recruiter else "Не указан"
        cit = d.vacancy.city if d.vacancy else "Не указан"
        vac = d.vacancy.title if d.vacancy else "Не указана"
        key = (dt, rec, cit, vac)

        if key not in report_map:
            report_map[key] = {
                "Отклики": 0, "начали_диалог": 0, "Собес": 0, 
                "Отказался КД": 0, "Отказали мы": 0, "Молчуны": 0
            }

        m = report_map[key]
        m["Отклики"] += 1
        
        history = d.history or []
        if any(isinstance(h, dict) and h.get('role') == 'user' and '[SYSTEM COMMAND]' not in h.get('content', '') for h in history):
            m["начали_диалог"] += 1

        if d.status == 'qualified': m["Собес"] += 1
        if d.dialogue_state == 'declined_vacancy': m["Отказался КД"] += 1
        if d.dialogue_state == 'qualification_failed': m["Отказали мы"] += 1
        if d.inactive_alerts: m["Молчуны"] += 1

    # Формируем DataFrame и считаем новые метрики
    rows = []
    for (dt, rec, cit, vac), m in report_map.items():
        # Новые расчеты
        not_started = m["Отклики"] - m["начали_диалог"]
        # "Чистые молчуны" = Те, кто попал в базу молчунов, НО при этом начал диалог
        # Если человек не начал диалог, он уже учтен в "Не вступили".
        # Но в твоей логике (исходя из ТЗ) мы просто вычитаем:
        pure_silents = max(0, m["Молчуны"] - not_started)

        rows.append({
            "Дата": dt, "Рекрутер": rec, "Город": cit, "Вакансия": vac,
            "Отклики": m["Отклики"], 
            "Не вступили": not_started,
            "Начали диалог": m["начали_диалог"], 
            "Собес": m["Собес"],
            "Отказался КД": m["Отказался КД"], 
            "Отказали мы": m["Отказали мы"], 
            "Молчуны": pure_silents, # Обновленный столбец
            "Отказы всего": m["Отказался КД"] + m["Отказали мы"]
        })

    df_base = pd.DataFrame(rows)
    df_base['dt_obj'] = pd.to_datetime(df_base['Дата'], format='%d.%m.%Y')
    df_base = df_base.sort_values(['dt_obj', 'Рекрутер']).drop(columns=['dt_obj'])

    # 2. СВОДНЫЕ ТАБЛИЦЫ
    def create_summary_df(groupby_col):
        summary = df_base.groupby(groupby_col).agg({
            'Отклики': 'sum', 'Не вступили': 'sum', 'Начали диалог': 'sum', 'Собес': 'sum',
            'Отказался КД': 'sum', 'Отказали мы': 'sum', 'Молчуны': 'sum', 'Отказы всего': 'sum'
        }).reset_index()

        # Расчет конверсий с .fillna(0)
        s = summary # алиас для краткости
        s['Не вступили/Отклик'] = (s['Не вступили'] / s['Отклики']).fillna(0)
        s['Диалог/Отклик'] = (s['Начали диалог'] / s['Отклики']).fillna(0)
        s['Собес/отклик'] = (s['Собес'] / s['Отклики']).fillna(0)
        s['Отказался КД/Отклик'] = (s['Отказался КД'] / s['Отклики']).fillna(0)
        s['Отказали мы/Отклик'] = (s['Отказали мы'] / s['Отклики']).fillna(0)
        s['Молчуны/отклик'] = (s['Молчуны'] / s['Отклики']).fillna(0)
        s['Молчуны/Диалог'] = (s['Молчуны'] / s['Начали диалог']).fillna(0)
        s['Отказы всего/Диалог'] = (s['Отказы всего'] / s['Начали диалог']).fillna(0)

        # Строка ИТОГО
        total = s.sum(numeric_only=True)
        total[groupby_col] = 'ИТОГО'
        
        # Пересчет итогов процентов
        t_resp = total['Отклики'] if total['Отклики'] > 0 else 1
        t_dial = total['Начали диалог'] if total['Начали диалог'] > 0 else 1
        
        total['Не вступили/Отклик'] = total['Не вступили'] / t_resp
        total['Диалог/Отклик'] = total['Начали диалог'] / t_resp
        total['Собес/отклик'] = total['Собес'] / t_resp
        total['Отказался КД/Отклик'] = total['Отказался КД'] / t_resp
        total['Отказали мы/Отклик'] = total['Отказали мы'] / t_resp
        total['Молчуны/отклик'] = total['Молчуны'] / t_resp
        total['Молчуны/Диалог'] = total['Молчуны'] / t_dial
        total['Отказы всего/Диалог'] = total['Отказы всего'] / t_dial
        
        return pd.concat([s, pd.DataFrame([total])], ignore_index=True)

    df_date = create_summary_df('Дата')
    df_rec = create_summary_df('Рекрутер')
    df_city = create_summary_df('Город')
    df_vac = create_summary_df('Вакансия')

    # 3. EXCEL + СТИЛИ
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_date.to_excel(writer, index=False, sheet_name='Свод по датам')
        df_rec.to_excel(writer, index=False, sheet_name='Свод по рекрутерам')
        df_city.to_excel(writer, index=False, sheet_name='Свод по городам')
        df_vac.to_excel(writer, index=False, sheet_name='Свод по вакансиям')
        df_base.to_excel(writer, index=False, sheet_name='Отчет')

        workbook = writer.book
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#D9EAD3', 'border': 1, 'align': 'center', 'text_wrap': True})
        fmt_perc = workbook.add_format({'num_format': '0.0%', 'border': 1, 'align': 'center'})
        fmt_num = workbook.add_format({'border': 1, 'align': 'center'})
        fmt_total = workbook.add_format({'bold': True, 'bg_color': '#F4CCCC', 'border': 1, 'align': 'center'})
        fmt_total_perc = workbook.add_format({'bold': True, 'bg_color': '#F4CCCC', 'border': 1, 'align': 'center', 'num_format': '0.0%'})
        fmt_blue = workbook.add_format({'bold': True, 'bg_color': '#CFE2F3', 'font_color': 'black', 'border': 1, 'align': 'center'})

        for sheet_name, current_df in [
            ('Свод по датам', df_date), ('Свод по рекрутерам', df_rec), 
            ('Свод по городам', df_city), ('Свод по вакансиям', df_vac)
        ]:
            ws = writer.sheets[sheet_name]
            last_row_idx = len(current_df) - 1
            
            for i, col in enumerate(current_df.columns):
                # Настройка ширины
                max_len = max(current_df[col].astype(str).map(len).max(), len(col)) + 3
                # Колонки с 9 по 16 (индексы J-Q) - это проценты
                is_perc = 9 <= i <= 16
                ws.set_column(i, i, max_len, fmt_perc if is_perc else fmt_num)
                
                # ИТОГО: проценты
                if is_perc:
                    ws.write(last_row_idx + 1, i, current_df.iloc[last_row_idx, i], fmt_total_perc)

            # ИТОГО: строка (числа)
            ws.set_row(last_row_idx + 1, None, fmt_total)
            # Шапка
            for col_num, value in enumerate(current_df.columns.values):
                ws.write(0, col_num, value, fmt_header)

        # ЛИСТ ОТЧЕТ
        ws_rep = writer.sheets['Отчет']
        for i, col in enumerate(df_base.columns):
            max_len = max(df_base[col].astype(str).map(len).max(), len(col)) + 3
            ws_rep.set_column(i, i, max_len, fmt_num)
        for col_num, value in enumerate(df_base.columns.values):
            ws_rep.write(0, col_num, value, fmt_blue)
        ws_rep.freeze_panes(1, 0)

    output.seek(0)
    filename = f"HR_Full_Report_{date.today()}.xlsx"
    await message.answer_document(
        BufferedInputFile(output.read(), filename=filename),
        caption=f"📊 Полный отчет с новыми метриками (Не вступили / Чистые молчуны)."
    )
    await msg_wait.delete()
    await state.clear()