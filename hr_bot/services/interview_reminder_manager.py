# hr_bot/services/interview_reminder_manager.py
import datetime
import logging
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError # <--- Добавить ZoneInfoNotFoundError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from sqlalchemy.orm import selectinload

from hr_bot.db.models import InterviewReminder, Dialogue, TrackedRecruiter # <--- Обновить импорт моделей

logger = logging.getLogger(__name__)

# Определяем часовой пояс Санкт-Петербурга
try:
    SPB_TIMEZONE = ZoneInfo("Europe/Moscow")
except ZoneInfoNotFoundError:
    logger.critical("Часовой пояс 'Europe/Moscow' не найден. Убедитесь, что система имеет актуальную базу данных часовых поясов (tzdata).")
    SPB_TIMEZONE = None # Fallback, чтобы избежать ошибок инициализации, но функционал будет нарушен

async def schedule_interview_reminders(
    dialogue_id: int,
    interview_date_str: str,
    interview_time_str: str,
    db_session: AsyncSession
):
    """
    Планирует напоминания о собеседовании для кандидата.
    Сначала отменяет все существующие незавершенные напоминания для данного диалога,
    затем создает новые на основе предоставленной даты и времени.
    """
    if SPB_TIMEZONE is None:
        logger.error("Не удалось инициализировать часовой пояс Санкт-Петербурга. Напоминания не будут запланированы.")
        return

    logger.info(f"Начало планирования напоминаний для диалога {dialogue_id} на дату {interview_date_str} {interview_time_str}.")

    # 1. Загружаем диалог, чтобы получить recruiter_id и обновить interview_datetime_utc
    # Используем selectinload для предзагрузки рекрутера, это нужно будет для отправки
    dialogue_result = await db_session.execute(
        select(Dialogue)
        .options(selectinload(Dialogue.recruiter))
        .filter_by(id=dialogue_id)
    )
    dialogue = dialogue_result.scalar_one_or_none()

    if not dialogue:
        logger.error(f"Диалог с ID {dialogue_id} не найден. Невозможно запланировать напоминания.")
        return
    
    if not dialogue.recruiter:
        logger.error(f"Рекрутер для диалога {dialogue_id} не найден. Невозможно запланировать напоминания.")
        return

    recruiter_id = dialogue.recruiter_id
    recruiter_name = dialogue.recruiter.name
    hh_response_id = dialogue.hh_response_id

    # 2. Парсим дату и время и приводим к UTC
    try:
        # Объединяем дату и время в один datetime объект без часового пояса
        interview_date_obj = datetime.datetime.strptime(interview_date_str, "%Y-%m-%d").date()
        interview_time_obj = datetime.datetime.strptime(interview_time_str, "%H:%M").time()
        
        interview_datetime_spb_naive = datetime.datetime.combine(interview_date_obj, interview_time_obj)
        interview_datetime_spb = interview_datetime_spb_naive.replace(tzinfo=SPB_TIMEZONE)
        interview_datetime_utc = interview_datetime_spb.astimezone(datetime.timezone.utc)
        
        logger.debug(f"Время собеседования: СПб местное: {interview_datetime_spb.isoformat()}, UTC: {interview_datetime_utc.isoformat()}")

    except ValueError as e:
        logger.error(f"Ошибка парсинга даты/времени собеседования '{interview_date_str} {interview_time_str}': {e}")
        return

    # 3. Отменяем все предыдущие "pending" напоминания для этого диалога
    await db_session.execute(
        update(InterviewReminder)
        .where(InterviewReminder.dialogue_id == dialogue_id)
        .where(InterviewReminder.status == 'pending')
        .values(status='cancelled', processed_at=datetime.datetime.now(datetime.timezone.utc))
    )
    logger.info(f"Отменены все предыдущие 'pending' напоминания для диалога {dialogue_id}.")

    # 4. Обновляем время собеседования в самом диалоге
    dialogue.interview_datetime_utc = interview_datetime_utc
    db_session.add(dialogue)
    logger.debug(f"Обновлено время собеседования в диалоге {dialogue_id}.")

    # 5. Планируем новые напоминания
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    reminders_to_add = []

    # Напоминание: За 2 часа до собеседования
    send_2h_before_utc = interview_datetime_utc - datetime.timedelta(hours=2)
    if send_2h_before_utc > now_utc:
        reminders_to_add.append(
            InterviewReminder(
                dialogue_id=dialogue_id,
                recruiter_id=recruiter_id,
                interview_datetime_utc=interview_datetime_utc,
                scheduled_send_time_utc=send_2h_before_utc,
                notification_type='2_hours_before'
            )
        )
        logger.debug(f"  Запланировано напоминание '2_hours_before' на {send_2h_before_utc.isoformat()} для {hh_response_id}.")

    # Напоминание: За 1 день до собеседования в 20:00 (СПБ местное)
    # Переводим дату собеседования в местное время для расчетов
    interview_datetime_spb_for_calc = interview_datetime_utc.astimezone(SPB_TIMEZONE)
    
    # Дата отправки напоминания (предыдущий день)
    send_date_1day_before_spb = interview_datetime_spb_for_calc.date() - datetime.timedelta(days=1)
    # Время отправки напоминания (20:00 местного)
    send_1day_before_spb_naive = datetime.datetime.combine(send_date_1day_before_spb, datetime.time(20, 0))
    send_1day_before_spb = send_1day_before_spb_naive.replace(tzinfo=SPB_TIMEZONE)
    send_1day_before_utc = send_1day_before_spb.astimezone(datetime.timezone.utc)

    # Проверка условия "Если собеседование назначено после 20:00 на следующий день, вечернее напоминание не отправляется."
    # Это значит, если само собеседование в 20:00 или позже (местного времени СПБ)
    # и оно на следующий день или позже (чтобы не путать с "сегодня в 20:00")
    # Условие: если разница в датах между планируемой отправкой (20:00 предыдущего дня)
    # и временем собеседования (20:00 того же дня) не меньше 1 дня.
    
    # Это условие лучше проверить так: если время самого собеседования в СПб 20:00 и позже,
    # то вечернее напоминание за день до не имеет смысла, так как кандидат уже знает о собеседовании сегодня.
    
    # Более точная интерпретация: "Если время собеседования (на следующий день) >= 20:00 (МСК),
    # то вечернее напоминание (20:00 накануне) не отправляется."
    
    # Это правило немного запутанное, я его интерпретирую как:
    # Если собеседование *само* происходит в 20:00 или позже *на следующий день*
    # относительно момента, когда было назначено (или если 20:00 наступило),
    # то вечернее напоминание за день до *этого* собеседования не отправляется.
    # Если "завтрашнее" собеседование назначено в 20:00 или позже, то "вечернего" напоминания быть не должно.
    
    # Допустим, сегодня Пт 18:00. Назначаем на Сб 20:30.
    # Напоминание на Пт 20:00 - не должно отправляться.
    # Назначаем на Сб 14:00.
    # Напоминание на Пт 20:00 - должно отправляться.
    
    # Скорректированное условие:
    # 1. Время отправки напоминания (send_1day_before_utc) должно быть в будущем.
    # 2. Если собеседование в этот же день, и время собеседования >= 20:00 местного времени,
    # то вечернее напоминание (которое должно было быть вчера в 20:00) неактуально.
    
    # Упрощенное условие: если `send_1day_before_utc` > `now_utc` и 
    # время самого собеседования *не* 20:00 или позже в тот же день, что и напоминание
    # или если разница между отправкой напоминания и началом собеседования > 12 часов (чтобы исключить "сегодня в 20:00")

    # Упростим до: если время отправки напоминания в будущем и (если собеседование назначен на "завтра" И его время НЕ 20:00 или позже)
    # или (если собеседование назначен НЕ на "завтра")
    
    send_day_is_before_interview_day = send_date_1day_before_spb < interview_datetime_spb_for_calc.date()
    interview_time_is_20h_or_later = interview_datetime_spb_for_calc.time() >= datetime.time(20, 0)
    
    # Напоминание за 1 день до 20:00 отправляется, ЕСЛИ:
    # 1. Время отправки (UTC) в будущем
    # 2. ИЛИ (время собеседования в СПб МЕНЬШЕ 20:00)
    #    ИЛИ (дата отправки напоминания НЕ совпадает с датой собеседования, т.е. реально "за день до")
    #    -> эта логика немного сложная. Проще:
    #    Отправляем, если время отправки в будущем И
    #    (если собеседование на следующий день, то его время должно быть до 20:00)
    #    или (если собеседование не на следующий день, то всегда отправляем)
    
    # Дополнительно проверяем, что send_date_1day_before_spb реально "вчера" по отношению к interview_datetime_spb_for_calc
    if send_1day_before_utc > now_utc and \
       (interview_datetime_spb_for_calc.date() - send_date_1day_before_spb).days == 1 and \
       not interview_time_is_20h_or_later: # Условие: собеседование не в 20:00 или позже
        reminders_to_add.append(
            InterviewReminder(
                dialogue_id=dialogue_id,
                recruiter_id=recruiter_id,
                interview_datetime_utc=interview_datetime_utc,
                scheduled_send_time_utc=send_1day_before_utc,
                notification_type='1_day_before_20h_spb'
            )
        )
        logger.debug(f"  Запланировано напоминание '1_day_before_20h_spb' на {send_1day_before_utc.isoformat()} для {hh_response_id}.")
    elif send_1day_before_utc > now_utc: # Если время отправки в будущем, но правило с 20:00 не сработало
        logger.debug(f"  Напоминание '1_day_before_20h_spb' для {hh_response_id} не запланировано из-за правила 20:00 или слишком близкой даты. "
                     f"Interview SPB: {interview_datetime_spb_for_calc.isoformat()}. Send SPB: {send_1day_before_spb.isoformat()}")


    # Напоминание: В 9:00 утра в день собеседования (СПБ местное)
    # Дата отправки напоминания (день собеседования)
    send_date_day_of_spb = interview_datetime_spb_for_calc.date()
    # Время отправки напоминания (9:00 местного)
    send_day_of_9h_spb_naive = datetime.datetime.combine(send_date_day_of_spb, datetime.time(9, 0))
    send_day_of_9h_spb = send_day_of_9h_spb_naive.replace(tzinfo=SPB_TIMEZONE)
    send_day_of_9h_utc = send_day_of_9h_spb.astimezone(datetime.timezone.utc)

    # Условие: если собеседование после 12:00 текущей датой
    interview_time_is_after_12h = interview_datetime_spb_for_calc.time() >= datetime.time(12, 0)
    
    # Отправляем, если время отправки (UTC) в будущем И
    # (дата отправки напоминания совпадает с датой собеседования (это "сегодня")) И
    # (время собеседования в СПб >= 12:00)
    if send_day_of_9h_utc > now_utc and interview_time_is_after_12h:
        reminders_to_add.append(
            InterviewReminder(
                dialogue_id=dialogue_id,
                recruiter_id=recruiter_id,
                interview_datetime_utc=interview_datetime_utc,
                scheduled_send_time_utc=send_day_of_9h_utc,
                notification_type='day_of_9h_spb'
            )
        )
        logger.debug(f"  Запланировано напоминание 'day_of_9h_spb' на {send_day_of_9h_utc.isoformat()} для {hh_response_id}.")
    elif send_day_of_9h_utc > now_utc:
        logger.debug(f"  Напоминание 'day_of_9h_spb' для {hh_response_id} не запланировано из-за правила 12:00. Interview SPB: {interview_datetime_spb_for_calc.isoformat()}.")


    if reminders_to_add:
        db_session.add_all(reminders_to_add)
        logger.info(f"Для диалога {dialogue_id} добавлено {len(reminders_to_add)} новых напоминаний.")
    else:
        logger.info(f"Для диалога {dialogue_id} нечего было планировать (все напоминания просрочены или не подходят под условия).")

    # Коммит будет сделан в _process_single_dialogue в hh_worker/main.py
    logger.info(f"Завершено планирование напоминаний для диалога {dialogue_id}.")