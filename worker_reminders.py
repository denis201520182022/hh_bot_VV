import asyncio
import time
import logging
import json
import random
from hr_bot.services import hh_api_real
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select
import datetime
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import selectinload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from decimal import Decimal
import difflib
import re

from hr_bot.utils.logger_config import setup_logging
from hr_bot.db.models import SessionLocal, Dialogue, Candidate, Vacancy, NotificationQueue, TrackedRecruiter, AppSettings, InactiveNotificationQueue, RejectedNotificationQueue, InterviewReminder, LlmUsageLog
from hr_bot.services import hh_api_real as hh_api
from hr_bot.services import knowledge_base
from hr_bot.services import llm_handler
from hr_bot.db import statistics_manager

from hr_bot.utils.pii_masker import extract_and_mask_pii
from hr_bot.utils.system_notifier import send_system_alert
from hr_bot.utils.resh_in_code import check_candidate_eligibility, is_candidate_profile_complete
import signal
import sys
from hr_bot.services.llm_handler import cleanup
from hr_bot.utils.system_notifier import send_system_alert
from sqlalchemy import func, select, delete, update  
from hr_bot.services import interview_reminder_manager
from sqlalchemy import func, select, delete, and_, case, literal # <--- Добавьте case и literal
# ... остальные импорты

logger = logging.getLogger(__name__)
shutdown_requested = False

def signal_handler(sig, frame):
    global shutdown_requested
    logger.info("Получен сигнал остановки воркера напоминаний...")
    shutdown_requested = True

try:
    SPB_TIMEZONE = ZoneInfo("Europe/Moscow")
except ZoneInfoNotFoundError:
    logger.critical("Часовой пояс 'Europe/Moscow' не найден. Использую UTC.")
    SPB_TIMEZONE = datetime.timezone.utc

#CUTOFF_DATE_FOR_RESPONSES = datetime.datetime(2025, 11, 13, 11, 0, 0, tzinfo=datetime.timezone.utc)
#CUTOFF_DATE_FOR_RESPONSES = datetime.datetime(2025, 11, 16, 13, 56, 0, tzinfo=datetime.timezone.utc)
# --- КОНФИГУРАЦИЯ ---
DEBOUNCE_DELAY_SECONDS = 0
CYCLE_PAUSE_SECONDS = 1
TEST_NEGOTIATION_ID = None # Установите в None для боевого режима
MAX_CONCURRENT_RECRUITERS = 10 #одновременно рекрутеров
MAX_CONCURRENT_DIALOGUES = 40 #одновременно диалогов
VACANCY_CACHE_DURATION_MINUTES = 2 # Время кэширования списка вакансий для рекрутера
# Новые константы для окна доставки напоминаний (местное время сервера)
REMINDER_START_HOUR_LOCAL = 9  # Например, 9:00 утра
REMINDER_END_HOUR_LOCAL = 20 # Например, 20:00 вечера (напоминания отправляются до 19:59 включительно)

PRICE_PER_MILLION_INPUT_TOKENS = 0.150  # $0.150 за 1M входных токенов (gpt-4o-mini)
PRICE_PER_MILLION_OUTPUT_TOKENS = 0.600 # $0.600 за 1M выходных токенов (gpt-4o-mini)





async def check_and_send_interview_reminders():
    """
    Фоновая задача, которая проверяет очередь InterviewReminder
    и рассылает запланированные уведомления кандидатам на HH.ru.
    """
    logger.info("Фоновый обработчик напоминаний о собеседованиях запущен.")

    # Шаблоны сообщений (это черновик, нужны будут точные тексты)
    MESSAGE_TEMPLATES = {
        '2_hours_before': (
            "Здравствуйте! Напоминаю, что у вас запланировано собеседование по вакансии "
            "'{vacancy_title}' сегодня в {interview_time_spb} по московскому времени. "
            "Пожалуйста, будьте готовы."
        ),
        '1_day_before_20h_spb': (
            "Добрый вечер! Напоминаю, что завтра, {interview_date_spb} в {interview_time_spb} "
            "по московскому времени, у вас назначено собеседование по вакансии '{vacancy_title}'. "
            "Если у вас есть вопросы, напишите нам."
        ),
        'day_of_9h_spb': (
            "Доброе утро! Сегодня, {interview_date_spb} в {interview_time_spb} "
            "по московскому времени, состоится ваше собеседование по вакансии '{vacancy_title}'. "
            "Будем ждать вас!"
        )
    }

    # Определяем часовой пояс Санкт-Петербурга
    try:
        SPB_TIMEZONE = ZoneInfo("Europe/Moscow")
    except ZoneInfoNotFoundError: # <--- Нужно импортировать ZoneInfoNotFoundError в hh_worker/main.py тоже
        logger.critical("Часовой пояс 'Europe/Moscow' не найден. Напоминания не будут отправляться.")
        SPB_TIMEZONE = None
        return

    while True:
        if shutdown_requested:
            logger.info("Задача отправки напоминаний о собеседованиях остановлена из-за запроса на завершение работы.")
            break

        if SPB_TIMEZONE is None:
            await asyncio.sleep(60) # Ждем, если часовой пояс не определен
            continue

        async with SessionLocal() as db_session:
            try:
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                logger.debug("Проверка очереди напоминаний о собеседованиях")
                # Выбираем напоминания, которые пора отправить
                # Используем selectinload для всех необходимых связей
                result = await db_session.execute(
                    select(InterviewReminder)
                    .options(
                        selectinload(InterviewReminder.dialogue)
                        .selectinload(Dialogue.vacancy),
                        selectinload(InterviewReminder.dialogue)
                        .selectinload(Dialogue.candidate),
                        selectinload(InterviewReminder.recruiter) # Загружаем рекрутера напрямую
                    )
                    .filter(
                        InterviewReminder.status == 'pending',
                        InterviewReminder.scheduled_send_time_utc <= now_utc
                    )
                    .limit(20) # Обрабатываем по 20 за раз
                    .with_for_update(skip_locked=True) # <-- ДОБАВИТЬ ЭТО
                )
                reminders_to_send = result.scalars().all()

                if not reminders_to_send:
                    # logger.debug("[Interview Reminders] Нет ожидающих напоминаний. Пауза.")
                    await asyncio.sleep(30) # Пауза, если нет задач
                    continue

                logger.info("Найдена пачка напоминаний о собеседованиях", extra={
                    "count": len(reminders_to_send)
                })
                for reminder in reminders_to_send:
                    try:
                        dialogue = reminder.dialogue
                        recruiter = reminder.recruiter # Объект рекрутера уже загружен
                        vacancy = dialogue.vacancy
                        candidate = dialogue.candidate
                        log = logging.LoggerAdapter(logger, {
                            "dialogue_id": dialogue.id,
                            "hh_response_id": dialogue.hh_response_id,
                            "recruiter_id": recruiter.id,
                            "notification_type": reminder.notification_type
                        })
                        if not dialogue or not recruiter or not vacancy or not candidate:
                            log.error("Ошибка загрузки данных для напоминания о собеседовании")
                            reminder.status = 'error'
                            reminder.processed_at = now_utc
                            await db_session.commit()
                            continue

                        if not recruiter.access_token:
                            log.error("Напоминание не отправлено: отсутствует access_token рекрутера")
                            reminder.status = 'error'
                            reminder.processed_at = now_utc
                            await db_session.commit()
                            continue

                        # Форматируем дату и время собеседования для сообщения
                        interview_datetime_spb = reminder.interview_datetime_utc.astimezone(SPB_TIMEZONE)
                        interview_date_spb = interview_datetime_spb.strftime("%d.%m.%Y")
                        interview_time_spb = interview_datetime_spb.strftime("%H:%M")

                        # Получаем шаблон сообщения
                        template = MESSAGE_TEMPLATES.get(reminder.notification_type)
                        if not template:
                            log.error(f"Не найден шаблон сообщения для типа уведомления '{reminder.notification_type}'. Напоминание {reminder.id} не будет отправлено.")
                            reminder.status = 'error'
                            reminder.processed_at = now_utc
                            await db_session.commit()
                            continue

                        message_text = template.format(
                            vacancy_title=vacancy.title,
                            candidate_full_name=candidate.full_name, # Можно использовать, если нужно
                            interview_date_spb=interview_date_spb,
                            interview_time_spb=interview_time_spb
                        )

                        log.debug("Попытка отправки сообщения-напоминания в HH")
                        # Отправка сообщения кандидату через HH API
                        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
                        send_result = await hh_api.send_message(
                            recruiter=recruiter,
                            db=db_session,
                            negotiation_id=dialogue.hh_response_id,
                            message_text=message_text
                        )

                        if send_result == 200:
                            # УСПЕХ
                            reminder.status = 'sent'
                            log.info("Напоминание о собеседовании успешно отправлено кандидату")

                        elif send_result == 403:
                            # ВАКАНСИЯ ЗАКРЫТА
                            reminder.status = 'cancelled' # Отменяем, так как отправлять бессмысленно
                            log.warning("Напоминание отменено: вакансия в архиве или закрыта")

                        else:
                            # ПРОЧИЕ ОШИБКИ (False или другие коды)
                            reminder.status = 'error'
                            log.error("Сбой API при отправке напоминания о собеседовании")

                        reminder.processed_at = now_utc
                        await db_session.commit() # Коммитим каждое напоминание отдельно для надежности

                    except Exception as e:
                        log.error(f"Ошибка при обработке напоминания {reminder.id}: {e}", exc_info=True)
                        if reminder.id: # Убедимся, что reminder объект существует
                            reminder.status = 'error'
                            reminder.processed_at = now_utc
                            await db_session.commit() # Попытаемся сохранить статус ошибки
                        else:
                            await db_session.rollback() # Откатываем, если ошибка до создания reminder
            except Exception as e:
                log.critical(f"Критическая ошибка в фоновом обработчике напоминаний о собеседованиях: {e}", exc_info=True)
                await db_session.rollback() # Откат при ошибке верхнего уровня

        await asyncio.sleep(30) # Пауза между циклами проверки





async def _process_single_reminder_task(dialogue_id: int, recruiter_id: int, semaphore: asyncio.Semaphore):
    """
    Обрабатывает напоминание для одного диалога в изолированной сессии.
    """
    async with semaphore:
        async with SessionLocal() as db:
            try:
                # Загружаем диалог со всеми связями
                dialogue = await db.get(
                    Dialogue,
                    dialogue_id,
                    options=[
                        selectinload(Dialogue.candidate),
                        selectinload(Dialogue.vacancy),
                        selectinload(Dialogue.inactive_alerts)
                    ]
                )
                recruiter = await db.get(TrackedRecruiter, recruiter_id)
                log = logging.LoggerAdapter(logger, {
                    "dialogue_id": dialogue.id,
                    "hh_response_id": dialogue.hh_response_id,
                    "recruiter_id": recruiter_id,
                    "current_level": dialogue.reminder_level
                })
                if not dialogue or not recruiter:
                    return

                # Проверка: если диалог уже не in_progress (мог измениться параллельно), выходим
                EXCLUDED_REMINDER_STATUSES = ['declined_interview', 'declined_vacancy' 'call_later', 'refusal']
                if (dialogue.status not in ['in_progress'] or
                    dialogue.dialogue_state in EXCLUDED_REMINDER_STATUSES or
                    dialogue.reminder_level >= 6): # Теперь до 6 уровня включительно
                    return

                now = datetime.datetime.now(datetime.timezone.utc)
                dialogue_hh_id = dialogue.hh_response_id

                # --- API ЗАПРОС (самое долгое место) ---
                current_folder_on_hh = await hh_api.get_negotiation_current_folder(
                    recruiter, db, dialogue_hh_id
                )

                # Логика обработки папки
                if current_folder_on_hh is None:
                    # Отклик удален или не найден
                    return
                elif current_folder_on_hh == 404:
                    # Вакансия закрыта
                    log.info("Дожимы остановлены: вакансия закрыта (404)")
                    dialogue.status = 'timed_out'
                    dialogue.reminder_level = 6
                    await db.commit()
                    return

                elif current_folder_on_hh != 'consider':
                    # Кандидат перемещен вручную рекрутером
                    log.info("Дожимы остановлены: кандидат перемещен рекрутером вручную", extra={
                        "new_folder": current_folder_on_hh
                    })
                    dialogue.status = 'recruiter_handled'
                    dialogue.reminder_level = 3

                    if dialogue.inactive_alerts and dialogue.inactive_alerts.status == 'pending':
                        dialogue.inactive_alerts.status = 'cancelled'
                        dialogue.inactive_alerts.processed_at = now

                    await db.commit()
                    return

                # Логика времени
                dialogue_last_updated = dialogue.last_updated or dialogue.created_at
                time_since_update = now - dialogue_last_updated

                reminder_messages = []
                next_level = None
                should_timeout = False

                # Определение действия
                if dialogue.reminder_level == 0 and time_since_update > datetime.timedelta(minutes=30):
                    reminder_messages = [
                        "Напишу вам ещё раз, вдруг моё прошлое сообщение затерялось где-то между делами:-). ",
                        "Вакансия интересна или что-то смутило? Если что-то смущает, попробую разъяснить спорные моменты и подобрать для вас варианты ."
                    ]
                    next_level = 1

                elif dialogue.reminder_level == 1 and time_since_update > datetime.timedelta(minutes=60):
                    reminder_messages = [
                        "Пишу вам ещё раз, вдруг не увидели предыдущее сообщение. Если вам сейчас неудобно или вы думаете -  напишите, пожалуйста, чтобы я понимала, как лучше вам помочь."
                    ]
                    next_level = 2

                elif dialogue.reminder_level == 2 and time_since_update > datetime.timedelta(minutes=30):
                    should_timeout = True

                # --- НОВЫЕ УРОВНИ ---
                # elif dialogue.reminder_level == 3 and time_since_update > datetime.timedelta(days=7):
                #     reminder_messages = ["Добрый день. Если вы еще находитесь в поиске работы, то будем рады пригласить вас пройти собеседование. Готовы продолжить диалог?"]
                #     next_level = 4

                # elif dialogue.reminder_level == 4 and time_since_update > datetime.timedelta(days=21):
                #     reminder_messages = ["Добрый день. Вы трудоустроились? Если еще рассматриваете варианты, будем рады предложить вам пройти собеседование. А так же ответить на все вопросы, которые у вас есть. "]
                #     next_level = 5

                # elif dialogue.reminder_level == 5 and time_since_update > datetime.timedelta(days=51):
                #     reminder_messages = ["Еще раз добрый день. Как ваши дела? Хотели бы сообщить вам, что вакансия вновь актуальна и если вы в поиске или задумываетесь о смене работы, мы с удовольствием пригласили бы вас на собеседование"]
                #     next_level = 6

                # Выполнение действия
                if should_timeout:
                    # ТВОЕ ТРЕБОВАНИЕ: Если запись уже есть, ничего не делаем с таблицей молчунов
                    if not dialogue.inactive_alerts:
                        db.add(InactiveNotificationQueue(dialogue_id=dialogue.id, status='pending'))
                        log.info(f"Диалог впервые добавлен в InactiveNotificationQueue.")
                    else:
                        log.debug(f"Диалог уже зафиксирован в таблице молчунов. Повторная запись не требуется.")

                    # Но статус самого диалога и уровень напоминания обновляем в любом случае,
                    # чтобы пошел отсчет 7 дней для уровня 4.
                    dialogue.status = 'timed_out'
                    dialogue.reminder_level = 3
                    dialogue.last_updated = now
                    await db.commit()

                elif reminder_messages:
                    log.info("Отправлено автоматическое напоминание (дожим)", extra={
                        "next_level": next_level,
                        "time_since_last_msg": str(time_since_update)
                    })

                    # 1. Определяем типы напоминаний
                    is_long_reminder = next_level in [4, 5, 6]
                    # ТВОЕ ТРЕБОВАНИЕ: Списываем деньги только один раз (при переходе на 4 уровень)
                    should_charge = (next_level == 4) 
                    
                    settings = None

                    # 2. Проверяем баланс только если это ПЕРВОЕ долгое напоминание
                    if should_charge:
                        # Добавляем .with_for_update()
                        settings_res = await db.execute(
                            select(AppSettings).filter_by(id=1).with_for_update()
                        )
                        settings = settings_res.scalar_one_or_none()
                        
                        if not settings or settings.balance < settings.cost_per_long_reminder:
                            log.warning(f"Баланс пуст. Первое долгое напоминание для {dialogue_hh_id} отменено.")
                            return 

                    all_sent = True
                    for msg in reminder_messages:
                        status_code = await hh_api.send_message(recruiter, db, dialogue_hh_id, msg)

                        if status_code == 200:
                            # Записываем сообщение в историю
                            new_history_entry = {
                                'role': 'assistant', 
                                'content': msg,
                                'timestamp_msk': datetime.datetime.now(SPB_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S MSK')
                            }
                            current_history = list(dialogue.history) if dialogue.history else []
                            current_history.append(new_history_entry)

                            # Добавляем системную команду (для всех уровней 4, 5, 6)
                            if is_long_reminder:
                                system_instruction = {
                                    'role': 'user',
                                    'content': (
                                        "[SYSTEM COMMAND] если кандидат ответит после этого сообщения, то ты должен "
                                        "продолжить диалог по плану разговора, опираясь на текущее состояние (state), "
                                        "и не забывай перед переходом к анкете спросить про вопросы и ответить на них!"
                                    )
                                }
                                current_history.append(system_instruction)
                            
                            dialogue.history = current_history[-150:]

                            # 3. СПИСЫВАЕМ ДЕНЬГИ (только если это уровень 4)
                            # 3. СПИСЫВАЕМ ДЕНЬГИ + СТАТИСТИКА
                            if should_charge and settings:
                                cost = settings.cost_per_long_reminder
                                settings.balance -= cost
                                settings.total_spent_on_reminders += cost # Увеличиваем счетчик напоминалок
                                log.info("Списана плата за длинную цепочку напоминаний", extra={
                                    "amount": float(cost),
                                    "total_spent_reminders": float(settings.total_spent_on_reminders)
                                })
                        elif status_code == 403:
                             # Вакансия закрыта или доступ запрещен
                             dialogue.reminder_level = 6
                             dialogue.status = 'vacancy_closed'
                             await db.commit()
                             all_sent = False
                             break # Прерываем цикл
                        else:
                            all_sent = False # Ошибка отправки

                    # Если не было критической ошибки (403), обновляем уровень и время
                    if all_sent or status_code == 200:
                        dialogue.reminder_level = next_level
                        dialogue.last_updated = now
                        await db.commit()

            except Exception as e:
                log.error(f"Ошибка в задаче напоминания для диалога {dialogue_id}: {e}")
                # Не рейзим ошибку, чтобы не поломать gather


async def process_reminders(recruiter_id: int, db: AsyncSession):
    """
    Этап 4: Параллельная отправка напоминаний.
    Аргумент db здесь используется только для получения списка ID,
    далее каждый таск создает свою сессию.
    """
    function_start_time = time.monotonic()
    log = logging.LoggerAdapter(logger, {"recruiter_id": recruiter_id})

    # Семафор для ограничения одновременных проверок напоминаний (чтобы не убить базу)
    # Можно использовать тот же MAX_CONCURRENT_DIALOGUES или создать свой
    REMINDER_SEMAPHORE = asyncio.Semaphore(20)

    try:
        # 1. Проверка времени (быстро)
        if SPB_TIMEZONE is None:
            return

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        current_time_spb = now_utc.astimezone(SPB_TIMEZONE)

        if not (REMINDER_START_HOUR_LOCAL <= current_time_spb.hour < REMINDER_END_HOUR_LOCAL):
            # Вне рабочего времени просто выходим, не нагружая базу
            return

        # 2. Быстрая выборка ТОЛЬКО ID кандидатов, которым (возможно) нужны напоминания
        # Мы не грузим здесь объекты целиком, только ID
        EXCLUDED_REMINDER_STATUSES = ['declined_vacancy', 'declined_interview', 'call_later', 'refusal']

        result = await db.execute(
            select(Dialogue.id)
            .filter(
                Dialogue.recruiter_id == recruiter_id,
                Dialogue.status.in_(['in_progress']), 
                Dialogue.dialogue_state.notin_(EXCLUDED_REMINDER_STATUSES),
                Dialogue.reminder_level < 6
            )
            .with_for_update(skip_locked=True) # <-- ДОБАВИТЬ ЭТО
        )
        candidate_ids_to_check = result.scalars().all()

        if not candidate_ids_to_check:
            return

        log.debug("Запуск параллельной проверки напоминаний для кандидатов", extra={
            "candidates_count": len(candidate_ids_to_check)
        })

        # 3. Создаем задачи для параллельного выполнения
        tasks = [
            _process_single_reminder_task(d_id, recruiter_id, REMINDER_SEMAPHORE)
            for d_id in candidate_ids_to_check
        ]

        # 4. Запускаем и ждем выполнения (return_exceptions=True чтобы ошибка в одном не крашила всё)
        await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        log.error("Сбой в диспетчере проверки напоминаний", exc_info=True)
    finally:
        log.debug("Проверка напоминаний для рекрутера завершена", extra={
            "duration_sec": round(time.monotonic() - function_start_time, 2)
        })




async def run_reminders_cycle():
    logger.info("Глобальный цикл воркера напоминаний запущен")
    
    while not shutdown_requested:
        try:
            # 1. Запуск дожимов (молчащие кандидаты)
            # Для этого нам нужен список ID рекрутеров
            async with SessionLocal() as db:
                result = await db.execute(select(TrackedRecruiter.id))
                recruiter_ids = result.scalars().all()
            
            for rid in recruiter_ids:
                async with SessionLocal() as db_session:
                    await process_reminders(rid, db_session)
                    await db_session.commit()

        except Exception as e:
            logger.error("Критическая ошибка в главном цикле дожимов", exc_info=True)
        
        await asyncio.sleep(60) # Проверяем дожимы раз в минуту

async def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Запускаем две независимые задачи
    # 1. Напоминания о собеседованиях (работает внутри своего while True)
    interview_task = asyncio.create_task(check_and_send_interview_reminders())
    
    # 2. Дожимы молчунов
    reminders_task = asyncio.create_task(run_reminders_cycle())

    try:
        await asyncio.gather(interview_task, reminders_task)
    except asyncio.CancelledError:
        pass
    finally:
        await hh_api.close_api_client()
        logger.info("Воркер напоминаний остановлен.")

if __name__ == "__main__":
    setup_logging(log_filename="reminders.log")
    load_dotenv()
    asyncio.run(main())