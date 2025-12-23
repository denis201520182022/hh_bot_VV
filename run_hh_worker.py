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


def _format_timestamp_to_msk(timestamp_str: str) -> str:
    """
    Преобразует строку времени из формата ISO в читаемую строку по МСК.
    Если строка некорректна, возвращает заглушку.
    """
    try:
        # SPB_TIMEZONE у вас уже определен глобально
        dt_object = datetime.datetime.fromisoformat(timestamp_str)
        msk_dt = dt_object.astimezone(SPB_TIMEZONE)
        return msk_dt.strftime('%Y-%m-%d %H:%M:%S MSK')
    except (ValueError, TypeError):
        return "время не определено"


try:
    SPB_TIMEZONE = ZoneInfo("Europe/Moscow")
except ZoneInfoNotFoundError:
    logger.critical("Часовой пояс 'Europe/Moscow' не найден. Убедитесь, что система имеет актуальную базу данных часовых поясов (tzdata).")
    SPB_TIMEZONE = None # Fallback

# Флаг для graceful shutdown
shutdown_requested = False




async def _record_citizenship_usage(db: AsyncSession, dialogue: Dialogue, llm_data: dict):
    """Специальная функция для учета токенов побочного запроса (гражданство)"""
    usage_stats = llm_data.get("usage_stats")
    if not usage_stats:
        return

    try:
        p_tokens = usage_stats.get('prompt_tokens', 0)
        c_tokens = usage_stats.get('completion_tokens', 0)
        cached_tokens = usage_stats.get('cached_tokens', 0)
        total_tokens = usage_stats.get('total_tokens', 0)

        # Расчет стоимости
        cost_input_regular = (max(0, p_tokens - cached_tokens) / 1_000_000) * PRICE_PER_MILLION_INPUT_TOKENS
        cost_input_cached = (cached_tokens / 1_000_000) * (PRICE_PER_MILLION_INPUT_TOKENS / 2)
        cost_output = (c_tokens / 1_000_000) * PRICE_PER_MILLION_OUTPUT_TOKENS
        total_call_cost = Decimal(str(cost_input_regular + cost_input_cached + cost_output))

        # Запись в лог
        usage_log = LlmUsageLog(
            dialogue_id=dialogue.id,
            dialogue_state_at_call="Citizenship_Analysis",
            prompt_tokens=p_tokens,
            completion_tokens=c_tokens,
            cached_tokens=cached_tokens,
            total_tokens=total_tokens,
            cost=total_call_cost
        )
        db.add(usage_log)

        # Обновление счетчиков диалога
        dialogue.total_prompt_tokens += p_tokens
        dialogue.total_completion_tokens += c_tokens
        dialogue.total_cached_tokens += cached_tokens
        dialogue.total_cost += total_call_cost
        
        await db.flush()
    except Exception as e:
        logger.error(f"Ошибка логирования токенов гражданства: {e}")

def signal_handler(sig, frame):
    """Обработчик сигналов для graceful shutdown"""
    global shutdown_requested
    logger.info("Получен сигнал остановки. Завершаем работу...")
    shutdown_requested = True


async def get_all_active_vacancies_for_recruiter(recruiter_id: int) -> list:
    """
    Асинхронно получает список всех активных вакансий для рекрутера,
    и синхронизирует их с локальной базой данных.
    Использует кэш: если вакансии синхронизировались менее 10 минут назад, возвращает данные из БД.
    """

    function_start_time = time.monotonic()

    async with SessionLocal() as db:
        try:
            # Проверяем, нужно ли обновлять данные из API
            now = datetime.datetime.now(datetime.timezone.utc)
            cache_expiry_time = datetime.timedelta(minutes=VACANCY_CACHE_DURATION_MINUTES)

            # Получаем актуальный объект recruiter в этой сессии
            current_recruiter = await db.get(TrackedRecruiter, recruiter_id)

            if not current_recruiter:
                logger.error(f"Рекрутер с ID {recruiter_id} не найден в текущей сессии для синхронизации вакансий.")
                return []
            logger.debug(f"Получение и синхронизация списка активных вакансий для рекрутера {current_recruiter.name}...")
            if current_recruiter.vacancies_last_synced_at:
                time_since_sync = now - current_recruiter.vacancies_last_synced_at

                if time_since_sync < cache_expiry_time:
                    logger.debug(f"Используем кэшированные вакансии для рекрутера {current_recruiter.name}. "
                                f"Последняя синхронизация: {time_since_sync.total_seconds() / 60:.1f} минут назад.")

                    result = await db.execute(select(Vacancy).filter(
                        Vacancy.recruiter_id == current_recruiter.id
                    ))
                    cached_vacancies = result.scalars().all()

                    cached_vacancies_list = [
                        {
                            "id": vacancy.hh_vacancy_id,
                            "name": vacancy.title,
                            "area": {"name": vacancy.city} if vacancy.city else {}
                        }
                        for vacancy in cached_vacancies
                    ]

                    logger.debug(f"Возвращено {len(cached_vacancies_list)} вакансий из кэша для рекрутера {current_recruiter.name}.")
                    return cached_vacancies_list

            logger.debug(f"Кэш устарел или отсутствует. Получаем актуальные данные из API...")

            api_request_start = time.monotonic()

            me_data = await hh_api._make_request(current_recruiter, db, "GET", "me")
            logger.debug(f"[Recruiter {current_recruiter.name}] API call 'me' took: {time.monotonic() - api_request_start:.2f} sec.")
            if not me_data or not me_data.get('employer') or not me_data['employer'].get('id'):
                logger.error(f"Не удалось получить employer_id для рекрутера {current_recruiter.name}.")
                return []
            employer_id = me_data['employer']['id']

            all_vacancies_from_api = []
            page = 0
            while True:
                api_request_page_start = time.monotonic()
                vacancies_page = await hh_api._make_request(
                    current_recruiter, db, "GET", f"employers/{employer_id}/vacancies/active",
                    params={'page': page, 'per_page': 20}
                )
                logger.debug(f"[Recruiter {current_recruiter.name}] API call 'vacancies/active' page {page} took: {time.monotonic() - api_request_page_start:.2f} sec.")
                if not vacancies_page or not vacancies_page.get('items'):
                    break

                all_vacancies_from_api.extend(vacancies_page['items'])

                if page >= vacancies_page.get('pages', 1) - 1:
                    break
                page += 1

            if not all_vacancies_from_api:
                logger.info(f"У рекрутера {current_recruiter.name} сейчас нет активных вакансий. Запускаю очистку старых...")
            else:
                logger.info(f"Найдено {len(all_vacancies_from_api)} активных вакансий. Синхронизация с БД...")

            # Список ID вакансий, которые сейчас активны на HH
            active_hh_ids = {str(v["id"]) for v in all_vacancies_from_api}


            for vacancy_data in all_vacancies_from_api:
                hh_vacancy_id = str(vacancy_data.get("id"))

                result = await db.execute(select(Vacancy).filter_by(hh_vacancy_id=hh_vacancy_id))
                vacancy_in_db = result.scalar_one_or_none()

                if not vacancy_in_db:
                    new_vacancy = Vacancy(
                        hh_vacancy_id=hh_vacancy_id,
                        title=vacancy_data.get("name", "Без названия"),
                        city=vacancy_data.get("area", {}).get("name"),
                        recruiter_id=current_recruiter.id
                    )
                    db.add(new_vacancy)
                    logger.info(f"  -> Добавлена новая вакансия в БД: '{new_vacancy.title}' (ID: {hh_vacancy_id})")
                else:
                    if (vacancy_in_db.title != vacancy_data.get("name") or
                        vacancy_in_db.city != vacancy_data.get("area", {}).get("name") or
                        vacancy_in_db.recruiter_id != current_recruiter.id):

                        vacancy_in_db.title = vacancy_data.get("name", "Без названия")
                        vacancy_in_db.city = vacancy_data.get("area", {}).get("name")
                        vacancy_in_db.recruiter_id = current_recruiter.id
                        logger.debug(f"  -> Обновлены данные для вакансии: '{vacancy_in_db.title}' (ID: {hh_vacancy_id})")

            # Удаляем вакансии, которые больше не активны
            # Находим вакансии в БД, которые числятся за этим рекрутером, но которых НЕТ в списке active_hh_ids

            stale_vacancies_query = select(Vacancy).filter(
                Vacancy.recruiter_id == current_recruiter.id,
                Vacancy.hh_vacancy_id.notin_(active_hh_ids)
            )
            stale_result = await db.execute(stale_vacancies_query)
            stale_vacancies = stale_result.scalars().all()

            for stale_vac in stale_vacancies:
                logger.info(f"Вакансия {stale_vac.title} ({stale_vac.hh_vacancy_id}) больше не активна у рекрутера {current_recruiter.name}. Отвязываем.")
                # Вариант А: Просто отвязать (установить NULL)
                stale_vac.recruiter_id = None

            db_commit_start = time.monotonic()
            current_recruiter.vacancies_last_synced_at = now
            await db.commit()
            logger.debug(f"[Recruiter {current_recruiter.name}] DB commit for vacancies sync took: {time.monotonic() - db_commit_start:.2f} sec.")
            logger.debug(f"Кэш обновлен. Следующее обновление через {VACANCY_CACHE_DURATION_MINUTES} минут.")

            return all_vacancies_from_api

        except Exception as e:
            logger.error(f"Ошибка при получении вакансий для рекрутера {recruiter_id}: {e}", exc_info=True)
            await db.rollback()
            return []
        finally:
            logger.debug(f"[Recruiter {recruiter_id}] Total function execution time: {time.monotonic() - function_start_time:.2f} sec.")



async def process_new_responses(recruiter_id: int, vacancy_ids: list):
    """Этап 1: Ищет новые отклики по СПИСКУ вакансий."""
    function_start_time = time.monotonic()

    recruiter = None
    recruiter_name_for_logging = f"ID {recruiter_id}"

    async with SessionLocal() as db:
        try:
            recruiter = await db.get(TrackedRecruiter, recruiter_id)
            if not recruiter:
                logger.warning(f"process_new_responses: Рекрутер с ID {recruiter_id} не найден.")
                return
            recruiter_name_for_logging = recruiter.name
            cutoff_date = recruiter.created_at or (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1))
            logger.debug(f"Используем дату старта для рекрутера {recruiter.name}: {cutoff_date}")
            if not vacancy_ids:
                logger.error("Этап 1: Нет активных вакансий для проверки 'Неразобранных'.")
                return

            logger.debug(f"Этап 1: Проверка 'Неразобранных' для {len(vacancy_ids)} вакансий...")

            new_responses_with_vacancy_ids = await hh_api.get_responses_from_folder(
                recruiter, db, 'response', vacancy_ids, since_datetime=cutoff_date
            )

            for resp, associated_vacancy_id_str in new_responses_with_vacancy_ids:
                # Используем SAVEPOINT для каждого кандидата, чтобы ошибка в одном не ломала всю транзакцию
                try:
                    response_id = resp.get('id')

                    # --- БЕЗОПАСНЫЙ ДОСТУП К ДАННЫМ РЕЗЮМЕ ---
                    resume_info = resp.get('resume')
                    if not resume_info:
                        logger.warning(f"Отклик {response_id} без резюме. Пропуск.")
                        continue

                    candidate_first_name = resume_info.get('first_name', 'Неизвестно')
                    candidate_last_name = resume_info.get('last_name', '')
                    candidate_full_name = f"{candidate_first_name} {candidate_last_name}".strip()
                    candidate_hh_resume_id = resume_info.get('id')
                    # -----------------------------------------

                    if not response_id or (TEST_NEGOTIATION_ID and response_id != TEST_NEGOTIATION_ID):
                        continue

                    # Проверка существования
                    exists_query = select(func.count()).select_from(Dialogue).filter_by(hh_response_id=response_id)
                    result = await db.execute(exists_query)
                    if result.scalar() > 0:
                        continue

                    # Проверка лимитов
                    settings_result = await db.execute(select(AppSettings).filter_by(id=1))
                    settings = settings_result.scalar_one_or_none()

                    if not settings:
                        logger.error("Настройки AppSettings не найдены в БД!")
                        continue

                    # ПРОВЕРКА БАЛАНСА
                    if settings.balance < settings.cost_per_dialogue:
                        logger.warning(f"Недостаточно средств на балансе ({settings.balance}). Отклик {response_id} пропущен.")
                        continue

                    logger.info(f"\nНайден новый отклик {response_id} ({candidate_full_name}).")

                    vacancy_in_db_result = await db.execute(
                        select(Vacancy).filter(Vacancy.hh_vacancy_id == associated_vacancy_id_str)
                    )
                    vacancy_in_db = vacancy_in_db_result.scalar_one_or_none()

                    if not vacancy_in_db:
                        logger.error(f"Вакансия {associated_vacancy_id_str} не найдена в БД. Пропуск.")
                        continue

                    # Работа с кандидатом
                    candidate_result = await db.execute(
                        select(Candidate).filter(Candidate.hh_resume_id == candidate_hh_resume_id)
                    )
                    candidate = candidate_result.scalar_one_or_none()
                    if not candidate:
                        candidate = Candidate(
                            hh_resume_id=candidate_hh_resume_id,
                            full_name=candidate_full_name
                        )
                        db.add(candidate)

                    await db.flush() # Чтобы получить ID кандидата

                    response_created_at_str = resp.get('created_at')
                    response_created_at_dt = None
                    if response_created_at_str:
                        try:
                            response_created_at_dt = datetime.datetime.fromisoformat(response_created_at_str)
                        except (ValueError, TypeError):
                            logger.warning(f"Не удалось распознать дату отклика: {response_created_at_str}")
                    # <<< КОНЕЦ ИСПРАВЛЕНИЯ >>>

                    # Создаем диалог
                    dialogue = Dialogue(
                        hh_response_id=response_id,
                        candidate_id=candidate.id,
                        vacancy_id=vacancy_in_db.id,
                        recruiter_id=recruiter_id,
                        status='new',
                        dialogue_state='initial_processing',
                        response_created_at=response_created_at_dt # <<< ИСПОЛЬЗУЕМ ОБЪЕКТ DATETIME
                    )
                    db.add(dialogue)

                    # --- КРИТИЧЕСКИЙ МОМЕНТ: ПЕРЕМЕЩЕНИЕ ---
                    # Сначала перемещаем, чтобы зафиксировать намерение
                    await hh_api.move_response_to_folder(recruiter, db, response_id, 'consider')

                    # СПИСАНИЕ СРЕДСТВ
                    settings.balance -= settings.cost_per_dialogue

                    # Пытаемся получить сообщения, но ошибка здесь НЕ ДОЛЖНА отменять создание диалога
                    try:
                        messages_data = await hh_api.get_messages(recruiter, db, resp['messages_url'])
                        messages = [
                            {
                                'message_id': str(m.get('id')),
                                'role': 'user',
                                'content': m['text'],
                                'timestamp_msk': _format_timestamp_to_msk(m.get('created_at')) # <-- ДОБАВЛЕНО
                            }
                            for m in messages_data if m.get('text')
                        ]
                    except Exception as msg_err:
                        logger.error(f"Ошибка получения сообщений для {response_id}: {msg_err}. Использую заглушку.")
                        messages = []

                    if not messages:
                        # Если сопроводительного нет, берем время самого отклика
                        now_msk = datetime.datetime.now(SPB_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S MSK')
                        messages = [{
                            'message_id': f'no_msg_{response_id}',
                            'role': 'user',
                            'content': "[SYSTEM COMMAND] Кандидат откликнулся без сопроводительного письма. Поздоровайся и предложи задать вопросы",
                            'timestamp_msk': _format_timestamp_to_msk(resp.get('created_at', now_msk)) # <-- ДОБАВЛЕНО
                        }]

                    dialogue.pending_messages = messages
                    dialogue.last_updated = datetime.datetime.now(datetime.timezone.utc)

                    await statistics_manager.update_stats(db, vacancy_in_db.id, responses=1, started_dialogs=1)

                    # --- ВАЖНО: КОММИТИМ СРАЗУ ДЛЯ КАЖДОГО КАНДИДАТА ---
                    # Это гарантирует, что если мы перенесли его в consider, он сохранится в БД
                    await db.commit()
                    logger.info(f"✅ Диалог {response_id} успешно сохранен в БД.")

                    # Проверка лимита для уведомления
                    # УВЕДОМЛЕНИЕ О НИЗКОМ БАЛАНСЕ
                    if settings.balance < settings.low_balance_threshold and not settings.low_limit_notified:
                        asyncio.create_task(send_system_alert(
                            f"⚠️ Внимание! Баланс ниже {settings.low_balance_threshold} руб. "
                            f"Текущий остаток: {settings.balance} руб.", alert_type="balance"
                        ))
                        settings.low_limit_notified = True

                    # Если баланс пополнили выше порога, сбрасываем флаг (опционально, но удобно)
                    if settings.balance >= settings.low_balance_threshold:
                        settings.low_limit_notified = False

                except Exception as e:
                    logger.error(f"Ошибка при обработке отклика {resp.get('id')}: {e}", exc_info=True)
                    await db.rollback() # Откат только для текущего отклика
                    continue

        except Exception as e:
            logger.error(f"Критическая ошибка в process_new_responses: {e}", exc_info=True)
        finally:
            logger.debug(f"process_new_responses завершено за {time.monotonic() - function_start_time:.2f}s")



async def process_ongoing_responses(recruiter_id: int, vacancy_ids: list):
    """Этап 2: Ищет новые сообщения в папках 'Подумать' и 'Собеседование'."""
    function_start_time = time.monotonic()

    recruiter = None
    recruiter_name_for_logging = f"ID {recruiter_id}" # Значение по умолчанию на случай, если рекрутер не найден
    async with SessionLocal() as db:
        try:
            recruiter = await db.get(TrackedRecruiter, recruiter_id)
            if not recruiter:
                logger.warning(f"process_ongoing_responses: Рекрутер с ID {recruiter_id} не найден.")
                return

            # --- ЗАГРУЗКА ЗДЕСЬ ---
            recruiter_name_for_logging = recruiter.name
            # --- КОНЕЦ ЗАГРУЗКИ ---
            cutoff_date = recruiter.created_at or (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1))

            if not vacancy_ids:
                logger.warning("Этап 2: Нет активных вакансий для проверки обновлений.")
                return

            logger.debug(
                f"Этап 2: Проверка обновлений в папках 'Подумать' и 'Собеседование' "
                f"для {len(vacancy_ids)} вакансий..."
            )

            api_get_responses_gather_start = time.monotonic()

            consider_task = hh_api.get_responses_from_folder(
                recruiter, db, 'consider', vacancy_ids,
                since_datetime=cutoff_date,
                check_for_updates=True
            )
            interview_task = hh_api.get_responses_from_folder(
                recruiter, db, 'interview', vacancy_ids,
                since_datetime=cutoff_date,
                check_for_updates=True
            )

            # Выполняем запросы параллельно
            consider_results, interview_results = await asyncio.gather(consider_task, interview_task)
            
            logger.debug(
                f"[Recruiter {recruiter_name_for_logging}] API calls took: "
                f"{time.monotonic() - api_get_responses_gather_start:.2f} sec. "
                f"Found {len(consider_results)} in consider, {len(interview_results)} in interview."
            )

            # Объединяем результаты в один список, помечая источник
            # Каждая запись: (название_папки, (данные_отклика, id_вакансии))
            tagged_responses = [('consider', item) for item in consider_results]
            tagged_responses.extend([('interview', item) for item in interview_results])

            for folder_name, (resp, _) in tagged_responses:
                response_id = resp.get('id')

                if not response_id or (TEST_NEGOTIATION_ID and response_id != TEST_NEGOTIATION_ID):
                    continue

                dialogue_result = await db.execute(select(Dialogue).filter_by(hh_response_id=response_id))
                dialogue = dialogue_result.scalar_one_or_none()
                
                if not dialogue:
                    logger.debug(f"Найдено обновление для отклика {response_id}, которого нет в нашей БД. Пропускаем.")
                    continue

                # --- КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: ПРОВЕРКА ПАПКИ ИНТЕРВЬЮ ---
                if folder_name == 'interview':
                    if dialogue.dialogue_state != 'post_qualification_chat':
                        logger.debug(f"[{response_id}] Обнаружен в папке 'interview'. Принудительный стейт: post_qualification_chat.")
                        dialogue.dialogue_state = 'post_qualification_chat'
                        
                # -----------------------------------------------------

                api_get_messages_start = time.monotonic()
                all_messages_from_api = await hh_api.get_messages(recruiter, db, resp['messages_url'])
                
                logger.debug(
                    f"[Recruiter {recruiter.name}, Dialogue {response_id}] "
                    f"API get_messages took: {time.monotonic() - api_get_messages_start:.2f} sec."
                )

                saved_message_ids = {str(h.get('message_id')) for h in (dialogue.history or [])}
                pending_message_ids = {
                    str(p.get('message_id'))
                    for p in (dialogue.pending_messages or [])
                    if isinstance(p, dict)
                }
                seen_ids = saved_message_ids.union(pending_message_ids)

                new_messages_for_pending = [
                    {
                        'message_id': str(msg.get('id')),
                        'role': 'user',
                        'content': msg['text'],
                        'timestamp_msk': _format_timestamp_to_msk(msg.get('created_at'))
                    }
                    for msg in all_messages_from_api
                    if (msg.get('text') and
                        str(msg.get('id')) not in seen_ids and
                        msg.get('author', {}).get('participant_type') == 'applicant')
                ]

                if new_messages_for_pending:
                    if dialogue.reminder_level > 0:
                        dialogue.reminder_level = 0

                    dialogue.pending_messages = (dialogue.pending_messages or []) + new_messages_for_pending
                    dialogue.last_updated = datetime.datetime.now(datetime.timezone.utc)

                    logger.info(f"Добавлено {len(new_messages_for_pending)} новых сообщений в диалог {response_id}.")

            await db.flush()
            await db.commit()
            
        except Exception as e:
            logger.error(f"Error in process_ongoing_responses: {e}", exc_info=True)
            await db.rollback()
            raise
        finally:
            logger.debug(
                f"[Recruiter {recruiter_name_for_logging}] "
                f"process_ongoing_responses finished in {time.monotonic() - function_start_time:.2f}s"
            )

def _log_missing_vacancy(title: str, city: str):
    """
    Записывает ненайденную вакансию в файл missing_vacancies.txt.
    Проверяет файл на наличие дубликатов перед записью.
    """
    file_path = "missing_vacancies.txt"
    # Формируем строку для записи
    entry = f"{title} | {city}"
    
    try:
        # Читаем существующие записи, чтобы избежать дублей
        existing_lines = set()
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                # Собираем set очищенных строк
                existing_lines = {line.strip() for line in f}
        except FileNotFoundError:
            # Если файла нет, просто создадим его при записи
            pass

        # Если такой записи еще нет, добавляем
        if entry not in existing_lines:
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(f"{entry}\n")
                
    except Exception as e:
        # Логируем ошибку, но не ломаем работу бота
        logger.error(f"Ошибка при записи в missing_vacancies.txt: {e}")

def _find_relevant_vacancy(prompt_library: dict, vacancy_title: str, vacancy_city: str) -> str:
    """
    Поиск вакансии по принципу BEST MATCH (Лучшее совпадение).
    Просматривает ВСЕ варианты и выбирает тот, где сумма сходства (город + название) максимальна.
    """

    def normalize_text(text: str) -> str:
        if not text: 
            return ""
        text = text.lower().replace('ё', 'е')
        text = re.sub(r'[^\w\s]', ' ', text)
        return " ".join(text.split())

    def get_similarity(str1: str, str2: str) -> float:
        """Возвращает коэффициент сходства от 0.0 до 1.0"""
        if not str1 or not str2:
            return 0.0
        if str1 in str2 or str2 in str1:
            return 1.0 # Полное вхождение считаем идеальным
        return difflib.SequenceMatcher(None, str1, str2).ratio()

    logger.debug(f"🔍 Поиск вакансии (Best Match): '{vacancy_title}' в '{vacancy_city}'")

    norm_input_title = normalize_text(vacancy_title)
    norm_input_city = normalize_text(vacancy_city)

    best_match_description = None
    best_match_score = 0.0

    # Перебираем ВСЕ вакансии
    for vacancy in prompt_library.get("vacancies", []):
        
        # 1. Считаем лучший балл по городу в этом блоке
        best_city_score = 0.0
        for db_city_raw in vacancy.get("cities", []):
            score = get_similarity(norm_input_city, normalize_text(db_city_raw))
            if score > best_city_score:
                best_city_score = score
        
        # Если город совсем не похож (меньше 0.65), этот блок нам точно не нужен
        if best_city_score < 0.65:
            continue

        # 2. Считаем лучший балл по названию в этом блоке
        best_title_score = 0.0
        for db_title_raw in vacancy.get("titles", []):
            score = get_similarity(norm_input_title, normalize_text(db_title_raw))
            if score > best_title_score:
                best_title_score = score
        
        # Если название не похоже (меньше 0.65), пропускаем
        if best_title_score < 0.65:
            continue

        # 3. Суммарный балл текущего блока
        total_score = best_city_score + best_title_score

        # Если этот блок подходит лучше, чем предыдущий найденный
        if total_score > best_match_score:
            best_match_score = total_score
            best_match_description = vacancy["description"]
            # Логируем кандидата на победу
            # logger.debug(f"📈 Новый лидер: {vacancy.get('titles')[0]} (Score: {total_score:.2f})")

    if best_match_description:
        logger.info(f"✅ Выбрано лучшее совпадение (Score: {best_match_score:.2f})")
        return best_match_description

    # Если ничего не нашли
    logger.warning(f"🤡 Не найдено точное описание для '{vacancy_title}' в '{vacancy_city}'.")
    _log_missing_vacancy(vacancy_title, vacancy_city)
    
    return "ОПИСАНИЕ ВАКАНСИИ НЕ НАЙДЕНО. Отвечай на вопросы кандидата на основе общей информации из FAQ."
def _generate_calendar_context() -> str:
    """
    Генерирует текстовый блок с календарем и правилами работы с датами.
    """
    current_datetime_utc = datetime.datetime.now(datetime.timezone.utc)
    weekdays_ru = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]

    current_weekday = weekdays_ru[current_datetime_utc.weekday()]
    current_date_str = current_datetime_utc.strftime("%Y-%m-%d")

    calendar_context_lines = []
    for i in range(14):  # Сегодня + 13 дней вперед = 14 дней
        date_cursor = current_datetime_utc + datetime.timedelta(days=i)
        wd_name = weekdays_ru[date_cursor.weekday()]
        date_str = date_cursor.strftime("%Y-%m-%d")

        label = ""
        if i == 0:
            label = " ← ТЫ ЗДЕСЬ (СЕГОДНЯ)"
        elif i == 1:
            label = " ← ЗАВТРА"
        elif i == 2:
            label = " ← ПОСЛЕЗАВТРА"

        calendar_context_lines.append(f"{wd_name}: {date_str}{label}")

    calendar_string = "\n".join(calendar_context_lines)

    calendar_context = (
        f"\n\n[CRITICAL CALENDAR CONTEXT]\n"
        f"ТЕКУЩАЯ ДАТА И ВРЕМЯ (UTC): {current_datetime_utc.strftime('%Y-%m-%d %H:%M')}\n"
        f"СЕГОДНЯ: {current_weekday}, {current_date_str}\n\n"
        f"⚠️ ВАЖНО: Ты ОЧЕНЬ ПЛОХО считаешь даты в уме. НИКОГДА НЕ ВЫЧИСЛЯЙ ДАТЫ САМОСТОЯТЕЛЬНО!\n"
        f"Используй ТОЛЬКО эту таблицу (таблица начинается с СЕГОДНЯ и идет на 14 дней вперед):\n\n"
        f"{calendar_string}\n\n"
        f"ПРАВИЛА РАБОТЫ С ДАТАМИ:\n"
        f"1. Если кандидат говорит конкретный день недели БЕЗ уточнений (например, просто 'понедельник'):\n"
        f"   → Бери ПЕРВЫЙ такой день (то есть ближайший) из списка выше\n\n"
        f"2. Если кандидат говорит 'СЛЕДУЮЩИЙ [день недели]' (например, 'следующий понедельник'):\n"
        f"   → Бери ВТОРОЙ такой день из списка выше\n\n"
        f"3. Если кандидат называет день недели, который совпадает с СЕГОДНЯ:\n"
        f"   → ОБЯЗАТЕЛЬНО уточни: 'Вы имеете в виду сегодня или через неделю?'\n\n"
        f"4. Если кандидат говорит 'сегодня', 'завтра', 'послезавтра':\n"
        f"   → Ищи в списке пометку '← СЕГОДНЯ', '← ЗАВТРА' или '← ПОСЛЕЗАВТРА'\n\n"
        f"5. ВСЕГДА копируй дату ТОЧНО из таблицы в формате YYYY-MM-DD\n"
        f"6. НИКОГДА не изобретай даты сам - только из этой таблицы!\n"
    )
    return calendar_context

def _assemble_dynamic_prompt(prompt_library: dict, dialogue_state: str, user_message: str, vacancy_description: str) -> str:
    """Собирает динамический системный промпт из блоков библиотеки (упрощенная версия с единым FAQ)."""

    required_blocks = ['#ROLE_AND_STYLE#']

    state_specific_blocks = {
        'initial_processing': ['#QUALIFICATION_RULES#'],
        'awaiting_questions': ['#QUALIFICATION_RULES#'],
        'awaiting_phone': ['#QUALIFICATION_RULES#'],
        'awaiting_city': ['#QUALIFICATION_RULES#'],
        'awaiting_readiness': ['#QUALIFICATION_RULES#'],
        'awaiting_citizenship': ['#QUALIFICATION_RULES#'],
        'clarifying_citizenship': ['#QUALIFICATION_RULES#'],
        'awaiting_age': ['#QUALIFICATION_RULES#'],
        'clarifying_anything': ['#QUALIFICATION_RULES#'],
        'clarifying_declined_vacancy': ['#QUALIFICATION_RULES#'],

        'qualification_complete': ['#QUALIFICATION_RULES#'],
        'call_later': ['#QUALIFICATION_RULES#'],

        'init_scheduling_spb': ['#SCHEDULING_ALGORITHM#'],
        'scheduling_spb_day': ['#SCHEDULING_ALGORITHM#'],
        'scheduling_spb_time': ['#SCHEDULING_ALGORITHM#'],
        'interview_scheduled_spb': ['#SCHEDULING_ALGORITHM#']
    }
    required_blocks.extend(state_specific_blocks.get(dialogue_state, []))

    if dialogue_state in ['forwarded_to_researcher','interview_scheduled_spb', 'post_qualification_chat', 'awaiting_questions', 'initial_processing', 'call_later']:
        required_blocks.append('#FAQ#')

    final_block_keys = list(dict.fromkeys(required_blocks))

    prompt_pieces = [prompt_library.get(key, '') for key in final_block_keys]
    
    

    # +++ КЛЮЧЕВОЕ ИЗМЕНЕНИЕ +++
    # Определяем состояния, для которых нужен календарь
    SCHEDULING_STATES = ['init_scheduling_spb', 'scheduling_spb_day', 'scheduling_spb_time', 'post_qualification_chat', 'interview_scheduled_spb']

    # Если текущее состояние требует календаря, генерируем и добавляем его
    if dialogue_state in SCHEDULING_STATES:
        calendar_block = _generate_calendar_context()
        prompt_pieces.append(calendar_block)
    # +++ КОНЕЦ ИЗМЕНЕНИЯ +++

    POST_QUALIFICATION_STATES = ['forwarded_to_researcher', 'interview_scheduled_spb', 'post_qualification_chat']
    
    if dialogue_state in POST_QUALIFICATION_STATES:
        post_qual_block = prompt_library.get('#POSTCVAL#', '')
        if post_qual_block:
            prompt_pieces.append(post_qual_block)

    vacancy_context = (
        "[CRITICAL CONTEXT] Ниже представлено описание ТОЛЬКО ТОЙ вакансии, на которую откликнулся кандидат. "
        "Используй ИСКЛЮЧИТЕЛЬНО эту информацию при ответах на вопросы о вакансии.\n" +
        vacancy_description
    )
    prompt_pieces.insert(1, vacancy_context)
    

    return "\n\n".join(prompt_pieces)


async def _process_single_dialogue(dialogue_id: int, recruiter_id: int, prompt_library: dict, db: AsyncSession):
    """Исправленная версия с правильной работой с ORM"""
    dialogue_processing_start_time = time.monotonic()

    dialogue = None
    recruiter = None
    # --- ЗАГРУЗКА ЗДЕСЬ (значение по умолчанию) ---
    log_dialogue_hh_response_id = f"ID {dialogue_id}"
    # --- КОНЕЦ ЗАГРУЗКИ ---

    try:
        # Проверка активности сессии
        if not db.is_active:
            logger.error(f"Session is not active for dialogue {dialogue_id}")
            return

        db_fetch_start = time.monotonic()

        # Загружаем dialogue с явной загрузкой связей
        dialogue = await db.get(
            Dialogue,
            dialogue_id,
            options=[
                selectinload(Dialogue.vacancy),
                selectinload(Dialogue.candidate),
                selectinload(Dialogue.rejected_alerts),
                selectinload(Dialogue.inactive_alerts)
            ]
        )

        # Загружаем recruiter
        recruiter = await db.get(TrackedRecruiter, recruiter_id)

        logger.debug(f"[Dialogue {dialogue_id}] DB fetch took: {time.monotonic() - db_fetch_start:.4f} sec.")

        if not dialogue or not recruiter:
            logger.error(f"Dialogue {dialogue_id} or recruiter {recruiter_id} not found")
            return
        # --- ЗАГРУЗКА ЗДЕСЬ ---
        log_dialogue_hh_response_id = dialogue.hh_response_id
        # --- КОНЕЦ ЗАГРУЗКИ ---
        # Принудительно загружаем связанные объекты в сессию
        await db.refresh(dialogue.candidate)
        await db.refresh(dialogue.vacancy)

        logger.debug(f"Processing dialogue {dialogue.hh_response_id}...")

        pending_messages = dialogue.pending_messages or []
        if not pending_messages:
            logger.debug(f"Dialogue {dialogue.id}: no pending messages")
            return

        # *************************************************************************************************************************************
        # СПЕЦИАЛЬНАЯ ОБРАБОТКА ДЛЯ AWAITING_CITIZENSHIP
        # *************************************************************************************************************************************
        if dialogue.dialogue_state == "awaiting_citizenship" and pending_messages:
            all_pending_content = "\n".join([pm.get('content', '') if isinstance(pm, dict) else str(pm) for pm in pending_messages])
            citizenship_analysis_prompt = (
                '''Проанализируй сообщения кандидата и верни ответ\n
                [CRITICAL RULE] Твой ответ ВСЕГДА должен быть в формате JSON.
                Структура JSON должна быть следующей:
                {
                "is": "yes" или "no",
                "citizenship": "ЕАЭС" или название страны или Null,
                }\n

                Если в сообщениях содержится гражданство или название страны то в поле `is` верни `yes`\n
                Если не сообщениях нет инфы о гражданстве (стране) то в поле `is` верни `no`\n
                Если в сообщениях содержится информация, что человек гражданин (или просто указана страна) Россия (РФ) или Беларусь или Армения или Киргизия или Казахстан то `ЕАЭС`.\n"
                Если другое гражданство то верни в `citizenship` название страны.\n'''
            )
            
            citizenship_attempts = [] 
            llm_citizenship_response = None
            
            try:
                # Вызов LLM с трекером попыток для tenacity
                llm_citizenship_response = await llm_handler.get_bot_response(
                    system_prompt=citizenship_analysis_prompt,
                    dialogue_history=[],
                    user_message=all_pending_content,
                    current_datetime_utc=datetime.datetime.now(datetime.timezone.utc),
                    attempt_tracker=citizenship_attempts, 
                    skip_instructions=True
                )

                if llm_citizenship_response:
                    # 1. Логируем успешный расход (токены и деньги)
                    await _record_citizenship_usage(db, dialogue, llm_citizenship_response)
                    
                    # 2. Логируем "пустышки" для всех предыдущих неудачных попыток (ретраев), если они были
                    total_attempts = len(citizenship_attempts)
                    if total_attempts > 1:
                        logger.warning(f"[{dialogue.hh_response_id}] Анализ гражданства: выполнено успешно после {total_attempts-1} ретраев.")
                        for i in range(total_attempts - 1):
                            retry_log = LlmUsageLog(
                                dialogue_id=dialogue.id,
                                dialogue_state_at_call=f"Citizenship_Analysis (RETRY #{i+1})",
                                prompt_tokens=0, completion_tokens=0, cached_tokens=0, total_tokens=0, cost=0.0
                            )
                            db.add(retry_log)
                    
                    await db.commit()
                    await db.refresh(dialogue)

                    # 3. Обработка полученного результата
                    try:
                        parsed_response = llm_citizenship_response.get('parsed_response')
                        if parsed_response and parsed_response.get("is") == "yes":
                            logger.info(f"[{dialogue.hh_response_id}] Распарсили гражданство: {parsed_response.get('citizenship')}")
                            citizenship = parsed_response.get("citizenship")
                            system_command_content = None

                            if citizenship == "ЕАЭС":
                                system_command_content = "[SYSTEM COMMAND] Кандидат сообщил что у него гражданство одной из стран ЕАЭС, поставь в поле citizenship строго значение 'ЕАЭС' и переходи к следующему этапу анкеты (возрасту)"
                                
                            else:
                                system_command_content = f"[SYSTEM COMMAND] Кандидат сообщил что у него гражданство {citizenship}, уточни есть ли у него РВП или ВНЖ в России."
                                dialogue.dialogue_state = "clarifying_citizenship"

                            if system_command_content:
                                system_command = {
                                    'message_id': f'sys_cmd_citizenship_{time.time()}',
                                    'role': 'user',
                                    'content': system_command_content
                                }
                                # Добавляем системную команду в очередь
                                pending_messages = (pending_messages or []) + [system_command]
                                dialogue.last_updated = datetime.datetime.now(datetime.timezone.utc)
                                await db.commit()
                                
                                
                        else:
                            logger.info(f"[{dialogue.hh_response_id}] Информация о гражданстве не найдена в текущем сообщении.")

                    except Exception as parse_err:
                        logger.error(f"[{dialogue.hh_response_id}] Ошибка разбора JSON гражданства: {parse_err}")

            except Exception as citizenship_err:
                # --- ЛОГИРОВАНИЕ ПОЛНОГО ПРОВАЛА ---
                # Если tenacity исчерпала попытки, записываем в БД все неудачные заходы
                logger.error(f"[{dialogue.hh_response_id}] Анализ гражданства ПРОВАЛЕН после {len(citizenship_attempts)} попыток: {citizenship_err}")
                for i in range(len(citizenship_attempts)):
                    failure_log = LlmUsageLog(
                        dialogue_id=dialogue.id,
                        dialogue_state_at_call=f"Citizenship_Analysis (FAILED #{i+1}: {type(citizenship_err).__name__})",
                        prompt_tokens=0, completion_tokens=0, cached_tokens=0, total_tokens=0, cost=0.0
                    )
                    db.add(failure_log)
                await db.commit()
                # Пробрасываем ошибку дальше, чтобы воркер мог её обработать (или просто логируем и идем дальше)
                raise citizenship_err

        # *************************************************************************************************************************************
        # КОНЕЦ СПЕЦИАЛЬНОЙ ОБРАБОТКИ ДЛЯ AWAITING_CITIZENSHIP
        # *************************************************************************************************************************************
        # Обработка сообщений
        user_entries_to_history = []
        all_masked_content = []

        for pm in pending_messages:
            original_content = pm.get('content', '') if isinstance(pm, dict) else str(pm)
            masked_content, extracted_fio, extracted_phone = extract_and_mask_pii(original_content)

            # Обновляем candidate (объект уже в сессии после refresh)
            # if extracted_fio:
            #     dialogue.candidate.full_name = extracted_fio

            if extracted_phone:
                dialogue.candidate.phone_number = extracted_phone

            message_id = pm.get('message_id') if isinstance(pm, dict) else f'legacy_{int(time.time())}'
            user_entries_to_history.append({
                'message_id': message_id,
                'role': 'user',
                'content': masked_content,
                'timestamp_msk': pm.get('timestamp_msk', 'время не определено') if isinstance(pm, dict) else 'время не определено' # <-- ДОБАВЛЕНО
            })
            all_masked_content.append(masked_content)

        combined_masked_message = "\n".join(all_masked_content)

        # Получаем данные вакансии (объект уже в сессии)
        vacancy_title = dialogue.vacancy.title
        vacancy_city = dialogue.vacancy.city or "город не указан"

        relevant_vacancy_desc = _find_relevant_vacancy(prompt_library, vacancy_title, vacancy_city)

        system_prompt = _assemble_dynamic_prompt(
            prompt_library,
            dialogue.dialogue_state,
            combined_masked_message.lower(),
            relevant_vacancy_desc
        )

        context_postfix = (
            f"[CURRENT TASK] Ты общаешься с кандидатом по вакансии '{vacancy_title}' "
            f"в городе '{vacancy_city}'. Текущее состояние: '{dialogue.dialogue_state}'."
        )
        final_system_prompt = system_prompt + "\n\n" + context_postfix

        # LLM запрос
        llm_call_start = time.monotonic()
        llm_data = None
        attempt_tracker = [] # <--- Создаем "ловушку" для попыток

        try:
            # Передаем attempt_tracker в функцию
            llm_data = await llm_handler.get_bot_response(
                system_prompt=final_system_prompt,
                dialogue_history=dialogue.history or [],
                user_message=combined_masked_message,
                current_datetime_utc=datetime.datetime.now(datetime.timezone.utc),
                attempt_tracker=attempt_tracker # <--- Передаем список
            )
            
            # --- УСПЕШНЫЙ СЦЕНАРИЙ ---
            # Если мы здесь, значит последняя попытка была успешной.
            # Если в attempt_tracker больше 1 элемента, значит были скрытые ретраи.
            
            total_attempts = len(attempt_tracker)
            failed_attempts = total_attempts - 1 # Все кроме последней (успешной)
            
            if failed_attempts > 0:
                logger.warning(f"[{dialogue.hh_response_id}] Было {failed_attempts} скрытых ретраев tenacity.")
                for i in range(failed_attempts):
                    # Записываем "пустышки" для ретраев
                    retry_log = LlmUsageLog(
                        dialogue_id=dialogue.id,
                        dialogue_state_at_call=f"{dialogue.dialogue_state} (RETRY #{i+1})",
                        prompt_tokens=0,
                        completion_tokens=0,
                        cached_tokens=0,
                        total_tokens=0,
                        cost=0.0
                    )
                    db.add(retry_log)
                await db.commit() # Сохраняем логи ретраев сразу

        except Exception as llm_error:
            # --- СЦЕНАРИЙ ПОЛНОГО ПРОВАЛА ---
            # Если упало здесь, значит tenacity исчерпал все попытки и выкинул ошибку.
            # В attempt_tracker лежат метки ВСЕХ попыток (например, 3 штуки).
            # Все они считаются провальными.
            
            logger.error(f"[{dialogue.hh_response_id}] LLM Request FAILED completely after {len(attempt_tracker)} attempts: {llm_error}")
            
            try:
                for i in range(len(attempt_tracker)):
                    # Пишем лог для КАЖДОЙ попытки
                    failure_log = LlmUsageLog(
                        dialogue_id=dialogue.id,
                        dialogue_state_at_call=f"{dialogue.dialogue_state} (FAILED #{i+1}: {type(llm_error).__name__})",
                        prompt_tokens=0,
                        completion_tokens=0,
                        cached_tokens=0,
                        total_tokens=0,
                        cost=0.0
                    )
                    db.add(failure_log)
                await db.commit()
            except Exception as log_ex:
                logger.error(f"Failed to log LLM errors to DB: {log_ex}")

            raise llm_error # Пробрасываем ошибку дальше

        logger.debug(f"[{dialogue.hh_response_id}] LLM call: {time.monotonic() - llm_call_start:.2f} sec.")

        if llm_data is None:
            alert_message = "⚠️ LLM service unavailable!"
            await send_system_alert(alert_message, alert_type="admin_only")
            return

        # Распаковка ответа
        llm_response = llm_data.get("parsed_response")
        usage_stats = llm_data.get("usage_stats")

        # === ЛОГИРОВАНИЕ ТОКЕНОВ ===
        if usage_stats:
            try:
                p_tokens = usage_stats.get('prompt_tokens', 0)
                c_tokens = usage_stats.get('completion_tokens', 0)
                cached_tokens = usage_stats.get('cached_tokens', 0)
                total_tokens = usage_stats.get('total_tokens', 0)

                # 1. Обычные входные токены (которые НЕ попали в кеш)
                non_cached_input = max(0, p_tokens - cached_tokens)
                cost_input_regular = (non_cached_input / 1_000_000) * PRICE_PER_MILLION_INPUT_TOKENS

                # 2. Кешированные входные токены (стоят в 2 раза дешевле)
                cost_input_cached = (cached_tokens / 1_000_000) * (PRICE_PER_MILLION_INPUT_TOKENS / 2)

                # 3. Выходные токены (ответ бота)
                cost_output = (c_tokens / 1_000_000) * PRICE_PER_MILLION_OUTPUT_TOKENS

                # Итоговая цена за этот запрос
                total_call_cost = cost_input_regular + cost_input_cached + cost_output

                # 1. Запись в лог
                usage_log = LlmUsageLog(
                    dialogue_id=dialogue.id,
                    dialogue_state_at_call=dialogue.dialogue_state,
                    prompt_tokens=p_tokens,
                    completion_tokens=c_tokens,
                    cached_tokens=cached_tokens,
                    total_tokens=total_tokens,
                    cost=total_call_cost
                )
                db.add(usage_log)

                # 2. Обновление счетчиков диалога
                dialogue.total_prompt_tokens += p_tokens
                dialogue.total_completion_tokens += c_tokens
                dialogue.total_cached_tokens += cached_tokens
                # Преобразуем float в Decimal перед сложением
                dialogue.total_cost += Decimal(str(total_call_cost))
                
                await db.commit() 
                
                # После коммита объекты могут "отцепиться" (expire), поэтому рефрешим dialogue
                # чтобы дальше с ним работать в этой сессии
                await db.refresh(dialogue) 
                # (Если usage_log дальше не нужен, его можно не рефрешить)

            except Exception as e:
                logger.error(f"Error logging tokens for dialogue {dialogue.id}: {e}")
        # ===========================

        bot_response_text = llm_response.get("response_text")
        new_state = llm_response.get("new_state", "error_state")
        extracted_data = llm_response.get("extracted_data")

        # Обновляем статус
        if dialogue.status == 'new':
            dialogue.status = 'in_progress'

        # Обновляем extracted_data (candidate уже в сессии)
        if extracted_data and dialogue.status != 'qualified':
            if extracted_data.get("age"):
                dialogue.candidate.age = extracted_data["age"]
            if extracted_data.get("citizenship"):
                dialogue.candidate.citizenship = extracted_data["citizenship"]
            if extracted_data.get("city"):
                dialogue.candidate.city = extracted_data["city"]
            if extracted_data.get("readiness_to_start"):
                dialogue.candidate.readiness_to_start = extracted_data["readiness_to_start"]
            await db.flush()
        # ==========================================================================================
        # БЛОК ВАЛИДАЦИИ И ПРИНЯТИЯ РЕШЕНИЙ (КОД)
        # ==========================================================================================

        # Проверяем условия только если диалог еще не завершен
        if (dialogue.status not in ['qualified', 'rejected'] and  new_state in ['qualification_complete']
    #and dialogue.dialogue_state not in ['scheduling_spb_day', 'scheduling_spb_time', 'interview_scheduled_spb', 'init_scheduling_spb']  # <-- ДОБАВИТЬ
    #and new_state not in ['scheduling_spb_day', 'scheduling_spb_time', 'interview_scheduled_spb', 'init_scheduling_spb']
        and is_candidate_profile_complete(dialogue.candidate)):
            logger.info(f"[{dialogue.hh_response_id}] Анкета собрана полностью. Запускаю проверку критериев.")

            is_eligible = check_candidate_eligibility(dialogue.candidate)

            if not is_eligible:
                # --- СЦЕНАРИЙ 1: ОТКАЗ ---
                logger.info(f"[{dialogue.hh_response_id}] Кандидат НЕ прошел проверку кодом (Age/Citizenship).")

                # Принудительно меняем состояние и текст (игнорируем то, что написала LLM)
                new_state = 'qualification_failed'
                bot_response_text = "Спасибо! Я передам Вашу анкету для рассмотрения. Если по Вашей анкете будет принято положительное решение с Вами свяжутся в течение трёх рабочих дней."

                # Логика перемещения в папку 'assessment' и создания уведомления сработает ниже,
                # так как мы установили new_state = 'qualification_failed'

            else:
                # --- СЦЕНАРИЙ 2: ПОДХОДИТ ---
                logger.info(f"[{dialogue.hh_response_id}] Кандидат успешно прошел проверку кодом.")

                # Проверяем город
                city_lower = (dialogue.vacancy.city or "").lower()
                is_spb = any(x in city_lower for x in ['санкт-петербург'])

                if not is_spb:
                    # --- 2.1 НЕ СПБ (Регионы) ---
                    new_state = 'forwarded_to_researcher'
                    bot_response_text = "Спасибо! Я передам Вашу заявку нашим коллегам. Мы свяжемся с Вами в рабочее время, чтобы согласовать время собеседования."
                    # Логика перемещения в папку 'interview' и смены статуса на 'qualified' сработает ниже



                else:

                    current_title_lower = (vacancy_title or "").lower()

                    # Список фраз, которые ищем в названии
                    excluded_vacancies = ['повар-пекарь', 'повар неполный день', 'повар', 'бариста', 'уборщик','уборщица','помошник повара',]

                    # Проверяем, входит ли хоть одна фраза из списка в название вакансии
                    if any(phrase in current_title_lower for phrase in excluded_vacancies):
                        logger.info(f"[{dialogue.hh_response_id}] Вакансия '{vacancy_title}' (СПб) переведена на рекрутера (исключение).")
                        new_state = 'forwarded_to_researcher'
                        bot_response_text = "Спасибо! Я передам Вашу заявку нашим коллегам. Мы свяжемся с Вами в рабочее время, чтобы согласовать время собеседования."

                    else:
                        # --- 2.2 СПБ (Запись на собеседование) ---
                        logger.info(f"[{dialogue.hh_response_id}] Город СПб. Кандидат подходит. Добавляю команду для LLM на запись.")


                        # 2. ВАЖНО: Нам нужно сохранить текущие ответы пользователя в историю прямо сейчас.
                        # Так как мы прерываем цикл (return), стандартное сохранение истории в конце функции не сработает.
                        # Если этого не сделать, бот "забудет", что кандидат только что ответил про возраст/гражданство.
                        current_history = list(dialogue.history) if dialogue.history else []
                        # user_entries_to_history мы сформировали в начале функции
                        dialogue.history = (current_history + user_entries_to_history)[-150:]

                        # 3. Формируем скрытую команду для LLM
                        # Используем role='system' или 'user' с пометкой, чтобы направить LLM.
                        system_command = {
                            'message_id': f'sys_cmd_{time.time()}',
                            'role': 'user',
                            'content': '[SYSTEM COMMAND] Кандидат прошел квалификацию. Начни запись на собеседование в Санкт-Петербурге (предложи выбрать день).'
                        }

                        # 4. Кладем команду в pending_messages
                        # Мы перезаписываем очередь, убирая оттуда сообщения пользователя (они уже в истории)
                        # и оставляя только нашу команду.
                        dialogue.pending_messages = [system_command]
                        dialogue.dialogue_state = 'init_scheduling_spb'
                        # 5. Обновляем время (last_updated), чтобы воркер подхватил диалог в следующем цикле мгновенно
                        dialogue.last_updated = datetime.datetime.now(datetime.timezone.utc)

                        # 6. Сохраняем и ПРЕРЫВАЕМ текущую обработку.
                        # Мы не отправляем сообщение bot_response_text из этого цикла, так как ждем,
                        # что LLM сгенерирует приглашение в ответ на нашу команду.

                        await db.commit()
                        return
        elif (dialogue.status not in ['qualified', 'rejected'] and  new_state in ['qualification_complete']
    #and dialogue.dialogue_state not in ['scheduling_spb_day', 'scheduling_spb_time', 'interview_scheduled_spb', 'init_scheduling_spb']  # <-- ДОБАВИТЬ
    #and new_state not in ['scheduling_spb_day', 'scheduling_spb_time', 'interview_scheduled_spb', 'init_scheduling_spb']
        and not is_candidate_profile_complete(dialogue.candidate)):
            command_content = (
                f"[SYSTEM COMMAND] Анкета кандидата не заполнена полностью. "
                f"Используй историю диалога, чтобы определить, какие из необходимых данных (Возраст, гражданство, готовность выйти на работу, город) кандидат сообщил и верни их в 'extracted_data'. "
                f"Если какие то данные еще не были предоставлены, задай прямой вопрос кандидату (или вежливо переспроси, если кандидат в течении диалога проигнорировал какой то твой вопрос)."
            )

            system_command = {
                'message_id': f'sys_cmd_{time.time()}',
                'role': 'user',
                'content': command_content
            }

            dialogue.pending_messages = (dialogue.pending_messages or []) + [system_command]
            dialogue.dialogue_state = 'clarifying_anything'
            # важно обновить last_updated, чтобы воркер как можно скорее обработал это изменение
            dialogue.last_updated = datetime.datetime.now(datetime.timezone.utc)
            await db.commit()
            return

            #Сюда надо код, который даст команду llm (просто положит в пендинг сообщение с ролью user что анкета не полная, кандидат не сообщил какие то данные, проверь какие данные и запроси их)
        # ==========================================================================================
        # КОНЕЦ БЛОКА ВАЛИДАЦИИ
        # ==========================================================================================

        #Вот сюда надо написать код, который будет проверять, заполнены ли в БД у кандидата все необходимые поля (Номер телефона, Гражданство, Возраст, Готовность выйти на работу, город)
        # Если да, то вызов функции проверки соответствия кандидата
        # Если не подходит, то перевод в состояние 'qualification_failed' и текст сообщения «Спасибо! Я передам Вашу анкету для рассмотрения. Если по Вашей анкете будет принято положительное решение с Вами свяжутся в течение трёх рабочих дней.»
        # Если подходит и город не СПб, то перевод в состояние 'forwarded_to_researcher' и текст сообщения «Спасибо! Я передам Вашу заявку нашим коллегам. Мы свяжемся с Вами в рабочее время, чтобы согласовать время собеседования».
        # Если подходит и город СПб, то перевод в состояние 'init_scheduling_spb' и запрос к llm с добавкой "Начни запись кандидата на собеседование в Санкт-Петербурге."

        # Обработка квалификации
        if new_state in ['forwarded_to_researcher', 'interview_scheduled_spb'] and dialogue.status != 'qualified':
            dialogue.status = 'qualified'

            await statistics_manager.update_stats(db, dialogue.vacancy_id, qualified=1)

            # Проверка существования уведомления (оптимизированная)
            exists_query = select(func.count()).select_from(NotificationQueue).filter_by(
                candidate_id=dialogue.candidate.id,
                status='pending'
            )
            result = await db.execute(exists_query)

            if result.scalar() == 0:
                db.add(NotificationQueue(candidate_id=dialogue.candidate.id, status='pending'))

            logger.info(f"Candidate {dialogue.hh_response_id} qualified 🟢. Moving to 'interview'.")

            api_move_start = time.monotonic()
            await hh_api.move_response_to_folder(recruiter, db, dialogue.hh_response_id, 'interview')
            logger.debug(f"[{dialogue.hh_response_id}] API move: {time.monotonic() - api_move_start:.2f} sec.")

            # --- ДОБАВИТЬ ЭТОТ БЛОК КОДА ---
            if new_state == 'interview_scheduled_spb':
                interview_date = extracted_data.get("interview_date")
                interview_time = extracted_data.get("interview_time")

                if interview_date and interview_time:
                    logger.info(
                        f"Собеседование запланировано для диалога {dialogue.id} на "
                        f"{interview_date} в {interview_time} (СПБ). Планирую напоминания."
                    )
                    await interview_reminder_manager.schedule_interview_reminders(
                        dialogue_id=dialogue.id,
                        interview_date_str=interview_date,
                        interview_time_str=interview_time,
                        db_session=db
                    )
                else:
                    logger.error(
                        f"LLM установил 'interview_scheduled_spb', но не предоставил "
                        f"interview_date или interview_time для диалога {dialogue.id}. Напоминания не будут запланированы."
                    )
            # --- КОНЕЦ ДОБАВЛЕНИЯ ---


        elif (new_state == 'qualification_failed' or new_state == 'declined_vacancy' or new_state == 'declined_interview'):

            if new_state == 'declined_vacancy':
                # --- ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА ОТКАЗА ---
                # Собираем всю историю диалога + pending_messages для анализа
                # --- ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА ОТКАЗА с ПОЛНЫМ УЧЁТОМ ---
                full_dialogue_text = "\n".join(
                    [entry.get('content', '') for entry in (dialogue.history or [])]
                )
                pending_text = "\n".join(
                    [pm.get('content', '') for pm in (dialogue.pending_messages or []) if isinstance(pm, dict)]
                )
                full_context_for_llm = (full_dialogue_text + "\n" + pending_text).strip()

                clarification_prompt = (
                    'Проанализируй диалог и определи: действительно ли кандидат чётко отказался от вакансии? '
                    'Верни ответ строго в формате JSON: {"answer": "yes" или "no"} '
                    'Ответ "yes" — только если кандидат прямо сказал, что вакансия его не интересует. '
                    'Если есть хоть малейшее сомнение — верни "no".'
                )

                clarification_attempts = []
                clarification_result = None
                try:
                    clarification_result = await llm_handler.get_bot_response(
                        system_prompt=clarification_prompt,
                        dialogue_history=[],
                        user_message=full_context_for_llm,
                        current_datetime_utc=datetime.datetime.now(datetime.timezone.utc),
                        attempt_tracker=clarification_attempts,
                        skip_instructions=True
                    )

                    # === УСПЕШНЫЙ ВЫЗОВ ===
                    total_attempts = len(clarification_attempts)
                    failed_attempts = total_attempts - 1
                    if failed_attempts > 0:
                        logger.warning(f"[{dialogue.hh_response_id}] Уточнение declined_vacancy: {failed_attempts} скрытых ретраев.")
                        for i in range(failed_attempts):
                            retry_log = LlmUsageLog(
                                dialogue_id=dialogue.id,
                                dialogue_state_at_call=f"DeclineClarification (RETRY #{i+1})",
                                prompt_tokens=0,
                                completion_tokens=0,
                                cached_tokens=0,
                                total_tokens=0,
                                cost=Decimal('0.0')
                            )
                            db.add(retry_log)
                        await db.commit()

                    # === ЛОГИРОВАНИЕ ТОКЕНОВ ===
                    if clarification_result and 'usage_stats' in clarification_result:
                        usage = clarification_result['usage_stats']
                        p_tokens = usage.get('prompt_tokens', 0)
                        c_tokens = usage.get('completion_tokens', 0)
                        cached_tokens = usage.get('cached_tokens', 0)
                        non_cached = max(0, p_tokens - cached_tokens)
                        cost = (
                            (non_cached / 1_000_000) * PRICE_PER_MILLION_INPUT_TOKENS +
                            (cached_tokens / 1_000_000) * (PRICE_PER_MILLION_INPUT_TOKENS / 2) +
                            (c_tokens / 1_000_000) * PRICE_PER_MILLION_OUTPUT_TOKENS
                        )
                        usage_log = LlmUsageLog(
                            dialogue_id=dialogue.id,
                            dialogue_state_at_call="DeclineClarification",
                            prompt_tokens=p_tokens,
                            completion_tokens=c_tokens,
                            cached_tokens=cached_tokens,
                            total_tokens=p_tokens + c_tokens,
                            cost=Decimal(str(cost))
                        )
                        db.add(usage_log)
                        dialogue.total_prompt_tokens += p_tokens
                        dialogue.total_completion_tokens += c_tokens
                        dialogue.total_cached_tokens += cached_tokens
                        dialogue.total_cost += Decimal(str(cost))
                        await db.commit()
                        await db.refresh(dialogue)

                except Exception as e:
                    # === ПОЛНЫЙ ПРОВАЛ ===
                    logger.warning(f"[{dialogue.hh_response_id}] Ошибка при уточнении 'declined_vacancy': {e}. Считаем отказом по умолчанию.")
                    total_fails = len(clarification_attempts)
                    for i in range(total_fails):
                        fail_log = LlmUsageLog(
                            dialogue_id=dialogue.id,
                            dialogue_state_at_call=f"DeclineClarification (FAILED #{i+1}: {type(e).__name__})",
                            prompt_tokens=0,
                            completion_tokens=0,
                            cached_tokens=0,
                            total_tokens=0,
                            cost=Decimal('0.0')
                        )
                        db.add(fail_log)
                    await db.commit()
                    clarification_result = None

                is_real_decline = False
                if clarification_result and 'parsed_response' in clarification_result:
                    try:
                        parsed = clarification_result['parsed_response']
                        is_real_decline = (parsed.get('answer') == 'yes')
                    except Exception as e:
                        logger.warning(f"[{dialogue.hh_response_id}] Ошибка парсинга JSON при уточнении 'declined_vacancy': {e}")

                if not is_real_decline:
                    # Кандидат НЕ отказался → прерываем текущую обработку
                    system_command = {
                        'message_id': f'sys_cmd_recheck_decline_{time.time()}',
                        'role': 'user',
                        'content': '[SYSTEM COMMAND] Сейчас кандидат не отказывается от вакансии и анкетирования, продолжай дальше.'
                    }
                    
                    dialogue.pending_messages = (dialogue.pending_messages or []) + [system_command]
                    dialogue.last_updated = datetime.datetime.now(datetime.timezone.utc)
                    await db.commit()
                    logger.info(f"[{dialogue.hh_response_id}] Отказ от вакансии НЕ подтверждён. Отложен системный запрос для повторной обработки.")
                    return  # ← ВАЖНО: выходим из функции, НЕ переводя в статус 'rejected'
                else:
                    logger.info(f"[{dialogue.hh_response_id}] Отказ от вакансии подтверждён LLM.")
                # --- КОНЕЦ ДОПОЛНИТЕЛЬНОЙ ПРОВЕРКИ ---


            dialogue.status = 'rejected'
            # --- ДОБАВЛЕННЫЙ БЛОК: Отмена напоминаний при отказе ---
            if new_state == 'declined_interview':
                
                await db.execute(
                    update(InterviewReminder)
                    .where(InterviewReminder.dialogue_id == dialogue.id)
                    .where(InterviewReminder.status == 'pending')
                    .values(
                        status='cancelled', 
                        processed_at=datetime.datetime.now(datetime.timezone.utc)
                    )
                )
                logger.info(f"[{dialogue.hh_response_id}] Статус 'declined_interview': все запланированные напоминания отменены.")
            # -------------------------------------------------------

            




            # --- ИСПРАВЛЕННЫЙ БЛОК КОДА ---
            # Проверяем, существует ли уже запись RejectedNotificationQueue для этого диалога.
            # `dialogue.rejected_alerts` будет либо объектом RejectedNotificationQueue, либо None,
            # благодаря `uselist=False` в relationship.
            if dialogue.inactive_alerts:
                logger.info(f"[{dialogue.hh_response_id}] Кандидат уже в таблице молчунов. Пропускаю запись в отказники.")
            else:
                if dialogue.rejected_alerts:
                    # Запись уже существует. Если статус не 'pending', обновляем его.
                    if dialogue.rejected_alerts.status != 'pending':
                        logger.debug(
                            f"Уведомление об отклоненном кандидате для диалога {dialogue.hh_response_id} "
                            f"уже существует со статусом '{dialogue.rejected_alerts.status}'. Обновляю статус на 'pending'."
                        )
                        dialogue.rejected_alerts.status = 'pending'
                        dialogue.rejected_alerts.processed_at = None # Сбросим время обработки при переходе в pending
                        db.add(dialogue.rejected_alerts) # Добавляем для сохранения изменений
                    else:
                        logger.debug(
                            f"Уведомление об отклоненном кандидате для диалога {dialogue.hh_response_id} "
                            f"уже существует в RejectedNotificationQueue со статусом 'pending'.")
                else:
                    # Записи не существует, создаем новую
                    new_rejected_alert = RejectedNotificationQueue(
                        dialogue_id=dialogue.id,
                        status='pending'
                    )
                    db.add(new_rejected_alert)
                    logger.info(
                        f"Добавлено уведомление об отклоненном кандидате (диалог {dialogue.hh_response_id}) "
                        f"в RejectedNotificationQueue."
                    )
            # --- КОНЕЦ ИСПРАВЛЕННОГО БЛОКА КОДА ---



            logger.info(f"Candidate {dialogue.hh_response_id} rejected 🔴. Moving to 'assessment'.")

            api_move_start = time.monotonic()
            await hh_api.move_response_to_folder(recruiter, db, dialogue.hh_response_id, 'assessment')
            logger.debug(f"[{dialogue.hh_response_id}] API move: {time.monotonic() - api_move_start:.2f} sec.")

        # Если LLM не вернула текст, не отправляем сообщение
        if bot_response_text is None or bot_response_text == "":
            logger.info(f"[{dialogue.hh_response_id}] LLM вернула пустой ответ. Обновляем состояние без отправки сообщения.")

            # Сохраняем историю пользователя
            new_history = (dialogue.history or []) + user_entries_to_history
            dialogue.history = new_history[-150:]

            dialogue.dialogue_state = new_state
            dialogue.pending_messages = None
            dialogue.last_updated = datetime.datetime.now(datetime.timezone.utc)

            await db.commit()
            return

        # Отправка сообщения
        message_sent = await hh_api.send_message(recruiter, db, dialogue.hh_response_id, bot_response_text)

        if message_sent == 200:
            # Генерируем время ответа бота по МСК
            bot_response_time_msk = datetime.datetime.now(SPB_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S MSK') # <-- ДОБАВЛЕНО
            bot_message_entry = {
                'message_id': f'bot_{time.time()}',
                'role': 'assistant',
                'content': bot_response_text,
                'timestamp_msk': bot_response_time_msk, # <-- ДОБАВЛЕНО
                'extracted_data': extracted_data,
                'state': new_state
            }

            # Ограничиваем размер истории
            MAX_HISTORY_LENGTH = 150
            new_history = (dialogue.history or []) + user_entries_to_history + [bot_message_entry]
            dialogue.history = new_history[-MAX_HISTORY_LENGTH:]

            dialogue.dialogue_state = new_state
            dialogue.pending_messages = None
            dialogue.last_updated = datetime.datetime.now(datetime.timezone.utc)

            # Flush для проверки constraint violations перед commit
            await db.flush()
            await db.commit()

            logger.info(f"Dialogue {dialogue.hh_response_id} processed successfully")
        elif message_sent == 403:
            logger.warning(f"Failed to send message for dialogue {dialogue.hh_response_id}. Clearing pending messages to avoid loop.")
            dialogue.pending_messages = None
            await db.commit() # Сохраняем сброс очереди сообщений
            return
        else:
            logger.error(f"Failed to send message for dialogue {dialogue.hh_response_id}")
            await db.rollback()
            return

    except Exception as e:
        logger.error(f"Critical error processing dialogue {dialogue_id}: {e}", exc_info=True)
        if db and db.is_active:
            await db.rollback()
        raise  # Важно: пробрасываем исключение дальше

    finally:
        logger.debug(f"[{log_dialogue_hh_response_id}] Processing finished in: {time.monotonic() - dialogue_processing_start_time:.2f} sec.")


async def process_pending_dialogues(recruiter_id: int, prompt_library: dict, db: None):
    """
    Обновленная версия - каждый диалог в своей сессии.
    Параметр db больше не используется.
    """
    function_start_time = time.monotonic()

    try:
        logger.debug(f"Stage 3: Finding pending dialogues for recruiter {recruiter_id}...")
        debounce_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=DEBOUNCE_DELAY_SECONDS)

        # Быстрый запрос за списком ID
        async with SessionLocal() as lookup_db:
            db_query_start = time.monotonic()
            # --- ИСПРАВЛЕННЫЙ ЗАПРОС ---
            dialogues_info_result = await lookup_db.execute(
                select(Dialogue.id, Dialogue.hh_response_id)
                .join(Dialogue.vacancy)  # <--- 1. ПРИСОЕДИНЯЕМ ТАБЛИЦУ ВАКАНСИЙ
                .filter(
                    Dialogue.recruiter_id == recruiter_id,

                    # <--- 2. ВАЖНАЯ ПРОВЕРКА: Вакансия всё еще привязана к этому рекрутеру?
                    # Если get_all_active_vacancies... поставила NULL, этот диалог не попадет в выборку.
                    Vacancy.recruiter_id == recruiter_id,

                    Dialogue.last_updated <= debounce_time,
                    Dialogue.pending_messages.is_not(None),

                    # Проверка массива сообщений (как было у вас)
                    case(
                        (
                            func.jsonb_typeof(Dialogue.pending_messages) == 'array',
                            func.jsonb_array_length(Dialogue.pending_messages) > 0
                        ),
                        else_=False
                    )
                )
            )
            # -------------------------
            dialogues_to_process_info = dialogues_info_result.all()
            logger.debug(f"[Recruiter {recruiter_id}] DB query: {time.monotonic() - db_query_start:.2f}s. Found {len(dialogues_to_process_info)}")

        if not dialogues_to_process_info:
            logger.debug(f"No dialogues ready for recruiter {recruiter_id}")
            return

        logger.info(f"Found {len(dialogues_to_process_info)} dialogues for parallel processing")

        dialogue_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DIALOGUES)

        async def run_dialogue_task_with_semaphore(dialogue_id, rec_id, prompt_lib):
            """Каждый диалог получает СВОЮ сессию"""
            async with dialogue_semaphore:
                async with SessionLocal() as task_db_session:
                    try:
                        await _process_single_dialogue(dialogue_id, rec_id, prompt_lib, task_db_session)
                    except Exception as e:
                        logger.error(f"Dialogue {dialogue_id} processing failed: {e}", exc_info=True)
                        # Ошибка одного диалога не влияет на другие

        gather_start = time.monotonic()
        tasks = [
            run_dialogue_task_with_semaphore(d_id, recruiter_id, prompt_library)
            for d_id, hh_id in dialogues_to_process_info
        ]

        # return_exceptions=True - ошибки не прерывают обработку других диалогов
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if not isinstance(r, Exception))
        error_count = len(results) - success_count

        logger.debug(f"[Recruiter {recruiter_id}] Batch processing: {time.monotonic() - gather_start:.2f}s")
        logger.debug(f"Results: {success_count} success, {error_count} errors")

    finally:
        logger.debug(f"[Recruiter {recruiter_id}] process_pending_dialogues: {time.monotonic() - function_start_time:.2f}s")


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

                if not dialogue or not recruiter:
                    return

                # Проверка: если диалог уже не in_progress (мог измениться параллельно), выходим
                EXCLUDED_REMINDER_STATUSES = ['declined_interview', 'declined_vacancy' 'call_later']
                if (dialogue.status not in ['in_progress', 'timed_out'] or
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
                    logger.info(f"Вакансия для диалога {dialogue_hh_id} закрыта. Обновляю статус.")
                    dialogue.status = 'timed_out'
                    dialogue.reminder_level = 6
                    await db.commit()
                    return

                elif current_folder_on_hh != 'consider':
                    # Кандидат перемещен вручную рекрутером
                    logger.info(f"Диалог {dialogue_hh_id} перемещен в '{current_folder_on_hh}'. Отключаю напоминания.")
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
                elif dialogue.reminder_level == 3 and time_since_update > datetime.timedelta(days=7):
                    reminder_messages = ["Добрый день. Если вы еще находитесь в поиске работы, то будем рады пригласить вас пройти собеседование. Готовы продолжить диалог?"]
                    next_level = 4

                elif dialogue.reminder_level == 4 and time_since_update > datetime.timedelta(days=21):
                    reminder_messages = ["Добрый день. Вы трудоустроились? Если еще рассматриваете варианты, будем рады предложить вам пройти собеседование. А так же ответить на все вопросы, которые у вас есть. "]
                    next_level = 5

                elif dialogue.reminder_level == 5 and time_since_update > datetime.timedelta(days=51):
                    reminder_messages = ["Еще раз добрый день. Как ваши дела? Хотели бы сообщить вам, что вакансия вновь актуальна и если вы в поиске или задумываетесь о смене работы, мы с удовольствием пригласили бы вас на собеседование"]
                    next_level = 6

                # Выполнение действия
                if should_timeout:
                    # ТВОЕ ТРЕБОВАНИЕ: Если запись уже есть, ничего не делаем с таблицей молчунов
                    if not dialogue.inactive_alerts:
                        db.add(InactiveNotificationQueue(dialogue_id=dialogue.id, status='pending'))
                        logger.info(f"Диалог {dialogue_hh_id} впервые добавлен в InactiveNotificationQueue.")
                    else:
                        logger.debug(f"Диалог {dialogue_hh_id} уже зафиксирован в таблице молчунов. Повторная запись не требуется.")

                    # Но статус самого диалога и уровень напоминания обновляем в любом случае,
                    # чтобы пошел отсчет 7 дней для уровня 4.
                    dialogue.status = 'timed_out'
                    dialogue.reminder_level = 3
                    dialogue.last_updated = now
                    await db.commit()

                elif reminder_messages:
                    logger.info(f"Отправка напоминания уровня {next_level} для диалога {dialogue_hh_id}.")

                    # 1. Определяем типы напоминаний
                    is_long_reminder = next_level in [4, 5, 6]
                    # ТВОЕ ТРЕБОВАНИЕ: Списываем деньги только один раз (при переходе на 4 уровень)
                    should_charge = (next_level == 4) 
                    
                    settings = None

                    # 2. Проверяем баланс только если это ПЕРВОЕ долгое напоминание
                    if should_charge:
                        settings_res = await db.execute(select(AppSettings).filter_by(id=1))
                        settings = settings_res.scalar_one_or_none()
                        
                        if not settings or settings.balance < settings.cost_per_long_reminder:
                            logger.warning(f"Баланс пуст. Первое долгое напоминание для {dialogue_hh_id} отменено.")
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
                            if should_charge and settings:
                                settings.balance -= settings.cost_per_long_reminder
                                logger.info(f"ЕДИНОВРЕМЕННОЕ СПИСАНИЕ: {settings.cost_per_long_reminder} руб. за активацию долгих напоминаний.")

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
                logger.error(f"Ошибка в задаче напоминания для диалога {dialogue_id}: {e}")
                # Не рейзим ошибку, чтобы не поломать gather


async def process_reminders(recruiter_id: int, db: AsyncSession):
    """
    Этап 4: Параллельная отправка напоминаний.
    Аргумент db здесь используется только для получения списка ID,
    далее каждый таск создает свою сессию.
    """
    function_start_time = time.monotonic()

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
        EXCLUDED_REMINDER_STATUSES = ['declined_vacancy', 'declined_interview', 'call_later']

        result = await db.execute(
            select(Dialogue.id)
            .filter(
                Dialogue.recruiter_id == recruiter_id,
                # ИЗМЕНЕНИЕ: Разрешаем обработку и для тех, кто уже в статусе timed_out,
                # но еще не достиг финального уровня напоминаний
                Dialogue.status.in_(['in_progress', 'timed_out']), 

                Dialogue.dialogue_state.notin_(EXCLUDED_REMINDER_STATUSES),
                Dialogue.reminder_level < 6 # 6 — это будет последнее напоминание (21 день)
            )
        )
        candidate_ids_to_check = result.scalars().all()

        if not candidate_ids_to_check:
            return

        logger.debug(f"Запуск параллельной проверки напоминаний для {len(candidate_ids_to_check)} диалогов...")

        # 3. Создаем задачи для параллельного выполнения
        tasks = [
            _process_single_reminder_task(d_id, recruiter_id, REMINDER_SEMAPHORE)
            for d_id in candidate_ids_to_check
        ]

        # 4. Запускаем и ждем выполнения (return_exceptions=True чтобы ошибка в одном не крашила всё)
        await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        logger.error(f"Ошибка в process_reminders (диспетчер): {e}", exc_info=True)
    finally:
        logger.debug(
            f"[Recruiter ID {recruiter_id}] "
            f"process_reminders finished in {time.monotonic() - function_start_time:.2f}s"
        )

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
                )
                reminders_to_send = result.scalars().all()

                if not reminders_to_send:
                    # logger.debug("[Interview Reminders] Нет ожидающих напоминаний. Пауза.")
                    await asyncio.sleep(30) # Пауза, если нет задач
                    continue

                logger.info(f"[Interview Reminders] Найдено {len(reminders_to_send)} напоминаний для отправки.")

                for reminder in reminders_to_send:
                    try:
                        dialogue = reminder.dialogue
                        recruiter = reminder.recruiter # Объект рекрутера уже загружен
                        vacancy = dialogue.vacancy
                        candidate = dialogue.candidate

                        if not dialogue or not recruiter or not vacancy or not candidate:
                            logger.error(
                                f"Не удалось загрузить связанные объекты для напоминания {reminder.id}. "
                                f"Dialogue: {bool(dialogue)}, Recruiter: {bool(recruiter)}, "
                                f"Vacancy: {bool(vacancy)}, Candidate: {bool(candidate)}"
                            )
                            reminder.status = 'error'
                            reminder.processed_at = now_utc
                            await db_session.commit()
                            continue

                        if not recruiter.access_token:
                            logger.error(f"У рекрутера {recruiter.name} (ID: {recruiter.id}) нет access_token. Не могу отправить напоминание {reminder.id}.")
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
                            logger.error(f"Не найден шаблон сообщения для типа уведомления '{reminder.notification_type}'. Напоминание {reminder.id} не будет отправлено.")
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

                        logger.info(f"Отправка напоминания типа '{reminder.notification_type}' для диалога {dialogue.hh_response_id} от рекрутера {recruiter.name}...")

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
                            logger.info(f"Напоминание {reminder.id} успешно отправлено кандидату {candidate.full_name}.")

                        elif send_result == 403:
                            # ВАКАНСИЯ ЗАКРЫТА
                            reminder.status = 'cancelled' # Отменяем, так как отправлять бессмысленно
                            logger.warning(
                                f"Напоминание {reminder.id} ОТМЕНЕНО: Вакансия закрыта/в архиве. "
                                f"Кандидат: {candidate.full_name}, Диалог: {dialogue.hh_response_id}"
                            )

                        else:
                            # ПРОЧИЕ ОШИБКИ (False или другие коды)
                            reminder.status = 'error'
                            logger.error(f"Не удалось отправить напоминание {reminder.id} кандидату {candidate.full_name} (API Error).")
                        # -----------------------

                        reminder.processed_at = now_utc
                        await db_session.commit() # Коммитим каждое напоминание отдельно для надежности

                    except Exception as e:
                        logger.error(f"Ошибка при обработке напоминания {reminder.id}: {e}", exc_info=True)
                        if reminder.id: # Убедимся, что reminder объект существует
                            reminder.status = 'error'
                            reminder.processed_at = now_utc
                            await db_session.commit() # Попытаемся сохранить статус ошибки
                        else:
                            await db_session.rollback() # Откатываем, если ошибка до создания reminder
            except Exception as e:
                logger.critical(f"Критическая ошибка в фоновом обработчике напоминаний о собеседованиях: {e}", exc_info=True)
                await db_session.rollback() # Откат при ошибке верхнего уровня

        await asyncio.sleep(30) # Пауза между циклами проверки




async def handle_single_recruiter(rec_id: int, prompt_library: dict):
    """Гибридный подход: группируем связанные операции"""
    recruiter_processing_start_time = time.monotonic()
    # 1. Инициализируем имя заранее, чтобы блок finally не падал при раннем return
    recruiter_name = f"ID {rec_id}"
    try:
        # Быстрая проверка существования рекрутера
        async with SessionLocal() as check_db:
            result = await check_db.execute(
                select(TrackedRecruiter.name, TrackedRecruiter.access_token).filter_by(id=rec_id)
            )
            recruiter_data = result.first()

            if not recruiter_data or not recruiter_data[1]:
                logger.warning(f"Skipping recruiter {rec_id}: no token")
                return

        recruiter_name = recruiter_data[0]
        logger.debug(f"--- Starting work with recruiter: {recruiter_name} (ID: {rec_id}) ---")

        # Получаем вакансии (функция сама управляет сессией)
        active_vacancies = await get_all_active_vacancies_for_recruiter(rec_id)

        if not active_vacancies:
            logger.warning(f"No active vacancies for recruiter {recruiter_name}")
            return

        vacancy_ids = [v['id'] for v in active_vacancies]

        # ЭТАП 1+2: Сканирование новых откликов (логически связаны, короткие операции)
        # Эти этапы быстрые (несколько секунд) и логически одна "фаза сканирования"

        try:
            scan_start = time.monotonic()

                # Параллельное выполнение этапов 1 и 2
            await asyncio.gather(
                process_new_responses(rec_id, vacancy_ids),
                process_ongoing_responses(rec_id, vacancy_ids)
            )

                # Коммитим только если оба этапа успешны

            logger.debug(f"[{recruiter_name}] Scan phase: {time.monotonic() - scan_start:.2f}s")

        except Exception as e:
            logger.error(f"[{recruiter_name}] Scan phase failed: {e}", exc_info=True)

                # Не прерываем работу - идем дальше к обработке диалогов

        # ЭТАП 3: Обработка диалогов (ОТДЕЛЬНАЯ сессия - долгие операции)
        # Каждый диалог получает свою сессию через run_dialogue_task_with_semaphore
        try:
            dialogues_start = time.monotonic()
            await process_pending_dialogues(rec_id, prompt_library, None)  # Сессия не нужна
            logger.debug(f"[{recruiter_name}] Dialogues phase: {time.monotonic() - dialogues_start:.2f}s")
        except Exception as e:
            logger.error(f"[{recruiter_name}] Dialogues phase failed: {e}", exc_info=True)
            # Не прерываем - идем к напоминаниям

        # ЭТАП 4: Напоминания (ОТДЕЛЬНАЯ сессия - независимая операция)
        async with SessionLocal() as reminders_db:
            try:
                reminders_start = time.monotonic()
                await process_reminders(rec_id, reminders_db)
                await reminders_db.commit()
                logger.debug(f"[{recruiter_name}] Reminders phase: {time.monotonic() - reminders_start:.2f}s")
            except Exception as e:
                logger.error(f"[{recruiter_name}] Reminders phase failed: {e}", exc_info=True)
                await reminders_db.rollback()

    except Exception as e:
        logger.error(f"Critical error in handle_single_recruiter {rec_id}: {e}", exc_info=True)

    finally:
        logger.debug(f"--- Recruiter {recruiter_name} completed: {time.monotonic() - recruiter_processing_start_time:.2f}s ---")

async def run_worker_cycle():
    """Главный цикл, который запускает независимые асинхронные задачи."""
    cycle_start_time = time.monotonic()
    try:
        logger.info("Начало нового цикла воркера.")
        prompt_library = knowledge_base.get_prompt_library()

        all_recruiters_ids = []
        async with SessionLocal() as db: # Эта сессия только для получения списка ID рекрутеров
            try:
                result = await db.execute(select(TrackedRecruiter.id))
                all_recruiters_ids = result.scalars().all()
            finally:
                pass # Сессия 'db' закрывается здесь.

        if not all_recruiters_ids:
            logger.warning("Нет отслеживаемых рекрутеров в БД. Цикл пропущен.")
            return

        recruiter_semaphore = asyncio.Semaphore(MAX_CONCURRENT_RECRUITERS)

        # Теперь передаем только ID рекрутера, а handle_single_recruiter загрузит его в свою сессию
        tasks = [handle_single_recruiter(recruiter_id, prompt_library) for recruiter_id in all_recruiters_ids]

        async def run_task_with_semaphore(task_coro):
            async with recruiter_semaphore:
                await task_coro

        await asyncio.gather(*[run_task_with_semaphore(task) for task in tasks])

    except Exception as e:
        logger.critical("Критическая ошибка в главном цикле воркера!", exc_info=True)
    finally:
        cycle_end_time = time.monotonic()
        logger.info(f"Цикл воркера завершен. Общее время: {cycle_end_time - cycle_start_time:.2f} сек.")
        logger.debug("Цикл воркера завершен.")

async def main():
    """Главная асинхронная функция."""

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("HH-Worker запускается...")
    # --- ДОБАВИТЬ ЭТУ СТРОКУ ---
    interview_reminders_task = asyncio.create_task(check_and_send_interview_reminders())
    # --- КОНЕЦ ДОБАВЛЕНИЯ ---
    try:
        while not shutdown_requested:
            try:
                await run_worker_cycle()

                logger.debug(f"Пауза {CYCLE_PAUSE_SECONDS} секунд перед следующим циклом.")

                for _ in range(CYCLE_PAUSE_SECONDS):
                    if shutdown_requested:
                        break
                    await asyncio.sleep(1)

            except Exception as e:
                logger.critical(f"Неперехваченная критическая ошибка в главном цикле: {e}", exc_info=True)
                if not shutdown_requested:
                    await asyncio.sleep(120)
    finally:
        logger.info("Закрываем соединения...")
        await cleanup() # Очистка LLM ресурсов
        # --- ДОБАВИТЬ ЭТУ СТРОКУ ---
        await hh_api_real.close_api_client()
        # ---------------------------
        # --- ДОБАВИТЬ ЭТУ СТРОКУ ---
        interview_reminders_task.cancel() # Отмена задачи при завершении
        # --- КОНЕЦ ДОБАВЛЕНИЯ ---
        logger.info("HH-Worker полностью остановлен.")


if __name__ == "__main__":
    setup_logging(log_filename="hh_worker.log")
    load_dotenv()

    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Приложение принудительно завершено.")