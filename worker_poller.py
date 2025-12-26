#worker_poller.py

import asyncio
import logging
import argparse
import sys
import signal
import time
import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from dotenv import load_dotenv
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from hr_bot.utils.logger_config import setup_logging
from hr_bot.db.models import SessionLocal, Dialogue, Candidate, Vacancy, TrackedRecruiter, AppSettings
from hr_bot.services import hh_api_real as hh_api
from hr_bot.db import statistics_manager
from hr_bot.utils.system_notifier import send_system_alert

# --- КОНФИГУРАЦИЯ ПОЛЛЕРА ---
CYCLE_PAUSE_SECONDS = 5  # Можно сделать чаще, так как нет LLM
MAX_CONCURRENT_RECRUITERS = 10
VACANCY_CACHE_DURATION_MINUTES = 2
TEST_NEGOTIATION_ID = None

logger = logging.getLogger(__name__)

try:
    SPB_TIMEZONE = ZoneInfo("Europe/Moscow")
except ZoneInfoNotFoundError:
    SPB_TIMEZONE = datetime.timezone.utc

shutdown_requested = False

def signal_handler(sig, frame):
    global shutdown_requested
    logger.info("Получен сигнал остановки Поллера...")
    shutdown_requested = True





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
                    settings_result = await db.execute(
                        select(AppSettings).filter_by(id=1).with_for_update()
                    )
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

                    # СПИСАНИЕ СРЕДСТВ + СТАТИСТИКА
                    cost = settings.cost_per_dialogue
                    settings.balance -= cost
                    settings.total_spent_on_dialogues += cost # Увеличиваем счетчик диалогов

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


async def handle_single_recruiter_polling(rec_id: int):
    """Только сбор данных: вакансии, новые отклики, новые сообщения"""
    try:
        async with SessionLocal() as db:
            result = await db.execute(
                select(TrackedRecruiter).filter_by(id=rec_id)
            )
            recruiter = result.scalar_one_or_none()
            if not recruiter or not recruiter.access_token:
                return

        # 1. Синхронизация вакансий
        active_vacancies = await get_all_active_vacancies_for_recruiter(rec_id)
        if not active_vacancies:
            return

        vacancy_ids = [v['id'] for v in active_vacancies]

        # 2. Параллельный сбор откликов и сообщений
        # ВАЖНО: Мы не вызываем здесь обработку диалогов или LLM!
        await asyncio.gather(
            process_new_responses(rec_id, vacancy_ids),
            process_ongoing_responses(rec_id, vacancy_ids)
        )
        
    except Exception as e:
        logger.error(f"Ошибка Поллера для рекрутера {rec_id}: {e}", exc_info=True)

async def run_poller_cycle(target_ids=None):
    """Цикл опроса только для указанных ID рекрутеров"""
    while not shutdown_requested:
        start_time = time.monotonic()
        try:
            async with SessionLocal() as db:
                query = select(TrackedRecruiter.id)
                if target_ids:
                    query = query.where(TrackedRecruiter.id.in_(target_ids))
                
                result = await db.execute(query)
                recruiter_ids = result.scalars().all()

            if not recruiter_ids:
                logger.debug("Нет рекрутеров для опроса.")
            else:
                semaphore = asyncio.Semaphore(MAX_CONCURRENT_RECRUITERS)
                
                async def sem_task(rid):
                    async with semaphore:
                        await handle_single_recruiter_polling(rid)

                await asyncio.gather(*(sem_task(rid) for rid in recruiter_ids))

        except Exception as e:
            logger.error(f"Ошибка в цикле поллера: {e}")
        
        # Пауза между циклами
        wait_time = max(1, CYCLE_PAUSE_SECONDS - (time.monotonic() - start_time))
        await asyncio.sleep(wait_time)



async def main():
    parser = argparse.ArgumentParser(description="HH Poller Worker")
    parser.add_argument("--recruiters", type=str, help="ID рекрутеров через запятую (напр. 1,2,3)")
    args = parser.parse_args()

    target_ids = None
    if args.recruiters:
        target_ids = [int(i.strip()) for i in args.recruiters.split(",")]
        logger.info(f"Воркер запущен для рекрутеров: {target_ids}")
    else:
        logger.info("Воркер запущен для ВСЕХ рекрутеров")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    await run_poller_cycle(target_ids)
    
    await hh_api.close_api_client()
    logger.info("Поллер остановлен.")

if __name__ == "__main__":
    setup_logging(log_filename="poller.log")
    load_dotenv()
    asyncio.run(main())