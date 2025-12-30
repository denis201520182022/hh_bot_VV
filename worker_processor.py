#worker_processor.py


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
shutdown_requested = False

def signal_handler(sig, frame):
    global shutdown_requested
    logger.info("Получен сигнал остановки Процессора. Завершаем текущие задачи...")
    shutdown_requested = True


logger = logging.getLogger(__name__)


try:
    SPB_TIMEZONE = ZoneInfo("Europe/Moscow")
except ZoneInfoNotFoundError:
    logger.critical("Системная ошибка: часовой пояс 'Europe/Moscow' не найден", extra={"fallback": "UTC"})
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
        logger.debug("Расход токенов на анализ гражданства зафиксирован", extra={
            "hh_response_id": dialogue.hh_response_id,
            "cost": float(total_call_cost),
            "tokens": total_tokens
        })

        # Обновление счетчиков диалога
        dialogue.total_prompt_tokens += p_tokens
        dialogue.total_completion_tokens += c_tokens
        dialogue.total_cached_tokens += cached_tokens
        dialogue.total_cost += total_call_cost
        
        await db.flush()
    except Exception as e:
        logger.error("Ошибка при сохранении статистики токенов гражданства", extra={
            "hh_response_id": dialogue.hh_response_id,
            "error": str(e)
        })



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
        logger.error("Не удалось записать отсутствующую вакансию в файл", extra={
            "vacancy_entry": entry,
            "error": str(e)
        })

def _find_relevant_vacancy(prompt_library: dict, vacancy_title: str, vacancy_city: str) -> str:
    """
    Поиск вакансии по принципу BEST MATCH с логикой исключения по критическим словам.
    """

    def normalize_text(text: str) -> str:
        if not text: 
            return ""
        text = text.lower().replace("ё", "е")
        text = re.sub(r"[^\w\s]", " ", text)
        return " ".join(text.split())

    def get_title_similarity(input_str: str, db_str: str) -> float:
        input_words = set(normalize_text(input_str).split())
        db_words = set(normalize_text(db_str).split())

        intersection = input_words.intersection(db_words)
        if not intersection:
            return 0.0

        critical_words = {
            'старший', 'младший', 'ночной', 'неполный', 'мобильный', 
            'администратор', 'директор', 'товаровед', 'универсал', 
            'пекарь', 'повар', 'кафе', 'бариста', 'кухни', 'сборщик', 'кассир'
        }

        extra_in_db = db_words - input_words
        for word in extra_in_db:
            if word in critical_words:
                return 0.0

        extra_in_input = input_words - db_words
        for word in extra_in_input:
             if word in critical_words:
                return 0.0

        recall = len(intersection) / len(db_words)
        precision = len(intersection) / len(input_words)
        
        return (recall * 0.4) + (precision * 0.6)

    # Логика обработки городов
    norm_input_city = normalize_text(vacancy_city)
    city_synonyms = {
        "спб": "санкт петербург",
        "питер": "санкт петербург",
        "ленобласть": "ленинградская область"
    }
    for short, full in city_synonyms.items():
        if short in norm_input_city:
            norm_input_city = norm_input_city.replace(short, full)

    best_match_vacancy = None
    best_match_score = 0.0
    matched_city_name = "не определен" # Переменная для хранения города из БД

    # Перебираем вакансии из библиотеки
    for vacancy in prompt_library.get("vacancies", []):
        
        # --- ПРОВЕРКА ГОРОДА ---
        city_match = False
        current_vacancy_matched_city = None
        
        db_cities_raw = vacancy.get("cities", [])
        # Создаем список нормализованных городов для сравнения
        db_cities_norm = [normalize_text(c) for c in db_cities_raw]
        
        for i, db_city_norm in enumerate(db_cities_norm):
            if norm_input_city in db_city_norm or db_city_norm in norm_input_city:
                city_match = True
                # Запоминаем оригинальное название города из БД (например, "Бугры (СПб)")
                current_vacancy_matched_city = db_cities_raw[i]
                break
        
        if not city_match:
            continue

        # --- ПРОВЕРКА НАЗВАНИЯ ---
        max_title_score = 0.0
        for db_title in vacancy.get("titles", []):
            score = get_title_similarity(vacancy_title, db_title)
            if score > max_title_score:
                max_title_score = score

        if max_title_score < 0.4:
            continue

        # Запоминаем лучший результат
        if max_title_score > best_match_score:
            best_match_score = max_title_score
            best_match_vacancy = vacancy
            matched_city_name = current_vacancy_matched_city

    # --- РЕЗУЛЬТАТ С РАСШИРЕННЫМ ЛОГОМ ---
    if best_match_vacancy:
        logger.info(
            "✅ ВЫБРАНО ОПИСАНИЕ | HH: [%s, %s] | DB: [%s, %s] | score=%.2f",
            vacancy_title, 
            vacancy_city,
            best_match_vacancy['titles'][0], 
            matched_city_name,
            best_match_score
        )
        return best_match_vacancy.get("description", "")

    # Если ничего не нашли
    logger.warning(
        "🤡 Описание не найдено | HH title='%s' | HH city='%s'",
        vacancy_title,
        vacancy_city,
    )
    _log_missing_vacancy(vacancy_title, vacancy_city)

    return (
        "ОПИСАНИЕ ВАКАНСИИ НЕ НАЙДЕНО. "
        "Отвечай на вопросы кандидата на основе общей информации из FAQ."
    )

def _generate_calendar_context() -> str:
    """
    Генерирует текстовый блок с календарем и правилами работы с датами.
    """
    moscow_tz = ZoneInfo("Europe/Moscow")
    current_datetime_utc = datetime.datetime.now(moscow_tz)
    weekdays_ru = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
    
    # Словарь для склонения в "следующий/следующая/следующее"
    weekday_next_form = {
        "понедельник": "Следующий понедельник",
        "вторник": "Следующий вторник",
        "среда": "Следующая среда",
        "четверг": "Следующий четверг",
        "пятница": "Следующая пятница",
        "суббота": "Следующая суббота",
        "воскресенье": "Следующее воскресенье"
    }

    current_weekday = weekdays_ru[current_datetime_utc.weekday()]
    current_date_str = current_datetime_utc.strftime("%Y-%m-%d")
    current_time_str = current_datetime_utc.strftime("%H:%M")

    # Собираем информацию о днях недели для отслеживания повторений
    weekday_occurrences = {}
    
    calendar_context_lines = []
    for i in range(14):
        date_cursor = current_datetime_utc + datetime.timedelta(days=i)
        wd_name = weekdays_ru[date_cursor.weekday()]
        date_str = date_cursor.strftime("%Y-%m-%d")
        
        # Отслеживаем, сколько раз встречался этот день недели
        if wd_name not in weekday_occurrences:
            weekday_occurrences[wd_name] = 0
        weekday_occurrences[wd_name] += 1
        
        # Формируем префикс и суффикс
        prefix = ""
        suffix = ""
        
        if i == 0:
            prefix = "(СЕГОДНЯ) "
            suffix = " ← ТЫ ЗДЕСЬ"
            day_label = wd_name.capitalize()
        elif i == 1:
            prefix = "(ЗАВТРА) "
            day_label = wd_name.capitalize()
        elif i == 2:
            prefix = "(ПОСЛЕЗАВТРА) "
            day_label = wd_name.capitalize()
        else:
            # Для остальных дней
            if weekday_occurrences[wd_name] == 2:
                # Второе упоминание дня недели - добавляем "Следующий"
                day_label = weekday_next_form[wd_name]
            else:
                # Первое упоминание или третье+ - просто название с заглавной
                day_label = wd_name.capitalize()
        
        calendar_context_lines.append(f"{prefix}{date_str} {day_label}{suffix}")

    calendar_string = "\n".join(calendar_context_lines)

    calendar_context = (
        f"\n\n[CRITICAL CALENDAR CONTEXT]\n"
        f"ТЕКУЩАЯ ДАТА И ВРЕМЯ (МСК): {current_datetime_utc.strftime('%Y-%m-%d %H:%M')}\n"
        f"СЕГОДНЯ: {current_weekday}, {current_date_str}\n\n"
        f"СЕЙЧАС: {current_time_str} (МСК)\n"
        f"⚠️ ВАЖНО: Ты ОЧЕНЬ ПЛОХО считаешь даты в уме. НИКОГДА НЕ ВЫЧИСЛЯЙ ДАТЫ САМОСТОЯТЕЛЬНО!\n"
        f"Используй ТОЛЬКО эту таблицу (таблица начинается с СЕГОДНЯ и идет на 14 дней вперед):\n\n"
        f"{calendar_string}\n\n"
        f"ПРАВИЛА РАБОТЫ С ДАТАМИ:\n"
        f"1. Кандидат говорит просто день недели ('понедельник', 'вторник'):\n"
        f"   → Найди ПЕРВУЮ строку с этим днем (без слова 'Следующий')\n"
        f"   → Скопируй дату из этой строки\n\n"
        f"2. Если кандидат говорит 'СЛЕДУЮЩИЙ [день недели]' (например, 'следующий понедельник'):\n"
        f"   → Бери такой день из списка выше, где написано 'СЛЕДУЮЩИЙ [день недели]' (например, 'следующий понедельник')\n\n"
        f"   → Скопируй дату из этой строки\n\n"
        f"3. Если кандидат называет день недели, который совпадает с СЕГОДНЯ:\n"
        f"   → ОБЯЗАТЕЛЬНО уточни: 'Вы имеете в виду сегодня или через неделю?'\n\n"
        f"4. Если кандидат говорит 'сегодня', 'завтра', 'послезавтра':\n"
        f"   → Ищи в списке пометку 'СЕГОДНЯ', 'ЗАВТРА' или 'ПОСЛЕЗАВТРА'\n\n"
        f"5. ВСЕГДА копируй дату ТОЧНО из таблицы в формате YYYY-MM-DD\n"
        f"6. НИКОГДА не изобретай даты сам - только из этой таблицы!\n"
        f"═══════════════════════════════════════════════════════════\n"
        f"ПРИМЕРЫ:\n"
        f"═══════════════════════════════════════════════════════════\n"
        f"Кандидат: 'понедельник' → Ты ищешь 'Понедельник'\n"
        f"Кандидат: 'следующий понедельник' → Ты ищешь строчку 'Следующий понедельник'\n"
        f"Кандидат: 'завтра' → Ты ищешь строчку с пометкой '(ЗАВТРА)'\n"
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
        'clarifying_citizenship': ['#QUALIFICATION_RULES#','#CLARI#'],
        'awaiting_age': ['#QUALIFICATION_RULES#'],
        'clarifying_anything': ['#QUALIFICATION_RULES#'],
        'clarifying_declined_vacancy': ['#QUALIFICATION_RULES#'],

        'qualification_complete': ['#QUALIFICATION_RULES#'],
        'call_later': ['#QUALIFICATION_RULES#'],

        'init_scheduling_spb': ['#SCHEDULING_ALGORITHM#'],
        'post_qualification_chat': ['#SCHEDULING_ALGORITHM#'],
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
    # Начальный логгер (пока не загрузили данные из БД)
    log = logging.LoggerAdapter(logger, {
        "dialogue_id": dialogue_id,
        "recruiter_id": recruiter_id
    })
    dialogue = None
    recruiter = None
    # --- ЗАГРУЗКА ЗДЕСЬ (значение по умолчанию) ---
    log_dialogue_hh_response_id = f"ID {dialogue_id}"
    # --- КОНЕЦ ЗАГРУЗКИ ---

    try:
        # Проверка активности сессии
        if not db.is_active:
            log.error("Сессия базы данных не активна, обработка прервана")
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
        if not dialogue or not recruiter:
            log.error("Диалог или рекрутер не найдены в базе данных")
            return
        # Теперь у нас есть hh_response_id и vacancy_id — добавляем их в контекст всех будущих логов
        log = logging.LoggerAdapter(logger, {
            "dialogue_id": dialogue_id,
            "recruiter_id": recruiter_id,
            "hh_response_id": dialogue.hh_response_id,
            "vacancy_id": dialogue.vacancy.hh_vacancy_id
        })

        log.debug("Данные диалога успешно загружены из БД", extra={
            "fetch_duration": round(time.monotonic() - db_fetch_start, 4)
        })

        
        # --- ЗАГРУЗКА ЗДЕСЬ ---
        log_dialogue_hh_response_id = dialogue.hh_response_id
        # --- КОНЕЦ ЗАГРУЗКИ ---
        # Принудительно загружаем связанные объекты в сессию
        await db.refresh(dialogue.candidate)
        await db.refresh(dialogue.vacancy)

        log.info("Запуск обработки сообщений в диалоге")

        pending_messages = dialogue.pending_messages or []
        if not pending_messages:
            log.debug("В очереди pending_messages нет новых сообщений, пропуск")
            return

        # *************************************************************************************************************************************
        # СПЕЦИАЛЬНАЯ ОБРАБОТКА ДЛЯ AWAITING_CITIZENSHIP
        # *************************************************************************************************************************************
        if dialogue.dialogue_state == "awaiting_citizenship" and pending_messages:
            all_pending_content = "\n".join([pm.get('content', '') if isinstance(pm, dict) else str(pm) for pm in pending_messages])
            log.debug("Запуск анализа гражданства кандидата через LLM")
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
                Если в сообщениях содержится информация, что человек имеет ВНЖ России (РФ) или РВП России (РФ), то верни в "citizenship" строго значение "внж рф" или "рвп рф"
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
                    skip_instructions=True,
                    log_context=log
                )

                if llm_citizenship_response:
                    # 1. Логируем успешный расход (токены и деньги)
                    await _record_citizenship_usage(db, dialogue, llm_citizenship_response)
                    
                    # 2. Логируем "пустышки" для всех предыдущих неудачных попыток (ретраев), если они были
                    total_attempts = len(citizenship_attempts)
                    if total_attempts > 1:
                        log.warning("Анализ гражданства выполнен успешно после ретраев", extra={
                            "retry_attempts": total_attempts - 1
                        })
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
                            log.info("Гражданство успешно определено", extra={
                                "citizenship": citizenship,
                                "new_state": dialogue.dialogue_state
                            })
                            citizenship = parsed_response.get("citizenship")
                            system_command_content = None

                            if citizenship == "ЕАЭС":
                                system_command_content = "[SYSTEM COMMAND] Кандидат сообщил что у него гражданство одной из стран ЕАЭС, поставь в поле citizenship строго значение 'ЕАЭС' и переходи к следующему этапу анкеты (возрасту)"
                                
                            if citizenship == "внж рф" or  citizenship == "рвп рф":
                                system_command_content = "[SYSTEM COMMAND] Кандидат сообщил что у него РВП РФ или ВНЖ РФ, поставь в поле citizenship строго значение строго значение 'внж рф' или 'рвп рф' соответственно и переходи к следующему этапу анкеты (возрасту)"
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
                            log.info("Информация о гражданстве не найдена в текущих сообщениях")

                    except Exception as parse_err:
                        log.error("Ошибка разбора JSON ответа по гражданству", extra={"error": str(parse_err)})

            except Exception as citizenship_err:
                # --- ЛОГИРОВАНИЕ ПОЛНОГО ПРОВАЛА ---
                # Если tenacity исчерпала попытки, записываем в БД все неудачные заходы
                log.error("Анализ гражданства полностью провален после всех попыток", extra={
                    "attempts_made": len(citizenship_attempts)
                }, exc_info=True)
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
        log.debug("Подготовка к отправке запроса в LLM", extra={
            "current_state": dialogue.dialogue_state,
            "history_depth": len(dialogue.history or []),
            "pending_count": len(pending_messages)
        })
        try:
            # Передаем attempt_tracker в функцию
            llm_data = await llm_handler.get_bot_response(
                system_prompt=final_system_prompt,
                dialogue_history=dialogue.history or [],
                user_message=combined_masked_message,
                current_datetime_utc=datetime.datetime.now(datetime.timezone.utc),
                attempt_tracker=attempt_tracker,
                log_context=log # <--- Передаем список
            )
            
            # --- УСПЕШНЫЙ СЦЕНАРИЙ ---
            # Если мы здесь, значит последняя попытка была успешной.
            # Если в attempt_tracker больше 1 элемента, значит были скрытые ретраи.
            
            total_attempts = len(attempt_tracker)
            failed_attempts = total_attempts - 1 # Все кроме последней (успешной)
            
            if failed_attempts > 0:
                log.warning("Запрос к LLM выполнен успешно, но потребовались повторные попытки", extra={
                    "failed_retries": failed_attempts
                })
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
            
            log.error("Запрос к LLM полностью провален после всех попыток", extra={
                "attempts_count": len(attempt_tracker),
                "error_type": type(llm_error).__name__
            }, exc_info=True)
            
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
                log.error(f"Failed to log LLM errors to DB: {log_ex}")

            raise llm_error # Пробрасываем ошибку дальше

        log.debug("Ответ от LLM получен", extra={
            "llm_duration_sec": round(time.monotonic() - llm_call_start, 2)
        })

        if llm_data is None:
            log.error("LLM сервис вернул пустой ответ (None)")
            alert_message = "⚠️ LLM service unavailable!"
            await send_system_alert(alert_message, alert_type="admin_only")
            return

        # Распаковка ответа
        llm_response = llm_data.get("parsed_response")
        usage_stats = llm_data.get("usage_stats")

        # Достаем переменные, чтобы они были доступны ВЕЗДЕ ниже
        bot_response_text = llm_response.get("response_text")
        new_state = llm_response.get("new_state", "error_state") # <--- ВОТ ОНА НУЖНА В ЛОГЕ
        extracted_data = llm_response.get("extracted_data")

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
                log.info("Запрос к нейросети завершен успешно", extra={
                    "cost_usd": float(total_call_cost),
                    "tokens_prompt": p_tokens,
                    "tokens_completion": c_tokens,
                    "tokens_cached": cached_tokens,
                    "new_state": new_state
                })
                await db.commit() 
                
                # После коммита объекты могут "отцепиться" (expire), поэтому рефрешим dialogue
                # чтобы дальше с ним работать в этой сессии
                await db.refresh(dialogue) 
                # (Если usage_log дальше не нужен, его можно не рефрешить)

            except Exception as e:
                log.error("Ошибка при сохранении статистики токенов в БД", exc_info=True, extra={"error": str(e)})
        # ===========================

        

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
            log.info("Анкета собрана полностью, запуск программной проверки критериев")

            is_eligible = check_candidate_eligibility(dialogue.candidate)

            if not is_eligible:
                # --- СЦЕНАРИЙ 1: ОТКАЗ ---
                log.info("Кандидат отклонен по критериям (возраст/гражданство)", extra={
                    "age": dialogue.candidate.age,
                    "citizenship": dialogue.candidate.citizenship
                })

                # Принудительно меняем состояние и текст (игнорируем то, что написала LLM)
                new_state = 'qualification_failed'
                bot_response_text = "Спасибо! Я передам Вашу анкету для рассмотрения. Если по Вашей анкете будет принято положительное решение с Вами свяжутся в течение трёх рабочих дней."

                # Логика перемещения в папку 'assessment' и создания уведомления сработает ниже,
                # так как мы установили new_state = 'qualification_failed'

            else:
                # --- СЦЕНАРИЙ 2: ПОДХОДИТ ---
                log.info("Кандидат успешно прошел проверку критериев")

                # Проверяем город
                city_lower = (dialogue.vacancy.city or "").lower()
                is_spb = any(x in city_lower for x in ['санкт-петербург'])

                if not is_spb:
                    # --- 2.1 НЕ СПБ (Регионы) ---
                    log.info("Кандидат из региона (не СПб), передача анкеты рекрутеру", extra={"city": dialogue.candidate.city})
                    new_state = 'forwarded_to_researcher'
                    bot_response_text = "Спасибо! Я передам Вашу заявку нашим коллегам. Мы свяжемся с Вами в рабочее время, чтобы согласовать время собеседования."
                    # Логика перемещения в папку 'interview' и смены статуса на 'qualified' сработает ниже



                else:

                    current_title_lower = (vacancy_title or "").lower()

                    # Список фраз, которые ищем в названии
                    excluded_vacancies = ['повар-пекарь', 'повар неполный день', 'повар', 'бариста', 'уборщик','уборщица','помошник повара',]

                    # Проверяем, входит ли хоть одна фраза из списка в название вакансии
                    if any(phrase in current_title_lower for phrase in excluded_vacancies):
                        log.info("Вакансия в СПб входит в список исключений, передача анкеты рекрутеру", extra={"title": vacancy_title})
                        new_state = 'forwarded_to_researcher'
                        bot_response_text = "Спасибо! Я передам Вашу заявку нашим коллегам. Мы свяжемся с Вами в рабочее время, чтобы согласовать время собеседования."

                    else:
                        # --- 2.2 СПБ (Запись на собеседование) ---
                        log.info("Запуск алгоритма автоматической записи на собеседование (СПб)")


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
            
            log.info("Анкета заполнена не полностью, отправлен запрос недостающих данных")
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

        # --- НОВЫЙ БЛОК: Обработка состояния call_later ---
        if new_state == 'call_later':
            if not dialogue.inactive_alerts:
                db.add(InactiveNotificationQueue(dialogue_id=dialogue.id, status='pending'))
                log.info("Диалог переведен в состояние 'call_later', добавлена запись в очередь молчунов")
            else:
                log.debug("Диалог уже находится в очереди молчунов")
        # --------------------------------------------------

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

            
            log.info("Кандидат квалифицирован 🟢(Qualified), перемещение в папку 'Собеседование'")

            api_move_start = time.monotonic()
            await hh_api.move_response_to_folder(recruiter, db, dialogue.hh_response_id, 'interview')
            log.debug("Запрос на перемещение в папку 'interview' выполнен", extra={"duration_sec": round(time.monotonic() - api_move_start, 2)})

            # --- ДОБАВИТЬ ЭТОТ БЛОК КОДА ---
            if new_state == 'interview_scheduled_spb':
                interview_date = extracted_data.get("interview_date")
                interview_time = extracted_data.get("interview_time")

                if interview_date and interview_time:
                    log.info("Собеседование запланировано, напоминания созданы", extra={
                        "interview_date": interview_date,
                        "interview_time": interview_time
                    })
                    await interview_reminder_manager.schedule_interview_reminders(
                        dialogue_id=dialogue.id,
                        interview_date_str=interview_date,
                        interview_time_str=interview_time,
                        db_session=db
                    )
                else:
                    log.error(
                        f"LLM установил 'interview_scheduled_spb', но не предоставил "
                        f"interview_date или interview_time для диалога. Напоминания не будут запланированы."
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
                        skip_instructions=True,
                        log_context=log
                    )

                    # === УСПЕШНЫЙ ВЫЗОВ ===
                    total_attempts = len(clarification_attempts)
                    failed_attempts = total_attempts - 1
                    if failed_attempts > 0:
                        log.warning("Повторные попытки при уточнении отказа от вакансии", extra={"retries": failed_attempts})
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
                    log.warning("Ошибка при попытке уточнить отказ от вакансии, используем отказ по умолчанию", extra={"error": str(e)})
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
                        log.error("Ошибка парсинга JSON в ответе уточнения отказа", extra={"error": str(e)})

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
                    log.info("Отказ от вакансии не подтвержден нейросетью, продолжаем диалог")
                    return  # ← ВАЖНО: выходим из функции, НЕ переводя в статус 'rejected'
                else:
                    log.info("Отказ от вакансии подтвержден нейросетью")
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
                log.info("Кандидат отказался от собеседования, запланированные напоминания отменены")
            # -------------------------------------------------------

            




            # --- ИСПРАВЛЕННЫЙ БЛОК КОДА ---
            # Проверяем, существует ли уже запись RejectedNotificationQueue для этого диалога.
            # `dialogue.rejected_alerts` будет либо объектом RejectedNotificationQueue, либо None,
            # благодаря `uselist=False` в relationship.
            if dialogue.inactive_alerts:
                log.debug("Кандидат уже в списке молчунов, пропуск очереди отказников")
            else:
                if dialogue.rejected_alerts:
                    # Запись уже существует. Если статус не 'pending', обновляем его.
                    if dialogue.rejected_alerts.status != 'pending':
                        log.debug("Уведомление об отказе уже существует, статус обновлен на 'pending'")
                        dialogue.rejected_alerts.status = 'pending'
                        dialogue.rejected_alerts.processed_at = None # Сбросим время обработки при переходе в pending
                        db.add(dialogue.rejected_alerts) # Добавляем для сохранения изменений
                    else:
                        log.debug(
                            f"Уведомление об отклоненном кандидате для диалога {dialogue.hh_response_id} "
                            f"уже существует в RejectedNotificationQueue со статусом 'pending'.")
                else:
                    # Записи не существует, создаем новую
                    new_rejected_alert = RejectedNotificationQueue(
                        dialogue_id=dialogue.id,
                        status='pending'
                    )
                    db.add(new_rejected_alert)
                    log.info("Добавлена запись в очередь уведомлений об отказе (RejectedNotificationQueue)")
            # --- КОНЕЦ ИСПРАВЛЕННОГО БЛОКА КОДА ---



            
            log.info("Кандидат отклонен 🔴 (Rejected), перемещение в папку 'Отказ'")

            api_move_start = time.monotonic()
            await hh_api.move_response_to_folder(recruiter, db, dialogue.hh_response_id, 'assessment')
            log.debug("Запрос на перемещение в папку 'assessment' выполнен", extra={ # <-- ЗАМЕНИ logger на log
                "duration_sec": round(time.monotonic() - api_move_start, 2)
            })

        # Если LLM не вернула текст, не отправляем сообщение
        if bot_response_text is None or bot_response_text == "":
            log.info("Нейросеть вернула пустой текст, обновление стейта без отправки сообщения")

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

            log.info("Сообщение от бота успешно отправлено кандидату в HH")
        elif message_sent == 403:
            log.warning("Не удалось отправить сообщение: вакансия закрыта или резюме удалено (403)")
            dialogue.pending_messages = None
            await db.commit() # Сохраняем сброс очереди сообщений
            return
        else:
            log.error("Ошибка при отправке сообщения в HH API", extra={"status_code": message_sent})
            await db.rollback()
            return

    except Exception as e:
        log.critical("Критический сбой при обработке диалога", exc_info=True)
        if db and db.is_active:
            await db.rollback()
        raise  # Важно: пробрасываем исключение дальше

    finally:
        log.debug("Обработка диалога полностью завершена", extra={
            "total_processing_time": round(time.monotonic() - dialogue_processing_start_time, 2)
        })


async def process_any_pending_dialogues(prompt_library: dict):
    """
    Ищет любые диалоги с pending_messages и обрабатывает их.
    Использует семафор для ограничения нагрузки на LLM.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DIALOGUES)

    async with SessionLocal() as db:
        # 1. Ищем ID диалогов, где есть сообщения и которые пора обрабатывать
        # Добавляем SKIP LOCKED, чтобы другие воркеры не трогали эти строки
        now = datetime.datetime.now(datetime.timezone.utc)
        debounce_time = now - datetime.timedelta(seconds=DEBOUNCE_DELAY_SECONDS)

        # Запрос на получение пачки задач
        query = (
            select(Dialogue.id, Dialogue.recruiter_id)
            .filter(
                Dialogue.pending_messages.is_not(None),
                Dialogue.last_updated <= debounce_time,
                # Проверка, что массив не пустой (как в монолите)
                case(
                    (func.jsonb_typeof(Dialogue.pending_messages) == 'array',
                     func.jsonb_array_length(Dialogue.pending_messages) > 0),
                    else_=False
                )
            )
            .limit(MAX_CONCURRENT_DIALOGUES)
            .with_for_update(skip_locked=True) # КРИТИЧЕСКИ ВАЖНО для масштабирования
        )

        result = await db.execute(query)
        tasks_to_do = result.all()

        if not tasks_to_do:
            return 0

        logger.info("Захвачена пачка диалогов для обработки", extra={
            "batch_size": len(tasks_to_do),
            "dialogue_ids": [task.id for task in tasks_to_do]
        })

        async def run_task(d_id, r_id):
            async with semaphore:
                # Каждый диалог обрабатывается в своей сессии (как в монолите)
                async with SessionLocal() as task_db:
                    try:
                        await _process_single_dialogue(d_id, r_id, prompt_library, task_db)
                    except Exception as e:
                        logger.error("Сбой при обработке диалога внутри задачи", extra={
                            "dialogue_id": d_id,
                            "recruiter_id": r_id,
                            "error": str(e)
                        }, exc_info=True)

        # Запускаем обработку пачки параллельно
        await asyncio.gather(*(run_task(tid, rid) for tid, rid in tasks_to_do))
        return len(tasks_to_do)
    


async def run_processor_cycle():
    logger.info("Главный цикл воркера-процессора запущен")
    prompt_library = knowledge_base.get_prompt_library()
    
    while not shutdown_requested:
        start_time = time.monotonic()
        
        # Обрабатываем диалоги
        processed_count = await process_any_pending_dialogues(prompt_library)
        if processed_count > 0:
            logger.debug("Итерация цикла обработки завершена", extra={
                "processed_count": processed_count,
                "iteration_duration_sec": round(time.monotonic() - start_time, 2)
            })
        # Если работы было много, не спим, сразу проверяем еще раз
        # Если работы не было — спим 1-2 секунды
        if processed_count == 0:
            await asyncio.sleep(2)
        else:
            # Небольшая пауза, чтобы не заспамить БД
            await asyncio.sleep(0.5)

async def main():
    # Настройка сигналов остановки (как в поллере)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await run_processor_cycle()
    finally:
        await cleanup() # Очистка ресурсов LLM
        await hh_api.close_api_client()
        logger.info("Процессор полностью остановлен.")

if __name__ == "__main__":
    setup_logging(log_filename="processor.log")
    load_dotenv()
    asyncio.run(main())