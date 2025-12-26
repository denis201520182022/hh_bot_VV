import os
import logging
import datetime
import asyncio
import json
from dotenv import load_dotenv
# --- ИЗМЕНЕНИЕ: Замена Session на AsyncSession ---
from sqlalchemy.ext.asyncio import AsyncSession
# --- КОНЕЦ ИЗМЕНЕНИЯ ---
from hr_bot.db.models import TrackedRecruiter # TrackedRecruiter - это модель, не сессия, здесь без изменений
from hr_bot.utils.api_logger import setup_api_logger
from hr_bot.utils.system_notifier import send_system_alert
import httpx
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from aiolimiter import AsyncLimiter
load_dotenv()
logger = logging.getLogger(__name__)
HH_API_PER_PAGE_LIMIT = 20

api_raw_logger = setup_api_logger()

CLIENT_ID = os.getenv('HH_CLIENT_ID')
CLIENT_SECRET = os.getenv('HH_CLIENT_SECRET')

MAX_CONCURRENT_REQUESTS = 80
API_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

HH_API_RATE_LIMITER = AsyncLimiter(100, 1) # 2 запроса в секунду

# Создаем глобальный клиент с пулом соединений
# Создаем глобальный клиент БЕЗ ПРОКСИ, но с пулом соединений (для скорости)
shared_api_client = httpx.AsyncClient(
    timeout=60.0,
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=100)
)

# Где-то в глобальной области или в классе, управляющем рекрутерами
_refresh_locks = {} # Словарь для хранения локов по ID рекрутера

# ИЗМЕНЕНИЕ: Тип db изменен на AsyncSession
async def get_access_token(recruiter: TrackedRecruiter, db: AsyncSession) -> str | None: 
    now = datetime.datetime.now(datetime.timezone.utc)

    # 1. Быстрая проверка токена (без блокировок — «оптимистичный» путь)
    if recruiter.access_token and recruiter.token_expires_at and recruiter.token_expires_at > now:
        return recruiter.access_token

    # 2. Получаем или создаем локальный лок (защита от потоков внутри ОДНОГО процесса)
    lock = _refresh_locks.setdefault(recruiter.recruiter_id, asyncio.Lock())

    async with lock:
        # --- КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: Блокировка строки в БД (защита от ДРУГИХ процессов/воркеров) ---
        # Мы запрашиваем строку рекрутера с блокировкой FOR UPDATE.
        # Если другой воркер уже обновляет этот токен, наш текущий воркер ПРИОСТАНОВИТСЯ на этой строке.
        try:
            stmt = select(TrackedRecruiter).filter_by(id=recruiter.id).with_for_update()
            result = await db.execute(stmt)
            db_recruiter = result.scalar_one_or_none()

            if not db_recruiter:
                logger.error(f"Рекрутер с ID {recruiter.id} не найден в БД при блокировке.")
                return None

            # 4. Повторная проверка токена ПОСЛЕ получения всех блокировок.
            # Вдруг пока мы ждали очереди, другой воркер (процесс) уже обновил токен в БД.
            if db_recruiter.access_token and db_recruiter.token_expires_at and db_recruiter.token_expires_at > now:
                # Синхронизируем наш текущий объект с данными из БД и возвращаем свежий токен
                recruiter.access_token = db_recruiter.access_token
                recruiter.refresh_token = db_recruiter.refresh_token
                recruiter.token_expires_at = db_recruiter.token_expires_at
                return recruiter.access_token

            # Если мы дошли сюда, значит токен в БД все еще просрочен, и мы — ПЕРВЫЙ процесс, 
            # который получил право на обновление.
            
            logger.info("Обновление access_token рекрутера", extra={
                "recruiter_id": db_recruiter.id,
                "recruiter_name": db_recruiter.name
            })
            epp = f"Токен для рекрутера {db_recruiter.name} истек или отсутствует. Обновляю..."
            await send_system_alert(epp, alert_type="admin_only")

            if not db_recruiter.refresh_token:
                logger.error(f"У рекрутера {db_recruiter.name} (ID: {db_recruiter.recruiter_id}) нет refresh_token!")
                error_message = (
                    f"🔴 КРИТИЧЕСКАЯ ОШИБКА АВТОРИЗАЦИИ\n\n"
                    f"Бот остановлен для рекрутера: {db_recruiter.name}\n\n"
                    f"Причина: Отсутствует refresh_token в базе данных.\n"
                    f"Действие: Требуется провести повторную авторизацию."
                )
                await send_system_alert(error_message, alert_type="admin_only")
                return None

            url = "https://api.hh.ru/token"
            data = {
                "grant_type": "refresh_token",
                "refresh_token": db_recruiter.refresh_token,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }

            request_log = (
                f"🚨🚨🚨TOKEN EXCHANGE REQUEST -->\n  Method: POST\n  URL: {url}\n"
                f"  Data: {data}"
            )
            api_raw_logger.info(request_log)

            async with HH_API_RATE_LIMITER:
                async with API_SEMAPHORE:
                    response = await shared_api_client.post(url, data=data)

            response_log = (
                f"<--🚨🚨🚨 TOKEN EXCHANGE RESPONSE\n  Status Code: {response.status_code}\n"
                f"  Headers: {response.headers}\n  Body: {response.text}"
            )

            if response.status_code != 200:
                api_raw_logger.warning(response_log)
                await send_system_alert(f"🔴 ВНИМАНИЕ: Неуспешная попытка обновления токена для {db_recruiter.name}", alert_type="admin_only")
            else:
                api_raw_logger.info(response_log)
                await send_system_alert(f"✅ Успешно получен токен для {db_recruiter.name}", alert_type="admin_only")

            if response.status_code == 200:
                tokens = response.json()
                # Обновляем заблокированный в БД объект
                db_recruiter.access_token = tokens["access_token"]
                if "refresh_token" in tokens:
                    db_recruiter.refresh_token = tokens["refresh_token"]
                db_recruiter.token_expires_at = now + datetime.timedelta(seconds=tokens["expires_in"] - 300)
                
                # Фиксируем изменения в БД и СНИМАЕМ блокировку строки
                db.add(db_recruiter) 
                await db.commit() 
                
                # Синхронизируем локальный объект recruiter (чтобы вызывающий код увидел изменения)
                recruiter.access_token = db_recruiter.access_token
                recruiter.refresh_token = db_recruiter.refresh_token
                recruiter.token_expires_at = db_recruiter.token_expires_at

                logger.info(f"Успешно получен новый access_token для рекрутера {recruiter.name}.")
                await send_system_alert(f"✅ Успешно сохранен новый токен для {recruiter.name}.", alert_type="admin_only")
                return recruiter.access_token
            else:
                # Обработка ошибок (оригинальная логика сохранена)
                try:
                    error_data = response.json()
                    error_description = error_data.get("error_description")
                    oauth_error = error_data.get("oauth_error")

                    if error_description == "token not expired":
                        logger.info(f"Попытка обновить токен для {db_recruiter.name} отклонена: токен еще не истек. Возвращаем старый.")
                        db_recruiter.token_expires_at = now + datetime.timedelta(minutes=5)
                        db.add(db_recruiter)
                        await db.commit()
                        return db_recruiter.access_token
                    
                    elif error_description in ["password invalidated", "token deactivated"] or oauth_error == "token-revoked":
                        logger.critical(f"Критическая ошибка авторизации для {db_recruiter.name}: {response.text}")
                        error_message = (
                            f"🔴 КРИТИЧЕСКАЯ ОШИБКА АВТОРИЗАЦИИ\n\n"
                            f"Бот остановлен для рекрутера: {db_recruiter.name}\n\n"
                            f"Причина от HH.ru: {response.text}\n\n"
                            f"Действие: Требуется провести повторную авторизацию (восстановить пароль)."
                        )
                        await send_system_alert(error_message, alert_type="admin_only")
                        return None
                    else:
                        logger.critical(f"Ошибка обновления токена для {db_recruiter.name}: {response.text}")
                        await send_system_alert(f"🔴 КРИТИЧЕСКАЯ ОШИБКА: {response.text}", alert_type="admin_only")
                        return None

                except json.JSONDecodeError:
                    logger.critical(f"Критическая ошибка API (не JSON) для {db_recruiter.name}: {response.text}")
                    return None
                except Exception as e:
                    logger.critical(f"Неизвестная ошибка для {db_recruiter.name}: {e}")
                    return None
        except Exception as e:
            logger.error(f"Ошибка в блоке транзакции get_access_token: {e}")
            await db.rollback()
            return None
                
@retry(
    stop=stop_after_attempt(3),  # Пытаемся 3 раза (1 оригинал + 2 повтора)
    wait=wait_fixed(5),          # Ждем 5 секунд между попытками
    retry=retry_if_exception_type((httpx.ConnectTimeout, httpx.ReadTimeout)), # Повторяем только при сетевых ошибках
    reraise=True # Если все попытки провалятся, последняя ошибка будет выброшена дальше
)
# ИЗМЕНЕНИЕ: Тип db изменен на AsyncSession
async def _make_request( 
    recruiter: TrackedRecruiter,
    db: AsyncSession,
    method: str,
    endpoint: str,
    full_url: str = None,
    # Параметр add_user_agent полностью удален
    **kwargs,
):
    
    """Асинхронный универсальный запрос с ограничением по конкурентности."""
    token = await get_access_token(recruiter, db)
    if not token:
        raise ConnectionError(f"Нет валидного токена для {recruiter.name}.")

    url = full_url or f"https://api.hh.ru/{endpoint}"
    headers = kwargs.pop('headers', {})
    headers["Authorization"] = f"Bearer {token}"

    headers["HH-User-Agent"] = "ZaBota-Bot/1.0 (hbfys@mail.com)"

    request_log = (
        f"REQUEST -->\n  Method: {method}\n  URL: {url}\n  Headers: {headers}\n"
        f"  Params: {kwargs.get('params')}\n  Data: {kwargs.get('data')}\n  JSON: {kwargs.get('json')}"
    )
    

    async with HH_API_RATE_LIMITER:
        async with API_SEMAPHORE:
            # ИСПОЛЬЗУЕМ ГЛОБАЛЬНЫЙ КЛИЕНТ
            response = await shared_api_client.request(method, url, headers=headers, **kwargs)

    if 400 <= response.status_code:
        response_log = (
            f"<-- RESPONSE (ERROR)\n  Status Code: {response.status_code}\n"
            f"  Headers: {response.headers}\n  Body: {response.text}"
        )
        api_raw_logger.warning(f"{request_log}\n{response_log}")

        if response.status_code == 403:
            should_refresh_token = False
            try:
                error_data = response.json()
                oauth_error = error_data.get("oauth_error")

                if oauth_error in ["token-revoked", "token-expired"]:
                    logger.warning(f"Получен 403: '{oauth_error}' для {recruiter.name}. Инициирую обновление токена.")
                    should_refresh_token = True
                else:
                    logger.warning(
                        f"Получен 403 для {recruiter.name} с ошибкой '{oauth_error or error_data.get('description', 'Unknown 403')}'."
                        f" Токен не требует обновления. Пробрасываю ошибку."
                    )
            except json.JSONDecodeError:
                logger.warning(
                    f"Получен 403 для {recruiter.name}, тело не JSON (возможно DDoS). "
                    f"Пауза 30 сек..."
                )
                await asyncio.sleep(30) # Даем паузу, чтобы не спамить в лежащий сервер
                should_refresh_token = False
            except Exception as e:
                logger.error(f"Ошибка при обработке 403 ответа для {recruiter.name}: {e}. Пытаюсь обновить токен на всякий случай.")
                should_refresh_token = False

            if should_refresh_token:
                recruiter.access_token = None
                # ИЗМЕНЕНИЕ: await db.commit() вместо await asyncio.to_thread(db.commit)
                await db.commit() 
                
                token = await get_access_token(recruiter, db)
                if not token:
                    raise ConnectionError(f"Не удалось повторно получить токен для {recruiter.name}")
                
                headers["Authorization"] = f"Bearer {token}"
                headers["HH-User-Agent"] = "ZaBota-Bot/1.0 (hbfys@mail.com)"
                async with HH_API_RATE_LIMITER:
                    async with API_SEMAPHORE:
                        async with httpx.AsyncClient(timeout=60) as client:
                            # Повторный запрос после обновления токена
                            response = await client.request(method, url, headers=headers, **kwargs)
                # Если повторный запрос также вернул 4xx/5xx, он будет пойман в следующем if-блоке
                # или вызовет raise_for_status()

    if response.status_code in [201, 204]:
        return None
        
    response.raise_for_status()
    return response.json() if response.content else None


# hr_bot/services/hh_api_real.py
# Нужно убедиться, что AsyncSession импортирован в начале файла hh_api_real.py:
# from sqlalchemy.ext.asyncio import AsyncSession

async def get_responses_from_folder(
    recruiter: TrackedRecruiter,
    db: AsyncSession, # ИЗМЕНЕНИЕ: Тип db изменен на AsyncSession
    folder_id: str,
    vacancy_ids: list,
    since_datetime: datetime.datetime = None,
    check_for_updates: bool = False
) -> list:
    """
    Асинхронно получает список откликов из указанной папки,
    делая ОТДЕЛЬНЫЙ запрос для КАЖДОЙ вакансии и "помечая" каждый отклик
    ID его вакансии, обрабатывая ВСЕ страницы или до since_datetime.
    Если check_for_updates=True, запрашивает только отклики с обновлениями.
    """
    logger.debug(
        f"REAL_API: Запрос откликов из папки '{folder_id}' для {len(vacancy_ids)} вакансий"
        f"{(f' с датой от {since_datetime.isoformat()}' if since_datetime else '')}"
        f"{(', только с обновлениями' if check_for_updates else '')}..."
    )

    tasks = []

    for vacancy_id in vacancy_ids:
        if not vacancy_id:
            continue

        async def fetch_for_vacancy(vid):
            all_items_for_vacancy = []
            page = 0
            
            stop_fetching_for_this_vacancy = False
            if since_datetime:
                logger.debug(f"  [DEBUG] Для вакансии {vid}, since_datetime (UTC): {since_datetime.isoformat()}")

            try:
                while True:
                    params = {
                        "vacancy_id": str(vid),
                        "page": str(page),
                        "per_page": str(HH_API_PER_PAGE_LIMIT),
                        "order_by": "created_at",
                        "order": "desc"
                    }
                    
                    # --- ИСПРАВЛЕНИЕ: ИСПОЛЬЗУЕМ КОРРЕКТНЫЕ ПАРАМЕТРЫ ФИЛЬТРАЦИИ ОБНОВЛЕНИЙ ---
                    if check_for_updates:
                        if folder_id == 'response':
                            params["show_only_new_responses"] = "true"
                        else:
                            params["show_only_new"] = "true"
                    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

                    # db здесь передается в _make_request, которое уже обновлено
                    response_data = await _make_request(
                        recruiter, db, "GET", f"negotiations/{folder_id}", params=params
                    )

                    if not response_data or not response_data.get("items"):
                        logger.debug(f"  [DEBUG] Для вакансии {vid}, страница {page}: Нет данных или items пусты. Завершаю пагинацию.")
                        break

                    new_items_to_add = []

                    for item_index, item in enumerate(response_data["items"]):
                        if since_datetime:
                            item_created_at_str = item.get("created_at")
                            if item_created_at_str:
                                try:
                                    item_created_at = datetime.datetime.fromisoformat(item_created_at_str)
                                    if item_created_at.tzinfo is None:
                                        item_created_at = item_created_at.replace(tzinfo=datetime.timezone.utc)
                                    else:
                                        item_created_at = item_created_at.astimezone(datetime.timezone.utc)

                                    logger.debug(
                                        f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')} ({item_index}):\n"
                                        f"    'created_at' (API string): {item_created_at_str}\n"
                                        f"    Parsed 'created_at' (UTC): {item_created_at.isoformat()}\n"
                                        f"    Сравнение: {item_created_at.isoformat()} < {since_datetime.isoformat()} = {item_created_at < since_datetime}"
                                    )
                                    
                                    if item_created_at < since_datetime:
                                        logger.debug(f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')}: ОТБРОШЕН, СТАРЕЕ since_datetime. Активирую раннюю остановку.")
                                        stop_fetching_for_this_vacancy = True 
                                        break 
                                    else: 
                                        new_items_to_add.append(item)
                                        logger.debug(f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')}: ДОБАВЛЕН в new_items_to_add (прошел проверку даты).")
                                except ValueError:
                                    logger.warning(f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')}: Не удалось распарсить 'created_at' ({item_created_at_str}). ДОБАВЛЕН (временное включение).")
                                    new_items_to_add.append(item)
                            else:
                                logger.warning(f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')}: Отсутствует поле 'created_at'. ДОБАВЛЕН (временное включение).")
                                new_items_to_add.append(item)
                        else:
                            new_items_to_add.append(item)
                            logger.debug(f"  [DEBUG] Вакансия {vid}, стр {page}, отклик {item.get('id')}: ДОБАВЛЕН в new_items_to_add (без фильтра по дате since_datetime).")

                    logger.debug(f"  [DEBUG] Для вакансии {vid}, страница {page}: new_items_to_add содержит {len(new_items_to_add)} элементов.")
                    all_items_for_vacancy.extend(new_items_to_add)
                    logger.debug(f"  [DEBUG] Для вакансии {vid}: all_items_for_vacancy теперь содержит {len(all_items_for_vacancy)} элементов (после страницы {page}).")

                    if stop_fetching_for_this_vacancy: 
                         logger.debug(f"  [DEBUG] Вакансия {vid}: Флаг ранней остановки активен. Завершаю пагинацию для этой вакансии.")
                         break

                    if page >= response_data.get("pages", 1) - 1:
                        logger.debug(f"  [DEBUG] Для вакансии {vid}, страница {page}: Достигнута последняя страница. Завершаю пагинацию.")
                        break

                    page += 1

                if since_datetime:
                    final_filtered_items = []
                    for item in all_items_for_vacancy:
                        item_created_at_str = item.get("created_at")
                        if item_created_at_str:
                            try:
                                item_created_at = datetime.datetime.fromisoformat(item_created_at_str)
                                if item_created_at.tzinfo is None:
                                    item_created_at = item_created_at.replace(tzinfo=datetime.timezone.utc)
                                else:
                                    item_created_at = item_created_at.astimezone(datetime.timezone.utc)

                                if item_created_at >= since_datetime:
                                    final_filtered_items.append(item)
                                else:
                                    logger.debug(f"  [DEBUG] Вакансия {vid}, отклик {item.get('id')}: Отброшен на финальной фильтрации (старее since_datetime).")
                            except ValueError:
                                logger.warning(f"  [DEBUG] Вакансия {vid}, отклик {item.get('id')}: Не удалось распарсить 'created_at' при финальной фильтрации. Включен.")
                                final_filtered_items.append(item)
                        else:
                            logger.warning(f"  [DEBUG] Вакансия {vid}, отклик {item.get('id')}: Отсутствует 'created_at' при финальной фильтрации. Включен.")
                            final_filtered_items.append(item)
                    return [(item, str(vid)) for item in final_filtered_items]
                else:
                    return [(item, str(vid)) for item in all_items_for_vacancy]

            except Exception as e:
                logger.error(
                    f"Ошибка при запросе откликов для вакансии {vid} в папке '{folder_id}'"
                    f"{(', только с обновлениями' if check_for_updates else '')} (страница {page})",
                    exc_info=True
                )
                return []

        tasks.append(fetch_for_vacancy(vacancy_id))

    results_from_all_vacancies = await asyncio.gather(*tasks)

    all_responses_with_vacancy_id = []
    for single_vacancy_responses in results_from_all_vacancies:
        all_responses_with_vacancy_id.extend(single_vacancy_responses)

    logger.debug(f"Суммарно найдено {len(all_responses_with_vacancy_id)} откликов в папке '{folder_id}'"
                 f"{(', только с обновлениями' if check_for_updates else '')}.")
    return all_responses_with_vacancy_id

# ИЗМЕНЕНИЕ: Тип db изменен на AsyncSession
async def get_messages(recruiter: TrackedRecruiter, db: AsyncSession, messages_url: str) -> list:
    """Асинхронно получает ПОЛНУЮ историю сообщений постранично."""
    logger.info(f"REAL_API: Запрос ВСЕХ сообщений по {messages_url}...")
    all_messages, page = [], 0

    while True:
        try:
            params = {"page": page, "per_page": str(HH_API_PER_PAGE_LIMIT)}
            # db здесь передается в _make_request, которое уже обновлено для AsyncSession
            response_data = await _make_request(recruiter, db, "GET", "", full_url=messages_url, params=params)

            if not response_data or not response_data.get("items"):
                break

            all_messages.extend(response_data["items"])

            if page >= response_data.get("pages", 1) - 1:
                break
            page += 1
        except Exception as e:
            logger.error(f"Ошибка при получении страницы {page} сообщений: {e}")
            break

    all_messages.sort(key=lambda x: x.get("created_at", ""))
    return all_messages

# hr_bot/services/hh_api_real.py

# ... (остальной код выше без изменений)

# НАЙДИТЕ ЭТУ ФУНКЦИЮ И ЗАМЕНИТЕ ЕЁ ЦЕЛИКОМ:
async def send_message(recruiter: TrackedRecruiter, db: AsyncSession, negotiation_id: str, message_text: str) -> int | bool:
    """Асинхронно отправляет сообщение в чат отклика с обработкой закрытых вакансий."""
    logger.info(f"REAL_API: Отправка сообщения в диалог {negotiation_id} от {recruiter.name}...")
    try:
        # db здесь передается в _make_request, которое уже обновлено для AsyncSession
        await _make_request(
            recruiter,
            db,
            "POST",
            f"negotiations/{negotiation_id}/messages",
            data={"message": message_text},
        )
        return 200

    except httpx.HTTPStatusError as e:
        # --- ОБНОВЛЕННАЯ ОБРАБОТКА ---
        if e.response.status_code == 403:
            try:
                error_body = e.response.json()
                errors_list = error_body.get("errors", [])
                
                # Проверяем на invalid_vacancy (вакансия закрыта) И resume_not_found (резюме удалено)
                # Если любая из этих ошибок - возвращаем 403, чтобы воркер остановил диалог
                fatal_errors = ["invalid_vacancy", "resume_not_found"]
                
                if any(err.get("value") in fatal_errors for err in errors_list):
                    logger.warning(
                        f"⚠️ Сообщение не отправлено в диалог {negotiation_id}. "
                        f"Причина: Вакансия закрыта или резюме удалено ({error_body}). "
                        f"Диалог будет остановлен."
                    )
                    return 403 # Возвращаем 403, чтобы воркер перевел диалог в статус closed/archive
            except Exception:
                pass # Если не удалось распарсить JSON, идем к стандартной обработке ниже
        # --------------------------------------------------------------

        logger.error(f"Не удалось отправить сообщение в диалог {negotiation_id}: {e}", exc_info=True)
        return False

    except Exception as e:
        logger.error(f"Не удалось отправить сообщение в диалог {negotiation_id}: {e}", exc_info=True)
        return False

# ... (остальной код ниже без изменений)


# hr_bot/services/hh_api_real.py

# ИЗМЕНЕНИЕ: Тип db изменен на AsyncSession
async def move_response_to_folder(recruiter: TrackedRecruiter, db: AsyncSession, negotiation_id: str, folder_id: str):
    """Асинхронно перемещает отклик в указанную папку, используя правильный PUT-запрос."""
    logger.info(f"REAL_API: Перемещение отклика {negotiation_id} в папку '{folder_id}'...")
    try:
        endpoint = f"negotiations/{folder_id}/{negotiation_id}"
        # db здесь передается в _make_request, которое уже обновлено для AsyncSession
        await _make_request(recruiter, db, "PUT", endpoint)
        
        logger.info(f"УСПЕХ: Отклик {negotiation_id} был успешно перемещен в папку '{folder_id}'.")

    except httpx.HTTPStatusError as e:
        # --- ДОБАВЛЕННАЯ ОБРАБОТКА ОШИБОК ---
        if e.response.status_code == 403:
            try:
                error_body = e.response.json()
                errors_list = error_body.get("errors", [])
                
                # Проверяем на ошибки "резюме не найдено" или "вакансия закрыта/архивная"
                # Если это они, то перемещать больше некуда, просто логируем и выходим, не ломая воркер
                fatal_errors = ["resume_not_found", "invalid_vacancy"]
                
                if any(err.get("value") in fatal_errors for err in errors_list):
                    logger.warning(
                        f"⚠️ Не удалось переместить отклик {negotiation_id} в папку {folder_id}. "
                        f"Причина: {error_body}. Игнорируем ошибку."
                    )
                    return # Выходим успешно, считаем что действие "выполнено" (или невозможно выполнить)
            except Exception:
                pass # Если JSON не парсится, пробрасываем ошибку дальше
        
        # Если ошибка другая - логируем и пробрасываем, чтобы tenacity (если есть) или внешний код её увидел
        logger.error(f"Не удалось переместить отклик {negotiation_id} в папку {folder_id}: {e}", exc_info=True)
        raise e
        # ------------------------------------

    except Exception as e:
        logger.error(f"Не удалось переместить отклик {negotiation_id} в папку {folder_id}: {e}", exc_info=True)
        raise e



async def get_negotiation_current_folder(recruiter: TrackedRecruiter, db: AsyncSession, hh_response_id: str) -> str | None:
    """
    Получает ID текущей папки, в которой находится отклик на HH.ru.
    Возвращает ID папки (например, 'consider', 'response', 'interview') или None в случае ошибки/отсутствия.
    """
    try:
        logger.debug(f"Запрос текущей папки для отклика {hh_response_id} от рекрутера {recruiter.name}...")
        negotiation_data = await _make_request(recruiter, db, "GET", f"negotiations/{hh_response_id}")
        
        # --- КОРРЕКТНОЕ ИСПРАВЛЕНИЕ ЗДЕСЬ ---
        if negotiation_data and negotiation_data.get("employer_state") and negotiation_data["employer_state"].get("id"):
            folder_id = negotiation_data["employer_state"]["id"] # <--- ИЗМЕНЕНИЕ: ИСПОЛЬЗУЕМ "employer_state"
            logger.debug(f"Отклик {hh_response_id} находится в папке '{folder_id}'.")
            return folder_id
        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---
        
        logger.warning(f"Не удалось определить папку для отклика {hh_response_id}. Возможно, отклик удален или данные неполны.")
        return None
    except httpx.HTTPStatusError as http_error:
        if http_error.response.status_code == 404:
            logger.info(f"Отклик {hh_response_id} не найден на HH.ru (404). Считаем, что его нет в 'consider'.")
            return 404
        else:
            logger.error(f"HTTP-ошибка {http_error.response.status_code} при получении папки для отклика {hh_response_id}: {http_error}", exc_info=True)
            return None
    except Exception as e:
        logger.error(f"Ошибка при получении текущей папки для отклика {hh_response_id}: {e}", exc_info=True)
        return None
    
async def close_api_client():
    """Закрывает глобальный клиент при остановке приложения."""
    await shared_api_client.aclose()
    logger.info("🔒 HH API клиент закрыт")