# hr_bot/utils/logger_config.py

import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys

def setup_logging(log_filename: str):
    """
    Настраивает логирование в консоль и в указанный файл с ежедневной ротацией.

    :param log_filename: Имя файла для сохранения логов (например, 'telegram_bot.log').
    """
    # Создаем папку для логов, если ее нет
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Определяем формат сообщений
    log_format = "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s"
    formatter = logging.Formatter(log_format)

    # Настраиваем корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Предотвращаем дублирование обработчиков, если функция вызовется повторно
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Обработчик для вывода в консоль (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Обработчик для записи в файл с ротацией
    file_handler = TimedRotatingFileHandler(
        os.path.join(log_dir, log_filename),
        when='midnight',
        interval=1,
        backupCount=4,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    # Добавляем оба обработчика к корневому логгеру
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Устанавливаем уровень INFO для "шумных" библиотек, чтобы не засорять логи
    logging.getLogger('aiogram').setLevel(logging.INFO)
    logging.getLogger('httpx').setLevel(logging.WARNING) # Логи httpx могут быть слишком подробными
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.WARNING)