import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
from pythonjsonlogger import jsonlogger
from datetime import datetime

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        
        # 1. Твоя логика времени и уровня
        if not log_record.get('timestamp'):
            log_record['timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname

        # 2. НОВАЯ ЛОГИКА: Принудительно добавляем extra поля в JSON
        # Это берет все, что ты передал в extra={...}, и сует в лог
        skip_keys = {
            'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
            'funcName', 'levelname', 'levelno', 'lineno', 'module',
            'msecs', 'message', 'msg', 'name', 'pathname', 'process',
            'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName'
        }
        
        for key, value in record.__dict__.items():
            if key not in log_record and key not in skip_keys and not key.startswith('_'):
                log_record[key] = value

def setup_logging(log_filename: str):
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 3. ДОБАВЛЕНО %(exc_info)s — чтобы видеть трейсбэки ошибок
    json_format = "%(timestamp)s %(level)s %(name)s %(message)s %(exc_info)s"
    formatter = CustomJsonFormatter(json_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Обработчик консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Обработчик файла
    file_handler = TimedRotatingFileHandler(
        os.path.join(log_dir, log_filename),
        when='midnight',
        interval=1,
        backupCount=4,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Тишина в библиотеках
    logging.getLogger('aiogram').setLevel(logging.INFO)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.WARNING)