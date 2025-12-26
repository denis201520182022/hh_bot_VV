import logging
import sys
import os
# Импортируем наш JSON-форматтер из основного конфига
from .logger_config import CustomJsonFormatter

def setup_api_logger():
    """Настраивает JSON-логгер для записи сырых API-запросов и ответов в stdout."""
    
    # Имя логгера оставляем RawAPI, чтобы в Grafana фильтровать по полю logger_name
    api_logger = logging.getLogger('RawAPI')
    api_logger.setLevel(logging.DEBUG)
    
    # Отключаем передачу логов корневому логгеру, чтобы не было дублей в консоли
    api_logger.propagate = False

    if api_logger.hasHandlers():
        api_logger.handlers.clear()

    # В Docker пишем только в консоль (Loki это заберет)
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Используем наш JSON-форматтер
    formatter = CustomJsonFormatter('%(timestamp)s %(level)s %(name)s %(message)s')
    console_handler.setFormatter(formatter)
    
    api_logger.addHandler(console_handler)
    
    return api_logger