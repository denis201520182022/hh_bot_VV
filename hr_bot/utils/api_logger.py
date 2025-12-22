# hr_bot/utils/api_logger.py
import logging
import os

def setup_api_logger():
    """Настраивает отдельный логгер для записи сырых API-запросов и ответов."""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    api_logger = logging.getLogger('RawAPI')
    api_logger.setLevel(logging.DEBUG)
    api_logger.propagate = False

    if api_logger.hasHandlers():
        api_logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s\n%(message)s\n' + '='*80)
    
    # Будем писать в файл test00.log. `mode='a'` означает 'append' - дописывать в конец.
    file_handler = logging.FileHandler(os.path.join(log_dir, 'test00.log'), mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    api_logger.addHandler(file_handler)
    return api_logger