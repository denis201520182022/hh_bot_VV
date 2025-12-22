from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

# --- ИЗМЕНЕНИЕ: Замена импорта на синхронную фабрику сессий из models.py ---
from hr_bot.db.models import SyncSessionLocal # <--- Импортируем синхронную фабрику
# --- КОНЕЦ ИЗМЕНЕНИЯ ---

# --- ИЗМЕНЕНИЕ: Добавлен импорт Session для type hint ---
from sqlalchemy.orm import Session, sessionmaker # <--- Импортируем sessionmaker

class DbSessionMiddleware(BaseMiddleware):
    # --- ИЗМЕНЕНИЕ: Обновление type hint на сам класс-фабрику sessionmaker ---
    def __init__(self, session_pool: sessionmaker): # <--- Type hint изменен на sessionmaker
        super().__init__()
        self.session_pool = session_pool

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # ... (остальной код без изменений) ...
        with self.session_pool() as session:
            data["db_session"] = session
            return await handler(event, data)