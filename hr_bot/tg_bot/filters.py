from aiogram.filters import BaseFilter
from aiogram.types import Message
from sqlalchemy.orm import Session
from hr_bot.db.models import TelegramUser

class AdminFilter(BaseFilter):
    async def __call__(self, message: Message, db_session: Session) -> bool:
        user = db_session.query(TelegramUser).filter(
            TelegramUser.telegram_id == str(message.from_user.id)
        ).first()
        return user is not None and user.role == 'admin'