import os
import asyncio
import logging
from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from hr_bot.db.models import SessionLocal, TelegramUser

# Инициализируем логгер
logger = logging.getLogger(__name__)

async def send_system_alert(message_text: str, alert_type: str = "admin_only"):
    """
    alert_type: 
    - "admin_only": Технические ошибки, токены, логи (только админам)
    - "balance": Уведомление о пороге 500 руб (всем пользователям)
    - "all": Критические объявления (всем пользователям)
    """
    bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
    
    async with SessionLocal() as db:
        try:
            # ЛОГИКА ФИЛЬТРАЦИИ:
            if alert_type == "admin_only":
                # Только админы
                stmt = select(TelegramUser).where(TelegramUser.role == 'admin')
            else:
                # И админы, и обычные юзеры (для "balance" и "all")
                stmt = select(TelegramUser)
                
            result = await db.execute(stmt)
            users_to_notify = result.scalars().all()
            
            for user in users_to_notify:
                try:
                    await bot.send_message(chat_id=user.telegram_id, text=message_text)
                except Exception as e:
                    logger.warning(f"Не удалось отправить сообщение {user.telegram_id}: {e}")
                    continue 
        except Exception as e:
            logger.error(f"Ошибка в send_system_alert: {e}", exc_info=True)
        finally:
            await bot.session.close()