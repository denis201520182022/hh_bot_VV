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
    Отправляет системное уведомление пользователям Telegram-бота.

    Параметры:
    - message_text: Текст сообщения.
    - alert_type: 
        - "admin_only": (По умолчанию) Ошибки LLM, обновление токенов, сбои API. 
          Отправляется ТОЛЬКО пользователям с ролью 'admin'.
        - "balance": Сообщения о критическом остатке средств (порог 500р). 
          Отправляется ВСЕМ (админам и обычным юзерам).
        - "all": Критические анонсы или техработы. 
          Отправляется ВСЕМ.
    """
    
    # Создаем объект бота внутри функции, чтобы всегда использовать актуальный токен
    bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
    
    async with SessionLocal() as db:
        try:
            # СТРОГАЯ ЛОГИКА ФИЛЬТРАЦИИ:
            # Только если тип сообщения явно разрешен для всех, выбираем всех.
            # В любом другом случае (включая опечатки) — только админы.
            if alert_type in ["balance", "all"]:
                stmt = select(TelegramUser)
                logger.debug(f"Рассылка алерта '{alert_type}' всем пользователям.")
            else:
                stmt = select(TelegramUser).where(TelegramUser.role == 'admin')
                logger.debug(f"Рассылка алерта '{alert_type}' только администраторам.")
                
            result = await db.execute(stmt)
            users_to_notify = result.scalars().all()
            
            if not users_to_notify:
                logger.warning(f"Список получателей для алерта '{alert_type}' пуст.")
                return

            # Отправка сообщений
            for user in users_to_notify:
                try:
                    await bot.send_message(chat_id=user.telegram_id, text=message_text)
                except Exception as e:
                    logger.warning(f"Не удалось доставить сообщение пользователю {user.telegram_id} ({user.username}): {e}")
                    continue 

        except Exception as e:
            logger.error(f"Критическая ошибка в функции send_system_alert: {e}", exc_info=True)
        finally:
            # Обязательно закрываем сессию бота, чтобы не было утечек памяти
            if bot.session:
                await bot.session.close()