# hr_bot/utils/system_notifier.py
import os
import asyncio
from aiogram import Bot
# --- ИЗМЕНЕНИЕ: Добавлен импорт AsyncSession и select ---
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
# --- КОНЕЦ ИЗМЕНЕНИЯ ---
from hr_bot.db.models import SessionLocal, TelegramUser

async def send_system_alert(message_text: str):
    bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
    
    # ИЗМЕНЕНИЕ: Использование асинхронного контекстного менеджера для сессии БД
    async with SessionLocal() as db: # Было: db = SessionLocal()
        try:
            # Отправляем всем: и админам, и пользователям
            # ИЗМЕНЕНИЕ: Асинхронный запрос к БД
            result = await db.execute(select(TelegramUser))
            all_users = result.scalars().all() # ИЗМЕНЕНИЕ: Получение скалярных результатов
            
            for user in all_users:
                try:
                    await bot.send_message(chat_id=user.telegram_id, text=message_text)
                except Exception as e:
                    # Логируем ошибку, чтобы знать, кому не удалось отправить
                    logging.error(f"Не удалось отправить системное оповещение пользователю {user.telegram_id}: {e}")
                    pass # Игнорируем ошибки, если не удалось доставить кому-то одному
        except Exception as e:
            logging.critical(f"Критическая ошибка при попытке отправить системное оповещение: {e}", exc_info=True)
        finally:
            # ИЗМЕНЕНИЕ: db.close() не нужен, так как async with закрывает сессию автоматически
            # bot.session.close() остается, так как это относится к aiogram
            await bot.session.close() # Этот await уже был, это правильно для aiogram сессии.
            # db.close() # Удален