# hr_bot/tg_bot/handlers/user.py

from aiogram import Router, F
from aiogram.types import Message
from sqlalchemy.orm import Session
from aiogram.utils.formatting import Text, Bold

from hr_bot.db.models import AppSettings
from hr_bot.tg_bot.filters import AdminFilter

router = Router()
router.message.filter(~AdminFilter()) # Сработает, только если пользователь НЕ админ

@router.message(F.text == "⚙️ Лимиты")
async def user_limits_status(message: Message, db_session: Session):
    settings = db_session.query(AppSettings).filter_by(id=1).first()
    if not settings:
        await message.answer("❌ Не удалось загрузить настройки лимитов.")
        return
        
    remaining = settings.limit_total - settings.limit_used
    cost = settings.limit_used * settings.cost_per_response
    
    content = Text(
        Bold("📊 Текущий статус:"), "\n\n",
        "Лимит: ", Bold(settings.limit_total), " откликов\n",
        "Использовано: ", Bold(settings.limit_used), " (на сумму: ", Bold(f"{cost:.2f}"), " руб.)\n",
        "Осталось: ", Bold(remaining), "\n\n",
        "Текущий тариф: ", Bold(f"{settings.cost_per_response:.2f}"), " руб. за отклик"
    )
    
    await message.answer(**content.as_kwargs())