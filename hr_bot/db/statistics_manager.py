import datetime # ИЗМЕНЕНИЕ: Уточнен импорт datetime, чтобы не полагаться на from datetime import date
# ИЗМЕНЕНИЕ: Замена Session на AsyncSession
from sqlalchemy.ext.asyncio import AsyncSession
# ИЗМЕНЕНИЕ: Добавлен импорт select для асинхронных запросов
from sqlalchemy import select 
from .models import Statistic

# ИЗМЕНЕНИЕ: Функция стала асинхронной и принимает AsyncSession
async def update_stats(db: AsyncSession, vacancy_id: int, responses: int = 0, started_dialogs: int = 0, qualified: int = 0):
    """
    Находит или создает запись о статистике за сегодняшний день для вакансии
    и инкрементирует нужные счетчики.
    """
    today = datetime.date.today() # ИЗМЕНЕНИЕ: Использование datetime.date

    # Ищем запись статистики для этой вакансии за сегодня
    # ИЗМЕНЕНИЕ: Асинхронный запрос к БД (изменен синтаксис)
    result = await db.execute(
        select(Statistic).filter(
            Statistic.vacancy_id == vacancy_id,
            Statistic.date == today
        )
    )
    stats_record = result.scalar_one_or_none() # ИЗМЕНЕНИЕ: Получение скалярного результата

    # Если записи нет, создаем ее
    if not stats_record:
        stats_record = Statistic(
            vacancy_id=vacancy_id,
            date=today
        )
        db.add(stats_record) # add сам по себе не awaitable
        # ИЗМЕНЕНИЕ: Асинхронный flush
        await db.flush() 

    # Инкрементируем счетчики
    stats_record.responses_count += responses
    stats_record.started_dialogs_count += started_dialogs
    stats_record.qualified_count += qualified

    # В этой функции нет commit(), предполагается, что он будет выполнен выше по стеку вызовов
    # (например, в _process_single_dialogue), что является хорошей практикой для "единицы работы".
    
    print(f"  > Статистика для вакансии {vacancy_id} обновлена: +{responses} откликов, +{started_dialogs} диалогов, +{qualified} квалифицировано.")