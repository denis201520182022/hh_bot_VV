# hr_bot/db/models.py
import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import (
    Column, Integer, String, Text, ForeignKey, DateTime, Date, func, Numeric, Boolean, BigInteger, text
)
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB
from dotenv import load_dotenv
import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import create_engine
# Загружаем переменные из .env
load_dotenv()

# Получаем данные из окружения
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Формируем строки подключения динамически
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
SYNC_DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_async_engine( # 
    DATABASE_URL,
    pool_size=115,
    max_overflow=20,
    pool_timeout=60,
    pool_recycle=3600,
    pool_pre_ping=True
)

# Теперь SessionLocal будет возвращать асинхронные сессии (AsyncSession)
SessionLocal = async_sessionmaker(
    engine, # Асинхронный движок
    expire_on_commit=False, # Рекомендуется для асинхронных сессий
    class_=AsyncSession # Явно указываем класс сессии
)



# --- СИНХРОННЫЙ ДВИЖОК И ФАБРИКА СЕССИЙ (ДЛЯ ТГ-БОТА) ---
sync_engine = create_engine(
    SYNC_DATABASE_URL,
    pool_size=10,
    max_overflow=5,
    pool_timeout=60,
    pool_recycle=3600,
    pool_pre_ping=True
)
SyncSessionLocal = sessionmaker(
    sync_engine,
    expire_on_commit=False # Рекомендуется для синхронных сессий, чтобы избежать lazy loading после закрытия
)





Base = declarative_base()


class Vacancy(Base):
    __tablename__ = 'vacancies'
    id = Column(Integer, primary_key=True, index=True)
    hh_vacancy_id = Column(String(50), unique=True)
    title = Column(String(255), nullable=False)
    city = Column(String(100))
    # --- ДОБАВИТЬ ЭТО ПОЛЕ ---
    recruiter_id = Column(Integer, ForeignKey('tracked_recruiters.id'), nullable=True)
    # --- КОНЕЦ ---
    statistics = relationship("Statistic", back_populates="vacancy")
    dialogues = relationship("Dialogue", back_populates="vacancy")
    # --- ДОБАВИТЬ ЭТУ СВЯЗЬ ---
    recruiter = relationship("TrackedRecruiter", back_populates="vacancies")
    # --- КОНЕЦ ---

class Candidate(Base):
    __tablename__ = 'candidates'
    id = Column(Integer, primary_key=True, index=True)
    hh_resume_id = Column(String(50), unique=True)
    full_name = Column(String(255))
    age = Column(Integer)
    citizenship = Column(String(100))
    phone_number = Column(String(50), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=text("now()"))
    city = Column(String(255), nullable=True) # Поле с прошлого шага
    
    # --- ДОБАВЬТЕ ЭТУ СТРОКУ ---
    readiness_to_start = Column(String(255), nullable=True)
    dialogues = relationship("Dialogue", back_populates="candidate")

class TrackedRecruiter(Base):
    __tablename__ = 'tracked_recruiters'
    id = Column(Integer, primary_key=True, index=True)
    recruiter_id = Column(String(50), unique=True, nullable=False)
    name = Column(String(100))
    
    refresh_token = Column(Text, nullable=True)
    access_token = Column(Text, nullable=True)
    token_expires_at = Column(DateTime(timezone=True), nullable=True)
    vacancies_last_synced_at = Column(DateTime(timezone=True), nullable=True)
    
    dialogues = relationship("Dialogue", back_populates="recruiter")
    # --- ДОБАВИТЬ ЭТУ СВЯЗЬ ---
    vacancies = relationship("Vacancy", back_populates="recruiter")
    # --- КОНЕЦ ---
    # --- НОВЫЕ ПОЛЯ ДЛЯ НАСТРОЙКИ ЧАТА ---
    telegram_chat_id = Column(BigInteger, nullable=True) # ID группы рекрутера
    topic_qualified_id = Column(Integer, nullable=True)  # ID темы "Подходящие"
    topic_rejected_id = Column(Integer, nullable=True)   # ID темы "Отказники"
    topic_timeout_id = Column(Integer, nullable=True)    # ID темы "Молчуны"
    # -------------------------------------
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    dialogues = relationship("Dialogue", back_populates="recruiter")
    # --- ДОБАВИТЬ ЭТУ СВЯЗЬ ---
    vacancies = relationship("Vacancy", back_populates="recruiter")
    # --- КОНЕЦ ---

class Dialogue(Base):
    __tablename__ = 'dialogues'
    id = Column(Integer, primary_key=True, index=True)
    hh_response_id = Column(String(50), unique=True, nullable=False)
    recruiter_id = Column(Integer, ForeignKey('tracked_recruiters.id'))
    candidate_id = Column(Integer, ForeignKey('candidates.id'))
    vacancy_id = Column(Integer, ForeignKey('vacancies.id'))
    dialogue_state = Column(String(100))
    status = Column(String(50), nullable=False, default='new')
    reminder_level = Column(Integer, nullable=False, default=0, server_default='0')
    history = Column(JSONB)
    pending_messages = Column(JSONB)
    last_updated = Column(
        DateTime(timezone=True), 
        server_default=func.timezone('UTC', func.now()),
        onupdate=func.timezone('UTC', func.now()),
        index=True
    )
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    response_created_at = Column(DateTime(timezone=True), nullable=True)
    
    interview_datetime_utc = Column(DateTime(timezone=True), nullable=True)

    # --- ДОБАВИТЬ ЭТИ ПОЛЯ ДЛЯ СТАТИСТИКИ ТОКЕНОВ ---
    total_prompt_tokens = Column(Integer, nullable=False, default=0, server_default='0')
    total_completion_tokens = Column(Integer, nullable=False, default=0, server_default='0')
    total_cached_tokens = Column(Integer, nullable=False, default=0, server_default='0')
    total_cost = Column(Numeric(12, 6), nullable=False, default=0.0, server_default='0.0')
    # --- КОНЕЦ ДОБАВЛЕНИЯ ---

    candidate = relationship("Candidate", back_populates="dialogues")
    vacancy = relationship("Vacancy", back_populates="dialogues")
    recruiter = relationship("TrackedRecruiter", back_populates="dialogues")

    # --- ДОБАВИТЬ ЭТИ ДВЕ СТРОКИ СВЯЗЕЙ ---
    inactive_alerts = relationship("InactiveNotificationQueue", back_populates="dialogue", uselist=False) # uselist=False, т.к. unique=True на dialogue_id
    rejected_alerts = relationship("RejectedNotificationQueue", back_populates="dialogue", uselist=False) # uselist=False, т.к. unique=True на dialogue_id
    # --- КОНЕЦ ДОБАВЛЕНИЯ ---
    reminders = relationship("InterviewReminder", back_populates="dialogue")
    # --- ДОБАВЛЕНА НОВАЯ СВЯЗЬ ---
    # --- ДОБАВИТЬ НОВУЮ СВЯЗЬ С ЛОГАМИ ---
    llm_usage_logs = relationship("LlmUsageLog", back_populates="dialogue", cascade="all, delete-orphan")
    # --- КОНЕЦ ДОБАВЛЕНИЯ ---
    

class Statistic(Base):
    __tablename__ = 'statistics'
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, default=datetime.date.today)
    responses_count = Column(Integer, default=0)
    started_dialogs_count = Column(Integer, default=0)
    qualified_count = Column(Integer, default=0)
    vacancy_id = Column(Integer, ForeignKey('vacancies.id'))
    vacancy = relationship("Vacancy", back_populates="statistics")

class TelegramUser(Base):
    __tablename__ = 'telegram_users'
    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(String(50), unique=True, nullable=False)
    username = Column(String(100))
    role = Column(String(50), nullable=False, default='user')
    created_at = Column(DateTime(timezone=True), server_default=func.timezone('UTC', func.now()))

class NotificationQueue(Base):
    __tablename__ = 'notification_queue'
    id = Column(Integer, primary_key=True, index=True)
    candidate_id = Column(Integer, ForeignKey('candidates.id'), nullable=False)
    status = Column(String(50), nullable=False, default='pending')
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_at = Column(DateTime(timezone=True))
    candidate = relationship("Candidate")

class TrackedVacancy(Base):
    __tablename__ = 'tracked_vacancies'
    id = Column(Integer, primary_key=True, index=True)
    vacancy_id = Column(String(50), unique=True, nullable=False)
    title = Column(String(255))


class AppSettings(Base):
    __tablename__ = 'app_settings'
    
    id = Column(Integer, primary_key=True)
    # Используем server_default для базы данных и default для SQLAlchemy
    balance = Column(Numeric(12, 2), nullable=False, default=0.00, server_default='0.00')
    cost_per_dialogue = Column(Numeric(10, 2), nullable=False, default=19.00, server_default='19.00')
    cost_per_long_reminder = Column(Numeric(10, 2), nullable=False, default=5.00, server_default='5.00')
    low_balance_threshold = Column(Numeric(10, 2), nullable=False, default=500.00, server_default='500.00')
    low_limit_notified = Column(Boolean, nullable=False, default=False, server_default='false')

    # --- НОВАЯ МОДЕЛЬ ДЛЯ УВЕДОМЛЕНИЙ О НЕАКТИВНЫХ КАНДИДАТАХ ---
class InactiveNotificationQueue(Base):
    __tablename__ = 'inactive_notification_queue'
    id = Column(Integer, primary_key=True, index=True)
    # dialogue_id уникален, чтобы отправлять одно уведомление на один "зависший" диалог
    dialogue_id = Column(Integer, ForeignKey('dialogues.id'), unique=True, nullable=False)
    status = Column(String(50), nullable=False, default='pending') # pending, sent, error
    created_at = Column(DateTime(timezone=True), server_default=func.timezone('UTC', func.now()))
    processed_at = Column(DateTime(timezone=True))
    
    dialogue = relationship("Dialogue", back_populates="inactive_alerts")
# --- КОНЕЦ НОВОЙ МОДЕЛИ ---

# --- НОВАЯ МОДЕЛЬ ДЛЯ УВЕДОМЛЕНИЙ ОБ ОТКЛОНЕННЫХ КАНДИДАТАХ ---
class RejectedNotificationQueue(Base):
    __tablename__ = 'rejected_notification_queue'
    id = Column(Integer, primary_key=True, index=True)
    # dialogue_id уникален, чтобы отправлять одно уведомление на один отклонённый диалог
    dialogue_id = Column(Integer, ForeignKey('dialogues.id'), unique=True, nullable=False)
    status = Column(String(50), nullable=False, default='pending') # pending, sent, error
    created_at = Column(DateTime(timezone=True), server_default=func.timezone('UTC', func.now()))
    processed_at = Column(DateTime(timezone=True))
    
    dialogue = relationship("Dialogue", back_populates="rejected_alerts")
# --- КОНЕЦ НОВОЙ МОДЕЛИ ---

# --- НОВАЯ МОДЕЛЬ ДЛЯ УВЕДОМЛЕНИЙ О СОБЕСЕДОВАНИЯХ ---
class InterviewReminder(Base):
    __tablename__ = 'interview_reminders'
    id = Column(Integer, primary_key=True, index=True)
    dialogue_id = Column(Integer, ForeignKey('dialogues.id'), nullable=False, index=True)
    recruiter_id = Column(Integer, ForeignKey('tracked_recruiters.id'), nullable=False, index=True)
    
    interview_datetime_utc = Column(DateTime(timezone=True), nullable=False)
    scheduled_send_time_utc = Column(DateTime(timezone=True), nullable=False, index=True)
    notification_type = Column(String(50), nullable=False) # '2_hours_before', '1_day_before_20h_spb', 'day_of_9h_spb'
    status = Column(String(50), nullable=False, default='pending') # 'pending', 'sent', 'cancelled', 'error'
    
    created_at = Column(DateTime(timezone=True), server_default=func.timezone('UTC', func.now()))
    processed_at = Column(DateTime(timezone=True))
    
    dialogue = relationship("Dialogue", back_populates="reminders")
    recruiter = relationship("TrackedRecruiter") # Добавим связь для удобного доступа к рекрутеру
# --- КОНЕЦ НОВОЙ МОДЕЛИ ---

# --- НОВАЯ МОДЕЛЬ ДЛЯ ЛОГИРОВАНИЯ ИСПОЛЬЗОВАНИЯ ТОКЕНОВ ---
class LlmUsageLog(Base):
    __tablename__ = 'llm_usage_logs'
    id = Column(Integer, primary_key=True, index=True)
    
    # Связь с диалогом, в рамках которого был сделан запрос
    dialogue_id = Column(Integer, ForeignKey('dialogues.id'), nullable=False, index=True)
    
    # Состояние диалога НА МОМЕНТ вызова LLM
    dialogue_state_at_call = Column(String(100))
    
    # Данные по токенам из ответа API
    prompt_tokens = Column(Integer, default=0)       # Входные токены
    completion_tokens = Column(Integer, default=0)   # Выходные токены (сгенерированный ответ)
    cached_tokens = Column(Integer, default=0)       # Входные токены, которые были закешированы
    total_tokens = Column(Integer, default=0)        # Общее количество токенов в запросе
    
    # Рассчитанная стоимость этого конкретного вызова
    # Используем Numeric для точности финансовых данных. 
    # Precision=10, scale=6 означает до 10 знаков всего, из них 6 после запятой.
    cost = Column(Numeric(10, 6), nullable=False, default=0.0)
    
    created_at = Column(DateTime(timezone=True), server_default=func.timezone('UTC', func.now()))

    dialogue = relationship("Dialogue", back_populates="llm_usage_logs")
# --- КОНЕЦ НОВОЙ МОДЕЛИ ---