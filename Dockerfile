# Используем официальный образ Python 11
FROM python:3.11-slim

# Устанавливаем переменные окружения, чтобы Python не буферизировал логи
# и не создавал .pyc файлы внутри контейнера
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH=/app

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости для сборки пакетов (нужно для psycopg2/asyncpg)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем файл зависимостей
COPY requirements.txt .

# Обновляем pip и устанавливаем зависимости
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем весь код проекта в контейнер
COPY . .

# Мы не указываем CMD здесь, так как каждый сервис в docker-compose 
# будет запускать свой конкретный файл (poller, processor и т.д.)