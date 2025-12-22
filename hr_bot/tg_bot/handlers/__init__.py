# hr_bot/tg_bot/handlers/__init__.py

from aiogram import Router
from . import common, admin, user

main_router = Router()

# Порядок важен: сначала общие, потом специфичные для ролей
main_router.include_router(common.router)
main_router.include_router(admin.router)
main_router.include_router(user.router)