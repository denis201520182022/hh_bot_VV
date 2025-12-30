Сборка и запуск в фоне
docker compose up -d --build







Имена контейнеров

hr_bot_pgbouncer
hr_bot_poller
hr_bot_processor
hr_bot_reminders
hr_bot_loki
hr_bot_grafana



Названия воркеров:


poller
processor
reminder
tg_bot

docker compose logs -f poller
docker compose logs -f processor
docker compose logs -f reminders
docker compose logs -f tg_bot



docker compose logs -f poller processor reminders | awk '
/poller/    {print "\033[31m" $0 "\033[0m"; next}
/processor/ {print "\033[32m" $0 "\033[0m"; next}
/reminder/  {print "\033[33m" $0 "\033[0m"; next}
/tg_bot/    {print "\033[34m" $0 "\033[0m"; next}
{print}
'





docker compose logs -f poller processor reminder tg_bot



docker compose logs -f
docker compose ps




В графане
{container_name=~"hr_bot_(poller|processor|reminders)"}