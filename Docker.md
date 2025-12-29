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
docker compose logs -f reminder
docker compose logs -f tg_bot


docker compose logs -f
docker compose ps




В графане
{container_name=~"hr_bot_(poller|processor|reminders)"}