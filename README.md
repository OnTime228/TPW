# Scarecrow video stats Telegram bot

Telegram‑бот отвечает **одним числом** на запросы на русском языке и считает метрики по двум таблицам:

- `videos` — итоговая статистика по ролику
- `video_snapshots` — почасовые снапшоты + приращения (delta)

Проект рассчитан на запуск одной командой через `docker compose`.

## Быстрый старт

1) Скопируйте `.env.example` в `.env` и вставьте токен бота:

```bash
cp .env.example .env
# отредактируйте BOT_TOKEN
```

2) Положите предоставленный файл с данными в `./data/`.

Поддерживаются варианты:

- `./data/videos.json` (по умолчанию)
- `./data/videos.zip` (внутри должен быть .json)

> В `docker-compose.yml` по умолчанию указан путь `DATA_PATH=/data/videos.json`.

3) Запустите:

```bash
docker compose up --build
```

После старта контейнер:

- применит SQL‑миграции (`./migrations/*.sql`)
- загрузит JSON в Postgres (если таблица `videos` пустая)
- запустит Telegram‑бота

## Схема данных

Миграция: `migrations/001_create_tables.sql`

### videos
- `id` (uuid, PK)
- `creator_id` (text)
- `video_created_at` (timestamptz)
- `views_count`, `likes_count`, `comments_count`, `reports_count` (bigint)
- `created_at`, `updated_at` (timestamptz)

### video_snapshots
- `id` (uuid, PK)
- `video_id` (uuid, FK -> videos.id)
- текущие значения: `views_count`, `likes_count`, `comments_count`, `reports_count`
- приращения: `delta_views_count`, `delta_likes_count`, `delta_comments_count`, `delta_reports_count`
- `created_at` (timestamptz)
- `updated_at` (timestamptz)

## Как бот понимает запросы

В проекте сделан **детерминированный парсер** (без внешних LLM), который покрывает типовые формулировки из задания:

- количество видео (в целом / по креатору / в диапазоне дат)
- количество видео, у которых итоговый показатель (просмотры/лайки/комменты/жалобы) больше/меньше заданного порога
- суммарный прирост за дату (по `delta_*` в `video_snapshots`)
- количество **разных** видео, у которых за дату был прирост (например, новые просмотры)

Файлы:

- `app/nl2sql.py` — NL → SQL
- `app/ru_dates.py` — разбор дат вида `28 ноября 2025`, `с 1 по 5 ноября 2025`
- `app/ru_numbers.py` — разбор чисел `100 000`, `100к`, `2млн`

Если запрос не распознан, бот отвечает `0` (строго одним числом).

## Переменные окружения

Обязательные:

- `BOT_TOKEN` — токен Telegram‑бота
- `DATABASE_URL` — строка подключения к Postgres

Опциональные:

- `DATA_PATH` — путь к JSON/ZIP в контейнере (по умолчанию `/data/videos.json`)
- `FORCE_RELOAD=1` — принудительно перезалить данные (TRUNCATE + COPY)
- `AUTO_MIGRATE=0` — отключить миграции на старте
- `AUTO_LOAD_DATA=0` — отключить загрузку данных на старте

## Проверка

Для проверки через `@PPS_Check_bot` (как в задании) бот должен быть запущен и доступен.

Команда (пример):

```
/check @yourbotnickname https://github.com/yourrepo Фамилия
```

