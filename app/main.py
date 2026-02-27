from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.types import Message

from app.settings import load_settings
from app.loader import load_data_if_needed
from app.gigachat_client import GigaChatClient
from app.llm_nl2sql import nl_to_query_llm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scarecrow_bot")


async def apply_migrations(conn: asyncpg.Connection) -> None:
    migrations_dir = Path("/migrations")
    files = sorted(migrations_dir.glob("*.sql"))
    if not files:
        raise RuntimeError("No migrations found in /migrations")

    for f in files:
        sql = f.read_text(encoding="utf-8")
        await conn.execute(sql)
        logger.info("Running migration: %s", f.name)


async def handle_message(message: Message, pool: asyncpg.Pool, gc: GigaChatClient) -> None:
    q = await nl_to_query_llm(gc, message.text or "")

    try:
        async with pool.acquire() as conn:
            # params keys are "$1", "$2"... -> pass in order
            args = [q.params[k] for k in sorted(q.params.keys(), key=lambda x: int(x[1:]))]
            val = await conn.fetchval(q.sql, *args)
            if val is None:
                val = 0
    except Exception:
        logger.exception("Query failed")
        val = 0

    await message.answer(str(int(val)))


async def main() -> None:
    settings = load_settings()

    gc = GigaChatClient(
        auth_key=settings.gigachat_auth_key,
        scope=settings.gigachat_scope,
        ssl_verify=settings.gigachat_ssl_verify,
        model=settings.gigachat_model,
    )

    pool = await asyncpg.create_pool(dsn=settings.dsn, min_size=1, max_size=10)

    async with pool.acquire() as conn:
        await apply_migrations(conn)

    stats = await load_data_if_needed(pool, data_path=settings.data_path, force_reload=settings.force_reload)
    logger.info("Loaded: videos=%s snapshots=%s", stats.videos, stats.snapshots)

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()

    @dp.message()
    async def _any(message: Message) -> None:
        await handle_message(message, pool, gc)

    logger.info("Start polling")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())