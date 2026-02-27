from __future__ import annotations

import asyncio
import logging

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.types import Message

from db import create_pool_with_retry, run_migrations
from loader import load_data_if_needed
from nl2sql import build_query
from settings import get_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("scarecrow_bot")


async def handle_message(message: Message, pool: asyncpg.Pool) -> None:
    text = (message.text or "").strip()

    # Requirement: one request -> one numeric answer.
    # For commands we also return a number to keep the protocol strict.
    if not text:
        await message.answer("0")
        return

    try:
        built = build_query(text)
        logger.info("NL2SQL (%s): %s", built.debug, text)
        value = await pool.fetchval(built.sql, *built.args)
        if value is None:
            value = 0

        # Ensure the response is a plain number
        await message.answer(str(int(value)))
    except Exception as e:  # noqa: BLE001
        logger.exception("Failed to process query: %r", e)
        await message.answer("0")


async def main() -> None:
    settings = get_settings()

    pool = await create_pool_with_retry(settings.database_url)

    if settings.auto_migrate:
        await run_migrations(pool, settings.migrations_path)

    if settings.auto_load_data:
        stats = await load_data_if_needed(pool, data_path=settings.data_path, force_reload=settings.force_reload)
        logger.info("Loaded: videos=%s snapshots=%s", stats.videos, stats.snapshots)

    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()

    dp.message.register(handle_message)

    try:
        await dp.start_polling(bot, pool=pool)
    finally:
        await bot.session.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
