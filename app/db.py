from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import asyncpg

logger = logging.getLogger(__name__)


async def create_pool_with_retry(dsn: str, *, max_attempts: int = 30) -> asyncpg.Pool:
    """Create asyncpg pool with retries (useful when Postgres starts in docker)."""
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=10)
            # lightweight sanity check
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
            return pool
        except Exception as e:  # noqa: BLE001
            last_err = e
            logger.warning("DB connection attempt %s/%s failed: %s", attempt, max_attempts, e)
            await asyncio.sleep(min(2.0, 0.2 * attempt))

    raise RuntimeError(f"Could not connect to DB after {max_attempts} attempts: {last_err}")


async def run_migrations(pool: asyncpg.Pool, migrations_path: str) -> None:
    """Execute all *.sql files from migrations_path (sorted by name)."""
    path = Path(migrations_path)
    if not path.exists() or not path.is_dir():
        raise RuntimeError(f"Migrations path not found or not a directory: {migrations_path}")

    files = sorted(path.glob("*.sql"))
    if not files:
        raise RuntimeError(f"No .sql migrations found in: {migrations_path}")

    async with pool.acquire() as conn:
        for f in files:
            sql = f.read_text(encoding="utf-8")
            logger.info("Running migration: %s", f.name)
            await conn.execute(sql)


async def fetchval(pool: asyncpg.Pool, sql: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetchval(sql, *args)


async def execute(pool: asyncpg.Pool, sql: str, *args) -> str:
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args)
