from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    bot_token: str
    database_url: str

    # Paths inside the container
    migrations_path: str
    data_path: str

    # Startup behaviour
    auto_migrate: bool
    auto_load_data: bool
    force_reload: bool

    # Optional LLM fallback (rule-based parser is the default)
    llm_provider: str | None
    llm_api_key: str | None
    llm_base_url: str | None
    llm_model: str | None


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip() == "1"


def get_settings() -> Settings:
    """Load configuration from environment (.env is supported)."""

    load_dotenv(override=False)

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    database_url = os.getenv("DATABASE_URL", "").strip()

    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required")

    return Settings(
        bot_token=bot_token,
        database_url=database_url,
        migrations_path=os.getenv("MIGRATIONS_PATH", "/migrations"),
        data_path=os.getenv("DATA_PATH", "/data/videos.json"),
        auto_migrate=_env_bool("AUTO_MIGRATE", "1"),
        auto_load_data=_env_bool("AUTO_LOAD_DATA", "1"),
        force_reload=_env_bool("FORCE_RELOAD", "0"),
        llm_provider=os.getenv("LLM_PROVIDER"),
        llm_api_key=os.getenv("LLM_API_KEY"),
        llm_base_url=os.getenv("LLM_BASE_URL"),
        llm_model=os.getenv("LLM_MODEL"),
    )
