from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


@dataclass(frozen=True)
class Settings:
    bot_token: str
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    data_path: str
    force_reload: bool

    gigachat_auth_key: str
    gigachat_scope: str
    gigachat_model: str
    gigachat_ssl_verify: bool

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"


def load_settings() -> Settings:
    token = _env("BOT_TOKEN", "")
    if not token.strip():
        raise RuntimeError("BOT_TOKEN is empty. Put it into .env")

    auth_key = _env("GIGACHAT_AUTH_KEY", "").strip()
    if not auth_key:
        raise RuntimeError("GIGACHAT_AUTH_KEY is empty. Put it into .env")

    return Settings(
        bot_token=token.strip(),
        pg_host=_env("POSTGRES_HOST", "db"),
        pg_port=int(_env("POSTGRES_PORT", "5432")),
        pg_db=_env("POSTGRES_DB", "videos_db"),
        pg_user=_env("POSTGRES_USER", "postgres"),
        pg_password=_env("POSTGRES_PASSWORD", "postgres"),
        data_path=_env("DATA_PATH", "/data/videos.json"),
        force_reload=_env("FORCE_RELOAD", "0").strip() in ("1", "true", "True", "yes", "YES"),

        gigachat_auth_key=auth_key,
        gigachat_scope=_env("GIGACHAT_SCOPE", "GIGACHAT_API_PERS").strip(),
        gigachat_model=_env("GIGACHAT_MODEL", "GigaChat-2-Max").strip(),
        gigachat_ssl_verify=_env("GIGACHAT_SSL_VERIFY", "0").strip() in ("1", "true", "True", "yes", "YES"),
    )