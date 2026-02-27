from __future__ import annotations

import json
import logging
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Any, Optional

import asyncpg

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadStats:
    videos: int
    snapshots: int


def _read_json_from_path(data_path: str) -> dict:
    p = Path(data_path)
    if not p.exists():
        raise FileNotFoundError(
            f"Data file not found: {data_path}. "
            "Put the provided JSON (or ZIP with JSON) into ./data and mount it into the container."
        )

    if p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p, "r") as z:
            json_names = [n for n in z.namelist() if n.lower().endswith(".json")]
            if not json_names:
                raise RuntimeError("ZIP does not contain any .json files")
            name = json_names[0]
            logger.info("Reading JSON from zip entry: %s", name)
            raw = z.read(name)
            return json.loads(raw)

    logger.info("Reading JSON: %s", p)
    raw = p.read_text(encoding="utf-8")
    return json.loads(raw)


def parse_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    raise TypeError(f"Unexpected datetime value type: {type(v)}")


def to_int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return default
        s = s.replace(" ", "")
        return int(s)
    return int(v)


def _iter_video_records(videos: list[dict]) -> Iterable[tuple]:
    for v in videos:
        yield (
            str(v.get("id")),                 # UUID -> TEXT
            str(v.get("creator_id")),         # UUID/строка -> TEXT
            parse_dt(v.get("video_created_at")),
            to_int(v.get("views_count")),
            to_int(v.get("likes_count")),
            to_int(v.get("comments_count")),
            to_int(v.get("reports_count")),
            parse_dt(v.get("created_at")),
            parse_dt(v.get("updated_at")),
        )


def _iter_snapshot_records(videos: list[dict]) -> Iterable[tuple]:
    for v in videos:
        for s in v.get("snapshots", []):
            yield (
                str(s.get("id")),             # UUID -> TEXT
                str(s.get("video_id")),       # UUID -> TEXT (FK на videos.id)
                to_int(s.get("views_count")),
                to_int(s.get("likes_count")),
                to_int(s.get("comments_count")),
                to_int(s.get("reports_count")),
                to_int(s.get("delta_views_count")),
                to_int(s.get("delta_likes_count")),
                to_int(s.get("delta_comments_count")),
                to_int(s.get("delta_reports_count")),
                parse_dt(s.get("created_at")),
                parse_dt(s.get("updated_at")),
            )


async def load_data_if_needed(
    pool: asyncpg.Pool,
    *,
    data_path: str,
    force_reload: bool = False,
) -> LoadStats:
    async with pool.acquire() as conn:
        existing = await conn.fetchval("SELECT COUNT(*) FROM videos")
        if existing and not force_reload:
            logger.info("Data already loaded (videos=%s). Skipping.", existing)
            snaps = await conn.fetchval("SELECT COUNT(*) FROM video_snapshots")
            return LoadStats(videos=int(existing), snapshots=int(snaps))

    payload = _read_json_from_path(data_path)
    videos_list = payload.get("videos")
    if not isinstance(videos_list, list):
        raise RuntimeError("Invalid JSON: expected top-level key 'videos' to be a list")

    video_records = list(_iter_video_records(videos_list))
    snapshot_records = list(_iter_snapshot_records(videos_list))

    logger.info("Loading videos=%s, snapshots=%s", len(video_records), len(snapshot_records))

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("TRUNCATE TABLE video_snapshots, videos CASCADE")

            await conn.copy_records_to_table(
                "videos",
                records=video_records,
                columns=[
                    "id",
                    "creator_id",
                    "video_created_at",
                    "views_count",
                    "likes_count",
                    "comments_count",
                    "reports_count",
                    "created_at",
                    "updated_at",
                ],
            )

            await conn.copy_records_to_table(
                "video_snapshots",
                records=snapshot_records,
                columns=[
                    "id",
                    "video_id",
                    "views_count",
                    "likes_count",
                    "comments_count",
                    "reports_count",
                    "delta_views_count",
                    "delta_likes_count",
                    "delta_comments_count",
                    "delta_reports_count",
                    "created_at",
                    "updated_at",
                ],
            )

    return LoadStats(videos=len(video_records), snapshots=len(snapshot_records))