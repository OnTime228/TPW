from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional, Tuple

from app.gigachat_client import GigaChatClient


@dataclass(frozen=True)
class Query:
    sql: str
    params: Dict[str, Any]


def _month_range(year: int, month: int) -> Tuple[date, date]:
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def _extract_first_json(text: str) -> str:
    # Модель иногда добавляет текст — вырезаем первый {...}
    m = re.search(r"\{.*\}", text, flags=re.S)
    return m.group(0) if m else "{}"


SYSTEM_PROMPT = """Ты парсер запросов к базе статистики видео.

Верни ТОЛЬКО JSON (никакого текста) строго по схеме:

{
  "op": "count" | "sum",
  "metric": "videos" | "views" | "likes" | "comments" | "reports" | "delta_views",
  "filters": {
     "creator_id": "<string>" | null,
     "month": {"year": 2025, "month": 6} | null,
     "day": "YYYY-MM-DD" | null,
     "views_gt": 10000 | null
  }
}

Правила:
- "сколько видео" -> op=count, metric=videos
- "суммарное количество просмотров/лайков/комментариев/жалоб" -> op=sum, metric=views/likes/comments/reports
- "по итоговой статистике" означает использовать videos.<metric> (НЕ snapshots)
- "на сколько просмотров в сумме выросли ... за день" -> op=sum, metric=delta_views и filters.day
- "за <месяц> <год>" или "в <месяце> <год>" -> filters.month
- creator_id может быть UUID (с дефисами/без) — возвращай как строку
- Если не можешь распарсить — верни:
  {"op":"count","metric":"videos","filters":{"creator_id":null,"month":null,"day":null,"views_gt":null}}
"""


def _build_sql(obj: dict) -> Query:
    op = obj.get("op")
    metric = obj.get("metric")
    filters = obj.get("filters") or {}

    creator_id = filters.get("creator_id")
    month = filters.get("month")
    day = filters.get("day")
    views_gt = filters.get("views_gt")

    # ---- whitelist-only SQL builder ----
    where_parts = []
    params: Dict[str, Any] = {}
    p = 1

    def add_param(value: Any) -> str:
        nonlocal p
        key = f"${p}"
        params[key] = value
        p += 1
        return key

    # Month filter uses videos.video_created_at
    if isinstance(month, dict) and "year" in month and "month" in month:
        start, end = _month_range(int(month["year"]), int(month["month"]))
        k1 = add_param(start)
        k2 = add_param(end)
        where_parts.append(f"video_created_at >= {k1}::date")
        where_parts.append(f"video_created_at < {k2}::date")

    # Day filter for snapshots (delta)
    if isinstance(day, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        y, m, d = map(int, day.split("-"))
        dd = date(y, m, d)
        k1 = add_param(dd)
        where_parts.append(f"created_at >= {k1}::date")
        # reuse same dd as param for end-of-day
        k2 = add_param(dd)
        where_parts.append(f"created_at < ({k2}::date + interval '1 day')")

    if creator_id is not None:
        k = add_param(str(creator_id))
        where_parts.append(f"creator_id = {k}")

    if views_gt is not None:
        k = add_param(int(views_gt))
        where_parts.append(f"views_count > {k}")

    # SELECT mapping (ONLY allowed)
    if op == "count" and metric == "videos":
        table = "videos"
        select = "COUNT(*)::bigint"
    elif op == "sum" and metric == "views":
        table = "videos"
        select = "COALESCE(SUM(views_count),0)::bigint"
    elif op == "sum" and metric == "likes":
        table = "videos"
        select = "COALESCE(SUM(likes_count),0)::bigint"
    elif op == "sum" and metric == "comments":
        table = "videos"
        select = "COALESCE(SUM(comments_count),0)::bigint"
    elif op == "sum" and metric == "reports":
        table = "videos"
        select = "COALESCE(SUM(reports_count),0)::bigint"
    elif op == "sum" and metric == "delta_views":
        table = "video_snapshots"
        select = "COALESCE(SUM(delta_views_count),0)::bigint"
    else:
        return Query("SELECT 0::bigint", {})

    sql = f"SELECT {select} FROM {table}"
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    return Query(sql, params)


async def nl_to_query_llm(client: GigaChatClient, text: str) -> Query:
    raw = await client.chat(system=SYSTEM_PROMPT, user=text)
    js_text = _extract_first_json(raw)

    try:
        obj = json.loads(js_text)
    except Exception:
        obj = {
            "op": "count",
            "metric": "videos",
            "filters": {"creator_id": None, "month": None, "day": None, "views_gt": None},
        }

    return _build_sql(obj)