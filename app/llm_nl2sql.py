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


_RU_MONTHS = {
    "январ": 1,
    "феврал": 2,
    "март": 3,
    "апрел": 4,
    "ма": 5,
    "июн": 6,
    "июл": 7,
    "август": 8,
    "сентябр": 9,
    "октябр": 10,
    "ноябр": 11,
    "декабр": 12,
}


def _month_range(year: int, month: int) -> Tuple[date, date]:
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def _extract_first_json(text: str) -> str:
    m = re.search(r"\{.*\}", text, flags=re.S)
    return m.group(0) if m else "{}"


def _extract_ru_month_year(text: str) -> Optional[Tuple[int, int]]:
    t = text.lower()
    m = re.search(r"\b(?:за|в)\s+([а-яё]+)\s+(\d{4})(?:\s+года)?\b", t)
    if not m:
        return None
    mon_raw = m.group(1)
    year = int(m.group(2))
    month = None
    for k, v in _RU_MONTHS.items():
        if mon_raw.startswith(k):
            month = v
            break
    if not month:
        return None
    return year, month


def _extract_first_hours(text: str) -> Optional[int]:
    t = text.lower()
    m = re.search(r"\bпервые\s+(\d+)\s+час", t)
    if not m:
        m = re.search(r"\bза\s+первые\s+(\d+)\s+час", t)
    if not m:
        m = re.search(r"\bв\s+первые\s+(\d+)\s+час", t)
    return int(m.group(1)) if m else None


SYSTEM_PROMPT = """Ты парсер запросов к базе статистики видео.

Верни ТОЛЬКО JSON (никакого текста) строго по схеме:

{
  "op": "count" | "sum",
  "metric": "videos" | "views" | "likes" | "comments" | "reports"
           | "delta_views" | "delta_likes" | "delta_comments",
  "filters": {
     "creator_id": "<string>" | null,
     "month": {"year": 2025, "month": 11} | null,
     "day": "YYYY-MM-DD" | null,
     "views_gt": 10000 | null,
     "first_hours": 3 | null
  }
}

Подсказки:
- "сколько видео" -> op=count, metric=videos
- "суммарное количество просмотров/лайков/комментариев/жалоб" -> op=sum, metric=views/likes/comments/reports (итоговая статистика из videos)
- "прирост просмотров" -> op=sum, metric=delta_views (из video_snapshots.delta_views_count)
- "прирост лайков" -> op=sum, metric=delta_likes (из video_snapshots.delta_likes_count)
- "прирост комментариев" -> op=sum, metric=delta_comments (из video_snapshots.delta_comments_count)
- "первые N часов после публикации" -> filters.first_hours = N
- "за <месяц> <год>" / "в <месяце> <год>" -> filters.month
"""


def _build_sql(obj: dict) -> Query:
    op = obj.get("op")
    metric = obj.get("metric")
    filters = obj.get("filters") or {}

    creator_id = filters.get("creator_id")
    month = filters.get("month")
    day = filters.get("day")
    views_gt = filters.get("views_gt")
    first_hours = filters.get("first_hours")

    where_parts = []
    params: Dict[str, Any] = {}
    p = 1

    def add_param(value: Any) -> str:
        nonlocal p
        key = f"${p}"
        params[key] = value
        p += 1
        return key

    if op == "count" and metric == "videos":
        table = "videos"
        select = "COUNT(*)::bigint"
        month_field = "video_created_at"
        join_sql = ""
    elif op == "sum" and metric == "views":
        table = "videos"
        select = "COALESCE(SUM(views_count),0)::bigint"
        month_field = "video_created_at"
        join_sql = ""
    elif op == "sum" and metric == "likes":
        table = "videos"
        select = "COALESCE(SUM(likes_count),0)::bigint"
        month_field = "video_created_at"
        join_sql = ""
    elif op == "sum" and metric == "comments":
        table = "videos"
        select = "COALESCE(SUM(comments_count),0)::bigint"
        month_field = "video_created_at"
        join_sql = ""
    elif op == "sum" and metric == "reports":
        table = "videos"
        select = "COALESCE(SUM(reports_count),0)::bigint"
        month_field = "video_created_at"
        join_sql = ""
    elif op == "sum" and metric == "delta_views":
        table = "video_snapshots"
        select = "COALESCE(SUM(delta_views_count),0)::bigint"
        month_field = "created_at"
        join_sql = ""
    elif op == "sum" and metric == "delta_likes":
        table = "video_snapshots"
        select = "COALESCE(SUM(delta_likes_count),0)::bigint"
        month_field = "created_at"
        join_sql = ""
    elif op == "sum" and metric == "delta_comments":
        table = "video_snapshots"
        select = "COALESCE(SUM(delta_comments_count),0)::bigint"
        month_field = "created_at"
        join_sql = ""
    else:
        return Query("SELECT 0::bigint", {})

    if first_hours is not None:
        try:
            h = int(first_hours)
        except Exception:
            h = None
        if h is None or h <= 0:
            return Query("SELECT 0::bigint", {})

        if table != "video_snapshots":
            return Query("SELECT 0::bigint", {})


        join_sql = " JOIN videos v ON v.id = video_snapshots.video_id"
        k = add_param(h)
        where_parts.append("video_snapshots.created_at >= v.video_created_at")
        where_parts.append(f"video_snapshots.created_at < (v.video_created_at + ({k}::int * interval '1 hour'))")

    if isinstance(month, dict) and "year" in month and "month" in month:
        start, end = _month_range(int(month["year"]), int(month["month"]))
        k1 = add_param(start)
        k2 = add_param(end)

        prefix = "video_snapshots." if table == "video_snapshots" else "videos."
    
        if table == "video_snapshots":
            prefix = "video_snapshots."
        where_parts.append(f"{prefix}{month_field} >= {k1}::date")
        where_parts.append(f"{prefix}{month_field} < {k2}::date")

    if isinstance(day, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        y, m, d = map(int, day.split("-"))
        dd = date(y, m, d)
        k1 = add_param(dd)
        k2 = add_param(dd)
        where_parts.append(f"{'video_snapshots.' if table=='video_snapshots' else ''}created_at >= {k1}::date")
        where_parts.append(f"{'video_snapshots.' if table=='video_snapshots' else ''}created_at < ({k2}::date + interval '1 day')")

    if creator_id is not None and table == "videos":
        k = add_param(str(creator_id))
        where_parts.append(f"creator_id = {k}")

    if views_gt is not None and table == "videos":
        k = add_param(int(views_gt))
        where_parts.append(f"views_count > {k}")

    sql = f"SELECT {select} FROM {table}{join_sql}"
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    return Query(sql, params)


def _postprocess(obj: dict, text: str) -> dict:
    t = (text or "").lower()

    if not isinstance(obj, dict):
        obj = {}
    obj.setdefault("filters", {})
    if not isinstance(obj["filters"], dict):
        obj["filters"] = {}

    if "прирост" in t and ("лайк" in t or "лайков" in t):
        obj["op"] = "sum"
        obj["metric"] = "delta_likes"

    if "прирост" in t and ("коммент" in t or "комментар" in t):
        obj["op"] = "sum"
        obj["metric"] = "delta_comments"

    if "прирост" in t and ("просмотр" in t or "просмотров" in t):
        obj["op"] = "sum"
        obj["metric"] = "delta_views"

    ym = _extract_ru_month_year(t)
    if ym:
        y, m = ym
        obj["filters"]["month"] = {"year": y, "month": m}

    h = _extract_first_hours(t)
    if h is not None:
        obj["filters"]["first_hours"] = h

    return obj


async def nl_to_query_llm(client: GigaChatClient, text: str) -> Query:
    raw = await client.chat(system=SYSTEM_PROMPT, user=text)
    js_text = _extract_first_json(raw)

    try:
        obj = json.loads(js_text)
    except Exception:
        obj = {}

    obj = _postprocess(obj, text)
    return _build_sql(obj)