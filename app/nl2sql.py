from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any, Tuple


@dataclass(frozen=True)
class Query:
    sql: str
    params: Dict[str, Any]


def _parse_int(text: str) -> Optional[int]:
    m = re.search(r"\b(\d{1,18})\b", text)
    return int(m.group(1)) if m else None


def _parse_date_yyyy_mm_dd(text: str) -> Optional[date]:
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


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


def _parse_date_ru(text: str) -> Optional[date]:
    m = re.search(r"\b(\d{1,2})\s+([а-яё]+)\s+(\d{4})\b", text.lower())
    if not m:
        return None
    day = int(m.group(1))
    mon_raw = m.group(2)
    year = int(m.group(3))
    month = None
    for k, v in _RU_MONTHS.items():
        if mon_raw.startswith(k):
            month = v
            break
    if not month:
        return None
    return date(year, month, day)


def _extract_date_range(text: str) -> Tuple[Optional[date], Optional[date]]:
    t = text.lower()
    m = re.search(r"с\s+(.+?)\s+по\s+(.+?)(?:\b|$)", t)
    if m:
        d1 = _parse_date_ru(m.group(1)) or _parse_date_yyyy_mm_dd(m.group(1))
        d2 = _parse_date_ru(m.group(2)) or _parse_date_yyyy_mm_dd(m.group(2))
        return d1, d2

    d = _parse_date_ru(t) or _parse_date_yyyy_mm_dd(t)
    if d:
        return d, d

    return None, None


def _is_total_question(t: str) -> bool:
    # общие/всего/суммарно/в сумме + количество/сколько
    return (
        ("общее" in t or "всего" in t or "суммар" in t or "в сумме" in t)
        and ("колич" in t or "сколько" in t or "сумм" in t)
    )


def nl_to_query(text: str) -> Query:
    t = " ".join((text or "").strip().lower().split())

    # 0) total likes / views / comments / reports
    # "Какое общее количество лайков набрали все видео?"
    if _is_total_question(t) and "лайк" in t:
        return Query("SELECT COALESCE(SUM(likes_count),0)::bigint FROM videos", {})

    if _is_total_question(t) and "просмотр" in t:
        return Query("SELECT COALESCE(SUM(views_count),0)::bigint FROM videos", {})

    if _is_total_question(t) and "коммент" in t:
        return Query("SELECT COALESCE(SUM(comments_count),0)::bigint FROM videos", {})

    if _is_total_question(t) and ("жалоб" in t or "репорт" in t or "report" in t):
        return Query("SELECT COALESCE(SUM(reports_count),0)::bigint FROM videos", {})

    # 1) "Сколько всего видео есть в системе?"
    if re.search(r"\bсколько\b.*\bвсего\b.*\bвидео\b", t) or t == "сколько видео":
        return Query("SELECT COUNT(*)::bigint FROM videos", {})

    # 2) "Сколько видео у креатора с id X вышло с ... по ..."
    if ("креатор" in t or "creator" in t) and "id" in t and "видео" in t and ("вышло" in t or "вышли" in t):
        creator_id = _parse_int(t)
        d1, d2 = _extract_date_range(t)
        if creator_id is not None and d1 and d2:
            return Query(
                """
                SELECT COUNT(*)::bigint
                FROM videos
                WHERE creator_id = $1
                  AND video_created_at >= $2::date
                  AND video_created_at < ($3::date + interval '1 day')
                """.strip(),
                {"$1": creator_id, "$2": d1.isoformat(), "$3": d2.isoformat()},
            )

    # 3) "Сколько видео набрало больше 100000 просмотров"
    if "видео" in t and ("просмотр" in t or "просмотров" in t) and ("больше" in t or "более" in t):
        x = _parse_int(t)
        if x is not None:
            return Query(
                "SELECT COUNT(*)::bigint FROM videos WHERE views_count > $1",
                {"$1": x},
            )

    # 4) "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
    if "в сумме" in t and "просмотр" in t and ("вырос" in t or "выросли" in t or "прирос" in t or "прирост" in t):
        d1, d2 = _extract_date_range(t)
        if d1 and d2 and d1 == d2:
            return Query(
                """
                SELECT COALESCE(SUM(delta_views_count),0)::bigint
                FROM video_snapshots
                WHERE created_at >= $1::date
                  AND created_at < ($1::date + interval '1 day')
                """.strip(),
                {"$1": d1.isoformat()},
            )

    # 5) "Сколько разных видео получали новые просмотры 27 ноября 2025?"
    if "сколько" in t and ("разных" in t or "различных" in t) and "видео" in t and ("новые просмотры" in t or "новых просмотров" in t):
        d1, d2 = _extract_date_range(t)
        if d1 and d2 and d1 == d2:
            return Query(
                """
                SELECT COUNT(DISTINCT video_id)::bigint
                FROM video_snapshots
                WHERE delta_views_count > 0
                  AND created_at >= $1::date
                  AND created_at < ($1::date + interval '1 day')
                """.strip(),
                {"$1": d1.isoformat()},
            )

    return Query("SELECT 0::bigint", {})