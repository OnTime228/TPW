from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Optional, Dict, Any, Tuple


@dataclass(frozen=True)
class Query:
    sql: str
    params: Dict[str, Any]


_RU_MONTHS = {
    "январ": 1,
    "феврал": 2,
    "март": 3,
    "апрел": 4,
    "ма": 5,        # май
    "июн": 6,       # июнь
    "июл": 7,       # июль
    "август": 8,
    "сентябр": 9,
    "октябр": 10,
    "ноябр": 11,
    "декабр": 12,
}


def _parse_int(text: str) -> Optional[int]:
    m = re.search(r"\b(\d{1,18})\b", text)
    return int(m.group(1)) if m else None


def _parse_date_yyyy_mm_dd(text: str) -> Optional[date]:
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _parse_date_ru(text: str) -> Optional[date]:
    # "28 ноября 2025"
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

    # "с 1 ноября 2025 по 5 ноября 2025"
    m = re.search(r"с\s+(.+?)\s+по\s+(.+?)(?:\b|$)", t)
    if m:
        d1 = _parse_date_ru(m.group(1)) or _parse_date_yyyy_mm_dd(m.group(1))
        d2 = _parse_date_ru(m.group(2)) or _parse_date_yyyy_mm_dd(m.group(2))
        return d1, d2

    # одиночная дата
    d = _parse_date_ru(t) or _parse_date_yyyy_mm_dd(t)
    if d:
        return d, d

    return None, None


def _extract_month_range(text: str) -> Optional[Tuple[date, date]]:
    """
    Понимает:
      - "за май 2025"
      - "в мае 2025"
      - "за май 2025 года"
      - "в июне 2025 года"
    Возвращает (start_date, end_date) как [start, end).
    """
    t = text.lower()

    # допускаем "года"
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

    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, end


def _parse_id_after_id_word(text: str) -> Optional[str]:
    """
    Достаёт id после слова "id" (UUID или число).
    Примеры:
      - "id 123"
      - "id: ecd8a4e4-1f24-..."
    """
    t = text.lower()
    m = re.search(r"\bid\s*[:=]?\s*([0-9a-f\-]{6,})\b", t)
    if m:
        return m.group(1)
    n = _parse_int(t)
    return str(n) if n is not None else None


def _is_total_question(t: str) -> bool:
    # “общее/всего/суммарное/в сумме” + “количество/сколько/сумма”
    return (
        ("общее" in t or "всего" in t or "суммар" in t or "в сумме" in t)
        and ("колич" in t or "сколько" in t or "сумм" in t)
    )


def nl_to_query(text: str) -> Query:
    t = " ".join((text or "").strip().lower().split())

    # ------------------------------------------------------------
    # 0) Month-based SUM (must be BEFORE global totals)
    # Example: "Какое суммарное количество просмотров ... в июне 2025 года?"
    mr = _extract_month_range(t)
    if mr and "видео" in t and _is_total_question(t):
        start, end = mr

        if "просмотр" in t:
            return Query(
                """
                SELECT COALESCE(SUM(views_count),0)::bigint
                FROM videos
                WHERE video_created_at >= $1::date
                  AND video_created_at <  $2::date
                """.strip(),
                {"$1": start, "$2": end},
            )

        if "лайк" in t:
            return Query(
                """
                SELECT COALESCE(SUM(likes_count),0)::bigint
                FROM videos
                WHERE video_created_at >= $1::date
                  AND video_created_at <  $2::date
                """.strip(),
                {"$1": start, "$2": end},
            )

        if "коммент" in t:
            return Query(
                """
                SELECT COALESCE(SUM(comments_count),0)::bigint
                FROM videos
                WHERE video_created_at >= $1::date
                  AND video_created_at <  $2::date
                """.strip(),
                {"$1": start, "$2": end},
            )

        if "жалоб" in t or "репорт" in t or "report" in t:
            return Query(
                """
                SELECT COALESCE(SUM(reports_count),0)::bigint
                FROM videos
                WHERE video_created_at >= $1::date
                  AND video_created_at <  $2::date
                """.strip(),
                {"$1": start, "$2": end},
            )

    # ------------------------------------------------------------
    # 1) Global totals (all time)
    if _is_total_question(t) and "видео" in t:
        if "лайк" in t:
            return Query("SELECT COALESCE(SUM(likes_count),0)::bigint FROM videos", {})
        if "просмотр" in t:
            return Query("SELECT COALESCE(SUM(views_count),0)::bigint FROM videos", {})
        if "коммент" in t:
            return Query("SELECT COALESCE(SUM(comments_count),0)::bigint FROM videos", {})
        if "жалоб" in t or "репорт" in t or "report" in t:
            return Query("SELECT COALESCE(SUM(reports_count),0)::bigint FROM videos", {})

    # ------------------------------------------------------------
    # 2) Count all videos
    if re.search(r"\bсколько\b.*\bвсего\b.*\bвидео\b", t) or t == "сколько видео":
        return Query("SELECT COUNT(*)::bigint FROM videos", {})

    # ------------------------------------------------------------
    # 3) Month-based COUNT
    # Example: "Сколько видео появилось на платформе за май 2025?"
    if mr and "сколько" in t and "видео" in t:
        start, end = mr
        # фразы “появилось/вышло/опубликовано/создано” могут быть, но не обязательно
        return Query(
            """
            SELECT COUNT(*)::bigint
            FROM videos
            WHERE video_created_at >= $1::date
              AND video_created_at <  $2::date
            """.strip(),
            {"$1": start, "$2": end},
        )

    # ------------------------------------------------------------
    # 4) Creator + explicit date range (inclusive)
    # Example: "Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
    if ("креатор" in t or "creator" in t) and "id" in t and "видео" in t and (
        "вышл" in t or "появ" in t or "опублик" in t or "создан" in t
    ):
        creator_id = _parse_id_after_id_word(t)
        d1, d2 = _extract_date_range(t)
        if creator_id and d1 and d2:
            return Query(
                """
                SELECT COUNT(*)::bigint
                FROM videos
                WHERE creator_id = $1
                  AND video_created_at >= $2::date
                  AND video_created_at < ($3::date + interval '1 day')
                """.strip(),
                {"$1": creator_id, "$2": d1, "$3": d2},
            )

    # ------------------------------------------------------------
    # 5) Views greater than X
    if "видео" in t and ("просмотр" in t or "просмотров" in t) and ("больше" in t or "более" in t):
        x = _parse_int(t)
        if x is not None:
            return Query("SELECT COUNT(*)::bigint FROM videos WHERE views_count > $1", {"$1": x})

    # ------------------------------------------------------------
    # 6) Sum delta views for a specific day (snapshots)
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
                {"$1": d1},
            )

    # ------------------------------------------------------------
    # 7) Count distinct videos with new views on a day
    if "сколько" in t and ("разных" in t or "различных" in t) and "видео" in t and (
        "новые просмотры" in t or "новых просмотров" in t
    ):
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
                {"$1": d1},
            )

    return Query("SELECT 0::bigint", {})