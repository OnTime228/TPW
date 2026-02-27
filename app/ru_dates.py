from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone


# Month names in Russian (genitive + some common abbreviations)
_MONTHS: dict[str, int] = {
    "января": 1,
    "янв": 1,
    "февраля": 2,
    "фев": 2,
    "марта": 3,
    "мар": 3,
    "апреля": 4,
    "апр": 4,
    "мая": 5,
    "май": 5,
    "июня": 6,
    "июн": 6,
    "июля": 7,
    "июл": 7,
    "августа": 8,
    "авг": 8,
    "сентября": 9,
    "сен": 9,
    "сент": 9,
    "октября": 10,
    "окт": 10,
    "ноября": 11,
    "ноя": 11,
    "декабря": 12,
    "дек": 12,
}


@dataclass(frozen=True)
class DateRange:
    start: datetime  # inclusive
    end_exclusive: datetime


def _normalize(text: str) -> str:
    return text.lower().replace("ё", "е")


def _parse_month(month_raw: str) -> int:
    m = _normalize(month_raw)
    m = re.sub(r"[^а-я]", "", m)
    if m not in _MONTHS:
        raise ValueError(f"Unknown month: {month_raw}")
    return _MONTHS[m]


def _mk_dt(d: date) -> datetime:
    # All timestamps in the dataset are +00:00, so we use UTC boundaries.
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def extract_date_range(text: str) -> DateRange | None:
    """Extract a date range from Russian text.

    Supports:
      - "с 1 ноября 2025 по 5 ноября 2025 (включительно)"
      - "с 1 по 5 ноября 2025"
      - single date: "28 ноября 2025"

    Returns boundaries in UTC: [start, end_exclusive).
    """

    t = _normalize(text)

    # Range with 2 full dates
    m = re.search(
        r"с\s*(?P<d1>\d{1,2})\s+(?P<m1>[а-я]+)\s+(?P<y1>\d{4}).*?по\s*(?P<d2>\d{1,2})\s+(?P<m2>[а-я]+)\s+(?P<y2>\d{4})",
        t,
    )
    if m:
        d1 = int(m.group("d1"))
        d2 = int(m.group("d2"))
        m1 = _parse_month(m.group("m1"))
        m2 = _parse_month(m.group("m2"))
        y1 = int(m.group("y1"))
        y2 = int(m.group("y2"))
        start_d = date(y1, m1, d1)
        end_d = date(y2, m2, d2)
        return DateRange(start=_mk_dt(start_d), end_exclusive=_mk_dt(end_d + timedelta(days=1)))

    # Range like "с 1 по 5 ноября 2025"
    m = re.search(r"с\s*(?P<d1>\d{1,2})\s*по\s*(?P<d2>\d{1,2})\s+(?P<m>[а-я]+)\s+(?P<y>\d{4})", t)
    if m:
        d1 = int(m.group("d1"))
        d2 = int(m.group("d2"))
        mm = _parse_month(m.group("m"))
        yy = int(m.group("y"))
        start_d = date(yy, mm, d1)
        end_d = date(yy, mm, d2)
        return DateRange(start=_mk_dt(start_d), end_exclusive=_mk_dt(end_d + timedelta(days=1)))

    # Single date
    m = re.search(r"(?P<d>\d{1,2})\s+(?P<m>[а-я]+)\s+(?P<y>\d{4})", t)
    if m:
        dd = int(m.group("d"))
        mm = _parse_month(m.group("m"))
        yy = int(m.group("y"))
        day = date(yy, mm, dd)
        return DateRange(start=_mk_dt(day), end_exclusive=_mk_dt(day + timedelta(days=1)))

    return None
