from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from ru_dates import extract_date_range
from ru_numbers import extract_int

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BuiltQuery:
    sql: str
    args: tuple
    debug: str


def _normalize(text: str) -> str:
    return text.strip().lower().replace("ё", "е")


_CREATOR_ID_RE = re.compile(r"\b[a-f0-9]{32}\b")
_VIDEO_UUID_RE = re.compile(r"\b[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\b")


def _extract_creator_id(t: str) -> str | None:
    m = _CREATOR_ID_RE.search(t)
    return m.group(0) if m else None


def _extract_video_id(t: str) -> str | None:
    m = _VIDEO_UUID_RE.search(t)
    return m.group(0) if m else None


def _detect_metric(t: str) -> str | None:
    """Return one of: views/likes/comments/reports or None."""
    if "просмотр" in t:
        return "views"
    if "лайк" in t:
        return "likes"
    if "коммент" in t:
        return "comments"
    if "жалоб" in t or "репорт" in t or "report" in t:
        return "reports"
    return None


def _metric_col_final(metric: str) -> str:
    return {
        "views": "views_count",
        "likes": "likes_count",
        "comments": "comments_count",
        "reports": "reports_count",
    }[metric]


def _metric_col_delta(metric: str) -> str:
    return {
        "views": "delta_views_count",
        "likes": "delta_likes_count",
        "comments": "delta_comments_count",
        "reports": "delta_reports_count",
    }[metric]


def _detect_comparator(t: str) -> str | None:
    # order matters
    if "не меньше" in t or "как минимум" in t or "минимум" in t:
        return ">="
    if "не более" in t or "как максимум" in t or "максимум" in t:
        return "<="
    if "больше" in t or "выше" in t or "превыш" in t:
        return ">"
    if "меньше" in t or "ниже" in t:
        return "<"
    return None


class _SQLBuilder:
    def __init__(self):
        self.args: list = []

    def p(self, value) -> str:
        self.args.append(value)
        return f"${len(self.args)}"


def build_query(question: str) -> BuiltQuery:
    """Best-effort deterministic NL -> SQL builder.

    The checker expects one number as an answer, so this builder only supports
    COUNT / SUM / distinct-count patterns.

    If the query can't be parsed, raises ValueError.
    """

    t = _normalize(question)

    metric = _detect_metric(t)
    creator_id = _extract_creator_id(t)
    video_id = _extract_video_id(t)
    dr = extract_date_range(t)

    # Detect intent
    wants_sum_delta = any(k in t for k in ["вырос", "выросли", "прирост", "увелич", "прибав"])
    wants_distinct_new = (
        ("разн" in t or "уник" in t)
        and "видео" in t
        and ("нов" in t or "получ" in t)
    )

    # Some phrasings omit "разных", but still imply distinct videos
    wants_distinct_new = wants_distinct_new or (
        "сколько" in t and "видео" in t and ("получ" in t or "нов" in t) and metric is not None
    )

    comparator = _detect_comparator(t)
    threshold = extract_int(t) if comparator else None

    builder = _SQLBuilder()

    # 1) Sum of deltas (snapshots)
    if metric and wants_sum_delta:
        delta_col = _metric_col_delta(metric)

        join_videos = creator_id is not None
        if join_videos:
            sql = f"SELECT COALESCE(SUM(s.{delta_col}), 0) FROM video_snapshots s JOIN videos v ON v.id = s.video_id WHERE 1=1"
            sql += f" AND v.creator_id = {builder.p(creator_id)}"
        else:
            sql = f"SELECT COALESCE(SUM({delta_col}), 0) FROM video_snapshots WHERE 1=1"

        if video_id:
            if join_videos:
                sql += f" AND s.video_id = {builder.p(video_id)}"
            else:
                sql += f" AND video_id = {builder.p(video_id)}"

        if dr:
            col = "s.created_at" if join_videos else "created_at"
            sql += f" AND {col} >= {builder.p(dr.start)} AND {col} < {builder.p(dr.end_exclusive)}"

        return BuiltQuery(sql=sql, args=tuple(builder.args), debug="sum_delta")

    # 2) Distinct videos with new metric (delta > 0)
    if metric and wants_distinct_new:
        delta_col = _metric_col_delta(metric)

        join_videos = creator_id is not None
        if join_videos:
            sql = (
                f"SELECT COUNT(DISTINCT s.video_id) "
                f"FROM video_snapshots s JOIN videos v ON v.id = s.video_id "
                f"WHERE s.{delta_col} > 0 AND v.creator_id = {builder.p(creator_id)}"
            )
        else:
            sql = f"SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE {delta_col} > 0"

        if video_id:
            sql += f" AND {'s.video_id' if join_videos else 'video_id'} = {builder.p(video_id)}"

        if dr:
            col = "s.created_at" if join_videos else "created_at"
            sql += f" AND {col} >= {builder.p(dr.start)} AND {col} < {builder.p(dr.end_exclusive)}"

        return BuiltQuery(sql=sql, args=tuple(builder.args), debug="distinct_new")

    # 3) Count videos with final metric condition (videos table)
    if metric and comparator and threshold is not None and "видео" in t:
        col = _metric_col_final(metric)
        sql = f"SELECT COUNT(*) FROM videos WHERE {col} {comparator} {builder.p(int(threshold))}"

        if creator_id:
            sql += f" AND creator_id = {builder.p(creator_id)}"
        if video_id:
            sql += f" AND id = {builder.p(video_id)}"
        if dr:
            sql += f" AND video_created_at >= {builder.p(dr.start)} AND video_created_at < {builder.p(dr.end_exclusive)}"

        return BuiltQuery(sql=sql, args=tuple(builder.args), debug="count_videos_with_condition")

    # 4) Count videos (videos table)
    if "видео" in t and ("сколько" in t or "число" in t) and metric is None:
        sql = "SELECT COUNT(*) FROM videos WHERE 1=1"
        if creator_id:
            sql += f" AND creator_id = {builder.p(creator_id)}"
        if video_id:
            sql += f" AND id = {builder.p(video_id)}"
        if dr:
            sql += f" AND video_created_at >= {builder.p(dr.start)} AND video_created_at < {builder.p(dr.end_exclusive)}"
        return BuiltQuery(sql=sql, args=tuple(builder.args), debug="count_videos")

    # 5) Sum of final metric (videos table)
    if metric and (
        "всего" in t
        or "суммар" in t
        or "в сумме" in t
        or ("сколько" in t and "видео" not in t)
    ):
        col = _metric_col_final(metric)
        sql = f"SELECT COALESCE(SUM({col}), 0) FROM videos WHERE 1=1"
        if creator_id:
            sql += f" AND creator_id = {builder.p(creator_id)}"
        if video_id:
            sql += f" AND id = {builder.p(video_id)}"
        if dr:
            sql += f" AND video_created_at >= {builder.p(dr.start)} AND video_created_at < {builder.p(dr.end_exclusive)}"
        return BuiltQuery(sql=sql, args=tuple(builder.args), debug="sum_final")

    raise ValueError("Could not parse query")
