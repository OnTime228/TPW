from __future__ import annotations

import re


def extract_int(text: str) -> int | None:
    """Extract an integer from text.

    Supports:
      - "100000"
      - "100 000"
      - "100_000"
      - "100к" / "100 к" (thousands)
      - "2м" / "2 млн" (millions)

    Returns None if nothing found.
    """

    t = text.lower().replace("_", " ")

    # 1) patterns like "100 000" possibly with suffix
    m = re.search(r"(\d[\d\s]*\d|\d)\s*(к|k|тыс\.?|тысяч|м|млн\.?|миллион(?:а|ов)?)?", t)
    if not m:
        return None

    raw_num = re.sub(r"\s+", "", m.group(1))
    try:
        base = int(raw_num)
    except ValueError:
        return None

    suf = (m.group(2) or "").strip()
    if suf in {"к", "k", "тыс", "тыс.", "тысяч"}:
        return base * 1_000
    if suf in {"м", "млн", "млн.", "миллион", "миллиона", "миллионов"}:
        return base * 1_000_000

    return base
