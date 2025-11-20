import json
from pathlib import Path
import datetime
from typing import Tuple, List


def parse_date_with_precision(raw: str) -> Tuple[str, str]:
    """
    Parse many human-readable date formats.
    Returns (yyyy-mm-dd or "", precision: day|month|year|unknown)
    """
    if not raw:
        return "", "unknown"

    s = raw.replace("\xa0", " ").strip()

    # Day-level formats
    fmts_day = [
        "%B %d, %Y",   # April 17, 2025
        "%b %d, %Y",   # Apr 17, 2025
        "%d %B %Y",    # 10 December 2021
        "%d %b %Y",    # 10 Dec 2021
        "%Y-%m-%d",    # 2025-08-11
    ]
    for fmt in fmts_day:
        try:
            dt = datetime.datetime.strptime(s, fmt).date()
            return dt.isoformat(), "day"
        except ValueError:
            pass

    # Month-year
    for fmt in ("%B %Y", "%b %Y"):
        try:
            dt = datetime.datetime.strptime(s, fmt)
            dt = dt.replace(day=1)
            return dt.date().isoformat(), "month"
        except ValueError:
            pass

    # Year only
    if s.isdigit() and len(s) == 4:
        try:
            dt = datetime.datetime.strptime(s, "%Y").replace(month=1, day=1)
            return dt.date().isoformat(), "year"
        except ValueError:
            pass

    return "", "unknown"


def now_utc_iso() -> str:
    """Return current UTC time as ISO8601 string with 'Z'."""
    return (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


# === NEW: multilingual EDU keywords ===

_EDU_KEYWORDS_CACHE: List[str] = []


def load_edu_keywords(config_path: str = "data/config/edu_keywords.json") -> List[str]:
    global _EDU_KEYWORDS_CACHE
    if _EDU_KEYWORDS_CACHE:
        return _EDU_KEYWORDS_CACHE

    p = Path(config_path)
    if not p.exists():
        _EDU_KEYWORDS_CACHE = []
        return _EDU_KEYWORDS_CACHE

    data = json.loads(p.read_text(encoding="utf-8"))
    all_terms: List[str] = []
    for _, terms in data.items():
        all_terms.extend([t.lower() for t in terms])

    # dedupe
    _EDU_KEYWORDS_CACHE = sorted(set(all_terms))
    return _EDU_KEYWORDS_CACHE


def is_edu_keyword_in_text(text: str) -> bool:
    if not text:
        return False
    keywords = load_edu_keywords()
    t = text.lower()
    return any(k in t for k in keywords)