import json
import os
from importlib.resources import files
from pathlib import Path
import datetime
import re

from src.edu_cti_v2.env import get_env
from typing import Optional, Tuple, List


_ORDINAL_DAY_SUFFIX_RE = re.compile(r"(\d{1,2})(st|nd|rd|th)\b", re.IGNORECASE)


def _strip_ordinal_day_suffixes(raw: str) -> str:
    """Normalize ordinal day strings like 'January 8th, 2025' -> 'January 8, 2025'."""
    return _ORDINAL_DAY_SUFFIX_RE.sub(r"\1", raw)


def parse_date_with_precision(raw: str) -> Tuple[str, str]:
    """
    Parse many human-readable and machine date formats.
    Returns (yyyy-mm-dd or "", precision: day|month|year|unknown)
    
    Supports:
    - Human formats: "April 17, 2025", "17 April 2025", etc.
    - ISO 8601: "2025-11-19", "2025-11-19T11:23:06-05:00"
    - Month/year: "April 2025"
    - Year only: "2025"
    """
    if not raw:
        return "", "unknown"

    s = raw.replace("\xa0", " ").strip()
    s = _strip_ordinal_day_suffixes(s)
    
    # Handle ISO 8601 with timezone (e.g., "2025-11-19T11:23:06-05:00")
    # Extract just the date part before the 'T'
    if "T" in s:
        date_part = s.split("T")[0]
        try:
            dt = datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
            return dt.isoformat(), "day"
        except ValueError:
            pass

    # Day-level formats
    fmts_day = [
        "%B %d, %Y",   # April 17, 2025
        "%B %d %Y",    # April 17 2025
        "%b %d, %Y",   # Apr 17, 2025
        "%b %d %Y",    # Apr 17 2025
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
_EDU_KEYWORDS_CACHE_KEY: Optional[str] = None


def _default_edu_keywords_resource():
    return files("src.edu_cti").joinpath("config").joinpath("edu_keywords.json")


def load_edu_keywords(config_path: Optional[str] = None) -> List[str]:
    global _EDU_KEYWORDS_CACHE, _EDU_KEYWORDS_CACHE_KEY

    override_path = config_path or get_env("KEYWORDS_PATH", "EDU_CTI_KEYWORDS_PATH")
    cache_key = override_path or "package:src.edu_cti/config/edu_keywords.json"
    if _EDU_KEYWORDS_CACHE and _EDU_KEYWORDS_CACHE_KEY == cache_key:
        return _EDU_KEYWORDS_CACHE

    if override_path:
        config_file = Path(override_path)
        if not config_file.exists():
            _EDU_KEYWORDS_CACHE = []
            _EDU_KEYWORDS_CACHE_KEY = cache_key
            return _EDU_KEYWORDS_CACHE
        raw_text = config_file.read_text(encoding="utf-8")
    else:
        raw_text = _default_edu_keywords_resource().read_text(encoding="utf-8")

    data = json.loads(raw_text)
    all_terms: List[str] = []
    for _, terms in data.items():
        all_terms.extend([t.lower() for t in terms])

    # dedupe
    _EDU_KEYWORDS_CACHE = sorted(set(all_terms))
    _EDU_KEYWORDS_CACHE_KEY = cache_key
    return _EDU_KEYWORDS_CACHE


def is_edu_keyword_in_text(text: str) -> bool:
    if not text:
        return False
    keywords = load_edu_keywords()
    t = text.lower()
    return any(k in t for k in keywords)
