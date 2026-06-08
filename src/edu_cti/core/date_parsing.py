"""Date parsing helpers shared across ingestion and enrichment code."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional, Tuple


# dateutil does not understand many timezone abbreviations unless tzinfos is
# supplied. Keep this intentionally conservative: avoid highly ambiguous labels
# such as IST, but cover common article/RSS abbreviations that otherwise emit
# UnknownTimezoneWarning and may become hard failures in a future dateutil.
KNOWN_TZINFOS: dict[str, int] = {
    "UTC": 0,
    "GMT": 0,
    "BST": 3600,
    "CET": 3600,
    "CEST": 7200,
    "EET": 7200,
    "EEST": 10800,
    "WET": 0,
    "WEST": 3600,
    "EST": -5 * 3600,
    "EDT": -4 * 3600,
    "CST": -6 * 3600,
    "CDT": -5 * 3600,
    "MST": -7 * 3600,
    "MDT": -6 * 3600,
    "PST": -8 * 3600,
    "PDT": -7 * 3600,
    "AEST": 10 * 3600,
    "AEDT": 11 * 3600,
    "ACST": 9 * 3600 + 1800,
    "ACDT": 10 * 3600 + 1800,
    "AWST": 8 * 3600,
}


def parse_datetime_with_known_timezones(value: str, *, fuzzy: bool = False) -> datetime:
    """Parse a date string with known timezone abbreviations understood.

    Raises the same exceptions as ``dateutil.parser.parse`` when parsing fails.
    """
    from dateutil import parser as date_parser

    return date_parser.parse(value, fuzzy=fuzzy, tzinfos=KNOWN_TZINFOS)


# Two unrelated sentinel defaults. dateutil fills any component MISSING from the
# input with the supplied ``default``; by parsing twice with different defaults we
# can tell which components were actually present (equal across both parses) vs.
# silently defaulted (differ). This is what stops "August" / "Monday" / a bare
# time from being reported as the *current* date.
_SENTINEL_A = datetime(2222, 6, 5, 4, 3, 2)
_SENTINEL_B = datetime(3111, 7, 8, 9, 10, 11)

# Precision levels, ordered.
PRECISION_DAY = "day"
PRECISION_MONTH = "month_only"
PRECISION_YEAR = "year_only"


def parse_date_strict(
    value: object,
    *,
    fuzzy: bool = False,
) -> Tuple[Optional[date], Optional[str]]:
    """Parse a date string and report how precise it actually is.

    Returns ``(date, precision)`` where ``precision`` is one of ``"day"``,
    ``"month_only"`` or ``"year_only"`` reflecting which components were genuinely
    present in the input. If the **year** is not present in the string the result
    is ``(None, None)`` — we never invent a year (which is how dateutil's default
    silently produced "today"). ``(None, None)`` is also returned for empty or
    unparseable input.

    ``date``/``datetime`` inputs are passed through (precision ``"day"``).
    """
    if value is None:
        return None, None
    if isinstance(value, datetime):
        return value.date(), PRECISION_DAY
    if isinstance(value, date):
        return value, PRECISION_DAY

    text = str(value).strip()
    if not text:
        return None, None

    from dateutil import parser as date_parser

    try:
        a = date_parser.parse(text, default=_SENTINEL_A, fuzzy=fuzzy, tzinfos=KNOWN_TZINFOS)
        b = date_parser.parse(text, default=_SENTINEL_B, fuzzy=fuzzy, tzinfos=KNOWN_TZINFOS)
    except (ValueError, OverflowError, TypeError):
        return None, None

    year_present = a.year == b.year
    month_present = a.month == b.month
    day_present = a.day == b.day

    if not year_present:
        # No trustworthy year in the string — refuse to guess one.
        return None, None

    year = a.year
    # Sanity bound: reject absurd years that indicate a misparse.
    if year < 1990 or year > date.today().year + 1:
        return None, None

    if month_present and day_present:
        return date(year, a.month, a.day), PRECISION_DAY
    if month_present:
        return date(year, a.month, 1), PRECISION_MONTH
    return date(year, 1, 1), PRECISION_YEAR
