"""Date parsing helpers shared across ingestion and enrichment code."""

from __future__ import annotations

from datetime import datetime


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
