"""Backward-compatible environment-variable access.

The project is migrating from the long ``EDU_CTI_V2_`` / ``EDU_CTI_`` prefixes to
short, unprefixed names (e.g. ``WORKER_COUNT``, ``NEWS_MAX_PAGES``, ``DB_URL``).
To make the migration safe on a running deployment, every read goes through
``get_env`` which tries the new canonical name first and then any number of
legacy aliases, so the services keep working whether the Railway environment
still has the old names, already has the new ones, or a mix during cutover.

Usage::

    from src.edu_cti_v2.env import get_env, get_int, get_flag, get_optional_int

    db_url = get_env("DB_URL", "EDU_CTI_V2_DATABASE_URL", "DATABASE_URL")
    workers = get_env("WORKER_COUNT", "EDU_CTI_V2_WORKER_COUNT", default="auto")
    pages = get_int("NEWS_MAX_PAGES", "EDU_CTI_NEWS_MAX_PAGES", default=100)
    enabled = get_flag("OXYLABS_ENABLED", "EDU_CTI_OXYLABS_ENABLED", default=False)
"""

from __future__ import annotations

import os
from typing import Optional

_TRUTHY = {"1", "true", "yes", "on"}
_MISSING = object()


def get_env(name: str, *aliases: str, default: Optional[str] = None) -> Optional[str]:
    """Return the first non-empty value among ``name`` then ``aliases``, else ``default``.

    Empty / whitespace-only values are treated as unset so a blank override does
    not mask a legacy alias that still holds the real value.
    """
    for key in (name, *aliases):
        value = os.environ.get(key)
        if value is not None and value.strip() != "":
            return value
    return default


def get_int(name: str, *aliases: str, default: Optional[int] = None) -> Optional[int]:
    raw = get_env(name, *aliases)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def get_optional_int(name: str, *aliases: str) -> Optional[int]:
    return get_int(name, *aliases, default=None)


def get_float(name: str, *aliases: str, default: Optional[float] = None) -> Optional[float]:
    raw = get_env(name, *aliases)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default


def get_flag(name: str, *aliases: str, default: bool = False) -> bool:
    raw = get_env(name, *aliases)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY


def get_optional_flag(name: str, *aliases: str) -> Optional[bool]:
    raw = get_env(name, *aliases)
    if raw is None:
        return None
    return raw.strip().lower() in _TRUTHY
