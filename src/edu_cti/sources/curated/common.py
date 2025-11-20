"""
Common utilities for curated sources.

Re-exports shared utilities from news.common for use in curated sources.
"""

from src.edu_cti.sources.news.common import (
    default_client,
    extract_date,
    fetch_html,
)

__all__ = [
    "default_client",
    "extract_date",
    "fetch_html",
]

