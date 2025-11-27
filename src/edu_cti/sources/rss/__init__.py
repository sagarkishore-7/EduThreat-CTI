"""
RSS feed ingestion module for EduThreat-CTI.

This module handles RSS feed sources that provide real-time incident data.
RSS feeds are filtered by category (e.g., "Education Sector") or keywords.

Supported RSS Sources:
- DataBreaches.net: Filtered by "Education Sector" category
- BleepingComputer: Filtered by "Security" category + education keywords
"""

from src.edu_cti.sources.rss.databreaches_rss import build_databreaches_rss_incidents
from src.edu_cti.sources.rss.bleepingcomputer_rss import build_bleepingcomputer_rss_incidents

__all__ = [
    "build_databreaches_rss_incidents",
    "build_bleepingcomputer_rss_incidents",
]

