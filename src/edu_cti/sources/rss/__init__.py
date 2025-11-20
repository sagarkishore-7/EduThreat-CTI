"""
RSS feed ingestion module for EduThreat-CTI.

This module handles RSS feed sources that provide real-time incident data.
RSS feeds are filtered by category (e.g., "Education Sector") or keywords.
"""

from src.edu_cti.sources.rss.databreaches_rss import build_databreaches_rss_incidents

__all__ = [
    "build_databreaches_rss_incidents",
]

