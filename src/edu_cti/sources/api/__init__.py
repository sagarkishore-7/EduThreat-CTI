"""
API-based ingestors: free REST APIs that require no web scraping.
"""

from .ransomwatch import build_ransomlook_incidents
from .ransomware_live import build_ransomwarelive_incidents

__all__ = [
    "build_ransomlook_incidents",
    "build_ransomwarelive_incidents",
]
