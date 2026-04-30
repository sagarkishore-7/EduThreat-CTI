"""
Curated ingestors for sources with dedicated education sector sections.
These sources have specific endpoints/sections that contain only education-related incidents.

Note: ransomware.live was moved to sources/api/ransomware_live.py (it is a REST API,
not an HTML-scraped curated site).  The import alias below is kept for backward
compatibility so any code still referencing this package doesn't break.
"""

from .konbriefing import build_konbriefing_base_incidents
from .databreach import build_databreach_incidents
from .comparitech import build_comparitech_incidents
from src.edu_cti.sources.api.ransomware_live import build_ransomwarelive_incidents  # re-export

__all__ = [
    "build_konbriefing_base_incidents",
    "build_ransomwarelive_incidents",
    "build_databreach_incidents",
    "build_comparitech_incidents",
]

