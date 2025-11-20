"""
Curated ingestors for sources with dedicated education sector sections.
These sources have specific endpoints/sections that contain only education-related incidents.
"""

from .konbriefing import build_konbriefing_base_incidents
from .ransomware_live import build_ransomwarelive_incidents
from .databreach import build_databreach_incidents

__all__ = [
    "build_konbriefing_base_incidents",
    "build_ransomwarelive_incidents",
    "build_databreach_incidents",
]

