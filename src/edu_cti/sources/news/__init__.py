"""
News/search feed ingestors for education-centric CTI collection.
These sources use keyword-based search to find education-related incidents.
"""

from .darkreading import build_darkreading_incidents
from .krebsonsecurity import build_krebsonsecurity_incidents
from .securityweek import build_securityweek_incidents
from .thehackernews import build_thehackernews_incidents
from .therecord import build_therecord_incidents

__all__ = [
    "build_darkreading_incidents",
    "build_krebsonsecurity_incidents",
    "build_securityweek_incidents",
    "build_thehackernews_incidents",
    "build_therecord_incidents",
]

