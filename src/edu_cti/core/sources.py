"""
Source registry for EduThreat-CTI.

This module provides a centralized registry of all data sources,
making it easier to add new sources and maintain the codebase.

To add a new source:
1. Create the source builder function in the appropriate ingest module
2. Register it in the appropriate SOURCE_REGISTRY below
3. The pipeline will automatically pick it up
"""

from typing import Callable, Dict, List, Optional, Sequence

from src.edu_cti.core.models import BaseIncident

# Import curated source builders (sources with dedicated education sector sections)
from src.edu_cti.sources.curated import (
    build_konbriefing_base_incidents,
    build_ransomwarelive_incidents,
    build_databreach_incidents,
    build_comparitech_incidents,
)

# Import news source builders (keyword-based search sources)
from src.edu_cti.sources.news import (
    build_darkreading_incidents,
    build_krebsonsecurity_incidents,
    build_securityweek_incidents,
    build_thehackernews_incidents,
    build_therecord_incidents,
)

# Import RSS feed source builders
from src.edu_cti.sources.rss import (
    build_databreaches_rss_incidents,
    build_bleepingcomputer_rss_incidents,
    build_googlenews_rss_incidents,
    build_oxylabs_news_incidents,
)

# Import API-based source builders (free APIs, no scraping)
from src.edu_cti.sources.api.ransomwatch import build_ransomlook_incidents
from src.edu_cti.sources.api.cisa_kev import build_cisa_kev_incidents
from src.edu_cti.sources.api.otx_alienvault import build_otx_incidents
from src.edu_cti.sources.api.threatfox import build_threatfox_incidents
from src.edu_cti.sources.api.urlhaus import build_urlhaus_incidents

# Import new RSS sources
from src.edu_cti.sources.rss.cisa_rss import build_cisa_rss_incidents
from src.edu_cti.sources.rss.international_rss import build_international_rss_incidents

# Curated sources registry (sources with dedicated education sector endpoints/sections)
CURATED_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    "konbriefing": build_konbriefing_base_incidents,
    "ransomwarelive": build_ransomwarelive_incidents,
    "databreach": build_databreach_incidents,
    "comparitech": build_comparitech_incidents,
}

# News sources registry (keyword-based search sources)
NEWS_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    "krebsonsecurity": build_krebsonsecurity_incidents,
    "thehackernews": build_thehackernews_incidents,
    "therecord": build_therecord_incidents,
    "securityweek": build_securityweek_incidents,
    "darkreading": build_darkreading_incidents,
}

# RSS feed sources registry (real-time RSS feed sources)
RSS_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    "databreaches_rss": build_databreaches_rss_incidents,
    "bleepingcomputer": build_bleepingcomputer_rss_incidents,
    "cisa_rss": build_cisa_rss_incidents,
    "international_rss": build_international_rss_incidents,
    "googlenews_rss": build_googlenews_rss_incidents,
    "oxylabs_news": build_oxylabs_news_incidents,
}

# API-based sources registry (free APIs, no web scraping needed)
API_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    "ransomlook": build_ransomlook_incidents,
    "cisa_kev": build_cisa_kev_incidents,
    "otx_alienvault": build_otx_incidents,
    "threatfox": build_threatfox_incidents,
    "urlhaus": build_urlhaus_incidents,
}

# All sources (for reference)
ALL_SOURCES = {
    "curated": list(CURATED_SOURCE_REGISTRY.keys()),
    "news": list(NEWS_SOURCE_REGISTRY.keys()),
    "rss": list(RSS_SOURCE_REGISTRY.keys()),
    "api": list(API_SOURCE_REGISTRY.keys()),
}


def get_curated_sources() -> List[str]:
    """Get list of all registered curated source names."""
    return list(CURATED_SOURCE_REGISTRY.keys())


def get_news_sources() -> List[str]:
    """Get list of all registered news source names."""
    return list(NEWS_SOURCE_REGISTRY.keys())


def get_curated_builder(source_name: str) -> Optional[Callable[..., List[BaseIncident]]]:
    """Get the builder function for a curated source."""
    return CURATED_SOURCE_REGISTRY.get(source_name)


def get_news_builder(source_name: str) -> Optional[Callable[..., List[BaseIncident]]]:
    """Get the builder function for a news source."""
    return NEWS_SOURCE_REGISTRY.get(source_name)


def get_rss_sources() -> List[str]:
    """Get list of all registered RSS source names."""
    return list(RSS_SOURCE_REGISTRY.keys())


def get_all_source_names() -> List[str]:
    """Get list of all registered source names across all registries."""
    return (
        list(CURATED_SOURCE_REGISTRY.keys()) +
        list(NEWS_SOURCE_REGISTRY.keys()) +
        list(RSS_SOURCE_REGISTRY.keys())
    )


def get_rss_builder(source_name: str) -> Optional[Callable[..., List[BaseIncident]]]:
    """Get the builder function for an RSS source."""
    return RSS_SOURCE_REGISTRY.get(source_name)


def get_api_sources() -> List[str]:
    """Get list of all registered API source names."""
    return list(API_SOURCE_REGISTRY.keys())


def get_api_builder(source_name: str) -> Optional[Callable[..., List[BaseIncident]]]:
    """Get the builder function for an API source."""
    return API_SOURCE_REGISTRY.get(source_name)


def validate_sources(
    group: str,
    sources: Optional[Sequence[str]] = None,
) -> List[str]:
    """
    Validate source names for a given group.
    
    Args:
        group: Source group ("curated", "news", or "rss")
        sources: List of source names to validate
        
    Returns:
        List of valid source names
        
    Raises:
        ValueError: If any source name is invalid
    """
    if sources is None:
        return []
    
    if group == "curated":
        registry = CURATED_SOURCE_REGISTRY
    elif group == "news":
        registry = NEWS_SOURCE_REGISTRY
    elif group == "rss":
        registry = RSS_SOURCE_REGISTRY
    elif group == "api":
        registry = API_SOURCE_REGISTRY
    else:
        raise ValueError(f"Unknown group: {group}. Valid groups: curated, news, rss, api")
    
    invalid = [s for s in sources if s not in registry]
    if invalid:
        raise ValueError(
            f"Invalid {group} source names: {invalid}. "
            f"Valid sources: {list(registry.keys())}"
        )
    
    return list(dict.fromkeys(sources))  # Remove duplicates, preserve order

