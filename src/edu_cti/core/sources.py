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
)

# Curated sources registry (sources with dedicated education sector endpoints/sections)
CURATED_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    "konbriefing": build_konbriefing_base_incidents,
    "ransomwarelive": build_ransomwarelive_incidents,
    "databreach": build_databreach_incidents,
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
}

# All sources (for reference)
ALL_SOURCES = {
    "curated": list(CURATED_SOURCE_REGISTRY.keys()),
    "news": list(NEWS_SOURCE_REGISTRY.keys()),
    "rss": list(RSS_SOURCE_REGISTRY.keys()),
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
    else:
        raise ValueError(f"Unknown group: {group}. Valid groups: curated, news, rss")
    
    invalid = [s for s in sources if s not in registry]
    if invalid:
        raise ValueError(
            f"Invalid {group} source names: {invalid}. "
            f"Valid sources: {list(registry.keys())}"
        )
    
    return list(dict.fromkeys(sources))  # Remove duplicates, preserve order

