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
# NOTE: build_oxylabs_news_incidents is imported above but NOT registered in RSS_SOURCE_REGISTRY.
# It remains an opt-in paid discovery source. Article-fetch fallback through
# Oxylabs is configured independently from this source registry.

# Import API-based source builders (free APIs, no scraping)
from src.edu_cti.sources.api.ransomwatch import build_ransomlook_incidents
from src.edu_cti.sources.api.ransomware_live import build_ransomwarelive_incidents
# NOTE: threatfox, urlhaus, otx_alienvault, cisa_kev moved to sources/future_work/
# They provide IOC/malware hashes — not news articles about education sector incidents.
# Re-enable if you want to build a broader threat intelligence dataset in future.

# Import new RSS sources
from src.edu_cti.sources.rss.cisa_rss import build_cisa_rss_incidents
from src.edu_cti.sources.rss.international_rss import build_international_rss_incidents

# Curated sources registry (HTML-scraped sites with dedicated education sector sections)
CURATED_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    "konbriefing": build_konbriefing_base_incidents,
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

# RSS feed sources registry (free real-time RSS feed sources)
RSS_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    "databreaches_rss": build_databreaches_rss_incidents,
    "bleepingcomputer": build_bleepingcomputer_rss_incidents,
    "cisa_rss": build_cisa_rss_incidents,
    "international_rss": build_international_rss_incidents,
    "googlenews_rss": build_googlenews_rss_incidents,
}

# Paid RSS/search sources (not run in scheduled ingestion — used on-demand only)
# oxylabs_news: optional high-recall paid discovery with the same query-scoped
# semantics as Google News RSS; article fetching via Oxylabs is configured separately.
# Use OxylabsClient.search_news() directly in Phase 2 SERP fallback instead.
PAID_RSS_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    "oxylabs_news": build_oxylabs_news_incidents,
}

# API-based sources registry (free REST APIs, no web scraping needed)
API_SOURCE_REGISTRY: Dict[str, Callable[..., List[BaseIncident]]] = {
    "ransomlook": build_ransomlook_incidents,
    "ransomwarelive": build_ransomwarelive_incidents,
    # threatfox, urlhaus, otx_alienvault, cisa_kev removed — see sources/future_work/
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


def get_rss_sources(include_paid: bool = False) -> List[str]:
    """Get list of registered RSS source names, optionally including paid sources."""
    sources = list(RSS_SOURCE_REGISTRY.keys())
    if include_paid:
        sources.extend(PAID_RSS_SOURCE_REGISTRY.keys())
    return sources


def get_paid_rss_sources() -> List[str]:
    """Get list of paid RSS/search source names."""
    return list(PAID_RSS_SOURCE_REGISTRY.keys())


def get_all_source_names() -> List[str]:
    """Get list of all registered source names across all registries."""
    return (
        list(CURATED_SOURCE_REGISTRY.keys()) +
        list(NEWS_SOURCE_REGISTRY.keys()) +
        list(RSS_SOURCE_REGISTRY.keys())
    )


def get_rss_builder(
    source_name: str,
    include_paid: bool = False,
) -> Optional[Callable[..., List[BaseIncident]]]:
    """Get the builder function for an RSS source, optionally searching paid sources too."""
    builder = RSS_SOURCE_REGISTRY.get(source_name)
    if builder is None and include_paid:
        builder = PAID_RSS_SOURCE_REGISTRY.get(source_name)
    return builder


def get_api_sources() -> List[str]:
    """Get list of all registered API source names."""
    return list(API_SOURCE_REGISTRY.keys())


def get_api_builder(source_name: str) -> Optional[Callable[..., List[BaseIncident]]]:
    """Get the builder function for an API source."""
    return API_SOURCE_REGISTRY.get(source_name)


def validate_sources(
    group: str,
    sources: Optional[Sequence[str]] = None,
    include_paid: bool = False,
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
        registry = dict(RSS_SOURCE_REGISTRY)
        if include_paid:
            registry.update(PAID_RSS_SOURCE_REGISTRY)
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
