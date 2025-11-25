"""
Storage module for Phase 2.

Contains database and article storage components:
- article_fetcher: Fetches article content from URLs
- article_storage: Stores and retrieves articles from database
- db: Database operations for enrichment results
"""

from .article_fetcher import ArticleFetcher, ArticleContent
from .article_storage import (
    ArticleProcessor,
    init_articles_table,
    save_article,
    article_exists,
    get_all_articles_for_incident,
    cleanup_non_primary_articles,
)
from .db import (
    init_incident_enrichments_table,
    get_unenriched_incidents,
    save_enrichment_result,
    get_enrichment_result,
    mark_incident_skipped,
    revert_enrichment_for_incident,
    revert_all_enriched_incidents,
    get_enrichment_stats,
)

__all__ = [
    'ArticleFetcher',
    'ArticleContent',
    'ArticleProcessor',
    'init_articles_table',
    'save_article',
    'article_exists',
    'get_all_articles_for_incident',
    'cleanup_non_primary_articles',
    'init_incident_enrichments_table',
    'get_unenriched_incidents',
    'save_enrichment_result',
    'get_enrichment_result',
    'mark_incident_skipped',
    'get_enrichment_stats',
]

