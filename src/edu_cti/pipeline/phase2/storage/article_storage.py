"""
Article storage and retrieval for Phase 2.

Handles fetching articles, scoring URLs, and storing articles in the database
for later LLM enrichment.
"""

import json
import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Dict, TYPE_CHECKING
from datetime import datetime

from src.edu_cti.core.db import get_connection
from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleFetcher, ArticleContent
# metadata_extractor removed - no longer used for scoring
from src.edu_cti.core.models import BaseIncident

# URL scoring removed - no longer needed

logger = logging.getLogger(__name__)


def init_articles_table(conn: sqlite3.Connection) -> None:
    """Initialize the articles table if it doesn't exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            incident_id TEXT NOT NULL,
            url TEXT NOT NULL,
            title TEXT,
            content TEXT NOT NULL,
            author TEXT,
            publish_date TEXT,
            fetch_successful INTEGER DEFAULT 1,
            fetch_error TEXT,
            content_length INTEGER DEFAULT 0,
            fetched_at TEXT NOT NULL,
            url_score REAL,
            url_score_reasoning TEXT,
            is_primary INTEGER DEFAULT 0,
            PRIMARY KEY (incident_id, url),
            FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_articles_incident ON articles(incident_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_articles_primary ON articles(incident_id, is_primary)
        """
    )
    conn.commit()


def save_article(
    conn: sqlite3.Connection,
    incident_id: str,
    url: str,
    article: ArticleContent,
    url_score: Optional[float] = None,
    url_score_reasoning: Optional[str] = None,
    is_primary: bool = False,
) -> None:
    """
    Save an article to the database.
    
    Args:
        conn: Database connection
        incident_id: Incident ID this article belongs to
        url: URL of the article
        article: ArticleContent object
        url_score: Optional URL confidence score (typically set by LLM during enrichment)
        url_score_reasoning: Optional reasoning for the score (typically set by LLM)
        is_primary: Whether this is the primary (best) article for this incident (set by LLM)
    """
    init_articles_table(conn)
    
    now = datetime.utcnow().isoformat() + "Z"
    
    # If marking as primary, unmark other articles for this incident
    if is_primary:
        conn.execute(
            "UPDATE articles SET is_primary = 0 WHERE incident_id = ?",
            (incident_id,)
        )
    
    conn.execute(
        """
        INSERT OR REPLACE INTO articles
        (incident_id, url, title, content, author, publish_date, fetch_successful,
         fetch_error, content_length, fetched_at, url_score, url_score_reasoning, is_primary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            incident_id,
            url,
            article.title or "",
            article.content or "",
            article.author,
            article.publish_date,
            1 if article.fetch_successful else 0,
            article.error_message,
            article.content_length,
            now,
            url_score,
            url_score_reasoning,
            1 if is_primary else 0,
        )
    )
    conn.commit()


# update_article_scores_from_llm removed - URL scoring no longer used in simplified pipeline


def cleanup_non_primary_articles(
    conn: sqlite3.Connection,
    incident_id: str,
) -> int:
    """
    Remove all non-primary articles for an incident after LLM enrichment.
    
    This keeps only the primary (best) article in the database to save storage space.
    All CTI information has already been extracted and stored in the enrichment results.
    
    Args:
        conn: Database connection
        incident_id: Incident ID to clean up
        
    Returns:
        Number of articles deleted
    """
    init_articles_table(conn)
    
    # Count non-primary articles before deletion
    cur = conn.execute(
        "SELECT COUNT(*) as count FROM articles WHERE incident_id = ? AND is_primary = 0",
        (incident_id,)
    )
    count_before = cur.fetchone()["count"]
    
    if count_before == 0:
        logger.debug(f"No non-primary articles to clean up for incident {incident_id}")
        return 0
    
    # Delete all non-primary articles
    conn.execute(
        "DELETE FROM articles WHERE incident_id = ? AND is_primary = 0",
        (incident_id,)
    )
    conn.commit()
    
    logger.info(
        f"Cleaned up {count_before} non-primary articles for incident {incident_id}. "
        f"Only primary article remains in database."
    )
    
    return count_before


def get_primary_article(
    conn: sqlite3.Connection,
    incident_id: str,
) -> Optional[Dict]:
    """
    Get the primary (best) article for an incident.
    
    Args:
        conn: Database connection
        incident_id: Incident ID
        
    Returns:
        Dictionary with article data, or None if not found
    """
    init_articles_table(conn)
    
    cur = conn.execute(
        """
        SELECT url, title, content, author, publish_date, fetch_successful,
               fetch_error, content_length, url_score, url_score_reasoning
        FROM articles
        WHERE incident_id = ? AND is_primary = 1
        LIMIT 1
        """,
        (incident_id,)
    )
    
    row = cur.fetchone()
    if not row:
        return None
    
    return {
        "url": row["url"],
        "title": row["title"],
        "content": row["content"],
        "author": row["author"],
        "publish_date": row["publish_date"],
        "fetch_successful": bool(row["fetch_successful"]),
        "error_message": row["fetch_error"],
        "content_length": row["content_length"],
        "url_score": row["url_score"],
        "url_score_reasoning": row["url_score_reasoning"],
    }


def get_all_articles_for_incident(
    conn: sqlite3.Connection,
    incident_id: str,
) -> List[Dict]:
    """
    Get all articles for an incident, sorted by URL score (highest first).
    
    Args:
        conn: Database connection
        incident_id: Incident ID
        
    Returns:
        List of article dictionaries
    """
    init_articles_table(conn)
    
    cur = conn.execute(
        """
        SELECT url, title, content, author, publish_date, fetch_successful,
               fetch_error, content_length, url_score, url_score_reasoning, is_primary
        FROM articles
        WHERE incident_id = ?
        ORDER BY url_score DESC NULLS LAST, fetched_at DESC
        """,
        (incident_id,)
    )
    
    rows = cur.fetchall()
    return [
        {
            "url": row["url"],
            "title": row["title"],
            "content": row["content"],
            "author": row["author"],
            "publish_date": row["publish_date"],
            "fetch_successful": bool(row["fetch_successful"]),
            "error_message": row["fetch_error"],
            "content_length": row["content_length"],
            "url_score": row["url_score"],
            "url_score_reasoning": row["url_score_reasoning"],
            "is_primary": bool(row["is_primary"]),
        }
        for row in rows
    ]


def article_exists(
    conn: sqlite3.Connection,
    incident_id: str,
    url: str,
) -> bool:
    """Check if an article already exists in the database."""
    init_articles_table(conn)
    
    cur = conn.execute(
        "SELECT 1 FROM articles WHERE incident_id = ? AND url = ?",
        (incident_id, url)
    )
    return cur.fetchone() is not None


class ArticleProcessor:
    """
    Processes articles: fetches, scores, and saves them to the database.
    
    This is the first phase of enrichment - separate from LLM enrichment.
    """
    
    def __init__(self):
        self.article_fetcher = ArticleFetcher()
    
    def process_incident_articles(
        self,
        conn: sqlite3.Connection,
        incident: BaseIncident,
    ) -> Optional[str]:
        """
        Fetch and score all articles for an incident, then save them to DB.
        
        Returns the URL of the best (primary) article, or None if no articles were fetched.
        
        Args:
            conn: Database connection
            incident: BaseIncident to process articles for
            
        Returns:
            Primary URL if successful, None otherwise
        """
        if not incident.all_urls:
            logger.warning(f"No URLs to fetch for incident {incident.incident_id}")
            return None
        
        logger.info(f"Fetching articles for {len(incident.all_urls)} URLs for incident {incident.incident_id}")
        
        # Fetch all articles
        article_contents = {}
        for url in incident.all_urls:
            # Skip if already fetched
            if article_exists(conn, incident.incident_id, url):
                logger.debug(f"Article already exists in DB: {url}")
                continue
            
            try:
                article = self.article_fetcher.fetch_article(url)
                article_contents[url] = article
                
                if article.fetch_successful and article.content:
                    logger.debug(f"Successfully fetched article from {url} ({len(article.content)} chars)")
                else:
                    logger.warning(f"Failed to fetch article from {url}: {article.error_message}")
            except Exception as e:
                logger.error(f"Error fetching article from {url}: {e}")
                article_contents[url] = ArticleContent(
                    url=url,
                    title="",
                    content="",
                    fetch_successful=False,
                    error_message=str(e),
                )
        
        # Filter to only successful fetches with content
        successful_articles = {
            url: article for url, article in article_contents.items()
            if article.fetch_successful and article.content and len(article.content.strip()) > 50
        }
        
        if not successful_articles:
            logger.warning(f"No valid articles fetched for incident {incident.incident_id}")
            return None
        
        # Save all articles to DB WITHOUT marking any as primary
        # The LLM will evaluate all articles and determine the best one during enrichment
        saved_count = 0
        for url, article in article_contents.items():
            # Only save if not already exists (skip if already fetched)
            if not article_exists(conn, incident.incident_id, url):
                save_article(
                    conn=conn,
                    incident_id=incident.incident_id,
                    url=url,
                    article=article,
                    url_score=None,  # Will be set by LLM during enrichment
                    url_score_reasoning=None,  # Will be set by LLM during enrichment
                    is_primary=False,  # Will be set by LLM after enrichment
                )
                saved_count += 1
                if article.fetch_successful:
                    logger.debug(f"Saved article: {url} ({len(article.content)} chars)")
        
        logger.info(
            f"Saved {saved_count} articles for incident {incident.incident_id} "
            f"({len(successful_articles)} successful). "
            f"Primary article will be selected by LLM during enrichment phase."
        )
        
        # Return first successful URL as temporary identifier
        # (actual primary will be selected by LLM)
        return list(successful_articles.keys())[0] if successful_articles else None
    
    def _score_urls_metadata_only(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> Dict[str, Dict]:
        """
        Score URLs using metadata coverage only (no LLM call).
        
        NOTE: This method is kept for reference but is no longer used.
        LLM-based scoring is now performed during the enrichment phase.
        
        Returns:
            Dictionary mapping URL to {"score": float, "reasoning": str}
        """
        scores = {}
        
        for url, article in article_contents.items():
            try:
                # Use metadata extractor to get objective coverage score
                coverage = self.metadata_extractor.analyze_coverage(
                    incident=incident,
                    article_content=article,
                )
                
                scores[url] = {
                    "score": coverage.coverage_score,
                    "reasoning": f"Metadata coverage: {coverage.coverage_score:.2f}. "
                               f"Covered fields: {len(coverage.covered_fields)}/{len(coverage.total_fields)}",
                }
            except Exception as e:
                logger.error(f"Error scoring URL {url}: {e}")
                scores[url] = {
                    "score": 0.0,
                    "reasoning": f"Error during scoring: {str(e)}",
                }
        
        return scores

