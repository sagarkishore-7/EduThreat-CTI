"""
Smart article fetching strategy for Phase 2 enrichment.

Implements domain-based rate limiting and random incident selection
to avoid bot detection and ensure efficient article fetching.
"""

import random
import time
import logging
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse
import sqlite3

from src.edu_cti.core.db import get_connection
from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleFetcher, ArticleContent
from src.edu_cti.pipeline.phase2.storage.article_storage import (
    init_articles_table,
    save_article,
    article_exists,
)

logger = logging.getLogger(__name__)


class DomainRateLimiter:
    """
    Tracks and enforces rate limits per domain to avoid bot detection.
    
    Maintains:
    - Last fetch time per domain
    - Fetch counts per domain within time windows
    - Blocked domains (temporarily or permanently)
    """
    
    def __init__(
        self,
        min_delay_seconds: float = 2.0,
        max_delay_seconds: float = 5.0,
        max_fetches_per_hour: int = 10,
        block_duration_seconds: int = 3600,  # 1 hour
    ):
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.max_fetches_per_hour = max_fetches_per_hour
        self.block_duration_seconds = block_duration_seconds
        
        # Track last fetch time per domain
        self.domain_last_fetch: Dict[str, datetime] = {}
        
        # Track fetch counts per domain within time windows
        self.domain_fetch_counts: Dict[str, List[datetime]] = defaultdict(list)
        
        # Blocked domains (domain -> block_until_time)
        self.domain_blocks: Dict[str, datetime] = {}
        
        # Permanently blocked domains
        self.permanently_blocked: Set[str] = set()
    
    def extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove port if present
            if ':' in domain:
                domain = domain.split(':')[0]
            return domain
        except Exception as e:
            logger.warning(f"Error extracting domain from {url}: {e}")
            return ""
    
    def is_domain_blocked(self, domain: str) -> bool:
        """Check if domain is currently blocked."""
        if domain in self.permanently_blocked:
            return True
        
        if domain in self.domain_blocks:
            block_until = self.domain_blocks[domain]
            if datetime.utcnow() < block_until:
                return True
            else:
                # Block expired, remove it
                del self.domain_blocks[domain]
        
        return False
    
    def block_domain(self, domain: str, permanent: bool = False) -> None:
        """Block a domain temporarily or permanently."""
        if permanent:
            self.permanently_blocked.add(domain)
            logger.warning(f"Permanently blocked domain: {domain}")
        else:
            block_until = datetime.utcnow() + timedelta(seconds=self.block_duration_seconds)
            self.domain_blocks[domain] = block_until
            logger.warning(f"Temporarily blocked domain {domain} until {block_until}")
    
    def can_fetch_from_domain(self, domain: str) -> bool:
        """Check if we can fetch from this domain right now."""
        if not domain:
            return False
        
        # Check if blocked
        if self.is_domain_blocked(domain):
            return False
        
        # Check rate limit
        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)
        
        # Clean up old fetch times
        if domain in self.domain_fetch_counts:
            self.domain_fetch_counts[domain] = [
                fetch_time for fetch_time in self.domain_fetch_counts[domain]
                if fetch_time > one_hour_ago
            ]
        
        # Check if we've exceeded rate limit
        recent_fetches = len(self.domain_fetch_counts.get(domain, []))
        if recent_fetches >= self.max_fetches_per_hour:
            logger.debug(f"Rate limit exceeded for domain {domain} ({recent_fetches} fetches in last hour)")
            return False
        
        return True
    
    def wait_if_needed(self, domain: str) -> None:
        """Wait if necessary to respect rate limits."""
        if not domain:
            return
        
        if domain in self.domain_last_fetch:
            last_fetch = self.domain_last_fetch[domain]
            elapsed = (datetime.utcnow() - last_fetch).total_seconds()
            
            # Random delay between min and max
            delay = random.uniform(self.min_delay_seconds, self.max_delay_seconds)
            
            if elapsed < delay:
                wait_time = delay - elapsed
                logger.debug(f"Waiting {wait_time:.2f}s before fetching from {domain}")
                time.sleep(wait_time)
    
    def record_fetch(self, domain: str, success: bool = True) -> None:
        """Record a fetch attempt for a domain."""
        if not domain:
            return
        
        now = datetime.utcnow()
        self.domain_last_fetch[domain] = now
        
        if success:
            self.domain_fetch_counts[domain].append(now)
        else:
            # Multiple failures might indicate bot detection
            # Track failures and potentially block domain
            pass


class SmartArticleFetchingStrategy:
    """
    Smart article fetching strategy that:
    
    1. Randomly selects incidents to enrich
    2. Keeps track of incident IDs for later LLM enrichment
    3. Picks URLs from different domains (avoids same domain repeatedly)
    4. Implements domain-based rate limiting to prevent bot detection
    5. Organizes fetching efficiently
    """
    
    def __init__(
        self,
        conn: sqlite3.Connection,
        rate_limiter: Optional[DomainRateLimiter] = None,
        article_fetcher: Optional[ArticleFetcher] = None,
    ):
        self.conn = conn
        self.rate_limiter = rate_limiter or DomainRateLimiter()
        self.article_fetcher = article_fetcher or ArticleFetcher()
        
        # Track which incident IDs we're processing
        self.processing_incident_ids: Set[str] = set()
        
        # Track fetched URLs to avoid duplicates
        self.fetched_urls: Set[str] = set()
        
        init_articles_table(conn)
    
    def get_random_incidents_for_enrichment(
        self,
        limit: int,
        exclude_domains: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Get random incidents that need enrichment, prioritizing diversity in domains.
        
        Args:
            limit: Maximum number of incidents to return
            exclude_domains: List of domains to avoid (e.g., recently blocked)
            
        Returns:
            List of incident dictionaries with URLs
        """
        exclude_domains = exclude_domains or []
        
        # Get all unenriched incidents with URLs
        query = """
            SELECT 
                incident_id,
                all_urls,
                university_name,
                victim_raw_name,
                title,
                source_published_date
            FROM incidents
            WHERE llm_enriched = 0
              AND all_urls IS NOT NULL
              AND all_urls != ''
            ORDER BY RANDOM()
            LIMIT ?
        """
        
        cur = self.conn.execute(query, (limit * 3,))  # Get 3x more for filtering
        rows = cur.fetchall()
        
        if not rows:
            return []
        
        # Group by domain and select diverse incidents
        domain_incidents: Dict[str, List[Dict]] = defaultdict(list)
        no_domain_incidents: List[Dict] = []
        
        for row in rows:
            incident_id = row["incident_id"]
            all_urls_str = row["all_urls"] or ""
            all_urls = [url.strip() for url in all_urls_str.split(";") if url.strip()]
            
            if not all_urls:
                continue
            
            # Find first valid URL domain
            domain = None
            for url in all_urls:
                d = self.rate_limiter.extract_domain(url)
                if d and d not in exclude_domains:
                    if self.rate_limiter.can_fetch_from_domain(d):
                        domain = d
                        break
            
            incident_dict = {
                "incident_id": incident_id,
                "all_urls": all_urls,
                "university_name": row["university_name"] or row["victim_raw_name"] or "Unknown",
                "title": row["title"],
                "source_published_date": row["source_published_date"],
            }
            
            if domain:
                domain_incidents[domain].append(incident_dict)
            else:
                no_domain_incidents.append(incident_dict)
        
        # Select incidents: prioritize diversity across domains
        selected: List[Dict] = []
        selected_domains: Set[str] = set()
        
        # First pass: select one incident per domain (ensures diversity)
        for domain, incidents in domain_incidents.items():
            if len(selected) >= limit:
                break
            if domain not in selected_domains:
                incident = random.choice(incidents)
                selected.append(incident)
                selected_domains.add(domain)
        
        # Second pass: fill remaining slots randomly from any domain
        remaining = limit - len(selected)
        if remaining > 0:
            all_remaining = [
                inc for domain, incidents in domain_incidents.items()
                for inc in incidents if inc not in selected
            ] + no_domain_incidents
            
            if all_remaining:
                additional = random.sample(
                    all_remaining,
                    min(remaining, len(all_remaining))
                )
                selected.extend(additional)
        
        logger.info(
            f"Selected {len(selected)} incidents for fetching "
            f"(diversity: {len(selected_domains)} unique domains)"
        )
        
        return selected[:limit]
    
    def select_best_url_for_fetching(
        self,
        incident: Dict,
    ) -> Optional[str]:
        """
        Select the best URL to fetch for an incident.
        
        Prioritizes:
        1. URLs from domains we haven't fetched from recently
        2. URLs that aren't blocked
        3. URLs we haven't already fetched
        
        Args:
            incident: Incident dictionary with all_urls
            
        Returns:
            Best URL to fetch, or None if no suitable URL
        """
        all_urls = incident.get("all_urls", [])
        if not all_urls:
            return None
        
        # Score URLs by domain availability and freshness
        url_scores: List[Tuple[str, float]] = []
        
        for url in all_urls:
            # Skip if already fetched
            if url in self.fetched_urls:
                continue
            
            domain = self.rate_limiter.extract_domain(url)
            if not domain:
                continue
            
            # Check if domain is available
            if not self.rate_limiter.can_fetch_from_domain(domain):
                continue
            
            # Score: prefer domains we haven't used recently
            score = 1.0
            if domain in self.rate_limiter.domain_last_fetch:
                last_fetch = self.rate_limiter.domain_last_fetch[domain]
                hours_since = (datetime.utcnow() - last_fetch).total_seconds() / 3600
                score = min(1.0, hours_since / 24.0)  # Higher score if longer since last fetch
            
            url_scores.append((url, score))
        
        if not url_scores:
            return None
        
        # Select URL with highest score
        url_scores.sort(key=lambda x: x[1], reverse=True)
        return url_scores[0][0]
    
    def fetch_articles_for_incidents(
        self,
        incidents: List[Dict],
    ) -> Dict[str, List[ArticleContent]]:
        """
        Fetch articles for multiple incidents with domain-based rate limiting.
        
        Args:
            incidents: List of incident dictionaries to fetch articles for
            
        Returns:
            Dictionary mapping incident_id to list of ArticleContent objects
        """
        results: Dict[str, List[ArticleContent]] = {}
        
        # Track which incident IDs we're processing
        for incident in incidents:
            self.processing_incident_ids.add(incident["incident_id"])
        
        # Process incidents one by one with domain-based rate limiting
        for i, incident in enumerate(incidents, 1):
            incident_id = incident["incident_id"]
            all_urls = incident["all_urls"]
            
            logger.info(
                f"[{i}/{len(incidents)}] Fetching articles for incident {incident_id} "
                f"({len(all_urls)} URLs)"
            )
            
            incident_articles: List[ArticleContent] = []
            
            # Try to fetch from each URL, prioritizing different domains
            for url in all_urls:
                domain = self.rate_limiter.extract_domain(url)
                
                if not domain:
                    logger.debug(f"Skipping URL with invalid domain: {url}")
                    continue
                
                # Check if we can fetch from this domain
                if not self.rate_limiter.can_fetch_from_domain(domain):
                    logger.debug(f"Domain {domain} is blocked or rate-limited, skipping {url}")
                    continue
                
                # Check if already fetched
                if url in self.fetched_urls:
                    logger.debug(f"URL already fetched: {url}")
                    continue
                
                # Wait if needed to respect rate limits
                self.rate_limiter.wait_if_needed(domain)
                
                # Fetch article
                try:
                    logger.info(f"Fetching article from {domain}: {url}")
                    article_content = self.article_fetcher.fetch_article(url)
                    
                    # Record fetch attempt
                    success = article_content.fetch_successful
                    self.rate_limiter.record_fetch(domain, success=success)
                    
                    if success:
                        self.fetched_urls.add(url)
                        incident_articles.append(article_content)
                        
                        # Save to database
                        try:
                            save_article(
                                self.conn,
                                incident_id=incident_id,
                                url=url,
                                article=article_content,
                            )
                            logger.info(
                                f"✓ Successfully fetched and saved article from {domain} "
                                f"({len(article_content.content)} chars)"
                            )
                        except Exception as save_error:
                            logger.error(
                                f"✗ Failed to save article to database for {url}: {save_error}",
                                exc_info=True
                            )
                            # Still count as fetched even if save failed
                    else:
                        error_msg = article_content.error_message or "Unknown error"
                        content_len = article_content.content_length or 0
                        logger.warning(
                            f"✗ Failed to fetch from {domain}: {error_msg} "
                            f"(content_length: {content_len}, title: {article_content.title[:50] if article_content.title else 'None'})"
                        )
                        print(
                            f"[FETCH FAILED] {incident_id} | {domain} | {url} | "
                            f"Error: {error_msg} | Content length: {content_len}",
                            flush=True
                        )
                        
                        # If multiple failures from same domain, consider blocking
                        if "403" in error_msg or "Forbidden" in error_msg:
                            logger.warning(f"403 error from {domain}, may be blocked")
                    
                    # Small delay between URLs from same incident
                    time.sleep(random.uniform(0.5, 1.5))
                    
                except Exception as e:
                    logger.error(
                        f"✗ Exception while fetching article from {url}: {e}",
                        exc_info=True
                    )
                    print(
                        f"[FETCH EXCEPTION] {incident_id} | {domain} | {url} | "
                        f"Exception: {str(e)}",
                        flush=True
                    )
                    self.rate_limiter.record_fetch(domain, success=False)
            
            results[incident_id] = incident_articles
            
            if not incident_articles:
                logger.warning(
                    f"No articles fetched for incident {incident_id} "
                    f"(tried {len(all_urls)} URL(s))"
                )
                print(
                    f"[NO ARTICLES] {incident_id} | Tried {len(all_urls)} URL(s) | "
                    f"URLs: {', '.join(all_urls[:3])}{'...' if len(all_urls) > 3 else ''}",
                    flush=True
                )
            else:
                logger.info(
                    f"Fetched {len(incident_articles)} articles for incident {incident_id}"
                )
            
            # Delay between incidents
            if i < len(incidents):
                delay = random.uniform(1.0, 3.0)
                logger.debug(f"Waiting {delay:.2f}s before next incident...")
                time.sleep(delay)
        
        return results
    
    def get_processing_incident_ids(self) -> Set[str]:
        """Get set of incident IDs currently being processed."""
        return self.processing_incident_ids.copy()

