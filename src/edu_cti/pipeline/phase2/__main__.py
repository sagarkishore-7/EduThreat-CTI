"""
Phase 2: Enrichment Pipeline CLI

Main entry point for Phase 2 LLM enrichment pipeline.
Coordinates article fetching, education relevance checking, URL scoring,
and comprehensive CTI extraction.
"""

import argparse
import logging
import sys
import time
from typing import Dict, List, Optional

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.article_storage import ArticleProcessor
from src.edu_cti.pipeline.phase2.db import (
    get_unenriched_incidents,
    save_enrichment_result,
    mark_incident_skipped,
    get_enrichment_stats,
)
from src.edu_cti.pipeline.phase2.deduplication import deduplicate_by_institution
from src.edu_cti.pipeline.phase2.csv_export import export_enriched_dataset
from src.edu_cti.core.config import (
    DB_PATH,
    OLLAMA_API_KEY,
    OLLAMA_HOST,
    OLLAMA_MODEL,
    ENRICHMENT_BATCH_SIZE,
    ENRICHMENT_RATE_LIMIT_DELAY,
)
from src.edu_cti.core.logging_utils import configure_logging
from src.edu_cti.core.db import get_connection, init_db
from pathlib import Path


def dict_to_incident(incident_dict: Dict, conn) -> BaseIncident:
    """Convert incident dict to BaseIncident."""
    from src.edu_cti.core.db import get_incident_sources
    
    # Get source list to get primary source
    sources = get_incident_sources(conn, incident_dict["incident_id"])
    
    # Get primary source (first one seen)
    primary_source = sources[0]["source"] if sources else "unknown"
    primary_source_event_id = sources[0]["source_event_id"] if sources else None
    
    return BaseIncident(
        incident_id=incident_dict["incident_id"],
        source=primary_source,
        source_event_id=primary_source_event_id,
        title=incident_dict.get("title"),
        subtitle=incident_dict.get("subtitle"),
        university_name=incident_dict.get("university_name") or incident_dict.get("victim_raw_name") or "Unknown",
        victim_raw_name=incident_dict.get("victim_raw_name"),
        institution_type=incident_dict.get("institution_type"),
        country=incident_dict.get("country"),
        region=incident_dict.get("region"),
        city=incident_dict.get("city"),
        incident_date=incident_dict.get("incident_date"),
        date_precision=incident_dict.get("date_precision", "unknown"),
        source_published_date=incident_dict.get("source_published_date"),
        ingested_at=incident_dict.get("ingested_at"),
        all_urls=incident_dict.get("all_urls", []),
        primary_url=incident_dict.get("primary_url"),
        attack_type_hint=incident_dict.get("attack_type_hint"),
        status=incident_dict.get("status", "suspected"),
        source_confidence=incident_dict.get("source_confidence", "medium"),
        notes=incident_dict.get("notes"),
    )


def fetch_articles_phase(
    conn,
    unenriched: List[Dict],
    limit: Optional[int] = None,
    min_delay_seconds: float = 2.0,
    max_delay_seconds: float = 5.0,
) -> Dict[str, int]:
    """
    Phase 1: Fetch articles and store in database using smart fetching strategy.
    
    Uses domain-based rate limiting and random incident selection to avoid
    bot detection and ensure efficient fetching.
    
    Returns:
        Statistics dict with counts
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("PHASE 1: Smart Article Fetching (Domain-Based Rate Limiting)")
    logger.info("=" * 60)
    
    from src.edu_cti.pipeline.phase2.fetching_strategy import (
        SmartArticleFetchingStrategy,
        DomainRateLimiter,
    )
    from src.edu_cti.pipeline.phase2.article_fetcher import ArticleFetcher
    
    # Initialize rate limiter and fetching strategy
    rate_limiter = DomainRateLimiter(
        min_delay_seconds=min_delay_seconds,
        max_delay_seconds=max_delay_seconds,
        max_fetches_per_hour=10,  # Max 10 fetches per domain per hour
        block_duration_seconds=3600,  # Block for 1 hour if rate limit exceeded
    )
    
    article_fetcher = ArticleFetcher()
    fetching_strategy = SmartArticleFetchingStrategy(
        conn=conn,
        rate_limiter=rate_limiter,
        article_fetcher=article_fetcher,
    )
    
    stats = {
        "processed": 0,
        "articles_fetched": 0,
        "errors": 0,
    }
    
    # Get randomly selected incidents with domain diversity
    num_incidents = limit if limit else len(unenriched)
    incidents_to_process = fetching_strategy.get_random_incidents_for_enrichment(
        limit=num_incidents,
        exclude_domains=[],  # Can add recently blocked domains here
    )
    
    if not incidents_to_process:
        logger.warning("No incidents available for fetching")
        return stats
    
    logger.info(f"Selected {len(incidents_to_process)} incidents for fetching (random selection with domain diversity)")
    
    # Track which incident IDs we're processing (for later LLM enrichment)
    processing_incident_ids = fetching_strategy.get_processing_incident_ids()
    
    try:
        # Fetch articles for all incidents with domain-based rate limiting
        results = fetching_strategy.fetch_articles_for_incidents(incidents_to_process)
        
        # Count statistics
        stats["processed"] = len(incidents_to_process)
        stats["articles_fetched"] = sum(
            1 for incident_id, articles in results.items()
            if articles and any(article.fetch_successful for article in articles)
        )
        
        # Log results
        for incident_id, articles in results.items():
            successful_articles = [a for a in articles if a.fetch_successful]
            if successful_articles:
                logger.info(f"✓ Fetched {len(successful_articles)} article(s) for incident {incident_id}")
            else:
                logger.warning(f"⊘ No articles fetched for incident {incident_id}")
                stats["errors"] += 1
        
    except Exception as e:
        stats["errors"] += len(incidents_to_process)
        logger.error(f"✗ Error during article fetching: {e}", exc_info=True)
    
    logger.info("=" * 60)
    logger.info("Article Fetching Complete")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Articles Fetched: {stats['articles_fetched']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info(f"  Processing Incident IDs: {', '.join(sorted(processing_incident_ids)[:10])}{'...' if len(processing_incident_ids) > 10 else ''}")
    logger.info("=" * 60)
    
    return stats


def enrich_articles_phase(
    conn,
    enricher: IncidentEnricher,
    skip_if_not_education: bool = False,
    limit: Optional[int] = None,
    rate_limit_delay: float = 1.0,
) -> Dict[str, int]:
    """
    Phase 2: Sequential LLM enrichment (queue-based consumer).
    
    Processes incidents ONE AT A TIME, waiting for each LLM response before
    making the next call. This prevents rate limiting and ensures proper sequencing.
    
    Returns:
        Statistics dict with counts
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("PHASE 2: Sequential LLM Enrichment (Queue-Based Consumer)")
    logger.info("=" * 60)
    
    # Get incidents that have articles in DB but are not yet enriched
    from src.edu_cti.pipeline.phase2.article_storage import init_articles_table
    init_articles_table(conn)
    
    # Query for incidents with articles but not enriched
    query = """
        SELECT DISTINCT i.* FROM incidents i
        INNER JOIN articles a ON i.incident_id = a.incident_id
        WHERE i.llm_enriched = 0
          AND a.fetch_successful = 1
          AND a.content IS NOT NULL
          AND LENGTH(a.content) > 50
        ORDER BY i.ingested_at DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur = conn.execute(query)
    
    rows = cur.fetchall()
    incidents_to_enrich = []
    for row in rows:
        all_urls_str = row["all_urls"] or ""
        all_urls = [url.strip() for url in all_urls_str.split(";") if url.strip()]
        incident_dict = {
            "incident_id": row["incident_id"],
            "university_name": row["university_name"] or row["victim_raw_name"] or "Unknown",
            "victim_raw_name": row["victim_raw_name"],
            "institution_type": row["institution_type"],
            "country": row["country"],
            "region": row["region"],
            "city": row["city"],
            "incident_date": row["incident_date"],
            "date_precision": row["date_precision"] or "unknown",
            "source_published_date": row["source_published_date"],
            "ingested_at": row["ingested_at"],
            "title": row["title"],
            "subtitle": row["subtitle"],
            "primary_url": row["primary_url"],
            "all_urls": all_urls,
            "attack_type_hint": row["attack_type_hint"],
            "status": row["status"] or "suspected",
            "source_confidence": row["source_confidence"] or "medium",
            "notes": row["notes"],
        }
        incidents_to_enrich.append(incident_dict)
    
    if not incidents_to_enrich:
        logger.info("No incidents with articles ready for LLM enrichment")
        return {
            "processed": 0,
            "enriched": 0,
            "skipped": 0,
            "errors": 0,
        }
    
    logger.info(f"Processing {len(incidents_to_enrich)} incidents for LLM enrichment (SEQUENTIALLY)")
    
    stats = {
        "processed": 0,
        "enriched": 0,
        "skipped": 0,
        "errors": 0,
    }
    
    # SEQUENTIAL PROCESSING: One at a time, wait for each response
    for idx, incident_dict in enumerate(incidents_to_enrich, 1):
        incident_id = incident_dict["incident_id"]
        logger.info(f"[{idx}/{len(incidents_to_enrich)}] Processing incident: {incident_id}")
        
        try:
            # Convert to BaseIncident
            incident = dict_to_incident(incident_dict, conn=conn)
            
            # Process incident (reads articles from DB) - SEQUENTIAL, ONE AT A TIME
            # This is the queue-based consumer pattern - wait for each LLM response
            enrichment_result = enricher.process_incident(
                incident=incident,
                skip_if_not_education=skip_if_not_education,
                conn=conn,  # Required: pass connection to read articles from DB
            )
            
            if enrichment_result:
                # Save enrichment result
                saved = save_enrichment_result(conn, incident_id, enrichment_result)
                if saved:
                    stats["enriched"] += 1
                    logger.info(f"✓ Enriched incident: {incident_id}")
                else:
                    stats["skipped"] += 1
                    logger.info(f"⊘ Skipped enrichment upgrade for {incident_id} - lower confidence")
            else:
                # Mark as skipped
                reason = "Not education-related" if skip_if_not_education else "No enrichment result"
                mark_incident_skipped(conn, incident_id, reason)
                stats["skipped"] += 1
                logger.info(f"⊘ Skipped incident: {incident_id} - {reason}")
            
            stats["processed"] += 1
            
            # Rate limiting between LLM calls
            if rate_limit_delay > 0 and idx < len(incidents_to_enrich):
                logger.debug(f"Waiting {rate_limit_delay}s before next LLM call...")
                time.sleep(rate_limit_delay)
                
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"✗ Error processing incident {incident_id}: {e}", exc_info=True)
            # Don't mark as processed - will retry on next run
    
    logger.info("=" * 60)
    logger.info("LLM Enrichment Complete")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Enriched: {stats['enriched']}")
    logger.info(f"  Skipped: {stats['skipped']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("=" * 60)
    
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Phase 2: LLM enrichment pipeline for EduThreat-CTI.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run enrichment for all unenriched incidents
  python -m src.edu_cti.pipeline.phase2

  # Limit to first 10 incidents
  python -m src.edu_cti.pipeline.phase2 --limit 10

  # Process with custom batch size and rate limit
  python -m src.edu_cti.pipeline.phase2 --batch-size 5 --rate-limit-delay 3.0
        """,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of incidents to process (default: all unenriched)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=ENRICHMENT_BATCH_SIZE,
        help=f"Number of incidents to process per batch (default: {ENRICHMENT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--skip-non-education",
        action="store_true",
        default=True,
        help="Skip incidents not related to education (default: True)",
    )
    parser.add_argument(
        "--keep-non-education",
        action="store_false",
        dest="skip_non_education",
        help="Keep incidents even if not education-related",
    )
    parser.add_argument(
        "--rate-limit-delay",
        type=float,
        default=ENRICHMENT_RATE_LIMIT_DELAY,
        help=f"Delay between API calls in seconds (default: {ENRICHMENT_RATE_LIMIT_DELAY})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to log file (default: logs/pipeline.log)",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export enriched dataset to CSV after completion",
    )
    parser.add_argument(
        "--csv-output",
        type=Path,
        default=None,
        help="Path to output CSV file (default: data/processed/enriched_dataset.csv)",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for Phase 2 enrichment pipeline."""
    args = parse_args()
    
    # Setup logging
    from pathlib import Path as PathLib
    log_file_path = PathLib(args.log_file) if args.log_file else None
    configure_logging(args.log_level, log_file=log_file_path)
    logger = logging.getLogger(__name__)
    
    # Initialize database
    conn = get_connection()
    init_db(conn)  # Ensure tables exist
    
    # Get enrichment stats
    stats = get_enrichment_stats(conn)
    logger.info(f"Enrichment Statistics:")
    logger.info(f"  Total incidents: {stats['total_incidents']}")
    logger.info(f"  Already enriched: {stats['enriched_incidents']}")
    logger.info(f"  Unenriched: {stats['unenriched_incidents']}")
    logger.info(f"  Ready for enrichment: {stats['ready_for_enrichment']}")
    
    # Get unenriched incidents
    unenriched = get_unenriched_incidents(conn, limit=args.limit)
    
    if not unenriched:
        logger.info("No incidents ready for processing")
        conn.close()
        return
    
    # PHASE 1: Fetch and store articles
    logger.info(f"\n{'='*60}")
    logger.info(f"Starting Phase 2 Pipeline with {len(unenriched)} incidents")
    logger.info(f"{'='*60}\n")
    
    # Phase 1: Fetch articles using smart strategy with domain-based rate limiting
    fetch_stats = fetch_articles_phase(
        conn,
        unenriched,
        limit=args.limit,
        min_delay_seconds=2.0,
        max_delay_seconds=5.0,
    )
    
    # PHASE 2: Sequential LLM enrichment (one at a time, wait for response)
    if fetch_stats["articles_fetched"] > 0:
        # Initialize LLM enricher
        try:
            llm_client = OllamaLLMClient(
                api_key=OLLAMA_API_KEY,
                host=OLLAMA_HOST,
                model=OLLAMA_MODEL,
            )
            enricher = IncidentEnricher(llm_client=llm_client)
            logger.info(f"Initialized LLM client with model: {OLLAMA_MODEL}")
        except Exception as e:
            logger.error(f"Failed to initialize LLM client: {e}")
            logger.error("Make sure OLLAMA_API_KEY is set in environment")
            sys.exit(1)
        
        # Run sequential enrichment (one at a time, queue-based)
        enrich_stats = enrich_articles_phase(
            conn=conn,
            enricher=enricher,
            skip_if_not_education=args.skip_non_education,
            limit=args.limit,
            rate_limit_delay=args.rate_limit_delay,
        )
    else:
        logger.warning("No articles were fetched - skipping LLM enrichment phase")
        enrich_stats = {
            "processed": 0,
            "enriched": 0,
            "skipped": 0,
            "errors": 0,
        }
    
    # Final stats - combine both phases
    processed = fetch_stats["processed"]
    enriched = enrich_stats.get("enriched", 0)
    skipped = enrich_stats.get("skipped", 0)
    errors = fetch_stats.get("errors", 0) + enrich_stats.get("errors", 0)
    
    # Final pipeline stats
    logger.info("=" * 60)
    logger.info("Phase 2 Pipeline Complete")
    logger.info(f"  Articles Fetched: {fetch_stats['articles_fetched']}")
    logger.info(f"  LLM Enriched: {enriched}")
    logger.info(f"  Skipped: {skipped}")
    logger.info(f"  Errors: {errors}")
    logger.info("=" * 60)
    
    # Post-enrichment deduplication by institution name (if enrichment occurred)
    if enrich_stats.get("enriched", 0) > 0:
        logger.info("=" * 60)
        logger.info("Running post-enrichment deduplication by institution name...")
        try:
            dedup_stats = deduplicate_by_institution(conn, window_days=14)
            logger.info(f"Post-enrichment deduplication complete:")
            logger.info(f"  Checked: {dedup_stats['checked']}")
            logger.info(f"  Removed: {dedup_stats['removed']}")
            logger.info(f"  Remaining: {dedup_stats['remaining']}")
        except Exception as e:
            logger.error(f"Error during post-enrichment deduplication: {e}", exc_info=True)
    
    # Update final stats
    final_stats = get_enrichment_stats(conn)
    logger.info(f"\n{'='*60}")
    logger.info("Final Statistics:")
    logger.info(f"  Ready for enrichment: {final_stats['ready_for_enrichment']}")
    logger.info(f"  Enriched incidents: {final_stats['enriched_incidents']}")
    logger.info(f"{'='*60}")
    
    # Export enriched dataset to CSV if requested
    if args.export_csv or args.csv_output:
        logger.info("=" * 60)
        logger.info("Exporting enriched dataset to CSV...")
        try:
            output_path = export_enriched_dataset(output_path=args.csv_output)
            if output_path:
                logger.info(f"✓ Enriched dataset exported to: {output_path}")
            else:
                logger.info("No enriched incidents to export")
        except Exception as e:
            logger.error(f"Error exporting enriched dataset: {e}", exc_info=True)
    
    conn.close()


if __name__ == "__main__":
    main()
