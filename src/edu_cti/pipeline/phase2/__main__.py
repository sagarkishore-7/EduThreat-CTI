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
import queue
import threading
from typing import Dict, List, Optional

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.storage.article_storage import ArticleProcessor
from src.edu_cti.pipeline.phase2.storage.db import (
    get_unenriched_incidents,
    save_enrichment_result,
    mark_incident_skipped,
    get_enrichment_stats,
)
from src.edu_cti.pipeline.phase2.utils.deduplication import deduplicate_by_institution
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

# Module-level cancel event — set by pipeline manager to request graceful stop
_cancel_event = threading.Event()

# Module-level progress dict — updated during execution, read by pipeline manager
_progress = {"step": "", "detail": "", "percent": 0}


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
    incident_queue: queue.Queue,
    limit: Optional[int] = None,
    min_delay_seconds: float = 2.0,
    max_delay_seconds: float = 5.0,
) -> Dict[str, int]:
    """
    Phase 1: Fetch articles and store in database using smart fetching strategy.
    
    Uses domain-based rate limiting and random incident selection to avoid
    bot detection and ensure efficient fetching.
    
    Pushes incidents to queue as soon as articles are fetched (producer pattern).
    
    Args:
        conn: Database connection
        unenriched: List of unenriched incidents
        incident_queue: Queue to push incidents with fetched articles
        limit: Maximum number of incidents to process
        min_delay_seconds: Minimum delay between fetches
        max_delay_seconds: Maximum delay between fetches
    
    Returns:
        Statistics dict with counts
    """
    logger = logging.getLogger(__name__)
    logger.info("PHASE 1: Article Fetching")
    
    from src.edu_cti.pipeline.phase2.utils.fetching_strategy import (
        SmartArticleFetchingStrategy,
        DomainRateLimiter,
    )
    from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleFetcher
    
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
    
    # If we got fewer incidents than requested due to domain filtering,
    # try again with less strict filtering
    if len(incidents_to_process) < num_incidents and num_incidents < len(unenriched):
        logger.warning(
            f"Only selected {len(incidents_to_process)}/{num_incidents} incidents due to domain filtering. "
            f"Retrying with less strict filtering..."
        )
        # Get more incidents, including those with potentially blocked domains
        additional_needed = num_incidents - len(incidents_to_process)
        additional = fetching_strategy.get_random_incidents_for_enrichment(
            limit=additional_needed * 2,  # Get 2x to account for filtering
            exclude_domains=[],  # Don't exclude any domains
        )
        # Add unique incidents
        existing_ids = {inc["incident_id"] for inc in incidents_to_process}
        for inc in additional:
            if inc["incident_id"] not in existing_ids:
                incidents_to_process.append(inc)
                if len(incidents_to_process) >= num_incidents:
                    break
    
    if not incidents_to_process:
        logger.warning("No incidents available for fetching")
        return stats
    
    logger.info(f"Selected {len(incidents_to_process)} incidents for fetching (random selection with domain diversity)")
    
    try:
        # Process incidents one by one and push to queue as soon as articles are fetched
        total_incidents = len(incidents_to_process)
        for idx, incident in enumerate(incidents_to_process, 1):
            # Check for cancellation
            if _cancel_event.is_set():
                logger.info(f"Cancel requested — stopping article fetching at [{idx}/{total_incidents}]")
                break

            incident_id = incident["incident_id"]
            progress_pct = (idx / total_incidents) * 100
            # Update module-level progress for pipeline manager
            _progress["step"] = "Fetching articles"
            _progress["detail"] = f"{idx}/{total_incidents}"
            _progress["percent"] = 0  # Stay at 0% during fetch; enrichment drives the percent
            if idx % 10 == 0 or idx == total_incidents:  # Log every 10th or last
                logger.info(f"Fetching [{idx}/{total_incidents}] ({progress_pct:.1f}%)")
            
            try:
                # Check if incident already has articles in database
                from src.edu_cti.pipeline.phase2.storage.article_storage import get_all_articles_for_incident
                existing_articles = get_all_articles_for_incident(conn, incident_id)
                has_existing_articles = len(existing_articles) > 0
                
                # Fetch articles for this single incident
                results = fetching_strategy.fetch_articles_for_incidents([incident])
                
                articles = results.get(incident_id, [])
                successful_articles = [a for a in articles if a.fetch_successful]
                failed_articles = [a for a in articles if not a.fetch_successful]
                
                # Mark broken URLs in database
                if failed_articles:
                    from src.edu_cti.core.db import mark_urls_as_broken
                    broken_urls = [a.url for a in failed_articles if a.url]
                    if broken_urls:
                        mark_urls_as_broken(conn, incident_id, broken_urls)
                        logger.info(f"Marked {len(broken_urls)} URL(s) as broken for incident {incident_id}")
                        conn.commit()
                
                # Determine if we should push to queue:
                # 1. If we fetched new successful articles, OR
                # 2. If incident already has articles in database (from previous runs)
                should_push_to_queue = False
                
                if successful_articles:
                    stats["articles_fetched"] += 1
                    logger.info(f"Fetched {len(successful_articles)} articles for {incident_id}")
                    
                    # Clear broken status for successfully fetched URLs
                    from src.edu_cti.core.db import clear_broken_urls
                    successful_urls = [a.url for a in successful_articles if a.url]
                    if successful_urls:
                        clear_broken_urls(conn, incident_id, successful_urls)
                        conn.commit()
                    
                    should_push_to_queue = True
                elif has_existing_articles:
                    # Incident already has articles in DB from previous fetch attempts
                    logger.info(f"Incident {incident_id} has {len(existing_articles)} articles in DB")
                    should_push_to_queue = True
                else:
                    # All fetch methods failed and no existing articles.
                    # Check if incident has structured metadata (e.g. Comparitech
                    # provides ransomware strain, ransom amount in notes) — if so,
                    # keep it for metadata-only enrichment.
                    has_metadata = bool(incident.get("notes") or incident.get("attack_type_hint"))
                    if has_metadata:
                        logger.info(f"No articles for {incident_id} but has metadata — keeping for metadata-only enrichment")
                        should_push_to_queue = True
                        stats["errors"] += 1
                    else:
                        # Dead URLs with no metadata — delete to keep DB clean
                        from src.edu_cti.pipeline.phase2.storage.db import delete_incident
                        if delete_incident(conn, incident_id):
                            stats["errors"] += 1
                            logger.info(f"Deleted unfetchable incident {incident_id} — all URLs dead")
                        else:
                            stats["errors"] += 1
                            logger.warning(f"Failed to delete unfetchable incident {incident_id}")
                        should_push_to_queue = False

                # Push incident to queue for enrichment (with or without articles)
                if should_push_to_queue:
                    # Push incident to queue immediately after fetching articles
                    # This allows enrichment to start processing while we continue fetching
                    incident_dict = {
                        "incident_id": incident_id,
                        "university_name": incident.get("university_name") or incident.get("victim_raw_name") or "Unknown",
                        "victim_raw_name": incident.get("victim_raw_name"),
                        "institution_type": None,  # Will be read from DB
                        "country": None,  # Will be read from DB
                        "region": None,  # Will be read from DB
                        "city": None,  # Will be read from DB
                        "incident_date": None,  # Will be read from DB
                        "date_precision": "unknown",  # Will be read from DB
                        "source_published_date": incident.get("source_published_date"),
                        "ingested_at": None,  # Will be read from DB
                        "title": incident.get("title"),
                        "subtitle": None,  # Will be read from DB
                        "primary_url": None,  # Will be read from DB
                        "all_urls": incident.get("all_urls", []),
                        "attack_type_hint": None,  # Will be read from DB
                        "status": "suspected",  # Will be read from DB
                        "source_confidence": "medium",  # Will be read from DB
                        "notes": None,  # Will be read from DB
                    }
                    
                    # Push to queue - enrichment consumer will pick it up
                    incident_queue.put(incident_dict)
                    logger.debug(f"Pushed {incident_id} to queue (size: {incident_queue.qsize()})")
                
                stats["processed"] += 1
                
            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error fetching articles for {incident_id}: {e}")
        
    except Exception as e:
        stats["errors"] += len(incidents_to_process)
        logger.error(f"Error during article fetching: {e}")
    
    logger.info(f"Fetching complete: {stats['processed']} processed, {stats['articles_fetched']} fetched, {stats['errors']} errors")
    
    return stats


def enrich_articles_phase(
    conn,
    enricher: IncidentEnricher,
    incident_queue: queue.Queue,
    fetch_complete_event: threading.Event,
    skip_if_not_education: bool = False,
    rate_limit_delay: float = 1.0,
    total_expected: int = 0,
    cancel_event: Optional[threading.Event] = None,
) -> Dict[str, int]:
    """
    Phase 2: Sequential LLM enrichment (queue-based consumer).
    
    Consumes incidents from queue and processes them ONE AT A TIME, waiting for
    each LLM response before processing the next. This prevents rate limiting and
    ensures proper sequencing.
    
    Args:
        conn: Database connection
        enricher: IncidentEnricher instance
        incident_queue: Queue to consume incidents from
        fetch_complete_event: Event to signal when fetching is complete
        skip_if_not_education: Whether to skip non-education incidents
        rate_limit_delay: Delay between LLM calls
        total_expected: Total number of incidents expected (for progress tracking)
    
    Returns:
        Statistics dict with counts
    """
    logger = logging.getLogger(__name__)
    logger.info("PHASE 2: LLM Enrichment")
    
    # Initialize articles table
    from src.edu_cti.pipeline.phase2.storage.article_storage import init_articles_table
    init_articles_table(conn)
    
    stats = {
        "processed": 0,
        "enriched": 0,
        "skipped": 0,
        "errors": 0,
    }
    
    # SENTINEL value to signal end of queue
    SENTINEL = None
    
    logger.debug(f"Queue initial state: empty={incident_queue.empty()}, size={incident_queue.qsize()}")
    
    # CONSUMER LOOP: Process incidents from queue as they arrive
    items_processed = 0
    while True:
        # Check for cancellation before processing next incident
        if cancel_event and cancel_event.is_set():
            logger.info(f"Cancel requested — stopping enrichment (processed {items_processed} items)")
            break

        try:
            # Get incident from queue (blocks until available or timeout)
            # Use timeout to periodically check if fetching is complete
            try:
                incident_dict = incident_queue.get(timeout=5.0)
            except queue.Empty:
                # Check if fetching is complete
                if cancel_event and cancel_event.is_set():
                    logger.info(f"Cancel requested during queue wait — stopping enrichment")
                    break
                if fetch_complete_event.is_set():
                    # Fetch is done — but queue may still have items being processed by other workers.
                    # Use queue.unfinished_tasks (via join with timeout) to check if ALL work is done,
                    # not just that the queue is momentarily empty.
                    # Wait with exponential backoff: 5s, 10s, 20s, 30s, 30s (total ~95s max)
                    found_items = False
                    for check_attempt in range(5):
                        wait_time = min(5 * (2 ** check_attempt), 30)
                        logger.debug(f"Fetch complete, queue empty — waiting {wait_time}s (attempt {check_attempt + 1}/5)...")
                        try:
                            incident_dict = incident_queue.get(timeout=wait_time)
                            found_items = True
                            logger.info(f"Found item after wait (attempt {check_attempt + 1}), continuing processing...")
                            break
                        except queue.Empty:
                            # Check if queue has any unfinished tasks (other workers still processing)
                            if incident_queue.unfinished_tasks > 0:
                                logger.debug(f"Queue empty but {incident_queue.unfinished_tasks} unfinished tasks — other workers still active, waiting...")
                                continue
                            # Queue is truly empty and no unfinished work
                            continue

                    if not found_items:
                        # Confirm: fetch done, queue empty, no unfinished tasks after extended wait
                        if incident_queue.unfinished_tasks > 0:
                            logger.debug(f"Still {incident_queue.unfinished_tasks} unfinished tasks — continuing to wait...")
                            continue  # Go back to the main while loop and try queue.get again
                        logger.info(f"Queue confirmed empty after extended wait - stopping consumer (processed {items_processed} items)")
                        break
                # Continue waiting
                logger.debug(f"Queue empty, waiting... (processed {items_processed} items so far)")
                continue
            
            # Check for sentinel (end of queue)
            if incident_dict is SENTINEL:
                logger.info("Received sentinel - stopping consumer")
                break
            
            incident_id = incident_dict["incident_id"]
            stats["processed"] += 1

            # Calculate progress percentage - log every 10th or at milestones
            if total_expected > 0:
                progress_pct = (stats["processed"] / total_expected) * 100
                # Update module-level progress for pipeline manager
                _progress["step"] = "LLM Enrichment"
                _progress["detail"] = f"{stats['processed']}/{total_expected} ({stats.get('enriched', 0)} enriched)"
                _progress["percent"] = int(progress_pct)  # Linear 0-100% based on enrichment completion
                if stats["processed"] % 10 == 0 or progress_pct % 10 < 1:  # Every 10 or every 10%
                    logger.info(f"Enriching [{stats['processed']}/{total_expected}] ({progress_pct:.1f}%)")
            else:
                if stats["processed"] % 10 == 0:
                    logger.info(f"Enriching [{stats['processed']}]")
            
            try:
                # Read full incident data from DB (queue only has minimal data)
                query = """
                    SELECT * FROM incidents WHERE incident_id = ?
                """
                cur = conn.execute(query, (incident_id,))
                row = cur.fetchone()
                
                if not row:
                    logger.warning(f"Incident {incident_id} not found in database")
                    stats["errors"] += 1
                    incident_queue.task_done()
                    continue
                
                # Check if already enriched (shouldn't happen, but check anyway)
                if row["llm_enriched"] == 1:
                    logger.warning(f"Incident {incident_id} is already enriched - skipping")
                    stats["skipped"] += 1
                    incident_queue.task_done()
                    continue
                
                # Build full incident dict from DB
                all_urls_str = row["all_urls"] or ""
                all_urls = [url.strip() for url in all_urls_str.split(";") if url.strip()]
                full_incident_dict = {
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
                
                # Convert to BaseIncident
                incident = dict_to_incident(full_incident_dict, conn=conn)
                
                # Process incident (reads articles from DB) - SEQUENTIAL, ONE AT A TIME
                logger.debug(f"Processing {incident_id}")
                try:
                    enrichment_result = enricher.process_incident(
                        incident=incident,
                        skip_if_not_education=skip_if_not_education,
                        conn=conn,  # Required: pass connection to read articles from DB
                    )
                    logger.debug(f"process_incident returned: {type(enrichment_result)}, is None: {enrichment_result is None}")
                except Exception as e:
                    # Check if it's a rate limit error that should stop enrichment
                    from src.edu_cti.pipeline.phase2.llm_client import RateLimitError
                    if isinstance(e, RateLimitError):
                        logger.warning(f"Rate limit error in process_incident for {incident_id}: {e}")
                        wait_time = 60.0
                        logger.info(f"Waiting {wait_time}s before retrying due to rate limit...")
                        time.sleep(wait_time)
                        stats["errors"] += 1
                        incident_queue.task_done()
                        incident_queue.put(incident_dict)
                        continue
                    elif isinstance(e, (TimeoutError, OSError)):
                        # LLM request timed out or connection error — re-queue for retry
                        logger.warning(f"Timeout/connection error for {incident_id}: {e} — will retry")
                        stats["errors"] += 1
                        incident_queue.task_done()
                        incident_queue.put(incident_dict)
                        time.sleep(5.0)  # Brief pause before retry
                        continue
                    else:
                        logger.error(f"Error in process_incident for {incident_id}: {e}", exc_info=True)
                        stats["errors"] += 1
                        incident_queue.task_done()
                        continue
                
                # process_incident returns tuple (enrichment_result, raw_json_data)
                if isinstance(enrichment_result, tuple):
                    enrichment_result, raw_json_data = enrichment_result
                else:
                    raw_json_data = None
                
                if enrichment_result:
                    # Save enrichment result (with raw JSON data for country/region/city extraction)
                    saved = save_enrichment_result(conn, incident_id, enrichment_result, raw_json_data=raw_json_data)
                    if saved:
                        # Commit immediately after each save to prevent long-running transactions
                        # This allows API reads to proceed while enrichment is running
                        conn.commit()
                        stats["enriched"] += 1
                        logger.info(f"Enriched {incident_id}")
                    else:
                        stats["skipped"] += 1
                        logger.warning(f"Skipped {incident_id} - lower confidence")
                else:
                    # Check what kind of failure we have
                    if raw_json_data and isinstance(raw_json_data, dict):
                        if raw_json_data.get("_not_education_related"):
                            # Not education-related — delete from DB entirely
                            from src.edu_cti.pipeline.phase2.storage.db import delete_incident
                            reason = raw_json_data.get("_reason", "Not education-related")
                            if delete_incident(conn, incident_id):
                                stats["skipped"] += 1
                                logger.info(f"Deleted non-education incident {incident_id}: {reason[:80]}")
                            else:
                                stats["errors"] += 1
                                logger.warning(f"Failed to delete non-education incident {incident_id}")
                        elif raw_json_data.get("_enrichment_failed"):
                            # Enrichment failed (JSON parsing, etc.) - DON'T mark as skipped, will retry
                            reason = raw_json_data.get("_reason", "Enrichment failed")
                            stats["errors"] += 1
                            logger.warning(f"Enrichment failed for {incident_id}: {reason[:100]}")
                        else:
                            # Unknown error in raw_json_data - don't mark as skipped
                            stats["errors"] += 1
                            logger.warning(f"Unknown enrichment result for {incident_id}")
                    else:
                        # No enrichment result and no error info — do NOT mark as
                        # enriched.  This typically means the article could not be
                        # fetched or content was too short.  Leaving llm_enriched=0
                        # allows the incident to be retried on the next run (e.g.
                        # after Zyte API is configured or the site becomes reachable).
                        stats["errors"] += 1
                        logger.warning(f"No enrichment result for {incident_id} — will retry on next run")
                
                # Rate limiting between LLM calls
                if rate_limit_delay > 0:
                    logger.debug(f"Waiting {rate_limit_delay}s before next LLM call...")
                    time.sleep(rate_limit_delay)
                
                # Mark task as done
                incident_queue.task_done()
                items_processed += 1
                
            except Exception as e:
                stats["errors"] += 1
                logger.error(f"✗ Error processing incident {incident_id}: {e}", exc_info=True)
                incident_queue.task_done()
                items_processed += 1
                # Don't mark as processed - will retry on next run
                
        except Exception as e:
            logger.error(f"✗ Error in consumer loop: {e}", exc_info=True)
            stats["errors"] += 1
    
    logger.info(f"Enrichment complete: {stats['processed']} processed, {stats['enriched']} enriched, {stats['skipped']} skipped, {stats['errors']} errors")
    
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

  # Use 3 parallel consumer threads for faster enrichment
  python -m src.edu_cti.pipeline.phase2 --workers 3 --rate-limit-delay 0.5
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
        "--workers",
        type=int,
        default=1,
        help="Number of parallel enrichment consumer threads (default: 1). "
             "Higher values speed up enrichment but use more API quota.",
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
    
    # PHASE 1 & 2: Concurrent producer-consumer pattern
    logger.info(f"\n{'='*60}")
    logger.info(f"Starting Phase 2 Pipeline with {len(unenriched)} incidents")
    logger.info(f"Using concurrent producer-consumer pattern (fetching + enrichment)")
    logger.info(f"{'='*60}\n")
    
    # Initialize LLM enricher (needed for consumer thread)
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
    
    # Create queue and synchronization primitives
    incident_queue = queue.Queue()
    fetch_complete_event = threading.Event()
    fetch_stats = {"processed": 0, "articles_fetched": 0, "errors": 0}

    # Total incidents to process (for progress tracking)
    total_to_process = len(unenriched)
    num_workers = max(1, min(args.workers, 8))  # Cap at 8 workers

    # Aggregated stats across all consumer threads (thread-safe)
    stats_lock = threading.Lock()
    combined_enrich_stats = {"processed": 0, "enriched": 0, "skipped": 0, "errors": 0}

    def consumer_thread(worker_id: int):
        """Consumer thread that processes incidents from queue."""
        # Each worker gets its own DB connection and LLM client
        thread_conn = get_connection()
        try:
            if num_workers > 1:
                # Each worker needs its own LLM client for thread safety
                worker_llm = OllamaLLMClient(
                    api_key=OLLAMA_API_KEY,
                    host=OLLAMA_HOST,
                    model=OLLAMA_MODEL,
                )
                worker_enricher = IncidentEnricher(llm_client=worker_llm)
            else:
                worker_enricher = enricher

            logger.info(f"Consumer worker-{worker_id} started")
            worker_stats = enrich_articles_phase(
                conn=thread_conn,
                enricher=worker_enricher,
                incident_queue=incident_queue,
                fetch_complete_event=fetch_complete_event,
                skip_if_not_education=args.skip_non_education,
                rate_limit_delay=args.rate_limit_delay,
                total_expected=total_to_process,
                cancel_event=_cancel_event,
            )
            logger.info(f"Consumer worker-{worker_id} completed: {worker_stats}")

            # Merge stats thread-safely
            with stats_lock:
                for key in combined_enrich_stats:
                    combined_enrich_stats[key] += worker_stats.get(key, 0)
        except Exception as e:
            logger.error(f"Error in consumer worker-{worker_id}: {e}", exc_info=True)
            with stats_lock:
                combined_enrich_stats["errors"] += 1
        finally:
            thread_conn.close()

    # Start consumer threads
    consumers = []
    for i in range(num_workers):
        t = threading.Thread(target=consumer_thread, args=(i,), daemon=False, name=f"enricher-{i}")
        t.start()
        consumers.append(t)
    logger.info(f"Started {num_workers} enrichment consumer thread(s)")

    # Run producer (fetching) in main thread
    try:
        fetch_stats = fetch_articles_phase(
            conn,
            unenriched,
            incident_queue=incident_queue,
            limit=args.limit,
            min_delay_seconds=2.0,
            max_delay_seconds=5.0,
        )
    except Exception as e:
        logger.error(f"Error in producer (fetching): {e}", exc_info=True)
        fetch_stats["errors"] += 1
    finally:
        fetch_complete_event.set()
        logger.info("Fetching complete - signaled consumer threads")

    # Wait for all consumer threads — no timeout.
    # Each LLM call can take up to 180s, so even a modest batch (e.g. 500 incidents)
    # needs 500 * 180s = 25 hours. The old timeout of total*2 (e.g. 3300s = 55min)
    # was killing the consumer mid-enrichment. Use None to wait indefinitely; the
    # cancel_event mechanism provides graceful shutdown when needed.
    logger.info(f"Waiting for {num_workers} consumer(s) to finish (no timeout — cancel via admin panel)...")
    for t in consumers:
        # Poll every 60s so we can log progress and check for cancel
        while t.is_alive():
            t.join(timeout=60)
            if t.is_alive():
                with stats_lock:
                    done = combined_enrich_stats["enriched"] + combined_enrich_stats["skipped"] + combined_enrich_stats["errors"]
                logger.info(f"Still waiting for {t.name}... ({done}/{total_to_process} done so far)")

    logger.info("All consumer threads finished")

    # Final stats
    enrich_stats = combined_enrich_stats
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
