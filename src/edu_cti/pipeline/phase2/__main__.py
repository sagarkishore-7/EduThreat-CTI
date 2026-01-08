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
    logger.info("=" * 60)
    logger.info("PHASE 1: Smart Article Fetching (Producer - Pushing to Queue)")
    logger.info("=" * 60)
    
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
    
    if not incidents_to_process:
        logger.warning("No incidents available for fetching")
        return stats
    
    logger.info(f"Selected {len(incidents_to_process)} incidents for fetching (random selection with domain diversity)")
    
    try:
        # Process incidents one by one and push to queue as soon as articles are fetched
        total_incidents = len(incidents_to_process)
        for idx, incident in enumerate(incidents_to_process, 1):
            incident_id = incident["incident_id"]
            progress_pct = (idx / total_incidents) * 100
            logger.info(f"[{idx}/{total_incidents}] ({progress_pct:.1f}%) Fetching articles for incident: {incident_id}")
            
            try:
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
                
                if successful_articles:
                    stats["articles_fetched"] += 1
                    logger.info(f"✓ Fetched {len(successful_articles)} article(s) for incident {incident_id} - pushing to queue")
                    
                    # Clear broken status for successfully fetched URLs
                    if successful_articles:
                        from src.edu_cti.core.db import clear_broken_urls
                        successful_urls = [a.url for a in successful_articles if a.url]
                        if successful_urls:
                            clear_broken_urls(conn, incident_id, successful_urls)
                            conn.commit()
                    
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
                    logger.info(f"✓ Pushed incident {incident_id} to enrichment queue (queue size: {incident_queue.qsize()})")
                else:
                    logger.warning(f"⊘ No articles fetched for incident {incident_id}")
                    # Mark all URLs as broken if none were fetched
                    all_urls = incident.get("all_urls", [])
                    if all_urls:
                        from src.edu_cti.core.db import mark_urls_as_broken
                        mark_urls_as_broken(conn, incident_id, all_urls)
                        conn.commit()
                    stats["errors"] += 1
                
                stats["processed"] += 1
                
            except Exception as e:
                stats["errors"] += 1
                logger.error(f"✗ Error fetching articles for incident {incident_id}: {e}", exc_info=True)
        
    except Exception as e:
        stats["errors"] += len(incidents_to_process)
        logger.error(f"✗ Error during article fetching: {e}", exc_info=True)
    
    logger.info("=" * 60)
    logger.info("Article Fetching Complete")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Articles Fetched: {stats['articles_fetched']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("=" * 60)
    
    return stats


def enrich_articles_phase(
    conn,
    enricher: IncidentEnricher,
    incident_queue: queue.Queue,
    fetch_complete_event: threading.Event,
    skip_if_not_education: bool = False,
    rate_limit_delay: float = 1.0,
    total_expected: int = 0,
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
    logger.info("=" * 60)
    logger.info("PHASE 2: Sequential LLM Enrichment (Consumer - Processing from Queue)")
    logger.info("=" * 60)
    
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
    
    logger.info("Waiting for incidents in queue...")
    logger.info(f"Queue initial state: empty={incident_queue.empty()}, size={incident_queue.qsize()}")
    
    # CONSUMER LOOP: Process incidents from queue as they arrive
    items_processed = 0
    while True:
        try:
            # Get incident from queue (blocks until available or timeout)
            # Use timeout to periodically check if fetching is complete
            try:
                incident_dict = incident_queue.get(timeout=5.0)
            except queue.Empty:
                # Check if fetching is complete
                if fetch_complete_event.is_set():
                    logger.info(f"Fetching complete and queue is empty - stopping consumer (processed {items_processed} items)")
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
            
            # Calculate progress percentage
            if total_expected > 0:
                progress_pct = (stats["processed"] / total_expected) * 100
                logger.info(f"[{stats['processed']}/{total_expected}] ({progress_pct:.1f}%) Enriching: {incident_id}")
            else:
                logger.info(f"[{stats['processed']}] Enriching: {incident_id}")
            
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
                logger.info(f"Calling process_incident for {incident_id}...")
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
                        logger.error(f"Rate limit error in process_incident for {incident_id}: {e}")
                        print(f"[RATE LIMIT] ✗ Stopping enrichment due to persistent rate limit errors", flush=True)
                        stats["errors"] += 1
                        # Mark remaining items as done and break out of loop
                        while not incident_queue.empty():
                            try:
                                incident_queue.get_nowait()
                                incident_queue.task_done()
                            except queue.Empty:
                                break
                        # Signal that we're stopping due to rate limit
                        break
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
                        logger.info(f"✓✓✓ Successfully enriched and saved incident: {incident_id}")
                        # Verify it was saved
                        cur = conn.execute("SELECT llm_enriched FROM incidents WHERE incident_id = ?", (incident_id,))
                        verify_row = cur.fetchone()
                        if verify_row and verify_row["llm_enriched"] == 1:
                            logger.info(f"✓ Verified: incident {incident_id} marked as enriched in database")
                        else:
                            logger.error(f"✗ ERROR: incident {incident_id} NOT marked as enriched in database!")
                    else:
                        stats["skipped"] += 1
                        logger.warning(f"⊘ Skipped enrichment upgrade for {incident_id} - lower confidence or save failed")
                else:
                    # Check what kind of failure we have
                    if raw_json_data and isinstance(raw_json_data, dict):
                        if raw_json_data.get("_not_education_related"):
                            # Explicitly not education-related - mark as skipped
                            reason = raw_json_data.get("_reason", "Not education-related")
                            mark_incident_skipped(conn, incident_id, f"Not education-related: {reason}")
                            conn.commit()  # Commit skip marker immediately
                            stats["skipped"] += 1
                            logger.info(f"⊘ Skipped incident: {incident_id} - Not education-related")
                        elif raw_json_data.get("_enrichment_failed"):
                            # Enrichment failed (JSON parsing, etc.) - DON'T mark as skipped, will retry
                            reason = raw_json_data.get("_reason", "Enrichment failed")
                            stats["errors"] += 1
                            logger.warning(f"⚠ Enrichment failed for {incident_id}: {reason} - will retry on next run")
                        else:
                            # Unknown error in raw_json_data - don't mark as skipped
                            stats["errors"] += 1
                            logger.warning(f"⚠ Unknown enrichment result for {incident_id}")
                    else:
                        # No enrichment result and no error info - mark as skipped
                        mark_incident_skipped(conn, incident_id, "Enrichment returned no result")
                        conn.commit()  # Commit skip marker immediately
                        stats["skipped"] += 1
                        logger.warning(f"⊘ Skipped incident: {incident_id} - No enrichment result")
                
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
    enrich_stats = {"processed": 0, "enriched": 0, "skipped": 0, "errors": 0}
    
    # Total incidents to process (for progress tracking)
    total_to_process = len(unenriched)
    
    # Start consumer thread (enrichment) - it will wait for items in queue
    def consumer_thread():
        """Consumer thread that processes incidents from queue."""
        nonlocal enrich_stats
        # Create a new connection for this thread (SQLite connections are not thread-safe)
        thread_conn = get_connection()
        try:
            logger.info("Consumer thread started - waiting for incidents in queue")
            enrich_stats = enrich_articles_phase(
                conn=thread_conn,
                enricher=enricher,
                incident_queue=incident_queue,
                fetch_complete_event=fetch_complete_event,
                skip_if_not_education=args.skip_non_education,
                rate_limit_delay=args.rate_limit_delay,
                total_expected=total_to_process,
            )
            logger.info(f"Consumer thread completed: {enrich_stats}")
        except Exception as e:
            logger.error(f"Error in consumer thread: {e}", exc_info=True)
            enrich_stats = {"processed": 0, "enriched": 0, "skipped": 0, "errors": 1}
        finally:
            thread_conn.close()
    
    # Start consumer thread
    consumer = threading.Thread(target=consumer_thread, daemon=False)
    consumer.start()
    logger.info("Started enrichment consumer thread (waiting for incidents in queue)")
    
    # Run producer (fetching) in main thread
    # This will push incidents to queue as articles are fetched
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
        # Signal that fetching is complete
        fetch_complete_event.set()
        logger.info("Fetching complete - signaled consumer thread")
    
    # Wait for consumer thread to finish processing remaining items
    logger.info("Waiting for enrichment consumer to finish processing queue...")
    consumer.join(timeout=300)  # Wait up to 5 minutes
    
    if consumer.is_alive():
        logger.warning("Consumer thread did not finish within timeout - may still be processing")
    else:
        logger.info("Consumer thread finished")
    
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
