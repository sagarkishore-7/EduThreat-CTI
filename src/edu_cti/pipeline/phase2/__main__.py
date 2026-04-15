"""
Phase 2: Enrichment Pipeline CLI

Main entry point for Phase 2 LLM enrichment pipeline.
Coordinates article fetching, education relevance checking, URL scoring,
and comprehensive CTI extraction.
"""

import argparse
import logging
import os
import random
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
    checkpoint_mark,
    checkpoint_get_fetched,
    checkpoint_clear,
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
    ENRICHMENT_SKIP_SOURCES,
    FETCH_IMPOSSIBLE_SOURCES,
    SERP_MAX_ATTEMPTS,
)
from src.edu_cti.core.logging_utils import configure_logging
from src.edu_cti.core.db import get_connection, init_db
from pathlib import Path

# Module-level logger — used by module-level helpers (_record_serp_failure, etc.)
logger = logging.getLogger(__name__)

# Module-level cancel event — set by pipeline manager to request graceful stop
_cancel_event = threading.Event()

# Per-incident in-progress guard — prevents two workers from enriching the same
# incident simultaneously when the queue is fed from multiple sources.
_in_progress: set = set()
_in_progress_lock = threading.Lock()


class EnrichmentWatchdog:
    """
    Detects enrichment pipeline stalls and triggers a clean process exit.

    A stall is defined as no pipeline activity (fetch iteration or queue item
    processed) for more than ``stall_seconds`` seconds.  On stall detection the
    watchdog calls ``os._exit(1)`` so Railway/Docker restarts the container.

    Usage:
        watchdog = EnrichmentWatchdog(stall_seconds=300)
        watchdog.start()
        ...
        watchdog.heartbeat()   # call after each queue item is processed
        ...
        watchdog.stop()
    """

    def __init__(self, stall_seconds: int = 300):
        self._stall_seconds = stall_seconds
        self._last_beat = time.monotonic()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._last_beat = time.monotonic()
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._watch, daemon=True, name="enrichment-watchdog"
        )
        self._thread.start()
        logger.info(f"[WATCHDOG] Started — stall threshold {self._stall_seconds}s")

    def stop(self) -> None:
        self._stop_event.set()

    def heartbeat(self) -> None:
        """Call after every queue item processed (any outcome) to reset the stall timer."""
        self._last_beat = time.monotonic()

    def is_stalled(self) -> bool:
        return (time.monotonic() - self._last_beat) > self._stall_seconds

    def _watch(self) -> None:
        while not self._stop_event.wait(timeout=30):
            elapsed = time.monotonic() - self._last_beat
            if elapsed > self._stall_seconds:
                logger.error(
                    f"[WATCHDOG] Enrichment stalled for {elapsed:.0f}s "
                    f"(threshold {self._stall_seconds}s) — triggering restart"
                )
                os._exit(1)


# Module-level watchdog instance — shared by all consumer threads
_watchdog: Optional["EnrichmentWatchdog"] = None


def _get_watchdog() -> Optional["EnrichmentWatchdog"]:
    return _watchdog


def _clear_watchdog() -> None:
    """Stop and clear any lingering watchdog from the current process."""
    global _watchdog
    if _watchdog is None:
        return
    try:
        _watchdog.stop()
    except Exception:
        pass
    _watchdog = None


def _record_serp_failure(conn, incident_id: str) -> bool:
    """
    Increment serp_attempt_count for an incident after a failed SERP search.

    Returns True if the incident has now hit SERP_MAX_ATTEMPTS and was soft-deleted,
    False if it was only incremented (caller should skip it for this run).
    """
    conn.execute(
        "UPDATE incidents SET serp_attempt_count = COALESCE(serp_attempt_count, 0) + 1 WHERE incident_id = ?",
        (incident_id,),
    )
    conn.commit()
    row = conn.execute(
        "SELECT serp_attempt_count FROM incidents WHERE incident_id = ?", (incident_id,)
    ).fetchone()
    count = row[0] if row else 0
    if count >= SERP_MAX_ATTEMPTS:
        # Soft-delete: mark as excluded so we stop trying, but keep the row for audit.
        # (The public API never shows llm_excluded=1 rows since they have no enrichment data.)
        conn.execute(
            """
            UPDATE incidents
            SET llm_excluded = 1,
                llm_excluded_reason = 'serp_exhausted',
                llm_enriched = 1,
                llm_enriched_at = datetime('now')
            WHERE incident_id = ?
            """,
            (incident_id,),
        )
        conn.commit()
        logger.info(
            f"Soft-excluded {incident_id} after {count} failed SERP attempts "
            f"(no articles found, incident is unenrichable)"
        )
        return True
    logger.info(
        f"SERP attempt {count}/{SERP_MAX_ATTEMPTS} for {incident_id} — will retry next run"
    )
    return False

# Module-level progress dict — updated during execution, read by pipeline manager
# Uses two sub-phases: "Fetching articles" (0-30%) and "LLM Enrichment" (30-100%)
_progress = {"step": "", "detail": "", "percent": 0}

# SKIP_ENRICHMENT_SOURCES / FETCH_IMPOSSIBLE_SOURCES imported from config above.
# Kept as module-level aliases for backward compatibility with any direct references.
SKIP_ENRICHMENT_SOURCES = ENRICHMENT_SKIP_SOURCES


def _create_secondary_incidents(
    conn,
    parent_incident: Dict,
    secondary_list: List[Dict],
    source_url: str,
) -> None:
    """
    Create stub incidents for secondary victims found in a roundup article.

    Each stub gets:
    - The roundup article URL in all_urls so Phase 2 can SERP-discover a dedicated article
    - llm_enriched=0 so it will be picked up for enrichment on the next run
    - Source attribution copied from the parent incident

    Name+date dedup in _ingest_batch will silently skip any that already exist.
    """
    from src.edu_cti.core.models import BaseIncident, make_incident_id
    from src.edu_cti.core.db import insert_incident, add_incident_source, source_event_exists
    from src.edu_cti.core.db import find_duplicate_by_name_and_date

    parent_source = (parent_incident.get("source") or
                     parent_incident.get("incident_id", "unknown").split("_")[0])

    created = 0
    # Names that indicate the LLM couldn't identify the institution.
    # Creating a stub for these is pointless — SERP can't search for "Unknown"
    # and the LLM prompt hint would be useless. Skip them entirely.
    _INVALID_VICTIM_NAMES = {
        "", "unknown", "unknown school", "unknown institution", "unknown university",
        "unnamed", "unnamed school", "undisclosed", "undisclosed institution",
        "n/a", "none", "redacted", "unidentified",
    }

    skipped = 0
    for entry in secondary_list:
        victim_name = (entry.get("victim_name") or "").strip()
        if not victim_name or victim_name.lower() in _INVALID_VICTIM_NAMES:
            logger.debug(f"Skipping secondary stub with unusable victim name: {victim_name!r}")
            skipped += 1
            continue

        incident_date = entry.get("incident_date") or None
        attack_type   = entry.get("attack_type") or None
        country       = entry.get("country") or None
        brief_desc    = entry.get("brief_description") or None

        # Build notes: store the roundup reference + brief description for context.
        # The roundup URL is intentionally NOT put in all_urls — if it were, the
        # next enrichment run would re-fetch the same roundup article, the LLM
        # would see all N victims again with no context about which one to focus on,
        # and would arbitrarily pick a different "primary". Instead we leave
        # all_urls=[] so fetch_articles_phase() triggers SERP to find a dedicated article.
        stub_notes_parts = []
        if source_url:
            stub_notes_parts.append(f"Extracted from roundup: {source_url}")
        if brief_desc:
            stub_notes_parts.append(brief_desc)
        stub_notes = "\n".join(stub_notes_parts) or None

        # Dedup: skip if an incident for this victim+date already exists in DB
        stub = BaseIncident(
            incident_id="",  # temp — assigned below
            source=parent_source,
            university_name=victim_name,
            victim_raw_name=victim_name,
            incident_date=incident_date,
            attack_type_hint=attack_type,
            country=country,
            notes=stub_notes,
            all_urls=[],  # empty: SERP discovery will find a dedicated article
        )
        dup_id = find_duplicate_by_name_and_date(conn, stub)
        if dup_id:
            logger.debug(f"Secondary incident already exists for '{victim_name}' → {dup_id}")
            skipped += 1
            continue

        # Build stable ID and event key
        dedup_key = f"{victim_name.lower()}|{incident_date or ''}|roundup_extract"
        incident_id = make_incident_id(parent_source, dedup_key)

        if source_event_exists(conn, parent_source, dedup_key):
            skipped += 1
            continue

        stub.incident_id = incident_id
        stub.source_event_id = dedup_key

        try:
            insert_incident(conn, stub)
            add_incident_source(conn, incident_id, parent_source, dedup_key)
            conn.commit()
            created += 1
            logger.info(
                f"Created secondary incident {incident_id} for '{victim_name}' "
                f"(extracted from roundup article)"
            )
        except Exception as e:
            logger.warning(f"Failed to create secondary incident for '{victim_name}': {e}")

    if created or skipped:
        logger.info(
            f"Secondary incidents from roundup: {created} created, {skipped} already exist"
        )


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
    min_delay_seconds: float = 0.5,  # Oxylabs rotates IPs — no need for long domain delays
    max_delay_seconds: float = 1.5,
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
        max_fetches_per_hour=200,  # Oxylabs rotates IPs — server-side per-IP limits don't apply
        block_duration_seconds=3600,
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

    # --- Fast-path: incidents that already have articles saved from a previous run ---
    # Push them directly to the LLM queue without re-fetching anything, BUT only
    # when the stored article URLs are aligned with the incident's current all_urls.
    # If all_urls changed between runs (scraper re-ingested a different URL), the DB
    # articles are stale and would produce misaligned enrichment (wrong primary_url).
    from src.edu_cti.pipeline.phase2.storage.article_storage import get_all_articles_for_incident
    fast_path_raw = [i for i in incidents_to_process if i.get("has_articles")]
    needs_fetch = [i for i in incidents_to_process if not i.get("has_articles")]
    fast_path = []
    for fp_incident in fast_path_raw:
        fp_id = fp_incident["incident_id"]
        current_urls = set(fp_incident.get("all_urls") or [])
        if current_urls:
            db_articles = get_all_articles_for_incident(conn, fp_id)
            db_urls = {a["url"] for a in db_articles if a.get("url")}
            if not (current_urls & db_urls):
                # No overlap — stale articles from a different URL set.  Purge and re-fetch.
                conn.execute("DELETE FROM articles WHERE incident_id = ?", (fp_id,))
                conn.commit()
                logger.warning(
                    f"Fast-path purged stale articles for {fp_id}: "
                    f"DB had {db_urls}, current all_urls={current_urls}"
                )
                needs_fetch.append(fp_incident)
                continue
        fast_path.append(fp_incident)

    if fast_path:
        logger.info(f"Fast-pathing {len(fast_path)} incidents with existing aligned articles straight to LLM queue")
        for fp_incident in fast_path:
            incident_queue.put({
                "incident_id": fp_incident["incident_id"],
                "university_name": fp_incident.get("university_name") or "Unknown",
                "victim_raw_name": fp_incident.get("victim_raw_name"),
                "institution_type": None,
                "country": fp_incident.get("country"),
                "region": fp_incident.get("region"),
                "city": fp_incident.get("city"),
                "incident_date": fp_incident.get("incident_date"),
                "date_precision": "unknown",
                "source_published_date": fp_incident.get("source_published_date"),
                "ingested_at": None,
                "title": fp_incident.get("title"),
                "subtitle": None,
                "primary_url": None,
                "all_urls": fp_incident.get("all_urls") or [],
                "attack_type_hint": fp_incident.get("attack_type_hint"),
                "status": "suspected",
                "source_confidence": "medium",
                "notes": fp_incident.get("notes"),
            })
            stats["processed"] += 1
            stats["articles_fetched"] += 1
    incidents_to_process = needs_fetch

    # Keep the watchdog alive during the fetch phase via a background thread.
    # The main loop heartbeats once per incident, but a single slow fetch_article()
    # call (hanging newspaper3k / slow proxy) can block for minutes without
    # returning — preventing the next iteration's heartbeat.  This thread fires
    # every 30 s unconditionally, so the watchdog never false-fires during fetch.
    import threading as _threading
    _keepalive_stop = _threading.Event()

    def _fetch_keepalive():
        while not _keepalive_stop.is_set():
            if _watchdog:
                _watchdog.heartbeat()
            _keepalive_stop.wait(30)

    _keepalive_thread = _threading.Thread(target=_fetch_keepalive, daemon=True, name="fetch-keepalive")
    _keepalive_thread.start()

    try:
        # Process incidents one by one and push to queue as soon as articles are fetched
        total_incidents = len(incidents_to_process)
        for idx, incident in enumerate(incidents_to_process, 1):
            # Check for cancellation
            if _cancel_event.is_set():
                logger.info(f"Cancel requested — stopping article fetching at [{idx}/{total_incidents}]")
                break

            incident_id = incident["incident_id"]

            # Skip URL-less incidents with no identifiable institution name.
            # These can never be enriched: SERP can't build a query and the LLM
            # prompt hint would be empty. Mark them skipped to stop retry loops.
            _UNENRICHABLE_NAMES = {
                "unknown", "unknown school", "unknown institution", "unknown university",
                "unnamed", "unnamed school", "undisclosed", "n/a", "none",
                "redacted", "unidentified",
            }
            incident_name = (incident.get("university_name") or incident.get("victim_raw_name") or "").strip()
            incident_urls = incident.get("all_urls") or []
            if not incident_urls and incident_name.lower() in _UNENRICHABLE_NAMES:
                logger.info(f"Skipping unenrichable stub {incident_id} (name={incident_name!r}, no URLs) — marking skipped")
                from src.edu_cti.pipeline.phase2.storage.db import mark_incident_skipped
                mark_incident_skipped(conn, incident_id, reason=f"No URLs and unidentifiable institution name: {incident_name!r}")
                conn.commit()
                stats["processed"] += 1
                continue

            # Skip IOC-only sources at fetch time — no point fetching abuse.ch/censys URLs
            source_prefix = incident_id.split("_")[0]
            if source_prefix in SKIP_ENRICHMENT_SOURCES:
                logger.debug(f"Skipping fetch for IOC source incident: {incident_id}")
                stats["processed"] += 1
                continue

            # For paywall sources (e.g. securityweek), skip all 4 fetch tiers immediately.
            # securityweek.com is now in BLOCKED_FETCH_DOMAINS so fetch_article() would
            # return instantly with an error anyway — skip directly to SERP fallback.
            if source_prefix in FETCH_IMPOSSIBLE_SOURCES:
                from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp
                from src.edu_cti.pipeline.phase2.storage.article_storage import save_article, get_all_articles_for_incident
                existing_articles = get_all_articles_for_incident(conn, incident_id)
                if not existing_articles:
                    serp_urls = discover_articles_via_serp(incident)
                    fetched_any = False
                    for serp_url in serp_urls:
                        domain = fetching_strategy.rate_limiter.extract_domain(serp_url)
                        if not domain or not fetching_strategy.rate_limiter.can_fetch_from_domain(domain):
                            continue
                        fetching_strategy.rate_limiter.wait_if_needed(domain)
                        try:
                            ac = fetching_strategy.article_fetcher.fetch_article(serp_url)
                            fetching_strategy.rate_limiter.record_fetch(domain, success=ac.fetch_successful)
                            if ac.fetch_successful:
                                save_article(conn, incident_id=incident_id, url=serp_url, article=ac)
                                conn.commit()
                                fetched_any = True
                                break
                        except Exception as _e:
                            logger.debug(f"SERP fetch error {serp_url}: {_e}")
                    if fetched_any:
                        incident_queue.put({**incident, "incident_id": incident_id})
                        stats["articles_fetched"] += 1
                    else:
                        _record_serp_failure(conn, incident_id)
                else:
                    incident_queue.put({**incident, "incident_id": incident_id})
                stats["processed"] += 1
                continue

            progress_pct = (idx / total_incidents) * 100
            # Update module-level progress for pipeline manager
            # Fetch phase maps to 0-30% of overall enrichment progress
            _progress["step"] = "Fetching articles"
            _progress["detail"] = f"{idx}/{total_incidents} ({stats['articles_fetched']} fetched)"
            _progress["percent"] = int(progress_pct * 0.30)  # 0-30% range
            if idx % 10 == 0 or idx == total_incidents:  # Log every 10th or last
                logger.info(f"Fetching [{idx}/{total_incidents}] ({progress_pct:.1f}%)")
            # Heartbeat on every fetch iteration so the watchdog knows the pipeline
            # is alive even when enricher workers are blocked on an empty queue.
            if _watchdog:
                _watchdog.heartbeat()
            
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
                    # Validate that existing articles are actually from the incident's
                    # current all_urls.  If all_urls changed between runs (e.g. scraper
                    # re-ingested a different URL for the same incident), the DB articles
                    # come from a stale URL set and will produce misaligned enrichment
                    # (wrong primary_url vs all_urls, wrong article content).
                    current_urls = set(incident.get("all_urls") or [])
                    existing_urls = {a["url"] for a in existing_articles if a.get("url")}
                    aligned = current_urls & existing_urls if current_urls else existing_urls
                    if current_urls and not aligned:
                        # No overlap — stale articles from a different URL set.  Delete
                        # them so the next SERP/fetch attempt starts fresh.
                        conn.execute(
                            "DELETE FROM articles WHERE incident_id = ?", (incident_id,)
                        )
                        conn.commit()
                        logger.warning(
                            f"Purged {len(existing_articles)} stale article(s) for {incident_id} "
                            f"(DB URLs {existing_urls} ∉ current all_urls {current_urls})"
                        )
                        should_push_to_queue = False
                    else:
                        logger.info(f"Incident {incident_id} has {len(existing_articles)} aligned articles in DB")
                        should_push_to_queue = True
                else:
                    # No articles fetched and nothing in DB.
                    # For URL-less incidents (e.g. secondary stubs from roundup articles),
                    # try SERP to find a dedicated article before giving up.
                    incident_urls = incident.get("all_urls") or []
                    serp_found = False
                    if not incident_urls:
                        from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp
                        from src.edu_cti.pipeline.phase2.storage.article_storage import save_article
                        serp_urls = discover_articles_via_serp(incident)

                        # Filter out the roundup URL that spawned this stub so SERP
                        # doesn't loop back to the same multi-school article.
                        # The roundup URL is stored in notes as "Extracted from roundup: <url>"
                        notes_text = incident.get("notes") or ""
                        roundup_url = None
                        if notes_text.startswith("Extracted from roundup: "):
                            roundup_url = notes_text.split("Extracted from roundup: ", 1)[1].split("\n")[0].strip()
                        if roundup_url:
                            before = len(serp_urls)
                            serp_urls = [u for u in serp_urls if u.rstrip("/") != roundup_url.rstrip("/")]
                            if len(serp_urls) < before:
                                logger.debug(f"Filtered roundup URL from SERP results for {incident_id}")

                        for serp_url in serp_urls:
                            s_domain = fetching_strategy.rate_limiter.extract_domain(serp_url)
                            if not s_domain or not fetching_strategy.rate_limiter.can_fetch_from_domain(s_domain):
                                continue
                            fetching_strategy.rate_limiter.wait_if_needed(s_domain)
                            try:
                                ac = fetching_strategy.article_fetcher.fetch_article(serp_url)
                                fetching_strategy.rate_limiter.record_fetch(s_domain, success=ac.fetch_successful)
                                if ac.fetch_successful:
                                    save_article(conn, incident_id=incident_id, url=serp_url, article=ac)
                                    conn.commit()
                                    serp_found = True
                                    stats["articles_fetched"] += 1
                                    logger.info(f"SERP found article for {incident_id}: {serp_url[:80]}")
                                    should_push_to_queue = True
                                    break
                            except Exception as _se:
                                logger.debug(f"SERP fetch error {serp_url}: {_se}")

                    if not serp_found:
                        source = incident_id.split("_")[0]

                        # Comparitech incidents already carry fully structured metadata
                        # (school name, date, ransomware strain, ransom amount/paid,
                        # records affected) in the `notes` and `title` fields.
                        # Synthesize an article from that data so the LLM can produce
                        # a valid enrichment record without needing a real news article.
                        if source == "comparitech":
                            from src.edu_cti.pipeline.phase2.storage.article_storage import save_article
                            from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent
                            inc_name = incident.get("university_name") or incident.get("victim_raw_name") or "an educational institution"
                            inc_date = incident.get("incident_date") or ""
                            inc_city = incident.get("city") or ""
                            inc_region = incident.get("region") or ""
                            inc_country = incident.get("country") or "US"
                            inc_notes = incident.get("notes") or ""
                            inc_title = incident.get("title") or f"Ransomware attack on {inc_name}"
                            location_parts = [p for p in [inc_city, inc_region, inc_country] if p]
                            location_str = ", ".join(location_parts) if location_parts else inc_country
                            synthetic_content = (
                                f"{inc_title}\n\n"
                                f"{inc_name} suffered a ransomware cyberattack"
                                + (f" in {inc_date[:4]}" if inc_date else "") + "."
                                + (f" Location: {location_str}." if location_str else "")
                                + (f"\n\nDetails from Comparitech tracker: {inc_notes}" if inc_notes else "")
                                + "\n\nThis is a confirmed ransomware incident targeting an educational institution "
                                "recorded in the Comparitech ransomware attack database."
                            )
                            synthetic_article = ArticleContent(
                                url=f"comparitech://synthetic/{incident_id}",
                                title=inc_title,
                                content=synthetic_content,
                                fetch_successful=True,
                            )
                            save_article(conn, incident_id=incident_id,
                                         url=synthetic_article.url, article=synthetic_article)
                            conn.commit()
                            should_push_to_queue = True
                            stats["articles_fetched"] += 1
                            logger.info(f"Comparitech: synthesized article for {incident_id} ({inc_name})")
                        else:
                            # Track the failed SERP attempt. After SERP_MAX_ATTEMPTS the
                            # incident is permanently deleted — it has no articles and
                            # SERP consistently finds nothing, so it can never be enriched.
                            deleted = _record_serp_failure(conn, incident_id)
                            stats["errors"] += 1
                            should_push_to_queue = False
                            if deleted:
                                stats["processed"] += 1  # count deletion as processed

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
                    # Checkpoint: record that article fetch completed for this incident.
                    # On crash+restart, incidents already checkpointed skip re-fetch.
                    checkpoint_mark(conn, incident_id, phase="article_fetch")

                stats["processed"] += 1
                
            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error fetching articles for {incident_id}: {e}")
        
    except Exception as e:
        stats["errors"] += len(incidents_to_process)
        logger.error(f"Error during article fetching: {e}")
    finally:
        # Stop the keepalive thread — fetch phase is done.
        _keepalive_stop.set()
        _keepalive_thread.join(timeout=5)

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
    db_already_enriched: int = 0,
    db_total_incidents: int = 0,
) -> Dict[str, int]:
    """
    Phase 2: Sequential LLM enrichment (queue-based consumer).

    Args:
        conn: Database connection
        enricher: IncidentEnricher instance
        incident_queue: Queue to consume incidents from
        fetch_complete_event: Event to signal when fetching is complete
        skip_if_not_education: Whether to skip non-education incidents
        rate_limit_delay: Delay between LLM calls
        total_expected: Incidents to process in this run (for per-run progress)
        db_already_enriched: Incidents already enriched in DB at run start (for cumulative display)
        db_total_incidents: Total incidents in DB at run start (for cumulative display)

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
            # Block until an item is available (no timeout needed — sentinel handles exit)
            try:
                incident_dict = incident_queue.get(timeout=10.0)
            except queue.Empty:
                # Periodic wakeup — check cancel before blocking again
                if cancel_event and cancel_event.is_set():
                    logger.info(f"Cancel requested — stopping enrichment (processed {items_processed} items)")
                    break
                continue

            # Check for sentinel (None) — producer pushes one per worker after fetching
            if incident_dict is SENTINEL:
                logger.debug(f"Received sentinel — stopping consumer (processed {items_processed} items)")
                incident_queue.task_done()
                break
            
            incident_id = incident_dict["incident_id"]

            # Per-incident guard: if another worker already claimed this incident,
            # drop it — SQLite would serialise the write anyway and we'd overwrite.
            with _in_progress_lock:
                if incident_id in _in_progress:
                    logger.debug(f"Worker collision avoided — {incident_id} already in progress, skipping")
                    incident_queue.task_done()
                    continue
                _in_progress.add(incident_id)

            # Skip IOC-only sources that produce no education articles
            source_prefix = incident_id.split("_")[0]
            if source_prefix in SKIP_ENRICHMENT_SOURCES:
                logger.debug(f"Skipping IOC source incident: {incident_id}")
                stats["skipped"] += 1
                with _in_progress_lock:
                    _in_progress.discard(incident_id)
                incident_queue.task_done()
                continue

            stats["processed"] += 1

            # Calculate progress percentage - log every 10th or at milestones
            # Enrichment phase maps to 30-100% of overall progress
            if total_expected > 0:
                raw_pct = (stats["processed"] / total_expected) * 100
                # Map 0-100% enrichment progress into 30-100% overall range
                scaled_pct = 30 + int(raw_pct * 0.70)
                # Cumulative DB progress: already enriched before this run + enriched so far in this run
                db_enriched_now = db_already_enriched + stats.get("enriched", 0)
                db_total_str = f"/{db_total_incidents}" if db_total_incidents else ""
                # Update module-level progress for pipeline manager
                _progress["step"] = "LLM Enrichment"
                _progress["detail"] = (
                    f"{stats['processed']}/{total_expected} this run | "
                    f"{db_enriched_now}{db_total_str} enriched in DB"
                )
                _progress["percent"] = min(scaled_pct, 100)
                if stats["processed"] % 10 == 0 or raw_pct % 10 < 1:  # Every 10 or every 10%
                    logger.info(
                        f"Enriching [{stats['processed']}/{total_expected} this run"
                        f" | {db_enriched_now}{db_total_str} enriched in DB]"
                        f" ({raw_pct:.1f}%) — "
                        f"{stats.get('enriched', 0)} enriched, {stats.get('skipped', 0)} skipped, {stats.get('errors', 0)} errors"
                    )
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
                    checkpoint_clear(conn, incident_id)
                    with _in_progress_lock:
                        _in_progress.discard(incident_id)
                    incident_queue.task_done()
                    continue

                # Check if already enriched (shouldn't happen, but check anyway)
                if row["llm_enriched"] == 1:
                    logger.warning(f"Incident {incident_id} is already enriched - skipping")
                    stats["skipped"] += 1
                    checkpoint_clear(conn, incident_id)
                    with _in_progress_lock:
                        _in_progress.discard(incident_id)
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
                    from src.edu_cti.pipeline.phase2.llm_client import RateLimitError
                    if isinstance(e, RateLimitError):
                        # Jittered exponential backoff — only THIS worker sleeps.
                        # Other workers keep enriching uninterrupted.
                        retry_count = incident_dict.get("_rate_limit_retries", 0)
                        wait_time = min(120, (2 ** retry_count) * 10 + random.uniform(0, 5))
                        logger.warning(
                            f"Rate limit for {incident_id} (retry #{retry_count+1}) — "
                            f"this worker sleeping {wait_time:.0f}s, others continue"
                        )
                        with _in_progress_lock:
                            _in_progress.discard(incident_id)
                        incident_queue.task_done()
                        # Annotate retry count so backoff grows on repeated hits
                        incident_dict["_rate_limit_retries"] = retry_count + 1
                        # Use put_nowait with fallback — queue may be full during back-pressure
                        try:
                            incident_queue.put_nowait(incident_dict)
                        except queue.Full:
                            # Queue full: drop requeue and let it be retried next pipeline run
                            logger.warning(f"Queue full — {incident_id} will retry next run")
                        time.sleep(wait_time)
                        stats["errors"] += 1
                        continue
                    elif isinstance(e, (TimeoutError, OSError)):
                        # LLM timeout/connection error — re-queue, brief pause, continue
                        logger.warning(f"Timeout/connection error for {incident_id}: {e} — requeueing")
                        with _in_progress_lock:
                            _in_progress.discard(incident_id)
                        incident_queue.task_done()
                        try:
                            incident_queue.put_nowait(incident_dict)
                        except queue.Full:
                            logger.warning(f"Queue full — {incident_id} timeout retry deferred to next run")
                        time.sleep(2.0)  # Reduced from 5s — other workers unaffected
                        stats["errors"] += 1
                        continue
                    else:
                        logger.error(f"Error in process_incident for {incident_id}: {e}", exc_info=True)
                        stats["errors"] += 1
                        with _in_progress_lock:
                            _in_progress.discard(incident_id)
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
                        # --- Post-save: delete sector-report / no-victim incidents ---
                        # If the LLM could not identify a specific institution (institution_name
                        # is null or a placeholder like "Unknown") this is a trend/report
                        # article, not a specific incident.  Delete it so the DB only contains
                        # real discrete incidents.
                        _UNKNOWN_INSTITUTION_NAMES = {
                            "", "unknown", "unknown institution", "unknown school",
                            "unknown university", "unnamed", "unidentified", "undisclosed",
                            "n/a", "none", "redacted",
                        }
                        resolved_name = ""
                        if enrichment_result.education_relevance and enrichment_result.education_relevance.institution_identified:
                            resolved_name = enrichment_result.education_relevance.institution_identified.strip()
                        elif raw_json_data and raw_json_data.get("institution_name"):
                            resolved_name = str(raw_json_data["institution_name"]).strip()
                        # Also check the original incident name (may have been set by ingestor).
                        # incident can be a dict OR a BaseIncident Pydantic object.
                        _ig = (lambda f: incident.get(f) if isinstance(incident, dict) else getattr(incident, f, None))
                        original_name = (_ig("university_name") or _ig("victim_raw_name") or "").strip()
                        effective_name = resolved_name or original_name

                        if effective_name.lower() in _UNKNOWN_INSTITUTION_NAMES:
                            from src.edu_cti.pipeline.phase2.storage.db import delete_incident
                            if delete_incident(conn, incident_id, reason="sector_report_no_institution"):
                                conn.commit()
                                stats["skipped"] += 1
                                checkpoint_clear(conn, incident_id)
                                logger.info(f"⊘ EXCLUDED  {incident_id} | no specific institution identified (sector report/trend article)")
                            else:
                                conn.commit()
                                stats["enriched"] += 1
                                checkpoint_clear(conn, incident_id)
                                logger.warning(f"✓ ENRICHED  {incident_id} | WARNING: institution unknown — could not delete")
                            with _in_progress_lock:
                                _in_progress.discard(incident_id)
                            incident_queue.task_done()
                            continue

                        # Commit immediately after each save to prevent long-running transactions
                        # This allows API reads to proceed while enrichment is running
                        conn.commit()
                        stats["enriched"] += 1
                        checkpoint_clear(conn, incident_id)
                        if _watchdog:
                            _watchdog.heartbeat()
                        primary = enrichment_result.primary_url or ""
                        logger.info(
                            f"✓ ENRICHED  {incident_id} | {primary[:80]}"
                        )

                        # --- Secondary incidents from roundup articles ---
                        # If the LLM detected other edu victims in the same article
                        # (e.g. "week in breach" digest), create stub incidents for each.
                        # They have no article URLs yet — Phase 2 SERP discovery will
                        # find dedicated articles for them on the next pipeline run.
                        if enrichment_result.other_edu_incidents:
                            _ig2 = (lambda f: incident.get(f) if isinstance(incident, dict) else getattr(incident, f, None))
                            _all_urls = _ig2("all_urls") or []
                            _source_url = _ig2("primary_url") or (_all_urls[0] if _all_urls else "")
                            _create_secondary_incidents(
                                conn,
                                parent_incident=incident if isinstance(incident, dict) else incident.__dict__,
                                secondary_list=enrichment_result.other_edu_incidents,
                                source_url=_source_url,
                            )
                    else:
                        stats["skipped"] += 1
                        logger.warning(f"~ SKIPPED   {incident_id} | save rejected (lower confidence)")
                else:
                    # Check what kind of failure we have
                    if raw_json_data and isinstance(raw_json_data, dict):
                        if raw_json_data.get("_not_education_related"):
                            # Curated education sources (comparitech, ransomwarelive, etc.) list
                            # real incidents but their stored articles may be stale/wrong.
                            # LLM correctly says "not edu" about wrong articles — but the incident
                            # IS real.  Protect these from deletion: clear bad articles and keep
                            # the incident as a stub (source metadata is still valuable).
                            _CURATED_EDU_SOURCES = {"comparitech", "ransomlook"}
                            _src_prefix = incident_id.split("_")[0]
                            if _src_prefix in _CURATED_EDU_SOURCES:
                                # Delete stale articles so they don't cause wrong classification again.
                                # Mark llm_enriched=1 so SERP doesn't retry this incident forever —
                                # the source metadata (institution, attack type, date) is already good.
                                conn.execute(
                                    "DELETE FROM articles WHERE incident_id = ?", (incident_id,)
                                )
                                conn.execute(
                                    "UPDATE incidents SET llm_enriched = 1, "
                                    "llm_enriched_at = datetime('now') WHERE incident_id = ?",
                                    (incident_id,)
                                )
                                conn.commit()
                                stats["skipped"] += 1
                                checkpoint_clear(conn, incident_id)
                                logger.warning(
                                    f"~ STUB      {incident_id} | curated-source: wrong articles cleared, "
                                    f"kept as metadata stub (institution/date/attack from source)"
                                )
                            else:
                                # Non-curated source — soft-delete (keep row, clear articles/enrichments)
                                from src.edu_cti.pipeline.phase2.storage.db import delete_incident
                                reason = raw_json_data.get("_reason", "Not education-related")
                                if delete_incident(conn, incident_id, reason="not_education_related"):
                                    stats["skipped"] += 1
                                    checkpoint_clear(conn, incident_id)
                                    logger.info(f"⊘ EXCLUDED  {incident_id} | not edu: {reason[:80]}")
                                else:
                                    stats["errors"] += 1
                                    logger.warning(f"✗ EXCL-FAIL {incident_id} | could not soft-exclude non-edu incident")
                        elif raw_json_data.get("_enrichment_failed"):
                            # Enrichment failed (JSON parsing, etc.) - DON'T mark as skipped, will retry
                            reason = raw_json_data.get("_reason", "Enrichment failed")
                            stats["errors"] += 1
                            logger.warning(f"✗ RETRY     {incident_id} | LLM failed: {reason[:80]}")
                        else:
                            stats["errors"] += 1
                            logger.warning(f"✗ RETRY     {incident_id} | unknown LLM result")
                    else:
                        # No enrichment result and no error info — typically means no
                        # fetchable article content.  Leaving llm_enriched=0 so the
                        # incident is retried on the next run.
                        stats["errors"] += 1
                        logger.warning(f"✗ RETRY     {incident_id} | no article content (will retry)")
                
                # Rate limiting between LLM calls
                if rate_limit_delay > 0:
                    logger.debug(f"Waiting {rate_limit_delay}s before next LLM call...")
                    time.sleep(rate_limit_delay)
                
                # Mark task as done
                with _in_progress_lock:
                    _in_progress.discard(incident_id)
                incident_queue.task_done()
                items_processed += 1
                # Heartbeat on any completed item (not just successful enrichment)
                # so the watchdog doesn't fire when enrichers are busy deleting/skipping.
                if _watchdog:
                    _watchdog.heartbeat()

                # Memory management: gc every 1,000 items; hard exit if RSS > 600 MB.
                # os._exit(0) triggers a clean Railway restart without a non-zero code.
                if items_processed % 1000 == 0:
                    import gc
                    gc.collect()
                    logger.debug(f"[MEM] gc.collect() after {items_processed} items")
                if items_processed % 100 == 0:
                    try:
                        import psutil, os as _os
                        rss_mb = psutil.Process(_os.getpid()).memory_info().rss / (1024 * 1024)
                        if rss_mb > 600:
                            logger.warning(
                                f"[MEM] RSS {rss_mb:.0f} MB > 600 MB threshold — "
                                "triggering clean restart to reclaim memory"
                            )
                            _os._exit(0)
                        elif rss_mb > 400:
                            logger.info(f"[MEM] RSS {rss_mb:.0f} MB (approaching threshold)")
                    except ImportError:
                        pass  # psutil not installed — memory monitoring disabled

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"✗ Error processing incident {incident_id}: {e}", exc_info=True)
                with _in_progress_lock:
                    _in_progress.discard(incident_id)
                incident_queue.task_done()
                items_processed += 1
                if _watchdog:
                    _watchdog.heartbeat()
                # Don't mark as processed - will retry on next run

        except Exception as e:
            logger.error(f"✗ Error in consumer loop: {e}", exc_info=True)
            stats["errors"] += 1

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
    _clear_watchdog()
    
    # Setup logging
    from pathlib import Path as PathLib
    log_file_path = PathLib(args.log_file) if args.log_file else None
    configure_logging(args.log_level, log_file=log_file_path)
    logger = logging.getLogger(__name__)
    
    # Initialize database
    conn = get_connection()
    init_db(conn)  # Ensure tables exist
    
    try:
        # Get enrichment stats
        stats = get_enrichment_stats(conn)
        logger.info(f"Enrichment Statistics:")
        logger.info(f"  Total incidents: {stats['total_incidents']}")
        logger.info(f"  Already enriched: {stats['enriched_incidents']}")
        logger.info(f"  Unenriched: {stats['unenriched_incidents']}")
        logger.info(f"  Ready for enrichment: {stats['ready_for_enrichment']}")
        
        # Get actionable incidents for this run.
        # Checkpointed incidents are intentionally retained here — if articles were
        # already fetched before a crash/restart, fetch_articles_phase() will fast-path
        # them directly into the LLM queue instead of re-fetching.
        unenriched = get_unenriched_incidents(conn, limit=args.limit)
        checkpointed = checkpoint_get_fetched(conn)
        if checkpointed:
            resumed = sum(1 for incident in unenriched if incident["incident_id"] in checkpointed)
            if resumed:
                logger.info(
                    f"Checkpoint: resuming {resumed} already-fetched incident(s) via fast-path"
                )

        if not unenriched:
            logger.info("No incidents ready for processing")
            return
    
        # PHASE 1 & 2: Concurrent producer-consumer pattern
        already_e = stats.get("enriched_incidents", 0)
        total_db  = stats.get("total_incidents", 0)
        logger.info(f"{'='*60}")
        logger.info(
            f"Phase 2 run starting | DB: {already_e}/{total_db} enriched | "
            f"{len(unenriched)} remaining in this batch"
        )
        logger.info(f"{'='*60}")
        
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
        
        # Start enrichment watchdog — kills process if no heartbeat for 10 minutes.
        # The fetch phase can be slow (SERP retries, TheRecord pagination), so we give
        # it 600s before declaring a stall. Railway auto-restarts on exit(1).
        global _watchdog
        _watchdog = EnrichmentWatchdog(stall_seconds=600)
        _watchdog.start()

        # Create queue and synchronization primitives
        # maxsize=100 provides back-pressure: if enrichment falls behind (slow LLM),
        # the fetch producer blocks on put() automatically — prevents memory explosion.
        incident_queue = queue.Queue(maxsize=100)
        fetch_complete_event = threading.Event()
        fetch_stats = {"processed": 0, "articles_fetched": 0, "errors": 0}

        # Total incidents to process (for progress tracking)
        total_to_process = len(unenriched)
        num_workers = max(1, min(args.workers, 8))  # Cap at 8 workers

        # Snapshot DB enrichment state at run start for cumulative progress display.
        # This lets each progress line show "[N/M this run | X/Y enriched in DB]"
        # so it's clear how far along the overall dataset we are, not just this cycle.
        db_already_enriched = stats.get("enriched_incidents", 0)
        db_total_incidents = stats.get("total_incidents", 0)

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

                logger.info(f"Worker-{worker_id} started")
                worker_stats = enrich_articles_phase(
                    conn=thread_conn,
                    enricher=worker_enricher,
                    incident_queue=incident_queue,
                    fetch_complete_event=fetch_complete_event,
                    skip_if_not_education=args.skip_non_education,
                    rate_limit_delay=args.rate_limit_delay,
                    total_expected=total_to_process,
                    cancel_event=_cancel_event,
                    db_already_enriched=db_already_enriched,
                    db_total_incidents=db_total_incidents,
                )
                s = worker_stats
                logger.info(
                    f"Worker-{worker_id} done — "
                    f"processed={s['processed']} enriched={s['enriched']} "
                    f"skipped={s['skipped']} errors={s['errors']}"
                )

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
            # Push one sentinel (None) per consumer so each worker exits its get() loop cleanly
            # without backoff polling.
            for _ in range(num_workers):
                incident_queue.put(None)
            logger.info(f"Fetching complete — pushed {num_workers} sentinel(s) to queue")

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
    finally:
        _clear_watchdog()
        conn.close()


if __name__ == "__main__":
    main()
