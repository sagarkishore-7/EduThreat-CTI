"""
Phase 1: Ingestion Pipeline CLI

Main entry point for Phase 1 ingestion pipeline.
"""

import argparse
import logging
from typing import Dict, List, Optional, Sequence

from src.edu_cti.core.db import (
    get_connection,
    init_db,
    source_event_exists,
    register_source_event,
    insert_incident,
    add_incident_source,
    find_duplicate_incident_by_urls,
    load_incident_by_id,
)
from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase1.curated import collect_curated_incidents
from src.edu_cti.pipeline.phase1.news import collect_news_incidents, NEWS_SOURCE_BUILDERS
from src.edu_cti.pipeline.phase1.rss import collect_rss_incidents
from src.edu_cti.core.sources import RSS_SOURCE_REGISTRY

logger = logging.getLogger(__name__)

GROUP_COLLECTORS = {
    "curated": ("Curated sources", collect_curated_incidents),
    "news": ("News sources", collect_news_incidents),
    "rss": ("RSS feeds", collect_rss_incidents),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest incidents into SQLite with per-source deduplication.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all sources (default, fetches all pages)
  python -m src.edu_cti.cli.ingestion

  # Run only news sources
  python -m src.edu_cti.cli.ingestion --groups news

  # Run only curated sources
  python -m src.edu_cti.cli.ingestion --groups curated

  # Run specific news sources
  python -m src.edu_cti.cli.ingestion --groups news --sources darkreading krebsonsecurity

  # Run with page limit (for testing)
  python -m src.edu_cti.cli.ingestion --groups news --max-pages 10
  python -m src.edu_cti.cli.ingestion --groups curated --max-pages 1  # Limit databreach pages

  # Fetch all pages explicitly
  python -m src.edu_cti.cli.ingestion --groups news --max-pages all
        """,
    )
    parser.add_argument(
        "--groups",
        nargs="+",
        choices=list(GROUP_COLLECTORS.keys()),
        default=list(GROUP_COLLECTORS.keys()),
        help="Select which source groups to ingest. Defaults to all.",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        default=None,
        help="Select specific sources to run. Applies to the group(s) specified in --groups. "
             "For 'news' group, valid sources: " + ", ".join(NEWS_SOURCE_BUILDERS.keys()) + ". "
             "For 'rss' group, valid sources: " + ", ".join(RSS_SOURCE_REGISTRY.keys()) + ".",
    )
    parser.add_argument(
        "--max-pages",
        type=lambda x: None if x.lower() == "all" else int(x),
        default=None,
        help="Maximum number of pages to fetch per source. Applies to news sources and curated sources with pagination (e.g., databreach). "
             "Use 'all' to fetch all pages (default: all). "
             "Specify a number to limit pages (e.g., 10 for testing).",
    )
    parser.add_argument(
        "--rss-max-age-days",
        type=int,
        default=1,
        help="Maximum age in days for RSS feed items (default: 1). Only items published within this window are included.",
    )
    return parser.parse_args()


def _event_key_for_incident(incident: BaseIncident) -> str:
    """
    For per-source dedup, prefer source_event_id; fallback to first URL in all_urls; then incident_id.
    Phase 1: primary_url is None, so we use all_urls[0] if available.
    """
    if incident.source_event_id:
        return incident.source_event_id
    # Phase 1: primary_url is None, so check all_urls
    if incident.all_urls and len(incident.all_urls) > 0:
        return incident.all_urls[0]
    # Legacy fallback (shouldn't happen in Phase 1)
    if incident.primary_url:
        return incident.primary_url
    return incident.incident_id


def _ingest_batch(conn, incidents: List[BaseIncident], is_rss: bool = False) -> int:
    """
    Insert incidents into DB with cross-source deduplication.
    
    For RSS feeds, also checks rss_feed_items table for GUID-based deduplication.
    
    Process:
    1. Check per-source deduplication (source_events table, or rss_feed_items for RSS)
    2. If new from this source, check for cross-source duplicates (URL matching)
    3. If duplicate found: merge and update existing incident
    4. If new: insert new incident
    5. Always add to incident_sources and source_events (and rss_feed_items for RSS)
    
    Args:
        conn: Database connection
        incidents: List of incidents to ingest
        is_rss: If True, use RSS feed item tracking (GUID-based)
    
    Returns number of newly inserted/updated incidents.
    """
    from src.edu_cti.core.deduplication import merge_incidents
    
    new_count = 0
    for inc in incidents:
        source = inc.source
        event_key = _event_key_for_incident(inc)

        if not event_key:
            # Highly unusual, but fallback to incident_id
            event_key = inc.incident_id

        # Step 1: Check per-source deduplication
        # RSS feeds use source_events table with GUID as source_event_id
        if source_event_exists(conn, source, event_key):
            continue  # Already ingested from this source in a previous run

        # Step 2: Check for cross-source duplicates (URL matching)
        duplicate_result = find_duplicate_incident_by_urls(conn, inc)
        
        if duplicate_result:
            duplicate_incident_id, is_enriched, should_upgrade_or_drop = duplicate_result
            
            if is_enriched:
                # Existing incident is enriched
                if should_upgrade_or_drop:
                    # New incident has additional URLs - mark for enrichment upgrade
                    # Update URLs and reset enrichment flag to allow re-enrichment with new URLs
                    existing_incident = load_incident_by_id(conn, duplicate_incident_id)
                    if existing_incident:
                        # Get existing URLs
                        existing_urls = set(existing_incident.all_urls or [])
                        new_urls = set(inc.all_urls or [])
                        
                        # Merge URLs (keep unique ones)
                        merged_urls = list(existing_urls | new_urls)
                        
                        # Update incident with merged URLs
                        existing_incident.all_urls = merged_urls
                        # Reset enrichment flag to allow re-enrichment with new URLs
                        # The enrichment pipeline will check if upgrade is needed based on confidence
                        conn.execute(
                            "UPDATE incidents SET llm_enriched = 0 WHERE incident_id = ?",
                            (duplicate_incident_id,)
                        )
                        # Update URLs and other fields (preserve_enrichment=False since we reset the flag)
                        insert_incident(conn, existing_incident, preserve_enrichment=False)
                        incident_id = duplicate_incident_id
                        logger.info(
                            f"Merged URLs for enriched incident {duplicate_incident_id} - "
                            f"marked for re-enrichment with additional URLs"
                        )
                    else:
                        # Shouldn't happen, but fallback
                        incident_id = insert_incident(conn, inc)
                else:
                    # All URLs are duplicates - drop new incident
                    logger.info(
                        f"Dropping incident {inc.incident_id} - all URLs are duplicates "
                        f"of enriched incident {duplicate_incident_id}"
                    )
                    # Still add source attribution but don't create new incident
                    incident_id = duplicate_incident_id
            else:
                # Step 3: Merge with existing incident (not enriched)
                existing_incident = load_incident_by_id(conn, duplicate_incident_id)
                if existing_incident:
                    # Merge incidents (keep highest confidence, merge URLs/metadata)
                    merged = merge_incidents([existing_incident, inc])
                    # Preserve the existing incident_id (important for foreign keys)
                    merged.incident_id = duplicate_incident_id
                    # Update existing incident with merged data
                    # preserve_enrichment=True ensures enrichment data is not lost during merge
                    insert_incident(conn, merged, preserve_enrichment=True)
                    incident_id = duplicate_incident_id
                else:
                    # Shouldn't happen, but fallback
                    incident_id = insert_incident(conn, inc)
        else:
            # Step 4: New incident - insert
            incident_id = insert_incident(conn, inc)
            new_count += 1

        # Step 5: Always add source attribution
        add_incident_source(
            conn,
            incident_id,
            source,
            inc.source_event_id,
            inc.ingested_at or "",
            inc.source_confidence,
        )

        # Step 6: Register source_event for per-source deduplication
        # For RSS feeds, this uses GUID as event_key (already set above)
        register_source_event(conn, source, event_key, incident_id, inc.ingested_at or "")

    conn.commit()
    return new_count


def _ingest_group(
    conn,
    label: str,
    collector,
    sources: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    max_age_days: Optional[int] = None,
    is_rss: bool = False,
) -> int:
    """
    Ingest a group of sources with incremental saving.
    Saves incidents as they are collected to prevent data loss on errors.
    """
    from src.edu_cti.pipeline.phase1.incremental_save import create_db_saver
    
    print(f"[*] Ingesting {label} …")
    
    # Build collector arguments
    collector_kwargs = {}
    if max_pages is not None:
        collector_kwargs["max_pages"] = max_pages
    if sources is not None:
        collector_kwargs["sources"] = sources
    if max_age_days is not None and is_rss:
        collector_kwargs["max_age_days"] = max_age_days
    
    # Check if collector supports incremental saving (has save_callback parameter)
    # If not, fall back to old behavior (collect all, then save)
    import inspect
    sig = inspect.signature(collector)
    supports_incremental = "save_callback" in sig.parameters
    
    if supports_incremental:
        # Use incremental saving - create saver and pass to collector
        # Source name will be extracted from incidents automatically
        saver = create_db_saver(conn, is_rss=is_rss, source_name=label)
        collector_kwargs["save_callback"] = saver.add_batch
        
        try:
            incidents_by_source: Dict[str, List[BaseIncident]] = collector(**collector_kwargs)
            
            # Save any remaining incidents from sources that didn't use callback
            new_total = 0
            for source_label, incidents in incidents_by_source.items():
                if incidents:  # Only save if there are incidents not yet saved
                    added = _ingest_batch(conn, incidents, is_rss=is_rss)
                    print(f"    {source_label}: {len(incidents)} incidents ({added} new)")
                    new_total += added
            
            # Finish incremental saver (saves any remaining buffered incidents)
            new_total += saver.finish()
            return new_total
        except Exception as e:
            # Save any buffered incidents before re-raising
            try:
                saver.flush()
            except:
                pass
            raise
    else:
        # Fall back to old behavior for collectors that don't support incremental saving
        incidents_by_source: Dict[str, List[BaseIncident]] = collector(**collector_kwargs)

    new_total = 0
    for source_label, incidents in incidents_by_source.items():
        added = _ingest_batch(conn, incidents, is_rss=is_rss)
        print(f"    {source_label}: {len(incidents)} incidents ({added} new)")
        new_total += added
    return new_total


def main() -> None:
    args = parse_args()
    selected_groups = list(dict.fromkeys(args.groups))
    
    conn = get_connection()
    init_db(conn)

    total_new = 0

    for group in selected_groups:
        label, collector = GROUP_COLLECTORS[group]
        is_rss = (group == "rss")
        
        # Determine sources parameter
        sources = None
        if args.sources is not None:
            if group == "news" or group == "rss":
                sources = args.sources
            else:
                print(f"Warning: --sources is not applicable for '{group}' group. Ignoring --sources for this group.")
        
        total_new += _ingest_group(
            conn,
            label,
            collector,
            sources=sources,
            max_pages=args.max_pages if not is_rss else None,
            max_age_days=args.rss_max_age_days if is_rss else None,
            is_rss=is_rss,
        )

    print(f"[done] Ingestion finished. Newly inserted incidents this run: {total_new}")


if __name__ == "__main__":
    main()
