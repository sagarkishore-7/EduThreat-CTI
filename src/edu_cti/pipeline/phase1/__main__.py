"""
Phase 1: Ingestion Pipeline CLI

Main entry point for Phase 1 ingestion pipeline.

Supports incremental ingestion:
- Default (incremental): Only fetch new incidents since last run
- --full-historical: Fetch all pages/incidents (first-time run)
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
  # Incremental run (default) - only fetch new incidents
  python -m src.edu_cti.pipeline.phase1

  # Full historical run - fetch all pages/incidents
  python -m src.edu_cti.pipeline.phase1 --full-historical

  # Run only curated sources
  python -m src.edu_cti.pipeline.phase1 --groups curated

  # Run specific sources with page limit
  python -m src.edu_cti.pipeline.phase1 --groups curated --sources databreach --max-pages 10
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
        default=30,
        help="Maximum age in days for RSS feed items (default: 30). Only items published within this window are included.",
    )
    parser.add_argument(
        "--full-historical",
        action="store_true",
        help="Perform full historical scrape (fetch all pages/incidents). "
             "By default, incremental mode is used which only fetches new incidents.",
    )
    return parser.parse_args()


def _event_key_for_incident(incident: BaseIncident) -> str:
    """
    For per-source dedup, prefer source_event_id; fallback to first URL in all_urls; then incident_id.
    Phase 1: primary_url is None, so we use all_urls[0] if available.
    """
    if incident.source_event_id:
        return incident.source_event_id
    if incident.all_urls and len(incident.all_urls) > 0:
        return incident.all_urls[0]
    if incident.primary_url:
        return incident.primary_url
    return incident.incident_id


def _ingest_batch(conn, incidents: List[BaseIncident], is_rss: bool = False) -> int:
    """
    Insert incidents into DB with cross-source deduplication.
    
    Returns number of newly inserted/updated incidents.
    """
    from src.edu_cti.core.deduplication import merge_incidents
    
    new_count = 0
    for inc in incidents:
        source = inc.source
        event_key = _event_key_for_incident(inc)

        if not event_key:
            event_key = inc.incident_id

        # Step 1: Check per-source deduplication
        # BUT: Allow updates if existing incident has broken URLs and new incident has new URLs
        if source_event_exists(conn, source, event_key):
            # Check if we should allow update due to broken URLs
            from src.edu_cti.core.db import find_duplicate_incident_by_urls, has_broken_urls
            duplicate_result = find_duplicate_incident_by_urls(conn, inc)
            
            if duplicate_result:
                duplicate_incident_id, _, _ = duplicate_result
                if has_broken_urls(conn, duplicate_incident_id):
                    # Existing incident has broken URLs - check if new incident has different URLs
                    existing_incident = load_incident_by_id(conn, duplicate_incident_id)
                    if existing_incident:
                        existing_urls = set(existing_incident.all_urls or [])
                        new_urls = set(inc.all_urls or [])
                        # If new incident has URLs not in existing incident, allow update
                        if new_urls - existing_urls:
                            logger.info(
                                f"Allowing URL update for incident {duplicate_incident_id} "
                                f"(has broken URLs, new URLs available from {source})"
                            )
                            # Continue to cross-source deduplication logic below
                        else:
                            # No new URLs, skip
                            continue
                else:
                    # No broken URLs, skip per normal deduplication
                    continue
            else:
                # Not a duplicate, skip per normal deduplication
                continue

        # Step 2: Check for cross-source duplicates (URL matching)
        duplicate_result = find_duplicate_incident_by_urls(conn, inc)
        
        if duplicate_result:
            duplicate_incident_id, is_enriched, should_upgrade_or_drop = duplicate_result
            
            if is_enriched:
                if should_upgrade_or_drop:
                    existing_incident = load_incident_by_id(conn, duplicate_incident_id)
                    if existing_incident:
                        existing_urls = set(existing_incident.all_urls or [])
                        new_urls = set(inc.all_urls or [])
                        merged_urls = list(existing_urls | new_urls)
                        existing_incident.all_urls = merged_urls
                        conn.execute(
                            "UPDATE incidents SET llm_enriched = 0 WHERE incident_id = ?",
                            (duplicate_incident_id,)
                        )
                        insert_incident(conn, existing_incident, preserve_enrichment=False)
                        incident_id = duplicate_incident_id
                        logger.info(f"Merged URLs for enriched incident {duplicate_incident_id}")
                    else:
                        incident_id = insert_incident(conn, inc)
                else:
                    logger.info(f"Dropping incident {inc.incident_id} - duplicate of {duplicate_incident_id}")
                    incident_id = duplicate_incident_id
            else:
                # Step 3: Merge with existing incident (not enriched)
                existing_incident = load_incident_by_id(conn, duplicate_incident_id)
                if existing_incident:
                    merged = merge_incidents([existing_incident, inc])
                    merged.incident_id = duplicate_incident_id
                    insert_incident(conn, merged, preserve_enrichment=True)
                    incident_id = duplicate_incident_id
                else:
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
    incremental: bool = True,
) -> int:
    """
    Ingest a group of sources with incremental saving.
    
    Args:
        incremental: If True, use incremental ingestion (only new incidents)
    """
    from src.edu_cti.pipeline.phase1.incremental_save import create_db_saver
    
    mode = "incremental" if incremental else "full historical"
    print(f"[*] Ingesting {label} ({mode} mode)…")
    
    # Build collector arguments
    collector_kwargs = {}
    if max_pages is not None:
        collector_kwargs["max_pages"] = max_pages
    if sources is not None:
        collector_kwargs["sources"] = sources
    if max_age_days is not None and is_rss:
        collector_kwargs["max_age_days"] = max_age_days
    
    # Pass incremental flag
    collector_kwargs["incremental"] = incremental
    
    # Check if collector supports incremental saving (has save_callback parameter)
    import inspect
    sig = inspect.signature(collector)
    supports_incremental = "save_callback" in sig.parameters
    
    if supports_incremental:
        saver = create_db_saver(conn, is_rss=is_rss, source_name=label)
        collector_kwargs["save_callback"] = saver.add_batch
        
        try:
            incidents_by_source: Dict[str, List[BaseIncident]] = collector(**collector_kwargs)
            
            new_total = 0
            for source_label, incidents in incidents_by_source.items():
                if incidents:
                    added = _ingest_batch(conn, incidents, is_rss=is_rss)
                    print(f"    {source_label}: {len(incidents)} incidents ({added} new)")
                    new_total += added
            
            new_total += saver.finish()
            return new_total
        except Exception as e:
            try:
                saver.flush()
            except:
                pass
            raise
    else:
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
    
    # Determine incremental mode
    incremental = not args.full_historical
    
    if incremental:
        print("[*] Running in INCREMENTAL mode (only new incidents)")
    else:
        print("[*] Running in FULL HISTORICAL mode (all pages/incidents)")
    
    conn = get_connection()
    init_db(conn)

    total_new = 0

    for group in selected_groups:
        label, collector = GROUP_COLLECTORS[group]
        is_rss = (group == "rss")
        
        sources = None
        if args.sources is not None:
            if group == "news" or group == "rss" or group == "curated":
                sources = args.sources
            else:
                print(f"Warning: --sources is not applicable for '{group}' group.")
        
        total_new += _ingest_group(
            conn,
            label,
            collector,
            sources=sources,
            max_pages=args.max_pages if not is_rss else None,
            max_age_days=args.rss_max_age_days if is_rss else None,
            is_rss=is_rss,
            incremental=incremental,
        )

    print(f"[done] Ingestion finished. Newly inserted incidents this run: {total_new}")


if __name__ == "__main__":
    main()
