# src/edu_cti/cli/build_dataset.py

import argparse
from pathlib import Path
from typing import List, Optional, Sequence

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase1.base_io import (
    PROC_DIR,
    ensure_dirs,
    write_base_csv,
)
from src.edu_cti.pipeline.phase1.curated import (
    run_curated_pipeline,
    CURATED_SOURCE_BUILDERS,
)
from src.edu_cti.pipeline.phase1.news import (
    run_news_pipeline,
    NEWS_SOURCE_BUILDERS,
)
from src.edu_cti.pipeline.phase1.rss import run_rss_pipeline
from src.edu_cti.core.sources import RSS_SOURCE_REGISTRY
from src.edu_cti.core.logging_utils import configure_logging
from src.edu_cti.core.deduplication import deduplicate_by_urls
from src.edu_cti.core.db import get_connection, load_all_incidents_from_db

GROUP_RUNNERS = {
    "curated": run_curated_pipeline,
    "news": run_news_pipeline,
    "rss": run_rss_pipeline,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build base dataset snapshots for EduThreat-CTI.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build dataset from all sources (default, fetches all pages)
  python -m src.edu_cti.cli.build_dataset

  # Build dataset from only news sources
  python -m src.edu_cti.cli.build_dataset --groups news

  # Build dataset from only curated sources
  python -m src.edu_cti.cli.build_dataset --groups curated

  # Build dataset from specific news sources
  python -m src.edu_cti.cli.build_dataset --groups news --news-sources darkreading krebsonsecurity

  # Build dataset from specific curated sources
  python -m src.edu_cti.cli.build_dataset --groups curated --curated-sources konbriefing

  # Build dataset with page limit (for testing)
  python -m src.edu_cti.cli.build_dataset --groups news --news-max-pages 10
  python -m src.edu_cti.cli.build_dataset --groups curated --news-max-pages 1  # Limit databreach pages
  
  # Fetch all pages explicitly
  python -m src.edu_cti.cli.build_dataset --groups news --news-max-pages all

  # Build from database (production mode - no re-scraping)
  python -m src.edu_cti.cli.build_dataset --from-database
        """,
    )
    parser.add_argument(
        "--groups",
        nargs="+",
        choices=list(GROUP_RUNNERS.keys()),
        default=list(GROUP_RUNNERS.keys()),
        help="Select which source groups to include. Defaults to all.",
    )
    parser.add_argument(
        "--rss-max-age-days",
        type=int,
        default=1,
        help="Maximum age in days for RSS feed items (default: 1). Only items published within this window are included.",
    )
    parser.add_argument(
        "--news-sources",
        nargs="+",
        choices=list(NEWS_SOURCE_BUILDERS.keys()),
        default=None,
        help="Select specific news sources to run. Only applies when --groups includes 'news'. "
             f"Valid sources: {', '.join(NEWS_SOURCE_BUILDERS.keys())}",
    )
    parser.add_argument(
        "--curated-sources",
        nargs="+",
        choices=list(CURATED_SOURCE_BUILDERS.keys()),
        default=None,
        help="Select specific curated sources to run. Only applies when --groups includes 'curated'. "
             f"Valid sources: {', '.join(CURATED_SOURCE_BUILDERS.keys())}",
    )
    parser.add_argument(
        "--news-max-pages",
        type=lambda x: None if x.lower() == "all" else int(x),
        default=None,
        help="Maximum number of pages to fetch per source. Applies to news sources and curated sources with pagination (e.g., databreach). "
             "Use 'all' to fetch all pages (default: all). "
             "Specify a number to limit pages (e.g., 10 for testing).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Path to log file (default: logs/pipeline.log).",
    )
    parser.add_argument(
        "--write-raw",
        action="store_true",
        help="Write per-source CSV snapshots to raw/ directory (for debugging). "
             "Only applies when building from sources (not from database).",
    )
    parser.add_argument(
        "--no-deduplication",
        action="store_true",
        help="Skip cross-source deduplication (keep all incidents even if URLs match).",
    )
    parser.add_argument(
        "--from-database",
        action="store_true",
        help="Build dataset from database instead of re-scraping sources. "
             "This is more efficient for re-runs and ensures DB and CSV stay in sync.",
    )
    return parser.parse_args()


def ensure_primary_url_is_none(incidents: List[BaseIncident]) -> List[BaseIncident]:
    """
    Ensure all incidents have primary_url=None and all URLs are in all_urls.
    This is required for Phase 1, where we collect all URLs and let Phase 2
    (LLM enrichment) select the best URL as primary_url.
    """
    fixed_count = 0
    for incident in incidents:
        # Collect all URLs that might be in primary_url
        urls_to_add = []
        
        # If primary_url exists, add it to all_urls
        if incident.primary_url:
            urls_to_add.append(incident.primary_url)
        
        # Ensure all URLs are unique
        existing_urls = set(incident.all_urls or [])
        for url in urls_to_add:
            if url and url not in existing_urls:
                existing_urls.add(url)
        
        # Update all_urls with deduplicated list
        if urls_to_add:
            incident.all_urls = list(existing_urls)
            incident.primary_url = None
            fixed_count += 1
    
    if fixed_count > 0:
        print(f"[*] Fixed {fixed_count} incidents: moved primary_url to all_urls")
    
    return incidents


def build_dataset(
    selected_groups: Sequence[str],
    *,
    news_max_pages: Optional[int] = None,
    news_sources: Optional[Sequence[str]] = None,
    curated_sources: Optional[Sequence[str]] = None,
    rss_max_age_days: int = 1,
    deduplicate: bool = True,
    from_database: bool = False,
    write_raw: bool = False,
) -> List[BaseIncident]:
    """
    Build dataset either from sources (fresh collection) or from database (existing data).
    
    Args:
        selected_groups: Source groups to include
        news_max_pages: Max pages for sources with pagination (news sources and curated sources like databreach).
                       Only used if from_database=False
        news_sources: Specific news sources (only used if from_database=False)
        curated_sources: Specific curated sources (only used if from_database=False)
        deduplicate: Whether to apply cross-source deduplication
        from_database: If True, load from database instead of re-scraping
        write_raw: If True, write per-source CSV snapshots to raw/ directory (for debugging).
                   Only used if from_database=False. Default False for production efficiency.
        
    Returns:
        List of BaseIncident objects (deduplicated if deduplicate=True)
    """
    ensure_dirs()
    
    if from_database:
        # Load from database (production-efficient mode)
        print("[*] Loading incidents from database...")
        conn = get_connection()
        all_incidents = load_all_incidents_from_db(conn)
        conn.close()
        print(f"[*] Loaded {len(all_incidents)} incidents from database")
    else:
        # Collect from sources (fresh collection)
        all_incidents: List[BaseIncident] = []
        for group in selected_groups:
            runner = GROUP_RUNNERS[group]
            print(f"[*] Building {group} incidents from sources…")
            kwargs = {}
            
            if group == "news":
                kwargs["max_pages"] = news_max_pages
                if news_sources is not None:
                    kwargs["sources"] = news_sources
            elif group == "curated":
                kwargs["max_pages"] = news_max_pages  # Use same max_pages for curated sources too
                if curated_sources is not None:
                    kwargs["sources"] = curated_sources
            elif group == "rss":
                kwargs["max_age_days"] = rss_max_age_days
            
            # Pass write_raw flag to pipeline runners (only when building from sources)
            kwargs["write_raw"] = write_raw
            
            results = runner(**kwargs)
            all_incidents.extend(results)

    # Ensure primary_url is None for all incidents (Phase 1 requirement)
    all_incidents = ensure_primary_url_is_none(all_incidents)

    # Note: Cross-source deduplication is now done at database ingestion level
    # If loading from database, incidents are already deduplicated
    # If loading from sources (fresh collection), we still need deduplication
    if from_database:
        # Incidents from database are already deduplicated
        print("[*] Incidents loaded from database (already deduplicated)")
    else:
        # Fresh collection - apply deduplication if enabled
        if deduplicate:
            print("[*] Deduplicating incidents across sources...")
            all_incidents, dedup_stats = deduplicate_by_urls(all_incidents)
            print(
                f"[*] Deduplication: {dedup_stats['total_input']} -> {dedup_stats['total_output']} "
                f"({dedup_stats['duplicates_merged']} groups merged, {dedup_stats['incidents_removed']} removed)"
            )
        else:
            print("[*] Skipping deduplication (--no-deduplication flag set)")

    return all_incidents


def main() -> None:
    args = parse_args()
    log_file = args.log_file or Path("logs/pipeline.log")
    configure_logging(args.log_level, log_file=log_file)
    selected_groups = list(dict.fromkeys(args.groups))  # preserve order, dedupe
    
    # Validate that source arguments are only used with their respective groups
    if args.news_sources is not None and "news" not in selected_groups:
        print("Warning: --news-sources is only applicable when --groups includes 'news'. Ignoring --news-sources.")
        args.news_sources = None
    
    if args.curated_sources is not None and "curated" not in selected_groups:
        print("Warning: --curated-sources is only applicable when --groups includes 'curated'. Ignoring --curated-sources.")
        args.curated_sources = None
    
    incidents = build_dataset(
        selected_groups,
        news_max_pages=args.news_max_pages,
        news_sources=args.news_sources,
        curated_sources=args.curated_sources,
        rss_max_age_days=args.rss_max_age_days,
        deduplicate=not args.no_deduplication,
        from_database=args.from_database,
        write_raw=args.write_raw,
    )

    if incidents:
        print("[*] Writing unified base_dataset.csv …")
        write_base_csv(PROC_DIR / "base_dataset.csv", incidents)
        print(f"[done] Unified base dataset size: {len(incidents)} incidents.")
    else:
        print("[warn] No incidents collected from any ingestor.")


if __name__ == "__main__":
    main()
