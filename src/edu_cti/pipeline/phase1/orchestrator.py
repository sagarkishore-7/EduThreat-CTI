#!/usr/bin/env python3
"""
Main pipeline script for EduThreat-CTI Phase 1.

This script:
1. Initializes the SQLite database
2. Runs the ingestion pipeline to collect incidents from all sources
3. Builds the unified base dataset CSV
4. Ensures all URLs are collected in all_urls field with primary_url=None

Usage:
    python -m src.edu_cti.cli.pipeline
    python -m src.edu_cti.cli.pipeline --groups news
    python -m src.edu_cti.cli.pipeline --groups curated news
"""

import argparse
from pathlib import Path
from typing import Sequence

from src.edu_cti.core.db import get_connection, init_db
from src.edu_cti.pipeline.phase1.build_dataset import build_dataset
from src.edu_cti.pipeline.phase1.base_io import PROC_DIR, ensure_dirs, write_base_csv
from src.edu_cti.pipeline.phase1.__main__ import (
    GROUP_COLLECTORS,
    _ingest_group,
)
from src.edu_cti.core.logging_utils import configure_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the complete EduThreat-CTI Phase 1 pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline (all sources)
  python -m src.edu_cti.run_pipeline

  # Run only news sources
  python -m src.edu_cti.run_pipeline --groups news

  # Run only curated sources
  python -m src.edu_cti.cli.pipeline --groups curated

  # Run with page limits (for testing)
  python -m src.edu_cti.run_pipeline --news-max-pages 10
  
  # Fetch all pages (explicit)
  python -m src.edu_cti.run_pipeline --news-max-pages all

  # Run specific news sources
  python -m src.edu_cti.run_pipeline --groups news --news-sources darkreading krebsonsecurity
        """,
    )
    parser.add_argument(
        "--groups",
        nargs="+",
        choices=["curated", "news", "rss"],
        default=["curated", "news", "rss"],
        help="Select which source groups to process. Defaults to all.",
    )
    parser.add_argument(
        "--news-sources",
        nargs="+",
        default=None,
        help="Select specific news sources to run (only applies with --groups news).",
    )
    parser.add_argument(
        "--curated-sources",
        nargs="+",
        default=None,
        help="Select specific curated sources to run (only applies with --groups curated).",
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
        "--rss-max-age-days",
        type=int,
        default=1,
        help="Maximum age in days for RSS feed items (default: 1). Only items published within this window are included.",
    )
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip database ingestion step (only build dataset CSV).",
    )
    parser.add_argument(
        "--skip-dataset",
        action="store_true",
        help="Skip dataset building step (only run ingestion).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Path to log file (default: logs/pipeline.log).",
    )
    parser.add_argument(
        "--no-deduplication",
        action="store_true",
        help="Skip cross-source deduplication (keep all incidents even if URLs match).",
    )
    parser.add_argument(
        "--fresh-collection",
        action="store_true",
        help="Re-scrape sources for CSV building instead of using database. "
             "By default, CSV is built from database for efficiency.",
    )
    return parser.parse_args()




def main() -> None:
    args = parse_args()
    
    # Setup logging
    log_file = args.log_file or Path("logs/pipeline.log")
    configure_logging(args.log_level, log_file=log_file)
    
    # Ensure directories exist
    ensure_dirs()
    
    # Initialize database
    print("[*] Initializing database...")
    conn = get_connection()
    init_db(conn)
    conn.close()
    print("[✓] Database initialized")
    
    # Step 1: Run ingestion pipeline (collects incidents into database)
    if not args.skip_ingestion:
        print("\n" + "="*70)
        print("[*] Step 1: Running ingestion pipeline...")
        print("="*70)
        
        conn = get_connection()
        init_db(conn)
        
        total_new = 0
        for group in args.groups:
            label, collector = GROUP_COLLECTORS[group]
            is_rss = (group == "rss")
            if group == "curated":
                total_new += _ingest_group(
                    conn,
                    label,
                    collector,
                    sources=args.curated_sources,
                    max_pages=args.news_max_pages,  # Use same max_pages arg for curated sources too
                )
            elif group == "news":
                total_new += _ingest_group(
                    conn,
                    label,
                    collector,
                    sources=args.news_sources,
                    max_pages=args.news_max_pages,
                )
            elif group == "rss":
                total_new += _ingest_group(
                    conn,
                    label,
                    collector,
                    sources=None,  # RSS sources can be specified via --sources in ingestion.py
                    max_age_days=args.rss_max_age_days,
                    is_rss=True,
                )
        
        conn.close()
        print(f"[✓] Ingestion pipeline completed. Newly inserted incidents: {total_new}")
    else:
        print("[*] Skipping ingestion step (--skip-ingestion)")
    
    # Step 2: Build unified base dataset CSV
    if not args.skip_dataset:
        print("\n" + "="*70)
        print("[*] Step 2: Building unified base dataset...")
        print("="*70)
        
        # Build dataset from database (production-efficient: no re-scraping)
        # This ensures DB and CSV stay in sync and only processes new data
        # Use --fresh-collection to re-scrape instead (for testing/debugging)
        incidents = build_dataset(
            args.groups,
            news_max_pages=args.news_max_pages,
            news_sources=args.news_sources,
            curated_sources=args.curated_sources,
            deduplicate=not args.no_deduplication,
            from_database=not args.fresh_collection,  # Use DB by default (production mode)
        )
        
        # Write unified dataset
        if incidents:
            output_path = PROC_DIR / "base_dataset.csv"
            print(f"[*] Writing unified base dataset to {output_path}...")
            write_base_csv(output_path, incidents)
            print(f"[✓] Base dataset written: {len(incidents)} incidents")
            print(f"[✓] Output: {output_path}")
        else:
            print("[warn] No incidents collected from any source.")
        
        print("[✓] Dataset building completed")
    else:
        print("[*] Skipping dataset building step (--skip-dataset)")
    
    print("\n" + "="*70)
    print("[✓] Phase 1 pipeline completed successfully!")
    print("="*70)
    print("\nNext steps:")
    print("  - Review data/processed/base_dataset.csv")
    print("  - All URLs are in 'all_urls' field (primary_url=None)")
    print("  - Ready for Phase 2: LLM enrichment to select best URL")


if __name__ == "__main__":
    main()

