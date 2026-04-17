#!/usr/bin/env python3
"""
Unified Pipeline Orchestrator for EduThreat-CTI

Manages the complete pipeline lifecycle:
  1. Historical collection (2019+ full scrape, run once)
  2. Incremental daily collection (new incidents only)
  3. LLM enrichment (article fetching + CTI extraction)
  4. Optional CSV export of enriched data

Usage:
    # First-time setup: collect all historical data from 2019+
    python -m src.edu_cti.pipeline.orchestrator historical

    # Daily incremental run (ingest new + enrich unenriched)
    python -m src.edu_cti.pipeline.orchestrator daily

    # Only run ingestion (Phase 1)
    python -m src.edu_cti.pipeline.orchestrator ingest --full-historical

    # Only run enrichment (Phase 2)
    python -m src.edu_cti.pipeline.orchestrator enrich --limit 50

    # Full pipeline status
    python -m src.edu_cti.pipeline.orchestrator status
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def cmd_status(args):
    """Show pipeline status: DB stats, enrichment progress, source coverage."""
    from src.edu_cti.core.db import get_connection, init_db
    from src.edu_cti.pipeline.phase2.storage.db import get_enrichment_stats

    conn = get_connection()
    init_db(conn)

    # Basic counts
    total = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    enriched = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_enriched = 1").fetchone()[0]
    unenriched = total - enriched

    # Source breakdown
    sources = conn.execute(
        "SELECT source, COUNT(*) as cnt FROM incident_sources GROUP BY source ORDER BY cnt DESC"
    ).fetchall()

    # Country breakdown (top 10)
    countries = conn.execute(
        "SELECT country, COUNT(*) as cnt FROM incidents WHERE country IS NOT NULL AND country != '' "
        "GROUP BY country ORDER BY cnt DESC LIMIT 10"
    ).fetchall()

    # Date range
    date_range = conn.execute(
        "SELECT MIN(incident_date), MAX(incident_date) FROM incidents WHERE incident_date IS NOT NULL"
    ).fetchone()

    # Attack types
    attacks = conn.execute(
        "SELECT attack_type_hint, COUNT(*) as cnt FROM incidents WHERE attack_type_hint IS NOT NULL AND attack_type_hint != '' "
        "GROUP BY attack_type_hint ORDER BY cnt DESC"
    ).fetchall()

    # Enrichment stats
    enrich_stats = get_enrichment_stats(conn)

    conn.close()

    print("\n" + "=" * 60)
    print("  EduThreat-CTI Pipeline Status")
    print("=" * 60)

    print(f"\n  Total incidents:     {total}")
    print(f"  LLM enriched:       {enriched}")
    print(f"  Awaiting enrichment: {unenriched}")
    if total > 0:
        print(f"  Enrichment progress: {enriched/total*100:.1f}%")

    if date_range[0]:
        print(f"\n  Date range: {date_range[0]} → {date_range[1]}")

    if sources:
        print(f"\n  Sources ({len(sources)}):")
        for row in sources:
            print(f"    {row[0]:30s} {row[1]:>5d} incidents")

    if countries:
        print(f"\n  Top countries:")
        for row in countries:
            print(f"    {row[0]:30s} {row[1]:>5d}")

    if attacks:
        print(f"\n  Attack categories:")
        for row in attacks:
            print(f"    {row[0]:30s} {row[1]:>5d}")

    print(f"\n  Ready for enrichment: {enrich_stats.get('ready_for_enrichment', 0)}")
    print("=" * 60 + "\n")


def cmd_ingest(args):
    """Run Phase 1 ingestion pipeline."""
    from src.edu_cti.core.db import get_connection, init_db
    from src.edu_cti.pipeline.phase1.__main__ import GROUP_COLLECTORS, _ingest_group

    incremental = not args.full_historical
    mode = "INCREMENTAL" if incremental else "FULL HISTORICAL"

    print(f"\n[*] Phase 1: Ingestion ({mode} mode)")
    print("=" * 60)

    conn = get_connection()
    init_db(conn)

    total_new = 0
    groups = args.groups or ["curated", "news", "rss", "api"]

    for group in groups:
        label, collector = GROUP_COLLECTORS[group]
        is_rss = group == "rss"

        kwargs = {
            "sources": args.sources if group in ("curated", "news", "rss") else None,
            "max_pages": args.max_pages if not is_rss else None,
            "max_age_days": args.rss_max_age_days if is_rss else None,
            "is_rss": is_rss,
            "incremental": incremental,
            "include_paid_rss": getattr(args, "include_paid_rss", False) if is_rss else False,
        }

        try:
            total_new += _ingest_group(conn, label, collector, **kwargs)
        except Exception as e:
            logger.error(f"Error ingesting {label}: {e}", exc_info=True)
            print(f"    [!] Error in {label}: {e}")

    conn.close()
    print(f"\n[done] Ingestion complete. New incidents: {total_new}")
    return total_new


def cmd_enrich(args):
    """Run Phase 2 enrichment pipeline."""
    from src.edu_cti.core.db import get_connection, init_db
    from src.edu_cti.pipeline.phase2.__main__ import main as phase2_main

    # Phase 2 has its own arg parser; we call main() which reads sys.argv
    # Build the argv for phase2
    phase2_argv = []
    if args.limit:
        phase2_argv.extend(["--limit", str(args.limit)])
    if args.rate_limit_delay:
        phase2_argv.extend(["--rate-limit-delay", str(args.rate_limit_delay)])
    if getattr(args, "export_csv", False):
        phase2_argv.append("--export-csv")
    phase2_argv.extend(["--log-level", args.log_level])

    # Temporarily replace sys.argv for phase2's argparse
    original_argv = sys.argv
    sys.argv = ["phase2"] + phase2_argv
    try:
        phase2_main()
    finally:
        sys.argv = original_argv


def cmd_historical(args):
    """
    Run full historical collection pipeline.

    1. Full historical scrape of all sources (2019+)
    2. LLM enrichment of all collected incidents
    3. Optional CSV export when explicitly requested
    """
    from src.edu_cti.core.config import HISTORICAL_START_YEAR

    print("\n" + "=" * 60)
    print(f"  Historical Collection Pipeline (from {HISTORICAL_START_YEAR})")
    print("=" * 60)
    print(f"\n  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  This will:")
    print("    1. Scrape ALL pages from curated/news sources")
    print("    2. Collect RSS feeds (last 365 days)")
    print("    3. Collect API sources")
    print("    4. Run LLM enrichment on all unenriched incidents")
    print()

    start_time = time.time()

    # Phase 1: Full historical ingestion
    args.full_historical = True
    args.groups = ["curated", "news", "rss", "api"]
    args.sources = None
    args.max_pages = None
    args.rss_max_age_days = 365  # Get RSS items from last year
    args.include_paid_rss = True
    new_count = cmd_ingest(args)

    # Phase 2: Enrich all unenriched
    if not args.skip_enrich:
        print("\n" + "=" * 60)
        print("[*] Phase 2: LLM Enrichment")
        print("=" * 60)
        args.limit = args.enrich_limit
        args.rate_limit_delay = args.rate_limit_delay or 2.0
        cmd_enrich(args)

    elapsed = time.time() - start_time
    hours, remainder = divmod(int(elapsed), 3600)
    minutes, seconds = divmod(remainder, 60)

    print(f"\n{'=' * 60}")
    print(f"  Historical collection complete!")
    print(f"  Duration: {hours}h {minutes}m {seconds}s")
    print(f"  New incidents ingested: {new_count}")
    print(f"{'=' * 60}\n")


def cmd_daily(args):
    """
    Run daily incremental pipeline.

    1. Incremental ingestion (only new incidents)
    2. LLM enrichment of unenriched incidents
    3. Optional CSV export when explicitly requested
    """
    print("\n" + "=" * 60)
    print(f"  Daily Incremental Pipeline")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    start_time = time.time()

    # Phase 1: Incremental ingestion
    args.full_historical = False
    args.groups = ["curated", "news", "rss", "api"]
    args.sources = None
    args.max_pages = None
    args.rss_max_age_days = 7  # Last week for daily runs
    new_count = cmd_ingest(args)

    # Phase 2: Enrich unenriched
    if not args.skip_enrich:
        print("\n" + "=" * 60)
        print("[*] Phase 2: LLM Enrichment")
        print("=" * 60)
        args.limit = args.enrich_limit
        args.rate_limit_delay = args.rate_limit_delay or 2.0
        cmd_enrich(args)

    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)

    print(f"\n{'=' * 60}")
    print(f"  Daily pipeline complete! ({minutes}m {seconds}s)")
    print(f"  New incidents: {new_count}")
    print(f"{'=' * 60}\n")


def cmd_serve(args):
    """Start the API server."""
    import uvicorn

    print(f"\n[*] Starting EduThreat-CTI API server on {args.host}:{args.port}")
    print(f"    Swagger UI: http://{args.host}:{args.port}/docs")
    print(f"    ReDoc:      http://{args.host}:{args.port}/redoc")

    uvicorn.run(
        "src.edu_cti.api.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level.lower(),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eduthreat-cti",
        description="EduThreat-CTI Unified Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  status       Show pipeline status and statistics
  historical   Run full historical collection (2019+) then enrich
  daily        Run incremental collection + enrichment (for cron/daily use)
  ingest       Run Phase 1 ingestion only
  enrich       Run Phase 2 LLM enrichment only
  serve        Start the API server

Examples:
  # First-time setup
  python -m src.edu_cti.pipeline.orchestrator historical

  # Daily cron job
  python -m src.edu_cti.pipeline.orchestrator daily

  # Check pipeline status
  python -m src.edu_cti.pipeline.orchestrator status

  # Run ingestion for specific sources
  python -m src.edu_cti.pipeline.orchestrator ingest --groups curated --sources konbriefing

  # Enrich 50 incidents
  python -m src.edu_cti.pipeline.orchestrator enrich --limit 50

  # Start API server
  python -m src.edu_cti.pipeline.orchestrator serve --port 8000
        """,
    )

    # Global options
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Pipeline command")

    # --- status ---
    subparsers.add_parser("status", help="Show pipeline status and statistics")

    # --- ingest ---
    ingest_parser = subparsers.add_parser("ingest", help="Run Phase 1 ingestion")
    ingest_parser.add_argument(
        "--groups", nargs="+",
        choices=["curated", "news", "rss", "api"],
        default=["curated", "news", "rss", "api"],
        help="Source groups to ingest",
    )
    ingest_parser.add_argument(
        "--sources", nargs="+", default=None,
        help="Specific sources within a group",
    )
    ingest_parser.add_argument(
        "--max-pages",
        type=lambda x: None if x.lower() == "all" else int(x),
        default=None,
        help="Max pages per source (default: all)",
    )
    ingest_parser.add_argument(
        "--rss-max-age-days", type=int, default=30,
        help="Max age for RSS items in days (default: 30)",
    )
    ingest_parser.add_argument(
        "--full-historical", action="store_true",
        help="Full historical scrape (all pages)",
    )
    ingest_parser.add_argument(
        "--include-paid-rss",
        action="store_true",
        help="Include paid RSS/search sources such as oxylabs_news during RSS ingestion.",
    )

    # --- enrich ---
    enrich_parser = subparsers.add_parser("enrich", help="Run Phase 2 LLM enrichment")
    enrich_parser.add_argument(
        "--limit", type=int, default=None,
        help="Max incidents to enrich (default: all unenriched)",
    )
    enrich_parser.add_argument(
        "--rate-limit-delay", type=float, default=2.0,
        help="Delay between LLM calls in seconds (default: 2.0)",
    )
    enrich_parser.add_argument(
        "--export-csv", action="store_true",
        help="Export enriched dataset to CSV after completion",
    )

    # --- historical ---
    hist_parser = subparsers.add_parser(
        "historical",
        help="Run full historical collection (2019+) then enrich",
    )
    hist_parser.add_argument(
        "--skip-enrich", action="store_true",
        help="Skip LLM enrichment after ingestion",
    )
    hist_parser.add_argument(
        "--enrich-limit", type=int, default=None,
        help="Max incidents to enrich (default: all)",
    )
    hist_parser.add_argument(
        "--rate-limit-delay", type=float, default=2.0,
        help="Delay between LLM calls (default: 2.0)",
    )
    hist_parser.add_argument(
        "--export-csv", action="store_true",
        help="Export enriched dataset to CSV after completion",
    )

    # --- daily ---
    daily_parser = subparsers.add_parser(
        "daily",
        help="Run daily incremental pipeline (ingest + enrich)",
    )
    daily_parser.add_argument(
        "--skip-enrich", action="store_true",
        help="Skip LLM enrichment",
    )
    daily_parser.add_argument(
        "--enrich-limit", type=int, default=None,
        help="Max incidents to enrich per run (default: all)",
    )
    daily_parser.add_argument(
        "--rate-limit-delay", type=float, default=2.0,
        help="Delay between LLM calls (default: 2.0)",
    )
    daily_parser.add_argument(
        "--export-csv", action="store_true",
        help="Export enriched dataset to CSV after completion",
    )

    # --- serve ---
    serve_parser = subparsers.add_parser("serve", help="Start the API server")
    serve_parser.add_argument(
        "--host", default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )
    serve_parser.add_argument(
        "--port", type=int, default=8000,
        help="Port to bind to (default: 8000)",
    )
    serve_parser.add_argument(
        "--reload", action="store_true",
        help="Enable auto-reload for development",
    )

    return parser


def main():
    from src.edu_cti.core.logging_utils import configure_logging

    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    configure_logging(args.log_level, phase="orchestrator")

    commands = {
        "status": cmd_status,
        "ingest": cmd_ingest,
        "enrich": cmd_enrich,
        "historical": cmd_historical,
        "daily": cmd_daily,
        "serve": cmd_serve,
    }

    handler = commands.get(args.command)
    if handler:
        handler(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
