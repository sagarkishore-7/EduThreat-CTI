"""
Scheduler for continuous data ingestion and enrichment pipeline.

Implements:
- Weekly historical ingestion (curated + news sources)
- RSS feeds every 2 hours for real-time news
- Continuous LLM enrichment of unenriched incidents
"""

import os
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional
import schedule

from src.edu_cti.core.db import get_connection, init_db
from src.edu_cti.core.config import DATA_DIR
from src.edu_cti.pipeline.phase1.__main__ import GROUP_COLLECTORS, _ingest_group
from src.edu_cti.pipeline.phase1.base_io import ensure_dirs

logger = logging.getLogger(__name__)


class IngestionScheduler:
    """
    Manages scheduled data ingestion and enrichment tasks.
    
    Default schedules:
    - RSS feeds: Every 2 hours
    - Full ingestion (curated + news): Weekly (Sunday at 2 AM)
    - LLM enrichment: Continuous (runs after each ingestion cycle)
    """
    
    def __init__(
        self,
        rss_interval_hours: int = 2,
        weekly_day: str = "sunday",
        weekly_time: str = "02:00",
        enable_enrichment: bool = True,
        enrichment_batch_size: int = 10,
        enrichment_delay: float = 1.0,
    ):
        """
        Initialize the scheduler.
        
        Args:
            rss_interval_hours: Hours between RSS feed checks
            weekly_day: Day of week for full ingestion (lowercase)
            weekly_time: Time for weekly ingestion (HH:MM format)
            enable_enrichment: Whether to run LLM enrichment after ingestion
            enrichment_batch_size: Number of incidents to enrich per batch
            enrichment_delay: Delay between enrichment calls (rate limiting)
        """
        self.rss_interval_hours = rss_interval_hours
        self.weekly_day = weekly_day
        self.weekly_time = weekly_time
        self.enable_enrichment = enable_enrichment
        self.enrichment_batch_size = enrichment_batch_size
        self.enrichment_delay = enrichment_delay
        
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._last_rss_run: Optional[datetime] = None
        self._last_weekly_run: Optional[datetime] = None
        
    def _run_rss_ingestion(self):
        """Run RSS feed ingestion."""
        from src.edu_cti.core.metrics import get_metrics, start_timer, stop_timer, increment
        
        metrics = get_metrics()
        start_timer("rss_ingestion")
        
        logger.info("="*70)
        logger.info("[SCHEDULER] Starting RSS feed ingestion...")
        logger.info("="*70)
        print("="*70, flush=True)
        print("[SCHEDULER] Starting RSS feed ingestion...", flush=True)
        print("="*70, flush=True)
        
        try:
            ensure_dirs()
            conn = get_connection()
            init_db(conn)
            
            # Get count before
            cur = conn.execute("SELECT COUNT(*) FROM incidents")
            count_before = cur.fetchone()[0]
            print(f"[SCHEDULER] Current incidents in DB: {count_before}", flush=True)
            
            label, collector = GROUP_COLLECTORS["rss"]
            print(f"[SCHEDULER] Running RSS collector: {label}", flush=True)
            
            new_count = _ingest_group(
                conn,
                label,
                collector,
                sources=None,
                max_age_days=30,
                is_rss=True,
                incremental=True,
            )
            
            # Get count after
            cur = conn.execute("SELECT COUNT(*) FROM incidents")
            count_after = cur.fetchone()[0]
            
            conn.close()
            self._last_rss_run = datetime.now()
            
            duration = stop_timer("rss_ingestion")
            increment("rss_ingestion_incidents", new_count)
            increment("rss_ingestion_runs", labels={"status": "success"})
            
            logger.info(f"[SCHEDULER] RSS ingestion complete. New incidents: {new_count} (total: {count_after})")
            print(f"[SCHEDULER] ✓ RSS ingestion complete!", flush=True)
            print(f"[SCHEDULER]   New incidents: {new_count}", flush=True)
            print(f"[SCHEDULER]   Total incidents: {count_after} (was {count_before})", flush=True)
            print(f"[SCHEDULER]   Duration: {duration:.2f}s", flush=True)
            
            # Run enrichment if enabled
            if self.enable_enrichment and new_count > 0:
                print(f"[SCHEDULER] Triggering enrichment for {new_count} new incidents...", flush=True)
                self._run_enrichment()
            else:
                print(f"[SCHEDULER] Skipping enrichment (enabled={self.enable_enrichment}, new={new_count})", flush=True)
                
        except Exception as e:
            stop_timer("rss_ingestion")
            increment("rss_ingestion_runs", labels={"status": "error"})
            logger.error(f"[SCHEDULER] RSS ingestion failed: {e}", exc_info=True)
            print(f"[SCHEDULER] ✗ RSS ingestion failed: {e}", flush=True)
            raise
    
    def _run_weekly_ingestion(self):
        """Run full weekly ingestion (curated + news)."""
        from src.edu_cti.core.metrics import get_metrics, start_timer, stop_timer, increment
        
        metrics = get_metrics()
        start_timer("weekly_ingestion")
        
        logger.info("="*70)
        logger.info("[SCHEDULER] Starting weekly full ingestion...")
        logger.info("="*70)
        print("="*70, flush=True)
        print("[SCHEDULER] Starting weekly full ingestion...", flush=True)
        print("="*70, flush=True)
        
        try:
            ensure_dirs()
            conn = get_connection()
            init_db(conn)
            
            # Get count before
            cur = conn.execute("SELECT COUNT(*) FROM incidents")
            count_before = cur.fetchone()[0]
            print(f"[SCHEDULER] Current incidents in DB: {count_before}", flush=True)
            
            total_new = 0
            
            # Run curated sources
            logger.info("[SCHEDULER] Running curated sources...")
            print("[SCHEDULER] Running curated sources...", flush=True)
            label, collector = GROUP_COLLECTORS["curated"]
            curated_new = _ingest_group(
                conn,
                label,
                collector,
                sources=None,
                max_pages=None,  # Fetch all new pages
                incremental=True,
            )
            total_new += curated_new
            print(f"[SCHEDULER]   Curated sources: {curated_new} new incidents", flush=True)
            
            # Run news sources
            logger.info("[SCHEDULER] Running news sources...")
            print("[SCHEDULER] Running news sources...", flush=True)
            label, collector = GROUP_COLLECTORS["news"]
            news_new = _ingest_group(
                conn,
                label,
                collector,
                sources=None,
                max_pages=None,  # Fetch all new pages
                incremental=True,
            )
            total_new += news_new
            print(f"[SCHEDULER]   News sources: {news_new} new incidents", flush=True)
            
            # Get count after
            cur = conn.execute("SELECT COUNT(*) FROM incidents")
            count_after = cur.fetchone()[0]
            
            conn.close()
            self._last_weekly_run = datetime.now()
            
            duration = stop_timer("weekly_ingestion")
            increment("weekly_ingestion_incidents", total_new)
            increment("weekly_ingestion_runs", labels={"status": "success"})
            
            logger.info(f"[SCHEDULER] Weekly ingestion complete. New incidents: {total_new} (total: {count_after})")
            print(f"[SCHEDULER] ✓ Weekly ingestion complete!", flush=True)
            print(f"[SCHEDULER]   New incidents: {total_new} (curated: {curated_new}, news: {news_new})", flush=True)
            print(f"[SCHEDULER]   Total incidents: {count_after} (was {count_before})", flush=True)
            print(f"[SCHEDULER]   Duration: {duration:.2f}s", flush=True)
            
            # Run enrichment if enabled
            if self.enable_enrichment and total_new > 0:
                print(f"[SCHEDULER] Triggering enrichment for {total_new} new incidents...", flush=True)
                self._run_enrichment()
            else:
                print(f"[SCHEDULER] Skipping enrichment (enabled={self.enable_enrichment}, new={total_new})", flush=True)
                
        except Exception as e:
            stop_timer("weekly_ingestion")
            increment("weekly_ingestion_runs", labels={"status": "error"})
            logger.error(f"[SCHEDULER] Weekly ingestion failed: {e}", exc_info=True)
            print(f"[SCHEDULER] ✗ Weekly ingestion failed: {e}", flush=True)
            raise
    
    def _run_enrichment(self, limit: Optional[int] = None, manual_trigger: bool = False) -> None:
        """
        Run LLM enrichment on unenriched incidents.
        
        Args:
            limit: Maximum number of incidents to process. If None, processes all unenriched.
        """
        from src.edu_cti.core.metrics import get_metrics, start_timer, stop_timer, increment
        
        metrics = get_metrics()
        start_timer("enrichment")
        
        logger.info("[SCHEDULER] Running LLM enrichment...")
        print("[SCHEDULER] Running LLM enrichment...", flush=True)
        
        # Mark if this is a manual trigger
        self._manual_trigger = manual_trigger
        
        try:
            from src.edu_cti.pipeline.phase2.__main__ import main as enrich_main
            from src.edu_cti.core.db import get_connection
            import sys
            
            # Get count before
            conn = get_connection()
            cur = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_enriched = 1")
            enriched_before = cur.fetchone()[0]
            cur = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_enriched = 0")
            unenriched_before = cur.fetchone()[0]
            conn.close()
            
            print(f"[SCHEDULER] Current status: {enriched_before} enriched, {unenriched_before} unenriched", flush=True)
            
            # Build args for enrichment (use valid arguments only)
            original_argv = sys.argv
            args = [
                "eduthreat-enrich",
                "--rate-limit-delay", str(self.enrichment_delay),
                "--skip-non-education",  # Skip incidents not related to education
            ]
            
            # Only add --limit if explicitly specified
            # When limit=None is passed (manual trigger), process ALL unenriched incidents
            # When limit is not passed (automatic trigger), use batch size
            if limit is not None:
                # Explicit limit provided (e.g., limit=50)
                args.extend(["--limit", str(limit)])
            elif manual_trigger:
                # Manual trigger with limit=None means process all
                # Don't add --limit flag - process all unenriched incidents
                pass
            elif self.enrichment_batch_size > 0:
                # Automatic trigger: use batch size
                args.extend(["--limit", str(self.enrichment_batch_size)])
            # If both are None/0, process all (no --limit flag)
            
            sys.argv = args
            
            try:
                enrich_main()
            finally:
                sys.argv = original_argv
            
            # Get count after
            conn = get_connection()
            cur = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_enriched = 1")
            enriched_after = cur.fetchone()[0]
            cur = conn.execute("SELECT COUNT(*) FROM incidents WHERE llm_enriched = 0")
            unenriched_after = cur.fetchone()[0]
            conn.close()
            
            duration = stop_timer("enrichment")
            new_enriched = enriched_after - enriched_before
            increment("enrichment_incidents", new_enriched)
            increment("enrichment_runs", labels={"status": "success"})
            
            logger.info(f"[SCHEDULER] LLM enrichment complete. New enriched: {new_enriched}")
            print(f"[SCHEDULER] ✓ LLM enrichment complete!", flush=True)
            print(f"[SCHEDULER]   New enriched: {new_enriched}", flush=True)
            print(f"[SCHEDULER]   Total enriched: {enriched_after} (was {enriched_before})", flush=True)
            print(f"[SCHEDULER]   Remaining unenriched: {unenriched_after}", flush=True)
            print(f"[SCHEDULER]   Duration: {duration:.2f}s", flush=True)
            
        except SystemExit as e:
            # argparse exits with SystemExit(2) on errors
            if e.code == 2:
                logger.error("[SCHEDULER] LLM enrichment argument error - check CLI arguments")
                print(f"[SCHEDULER] ✗ LLM enrichment failed: Invalid arguments", flush=True)
                increment("enrichment_runs", labels={"status": "error"})
                raise
            raise
        except Exception as e:
            stop_timer("enrichment")
            increment("enrichment_runs", labels={"status": "error"})
            logger.error(f"[SCHEDULER] LLM enrichment failed: {e}", exc_info=True)
            print(f"[SCHEDULER] ✗ LLM enrichment failed: {e}", flush=True)
            raise
    
    def _scheduler_loop(self):
        """Main scheduler loop."""
        logger.info("[SCHEDULER] Starting scheduler loop...")
        
        while self._running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def start(self, run_initial_rss: bool = False, run_initial_weekly: bool = False):
        """
        Start the scheduler.
        
        Args:
            run_initial_rss: If True, run RSS ingestion immediately on start
            run_initial_weekly: If True, run weekly ingestion immediately on start
        """
        if self._running:
            logger.warning("[SCHEDULER] Scheduler already running")
            return
        
        self._running = True
        
        # Setup schedules
        # RSS every N hours
        schedule.every(self.rss_interval_hours).hours.do(self._run_rss_ingestion)
        logger.info(f"[SCHEDULER] RSS ingestion scheduled every {self.rss_interval_hours} hours")
        
        # Weekly full ingestion
        getattr(schedule.every(), self.weekly_day).at(self.weekly_time).do(self._run_weekly_ingestion)
        logger.info(f"[SCHEDULER] Weekly ingestion scheduled: {self.weekly_day} at {self.weekly_time}")
        
        # Run initial jobs if requested
        if run_initial_rss:
            logger.info("[SCHEDULER] Running initial RSS ingestion...")
            self._run_rss_ingestion()
        
        if run_initial_weekly:
            logger.info("[SCHEDULER] Running initial weekly ingestion...")
            self._run_weekly_ingestion()
        
        # Start scheduler thread
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        
        logger.info("[SCHEDULER] Scheduler started")
    
    def stop(self):
        """Stop the scheduler."""
        self._running = False
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        schedule.clear()
        logger.info("[SCHEDULER] Scheduler stopped")
    
    def get_status(self) -> dict:
        """Get current scheduler status."""
        return {
            "running": self._running,
            "rss_interval_hours": self.rss_interval_hours,
            "weekly_schedule": f"{self.weekly_day} at {self.weekly_time}",
            "last_rss_run": self._last_rss_run.isoformat() if self._last_rss_run else None,
            "last_weekly_run": self._last_weekly_run.isoformat() if self._last_weekly_run else None,
            "enrichment_enabled": self.enable_enrichment,
            "next_jobs": [str(job) for job in schedule.get_jobs()[:5]],
        }


def run_once_historical():
    """Run a one-time full historical ingestion (first-time setup)."""
    logger.info("="*70)
    logger.info("[HISTORICAL] Running one-time full historical ingestion...")
    logger.info("[HISTORICAL] This will fetch ALL pages from all sources.")
    logger.info("[HISTORICAL] WARNING: This may take several hours!")
    logger.info("="*70)
    
    try:
        ensure_dirs()
        conn = get_connection()
        init_db(conn)
        
        total_new = 0
        
        # Run all groups in full historical mode
        for group in ["curated", "news", "rss"]:
            label, collector = GROUP_COLLECTORS[group]
            is_rss = (group == "rss")
            
            logger.info(f"[HISTORICAL] Running {group} sources (full historical)...")
            
            kwargs = {
                "sources": None,
                "incremental": False,  # Full historical
            }
            
            if is_rss:
                kwargs["max_age_days"] = 365  # Get 1 year of RSS
                kwargs["is_rss"] = True
            else:
                kwargs["max_pages"] = None  # All pages
            
            total_new += _ingest_group(conn, label, collector, **kwargs)
        
        conn.close()
        logger.info(f"[HISTORICAL] Full historical ingestion complete. Total incidents: {total_new}")
        
        return total_new
        
    except Exception as e:
        logger.error(f"[HISTORICAL] Full historical ingestion failed: {e}", exc_info=True)
        raise


# CLI entry point
def main():
    """CLI entry point for the scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(description="EduThreat-CTI Ingestion Scheduler")
    parser.add_argument(
        "--mode",
        choices=["scheduler", "historical", "rss-once", "weekly-once", "enrich-once"],
        default="scheduler",
        help="Run mode: scheduler (continuous), historical (one-time full), or individual jobs"
    )
    parser.add_argument(
        "--rss-interval",
        type=int,
        default=2,
        help="Hours between RSS checks (default: 2)"
    )
    parser.add_argument(
        "--weekly-day",
        default="sunday",
        help="Day for weekly ingestion (default: sunday)"
    )
    parser.add_argument(
        "--weekly-time",
        default="02:00",
        help="Time for weekly ingestion (default: 02:00)"
    )
    parser.add_argument(
        "--no-enrichment",
        action="store_true",
        help="Disable automatic LLM enrichment after ingestion"
    )
    parser.add_argument(
        "--run-initial-rss",
        action="store_true",
        help="Run RSS ingestion immediately on scheduler start"
    )
    parser.add_argument(
        "--run-initial-weekly",
        action="store_true",
        help="Run weekly ingestion immediately on scheduler start"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    if args.mode == "historical":
        run_once_historical()
        return
    
    if args.mode == "rss-once":
        scheduler = IngestionScheduler(enable_enrichment=not args.no_enrichment)
        scheduler._run_rss_ingestion()
        return
    
    if args.mode == "weekly-once":
        scheduler = IngestionScheduler(enable_enrichment=not args.no_enrichment)
        scheduler._run_weekly_ingestion()
        return
    
    if args.mode == "enrich-once":
        scheduler = IngestionScheduler(enable_enrichment=True)
        scheduler._run_enrichment()
        return
    
    # Default: run scheduler
    scheduler = IngestionScheduler(
        rss_interval_hours=args.rss_interval,
        weekly_day=args.weekly_day,
        weekly_time=args.weekly_time,
        enable_enrichment=not args.no_enrichment,
    )
    
    scheduler.start(
        run_initial_rss=args.run_initial_rss,
        run_initial_weekly=args.run_initial_weekly,
    )
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(60)
            status = scheduler.get_status()
            logger.debug(f"Scheduler status: {status}")
    except KeyboardInterrupt:
        logger.info("Shutting down scheduler...")
        scheduler.stop()


if __name__ == "__main__":
    main()
