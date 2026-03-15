"""
Pipeline Manager - Background execution engine for admin dashboard control.

Runs pipeline phases (ingest, enrich, historical, daily) in background threads
with real-time log capture, progress tracking, and run history.

Also manages a built-in scheduler for continuous real-time intelligence collection.
"""

import logging
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import schedule

logger = logging.getLogger(__name__)


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PipelineRun:
    """Represents a single pipeline execution."""

    def __init__(self, run_id: str, phase: str, params: Dict[str, Any]):
        self.run_id = run_id
        self.phase = phase
        self.params = params
        self.status: RunStatus = RunStatus.PENDING
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self.duration_seconds: Optional[float] = None
        self.result: Dict[str, Any] = {}
        self.error: Optional[str] = None
        self.progress: Dict[str, Any] = {"step": "", "detail": "", "percent": 0}
        self.logs: deque = deque(maxlen=5000)
        self._cancel_requested = False

    def to_dict(self, include_logs: bool = False) -> Dict[str, Any]:
        d = {
            "run_id": self.run_id,
            "phase": self.phase,
            "params": self.params,
            "status": self.status.value,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "result": self.result,
            "error": self.error,
            "progress": self.progress,
        }
        if include_logs:
            d["logs"] = list(self.logs)
        return d


class RunLogHandler(logging.Handler):
    """Logging handler that captures log records into a PipelineRun."""

    def __init__(self, run: PipelineRun):
        super().__init__()
        self.run = run
        self.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )

    def emit(self, record):
        try:
            msg = self.format(record)
            self.run.logs.append(msg)
        except Exception:
            pass


class PipelineManager:
    """
    Singleton manager for pipeline execution.

    - Only one pipeline phase runs at a time.
    - Keeps history of recent runs (last 50).
    - Provides log access for streaming to dashboard.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._current_run: Optional[PipelineRun] = None
        self._history: deque = deque(maxlen=50)
        self._thread: Optional[threading.Thread] = None
        self._run_lock = threading.Lock()

        # Scheduler state
        self._scheduler_running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._scheduler_stop_event = threading.Event()
        self._scheduler_schedule = schedule.Scheduler()
        self._scheduler_started_at: Optional[str] = None
        self._scheduler_last_runs: Dict[str, Optional[str]] = {
            "rss": None,
            "api": None,
            "daily": None,
        }
        self._scheduler_total_new: int = 0

    @property
    def current_run(self) -> Optional[PipelineRun]:
        return self._current_run

    @property
    def is_running(self) -> bool:
        return self._current_run is not None and self._current_run.status == RunStatus.RUNNING

    def get_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        runs = list(self._history)
        runs.reverse()
        return [r.to_dict() for r in runs[:limit]]

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        if self._current_run and self._current_run.run_id == run_id:
            return self._current_run
        for run in self._history:
            if run.run_id == run_id:
                return run
        return None

    def request_cancel(self) -> bool:
        if self._current_run and self._current_run.status == RunStatus.RUNNING:
            self._current_run._cancel_requested = True
            # Signal Phase 2 enrichment cancel event (if running)
            try:
                from src.edu_cti.pipeline.phase2.__main__ import _cancel_event
                _cancel_event.set()
                logger.info(f"Cancel requested for run {self._current_run.run_id} (phase2 cancel event set)")
            except ImportError:
                logger.info(f"Cancel requested for run {self._current_run.run_id}")
            return True
        return False

    def start_phase(self, phase: str, params: Optional[Dict[str, Any]] = None) -> PipelineRun:
        """
        Start a pipeline phase in a background thread.

        Phases: ingest, enrich, historical, daily, ingest_source, rss, weekly
        """
        with self._run_lock:
            if self.is_running:
                raise RuntimeError(
                    f"Pipeline already running: {self._current_run.phase} "
                    f"(run_id={self._current_run.run_id})"
                )

            run_id = str(uuid.uuid4())[:12]
            run = PipelineRun(run_id, phase, params or {})
            self._current_run = run

            self._thread = threading.Thread(
                target=self._execute_run,
                args=(run,),
                daemon=True,
                name=f"pipeline-{phase}-{run_id}",
            )
            self._thread.start()
            return run

    def _execute_run(self, run: PipelineRun):
        """Execute a pipeline run in background thread."""
        run.status = RunStatus.RUNNING
        run.started_at = datetime.utcnow().isoformat()

        # Attach log handler
        log_handler = RunLogHandler(run)
        log_handler.setLevel(logging.DEBUG)
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)

        start_time = time.time()
        try:
            result = self._dispatch_phase(run)
            run.result = result or {}
            run.status = RunStatus.CANCELLED if run._cancel_requested else RunStatus.COMPLETED
        except Exception as e:
            run.status = RunStatus.FAILED
            run.error = str(e)
            logger.error(f"Pipeline run {run.run_id} failed: {e}", exc_info=True)
        finally:
            run.finished_at = datetime.utcnow().isoformat()
            run.duration_seconds = round(time.time() - start_time, 2)
            root_logger.removeHandler(log_handler)
            self._history.append(run)
            # Close the default HTTP client to free Playwright/Chromium memory
            try:
                from src.edu_cti.core.http import _default_client
                if _default_client is not None:
                    _default_client.close()
            except Exception:
                pass

    def _dispatch_phase(self, run: PipelineRun) -> Dict[str, Any]:
        """Route to the correct pipeline phase handler."""
        phase = run.phase
        params = run.params

        if phase == "ingest":
            return self._run_ingest(run, params)
        elif phase == "enrich":
            return self._run_enrich(run, params)
        elif phase == "historical":
            return self._run_historical(run, params)
        elif phase == "daily":
            return self._run_daily(run, params)
        elif phase == "ingest_source":
            return self._run_ingest_source(run, params)
        elif phase == "rss":
            return self._run_rss(run, params)
        elif phase == "weekly":
            return self._run_weekly(run, params)
        else:
            raise ValueError(f"Unknown pipeline phase: {phase}")

    # ------------------------------------------------------------------
    # Phase implementations
    # ------------------------------------------------------------------

    @staticmethod
    def _scale_percent(raw_percent: float, pmin: int, pmax: int) -> int:
        """Map a 0-100 raw percent into the [pmin, pmax] range."""
        return pmin + int((raw_percent / 100.0) * (pmax - pmin))

    def _run_ingest(self, run: PipelineRun, params: Dict,
                    progress_range: tuple = (0, 100), step_prefix: str = "") -> Dict:
        """Run Phase 1 ingestion (all groups).

        progress_range: (min_percent, max_percent) to map internal 0-100 into.
        step_prefix: optional prefix for step text (e.g. "Phase 1: ").
        """
        from src.edu_cti.core.db import get_connection, init_db
        from src.edu_cti.pipeline.phase1.__main__ import GROUP_COLLECTORS, _ingest_group

        pmin, pmax = progress_range
        full_historical = params.get("full_historical", False)
        groups = params.get("groups", ["curated", "news", "rss", "api"])
        sources = params.get("sources")
        max_pages = params.get("max_pages")
        rss_max_age_days = params.get("rss_max_age_days", 30)

        conn = get_connection()
        init_db(conn)

        total_new = 0
        for i, group in enumerate(groups):
            if run._cancel_requested:
                logger.info("Ingestion cancelled by user")
                break

            raw_pct = (i / len(groups)) * 100
            run.progress = {
                "step": f"{step_prefix}Ingesting {group}",
                "detail": f"Group {i+1}/{len(groups)} — collecting from sources…",
                "percent": self._scale_percent(raw_pct, pmin, pmax),
            }

            label, collector = GROUP_COLLECTORS[group]
            is_rss = group == "rss"
            kwargs = {
                "sources": sources if group in ("curated", "news", "rss") else None,
                "max_pages": max_pages if not is_rss else None,
                "max_age_days": rss_max_age_days if is_rss else None,
                "is_rss": is_rss,
                "incremental": not full_historical,
            }
            try:
                count = _ingest_group(conn, label, collector, **kwargs)
                total_new += count
                # Update detail after group finishes
                run.progress = {
                    "step": f"{step_prefix}Ingested {group}",
                    "detail": f"{count} new incidents from {label}",
                    "percent": self._scale_percent(((i + 1) / len(groups)) * 100, pmin, pmax),
                }
                logger.info(f"{label}: {count} new incidents")
            except Exception as e:
                logger.error(f"Error ingesting {label}: {e}", exc_info=True)

        conn.close()
        run.progress = {"step": f"{step_prefix}Ingestion complete", "detail": f"{total_new} total new incidents", "percent": pmax}
        return {"new_incidents": total_new, "groups": groups, "full_historical": full_historical}

    def _run_enrich(self, run: PipelineRun, params: Dict,
                    progress_range: tuple = (0, 100), step_prefix: str = "") -> Dict:
        """Run Phase 2 LLM enrichment.

        progress_range: (min_percent, max_percent) to map internal 0-100 into.
        step_prefix: optional prefix for step text (e.g. "Phase 2: ").
        """
        import sys

        pmin, pmax = progress_range
        limit = params.get("limit")
        rate_limit_delay = params.get("rate_limit_delay", 2.0)
        export_csv = params.get("export_csv", False)
        from src.edu_cti.core.config import ENRICHMENT_WORKERS
        workers = params.get("workers", ENRICHMENT_WORKERS)

        run.progress = {"step": f"{step_prefix}Starting enrichment", "detail": "", "percent": pmin}

        # Clear any previous cancel signal so this run starts fresh
        from src.edu_cti.pipeline.phase2.__main__ import _cancel_event, _progress
        _cancel_event.clear()
        _progress["step"] = "Starting enrichment"
        _progress["detail"] = ""
        _progress["percent"] = 0

        # Build argv for phase2's argparse
        phase2_argv = []
        if limit:
            phase2_argv.extend(["--limit", str(limit)])
        if rate_limit_delay:
            phase2_argv.extend(["--rate-limit-delay", str(rate_limit_delay)])
        if workers and workers > 1:
            phase2_argv.extend(["--workers", str(workers)])
        if export_csv:
            phase2_argv.append("--export-csv")
        phase2_argv.extend(["--log-level", "INFO"])

        # Run phase2 in a sub-thread so we can poll progress from this thread
        phase2_error = [None]  # mutable container for thread result

        def _run_phase2():
            nonlocal original_argv
            try:
                from src.edu_cti.pipeline.phase2.__main__ import main as phase2_main
                phase2_main()
            except Exception as e:
                phase2_error[0] = e
            finally:
                sys.argv = original_argv

        original_argv = sys.argv
        sys.argv = ["phase2"] + phase2_argv

        phase2_thread = threading.Thread(target=_run_phase2, daemon=True, name="phase2-exec")
        phase2_thread.start()

        # Poll progress from _progress dict until phase2 finishes
        while phase2_thread.is_alive():
            raw_pct = _progress.get("percent", 0)
            run.progress = {
                "step": f"{step_prefix}{_progress.get('step', '')}",
                "detail": _progress.get("detail", ""),
                "percent": self._scale_percent(raw_pct, pmin, pmax),
            }
            phase2_thread.join(timeout=2.0)

        # Re-raise any error from phase2
        if phase2_error[0] is not None:
            raise phase2_error[0]

        # Get enrichment stats
        from src.edu_cti.core.db import get_connection, init_db
        from src.edu_cti.pipeline.phase2.storage.db import get_enrichment_stats

        conn = get_connection()
        init_db(conn)
        stats = get_enrichment_stats(conn)
        conn.close()

        run.progress = {"step": f"{step_prefix}Enrichment complete", "detail": "", "percent": pmax}
        return {"enrichment_stats": stats}

    def _run_historical(self, run: PipelineRun, params: Dict) -> Dict:
        """Run full historical pipeline (ingest all + enrich)."""
        skip_enrich = params.get("skip_enrich", False)
        enrich_limit = params.get("enrich_limit")

        # Phase 1: Full historical ingest (0-50% of overall progress)
        ingest_result = self._run_ingest(run, {
            "full_historical": True,
            "groups": ["curated", "news", "rss", "api"],
            "rss_max_age_days": 365,
        }, progress_range=(0, 50), step_prefix="Phase 1: ")

        if run._cancel_requested:
            return {"ingest": ingest_result, "enrich": None, "cancelled": True}

        # Phase 2: Enrich (50-100% of overall progress)
        enrich_result = None
        if not skip_enrich:
            enrich_result = self._run_enrich(run, {
                "limit": enrich_limit,
                "rate_limit_delay": 2.0,
                "export_csv": True,
            }, progress_range=(50, 100), step_prefix="Phase 2: ")

        run.progress = {"step": "Complete", "detail": "", "percent": 100}
        return {"ingest": ingest_result, "enrich": enrich_result}

    def _run_daily(self, run: PipelineRun, params: Dict) -> Dict:
        """Run daily incremental pipeline (ingest new + enrich)."""
        skip_enrich = params.get("skip_enrich", False)
        enrich_limit = params.get("enrich_limit")

        # Phase 1: Incremental ingest (0-50% of overall progress)
        ingest_result = self._run_ingest(run, {
            "full_historical": False,
            "groups": ["curated", "news", "rss", "api"],
            "rss_max_age_days": 7,
        }, progress_range=(0, 50), step_prefix="Phase 1: ")

        if run._cancel_requested:
            return {"ingest": ingest_result, "enrich": None, "cancelled": True}

        # Phase 2: Enrich unenriched (50-100% of overall progress)
        enrich_result = None
        if not skip_enrich:
            enrich_result = self._run_enrich(run, {
                "limit": enrich_limit,
                "rate_limit_delay": 2.0,
                "export_csv": True,
            }, progress_range=(50, 100), step_prefix="Phase 2: ")

        run.progress = {"step": "Complete", "detail": "", "percent": 100}
        return {"ingest": ingest_result, "enrich": enrich_result}

    def _run_ingest_source(self, run: PipelineRun, params: Dict) -> Dict:
        """Run ingestion for a specific source group."""
        group = params.get("group", "curated")
        sources = params.get("sources")
        max_pages = params.get("max_pages")
        full_historical = params.get("full_historical", False)

        return self._run_ingest(run, {
            "full_historical": full_historical,
            "groups": [group],
            "sources": sources,
            "max_pages": max_pages,
        })

    def _run_rss(self, run: PipelineRun, params: Dict) -> Dict:
        """Run RSS feed ingestion only."""
        return self._run_ingest(run, {
            "full_historical": False,
            "groups": ["rss"],
            "rss_max_age_days": params.get("max_age_days", 7),
        })

    def _run_weekly(self, run: PipelineRun, params: Dict) -> Dict:
        """Run weekly full ingestion (curated + news)."""
        return self._run_ingest(run, {
            "full_historical": False,
            "groups": ["curated", "news"],
            "max_pages": params.get("max_pages"),
        })


    # ------------------------------------------------------------------
    # Scheduler — continuous real-time intelligence pipeline
    # ------------------------------------------------------------------

    @property
    def scheduler_running(self) -> bool:
        return self._scheduler_running

    def start_scheduler(
        self,
        rss_interval_hours: int = 1,
        api_interval_hours: int = 6,
        daily_interval_hours: int = 24,
        catch_up: bool = True,
    ) -> Dict[str, Any]:
        """
        Start the real-time intelligence pipeline scheduler.

        Runs recurring jobs:
        - RSS feeds: every rss_interval_hours (default 1h)
        - API sources: every api_interval_hours (default 6h)
        - Daily pipeline (all sources + enrich): every daily_interval_hours (default 24h)

        On first start, runs an immediate catch-up cycle.
        """
        if self._scheduler_running:
            return {"status": "already_running", "started_at": self._scheduler_started_at}

        self._scheduler_running = True
        self._scheduler_stop_event.clear()
        self._scheduler_started_at = datetime.utcnow().isoformat()
        self._scheduler_total_new = 0

        # Reset last run timestamps
        for key in self._scheduler_last_runs:
            self._scheduler_last_runs[key] = None

        # Clear any previous jobs and register new ones
        self._scheduler_schedule.clear()

        self._scheduler_schedule.every(rss_interval_hours).hours.do(
            self._scheduler_run_job, "rss", {"max_age_days": 7}
        )
        self._scheduler_schedule.every(api_interval_hours).hours.do(
            self._scheduler_run_job, "ingest_source", {"group": "api"}
        )
        self._scheduler_schedule.every(daily_interval_hours).hours.do(
            self._scheduler_run_job, "daily", {}
        )

        logger.info(
            f"[SCHEDULER] Started — RSS every {rss_interval_hours}h, "
            f"API every {api_interval_hours}h, Daily every {daily_interval_hours}h"
        )

        # Start the scheduler loop thread
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            daemon=True,
            name="scheduler-loop",
        )
        self._scheduler_thread.start()

        # Run initial catch-up in yet another thread so start_scheduler returns immediately
        if catch_up:
            threading.Thread(
                target=self._scheduler_catchup,
                daemon=True,
                name="scheduler-catchup",
            ).start()

        return {
            "status": "started",
            "started_at": self._scheduler_started_at,
            "rss_interval_hours": rss_interval_hours,
            "api_interval_hours": api_interval_hours,
            "daily_interval_hours": daily_interval_hours,
        }

    def stop_scheduler(self) -> Dict[str, Any]:
        """Stop the scheduler. Does NOT cancel the currently running pipeline phase."""
        if not self._scheduler_running:
            return {"status": "not_running"}

        self._scheduler_running = False
        self._scheduler_stop_event.set()
        self._scheduler_schedule.clear()

        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=10)

        logger.info("[SCHEDULER] Stopped")
        return {"status": "stopped"}

    def get_scheduler_status(self) -> Dict[str, Any]:
        """Return scheduler status for the admin API."""
        jobs = []
        for job in self._scheduler_schedule.get_jobs():
            jobs.append({
                "interval": str(job.interval),
                "unit": job.unit,
                "next_run": job.next_run.isoformat() if job.next_run else None,
            })

        return {
            "running": self._scheduler_running,
            "started_at": self._scheduler_started_at,
            "last_runs": dict(self._scheduler_last_runs),
            "total_new_incidents": self._scheduler_total_new,
            "jobs": jobs,
        }

    # --- internal helpers ---

    def _scheduler_loop(self):
        """Background loop that ticks the schedule library."""
        logger.info("[SCHEDULER] Loop started")
        while self._scheduler_running and not self._scheduler_stop_event.is_set():
            self._scheduler_schedule.run_pending()
            # Sleep in small increments so we can react to stop quickly
            self._scheduler_stop_event.wait(timeout=30)
        logger.info("[SCHEDULER] Loop exited")

    def _scheduler_catchup(self):
        """Run an initial catch-up: daily pipeline to ingest recent incidents."""
        logger.info("[SCHEDULER] Running initial catch-up cycle...")
        self._scheduler_run_job("daily", {})

    def _scheduler_run_job(self, phase: str, params: Dict[str, Any]):
        """
        Execute a scheduled job. Waits if another pipeline is already running.
        """
        if not self._scheduler_running:
            return

        # Determine the job key for tracking
        if phase == "ingest_source" and params.get("group") == "api":
            job_key = "api"
        elif phase in self._scheduler_last_runs:
            job_key = phase
        else:
            job_key = phase

        logger.info(f"[SCHEDULER] Job triggered: {phase} (params={params})")

        # Wait for any running pipeline to finish (up to 30 min)
        wait_start = time.time()
        max_wait = 1800  # 30 minutes
        while self.is_running:
            if not self._scheduler_running:
                logger.info(f"[SCHEDULER] Scheduler stopped while waiting for {phase}")
                return
            if time.time() - wait_start > max_wait:
                logger.warning(f"[SCHEDULER] Timed out waiting for pipeline to finish, skipping {phase}")
                return
            time.sleep(10)

        # Start the phase
        try:
            run = self.start_phase(phase, params)
            logger.info(f"[SCHEDULER] Started {phase} (run_id={run.run_id})")

            # Wait for it to complete
            while self.is_running:
                if not self._scheduler_running:
                    break
                time.sleep(5)

            # Record results
            self._scheduler_last_runs[job_key] = datetime.utcnow().isoformat()
            if run.result and isinstance(run.result, dict):
                new_incidents = run.result.get("new_incidents", 0)
                if isinstance(run.result.get("ingest"), dict):
                    new_incidents = run.result["ingest"].get("new_incidents", 0)
                self._scheduler_total_new += new_incidents

            logger.info(f"[SCHEDULER] Completed {phase} — status={run.status.value}")

        except RuntimeError as e:
            logger.warning(f"[SCHEDULER] Could not start {phase}: {e}")
        except Exception as e:
            logger.error(f"[SCHEDULER] Error running {phase}: {e}", exc_info=True)


def get_pipeline_manager() -> PipelineManager:
    """Get the singleton PipelineManager instance."""
    return PipelineManager()
