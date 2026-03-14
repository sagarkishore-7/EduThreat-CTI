"""
Pipeline Manager - Background execution engine for admin dashboard control.

Runs pipeline phases (ingest, enrich, historical, daily) in background threads
with real-time log capture, progress tracking, and run history.
"""

import logging
import threading
import time
import uuid
from collections import deque
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

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

    def _run_ingest(self, run: PipelineRun, params: Dict) -> Dict:
        """Run Phase 1 ingestion (all groups)."""
        from src.edu_cti.core.db import get_connection, init_db
        from src.edu_cti.pipeline.phase1.__main__ import GROUP_COLLECTORS, _ingest_group

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

            run.progress = {
                "step": f"Ingesting {group}",
                "detail": f"Group {i+1}/{len(groups)}",
                "percent": int((i / len(groups)) * 100),
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
                logger.info(f"{label}: {count} new incidents")
            except Exception as e:
                logger.error(f"Error ingesting {label}: {e}", exc_info=True)

        conn.close()
        run.progress = {"step": "Complete", "detail": "", "percent": 100}
        return {"new_incidents": total_new, "groups": groups, "full_historical": full_historical}

    def _run_enrich(self, run: PipelineRun, params: Dict) -> Dict:
        """Run Phase 2 LLM enrichment."""
        import sys

        limit = params.get("limit")
        rate_limit_delay = params.get("rate_limit_delay", 2.0)
        export_csv = params.get("export_csv", False)

        run.progress = {"step": "Starting enrichment", "detail": "", "percent": 0}

        # Build argv for phase2's argparse
        phase2_argv = []
        if limit:
            phase2_argv.extend(["--limit", str(limit)])
        if rate_limit_delay:
            phase2_argv.extend(["--rate-limit-delay", str(rate_limit_delay)])
        if export_csv:
            phase2_argv.append("--export-csv")
        phase2_argv.extend(["--log-level", "INFO"])

        original_argv = sys.argv
        sys.argv = ["phase2"] + phase2_argv
        try:
            from src.edu_cti.pipeline.phase2.__main__ import main as phase2_main
            phase2_main()
        finally:
            sys.argv = original_argv

        # Get enrichment stats
        from src.edu_cti.core.db import get_connection, init_db
        from src.edu_cti.pipeline.phase2.storage.db import get_enrichment_stats

        conn = get_connection()
        init_db(conn)
        stats = get_enrichment_stats(conn)
        conn.close()

        run.progress = {"step": "Complete", "detail": "", "percent": 100}
        return {"enrichment_stats": stats}

    def _run_historical(self, run: PipelineRun, params: Dict) -> Dict:
        """Run full historical pipeline (ingest all + enrich)."""
        skip_enrich = params.get("skip_enrich", False)
        enrich_limit = params.get("enrich_limit")

        # Phase 1: Full historical ingest
        run.progress = {"step": "Phase 1: Historical ingestion", "detail": "Starting...", "percent": 0}
        ingest_result = self._run_ingest(run, {
            "full_historical": True,
            "groups": ["curated", "news", "rss", "api"],
            "rss_max_age_days": 365,
        })

        if run._cancel_requested:
            return {"ingest": ingest_result, "enrich": None, "cancelled": True}

        # Phase 2: Enrich
        enrich_result = None
        if not skip_enrich:
            run.progress = {"step": "Phase 2: LLM Enrichment", "detail": "Starting...", "percent": 50}
            enrich_result = self._run_enrich(run, {
                "limit": enrich_limit,
                "rate_limit_delay": 2.0,
                "export_csv": True,
            })

        run.progress = {"step": "Complete", "detail": "", "percent": 100}
        return {"ingest": ingest_result, "enrich": enrich_result}

    def _run_daily(self, run: PipelineRun, params: Dict) -> Dict:
        """Run daily incremental pipeline (ingest new + enrich)."""
        skip_enrich = params.get("skip_enrich", False)
        enrich_limit = params.get("enrich_limit")

        # Phase 1: Incremental ingest
        run.progress = {"step": "Phase 1: Incremental ingestion", "detail": "", "percent": 0}
        ingest_result = self._run_ingest(run, {
            "full_historical": False,
            "groups": ["curated", "news", "rss", "api"],
            "rss_max_age_days": 7,
        })

        if run._cancel_requested:
            return {"ingest": ingest_result, "enrich": None, "cancelled": True}

        # Phase 2: Enrich unenriched
        enrich_result = None
        if not skip_enrich:
            run.progress = {"step": "Phase 2: Enrichment", "detail": "", "percent": 50}
            enrich_result = self._run_enrich(run, {
                "limit": enrich_limit,
                "rate_limit_delay": 2.0,
                "export_csv": True,
            })

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


def get_pipeline_manager() -> PipelineManager:
    """Get the singleton PipelineManager instance."""
    return PipelineManager()
